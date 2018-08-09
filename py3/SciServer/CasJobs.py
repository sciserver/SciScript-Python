import json
import time

import sys
from io import StringIO, BytesIO

import requests as requests
import pandas

from SciServer import Authentication, Config


class Task:
    """
    The class TaskName stores the name of the task that executes the API call.
    """
    name = None


task = Task();


def getSchemaName():
    """
    Returns the WebServiceID that identifies the schema for a user in MyScratch database with CasJobs.

    :return: WebServiceID of the user (string).
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: wsid = CasJobs.getSchemaName()

    .. seealso:: CasJobs.getTables.
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        keystoneUserId = Authentication.getKeystoneUserWithToken(token).id

        taskName = ""
        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.CasJobs.getSchemaName"
        else:
            taskName = "SciScript-Python.CasJobs.getSchemaName"

        usersUrl = Config.CasJobsRESTUri + "/users/" + keystoneUserId + "?TaskName=" + taskName
        headers={'X-Auth-Token': token,'Content-Type': 'application/json'}
        getResponse = requests.get(usersUrl,headers=headers)
        if getResponse.status_code != 200:
            raise Exception("Error when getting schema name. Http Response from CasJobs API returned status code " + str(getResponse.status_code) + ":\n" + getResponse.content.decode());

        jsonResponse = json.loads(getResponse.content.decode())
        return "wsid_" + str(jsonResponse["WebServicesId"])
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def getTables(context="MyDB"):
    """
    Gets the names, size and creation date of all tables in a database context that the user has access to.

    :param context:	database context (string)
    :return: The result is a json object with format [{"Date":seconds,"Name":"TableName","Rows":int,"Size",int},..]
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: tables = CasJobs.getTables("MyDB")

    .. seealso:: CasJobs.getSchemaName
    """

    token = Authentication.getToken()
    if token is not None and token != "":

        taskName = "";
        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.CasJobs.getTables"
        else:
            taskName = "SciScript-Python.CasJobs.getTables"

        TablesUrl = Config.CasJobsRESTUri + "/contexts/" + context + "/Tables" + "?TaskName=" + taskName

        headers={'X-Auth-Token': token,'Content-Type': 'application/json'}

        getResponse = requests.get(TablesUrl,headers=headers)

        if getResponse.status_code != 200:
            raise Exception("Error when getting table description from database context " + str(context) + ".\nHttp Response from CasJobs API returned status code " + str(getResponse.status_code) + ":\n" + getResponse.content.decode());

        jsonResponse = json.loads(getResponse.content.decode())

        return jsonResponse
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def executeQuery(sql, context="MyDB", format="pandas"):
    """
    Executes a synchronous SQL query in a CasJobs database context.

    :param sql: sql query (string)
    :param context: database context (string)
    :param format: parameter (string) that specifies the return type:\n
    \t\t'pandas': pandas.DataFrame.\n
    \t\t'json': a JSON string containing the query results. \n
    \t\t'dict': a dictionary created from the JSON string containing the query results.\n
    \t\t'csv': a csv string.\n
    \t\t'readable': an object of type io.StringIO, which has the .read() method and wraps a csv string that can be passed into pandas.read_csv for example.\n
    \t\t'StringIO': an object of type io.StringIO, which has the .read() method and wraps a csv string that can be passed into pandas.read_csv for example.\n
    \t\t'fits': an object of type io.BytesIO, which has the .read() method and wraps the result in fits format.\n
    \t\t'BytesIO': an object of type io.BytesIO, which has the .read() method and wraps the result in fits format.\n
    :return: the query result table, in a format defined by the 'format' input parameter.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error. Throws an exception if parameter 'format' is not correctly specified.
    :example: table = CasJobs.executeQuery(sql="select 1 as foo, 2 as bar",format="pandas", context="MyDB")

    .. seealso:: CasJobs.submitJob, CasJobs.getTables, SkyServer.sqlSearch
    """

    if (format == "pandas") or (format =="json") or (format =="dict"):
        acceptHeader="application/json+array"
    elif (format == "csv") or (format == "readable") or (format == "StringIO"):
        acceptHeader = "text/plain"
    elif format == "fits":
        acceptHeader = "application/fits"
    elif format == "BytesIO":
        acceptHeader = "application/fits" # defined later using specific serialization
    else:
        raise Exception("Error when executing query. Illegal format parameter specification: " + str(format));

    taskName = "";
    if task.name is not None:
        taskName = task.name;
        task.name = None;
    else:
        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.CasJobs.executeQuery"
        else:
            taskName = "SciScript-Python.CasJobs.executeQuery"

    QueryUrl = Config.CasJobsRESTUri + "/contexts/" + context + "/query"  + "?TaskName=" + taskName

    query = {"Query": sql, "TaskName": taskName}

    data = json.dumps(query).encode()

    headers = {'Content-Type': 'application/json', 'Accept': acceptHeader}
    token = Authentication.getToken()
    if token is not None and token != "":
        headers['X-Auth-Token'] = token

    postResponse = requests.post(QueryUrl,data=data,headers=headers, stream=True)
    if postResponse.status_code != 200:
        raise Exception("Error when executing query. Http Response from CasJobs API returned status code " + str(postResponse.status_code) + ":\n" + postResponse.content.decode());

    if (format == "readable") or (format == "StringIO"):
        return StringIO(postResponse.content.decode())
    elif format == "pandas":
        r=json.loads(postResponse.content.decode())
        if len(r['Result']) > 1:
            res = []
            for result in r['Result']:
                res.append(pandas.DataFrame(result['Data'],columns=result['Columns']))

            return res
        else:
            return pandas.DataFrame(r['Result'][0]['Data'],columns=r['Result'][0]['Columns'])

    elif format == "csv":
        return postResponse.content.decode()
    elif format == "dict":
        return json.loads(postResponse.content.decode())
    elif format == "json":
        return  postResponse.content.decode()
    elif format == "fits":
        return BytesIO(postResponse.content)
    elif format == "BytesIO":
        return BytesIO(postResponse.content)
    else: # should not occur
        raise Exception("Error when executing query. Illegal format parameter specification: " + str(format));

def submitJob(sql, context="MyDB"):
    """
    Submits an asynchronous SQL query to the CasJobs queue.

    :param sql: sql query (string)
    :param context:	database context (string)
    :return: Returns the CasJobs jobID (integer).
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: jobid = CasJobs.submitJob("select 1 as foo","MyDB")

    .. seealso:: CasJobs.executeQuery, CasJobs.getJobStatus, CasJobs.waitForJob, CasJobs.cancelJob.
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        taskName = "";
        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.CasJobs.submitJob"
        else:
            taskName = "SciScript-Python.CasJobs.submitJob"

        QueryUrl = Config.CasJobsRESTUri + "/contexts/" + context + "/jobs" + "?TaskName=" + taskName

        query = {"Query": sql, "TaskName": taskName}

        data = json.dumps(query).encode()

        headers = {'Content-Type': 'application/json', 'Accept': "text/plain"}
        headers['X-Auth-Token']=  token


        putResponse = requests.put(QueryUrl,data=data,headers=headers)
        if putResponse.status_code != 200:
            raise Exception("Error when submitting a job. Http Response from CasJobs API returned status code " + str(putResponse.status_code) + ":\n" + putResponse.content.decode());

        return int(putResponse.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def getJobStatus(jobId):
    """
    Shows the status of a job submitted to CasJobs.

    :param jobId: id of job (integer)
    :return: Returns a dictionary object containing the job status and related metadata. The "Status" field can be equal to 0 (Ready), 1 (Started), 2 (Canceling), 3(Canceled), 4 (Failed) or 5 (Finished). If jobId is the empty string, then returns a list with the statuses of all previous jobs.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: status = CasJobs.getJobStatus(CasJobs.submitJob("select 1"))

    .. seealso:: CasJobs.submitJob, CasJobs.waitForJob, CasJobs.cancelJob.
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        taskName = "";
        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.CasJobs.getJobStatus"
        else:
            taskName = "SciScript-Python.CasJobs.getJobStatus"

        QueryUrl = Config.CasJobsRESTUri + "/jobs/" + str(jobId) + "?TaskName=" + taskName

        headers={'X-Auth-Token': token,'Content-Type': 'application/json'}

        postResponse =requests.get(QueryUrl,headers=headers)
        if postResponse.status_code != 200:
            raise Exception("Error when getting the status of job " + str(jobId) + ".\nHttp Response from CasJobs API returned status code " + str(postResponse.status_code) + ":\n" + postResponse.content.decode());

        return json.loads(postResponse.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def cancelJob(jobId):
    """
    Cancels a job already submitted.

    :param jobId: id of job (integer)
    :return: Returns True if the job was canceled successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: response = CasJobs.cancelJob(CasJobs.submitJob("select 1"))

    .. seealso:: CasJobs.submitJob, CasJobs.waitForJob.
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        taskName = "";
        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.CasJobs.cancelJob"
        else:
            taskName = "SciScript-Python.CasJobs.cancelJob"

        QueryUrl = Config.CasJobsRESTUri + "/jobs/" + str(jobId) + "?TaskName=" + taskName

        headers={'X-Auth-Token': token,'Content-Type': 'application/json'}

        response =requests.delete(QueryUrl,headers=headers)
        if response.status_code != 200:
            raise Exception("Error when canceling job " + str(jobId) + ".\nHttp Response from CasJobs API returned status code " + str(response.status_code) + ":\n" + response.content.decode());

        return True;#json.loads(response.content)
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def waitForJob(jobId, verbose=False, pollTime = 5):
    """
    Queries regularly the job status and waits until the job is completed.

    :param jobId: id of job (integer)
    :param verbose: if True, will print "wait" messages on the screen while the job is still running. If False, will suppress the printing of messages on the screen.
    :param pollTime: idle time interval (integer, in seconds) before querying again for the job status. Minimum value allowed is 5 seconds.
    :return: After the job is finished, returns a dictionary object containing the job status and related metadata. The "Status" field can be equal to 0 (Ready), 1 (Started), 2 (Canceling), 3(Canceled), 4 (Failed) or 5 (Finished).
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: CasJobs.waitForJob(CasJobs.submitJob("select 1"))

    .. seealso:: CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.cancelJob.
    """

    try:
        minPollTime = 5 # in seconds
        complete = False

        waitingStr = "Waiting..."
        back = "\b" * len(waitingStr)
        if verbose:
            print(waitingStr, end="")

        while not complete:
            if verbose:
                #print(back, end="")
                print(waitingStr, end="")
            jobDesc = getJobStatus(jobId)
            jobStatus = int(jobDesc["Status"])
            if jobStatus in (3, 4, 5):
                complete = True
                if verbose:
                    #print(back, end="")
                    print("Done!")
            else:
                time.sleep(max(minPollTime,pollTime));

        return jobDesc
    except Exception as e:
        raise e;


def writeFitsFileFromQuery(fileName, queryString, context="MyDB"):
    """
    Executes a quick CasJobs query and writes the result to a local Fits file (http://www.stsci.edu/institute/software_hardware/pyfits).

    :param fileName: path to the local Fits file to be created (string)
    :param queryString: sql query (string)
    :param context: database context (string)
    :return: Returns True if the fits file was created successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: CasJobs.writeFitsFileFromQuery("/home/user/myFile.fits","select 1 as foo")

    .. seealso:: CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.executeQuery, CasJobs.getPandasDataFrameFromQuery, CasJobs.getNumpyArrayFromQuery
    """
    try:


        if Config.isSciServerComputeEnvironment():
            task.name = "Compute.SciScript-Python.CasJobs.writeFitsFileFromQuery"
        else:
            task.name = "SciScript-Python.CasJobs.writeFitsFileFromQuery"

        bytesio = executeQuery(queryString, context=context, format="fits")

        theFile = open(fileName, "w+b")
        theFile.write(bytesio.read())
        theFile.close()

        return True

    except Exception as e:
        raise e

# no explicit index column by default
def getPandasDataFrameFromQuery(queryString, context="MyDB"):
    """
    Executes a casjobs quick query and returns the result as a pandas dataframe object with an index (http://pandas.pydata.org/pandas-docs/stable/).

    :param queryString: sql query (string)
    :param context: database context (string)
    :return: Returns a Pandas dataframe containing the results table.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: df = CasJobs.getPandasDataFrameFromQuery("select 1 as foo", context="MyDB")

    .. seealso:: CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.executeQuery, CasJobs.writeFitsFileFromQuery, CasJobs.getNumpyArrayFromQuery
    """
    try:

        if Config.isSciServerComputeEnvironment():
            task.name = "Compute.SciScript-Python.CasJobs.getPandasDataFrameFromQuery"
        else:
            task.name = "SciScript-Python.CasJobs.getPandasDataFrameFromQuery"

        cvsResponse = executeQuery(queryString, context=context,format="readable")

        #if the index column is not specified then it will add it's own column which causes problems when uploading the transformed data
        dataFrame = pandas.read_csv(cvsResponse, index_col=None)

        return dataFrame

    except Exception as e:
        raise e

def getNumpyArrayFromQuery(queryString, context="MyDB"):
    """
    Executes a casjobs query and returns the results table as a Numpy array (http://docs.scipy.org/doc/numpy/).

    :param queryString: sql query (string)
    :param context: database context (string)
    :return: Returns a Numpy array storing the results table.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: array = CasJobs.getNumpyArrayFromQuery("select 1 as foo", context="MyDB")

    .. seealso:: CasJobs.submitJob, CasJobs.getJobStatus, CasJobs.executeQuery, CasJobs.writeFitsFileFromQuery, CasJobs.getPandasDataFrameFromQuery

    """
    try:

        if Config.isSciServerComputeEnvironment():
            task.name = "Compute.SciScript-Python.CasJobs.getNumpyArrayFromQuery"
        else:
            task.name = "SciScript-Python.CasJobs.getNumpyArrayFromQuery"

        dataFrame = getPandasDataFrameFromQuery(queryString, context)

        return dataFrame.as_matrix()

    except Exception as e:
        raise e


#require pandas for now but be able to take a string in the future
def uploadPandasDataFrameToTable(dataFrame, tableName, context="MyDB"):
    """
    Uploads a pandas dataframe object into a CasJobs table. If the dataframe contains a named index, then the index will be uploaded as a column as well.

    :param dataFrame: Pandas data frame containg the data (pandas.core.frame.DataFrame)
    :param tableName: name of CasJobs table to be created.
    :param context: database context (string)
    :return: Returns True if the dataframe was uploaded successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: response = CasJobs.uploadPandasDataFrameToTable(CasJobs.getPandasDataFrameFromQuery("select 1 as foo", context="MyDB"), "NewTableFromDataFrame")

    .. seealso:: CasJobs.uploadCSVDataToTable
    """
    try:

        if Config.isSciServerComputeEnvironment():
            task.name = "Compute.SciScript-Python.CasJobs.uploadPandasDataFrameToTable"
        else:
            task.name = "SciScript-Python.CasJobs.uploadPandasDataFrameToTable"

        if dataFrame.index.name is not None and dataFrame.index.name != "":
            sio = dataFrame.to_csv().encode("utf8")
        else:
            sio = dataFrame.to_csv(index_label=False, index=False).encode("utf8")

        return uploadCSVDataToTable(sio, tableName, context)

    except Exception as e:
        raise e

def uploadCSVDataToTable(csvData, tableName, context="MyDB"):
    """
    Uploads CSV data into a CasJobs table.

    :param csvData: a CSV table in string format.
    :param tableName: name of CasJobs table to be created.
    :param context: database context (string)
    :return: Returns True if the csv data was uploaded successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the CasJobs API returns an error.
    :example: csv = CasJobs.getPandasDataFrameFromQuery("select 1 as foo", context="MyDB").to_csv().encode("utf8"); response = CasJobs.uploadCSVDataToTable(csv, "NewTableFromDataFrame")

    .. seealso:: CasJobs.uploadPandasDataFrameToTable
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        #if (Config.executeMode == "debug"):
        #    print "Uploading ", sys.getsizeof(CVSdata), "bytes..."

        taskName = "";
        if task.name is not None:
            taskName = task.name;
            task.name = None;
        else:
            if Config.isSciServerComputeEnvironment():
                taskName = "Compute.SciScript-Python.CasJobs.uploadCSVDataToTable"
            else:
                taskName = "SciScript-Python.CasJobs.uploadCSVDataToTable"

        tablesUrl = Config.CasJobsRESTUri + "/contexts/" + context + "/Tables/" + tableName + "?TaskName=" + taskName

        headers={}
        headers['X-Auth-Token']= token

        postResponse = requests.post(tablesUrl,data=csvData,headers=headers, stream=True)
        if postResponse.status_code != 200:
            raise Exception("Error when uploading CSV data into CasJobs table " + tableName + ".\nHttp Response from CasJobs API returned status code " + str(postResponse.status_code) + ":\n" + postResponse.content.decode());

        return True

    else:
        raise Exception("User token is not defined. First log into SciServer.")
