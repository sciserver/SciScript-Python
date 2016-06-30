import json
import time

import sys
from io import StringIO

import requests
import pandas

from SciServer import LoginPortal, Config


def getSchemaName(token=""):
    if (token == ""):
        userToken = LoginPortal.getToken()
    else:
        userToken = token;
    keystoneUserId = LoginPortal.getKeystoneUserWithToken(userToken).id
    usersUrl = Config.CasJobsRESTUri + "/users/" + keystoneUserId
    headers={'X-Auth-Token': userToken,'Content-Type': 'application/json'}
    getResponse = requests.get(usersUrl,headers=headers)
    jsonResponse = json.loads(getResponse.content.decode())
    return "wsid_" + str(jsonResponse["WebServicesId"])


def getTables(context="MyDB"):
    """Returns all the tables from the current user in the given context.
    The result is a json object with format [{"Date":seconds,"Name":"TableName","Rows":int,"Size",int},..]"""

    TablesUrl = Config.CasJobsRESTUri + "/contexts/" + context + "/Tables"

    headers={'X-Auth-Token': LoginPortal.getToken(),'Content-Type': 'application/json'}

    getResponse = requests.get(TablesUrl,headers=headers)

    jsonResponse = json.loads(getResponse.content.decode())

    return jsonResponse


def executeQuery(queryString, context="MyDB", acceptHeader="application/json+array", token="", format="pandas"):
    """Executes a casjob query.  If a token is supplied then it will execute on behalf of the token's user.
    format parameter specifies the return type:
    'pandas': pandas.DataFrame
    'csv': a csv string
    'readable' : a StringIO, readable object wrapping a csv string that can be passed into pandas.read_csv for example.
    'json': a dict created from a JSON string with the Query, a Result consisting of a Columns and a Data field.
    """

    if (format == "pandas") or (format =="json"):
        acceptHeader="application/json+array"
    elif (format == "csv") or (format == "readable"):
        acceptHeader = "text/plain"
    else:
        return {"Error":{"Message":"Illegal format specification '"+format+"'"}}

    QueryUrl = Config.CasJobsRESTUri + "/contexts/" + context + "/query"

    query = {"Query": queryString}

    data = json.dumps(query).encode()

    headers = {'Content-Type': 'application/json', 'Accept': acceptHeader}
    if (token == ""):
        headers['X-Auth-Token'] = LoginPortal.getToken()
    else:
        headers['X-Auth-Token'] = token


    try:
        postResponse = requests.post(QueryUrl,data=data,headers=headers)
        if postResponse.status_code != 200:
            return {"Error":{"ErrorCode":postResponse.status_code,"Message":postResponse.content.decode()}}
        r=postResponse.content.decode()
        if (format == "readable"):
            return StringIO(r)
        elif format == "pandas":
            r=json.loads(r)
            return pandas.DataFrame(r['Result'][0]['Data'],columns=r['Result'][0]['Columns'])
        elif format == "csv":
            return r
        elif format == "json":
            return json.loads(r)
        else: # should not occur
            return {"Error":{"Message":"Illegal format specification '"+format+"'"}}
    except requests.exceptions.RequestException as e:
        return e


def submitJob(queryString, context="MyDB", acceptHeader="text/plain", token=""):
    """Submits a query to the casjobs queue.  If a token is supplied then it will execute on behalf of the token's user.
    Returns the casjobs jobID (int)."""

    QueryUrl = Config.CasJobsRESTUri + "/contexts/" + context + "/jobs"

    query = {"Query": queryString}

    data = json.dumps(query).encode()

    headers = {'Content-Type': 'application/json', 'Accept': acceptHeader}
    if (token == ""):
        headers['X-Auth-Token']= LoginPortal.getToken()
    else:
        headers['X-Auth-Token']=  token

    try:
        putResponse = requests.put(QueryUrl,data=data,headers=headers)

        return int(putResponse.content.decode())
    except requests.exceptions.RequestException as e:
        return e


def getJobStatus(jobid):
    """Gets a casjobs job status.
    Returns the dict object (https://docs.python.org/3.4/library/stdtypes.html#dict) coresponding to the json received from casjobs."""

    QueryUrl = Config.CasJobsRESTUri + "/jobs/" + str(jobid)

    headers={'X-Auth-Token': LoginPortal.getToken(),'Content-Type': 'application/json'}

    try:
        postResponse =requests.get(QueryUrl,headers=headers)

        return json.loads(postResponse.content.decode())
    except requests.exceptions.RequestException as e:
        return e.code


def waitForJob(jobid):
    """Waits for the casjobs job to return a status of 3, 4, or 5.
    Queries the job status from casjobs every 2 seconds."""

    complete = False

    waitingStr = "Waiting..."
    back = "\b" * len(waitingStr)
    print waitingStr,

    while not complete:
        print back,
        print waitingStr,
        jobDesc = getJobStatus(jobid)
        jobStatus = int(jobDesc["Status"])
        if jobStatus in (3, 4, 5):
            complete = True
            print back,
            print "Done!"
        else:
            time.sleep(2)

    return jobDesc


def getFitsFileFromQuery(filename, queryString, context="MyDB", token=""):
    """Executes a casjobs query and writes the result to a fits (http://www.stsci.edu/institute/software_hardware/pyfits) file.
    Returns True if successful. """

    try:
        fitsResponse = executeQuery(queryString, context=context, acceptHeader="application/fits", token=token)

        theFile = open(filename, "w+b")
        theFile.write(fitsResponse.read())
        theFile.close()

        return True
    except Exception as e:
        print e
        return False


# no explicit index column by default
def getPandasDataFrameFromQuery(queryString, context="MyDB", token="", index_col=None):
    """Executes a casjobs query and stores the cvs result in a pandas (http://pandas.pydata.org/pandas-docs/stable/) dataframe object.
    Returns the dataframe."""

    cvsResponse = executeQuery(queryString, context=context, token=token)

    #if the index column is not specified then it will add it's own column which causes problems when uploading the transformed data
    dataFrame = pandas.read_csv(cvsResponse, index_col=index_col)

    return dataFrame


def getNumpyArrayFromQuery(queryString, context="MyDB", token=""):
    """Executes a casjobs query and stores the cvs result in a numpy (http://docs.scipy.org/doc/numpy/) array.
    Returns numpy array."""

    dataFrame = getPandasDataFrameFromQuery(queryString, context, token)

    return dataFrame.as_matrix()


#require pandas for now but be able to take a string in the future
def uploadPandasDataFrameToTable(dataFrame, tableName, context="MyDB", token=""):
    """Uploads a pandas dataframe object into casjobs.
    Returns the output from casjobs in string form."""

    if dataFrame.index.name == "" or dataFrame.index.name is None:
        dataFrame.index.name = "index"

    sio = dataFrame.to_csv().encode("utf8")

    return uploadCVSDataToTable(sio, tableName, context, token)


def uploadCVSDataToTable(CVSdata, tableName, context="MyDB", token=""):
    """Uploads  cvs data into casjobs.  data should support the buffered interface (it should have a read() method).
    https://docs.python.org/3/library/urllib.request.html
    Returns the output from casjobs in string form."""

    print "Uploading ", sys.getsizeof(CVSdata), "bytes..."
    tablesUrl = Config.CasJobsRESTUri + "/contexts/" + context + "/Tables/" + tableName

    headers={}
    if (token == ""):
        headers['X-Auth-Token']= LoginPortal.getToken()
    else:
        headers['X-Auth-Token']= token

    try:
        postResponse = requests.post(tablesUrl,data=CVSdata,headers=headers)
        print "uploadCVSDataFrameToTable POST response: ", postResponse.status_code, postResponse.reason

        return postResponse.content.decode()
    except Exception as error:
        print "There was a problem uploading the data. Exception message: ", error.read()