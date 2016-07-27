import json

import sys
from io import StringIO

import requests
import pandas

from SciServer import Authentication, Config


######################################################################################################################
# Jobs:


def getJobStatus(jobID, token = ''):
    """
    Gets the status of a job, as well as other metadata.
    Parameters:
    jobID: the ID of the job, which is obtained at the moment of submitting the job.
    token: SciServer's authentication token.

    Output: json string with the job status and other related metadata.
    """
    statusURL = Config.SkyQueryUrl + '/Jobs.svc/jobs/' + jobID

    headers = {'Content-Type': 'application/json','Accept': 'application/json'}
    if (token == ""):
        headers['X-Auth-Token']= Authentication.getToken()
    else:
        headers['X-Auth-Token']=  token

    response = requests.get(statusURL, headers=headers)

    if response.status_code == 200:
        r = response.json()
        return(r['queryJob'])
    else:
        raise NameError(response.json())


def cancelJob(jobID, token = ''):
    """
    Cancels a single job.
    Parameters:
    jobID: the ID of the job, which is obtained at the moment of submitting the job.
    token: SciServer's authentication token.
    """
    statusURL = Config.SkyQueryUrl + '/Jobs.svc/jobs/' + jobID

    headers = {'Content-Type': 'application/json','Accept': 'application/json'}
    if (token == ""):
        headers['X-Auth-Token']= Authentication.getToken()
    else:
        headers['X-Auth-Token']=  token

    response = requests.delete(statusURL, headers=headers)

    if response.status_code == 200:
        r = response.json()
        return(r['queryJob'])
    else:
        raise NameError(response.json())


def listQueues(token = ''):
    """
    Returns a list of all available job queues and related metadata.
    """
    jobsURL = Config.SkyQueryUrl + '/Jobs.svc/queues'

    headers = {'Content-Type': 'application/json','Accept': 'application/json'}
    if (token == ""):
        headers['X-Auth-Token']= Authentication.getToken()
    else:
        headers['X-Auth-Token']=  token

    response = requests.get(jobsURL, headers=headers)

    if response.status_code == 200:
        r = response.content
        return(r)
    else:
        raise NameError(response.json())


def getQueueInfo(queue, token = ''):
    """
 	Returns information about a particular job queue.
 	"""
    jobsURL = Config.SkyQueryUrl + '/Jobs.svc/queues/' + queue

    headers = {'Content-Type': 'application/json','Accept': 'application/json'}
    if (token == ""):
        headers['X-Auth-Token']= Authentication.getToken()
    else:
        headers['X-Auth-Token']=  token

    response = requests.get(jobsURL, headers=headers)

    if response.status_code == 200:
        r = response.content
        return(r)
    else:
        raise NameError(response.json())


def submitJob(query,token = '', queue='quick'):
    """
    Submits a new job.
    Parameters:
    query: sql query
    token: SciServer's authentication token.
    queue: can be set to 'quick' for a quick job, or 'long' for a long job.

    Output: returns the jobID unique identifier of the job.
    """
    jobsURL = Config.SkyQueryUrl + '/Jobs.svc/queues/' + queue + '/jobs'

    headers = {'Content-Type': 'application/json','Accept': 'application/json'}
    if (token == ""):
        headers['X-Auth-Token']= Authentication.getToken()
    else:
        headers['X-Auth-Token']=  token

    body={"queryJob":{"query":query}}

    data=json.dumps(body).encode()

    response = requests.post(jobsURL,data=data,headers=headers)

    if response.status_code == 200:
        r = response.json()
        return(r['queryJob']['guid'])
    else:
        raise NameError(response.json())


def listJobs(queue, token = ''):
    """
    Lists the jobs in the queue in descending order by submission time. Only jobs of the authenticated user are listed.
    Parameters:
    queue: can be set to 'quick' for a quick job, or 'long' for a long job.
    token: SciServer's authentication token.

    Output: returns the jobID unique identifier of the job.
    """
    jobsURL = Config.SkyQueryUrl + '/Jobs.svc/queues/' + queue + '/jobs?'

    headers = {'Content-Type': 'application/json','Accept': 'application/json'}
    if (token == ""):
        headers['X-Auth-Token']= Authentication.getToken()
    else:
        headers['X-Auth-Token']=  token

    response = requests.get(jobsURL,headers=headers)

    if response.status_code == 200:
        r = response.json()
        return(r)
    else:
        raise NameError(response.json())




######################################################################################################################
# Schema:


def listAllDatasets(token = ''):
    """
    lists all available datasets
    """
    schemaURL = Config.SkyQueryUrl + '/Schema.svc/datasets'

    headers = {'Content-Type': 'application/json','Accept': 'application/json'}
    if (token == ""):
        headers['X-Auth-Token']= Authentication.getToken()
    else:
        headers['X-Auth-Token']=  token

    response = requests.get(schemaURL, headers=headers)

    if response.status_code == 200:
        return(response.json())
    else:
        return(response.text)


def getDatasetInfo(datasetName, token = ''):
    """
    Gets information related to a particular dataset.
    """
    schemaURL = Config.SkyQueryUrl + '/Schema.svc/datasets/' +  datasetName

    headers = {'Content-Type': 'application/json','Accept': 'application/json'}
    if (token == ""):
        headers['X-Auth-Token']= Authentication.getToken()
    else:
        headers['X-Auth-Token']=  token

    response = requests.get(schemaURL, headers=headers)

    if response.status_code == 200:
        return(response.json())
    else:
        return(response.text)


def listDatasetTables(datasetName, token = ''):
    """
    Returns a list of all tables within a dataset
    """
    url = Config.SkyQueryUrl + '/Schema.svc/datasets/' + datasetName +'/tables'

    headers = {'Content-Type': 'application/json','Accept': 'application/json'}
    if (token == ""):
        headers['X-Auth-Token']= Authentication.getToken()
    else:
        headers['X-Auth-Token']=  token

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return(response.json())
    else:
        return(response.text)


def getTableInfo(datasetName, tableName, token = ''):
    """
    Returns info about a particular table belonging to a dataset.
    """
    url = Config.SkyQueryUrl + '/Schema.svc/datasets/' + datasetName +'/tables/' + tableName

    headers = {'Content-Type': 'application/json','Accept': 'application/json'}
    if (token == ""):
        headers['X-Auth-Token']= Authentication.getToken()
    else:
        headers['X-Auth-Token']=  token

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return(response.json())
    else:
        return(response.text)


def listTableColumns(datasetName, tableName, token = ''):
    """
    Returns a list of all columns in a table belonging to a particular dataset.
    Output: json string.
    """
    url = Config.SkyQueryUrl + '/Schema.svc/datasets/' + datasetName +'/tables/' + tableName + '/columns'

    headers = {'Content-Type': 'application/json','Accept': 'application/json'}
    if (token == ""):
        headers['X-Auth-Token']= Authentication.getToken()
    else:
        headers['X-Auth-Token']=  token

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return(response.json())
    else:
        return(response.text)


######################################################################################################################
# Data:

def getTable(datasetName, tableName, top = '',  token = ''):
    """
    Returns a dataset table as a pandas DataFrame. 'top' parameters makes the function to return only the first 'top' rows.
    """
    url = Config.SkyQueryUrl + '/Data.svc/' + datasetName +'/' + tableName + '?top=' + top

    headers = {'Content-Type': 'application/json','Accept': 'application/json'}
    if (token == ""):
        headers['X-Auth-Token']= Authentication.getToken()
    else:
        headers['X-Auth-Token']=  token

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return(pandas.read_csv(StringIO(response.content.decode()), sep="\t"))
    else:
        return(response.text)


def dropTable(datasetName, tableName, token = ''):
    """
    Drops (deletes) a table from the user database.
    """
    url = Config.SkyQueryUrl + '/Data.svc/' + datasetName +'/' + tableName

    headers = {'Content-Type': 'application/json','Accept': 'application/json'}
    if (token == ""):
        headers['X-Auth-Token']= Authentication.getToken()
    else:
        headers['X-Auth-Token']=  token

    response = requests.delete(url, headers=headers)

    if response.status_code == 200:
        return(response.json())
    else:
        return(response.text)
