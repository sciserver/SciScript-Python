__author__ = 'mtaghiza'

import sys
import os;
import os.path;
from SciServer import Authentication, Config
import requests
import json
import time;



def getJobDirectory(jobId = None):
    """
    Gets the root directory path where the job is being executed. Since its value depends on the value of jobID, the path will be defined only after the job is submitted.
    """
    computeJobDirectoryFile = Config.ComputeJobDirectoryFile
    if os.path.isfile(computeJobDirectoryFile):
        jobDirectory = None;
        with open(computeJobDirectoryFile, 'r') as f:
            jobDirectory = f.read().rstrip('\n')

        return jobDirectory
    else:
        raise OSError("Cannot find jobs directory for jobId=" + jobId + ".")


def getComputeDomains():
    """
    Gets a list of all registered compute domains that the user has access to.
    """
    token = Authentication.getToken()
    if token is not None and token != "":
        url = Config.JobmApiURL + "/computedomains"
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.get(url, headers=headers, stream=True)

        if res.status_code != 200:
            raise Exception("Error when getting Compute Domains from JOBM API.\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return json.loads(res.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def getJobTypes():
    return ""


def getJobsList():
    """
    Gets the list of Jobs submitted by the user.
    :return:
    """
    token = Authentication.getToken()
    if token is not None and token != "":
        url = Config.JobmApiURL + "/jobs"
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.get(url, headers=headers, stream=True)

        if res.status_code != 200:
            raise Exception("Error when getting Compute Domains from JOBM API.\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return json.loads(res.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")

def getJobDescription(jobId):
    """
    Gets the definition of the job, including the current status.
    :param jobId: Job ID
    :return:
    """
    token = Authentication.getToken()
    if token is not None and token != "":
        url = Config.JobmApiURL + "/jobs/" + str(jobId)
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.get(url, headers=headers, stream=True)

        if res.status_code != 200:
            raise Exception("Error when getting from JOBM API the job status of jobId=" + str(jobId) + ".\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return json.loads(res.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def getJobStatus(jobId):
    """
    Gets the semantic meaning of the integer status value included in the job description. The integer value is a power of 2, that is, 1:PENDING, 2:QUEUED, 4:ACCEPTED, 8:STARTED, 16:FINISHED, 32:SUCCESS, 64:ERROR and 128:CANCELED
    :param jobId: Id of job.
    :return: string with job status.
    """
    intStatus = getJobDescription(jobId)["status"]

    if intStatus == 1:
        return "PENDING"
    elif intStatus == 2:
        return "QUEUED"
    elif intStatus == 4:
        return "ACCEPTED"
    elif intStatus == 8:
        return "STARTED"
    elif intStatus == 16:
        return "FINISHED"
    elif intStatus == 32:
        return "SUCCESS"
    elif intStatus == 64:
        return "ERROR"
    elif intStatus == 128:
        return "CANCELED"
    else:
        raise Exception("Invalid integer value given to job status.")


def submitNotebookJob(computeDomainName, notebookPath, dockerImage=[], volumes=[], parameters="", jobAlias = ""):
    """
    Submits a Jupyter Notebook for execution as an asynchronous job,
    :param computeDomainName: Name of compute domain (string). E.g: http://compute.sciserver.org/dashboard/api/container/
    :param notebookPath: path of the notebook within the filesystem mounted in SciServer-Compute (string).
    :param dockerImage: name of Docker image where the job is being submitted. E.g.,  dockerImage=["Python + Dev (astro)"]
    :param volumes: volume conatiners mounted in the docker image for the job execution. E.g., volumes=[{"name":"SDSS_DAS"}, {"name":"Recount"}]
    :param parameters: string containing parameters.
    :param jobAlias: alias of job
    :return:
    """

    token = Authentication.getToken()
    if token is not None and token != "":

        dockerJobModel = {
            "command": parameters,
            "scriptURI": notebookPath,
            "submitterDID": jobAlias,
            "dockerComputeEndpoint": computeDomainName,
            "dockerImageName": dockerImage,
            "volumeContainers": volumes
        }
        data = json.dumps(dockerJobModel).encode()

        url = Config.JobmApiURL + "/jobs/docker"
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.post(url, data=data, headers=headers, stream=True)

        if res.status_code != 200:
            raise Exception("Error when submitting a job to the JOBM API.\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return json.loads(res.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def submitShellCommandJob(computeDomainName, shellCommand, dockerImage=[], volumes=[], jobAlias = ""):
    """
    Submits a shell command for execution as an asynchronous job,
    :param computeDomainName: Name of compute domain (string). E.g: http://compute.sciserver.org/dashboard/api/container/
    :param shellCommand: path of the notebook within the filesystem mounted in SciServer-Compute
    :param dockerImage: name of Docker image where the job is being submitted. E.g.,  dockerImage=["Python + Dev (astro)"]
    :param volumes: volume conatiners mounted in the docker image for the job execution. E.g., volumes=[{"name":"SDSS_DAS"}, {"name":"Recount"}]
    :param parameters: string containing parameters.
    :param jobAlias: alias of job
    :return:
    """

    token = Authentication.getToken()
    if token is not None and token != "":

        dockerJobModel = {
            "command": shellCommand,
            "submitterDID": jobAlias,
            "dockerComputeEndpoint": computeDomainName,
            "dockerImageName": dockerImage,
            "volumeContainers": volumes
        }
        data = json.dumps(dockerJobModel).encode()

        url = Config.JobmApiURL + "/jobs/docker"
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.post(url, data=data, headers=headers, stream=True)

        if res.status_code != 200:
            raise Exception("Error when submitting a job to the JOBM API.\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return json.loads(res.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")



def cancelJob(jobId):

    token = Authentication.getToken()
    if token is not None and token != "":
        url = Config.JobmApiURL + "/jobs/" + jobId + "/cancel"
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.post(url, headers=headers, stream=True)

        if res.status_code != 200:
            raise Exception("Error when getting from JOBM API the job status of jobId=" + str(jobId) + ".\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return True;
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def waitForJob(jobId, verbose=True, pollTime = 5):
    try:
        complete = False

        waitingStr = "Waiting..."
        back = "\b" * len(waitingStr)
        if verbose:
            print waitingStr,

        while not complete:
            if verbose:
                #print back,
                print waitingStr,
            jobDesc = getJobStatus(jobId)
            jobStatus = int(jobDesc["status"])
            if jobStatus >= 32:
                complete = True
                if verbose:
                    #print back,
                    print "Done!"
            else:
                time.sleep(max(pollTime,5));

        return jobDesc
    except Exception as e:
        raise e;



# for out internal testing only:
