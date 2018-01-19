__author__ = 'mtaghiza'

import sys
import os;
import os.path;
from SciServer import Authentication, Config
import requests
import json
import time;



def getJobDirectory():
    """
    Gets the root directory path where the job is being executed. Since its value depends on the value of jobId, the path will be defined only after the job is submitted.
    """
    computeJobDirectoryFile = Config.ComputeJobDirectoryFile
    if os.path.isfile(computeJobDirectoryFile):
        jobDirectory = None;
        with open(computeJobDirectoryFile, 'r') as f:
            jobDirectory = f.read().rstrip('\n')

        return jobDirectory
    else:
        raise OSError("Cannot find job execution directory")


def getDockerComputeDomains():
    """
    Gets a list of all registered Docker compute domains that the user has access to.
    :return: a list of dictionaries, each one containing the definition of a Docker compute domain.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the JOBM API returns an error.
    :example: dockerComputeDomains = Jobs.getDockerComputeDomains();
    .. seealso:: Jobs.submitShellCommandJob, Jobs.getJobStatus, Jobs.getRDBComputeDomains, Jobs.cancelJob
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        taskName = "";
        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Jobs.getDockerComputeDomains"
        else:
            taskName = "SciScript-Python.Jobs.getDockerComputeDomains"

        url = Config.RacmApiURL + "/jobm/rest/computedomains?batch=true&interactive=false&TaskName=" + taskName
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.get(url, headers=headers, stream=True)

        if res.status_code != 200:
            raise Exception("Error when getting Docker Compute Domains from JOBM API.\nHttp Response from JOBM API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return json.loads(res.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def getRDBComputeDomains():
    """
    Gets a list of all registered Relational Database (RDB) compute domains that the user has access to.
    :return: a list of dictionaries, each one containing the definition of an RDB compute domain.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the JOBM API returns an error.
    :example: rdbComputeDomains = Jobs.getRDBComputeDomains();
    .. seealso:: Jobs.submitShellCommandJob, Jobs.getJobStatus, Jobs.getDockerComputeDomains, Jobs.cancelJob
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Jobs.getRDBComputeDomains"
        else:
            taskName = "SciScript-Python.Jobs.getRDBComputeDomains"

        url = Config.RacmApiURL + "/jobm/rest/computedomains/rdb?TaskName=" + taskName
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.get(url, headers=headers, stream=True)
        if res.status_code != 200:
            raise Exception("Error when getting RDB Compute Domains from JOBM API.\nHttp Response from JOBM API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return json.loads(res.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def getDockerComputeDomainFromName(dockerComputeDomainName, dockerComputeDomains = None):
    """
    Returns a DockerComputeDomain object, given its registered name.
    :param dockerComputeDomainName: name of the DockerComputeDomainName, as shown within the results of Jobs.getDockerComputeDomains()
    :param dockerComputeDomains: a list of DockerComputeDomain objects (dictionaries), as returned by Jobs.getDockerComputeDomains(). If not set, then an internal call to Jobs.getDockerComputeDomains() is made.
    :return: a DockerComputeDomain object (dictionary) that defines a Docker compute domain. A list of these kind of objects available to the user is returned by the function Jobs.getDockerComputeDomains().
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the JOBM API returns an error.
    :example: dockerComputeDomain = Jobs.getDockerComputeDomainFromName('dockerComputeDomainAtJHU');
    .. seealso:: Jobs.getDockerComputeDomains, Jobs.getRDBComputeDomains, Jobs.getRDBComputeDomainFromName
    """
    if dockerComputeDomainName is None:
        raise Exception("dockerComputeDomainName is not defined.")
    else:
        if dockerComputeDomains is None:
            dockerComputeDomains = getDockerComputeDomains();

        if dockerComputeDomains.__len__() > 0:
            for dockerComputeDomain in dockerComputeDomains:
                if dockerComputeDomainName == dockerComputeDomain.get('name'):
                    return dockerComputeDomain;
        else:
            raise Exception("There are no DockerComputeDomains available for the user.");

        raise Exception("DockerComputeDomain of name '" + dockerComputeDomainName + "' is not available or does not exist.");


def getRDBComputeDomainFromName(rdbComputeDomainName, rdbComputeDomains = None):
    """
    Returns an RDBComputeDomain object, given its registered name.
    :param rdbComputeDomainName: name of the RDBComputeDomainName, as shown within the results of Jobs.getRDBComputeDomains()
    :param rdbComputeDomains: a list of rdbComputeDomain objects (dictionaries), as returned by Jobs.getRDBComputeDomains(). If not set, then an extra internal call to Jobs.getRDBComputeDomains() is made.
    :return: an RDBComputeDomain object (dictionary) that defines an RDB compute domain. A list of these kind of objects available to the user is returned by the function Jobs.getRDBComputeDomains().
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the JOBM API returns an error.
    :example: rdvComputeDomain = Jobs.getRDBComputeDomainFromName('rdbComputeDomainAtJHU');
    .. seealso:: Jobs.getDockerComputeDomains, Jobs.getRDBComputeDomains, Jobs.getDockerComputeDomainFromName
    """
    if rdbComputeDomainName is None:
        raise Exception("rdbComputeDomainName is not defined.")
    else:
        if rdbComputeDomains is None:
            rdbComputeDomains = getRDBComputeDomains();

        if rdbComputeDomains.__len__() > 0:
            for rdbComputeDomain in rdbComputeDomains:
                if rdbComputeDomainName == rdbComputeDomain.get('name'):
                    return rdbComputeDomain;
        else:
            raise Exception("There are no RDBComputeDomains available for the user.");

        raise Exception("RDBComputeDomain of name '" + rdbComputeDomainName + "' is not available or does not exist.");


def getJobsList(top=10, open=None, start=None, end=None, type='all'):
    """
    Gets the list of Jobs submitted by the user.
    :param top: top number of jobs (integer) returned. If top=None, then all jobs are returned.
    :param open: If set to 'True', then only returns jobs that have not finished executing and wrapped up  (status <= FINISHED). If set to 'False' then only returnes jobs that are still running. If set to 'None', then returns both finished and unfinished jobs.
    :param start: The earliest date (inclusive) to search for jobs, in string format yyyy-MM-dd hh:mm:ss.SSS. If set to 'None', then there is no lower bound on date.
    :param end: The latest date (inclusive) to search for jobs, in string format yyyy-MM-dd hh:mm:ss.SSS. If set to 'None', then there is no upper bound on date.
    :param type: type (string) of jobs returned. Can take values of 'rdb' (for returning only relational database jobs), 'docker' (for returning only Docker jobs) and 'all' (all job types are returned).
    :return: a list of dictionaries, each one containing the definition of a submitted job.
    :raises: Throws an exception if the HTTP request to the Authentication URL returns an error, and if the HTTP request to the JOBM API returns an error.
    :example: jobs = Jobs.getJobsList(top=2);
    .. seealso:: Jobs.submitNotebookJob, Jobs.submitShellCommandJob, Jobs.getJobStatus, Jobs.getDockerComputeDomains, Jobs.cancelJob
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Jobs.getJobsList"
        else:
            taskName = "SciScript-Python.Jobs.getJobsList"


        topString = ("top=" + str(top) + "&" if top != None else "")
        startString = ("start=" + str(start) + "&" if start != None else "")
        endString = ("end=" + str(end) + "&" if end != None else "")
        if open is None:
            openString = "";
        else:
            if open is True:
                openString = "open=true&";
            else:
                openString = "open=false&";

        url = Config.RacmApiURL + "/jobm/rest/jobs?"
        if(type=='rdb'):
            url = Config.RacmApiURL + "/jobm/rest/rdbjobs?"
        if(type=='docker'):
            url = Config.RacmApiURL + "/jobm/rest/dockerjobs?"

        url = url + topString + startString + endString + "TaskName=" + taskName;

        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.get(url, headers=headers, stream=True)

        if res.status_code != 200:
            raise Exception("Error when getting list of jobs from JOBM API.\nHttp Response from JOBM API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return json.loads(res.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")

def getJobDescription(jobId):
    """
    Gets the definition of the job,
    :param jobId: Id of job
    :return: dictionary containing the description or definition of the job.
    :raises: Throws an exception if the HTTP request to the Authentication URL returns an error, and if the HTTP request to the JOBM API returns an error.
    :example: job1 = Jobs.submitShellCommandJob(Jobs.getDockerComputeDomains()[0],'pwd', 'Python (astro)'); job2 = Jobs.getJobDescription(job1.get('id'));
    .. seealso:: Jobs.submitShellCommandJob, Jobs.getJobStatus, Jobs.getDockerComputeDomains, Jobs.cancelJob
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Jobs.getJobDescription"
        else:
            taskName = "SciScript-Python.Jobs.getJobDescription"

        url = Config.RacmApiURL + "/jobm/rest/jobs/" + str(jobId) + "?TaskName="+taskName
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.get(url, headers=headers, stream=True)

        if res.status_code != 200:
            raise Exception("Error when getting from JOBM API the job status of jobId=" + str(jobId) + ".\nHttp Response from JOBM API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return json.loads(res.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def getJobStatus(jobId):
    """
    Gets a dictionary with the job status as an integer value, together with its semantic meaning. The integer value is a power of 2, that is, 1:PENDING, 2:QUEUED, 4:ACCEPTED, 8:STARTED, 16:FINISHED, 32:SUCCESS, 64:ERROR and 128:CANCELED
    :param jobId: Id of job (integer).
    :return: dictionary with the integer value of the job status, as well as its semantic meaning.
    :raises: Throws an exception if the HTTP request to the Authentication URL returns an error, and if the HTTP request to the JOBM API returns an error.
    :example: job = Jobs.submitShellCommandJob(Jobs.getDockerComputeDomains()[0],'pwd', 'Python (astro)'); status = Jobs.getJobStatus(job.get('id'));
    .. seealso:: Jobs.submitShellCommandJob, Jobs.getJobStatus, Jobs.getDockerComputeDomains, Jobs.cancelJob
    """
    intStatus = getJobDescription(jobId)["status"]

    if intStatus == 1:
        return {'status':"PENDING", 'intStatus':intStatus}
    elif intStatus == 2:
        return {'status': "QUEUED", 'intStatus': intStatus}
    elif intStatus == 4:
        return {'status': "ACCEPTED", 'intStatus': intStatus}
    elif intStatus == 8:
        return {'status': "STARTED", 'intStatus': intStatus}
    elif intStatus == 16:
        return {'status': "FINISHED", 'intStatus': intStatus}
    elif intStatus == 32:
        return {'status': "SUCCESS", 'intStatus': intStatus}
    elif intStatus == 64:
        return {'status': "ERROR", 'intStatus': intStatus}
    elif intStatus == 128:
        return {'status': "CANCELED", 'intStatus': intStatus}
    else:
        raise Exception("Invalid integer value given to job status.")


def submitNotebookJob(notebookPath, dockerComputeDomain=None, dockerImageName=None, userVolumes=None,  dataVolumes=None, parameters="", jobAlias= ""):
    """
    Submits a Jupyter Notebook for execution (as an asynchronous job) inside a Docker compute domain,
    :param notebookPath: path of the notebook within the filesystem mounted in SciServer-Compute (string). Example: notebookPath = '/home/idies/worskpace/persistent/JupyterNotebook.ipynb'
    :param dockerComputeDomain: object (dictionary) that defines a Docker compute domain. A list of these kind of objects available to the user is returned by the function Jobs.getDockerComputeDomains().
    :param dockerImageName: name (string) of the Docker image for executing the notebook. E.g.,  dockerImageName="Python (astro)". An array of available Docker images is defined as the 'images' property in the dockerComputeDomain object.
    :param userVolumes: a list with the names of user volumes (with optional write permissions) that will be mounted to the docker Image.
           E.g., userVolumes = [{'name':'persistent', 'needsWriteAccess':False},{'name':'scratch', , 'needsWriteAccess':True}]
           A list of available user volumes can be found as the 'userVolumes' property in the dockerComputeDomain object. If userVolumes=None, then all available user volumes are mounted, with 'needsWriteAccess' = True if the user has Write permissions on the volume.
    :param dataVolumes: a list with the names of data volumes that will be mounted to the docker Image.
           E.g., dataVolumes=[{"name":"SDSS_DAS"}, {"name":"Recount"}].
           A list of available data volumes can be found as the 'volumes' property in the dockerComputeDomain object. If dataVolumes=None, then all available data volumes are mounted.
    :param parameters: string containing parameters that the notebook might need during its execution. This string is written in the 'parameters.txt' file in the same directory level where the notebook is being executed.
    :param jobAlias: alias (string) of job, defined by the user.
    :return: a dictionary containing the definition of the submitted job.
    :raises: Throws an exception if the HTTP request to the Authentication URL returns an error. Throws an exception if the HTTP request to the JOBM API returns an error, or if the volumes defined by the user are not available in the Docker compute domain.
    :example: dockerComputeDomain = Jobs.getDockerComputeDomains()[0]; job = Jobs.submitNotebookJob('/home/idies/workspace/persistent/Notebook.ipynb', dockerComputeDomain, 'Python (astro)', [{'name':'persistent'},{'name':'scratch', 'needsWriteAccess':True}], [{'name':'SDSS_DAS'}], 'param1=1\nparam2=2\nparam3=3','myNewJob')
    .. seealso:: Jobs.submitShellCommandJob, Jobs.getJobStatus, Jobs.getDockerComputeDomains, Jobs.cancelJob
    """

    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Jobs.submitNotebookJob"
        else:
            taskName = "SciScript-Python.Jobs.submitNotebookJob"

        if dockerComputeDomain is None:
            dockerComputeDomains = getDockerComputeDomains();
            if dockerComputeDomains .__len__() > 0:
                dockerComputeDomain = dockerComputeDomains[0];
            else:
                raise Exception("There are no dockerComputeDomains available for the user.");

        if dockerImageName is None:
            images = dockerComputeDomain.get('images');
            if images.__len__() > 0:
                dockerImageName = images[0].get('name')
            else:
                raise Exception("dockerComputeDomain has no docker images available for the user.");

        uVols = [];
        if userVolumes is None:
            for vol in dockerComputeDomain.get('userVolumes'):
                if 'write' in vol.get('allowedActions'):
                    uVols.append({'userVolumeId': vol.get('id'), 'needsWriteAccess': True});
                else:
                    uVols.append({'userVolumeId': vol.get('id'), 'needsWriteAccess': False});


        else:
            for uVol in userVolumes:
                found = False;
                for vol in dockerComputeDomain.get('userVolumes'):
                    if vol.get('name') == uVol.get('name'):
                        found = True;
                        if (uVol.has_key('needsWriteAccess')):
                            if uVol.get('needsWriteAccess') == True and 'write' in vol.get('allowedActions'):
                                uVols.append({'userVolumeId': vol.get('id'), 'needsWriteAccess': True});
                            else:
                                uVols.append({'userVolumeId': vol.get('id'), 'needsWriteAccess': False});
                        else:
                            if 'write' in vol.get('allowedActions'):
                                uVols.append({'userVolumeId': vol.get('id'), 'needsWriteAccess': True});
                            else:
                                uVols.append({'userVolumeId': vol.get('id'), 'needsWriteAccess': False});

                if not found:
                    raise Exception("User volume '" + uVol.get('name') + "' not found within Compute domain")

        datVols = [];
        if dataVolumes is None:
            for vol in dockerComputeDomain.get('volumes'):
                datVols.append({'name': vol.get('name')});

        else:
            for dVol in dataVolumes:
                found = False;
                for vol in dockerComputeDomain.get('volumes'):
                    if vol.get('name') == dVol.get('name'):
                        found = True;
                        datVols.append({'name': vol.get('name')});

                if not found:
                    raise Exception("Data volume '" + dVol.get('name') + "' not found within Compute domain")


        dockerComputeEndpoint = dockerComputeDomain.get('apiEndpoint');

        dockerJobModel = {
            "command": parameters,
            "scriptURI": notebookPath,
            "submitterDID": jobAlias,
            "dockerComputeEndpoint": dockerComputeEndpoint,
            "dockerImageName": dockerImageName,
            "volumeContainers": datVols,
            "userVolumes": uVols
        }
        data = json.dumps(dockerJobModel).encode()
        url = Config.RacmApiURL + "/jobm/rest/jobs/docker?TaskName="+taskName;
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.post(url, data=data, headers=headers, stream=True)

        if res.status_code != 200:
            raise Exception("Error when submitting a notebook job to the JOBM API.\nHttp Response from JOBM API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return json.loads(res.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def submitShellCommandJob(shellCommand, dockerComputeDomain = None, dockerImageName = None, userVolumes = None,  dataVolumes = None, jobAlias = ""):
    """
    Submits a shell command for execution (as an asynchronous job) inside a Docker compute domain.
    :param shellCommand: shell command (string) defined by the user.
    :param dockerComputeDomain: object (dictionary) that defines a Docker compute domain. A list of these kind of objects available to the user is returned by the function Jobs.getDockerComputeDomains().
    :param dockerImageName: name (string) of the Docker image for executing the notebook. E.g.,  dockerImageName="Python (astro)". An array of available Docker images is defined as the 'images' property in the dockerComputeDomain object.
    :param userVolumes: a list with the names of user volumes (with optional write permissions) that will be mounted to the docker Image.
           E.g., userVolumes = [{'name':'persistent', 'needsWriteAccess':False},{'name':'scratch', , 'needsWriteAccess':True}]
           A list of available user volumes can be found as the 'userVolumes' property in the dockerComputeDomain object. If userVolumes=None, then all available user volumes are mounted, with 'needsWriteAccess' = True if the user has Write permissions on the volume.
    :param dataVolumes: a list with the names of data volumes that will be mounted to the docker Image.
           E.g., dataVolumes=[{"name":"SDSS_DAS"}, {"name":"Recount"}].
           A list of available data volumes can be found as the 'volumes' property in the dockerComputeDomain object. If dataVolumes=None, then all available data volumes are mounted.
    :param jobAlias: alias (string) of job, defined by the user.
    :return: a dictionary containing the definition of the submitted job.
    :raises: Throws an exception if the HTTP request to the Authentication URL returns an error. Throws an exception if the HTTP request to the JOBM API returns an error, or if the volumes defined by the user are not available in the Docker compute domain.
    :example: dockerComputeDomain = Jobs.getDockerComputeDomains()[0]; job = Jobs.submitShellCommandJob('pwd', dockerComputeDomain, 'Python (astro)', [{'name':'persistent'},{'name':'scratch', 'needsWriteAccess':True}], [{'name':'SDSS_DAS'}], 'myNewJob')
    .. seealso:: Jobs.submitNotebookJob, Jobs.getJobStatus, Jobs.getDockerComputeDomains, Jobs.cancelJob
    """

    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Jobs.submitShellCommandJob"
        else:
            taskName = "SciScript-Python.Jobs.submitShellCommandJob"


        if dockerComputeDomain is None:
            dockerComputeDomains = getDockerComputeDomains();
            if dockerComputeDomains .__len__() > 0:
                dockerComputeDomain = dockerComputeDomains[0];
            else:
                raise Exception("There are no dockerComputeDomains available for the user.");

        if dockerImageName is None:
            images = dockerComputeDomain.get('images');
            if images.__len__() > 0:
                dockerImageName = images[0].get('name')
            else:
                raise Exception("dockerComputeDomain has no docker images available for the user.");

        uVols = [];
        if userVolumes is None:
            for vol in dockerComputeDomain.get('userVolumes'):
                if 'write' in vol.get('allowedActions'):
                    uVols.append({'userVolumeId': vol.get('id'), 'needsWriteAccess': True});
                else:
                    uVols.append({'userVolumeId': vol.get('id'), 'needsWriteAccess': False});


        else:
            for uVol in userVolumes:
                found = False;
                for vol in dockerComputeDomain.get('userVolumes'):
                    if vol.get('name') == uVol.get('name'):
                        found = True;
                        if (uVol.has_key('needsWriteAccess')):
                            if uVol.get('needsWriteAccess') == True and 'write' in vol.get('allowedActions'):
                                uVols.append({'userVolumeId': vol.get('id'), 'needsWriteAccess': True});
                            else:
                                uVols.append({'userVolumeId': vol.get('id'), 'needsWriteAccess': False});
                        else:
                            if 'write' in vol.get('allowedActions'):
                                uVols.append({'userVolumeId': vol.get('id'), 'needsWriteAccess': True});
                            else:
                                uVols.append({'userVolumeId': vol.get('id'), 'needsWriteAccess': False});

                if not found:
                    raise Exception("User volume '" + uVol.get('name') + "' not found within Compute domain")

        datVols = [];
        if dataVolumes is None:
            for vol in dockerComputeDomain.get('volumes'):
                datVols.append({'name': vol.get('name')});

        else:
            for dVol in dataVolumes:
                found = False;
                for vol in dockerComputeDomain.get('volumes'):
                    if vol.get('name') == dVol.get('name'):
                        found = True;
                        datVols.append({'name': vol.get('name')});

                if not found:
                    raise Exception("Data volume '" + dVol.get('name') + "' not found within Compute domain")


        dockerComputeEndpoint = dockerComputeDomain.get('apiEndpoint');

        dockerJobModel = {
            "command": shellCommand,
            "submitterDID": jobAlias,
            "dockerComputeEndpoint": dockerComputeEndpoint,
            "dockerImageName": dockerImageName,
            "volumeContainers": datVols,
            "userVolumes": uVols
        }
        data = json.dumps(dockerJobModel).encode()
        url = Config.RacmApiURL + "/jobm/rest/jobs/docker?TaskName="+taskName;
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.post(url, data=data, headers=headers, stream=True)

        if res.status_code != 200:
            raise Exception("Error when submitting a job to the JOBM API.\nHttp Response from JOBM API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return json.loads(res.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")

def submitRDBQueryJob(sqlQuery, rdbComputeDomain=None, databaseContextName = None, resultsName=None, jobAlias = ""):
    """
    Submits a sql query for execution (as an asynchronous job) inside a relational database (RDB) compute domain.
    :param sqlQuery: sql query (string)
    :param rdbComputeDomain: object (dictionary) that defines a relational database (RDB) compute domain. A list of these kind of objects available to the user is returned by the function Jobs.getRDBComputeDomains().
    :param databaseContextName: database context name (string) on which the sql query is executed.
    :param resultsName: name (string) of the table or file containing the query result.
    :param jobAlias: alias (string) of job, defined by the user.
    :return: a dictionary containing the definition of the submitted job.
    :raises: Throws an exception if the HTTP request to the Authentication URL returns an error. Throws an exception if the HTTP request to the JOBM API returns an error, or if the volumes defined by the user are not available in the Docker compute domain.
    :example: job = Jobs.submitRDBQueryJob('select 1';,None, None, 'myQueryResults', 'myNewJob')
    .. seealso:: Jobs.submitNotebookJob, Jobs.submitShellCommandJob, Jobs.getJobStatus, Jobs.getDockerComputeDomains, Jobs.cancelJob
    """

    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Jobs.submitRDBQueryJob"
        else:
            taskName = "SciScript-Python.Jobs.submitRDBQueryJob"

        if rdbComputeDomain is None:
            rdbComputeDomains = getRDBComputeDomains();
            if rdbComputeDomains .__len__() > 0:
                rdbComputeDomain = rdbComputeDomains[0];
            else:
                raise Exception("There are no rdbComputeDomains available for the user.");

        if databaseContextName is None:
            databaseContexts = rdbComputeDomain.get('databaseContexts');
            if databaseContexts.__len__() > 0:
                databaseContextName = databaseContexts[0].get('name')
            else:
                raise Exception("rbdComputeDomain has no database contexts available for the user.");

        if resultsName is None:
            resultsName = 'results'

        targets = [{'location':resultsName+'.csv', 'type':'FILE_CSV', 'resultNumber':1}]
        rdbDomainId = rdbComputeDomain.get('id');

        dockerJobModel = {
            "inputSql": sqlQuery,
            "submitterDID": jobAlias,
            "databaseContextName": databaseContextName,
            "rdbDomainId": rdbDomainId,
            "targets": targets
        }

        data = json.dumps(dockerJobModel).encode()
        url = Config.RacmApiURL + "/jobm/rest/jobs/rdb?TaskName="+taskName;
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.post(url, data=data, headers=headers, stream=True)

        if res.status_code != 200:
            raise Exception("Error when submitting a job to the JOBM API.\nHttp Response from JOBM API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return json.loads(res.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def cancelJob(jobId):
    """
    Cancels the execution of a job.
    :param jobId: Id of the job (integer)
    :return: True if the job is correctly canceled.
    :raises: Throws an exception if the HTTP request to the Authentication URL returns an error. Throws an exception if the HTTP request to the JOBM API returns an error.
    :example: job = Jobs.submitShellCommandJob(Jobs.getDockerComputeDomains()[0],'pwd', 'Python (astro)'); isCanceled = Jobs.cancelJob(job.get('id'));
    .. seealso:: Jobs.submitNotebookJob, Jobs.getJobStatus, Jobs.getDockerComputeDomains.
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Jobs.cancelJob"
        else:
            taskName = "SciScript-Python.Jobs.cancelJob"


        url = Config.RacmApiURL + "/jobm/rest/jobs/" + str(jobId) + "/cancel?TaskName="+taskName
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.post(url, headers=headers, stream=True)

        if res.status_code != 200:
            raise Exception("Error when getting from JOBM API the job status of jobId=" + str(jobId) + ".\nHttp Response from JOBM API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return True;
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def waitForJob(jobId, verbose=True, pollTime = 5):
    """
    Queries regularly the job status and waits for the job to be completed.

    :param jobId: id of job (integer)
    :param verbose: if True, will print "wait" messages on the screen while the job is not done. If False, will suppress printing messages on the screen.
    :param pollTime: idle time interval (integer, in seconds) before a new query for the job status. Minimum value allowed is 5 seconds.
    :return: After the job is finished, returns a dictionary object containing the job definition.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the JOBM API returns an error.
    :example:  dockerComputeDomain = Jobs.getDockerComputeDomains()[0]; job = Jobs.submitShellCommandJob(dockerComputeDomain,'pwd', 'Python (astro)');Jobs.waitForJob(job.get('id'))
    .. seealso:: Jobs.getJobStatus, Jobs.getDockerComputeDomains, Jobs.submitNotebookJob, Jobs.submitShellCommandJob
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
                #print back,
                print(waitingStr, end="")
            jobDesc = getJobDescription(jobId)
            jobStatus = jobDesc["status"]
            if jobStatus >= 32:
                complete = True
                if verbose:
                    #print back,
                    print("Done!")
            else:
                time.sleep(max(minPollTime,pollTime));

        return jobDesc
    except Exception as e:
        raise e;
