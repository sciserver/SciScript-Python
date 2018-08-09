__author__ = 'mtaghiza'

import sys
import os;
import os.path;
from SciServer import Authentication, Config
import requests
import json
import time;



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


def getDockerComputeDomainsNames(dockerComputeDomains=None):
    """
    Returns the names of the docker compute domains available to the user.

    :param dockerComputeDomains: a list of dockerComputeDomain objects (dictionaries), as returned by Jobs.getDockerComputeDomains(). If not set, then an extra internal call to Jobs.getDockerComputeDomains() is made.
    :return: an array of strings, each being the name of a docker compute domain available to the user.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the RACM API returns an error.
    :example: dockerComputeDomainsNames = Files.getDockerComputeDomainsNames();

    .. seealso:: Files.getDockerComputeDomains
    """
    if dockerComputeDomains is None:
        dockerComputeDomains = getDockerComputeDomains();

    dockerComputeDomainsNames = [];
    for dockerComputeDomain in dockerComputeDomains:
        dockerComputeDomainsNames.append(dockerComputeDomain.get('name'))

    return dockerComputeDomainsNames;


def getDockerComputeDomainFromName(dockerComputeDomainName, dockerComputeDomains = None):
    """
    Returns a DockerComputeDomain object, given its registered name.

    :param dockerComputeDomainName: name of the DockerComputeDomainName, as shown within the results of Jobs.getDockerComputeDomains()
    :param dockerComputeDomains: a list of dockerComputeDomain objects (dictionaries), as returned by Jobs.getDockerComputeDomains(). If not set, then an internal call to Jobs.getDockerComputeDomains() is made.
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


def getRDBComputeDomainsNames(rdbComputeDomains=None):
    """
    Returns the names of the RDB compute domains available to the user.

    :param rdbComputeDomains: a list of rdbComputeDomain objects (dictionaries), as returned by Jobs.getRDBComputeDomains(). If not set, then an extra internal call to Jobs.getRDBComputeDomains() is made.
    :return: an array of strings, each being the name of a rdb compute domain available to the user.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the RACM API returns an error.
    :example: dockerComputeDomainsNames = Files.getDockerComputeDomainsNames();

    .. seealso:: Files.getRDBComputeDomains
    """
    if rdbComputeDomains is None:
        rdbComputeDomains = getRDBComputeDomains();

    rdbComputeDomainsNames = [];
    for rdbComputeDomain in rdbComputeDomains:
        rdbComputeDomainsNames.append(rdbComputeDomain.get('name'))

    return rdbComputeDomainsNames;


def getRDBComputeDomainFromName(rdbComputeDomainName, rdbComputeDomains = None):
    """
    Returns an RDBComputeDomain object, given its registered name.

    :param rdbComputeDomainName: name of the RDBComputeDomainName, as shown within the results of Jobs.getRDBComputeDomains()
    :param rdbComputeDomains: a list of rdbComputeDomain objects (dictionaries), as returned by Jobs.getRDBComputeDomains(). If not set, then an extra internal call to Jobs.getRDBComputeDomains() is made.
    :return: an RDBComputeDomain object (dictionary) that defines an RDB compute domain. A list of these kind of objects available to the user is returned by the function Jobs.getRDBComputeDomains().
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the JOBM API returns an error.
    :example: rdbComputeDomain = Jobs.getRDBComputeDomainFromName('rdbComputeDomainAtJHU');

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
    :example: jobId = Jobs.submitShellCommandJob(Jobs.getDockerComputeDomains()[0],'pwd', 'Python (astro)'); status = Jobs.getJobStatus(jobId);

    .. seealso:: Jobs.submitShellCommandJob, Jobs.getJobStatus, Jobs.getDockerComputeDomains, Jobs.cancelJob
    """
    intStatus = getJobDescription(jobId)["status"]

    if intStatus == 1:
        return {'status':intStatus, 'statusMeaning':"PENDING", 'jobId':jobId}
    elif intStatus == 2:
        return {'status': intStatus, 'statusMeaning': "QUEUED", 'jobId':jobId}
    elif intStatus == 4:
        return {'status': intStatus, 'statusMeaning': "ACCEPTED", 'jobId':jobId}
    elif intStatus == 8:
        return {'status': intStatus, 'statusMeaning': "STARTED", 'jobId':jobId}
    elif intStatus == 16:
        return {'status': intStatus, 'statusMeaning': "FINISHED", 'jobId':jobId}
    elif intStatus == 32:
        return {'status': intStatus, 'statusMeaning': "SUCCESS", 'jobId':jobId}
    elif intStatus == 64:
        return {'status': intStatus, 'statusMeaning': "ERROR", 'jobId':jobId}
    elif intStatus == 128:
        return {'status': intStatus, 'statusMeaning': "CANCELED", 'jobId':jobId}
    else:
        raise Exception("Invalid integer value given to job status.")


def submitNotebookJob(notebookPath, dockerComputeDomain=None, dockerImageName=None, userVolumes=None,  dataVolumes=None, resultsFolderPath="", parameters="", jobAlias= ""):
    """
    Submits a Jupyter Notebook for execution (as an asynchronous job) inside a Docker compute domain.

    :param notebookPath: path of the notebook within the filesystem mounted in SciServer-Compute (string). Example: notebookPath = '/home/idies/worskpace/persistent/JupyterNotebook.ipynb'
    :param dockerComputeDomain: object (dictionary) that defines a Docker compute domain. A list of these kind of objects available to the user is returned by the function Jobs.getDockerComputeDomains().
    :param dockerImageName: name (string) of the Docker image for executing the notebook. E.g.,  dockerImageName="Python (astro)". An array of available Docker images is defined as the 'images' property in the dockerComputeDomain object.
    :param userVolumes: a list with the names of user volumes (with optional write permissions) that will be mounted to the docker Image. E.g.: userVolumes = [{'name':'persistent', 'needsWriteAccess':False},{'name':'scratch', , 'needsWriteAccess':True}] . A list of available user volumes can be found as the 'userVolumes' property in the dockerComputeDomain object. If userVolumes=None, then all available user volumes are mounted, with 'needsWriteAccess' = True if the user has Write permissions on the volume.
    :param dataVolumes: a list with the names of data volumes that will be mounted to the docker Image. E.g.: dataVolumes=[{"name":"SDSS_DAS"}, {"name":"Recount"}]. A list of available data volumes can be found as the 'volumes' property in the dockerComputeDomain object. If dataVolumes=None, then all available data volumes are mounted.
    :param resultsFolderPath: full path to results folder (string) where the original notebook is copied to and executed. E.g.: /home/idies/workspace/rootVolume/username/userVolume/jobsFolder. If not set, then a default folder will be set automatically.
    :param parameters: string containing parameters that the notebook might need during its execution. This string is written in the 'parameters.txt' file in the same directory level where the notebook is being executed.
    :param jobAlias: alias (string) of job, defined by the user.
    :return: the job ID (int)
    :raises: Throws an exception if the HTTP request to the Authentication URL returns an error. Throws an exception if the HTTP request to the JOBM API returns an error, or if the volumes defined by the user are not available in the Docker compute domain.
    :example: dockerComputeDomain = Jobs.getDockerComputeDomains()[0]; job = Jobs.submitNotebookJob('/home/idies/workspace/persistent/Notebook.ipynb', dockerComputeDomain, 'Python (astro)', [{'name':'persistent'},{'name':'scratch', 'needsWriteAccess':True}], [{'name':'SDSS_DAS'}], 'param1=1\\nparam2=2\\nparam3=3','myNewJob')

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
                    if vol.get('name') == uVol.get('name') and vol.get('rootVolumeName') == uVol.get('rootVolumeName') and vol.get('owner') == uVol.get('owner'):
                        found = True;
                        if (uVol.get('needsWriteAccess')):
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
            "resultsFolderURI": resultsFolderPath,
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
            return (json.loads(res.content.decode())).get('id')
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def submitShellCommandJob(shellCommand, dockerComputeDomain = None, dockerImageName = None, userVolumes = None, dataVolumes = None, resultsFolderPath = "", jobAlias = ""):
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
    :param resultsFolderPath: full path to results folder (string) where the shell command is executed. E.g.: /home/idies/workspace/rootVolume/username/userVolume/jobsFolder. If not set, then a default folder will be set automatically.
    :param jobAlias: alias (string) of job, defined by the user.
    :return: the job ID (int)
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
                        if (uVol.get('needsWriteAccess')):
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
            "userVolumes": uVols,
            "resultsFolderURI": resultsFolderPath
        }
        data = json.dumps(dockerJobModel).encode()
        url = Config.RacmApiURL + "/jobm/rest/jobs/docker?TaskName="+taskName;
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.post(url, data=data, headers=headers, stream=True)

        if res.status_code != 200:
            raise Exception("Error when submitting a job to the JOBM API.\nHttp Response from JOBM API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return (json.loads(res.content.decode())).get('id')
    else:
        raise Exception("User token is not defined. First log into SciServer.")

def submitRDBQueryJob(sqlQuery, rdbComputeDomain=None, databaseContextName = None, resultsName='queryResults', resultsFolderPath="", jobAlias = ""):
    """
    Submits a sql query for execution (as an asynchronous job) inside a relational database (RDB) compute domain.

    :param sqlQuery: sql query (string)
    :param rdbComputeDomain: object (dictionary) that defines a relational database (RDB) compute domain. A list of these kind of objects available to the user is returned by the function Jobs.getRDBComputeDomains().
    :param databaseContextName: database context name (string) on which the sql query is executed.
    :param resultsName: name (string) of the table or file (without file type ending) that contains the query result. In case the sql query has multiple statements, should be set to a list of names (e.g., ['result1','result2']).
    :param resultsFolderPath: full path to results folder (string) where query output tables are written into. E.g.: /home/idies/workspace/rootVOlume/username/userVolume/jobsFolder . If not set, then a default folder will be set automatically.
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

        targets = [];
        if type(resultsName) == str:
            targets.append({'location': resultsName, 'type': 'FILE_CSV', 'resultNumber': 1});
        elif type(resultsName) == list:
            if len(set(resultsName)) != len(resultsName):
                raise Exception("Elements of parameter 'resultsName' must be unique");

            for i in range(len(resultsName)):
                if type(resultsName[i]) == str:
                    targets.append({'location': resultsName[i], 'type': 'FILE_CSV', 'resultNumber': i+1});
                else:
                    raise Exception("Elements of array 'resultsName' are not strings");

        else:
            raise Exception("Type of parameter 'resultsName' is not supported");


        rdbDomainId = rdbComputeDomain.get('id');

        dockerJobModel = {
            "inputSql": sqlQuery,
            "submitterDID": jobAlias,
            "databaseContextName": databaseContextName,
            "rdbDomainId": rdbDomainId,
            "targets": targets,
            "resultsFolderURI":resultsFolderPath
        }

        data = json.dumps(dockerJobModel).encode()
        url = Config.RacmApiURL + "/jobm/rest/jobs/rdb?TaskName="+taskName;
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.post(url, data=data, headers=headers, stream=True)

        if res.status_code != 200:
            raise Exception("Error when submitting a job to the JOBM API.\nHttp Response from JOBM API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return (json.loads(res.content.decode())).get('id')
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def cancelJob(jobId):
    """
    Cancels the execution of a job.

    :param jobId: Id of the job (integer)
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
            pass;
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def waitForJob(jobId, verbose=False, pollTime = 5):
    """
    Queries regularly the job status and waits until the job is completed.

    :param jobId: id of job (integer)
    :param verbose: if True, will print "wait" messages on the screen while the job is still running. If False, will suppress the printing of messages on the screen.
    :param pollTime: idle time interval (integer, in seconds) before querying again for the job status. Minimum value allowed is 5 seconds.
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
            print(waitingStr)

        while not complete:
            if verbose:
                #print back,
                print(waitingStr)
            jobDesc = getJobStatus(jobId)
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

