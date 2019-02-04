__author__ = 'mtaghiza'

from SciServer import Authentication, Config
import requests
import json
from io import StringIO
from io import BytesIO
import warnings
import os


def getFileServices(verbose=True):
    """
    Gets the definitions of file services that a user is able to access. A FileService represents a file system that contains root volumes accessible to the user for public/private data storage. Within each rootVolume, users can create sharable userVolumes for storing files.

    :param verbose: boolean parameter defining whether warnings will be printed (set to True) or not (set to False).
    :return: list of dictionaries, where each dictionary represents the description of a FileService that the user is able to access.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the RACM API returns an error.
    :example: fileServices = Files.getFileServices();

    .. seealso:: Files.getFileServiceFromName
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.getFileServices"
        else:
            taskName = "SciScript-Python.Files.getFileServices"

        url = Config.RacmApiURL + "/storem/fileservices?TaskName="+taskName;

        headers = {'X-Auth-Token': token}
        res = requests.get(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            fileServices = [];
            fileServicesAPIs = json.loads(res.content.decode())
            for fileServicesAPI in fileServicesAPIs:
                url = fileServicesAPI.get("apiEndpoint")
                name = fileServicesAPI.get("name")
                url = url + "api/volumes/?TaskName="+taskName;
                try:
                    res = requests.get(url, headers=headers)
                except:
                    if verbose:
                        warnings.warn("Error when getting definition of FileService named '" + name + "' with API URL '" + fileServicesAPI.get("apiEndpoint") + "'. This FileService might be not available", Warning, stacklevel=2)

                if res.status_code >= 200 and res.status_code < 300:
                    fileServices.append(json.loads(res.content.decode()));
                else:
                    if verbose:
                        warnings.warn("Error when getting definition of FileService named '" + name + "'.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode(),Warning, stacklevel=2)
            return fileServices;
        else:
            raise Exception("Error when getting the list of FileServices.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")



def getFileServicesNames(fileServices=None, verbose=True):
    """
    Returns the names and description of the fileServices available to the user.

    :param fileServices: a list of FileService objects (dictionaries), as returned by Files.getFileServices(). If not set, then an extra internal call to Jobs.getFileServices() is made.
    :param verbose: boolean parameter defining whether warnings will be printed (set to True) or not (set to False).
    :return: an array of dicts, where each dict has the name and description of a file service available to the user.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the RACM API returns an error.
    :example: fileServiceNames = Files.getFileServicesNames();

    .. seealso:: Files.getFileServices
    """

    if fileServices is None:
        fileServices = getFileServices(verbose);

    fileServiceNames = [];
    for fileService in fileServices:
        fileServiceNames.append({"name":fileService.get('name'),"description":fileService.get('description')})

    return fileServiceNames





def getFileServiceFromName(fileServiceName, fileServices=None, verbose=True):
    """
    Returns a FileService object, given its registered name.

    :param fileServiceName: name of the FileService, as shown within the results of Files.getFileServices()
    :param fileServices: a list of FileService objects (dictionaries), as returned by Files.getFileServices(). If not set, then an extra internal call to Jobs.getFileServices() is made.
    :param verbose: boolean parameter defining whether warnings will be printed (set to True) or not (set to False).
    :return: a FileService object (dictionary) that defines a FileService. A list of these kind of objects available to the user is returned by the function Jobs.getFileServices(). If no fileService can be found, then returns None.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the RACM API returns an error.
    :example: fileService = Files.getFileServiceFromName('FileServiceAtJHU');

    .. seealso:: Files.getFileServices
    """

    if fileServiceName is None:
        raise Exception("fileServiceName is not defined.")
    else:
        if fileServices is None:
            fileServices = getFileServices(verbose);

        if fileServices.__len__() > 0:
            for fileService in fileServices:
                if fileServiceName == fileService.get('name'):
                    return fileService;

            if verbose:
                warnings.warn("FileService of name '" + fileServiceName + "' is not available to the user or does not exist.", Warning, stacklevel=2)
            return None
        else:
            if verbose:
                warnings.warn("There are no FileServices available for the user.", Warning, stacklevel=2)
            return None




def __getFileServiceAPIUrl(fileService):
    """
    Gets the API endpoint URL of a FileService.

    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :return: API endpoint URL of the FileService (string).
    :example: fileServiceAPIUrl = Files.__getFileServiceAPIUrl();

    .. seealso:: Files.getFileServiceFromName
    """

    url = None;
    if type(fileService) == type(""):
        fileServices = getFileServices(False);
        _fileService = getFileServiceFromName(fileService, fileServices, verbose=False);
        url = _fileService.get("apiEndpoint");

    else:
        url = fileService.get("apiEndpoint");

    if not url.endswith("/"):
        url = url + "/"

    return url


def getRootVolumesInfo(fileService, verbose=True):
    """
    Gets the names and descriptions of root volumes available to the user in a particular FileService.

    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param verbose: boolean parameter defining whether warnings will be printed (set to True) or not (set to False).
    :return: list of dictionaries, where each dictionary contains the name and description of a root volume.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the RACM API returns an error.
    :example: fileServices = Files.getFileServices(); rootVolumesInfo = getRootVolumesInfo(fileServices[0])

    .. seealso:: Files.getUserVolumesInfo
    """
    if type(fileService) == str:
        fileService = getFileServiceFromName(fileService, verbose=verbose)

    rootVolumes = []
    for rootVolume in fileService.get("rootVolumes"):
        rootVolumes.append({"rootVolumeName":rootVolume.get("name"), "rootVolumeDescription":rootVolume.get("description")})

    return rootVolumes

def getDataVolumesInfo(fileService, verbose=True):
    """
    Gets the names and descriptions of data volumes available to the user in a particular FileService.

    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param verbose: boolean parameter defining whether warnings will be printed (set to True) or not (set to False).
    :return: list of dictionaries, where each dictionary contains the name and description of a data volume.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the RACM API returns an error.
    :example: fileServices = Files.getFileServices(); dataVolumesInfo = getDataVolumesInfo(fileServices[0])

    .. seealso:: Files.getRootVolumesInfo
    """
    if type(fileService) == str:
        fileService = getFileServiceFromName(fileService, verbose=verbose)

    dataVolumes = []
    for dataVolume in fileService.get("dataVolumes"):
        dataVolumes.append({"dataVolumeName":dataVolume.get("name"), "dataVolumeDescription":dataVolume.get("description")})

    return dataVolumes


def getUserVolumesInfo(fileService, rootVolumeName = None, verbose=True):
    """
    Gets the names definitions the RootVolumes available in a particular FileService, and  of the file services that a user is able to access.

    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param rootVolumeName: name of root Volume (string) for which the user volumes are fetched. If set to None, then user volumes in all root folders are fetched.
    :param verbose: boolean parameter defining whether warnings will be printed (set to True) or not (set to False).
    :return: list of dictionaries, where each dictionary contains the name and description of a root volumes that a user is able to access.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the RACM API returns an error.
    :example: fileServices = Files.getFileServices(); rootVolumeNames = Files.getUserVolumesInfo(fileServices[0])

    .. seealso:: Files.getRootVolumes,
    """
    if type(fileService) == str:
        fileService = getFileServiceFromName(fileService, verbose=verbose)

    userVolumes = [];
    for rootVolume in fileService.get("rootVolumes"):
        for userVolume in rootVolume.get('userVolumes'):
            path=os.path.join(rootVolume.get('name'),userVolume.get('owner'),userVolume.get('name'))
            if rootVolumeName is not None:
                if rootVolume.get('name') == rootVolumeName:
                    userVolumes.append({"userVolumeName": userVolume.get('name'), "path":path, "userVolumeDescription": userVolume.get('description'), "rootVolumeName":rootVolume.get("name"), "rootVolumeDescription":rootVolume.get("description")})

            else:
                userVolumes.append({"userVolumeName": userVolume.get('name'), "path":path,"userVolumeDescription": userVolume.get('description'),"rootVolumeName": rootVolume.get("name"), "rootVolumeDescription": rootVolume.get("description")})


    return userVolumes



def splitPath(path, fileService):
    """
    If the topVolume in the 'path' is a rootVolume, then this function splits the path of the form rootVolume/userVolumeOwner/userVolume/relativePath and returns the tuple (rootVolume, userVolumeOwner, userVolume, relativePath, True). On the other hand,
    if the topVolume in the 'path' is a dataVolume, then this function splits the path of the form dataVolume/relativePath and returns the tuple (dataVolume, None, None, relativePath, False).

    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param path: file system path (string), starting from the root volume level or data volume level. Example: rootVolume/userVolumeOwner/userVolume/relativePath... or dataVolume/relativePath...
    :return: a tuple containing the components: (topVolume, userVolumeOwner, userVolume, relativePath, isTopVolumeADataVolume)
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); Files.splitPath("Storage/myUserName/persistent/my/Relative/Path", fileServices[0]);

    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName
    """

    if type(fileService) == str:
        fileService = getFileServiceFromName(fileService)

    if path.startswith("/"):
        path = path[1:]


    path = path.split("/")
    if len(path) == 0:
        raise Exception("path variable does not conform with the format 'rootVolume/userVolumeOwner/userVolume/relativePath...'  or format 'dataVolume/relativePath...'")

    topVolume = path[0]
    isTopVolumeARootVolume = False

    for rootVolume in fileService.get("rootVolumes"):
        if rootVolume.get("name") == topVolume:
            isTopVolumeARootVolume = True

    if isTopVolumeARootVolume:
        if len(path) < 3:
            raise Exception("path variable does not conform with the format 'rootVolume/userVolumeOwner/userVolume/relativePath...'")

        rootVolume = path[0]
        userVolumeOwner = path[1]
        userVolume = path[2]
        relativePath = "/".join(path[3:])
        return (rootVolume, userVolumeOwner, userVolume, relativePath, isTopVolumeARootVolume)
    else:
        if len(path) < 1:
            raise Exception("path variable does not conform with the format 'dataVolume/relativePath...'")

        dataVolume = path[0]
        relativePath = "/".join(path[1:])
        return (dataVolume, None, None, relativePath, isTopVolumeARootVolume)




def createUserVolume(fileService, path, quiet=True):
    """
    Create a user volume.

    :param fileService: name of fileService (string), or object (dictionary) that defines the file service that contains the user volume. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param path: path (in the remote file service) to the user volume (string), starting from the root volume level. Example: rootVolume/userVolumeOwner/userVolume
    :param quiet: if set to False, will throw an error if the User Volume already exists. If True, won't throw an error.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); Files.createUserVolume(fileServices[0], "volumes","newUserVolume");

    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.delete, Files.upload, Files.download, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.createUserVolume"
        else:
            taskName = "SciScript-Python.Files.createUserVolume"

        if type(fileService) == str:
            fileService = getFileServiceFromName(fileService)

        (rootVolume, userVolumeOwner, userVolume, relativePath, isTopVolumeARootVolume) = splitPath(path, fileService);

        url = __getFileServiceAPIUrl(fileService) + "api/volume/" + rootVolume + "/" + userVolumeOwner + "/" + userVolume + "?quiet="+str(quiet) + "&TaskName="+taskName;

        headers = {'X-Auth-Token': token}
        res = requests.put(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            pass;
        else:
            raise Exception("Error when creating user volume  '" + str(path) + "' in file service '" + str(fileService.get('name')) + "'.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def deleteUserVolume(fileService, path, quiet=True):
    """
    Delete a user volume.

    :param fileService: name of fileService (string), or object (dictionary) that defines the file service that contains the root volume. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param path: path (in the remote file service) to the user volume (string), starting from the root volume level. Example: rootVolume/userVolumeOwner/userVolume
    :param quiet: If set to False, it will throw an error if user volume does not exists. If set to True. it will not throw an error.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); Files.deleteUserVolume("volumes","newUserVolume",fileServices[0]);

    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.delete, Files.upload, Files.download, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.createUserVolume"
        else:
            taskName = "SciScript-Python.Files.createUserVolume"

        if type(fileService) == str:
            fileService = getFileServiceFromName(fileService)

        (rootVolume, userVolumeOwner, userVolume, relativePath, isTopVolumeARootVolume) = splitPath(path, fileService);

        url = __getFileServiceAPIUrl(fileService) + "api/volume/" + rootVolume + "/" + userVolumeOwner + "/" + userVolume + "?quiet="+str(quiet)+"&TaskName="+taskName;

        headers = {'X-Auth-Token': token}
        res = requests.delete(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            pass
        else:
            raise Exception("Error when deleting user volume '" + str(path) + "' in file service '" + str(fileService.get('name')) + "'.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def createDir(fileService, path, quiet=True):
    """
    Create a directory.

    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param path: path (in the remote file service) to the directory (string), starting from the root volume level or data volume level. Examples: rootVolume/userVolumeOwner/userVolume/directory or dataVolume/directory
    :param quiet: If set to False, it will throw an error if the directory already exists. If set to True. it will not throw an error.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); Files.createDir(fileServices[0], "Storage/myUserName/persistent/myNewDir");

    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.delete, Files.upload, Files.download, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.createDir"
        else:
            taskName = "SciScript-Python.Files.createDir"

        if type(fileService) == str:
            fileService = getFileServiceFromName(fileService)

        (topVolume, userVolumeOwner, userVolume, relativePath, isTopVolumeARootVolume) = splitPath(path, fileService);

        if isTopVolumeARootVolume:
            url = __getFileServiceAPIUrl(fileService) + "api/folder/" + topVolume + "/" + userVolumeOwner + "/" + userVolume + "/" + relativePath + "?quiet=" + str(quiet) + "&TaskName=" + taskName;
        else:
            url = __getFileServiceAPIUrl(fileService) + "api/folder/" + topVolume + "/" + relativePath + "?quiet=" + str(quiet) + "&TaskName=" + taskName;

        headers = {'X-Auth-Token': token}
        res = requests.put(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            pass;
        else:
            raise Exception("Error when creating directory '" + str(path) + "' in file service '" + str(fileService.get('name')) + "'.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def upload(fileService, path, data="", localFilePath=None, quiet=True):
    """
    Uploads data or a local file into a path defined in the file system.

    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param path: path (in the remote file service) to the destination file (string), starting from the root volume level or data volume level. Examples: rootVolume/userVolumeOwner/userVolume/destinationFile.txt or dataVolume/destinationFile.txt
    :param data: string containing data to be uploaded, in case localFilePath is not set.
    :param localFilePath: path to a local file to be uploaded (string),
    :param userVolumeOwner: name (string) of owner of the userVolume. Can be left undefined if requester is the owner of the user volume.
    :param quiet: If set to False, it will throw an error if the file already exists. If set to True. it will not throw an error.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); Files.upload(fileServices[0], "myRootVolume/myUserName/myUserVolume/myUploadedFile.txt", None, localFilePath="/myFile.txt");

    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.createDir, Files.delete, Files.download, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.UploadFile"
        else:
            taskName = "SciScript-Python.Files.UploadFile"

        if type(fileService) == str:
            fileService = getFileServiceFromName(fileService)

        (topVolume, userVolumeOwner, userVolume, relativePath, isTopVolumeARootVolume) = splitPath(path, fileService);

        if isTopVolumeARootVolume:
            url = __getFileServiceAPIUrl(fileService) + "api/file/" + topVolume + "/" + userVolumeOwner + "/" + userVolume + "/" + relativePath + "?quiet=" + str(quiet) + "&TaskName="+taskName
        else:
            url = __getFileServiceAPIUrl(fileService) + "api/file/" + topVolume + "/" + relativePath + "?quiet=" + str(quiet) + "&TaskName=" + taskName

        headers = {'X-Auth-Token': token}

        if localFilePath is not None and localFilePath != "":
            with open(localFilePath, "rb") as file:
                res = requests.put(url, data=file, headers=headers, stream=True)
        else:
            if data != None:
                res = requests.put(url, data=data, headers=headers, stream=True)
            else:
                raise Exception("Error: No local file or data specified for uploading.");

        if res.status_code >= 200 and res.status_code < 300:
            pass;
        else:
            raise Exception("Error when uploading file to '" + str(path) + "' in file service '" + str(fileService.get('name')) + "'.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def download(fileService, path, localFilePath=None, format="txt", quiet=True):
    """
    Downloads a file from the remote file system into the local file system, or returns the file content as an object in several formats.

    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param path: String defining the path (in the remote file service) of the file to be downloaded, starting from the root volume level or data volume level. Examples: rootVolume/userVolumeOwner/userVolume/fileToBeDownloaded.txt or dataVolume/fileToBeDownloaded.txt
    :param localFilePath: local destination path of the file to be downloaded. If set to None, then an object of format 'format' will be returned.
    :param format: name (string) of the returned object's type (if localFilePath is not defined). This parameter can be "StringIO" (io.StringIO object containing readable text), "BytesIO" (io.BytesIO object containing readable binary data), "response" ( the HTTP response as an object of class requests.Response) or "txt" (a text string). If the parameter 'localFilePath' is defined, then the 'format' parameter is not used and the file is downloaded to the local file system instead.
    :param userVolumeOwner: name (string) of owner of the volume. Can be left undefined if requester is the owner of the volume.
    :param quiet: If set to False, it will throw an error if the file already exists. If set to True. it will not throw an error.
    :return: If the 'localFilePath' parameter is defined, then it will return True when the file is downloaded successfully in the local file system. If the 'localFilePath' is not defined, then the type of the returned object depends on the value of the 'format' parameter (either io.StringIO, io.BytesIO, requests.Response or string).
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); Files.upload(fileServices[0], "Storage/myUserName/persistent/fileToBeDownloaded.txt", localFilePath="/fileToBeDownloaded.txt");

    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.createDir, Files.delete, Files.upload, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":


        if type(fileService) == str:
            fileService = getFileServiceFromName(fileService)

        (topVolume, userVolumeOwner, userVolume, relativePath, isTopVolumeARootVolume) = splitPath(path, fileService);

        if localFilePath is not None:
            if os.path.isfile(localFilePath) and not quiet:
                raise Exception("Error when downloading '" + str(path) + "' from file service '" + str(fileService.get("name")) + "'. Local file '" + localFilePath + "' already exists.");

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.DownloadFile"
        else:
            taskName = "SciScript-Python.Files.DownloadFile"

        if isTopVolumeARootVolume:
            url = __getFileServiceAPIUrl(fileService) + "api/file/" + topVolume + "/" + userVolumeOwner + "/" + userVolume + "/" + relativePath + "?TaskName=" + taskName;
        else:
            url = __getFileServiceAPIUrl(fileService) + "api/file/" + topVolume + "/" + relativePath + "?TaskName=" + taskName;

        headers = {'X-Auth-Token': token}

        res = requests.get(url, stream=True, headers=headers)

        if res.status_code < 200 or res.status_code >= 300:
            raise Exception("Error when downloading '" + str(path) + "' from file service '" + str(fileService.get("name")) + "'.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());

        if localFilePath is not None and localFilePath != "":

            bytesio = BytesIO(res.content)
            theFile = open(localFilePath, "w+b")
            theFile.write(bytesio.read())
            theFile.close()
            return True

        else:

            if format is not None and format != "":
                if format == "StringIO":
                    return StringIO(res.content.decode())
                if format == "txt":
                    return res.content.decode()
                elif format == "BytesIO":
                    return BytesIO(res.content)
                elif format == "response":
                    return res;
                else:
                    raise Exception("Unknown format '" + format + "' when trying to download from remote File System the file " + str(relativePath) + ".\n");
            else:
                raise Exception("Wrong format parameter value\n");

    else:
        raise Exception("User token is not defined. First log into SciServer.")



def dirList(fileService, path, level=1, options=''):
    """
    Lists the contents of a directory.

    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param path: String defining the path (in the remote file service) of the directory to be listed, starting from the root volume level or data volume level. Examples: rootVolume/userVolumeOwner/userVolume/directoryToBeListed or dataVolume/directoryToBeListed
    :param level: amount (int) of listed directory levels that are below or at the same level to that of the relativePath.
    :param options: string of file filtering options.
    :return: dictionary containing the directory listing.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); dirs = Files.dirList(fileServices[0], "Storage/myUserName/persistent/", level=2);

    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.delete, Files.upload, Files.download, Files.createDir
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.dirList"
        else:
            taskName = "SciScript-Python.Files.dirList"

        if type(fileService) == str:
            fileService = getFileServiceFromName(fileService)

        (topVolume, userVolumeOwner, userVolume, relativePath, isTopVolumeARootVolume) = splitPath(path, fileService);

        if isTopVolumeARootVolume:
            url = __getFileServiceAPIUrl(fileService) + "api/jsontree/" + topVolume + "/" + userVolumeOwner + "/" + userVolume + "/" + relativePath + "?options=" + options + "&level=" + str(level) + "&TaskName=" + taskName;
        else:
            url = __getFileServiceAPIUrl(fileService) + "api/jsontree/" + topVolume + "/" + relativePath + "?options=" + options + "&level=" + str(level) + "&TaskName=" + taskName;

        headers = {'X-Auth-Token': token}
        res = requests.get(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            return json.loads(res.content.decode());
        else:
            raise Exception("Error when listing contents of '" + str(path) + "'.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def move(fileService, path, destinationFileService, destinationPath, replaceExisting=True, doCopy=True):
    """
    Moves or copies a file or folder.

    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param path: String defining the origin path (in the remote fileService) of the file or directory to be copied/moved, starting from the root volume level or data volume level. Examples: rootVolume/userVolumeOwner/userVolume/fileToBeMoved.txt or dataVolume/fileToBeMoved.txt
    :param destinationFileService: name of fileService (string), or object (dictionary) that defines a destination file service (where the file is moved/copied into). A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param destinationRelativePath: String defining the destination path (in the remote destinationFileService) of the file or directory to be copied/moved, starting from the root volume level or data volume level. Examples: rootVolume/userVolumeOwner/userVolume/recentlyMovedFile.txt or dataVolume/recentlyMovedFile.txt}
    :param replaceExisting: If set to False, it will throw an error if the file already exists, If set to True, it will not throw and eeror in that case.
    :param doCopy: if set to True, then it will copy the file or folder. If set to False, then the file or folder will be moved.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); Files.move(fileServices[0], "Storage/myUserName/persistent/myFile.txt", fileServices[0], "Storage/myUserName/persistent/myFile2.txt")

    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.createDir, Files.delete, Files.upload, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.Move"
        else:
            taskName = "SciScript-Python.Files.Move"

        if type(fileService) == str:
            destinationFileServiceName = destinationFileService
            fileService = getFileServiceFromName(fileService)
        else:
            destinationFileServiceName = destinationFileService['name']


        if type(destinationFileService) == str:
            destinationFileService = getFileServiceFromName(destinationFileService)

        (topVolume, userVolumeOwner, userVolume, relativePath, isTopVolumeARootVolume) = splitPath(path, fileService);
        (destinationTopVolume, destinationUserVolumeOwner, destinationUserVolume, destinationRelativePath, isDestinationTopVolumeARootVolume) = splitPath(destinationPath, destinationFileService);

        if isTopVolumeARootVolume:
            url = __getFileServiceAPIUrl(fileService) + "api/data/" + topVolume + "/" + userVolumeOwner + "/" + userVolume + "/" + relativePath + "?replaceExisting=" + str(replaceExisting) + "&doCopy=" + str(doCopy) + "&TaskName=" + taskName;
            jsonDict = {'destinationPath': destinationRelativePath,
                        'destinationRootVolume': destinationTopVolume,
                        'destinationUserVolume': destinationUserVolume,
                        'destinationOwnerName': destinationUserVolumeOwner,
                        'destinationFileService': destinationFileServiceName};
        else:
            url = __getFileServiceAPIUrl(fileService) + "api/data/" + topVolume + "/" + relativePath + "?replaceExisting=" + str(replaceExisting) + "&doCopy=" + str(doCopy) + "&TaskName=" + taskName;
            jsonDict = {'destinationPath': destinationRelativePath,
                        'destinationDataVolume': destinationTopVolume,
                        'destinationFileService': destinationFileServiceName};

        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.put(url, stream=True, headers=headers, json=jsonDict)

        if res.status_code < 200 or res.status_code >= 300:
            raise Exception("Error when moving '" + str(path) + "' in file service '" + str(fileService.get("name")) + "' to '" + str(destinationPath) + "' in file service '" + str(destinationFileService.get("name")) + "'. \nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());

    else:
        raise Exception("User token is not defined. First log into SciServer.")



def delete(fileService, path, quiet=True):
    """
    Deletes a directory or file in the File System.

    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param path: String defining the path (in the remote fileService) of the file or directory to be deleted, starting from the root volume level or data volume level. Examples: rootVolume/userVolumeOwner/userVolume/fileToBeDeleted.txt or dataVolume/fileToBeDeleted.txt
    :param quiet: If set to False, it will throw an error if the file does not exist. If set to True. it will not throw an error.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); Files.delete(fileServices[0], "Storage/myUserName/persistent/myUselessFile.txt");

    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.createDir, Files.upload, Files.download, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.delete"
        else:
            taskName = "SciScript-Python.Files.delete"

        if type(fileService) == str:
            fileService = getFileServiceFromName(fileService)

        (topVolume, userVolumeOwner, userVolume, relativePath, isTopVolumeARootVolume) = splitPath(path, fileService);

        if isTopVolumeARootVolume:
            url = __getFileServiceAPIUrl(fileService) + "api/data/" + topVolume + "/" + userVolumeOwner + "/" + userVolume + "/" + relativePath + "?quiet=" + str(quiet) + "&TaskName="+taskName
        else:
            url = __getFileServiceAPIUrl(fileService) + "api/data/" + topVolume + "/" + relativePath + "?quiet=" + str(quiet) + "&TaskName=" + taskName

        headers = {'X-Auth-Token': token}
        res = requests.delete(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            pass;
        else:
            raise Exception("Error when deleting '" + str(path) + "'.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")



def shareUserVolume(fileService, path, sharedWith, allowedActions, sharee_type="USER"):
    """
    Shares a user volume with another user or group

    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param path: String defining the path (in the remote fileService) of the user volume to be shared, starting from the root volume level. Example: rootVolume/userVolumeOwner/userVolume.
    :param sharedWith: name (string) of user or group that the user volume is shared with.
    :param allowedActions: array of strings defining actions the user or group is allowed to do with respect to the shared user volume. E.g.: ["read","write","grant","delete"]. The "grant" action means that the user or group can also share the user volume with another user or group. The "delete" action meand ability to delete the user volume (use with care).
    :param sharee_type: type (string) of the entity defined by the "sharedWith" parameter. Can be set to "USER" or "GROUP".
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); Files.shareUserVolume(getFileServices[0], "Storage/myUserName/myUSerVolume", "userName", ["read","write"], type="USER");

    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.createDir, Files.upload, Files.download, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.ShareUserVolume"
        else:
            taskName = "SciScript-Python.Files.ShareUserVolume"

        if type(fileService) == str:
            fileService = getFileServiceFromName(fileService)

        (rootVolume, userVolumeOwner, userVolume, relativePath, isTopVolumeARootVolume) = splitPath(path, fileService);


        data = [{'name': sharedWith, 'type':sharee_type, 'allowedActions': allowedActions }]
        body = json.dumps(data).encode()

        url = __getFileServiceAPIUrl(fileService) + "api/share/" + rootVolume + "/" + userVolumeOwner + "/" + userVolume + "?TaskName="+taskName

        headers = {'X-Auth-Token': token,'Content-Type':'application/json'}
        res = requests.patch(url, headers=headers, data=body)

        if res.status_code >= 200 and res.status_code < 300:
            pass;
        else:
            raise Exception("Error when sharing userVolume '" + str(path) + "' in file service '" + str(fileService.get('name')) + "'.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")
