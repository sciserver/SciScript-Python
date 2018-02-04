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
    Returns the names of the fileServices available to the user.
    :param fileServices: a list of FileService objects (dictionaries), as returned by Jobs.getFileServices(). If not set, then an extra internal call to Jobs.getFileServices() is made.
    :param verbose: boolean parameter defining whether warnings will be printed (set to True) or not (set to False).
    :return: an array of strings, each being the name of a file service available to the user.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the RACM API returns an error.
    :example: fileServiceNames = Files.getFileServicesNames();
    .. seealso:: Files.getFileServices
    """

    if fileServices is None:
        fileServices = getFileServices(verbose);

    fileServiceNames = [];
    if fileServices.__len__() > 0:
        for fileService in fileServices:
            fileServiceNames.append(fileService.get('name'))

        return fileServiceNames;
    else:
            #raise Exception("There are no FileServices available for the user.");
        if verbose:
            warnings.warn("There are no FileServices available for the user.", Warning, stacklevel=2)
        return fileServiceNames





def getFileServiceFromName(fileServiceName, fileServices=None, verbose=True):
    """
    Returns a FileService object, given its registered name.
    :param fileServiceName: name of the FileService, as shown within the results of Jobs.getFileServices()
    :param fileServices: a list of FileService objects (dictionaries), as returned by Jobs.getFileServices(). If not set, then an extra internal call to Jobs.getFileServices() is made.
    :param verbose: boolean parameter defining whether warnings will be printed (set to True) or not (set to False).
    :return a FileService object (dictionary) that defines a FileService. A list of these kind of objects available to the user is returned by the function Jobs.getFileServices(). If no fileService can be found, then returns None.
    :raises Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the RACM API returns an error.
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

            #raise Exception("FileService of name '" + fileServiceName + "' is not available or does not exist.");
            if verbose:
                warnings.warn("FileService of name '" + fileServiceName + "' is not available or does not exist.", Warning, stacklevel=2)
            return None
        else:
            #raise Exception("There are no FileServices available for the user.");
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


def getRootVolumes(fileService):
    """
    Gets the definitions the RootVolumes available in a particular FileService, and  of the file services that a user is able to access.
    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :return: list of dictionaries, where each dictionary contains the definition of a FileService that a user is able to access.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the RACM API returns an error.
    :example: fileServices = Files.getFileServices(); rootVolumes = getRootVolumes(fileServices[0])
    .. seealso:: Files.getFileServiceFromName
    """
    return fileService.get("rootVolumes");


def createUserVolume(fileService, rootVolume, userVolume, userVolumeOwner=None, quiet=True):
    """
    Create a user volume.
    :param fileService: name of fileService (string), or object (dictionary) that defines the file service that contains the root volume. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param rootVolume: name of root volume (string) that contains the user volume to be created. A list of user volumes is given by getRootVolumes(fileService)
    :param userVolume: name of user volume (string) to be created, one level below the root volume level.
    :param userVolumeOwner: name (string) of owner of the userVolume. Can be left undefined if requester is the owner of the user volume.
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

        if userVolumeOwner is None:
            userVolumeOwner = Authentication.getKeystoneUserWithToken(token).userName;

        url = __getFileServiceAPIUrl(fileService) + "api/volume/" + rootVolume + "/" + userVolumeOwner + "/" + userVolume + "?quiet="+str(quiet) + "&TaskName="+taskName;

        headers = {'X-Auth-Token': token}
        res = requests.put(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            pass;
        else:
            raise Exception("Error when creating user volume  '" + userVolume + "' in root volume '" + rootVolume + "' in file service named '" + fileService.get('name') + "'.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def deleteUserVolume(fileService, rootVolume, userVolume, userVolumeOwner=None, quiet=True):
    """
    Delete a user volume.
    :param fileService: name of fileService (string), or object (dictionary) that defines the file service that contains the root volume. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param rootVolume: root volume name (string) that contains the user volume to be deleted. A list of user volumes is given by getRootVolumes(fileService)
    :param userVolume: user volume name (string) to be deleted.
    :param userVolumeOwner: name (string) of owner of the userVolume. Can be left undefined if requester is the owner of the user volume.
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

        if userVolumeOwner is None:
            userVolumeOwner = Authentication.getKeystoneUserWithToken(token).userName;

        url = __getFileServiceAPIUrl(fileService) + "api/volume/" + rootVolume + "/" + userVolumeOwner + "/" + userVolume + "?quiet="+str(quiet)+"&TaskName="+taskName;

        headers = {'X-Auth-Token': token}
        res = requests.delete(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            pass
        else:
            raise Exception("Error when deleting user volume  '" + userVolume + "' in root volume '" + rootVolume + "' in file service named '" + fileService.get('name') + "'.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def createDir(fileService, rootVolume, userVolume, relativePath, userVolumeOwner=None, quiet=True):
    """
    Create a directory.
    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param rootVolume: root volume name (string) that contains the userVolume. A list of root volumes is contained by the fileSerive object.
    :param userVolume: user volume name (string) that contains the relativePath.
    :param relativePath: path (string) to the directory, relative to the userVolume level. E.g: path="/newDirectory"
    :param userVolumeOwner: name (string) of owner of the userVolume. Can be left undefined if requester is the owner of the user volume.
    :param quiet: If set to False, it will throw an error if the directory already exists. If set to True. it will not throw an error.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); Files.createDir(fileServices[0], "myRootVolume","myUserVolume", "myNewDir");
    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.delete, Files.upload, Files.download, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.createDir"
        else:
            taskName = "SciScript-Python.Files.createDir"

        if not relativePath.startswith("/"):
            relativePath = "/" + relativePath;

        if userVolumeOwner is None:
            userVolumeOwner = Authentication.getKeystoneUserWithToken(token).userName;

        url =  __getFileServiceAPIUrl(fileService) + "api/folder/" + rootVolume + "/" + userVolumeOwner + "/" + userVolume + relativePath + "?quiet=" + str(quiet) + "&TaskName=" + taskName;

        headers = {'X-Auth-Token': token}
        res = requests.put(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            pass;
        else:
            raise Exception("Error when creating directory '" + relativePath + "' in user volume  '" + userVolume + "' in root volume '" + rootVolume + "' in file service named '" + fileService.get('name') + "'.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def upload(fileService, rootVolume, userVolume, relativePath, data="", localFilePath=None, userVolumeOwner=None, quiet=True):
    """
    Uploads data or a local file into a path defined in the file system.
    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param rootVolume: root volume name (string) that contains the userVolume. A list of root volumes is contained by the fileSerive object.
    :param userVolume: user volume name (string) that contains the relativePath.
    :param relativePath: destination path (string) of the file to be uploaded, relative to the user volume level. E.g: path="/newDirectory/myNewFile.txt".
    :param data: string containing data to be uploaded, in case localFilePath is not set.
    :param localFilePath: path to a local file to be uploaded (string),
    :param userVolumeOwner: name (string) of owner of the userVolume. Can be left undefined if requester is the owner of the user volume.
    :param quiet: If set to False, it will throw an error if the file already exists. If set to True. it will not throw an error.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); Files.upload(fileServices[0], "myRootVolume", "myUserVolume", "/myUploadedFile.txt", None, None, localFilePath="/myFile.txt");
    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.createDir, Files.delete, Files.download, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.UploadFile"
        else:
            taskName = "SciScript-Python.Files.UploadFile"

        if not relativePath.startswith("/"):
            relativePath = "/" + relativePath;

        if userVolumeOwner is None:
            userVolumeOwner = Authentication.getKeystoneUserWithToken(token).userName;

        url = __getFileServiceAPIUrl(fileService) + "api/file/" + rootVolume + "/" + userVolumeOwner + "/" + userVolume + relativePath + "?quiet=" + str(quiet) + "&TaskName="+taskName

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
            raise Exception("Error when uploading file '" + relativePath + "' in user volume  '" + userVolume + "' in root volume '" + rootVolume + "' in file service named '" + fileService.get('name') + "'.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def download(fileService, rootVolume, userVolume, relativePath, localFilePath=None, format="txt", userVolumeOwner=None, quiet=True):
    """
    Downloads a file from the remote file system into the local file system, or returns the file content as an object in several formats.
    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param rootVolume: root volume name (string) that contains the userVolume. A list of root volumes is contained by the fileSerive object.
    :param userVolume: user volume name (string) that contains the relativePath.
    :param relativePath: remote path (string) of the file to be downloaded, relative to the user volume level. E.g: path="/newDirectory/myNewFile.txt"
    :param localFilePath: local destination path of the file to be downloaded. If set to None, then an object of format 'format' will be returned.
    :param format: name (string) of the returned object's type (if localFilePath is not defined). This parameter can be "StringIO" (io.StringIO object containing readable text), "BytesIO" (io.BytesIO object containing readable binary data), "response" ( the HTTP response as an object of class requests.Response) or "txt" (a text string). If the parameter 'localFilePath' is defined, then the 'format' parameter is not used and the file is downloaded to the local file system instead.
    :param userVolumeOwner: name (string) of owner of the volume. Can be left undefined if requester is the owner of the volume.
    :param quiet: If set to False, it will throw an error if the file already exists. If set to True. it will not throw an error.
    :return: If the 'localFilePath' parameter is defined, then it will return True when the file is downloaded successfully in the local file system. If the 'localFilePath' is not defined, then the type of the returned object depends on the value of the 'format' parameter (either io.StringIO, io.BytesIO, requests.Response or string).
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); isDownloaded = Files.upload("/myUploadedFile.txt","persistent","myUserName", fileServices[0], localFilePath="/myDownloadedFile.txt");
    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.createDir, Files.delete, Files.upload, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":


        if localFilePath is not None:
            if os.path.isfile(localFilePath) and not quiet:
                raise Exception("Error when downloading from remote File Syatem the file " + str(relativePath) + ".\nLocal file '" + localFilePath + "' already exists.");

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.DownloadFile"
        else:
            taskName = "SciScript-Python.Files.DownloadFile"

        if not relativePath.startswith("/"):
            relativePath = "/" + relativePath;

        if userVolumeOwner is None:
            userVolumeOwner = Authentication.getKeystoneUserWithToken(token).userName;

        url = __getFileServiceAPIUrl(fileService) + "api/file/" + rootVolume + "/" + userVolumeOwner + "/" + userVolume + relativePath + "?TaskName=" + taskName;
        headers = {'X-Auth-Token': token}

        res = requests.get(url, stream=True, headers=headers)

        if res.status_code < 200 or res.status_code >= 300:
            raise Exception("Error when downloading from remote File Syatem the file " + str(relativePath) + ".\nHttp Response from FileSystem API returned status code " + str(res.status_code) + ":\n" + res.content.decode());

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



def dirList(fileService, rootVolume, userVolume, relativePath, level=1, options='', userVolumeOwner=None):
    """
    Lists the contents of a directory.
    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param rootVolume: root volume name (string) that contains the userVolume. A list of root volumes is contained by the fileSerive object.
    :param userVolume: user volume name (string) that contains the relativePath.
    :param relativePath: remote path (string) of the directory, relative to the user volume level. E.g: path="/newDirectory/myNewFile.txt"
    :param level: amount (int) of listed directory levels that are below or at the same level to that of the relativePath.
    :param options: string of file filtering options.
    :param userVolumeOwner: name (string) of owner of the volume. Can be left undefined if requester is the owner of the volume.
    :return: dictionary containing the directory listing.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); dirs = Files.dirList("/","persistent","myUserName", fileServices[0], 2);
    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.delete, Files.upload, Files.download, Files.createDir
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.dirList"
        else:
            taskName = "SciScript-Python.Files.dirList"

        if userVolumeOwner is None:
            userVolumeOwner = Authentication.getKeystoneUserWithToken(token).userName;

        if not relativePath.startswith("/"):
            path = "/" + relativePath;

        url = __getFileServiceAPIUrl(fileService) + "api/jsontree/" + rootVolume + "/" + userVolumeOwner + "/" + userVolume + path + "?options=" + options + "&level=" + str(level) + "&TaskName=" + taskName;

        headers = {'X-Auth-Token': token}
        res = requests.get(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            return json.loads(res.content.decode());
        else:
            raise Exception("Error when listing directory.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def move(fileService, rootVolume, userVolume, relativePath, destinationFileService, destinationRootVolume, destinationUserVolume, destinationRelativePath, replaceExisting=True, doCopy=True, userVolumeOwner=None, destinationUserVolumeOwner=None):
    """
    Moves or copies a file or folder.
    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param rootVolume: root volume name (string) that contains the userVolume. A list of root volumes is contained by the fileService object.
    :param userVolume: user volume name (string) that contains the relativePath.
    :param relativePath: remote path (string) of the file to be moved/copied, relative to the user volume level. E.g: path="/newDirectory/myNewFile.txt"
    :param destinationFileService: name of fileService (string), or object (dictionary) that defines a destination file service (where the file is moved/copied into). A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param destinationRootVolume: root volume name (string) that contains the userVolume where the file is moved/copied into. A list of root volumes is contained by the fileService object.
    :param destinationUserVolume: user volume name (string) that contains the relativePath where the file is moved/copied into.
    :param destinationRelativePath: remote path (string) of the destination file, relative to the user volume level. E.g: path="/newDirectory2/myNewFile.txt"
    :param replaceExisting: If set to False, it will throw an error if the file already exists, If set to True, it will not throw and eeror in that case.
    :param doCopy: if set to True, then it will copy the file or folder. If set to False, then the file or folder will be moved.
    :param userVolumeOwner: name (string) of owner of the user volume. Can be left undefined if requester is the owner of the volume.
    :param destinationUserVolumeOwner: name (string) of owner of the destination user volume. Can be left undefined if requester is the owner of it.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); isDownloaded = Files.upload("/myUploadedFile.txt","persistent","myUserName", fileServices[0], localFilePath="/myDownloadedFile.txt");
    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.createDir, Files.delete, Files.upload, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.DownloadFile"
        else:
            taskName = "SciScript-Python.Files.DownloadFile"

        if __getFileServiceAPIUrl(fileService) != __getFileServiceAPIUrl(fileService):
            raise Exception("Use of multiple file services is not supported for now.");

        if not relativePath.startswith("/"):
            relativePath = "/" + relativePath;

        if not destinationRelativePath.startswith("/"):
            destinationRelativePath = "/" + destinationRelativePath;

        if userVolumeOwner is None:
            userVolumeOwner = Authentication.getKeystoneUserWithToken(token).userName;

        if destinationUserVolumeOwner is None:
            destinationuserVolumeOwner = Authentication.getKeystoneUserWithToken(token).userName;

        url = __getFileServiceAPIUrl(fileService) + "api/data/" + rootVolume + "/" + userVolumeOwner + "/" + userVolume + relativePath + "?replaceExisting=" + str(replaceExisting) + "&doCopy=" + str(doCopy) + "&TaskName=" + taskName;
        headers = {'X-Auth-Token': token}
        jsonDict = {'destinationPath': destinationRelativePath, 'destinationRootFolder': destinationRootVolume, 'destinationUserVolume':destinationUserVolume, 'destinationOwner': destinationuserVolumeOwner};
        data = json.dumps(jsonDict).encode()
        res = requests.put(url, stream=True, headers=headers, data=data)

        if res.status_code < 200 or res.status_code >= 300:
            raise Exception("Error when downloading from remote File System the file " + str(relativePath) + ".\nHttp Response from FileSystem API returned status code " + str(res.status_code) + ":\n" + res.content.decode());

    else:
        raise Exception("User token is not defined. First log into SciServer.")



def delete(fileService, rootVolume, userVolume, relativePath, userVolumeOwner=None, quiet=True):
    """
    Deletes a directory or file in the File System.
    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param rootVolume: root volume name (string) that contains the userVolume. A list of root volumes is contained by the fileSerive object.
    :param userVolume: user volume name (string) that contains the relativePath.
    :param relativePath: remote path (string) of the directory or file to be deleted, relative to the user volume level. E.g: path="/newDirectory/myNewFile.txt"
    :param userVolumeOwner: name (string) of owner of the volume. Can be left undefined if requester is the owner of the volume.
    :param quiet: If set to False, it will throw an error if the file does not exist. If set to True. it will not throw an error.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); isDeleted = Files.delete("/myUselessFile.txt","persistent","myUserName", fileServices[0]);
    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.createDir, Files.upload, Files.download, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.delete"
        else:
            taskName = "SciScript-Python.Files.delete"

        if not relativePath.startswith("/"):
            relativePath = "/" + relativePath;

        if userVolumeOwner is None:
            userVolumeOwner = Authentication.getKeystoneUserWithToken(token).userName;

        url = __getFileServiceAPIUrl(fileService) + "api/data/" + rootVolume + "/" + userVolumeOwner + "/" + userVolume + relativePath + "?quiet=" + str(quiet) + "&TaskName="+taskName

        headers = {'X-Auth-Token': token}
        res = requests.delete(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            pass;
        else:
            raise Exception("Error when deleting " + relativePath + ".\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")



def shareUserVolume(fileService, rootVolume, userVolume, sharedWith, allowedActions, type="USER", userVolumeOwner=None):
    """
    Deletes a directory or file in the File System.
    :param fileService: name of fileService (string), or object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param rootVolume: root volume name (string) that contains the userVolume. A list of root volumes is contained by the fileSerive object.
    :param userVolume: user volume name (string) that contains the relativePath.
    :param sharedWith: name (string) of user or group that the userVolume is shared with.
    :param allowedActions: array of strings defining actions the user or group is allowed to do with respect to the shared user volume. E.g.: ["read","write","grant","delete"]. The "grant" action means that the user or group can also share the user volume with another user or group. The "delete" action meand ability to delete the user volume (use with care).
    :param type: type (string) of the entity defined by the "sharedWith" parameter. Can be set to "USER" or "GROUP".
    :param userVolumeOwner: name (string) of owner of the user volume. Can be left undefined if requester is the owner of the volume.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.shareUserVolume(); isDeleted = Files.delete("/myUselessFile.txt","persistent","myUserName", fileServices[0]);
    .. seealso:: Files.getFileServices(), Files.getFilrmsieServiceFromName, Files.createDir, Files.upload, Files.download, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.ShareUserVolume"
        else:
            taskName = "SciScript-Python.Files.ShareUserVolume"

        if userVolumeOwner is None:
            userVolumeOwner = Authentication.getKeystoneUserWithToken(token).userName;


        data = [{'name': sharedWith, 'type':type, 'allowedActions': allowedActions }]
        body = json.dumps(data).encode()

        url = __getFileServiceAPIUrl(fileService) + "api/share/" + rootVolume + "/" + userVolumeOwner + "/" + userVolume + "?TaskName="+taskName

        headers = {'X-Auth-Token': token}
        res = requests.patch(url, headers=headers, data=body)

        if res.status_code >= 200 and res.status_code < 300:
            pass;
        else:
            raise Exception("Error when sharing userVolume '" + userVolume + "' in rootVolume '" + rootVolume + "' with '" + sharedWith + "' in file service named '" + fileService.get(
                    'name') + "'.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")
