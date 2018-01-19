__author__ = 'mtaghiza'

from SciServer import Authentication, Config
import requests
import json
from io import StringIO
from io import BytesIO



def getFileServices():
    """
    Gets the definitions of the file services that a user is able to access.
    :return: list of dictionaries, where each dictionary is is the descriptions of a FileService that a user is able to access.
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

        url = Config.RacmApiURL + "/storem?TaskName="+taskName;

        headers = {'X-Auth-Token': token}
        res = requests.get(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            return json.loads(res.content.decode());
        else:
            raise Exception("Error when getting the list of FileServices.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def getFileServiceFromName(fileServiceName, fileServices=None):
    """
    Returns a FileService object, given its registered name.
    :param fileServiceName: name of the FileService, as shown within the results of Jobs.getFileServices()
    :param fileServices: a list of FileService objects (dictionaries), as returned by Jobs.getFileServices(). If not set, then an extra internal call to Jobs.getFileServices() is made.
    :return: a FileService object (dictionary) that defines a FileService. A list of these kind of objects available to the user is returned by the function Jobs.getFileServices().
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the RACM API returns an error.
    :example: fileService = Files.getFileServiceFromName('fileServiceAtJHU');
    .. seealso:: Files.getFileServices
    """

    if fileServiceName is None:
        raise Exception("fileServiceName is not defined.")
    else:
        if fileServices is None:
            fileServices = getFileServices();

        if fileServices.__len__() > 0:
            for fileService in fileServices:
                if fileServiceName == fileService.get('name'):
                    return fileService;
        else:
            raise Exception("There are no FileServices available for the user.");

        raise Exception("FileService of name '" + fileServiceName + "' is not available or does not exist.");



def __getFileServiceAPIUrl(fileService=None):
    """
    Gets the API endpoint URL of a FileService.
    :param fileService: object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices(). If not set, then an extra internal call to Jobs.getFileServices() is made, and the returned URL is that of the first FileService found.
    :return: API endpoint URL of the FileService (string).
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the RACM API returns an error.
    :example: fileServiceUrl = Files.__getFileServiceAPIUrl();
    .. seealso:: Files.getFileServiceFromName
    """
    return 'http://scitest12.pha.jhu.edu/fileservice/'; # for now

    if fileService is None:
        fileServices = getFileServices();
        if fileServices.__len__() > 0:
            return fileServices[0].get("URL");
        else:
            raise Exception("There are no FileServices available for the user.");

    else:
        return fileService.get("URL");


def createDir(path, volume, ownerName=None, fileService=None):
    """
    Create a directory.
    :param path: path (string) to the directory, relative to the volume level. E.g: path="/newDirectory"
    :param volume: volume (string) containing the path.
    :param ownerName: name (string) of owner of the volume. Can be left undefined if requester is the owner of the volume.
    :param fileService: object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :return: True if the directory creation was successful.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); Files.createDir("myNewDir","persistent","myUserName", fileServices[0]);
    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.delete, Files.upload, Files.download, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.createDir"
        else:
            taskName = "SciScript-Python.Files.createDir"

        if ~path.startswith("/"):
            path = "/" + path;

        if ownerName is None:
            ownerName = Authentication.getKeystoneUserWithToken().userName;

        url = Config.FileServiceURL + "/folder/" + volume + "/" + ownerName + path + "?TaskName="+taskName;

        headers = {'X-Auth-Token': token}
        res = requests.put(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            return True;
        else:
            raise Exception("Error when creating directory '" + path + "' in volume '" + volume + "'.\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def dirList(path, volume, ownerName=None, fileService=None, level=1, options=''):
    """
    Lists the contents of a directory.
    :param path: path (string) to the directory, relative to the volume level. E.g: path="/newDirectory"
    :param volume: volume (string) containing the path.
    :param ownerName: name (string) of owner of the volume. Can be left undefined if requester is the owner of the volume.
    :param fileService: object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param level: amount (int) of listed directory levels that are below or at the same level of the path.
    :param options: string of file filtering options.
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

        if ownerName is None:
            ownerName = Authentication.getKeystoneUserWithToken().userName;

        if ~path.startswith("/"):
            path = "/" + path;

        url = Config.FileServiceURL + "/jsontree/" + volume + "/" + ownerName + path + "?options=" + str(options) + "&level=" + str(level) + "&TaskName=" + taskName;

        headers = {'X-Auth-Token': token}
        res = requests.get(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            return res.content.decode();
        else:
            raise Exception("Error when creating new directory " + path + ".\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")




def delete(path, volume, ownerName=None, fileService=None):
    """
    Deletes a directory or file in the File System.
    :param path: path (string) to the directory or file to be deleted, relative to the volume level. E.g: path="/directory/file.txt"
    :param volume: volume (string) containing the path.
    :param ownerName: name (string) of owner of the volume. Can be left undefined if requester is the owner of the volume.
    :param fileService: object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :return: True if the deletion was successful.
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

        if ~path.startswith("/"):
            path = "/" + path;

        url = Config.FileServiceURL + "/data" + path + "?TaskName="+taskName

        headers = {'X-Auth-Token': token}
        res = requests.delete(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            return True;
        else:
            raise Exception("Error when deleting " + path + ".\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")



def upload(path, volume, ownerName=None, fileService=None, data=None, localFilePath=None):
    """
    Uploads data or a local file into a path defined in the file system.
    :param path: destination path (string) of the file to be uploaded, relative to the volume level. E.g: path="/newDirectory/myNewFile.txt"
    :param volume: volume (string) containing the path.
    :param ownerName: name (string) of owner of the volume. Can be left undefined if requester is the owner of the volume.
    :param fileService: object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param data: string containing data to be uploaded.
    :param localFilePath: path to a local file to be uploaded (string).
    :return: True is upload was successful.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); isUploaded = Files.upload("/myUploadedFile.txt","persistent","myUserName", fileServices[0], localFilePath="/myFile.txt");
    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.createDir, Files.delete, Files.download, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.upload"
        else:
            taskName = "SciScript-Python.Files.upload"

        if ~path.startswith("/"):
            path = "/" + path;

        fileName = path.split("/")[-1]
        dirPath = path.replace(fileName, "")

        createDir(dirPath);

        url = Config.FileServiceURL + "/data" + path + "?TaskName="+taskName

        headers = {'X-Auth-Token': token}

        if localFilePath is not None and localFilePath != "":
            with open(localFilePath, "rb") as file:
                res = requests.put(url, data=file, headers=headers, stream=True)
        else:
            if data != None:
                res = requests.put(url, data=data, headers=headers, stream=True)
            else:
                raise Exception("Error when uploading to " + path + ". No local file or data specified for uploading.");

        if res.status_code >= 200 and res.status_code < 300:
            return True;
        else:
            raise Exception("Error when uploading to " + path +".\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def download(path, volume, ownerName=None, fileService=None, localFilePath="", format="text"):
    """
    Downloads a file from the remote file system into the local file system, or returns the file content as an object in several formats.
    :param path: remote path (string) of the file to be downloaded, relative to the volume level. E.g: path="/newDirectory/myNewFile.txt"
    :param volume: volume (string) containing the path.
    :param ownerName: name (string) of owner of the volume. Can be left undefined if requester is the owner of the volume.
    :param fileService: object (dictionary) that defines a file service. A list of these kind of objects available to the user is returned by the function Files.getFileServices().
    :param localFilePath: local path of the file to be downloaded. If 'localFilePath' is defined, then the 'format' parameter is not used.
    :param format: type of the returned object. Can be "StringIO" (io.StringIO object containing readable text), "BytesIO" (io.BytesIO object containing readable binary data), "response" ( the HTTP response as an object of class requests.Response) or "text" (a text string). If the parameter 'localFilePath' is defined, then the 'format' parameter is not used and the file is downloaded to the local file system instead.
    :return: If the 'localFilePath' parameter is defined, then it will return True when the file is downloaded successfully in the local file system. If the 'localFilePath' is not defined, then the type of the returned object depends on the value of the 'format' parameter (either io.StringIO, io.BytesIO, requests.Response or string).
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileService API returns an error.
    :example: fileServices = Files.getFileServices(); isDownloaded = Files.upload("/myUploadedFile.txt","persistent","myUserName", fileServices[0], localFilePath="/myDownloadedFile.txt");
    .. seealso:: Files.getFileServices(), Files.getFileServiceFromName, Files.createDir, Files.delete, Files.upload, Files.dirList
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Files.download"
        else:
            taskName = "SciScript-Python.Files.download"

        if ~path.startswith("/"):
            path = "/" + path;

        url = Config.FileServiceURL + "/data" + path + "?TaskName="+taskName;
        headers = {'X-Auth-Token': token}

        res = requests.get(url, stream=True, headers=headers)

        if res.status_code < 200 or res.status_code >= 300:
            raise Exception("Error when downloading from remote File Syatem the file " + str(path) + ".\nHttp Response from FileSystem API returned status code " + str(res.status_code) + ":\n" + res.content.decode());

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
                if format == "text":
                    return res.content.decode()
                elif format == "BytesIO":
                    return BytesIO(res.content)
                elif format == "response":
                    return res;
                else:
                    raise Exception("Unknown format '" + format + "' when trying to download from remote File System the file " + str(path) + ".\n");
            else:
                raise Exception("Wrong format parameter value\n");

    else:
        raise Exception("User token is not defined. First log into SciServer.")
