__author__ = 'mtaghiza'

from SciServer import Authentication, Config
import requests
from io import StringIO
from io import BytesIO


def createDir(path):
    """
    Create a directory.
    :param path: path to the directory. E.g: path="/persistent/newDirectory"
    :return: True if the directory creation was successful.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileSystem API returns an error.
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if ~path.startswith("/"):
            path = "/" + path;

        url = Config.FileServiceURL + "/data" + path + "?dir=true"

        headers = {'X-Auth-Token': token}
        res = requests.put(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            return True;
        else:
            raise Exception("Error when creating new directory " + path + ".\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def delete(path):
    """
    Delete a directory or file in the remote File System.
    :param path: path to the directory or file. E.g: path="/persistent/directory/file.txt"
    :return: True if the deletion was successful.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileSystem API returns an error.
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if ~path.startswith("/"):
            path = "/" + path;

        url = Config.FileServiceURL + "/data" + path + "?dir=true"

        headers = {'X-Auth-Token': token}
        res = requests.delete(url, headers=headers)

        if res.status_code >= 200 and res.status_code < 300:
            return True;
        else:
            raise Exception("Error when deleting " + path + ".\nHttp Response from FileService API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
    else:
        raise Exception("User token is not defined. First log into SciServer.")



def upload(path, data=None, localFilePath=None):
    """
    Uploads data or a local file into a path defined in the file system.
    :param path: path of the file to be created. E.g: path="/persistent/newDirectory/myNewFile.txt"
    :param data: string containing data to be uploaded.
    :param localFilePath: path to a local file to be uploaded (string).
    :return: True is upload was successful.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileSystem API returns an error.
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if ~path.startswith("/"):
            path = "/" + path;


        fileName = path.split("/")[-1]
        dirPath = path.replace(fileName, "")

        createDir(dirPath);

        url = Config.FileServiceURL + "/data" + path

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


def download(path, format="text", localFilePath=""):
    """
    Downloads a file from the remote file system into the local file system, or returns the file content as an object in several formats.

    :param path: path of the file (or directory) to be downloaded from the remote file system.
    :param format: type of the returned object. Can be "StringIO" (io.StringIO object containing readable text), "BytesIO" (io.BytesIO object containing readable binary data), "response" ( the HTTP response as an object of class requests.Response) or "text" (a text string). If the parameter 'localFilePath' is defined, then the 'format' parameter is not used and the file is downloaded to the local file system instead.
    :param localFilePath: local path of the file to be downloaded. If 'localFilePath' is defined, then the 'format' parameter is not used.
    :return: If the 'localFilePath' parameter is defined, then it will return True when the file is downloaded successfully in the local file system. If the 'localFilePath' is not defined, then the type of the returned object depends on the value of the 'format' parameter (either io.StringIO, io.BytesIO, requests.Response or string).
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the FileSystem API returns an error.
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        if ~path.startswith("/"):
            path = "/" + path;

        url = Config.FileServiceURL + "/data" + path
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
