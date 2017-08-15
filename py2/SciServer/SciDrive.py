import json
from io import StringIO
from io import BytesIO
import urllib
import requests as requests

from SciServer import Config, Authentication


class Task:
    """
    The class TaskName stores the name of the task that executes the API call.
    """
    name = None


task = Task();

def createContainer(path):
    """
    Creates a container (directory) in SciDrive

    :param path: path of the directory in SciDrive.
    :return: Returns True if the container (directory) was created successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the SciDrive API returns an error.
    :example: response = SciDrive.createContainer("MyDirectory")

    .. seealso:: SciDrive.upload.
    """
    token = Authentication.getToken()
    if token is not None and token != "":
        containerBody = ('<vos:node xmlns:xsi="http://www.w3.org/2001/thisSchema-instance" '
                         'xsi:type="vos:ContainerNode" xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0" '
                         'uri="vos://' + Config.SciDriveHost + '!vospace/' + path + '">'
                                                                                    '<vos:properties/><vos:accepts/><vos:provides/><vos:capabilities/>'
                                                                                    '</vos:node>')
        taskName = "";
        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.SciDrive.createContainer"
        else:
            taskName = "SciScript-Python.SciDrive.createContainer"

        url = Config.SciDriveHost + '/vospace-2.0/nodes/' + path + "?TaskName=" + taskName;
        data = str.encode(containerBody)
        headers = {'X-Auth-Token': token, 'Content-Type': 'application/xml'}
        res = requests.put(url, data=data, headers=headers)
        if res.status_code < 200 or res.status_code >= 300:
            raise Exception("Error when creating SciDrive container at " + str(path) + ".\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());

        return True
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def upload(path, data="", localFilePath=""):
    """
    Uploads data or a local file into a SciDrive directory.

    :param path: desired file path in SciDrive (string).
    :param data: data to be uploaded into SciDrive. If the 'localFilePath' parameter is set, then the local file will be uploaded instead.
    :param localFilePath: path to the local file to be uploaded (string).
    :return: Returns an object with the attributes of the uploaded file.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the SciDrive API returns an error.
    :example: response = SciDrive.upload("/SciDrive/path/to/file.csv", localFilePath="/local/path/to/file.csv")

    .. seealso:: SciDrive.createContainer
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        taskName = ""
        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.SciDrive.upload"
        else:
            taskName = "SciScript-Python.SciDrive.upload"

        url = Config.SciDriveHost + '/vospace-2.0/1/files_put/dropbox/' + path + "?TaskName=" + taskName;
        headers = {'X-Auth-Token': token}
        if(localFilePath != ""):
            with open(localFilePath, "rb") as file:
                res = requests.put(url, data=file, headers=headers, stream=True)
        else:
            res = requests.put(url, data=data, headers=headers, stream=True)

        if res.status_code != 200:
            if (localFilePath != None):
                raise Exception("Error when uploading local file " + str(localFilePath) + " to SciDrive path " + str(path) + ".\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());
            else:
                raise Exception("Error when uploading data to SciDrive path " + str(path) + ".\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());

        return json.loads(res.content.decode())
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def publicUrl(path):
    """
    Gets the public URL of a file (or directory) in SciDrive.

    :param path: path of the file (or directory) in SciDrive.
    :return: URL of a file in SciDrive (string).
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the SciDrive API returns an error.
    :example: url = SciDrive.publicUrl("path/to/SciDrive/file.csv")

    .. seealso:: SciDrive.upload
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        taskName = "";
        if task.name is not None:
            taskName = task.name;
            task.name = None;
        else:
            if Config.isSciServerComputeEnvironment():
                taskName = "Compute.SciScript-Python.SciDrive.publicUrl"
            else:
                taskName = "SciScript-Python.SciDrive.publicUrl"

        url = Config.SciDriveHost + '/vospace-2.0/1/media/sandbox/' + str(path) + "?TaskName=" + taskName
        headers = {'X-Auth-Token': token}
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            raise Exception("Error when getting the public URL of SciDrive file " + str(path) + ".\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());

        jsonRes = json.loads(res.content.decode())
        fileUrl = jsonRes["url"]
        return (fileUrl);

    else:
        raise Exception("User token is not defined. First log into SciServer.")


def directoryList(path=""):
    """
    Gets the contents and metadata of a SciDrive directory (or file).

    :param path: path of the directory (or file ) in SciDrive.
    :return: a dictionary containing info and metadata of the directory (or file).
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the SciDrive API returns an error.
    :example: dirList = SciDrive.directoryList("path/to/SciDrive/directory")

    .. seealso:: SciDrive.upload, SciDrive.download
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        taskName = "";
        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.SciDrive.directoryList"
        else:
            taskName = "SciScript-Python.SciDrive.directoryList"

        url = Config.SciDriveHost + "/vospace-2.0/1/metadata/sandbox/" + str(path) + "?list=True&path="  + str(path) + "&TaskName=" + taskName;
        headers = {'X-Auth-Token': token}
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            raise Exception("Error when getting the public URL of SciDrive file " + str(path) + ".\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());

        jsonRes = json.loads(res.content.decode())
        return (jsonRes);

    else:
        raise Exception("User token is not defined. First log into SciServer.")



def download(path, format="text", localFilePath=""):
    """
    Downloads a file (directory) from SciDrive into the local file system, or returns the file conetent as an object in several formats.

    :param path: path of the file (or directory) in SciDrive.
    :param format: type of the returned object. Can be "StringIO" (io.StringIO object containing readable text), "BytesIO" (io.BytesIO object containing readable binary data), "response" ( the HTTP response as an object of class requests.Response) or "text" (a text string). If the parameter 'localFilePath' is defined, then the 'format' parameter is not used and the file is downloaded to the local file system instead.
    :param localFilePath: local path of the file to be downloaded. If 'localFilePath' is defined, then the 'format' parameter is not used.
    :return: If the 'localFilePath' parameter is defined, then it will return True when the file is downloaded successfully in the local file system. If the 'localFilePath' is not defined, then the type of the returned object depends on the value of the 'format' parameter (either io.StringIO, io.BytesIO, requests.Response or string).
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the SciDrive API returns an error.
    :example: csvString = SciDrive.download("path/to/SciDrive/file.csv", format="text");

    .. seealso:: SciDrive.upload
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        taskName = "";
        if Config.isSciServerComputeEnvironment():
            task.name = "Compute.SciScript-Python.SciDrive.download"
        else:
            task.name = "SciScript-Python.SciDrive.download"

        fileUrl = publicUrl(path)
        res = requests.get(fileUrl, stream=True)
        if res.status_code != 200:
            raise Exception("Error when downloading SciDrive file " + str(path) + ".\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());

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
                    raise Exception("Unknown format '" + format + "' when trying to download SciDrive file " + str(path) + ".\n");
            else:
                raise Exception("Wrong format parameter value\n");

    else:
        raise Exception("User token is not defined. First log into SciServer.")


def delete(path):
    """
    Deletes a file or container (directory) in SciDrive.

    :param path: path of the file or container (directory) in SciDrive.
    :return: Returns True if the file or container (directory) was deleted successfully.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the SciDrive API returns an error.
        :example: response = SciDrive.delete("path/to/SciDrive/file.csv")

    .. seealso:: SciDrive.upload.
    """
    token = Authentication.getToken()
    if token is not None and token != "":
        containerBody = ('<vos:node xmlns:xsi="http://www.w3.org/2001/thisSchema-instance" '
                         'xsi:type="vos:ContainerNode" xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0" '
                         'uri="vos://' + Config.SciDriveHost + '!vospace/' + path + '">'
                                                                                    '<vos:properties/><vos:accepts/><vos:provides/><vos:capabilities/>'
                                                                                    '</vos:node>')
        taskName = "";
        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.SciDrive.delete"
        else:
            taskName = "SciScript-Python.SciDrive.delete"

        url = Config.SciDriveHost + '/vospace-2.0/nodes/' + path + "?TaskName=" + taskName;
        data = str.encode(containerBody)
        headers = {'X-Auth-Token': token, 'Content-Type': 'application/xml'}
        res = requests.delete(url, data=data, headers=headers)
        if res.status_code < 200 or res.status_code >= 300:
            raise Exception("Error when deleting " + str(path) + " in SciDrive.\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());

        return True
    else:
        raise Exception("User token is not defined. First log into SciServer.")
