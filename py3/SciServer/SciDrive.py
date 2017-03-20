import json
from io import StringIO
from io import BytesIO

import requests as requests

from SciServer import Config, Authentication


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
        url = Config.SciDriveHost + '/vospace-2.0/nodes/' + path
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
        url = Config.SciDriveHost + '/vospace-2.0/1/files_put/dropbox/' + path
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

        url = Config.SciDriveHost + '/vospace-2.0/1/media/sandbox/' + str(path)
        headers = {'X-Auth-Token': token}
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            raise Exception("Error when getting the public URL of SciDrive file " + str(path) + ".\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());

        jsonRes = json.loads(res.content.decode())
        fileUrl = jsonRes["url"]
        return (fileUrl);

    else:
        raise Exception("User token is not defined. First log into SciServer.")


def download(path, format="text"):
    """
    Downloads the file identified by the path as a read()-able stream, i.e., to get contents call read() on the resulting object.

    :param path: path of the file (or directory) in SciDrive.
    :param format: type of the returned object. Can be "StringIO" (io.StringIO object containing readable text), "BytesIO" (io.BytesIO object containing readable binary data), or "text" (a text string).
    :return: object of type io.StringIO, io.BytesIO or string.
    :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that purpose). Throws an exception if the HTTP request to the SciDrive API returns an error.
    :example: csvString = SciDrive.download("path/to/SciDrive/file.csv", format="text");

    .. seealso:: SciDrive.upload
    """
    token = Authentication.getToken()
    if token is not None and token != "":

        fileUrl = publicUrl(path)
        res = requests.get(fileUrl, stream=True)
        if res.status_code != 200:
            raise Exception("Error when downloading SciDrive file " + str(path) + ".\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());

        if format == "StringIO":
            return StringIO(res.content.decode())
        if format == "text":
                return res.content.decode()
        elif format == "BytesIO":
            return BytesIO(res.content)
        else:
            raise Exception("Unknown format '" + format + "' when trying to download SciDrive file " + str(path) + ".\n");

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
        url = Config.SciDriveHost + '/vospace-2.0/nodes/' + path
        data = str.encode(containerBody)
        headers = {'X-Auth-Token': token, 'Content-Type': 'application/xml'}
        res = requests.delete(url, data=data, headers=headers)
        if res.status_code < 200 or res.status_code >= 300:
            raise Exception("Error when deleting " + str(path) + " in SciDrive.\nHttp Response from SciDrive API returned status code " + str(res.status_code) + ":\n" + res.content.decode());

        return True
    else:
        raise Exception("User token is not defined. First log into SciServer.")
