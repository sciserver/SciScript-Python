import json
from io import StringIO

import requests

from SciServer import Config, LoginPortal


def createContainer(path, token=""):
    if (token == ""):
        userToken = LoginPortal.getToken()
    else:
        userToken = token;

    containerBody = ('<vos:node xmlns:xsi="http://www.w3.org/2001/thisSchema-instance" '
                     'xsi:type="vos:ContainerNode" xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0" '
                     'uri="vos://' + Config.SciDriveHost + '!vospace/' + path + '">'
                                                                                '<vos:properties/><vos:accepts/><vos:provides/><vos:capabilities/>'
                                                                                '</vos:node>')
    url = Config.SciDriveHost + '/vospace-2.0/nodes/' + path
    data = str.encode(containerBody)
    headers = {'X-Auth-Token': userToken, 'Content-Type': 'application/xml'}
    try:
        res = requests.put(url, data=data, headers=headers)
    except requests.exceptions.RequestException as error:
        print(error, error.read().decode())


def upload(path, data, token=""):
    if (token == ""):
        userToken = LoginPortal.getToken()
    else:
        userToken = token;

    url = Config.SciDriveHost + '/vospace-2.0/1/files_put/dropbox/' + path
    data = data
    headers = {'X-Auth-Token': userToken}
    try:
        res = requests.put(url, data=data, headers=headers)
        print(res.content.decode())
    except requests.exceptions.RequestException as error:
        print(error, error.read().decode())
        raise

def publicUrl(path, token):
    """
    retrieve public URL for file identified by path
    """
    if (token == ""):
        userToken = LoginPortal.getToken()
    else:
        userToken = token;

    url = Config.SciDriveHost + '/vospace-2.0/1/media/sandbox/' + path
    headers={'X-Auth-Token': userToken}
    try:
        res = requests.get(url, headers=headers)
    except requests.exceptions.RequestException as error:
        print(error, error.read().decode())
        raise

    jsonRes = json.loads(res.content.decode())
    fileUrl = jsonRes["url"]
    return fileUrl

def download(path, token=""):
    """
    Download the file identified by the path as a read()-able stream.
    I.e. to get contents call read() on the resulting object
    """
    fileUrl=publicUrl(path,token)
    try:
        res = requests.get(fileUrl,stream=True)
        return StringIO(res.content.decode())
    except requests.exceptions.RequestException as error:
        print(error, error.read().decode())
        raise
  