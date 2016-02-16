import urllib.request
import json

import SciServer.Session
import SciServer.Config


def createContainer(path, token=""):
  if (token == ""):
    userToken = SciServer.Session.getKeystoneToken()
  else:
    userToken = token;
  
  containerBody = ('<vos:node xmlns:xsi="http://www.w3.org/2001/thisSchema-instance" ' 
                 'xsi:type="vos:ContainerNode" xmlns:vos="http://www.ivoa.net/xml/VOSpace/v2.0" ' 
                 'uri="vos://'+SciServer.Config.SciDriveHost+'!vospace/'+path+'">'
                 '<vos:properties/><vos:accepts/><vos:provides/><vos:capabilities/>'
                 '</vos:node>')
  req = urllib.request.Request(url=SciServer.Config.SciDriveHost+'/vospace-2.0/nodes/'+path,data=str.encode(containerBody),method='PUT')
  req.add_header('X-Auth-Token', userToken)
  req.add_header('Content-Type','application/xml')
  try:
    res = urllib.request.urlopen(req)
  except urllib.error.HTTPError as error:
    print(error,error.read().decode())


def upload(path, data, token=""):
  if (token == ""):
    userToken = SciServer.Session.getKeystoneToken()
  else:
    userToken = token;
  
  req = urllib.request.Request(url=SciServer.Config.SciDriveHost+'/vospace-2.0/1/files_put/dropbox/'+path,data=data,method='PUT')
  req.add_header('X-Auth-Token', userToken)
  try:
    res = urllib.request.urlopen(req)
    print(res.read().decode())
  except urllib.error.HTTPError as error:
    print(error,error.read().decode())
    raise

def download(path, token=""):
  if (token == ""):
    userToken = SciServer.Session.getKeystoneToken()
  else:
    userToken = token;
  
  req = urllib.request.Request(url=SciServer.Config.SciDriveHost+'/vospace-2.0/1/media/sandbox/'+path,method='GET')
  req.add_header('X-Auth-Token', userToken)
  try:
    res = urllib.request.urlopen(req)
  except urllib.error.HTTPError as error:
    print(error,error.read().decode())
    raise
  
  jsonRes = json.loads(res.read().decode())
  fileUrl = jsonRes["url"]
  req = urllib.request.Request(fileUrl)
  try:
    res = urllib.request.urlopen(req)
    return res.read()
  except urllib.error.HTTPError as error:
    print(error,error.read().decode())
    raise
  