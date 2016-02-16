#Python v3.4

from base64 import b64encode
import json
import urllib.request
import numpy
from io import BytesIO
import os

import SciServer.Session
import SciServer.Config
import SciServer.Keystone

class WebDAVUser:
    userName = "UserName"
    password = "Password"

def getWebDAVUser():
	"""For some reason I wrote the owncloud app in such a way that the userid needs to be passed as the password."""

	token = SciServer.Session.getKeystoneToken()

	ksu = SciServer.Keystone.getKeystoneUserWithToken(token)

	wdu = WebDAVUser()

	wdu.userName = ksu.userName
	#wdu.password = token
	wdu.password = ksu.id

	return wdu

def openSharedFile(filePath):
	"""Returns a python file object to a file which has been shared with the logged in user."""

	formatedPath = getLocalSharedFilePath(filePath)

	return open(formatedPath)

def getLocalSharedFilePath(filePath):
	"""Returns the path to a file which has been shared with the logged in user.
	This should probably not be exposed to the user."""

	user = getWebDAVUser()

	urlToFile = SciServer.Config.WebDAVServerUri + SciServer.Config.WebDAVUserRESTPath + "/LocalFile/" + filePath

	req = urllib.request.Request(urlToFile,method='GET')

	req.add_header('X-Auth-Token', SciServer.Session.getKeystoneToken())

	#print(urlToFile, SciServer.Session.getKeystoneToken() )

	response = urllib.request.urlopen(req)

	print("getScratchFile response: ", response.status, response.reason)

	formatedPath = response.read().decode()

	formatedPath = formatedPath.strip("\"").replace("//", "/").replace("\\\\", "/").replace("\\/", "/")

	return formatedPath

def getHomeDirectory():
	"""Returns the users home file directory.
	It adds "/file" path to what is returned from the restful call to Owncloud."""

	user = getWebDAVUser()

	urlToFile = SciServer.Config.WebDAVServerUri + SciServer.Config.WebDAVUserRESTPath + "/home/" + user.userName

	req = urllib.request.Request(urlToFile)
	try:
	
		response = urllib.request.urlopen(req)
		
	except urllib.error.HTTPError as e:
		print(e.code)
		print(e.read().decode())
		return False

	print("getScratchFile response: ", response.status, response.reason)

	formatedPath = response.read().decode()

	formatedPath = formatedPath.strip("\"").replace("//", "/").replace("\\\\", "/").replace("\\/", "/") + "/files"

	return formatedPath

def getPROPFINDReaderForFile(filePath):

  return getPROPFINDReader(filePath, depth = 0)

def getPROPFINDReaderForDirectory(directoryPath):

  return getPROPFINDReader(directoryPath, depth = 1)

def getPROPFINDReader(path, depth = 0):

  user = getWebDAVUser()
  
  userAndPass = user.userName + ":" + user.password
  userAndPassb64 = b64encode(userAndPass.encode('ascii')).decode('ascii')

  urlToFile = SciServer.Config.WebDAVServerUri + SciServer.Config.WebDAVRESTPath + "/" + path
  req = urllib.request.Request(urlToFile,method='PROPFIND')
  
  req.add_header('Authorization', 'Basic %s' %  userAndPassb64)
  req.add_header('Depth', depth)

  response = urllib.request.urlopen(req)

  print("PROPFIND PUT response: ", response.status, response.reason)

  return response

def getReaderToScratchFile(fileName):
	"""Uses the WEBDAV interface to get a reader to a user's file."""

	user = getWebDAVUser()

	userAndPass = user.userName + ":" + user.password
	userAndPassb64 = b64encode(userAndPass.encode('ascii')).decode('ascii')

	urlToFile = SciServer.Config.WebDAVServerUri + SciServer.Config.WebDAVRESTPath + "/" + fileName

	req = urllib.request.Request(urlToFile)

	req.add_header('Authorization', 'Basic %s' %  userAndPassb64)
	req.add_header('Content-Type', 'application/octet-stream')

	print("Getting stream to file", fileName)

	f = urllib.request.urlopen(req)

	print("getReaderFromScratchFile PUT response: ", f.status, f.reason)

	return f

def writeToScratchFile(fileName, data, contentLength = 0):
	"""If contentLength is not given then data must support seek() and tell()."""
	if(contentLength == 0):
		data.seek(0, os.SEEK_END)
		contentLength = data.tell()
		data.seek(0)

	user = getWebDAVUser()

	userAndPass = user.userName + ":" + user.password
	userAndPassb64 = b64encode(userAndPass.encode('ascii')).decode('ascii')

	urlToFile = SciServer.Config.WebDAVServerUri + SciServer.Config.WebDAVRESTPath + "/" + fileName
	#print(urlToFile)
	req = urllib.request.Request(urlToFile, data=data,method='PUT')

	req.add_header('Authorization', 'Basic %s' %  userAndPassb64)
	req.add_header('Content-Length', '%i' % contentLength)
	req.add_header('Content-Type', 'application/octet-stream')

	putResponse = urllib.request.urlopen(req)

	print("writeToScratchFile PUT response: ", putResponse.status, putResponse.reason)

def info(object):
	"""Print methods and doc strings.
	Takes module, class, list, dictionary, or string."""

	methodList = [method for method in dir(object)]
	print(methodList)
	print(type(object))

