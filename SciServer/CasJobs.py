
from base64 import b64encode
import json
import urllib.request
import pandas
import numpy
from io import BytesIO
import os
import time
import sys

import SciServer.Session
import SciServer.Config
import SciServer.Keystone

def getSchemaName(token = ""):
  if (token == ""):
    userToken = SciServer.Session.getKeystoneToken()
  else:
    userToken = token;
  keystoneUserId = SciServer.Keystone.getKeystoneUserWithToken(userToken).id
  usersUrl = SciServer.Config.CasJobsRESTUri + "/users/" + keystoneUserId
  req = urllib.request.Request(usersUrl, method='Get')
  req.add_header('X-Auth-Token', userToken)
  req.add_header('Content-Type', 'application/json')
  getResponse = urllib.request.urlopen(req)
  jsonResponse = json.loads(getResponse.read().decode())
  return "wsid_" + str(jsonResponse["WebServicesId"])
  
def getTables(context = "MyDB"):

  """Returns all the tables from the current user in the given context.
  The result is a json object with format [{"Date":seconds,"Name":"TableName","Rows":int,"Size",int},..]"""

  TablesUrl = SciServer.Config.CasJobsRESTUri + "/contexts/" + context + "/Tables"
  
  req = urllib.request.Request(TablesUrl, method='Get')
  req.add_header('X-Auth-Token', SciServer.Session.getKeystoneToken())
  req.add_header('Content-Type', 'application/json')

  getResponse = urllib.request.urlopen(req)
#  print("getTables GET response: ", getResponse.status, getResponse.reason)

  jsonResponse = json.loads(getResponse.read().decode())
  
  return jsonResponse

def executeQuery(queryString, context = "MyDB", acceptHeader="text/plain", token=""):
	"""Executes a casjob query.  If a token is supplied then it will execute on behalf of the token's user.
	Returns a http.client.HTTPResponse (https://docs.python.org/3.4/library/http.client.html#httpresponse-objects) object."""

	QueryUrl = SciServer.Config.CasJobsRESTUri + "/contexts/" + context + "/query"

	query = {"Query":queryString}

	data = json.dumps(query).encode()

	req = urllib.request.Request(QueryUrl, data=data, method='POST')
	if(token==""):
	  req.add_header('X-Auth-Token', SciServer.Session.getKeystoneToken())
	else:
	  req.add_header('X-Auth-Token', token)

	req.add_header('Content-Type', 'application/json')
	req.add_header('Accept', acceptHeader)


	try:
		postResponse = urllib.request.urlopen(req)
#		print("executeQuery POST response: ", postResponse.getcode(), postResponse.reason)
		return postResponse
	except urllib.error.HTTPError as e:
#		print("executeQuery exception message: ", e, e.read().decode())
		return e

def submitJob(queryString, context = "MyDB", acceptHeader="text/plain", token=""):
	"""Submits a query to the casjobs queue.  If a token is supplied then it will execute on behalf of the token's user.
	Returns the casjobs jobID (int)."""
	
	QueryUrl = SciServer.Config.CasJobsRESTUri + "/contexts/" + context + "/jobs"

	query = {"Query":queryString}

	data = json.dumps(query).encode()

	req = urllib.request.Request(QueryUrl, data=data, method='PUT')
	req.add_header('X-Auth-Token', SciServer.Session.getKeystoneToken())
	req.add_header('Content-Type', 'application/json')
	req.add_header('Accept', acceptHeader)

	try:
		postResponse = urllib.request.urlopen(req)
#		print("submitJob PUT response: ", postResponse.getcode(), postResponse.reason)

		return int(postResponse.read())
	except urllib.error.HTTPError as e:
#		print("Exception message: ", e)
		return e

def getJobStatus(jobid):
	"""Gets a casjobs job status.
	Returns the dict object (https://docs.python.org/3.4/library/stdtypes.html#dict) coresponding to the json received from casjobs."""
	
	QueryUrl = SciServer.Config.CasJobsRESTUri + "/jobs/" + str(jobid)

	req = urllib.request.Request(QueryUrl, method='GET')
	req.add_header('X-Auth-Token', SciServer.Session.getKeystoneToken())
	req.add_header('Content-Type', 'application/json')

	try:
		postResponse = urllib.request.urlopen(req)

		return json.loads(postResponse.read().decode())
	except urllib.error.HTTPError as e:
#		print("Exception message: ", e)
		return e.code

def waitForJob(jobid):
	"""Waits for the casjobs job to return a status of 3, 4, or 5.
	Queries the job status from casjobs every 2 seconds."""

	complete = False

	waitingStr = "Waiting..."
	back="\b"*len(waitingStr)
	print(waitingStr, end="" )

	while not complete:
		print(back, end="")
		print(waitingStr, end="")
		jobDesc = getJobStatus(jobid)
		jobStatus = int(jobDesc["Status"])
		if jobStatus in (3,4,5):
			complete = True
#			print("Job ", jobid, " finished.")
		else:
			time.sleep(2)

		
	return jobDesc


def getFitsFileFromQuery(filename, queryString, context = "MyDB", token=""):
	"""Executes a casjobs query and writes the result to a fits (http://www.stsci.edu/institute/software_hardware/pyfits) file.
	Returns True if successful. """

	try:
		fitsResponse = executeQuery(queryString, context=context, acceptHeader="application/fits", token=token)

		theFile = open(filename, "w+b")
		theFile.write(fitsResponse.read())
		theFile.close()

		return True
	except Exception as e:
		print(e)
		return False
  

#make sure the index column is the fist column
def getPandasDataFrameFromQuery(queryString, context = "MyDB", token="", index_col=0):
	"""Executes a casjobs query and stores the cvs result in a pandas (http://pandas.pydata.org/pandas-docs/stable/) dataframe object.
	Returns the dataframe."""

	cvsResponse = executeQuery(queryString, context=context, token=token)

	#if the index column is not specified then it will add it's own column which causes problems when uploading the transformed data
	dataFrame = pandas.read_csv(cvsResponse, index_col=index_col)

	return dataFrame

def getNumpyArrayFromQuery(queryString, context = "MyDB", token=""):
	"""Executes a casjobs query and stores the cvs result in a numpy (http://docs.scipy.org/doc/numpy/) array.
	Returns numpy array."""

	dataFrame = getPandasDataFrameFromQuery(queryString, context, token)

	return dataFrame.as_matrix()

#require pandas for now but be able to take a string in the future
def uploadPandasDataFrameToTable(dataFrame, tableName, context = "MyDB", token=""):
	"""Uploads a pandas dataframe object into casjobs.
	Returns the output from casjobs in string form."""
  
	if dataFrame.index.name == "" or dataFrame.index.name is None:
		dataFrame.index.name = "index"
  
	sio = dataFrame.to_csv().encode("utf8")

	return uploadCVSDataToTable(sio, tableName, context, token)

def uploadCVSDataToTable(CVSdata, tableName, context="MyDB", token=""):
	"""Uploads  cvs data into casjobs.  data should support the buffered interface (it should have a read() method).
	https://docs.python.org/3/library/urllib.request.html
	Returns the output from casjobs in string form."""
  
	print("Uploading ",sys.getsizeof(CVSdata),"bytes...")
	tablesUrl = SciServer.Config.CasJobsRESTUri + "/contexts/" + context + "/Tables/" + tableName

	req = urllib.request.Request(tablesUrl, data=CVSdata, method='POST')
	if(token==""):
		req.add_header('X-Auth-Token', SciServer.Session.getKeystoneToken())
	else:
		req.add_header('X-Auth-Token', token)

	try:
		postResponse = urllib.request.urlopen(req)
		print("uploadCVSDataFrameToTable POST response: ", postResponse.status, postResponse.reason)

		return postResponse.read().decode()
	except urllib.error.HTTPError as error:
		print("There was a problem uploading the data. Exception message: ", error.read())
