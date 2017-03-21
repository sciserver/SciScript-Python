
# coding: utf-8

# In[ ]:

import SciServer
from SciServer import Authentication, LoginPortal, Config, CasJobs, SkyQuery, SciDrive, SkyServer
import os;
import pandas;
import sys;
import json;
from io import StringIO
from io import BytesIO
#from PIL import Image
import numpy as np
import matplotlib.pyplot as plt

# Define login Name and password before running these examples
Authentication_loginName = '***';
Authentication_loginPassword = '***'


# In[ ]:

#help(SciServer)


# In[ ]:

# *******************************************************************************************************
# Authentication section
# *******************************************************************************************************


# In[ ]:

#help(Authentication)


# In[ ]:

#logging in and getting current token from different ways

token1 = Authentication.login(Authentication_loginName, Authentication_loginPassword);
token2 = Authentication.getToken()
token3 = Authentication.getKeystoneToken()
token4 = Authentication.token.value
print("token1=" + token1)#
print("token2=" + token2)#
print("token3=" + token3)#
print("token4=" + token4)#


# In[ ]:

#getting curent user info

user = Authentication.getKeystoneUserWithToken(token1)
print("userName=" + user.userName)
print("id=" + user.id)
iden = Authentication.identArgIdentifier()
print("ident="+iden)


# In[ ]:

#reseting the current token to another value:

Authentication.setToken("myToken1")
token5 = Authentication.getToken()
Authentication.setKeystoneToken("myToken2")
token6 = Authentication.getKeystoneToken()

print("token5=" + token5)
print("token6=" + token6)


# In[ ]:

#logging-in again

token1 = Authentication.login(Authentication_loginName, Authentication_loginPassword);


# In[ ]:

# *******************************************************************************************************
# LoginPortal section:
# *******************************************************************************************************


# In[ ]:

#help(LoginPortal)


# In[ ]:

#logging in and getting current token from different ways

token1 = LoginPortal.login(Authentication_loginName, Authentication_loginPassword);
token2 = LoginPortal.getToken()
token3 = LoginPortal.getKeystoneToken()
print("token1=" + token1)#
print("token2=" + token2)#
print("token3=" + token3)#


# In[ ]:

#getting curent user info

user = LoginPortal.getKeystoneUserWithToken(token1)
print("userName=" + user.userName)
print("id=" + user.id)#
iden = LoginPortal.identArgIdentifier()
print("ident="+iden)#


# In[ ]:

#reseting the current token to another value:

LoginPortal.setKeystoneToken("myToken2")
token6 = LoginPortal.getKeystoneToken()
print("token6=" + token6)


# In[ ]:

#logging-in again

token1 = LoginPortal.login(Authentication_loginName, Authentication_loginPassword);


# In[ ]:

# *******************************************************************************************************
# CasJobs section:
# *******************************************************************************************************


# In[ ]:

#help(CasJobs)


# In[ ]:

#Defining databse context and query, and other variables

CasJobs_TestDatabase = "MyDB"
CasJobs_TestQuery = "select 4 as Column1, 5 as Column2 "
CasJobs_TestTableName1 = "MyNewtable1"
CasJobs_TestTableName2 = "MyNewtable2"
CasJobs_TestTableCSV = u"Column1,Column2\n4,5\n"
CasJobs_TestFitsFile = "SciScriptTestFile.fits"
CasJobs_TestCSVFile = "SciScriptTestFile.csv"


# In[ ]:

#get user schema info

casJobsId = CasJobs.getSchemaName()
print(casJobsId)


# In[ ]:

#get info about tables inside MyDB database context:

tables = CasJobs.getTables(context="MyDB")
print(tables)


# In[ ]:

#execute a quick SQL query:

df = CasJobs.executeQuery(sql=CasJobs_TestQuery, context=CasJobs_TestDatabase, format="pandas")
print(df)


# In[ ]:

#submit a job, which inserts the query results into a table in the MyDB database context. 
#Wait until the job is done and get its status.

jobId = CasJobs.submitJob(sql=CasJobs_TestQuery + " into MyDB." + CasJobs_TestTableName1, context="MyDB")
jobDescription = CasJobs.waitForJob(jobId=jobId, verbose=False)
print(jobId)
print(jobDescription)


# In[ ]:

# drop or delete table in MyDB database context

df = CasJobs.executeQuery(sql="DROP TABLE " + CasJobs_TestTableName1, context="MyDB", format="pandas")
print(df)


# In[ ]:

#get job status

jobId = CasJobs.submitJob(sql=CasJobs_TestQuery, context=CasJobs_TestDatabase)
jobDescription = CasJobs.getJobStatus(jobId)
print(jobId)
print(jobDescription)


# In[ ]:

#cancel a job

jobId = CasJobs.submitJob(sql=CasJobs_TestQuery, context=CasJobs_TestDatabase)
jobDescription = CasJobs.cancelJob(jobId=jobId)
print(jobId)
print(jobDescription)


# In[ ]:

#execute a query and write a local Fits file containing the query results:

result = CasJobs.writeFitsFileFromQuery(fileName=CasJobs_TestFitsFile, queryString=CasJobs_TestQuery, context="MyDB")
print(result)


# In[ ]:

#delete local FITS file just created:

os.system("rm " + CasJobs_TestFitsFile)


# In[ ]:

#get a Pandas dataframe containing the results of a query

df = CasJobs.getPandasDataFrameFromQuery(queryString=CasJobs_TestQuery, context=CasJobs_TestDatabase)
print(df)


# In[ ]:

# get numpy array containing the results of a query

array = CasJobs.getNumpyArrayFromQuery(queryString=CasJobs_TestQuery, context=CasJobs_TestDatabase)
print(array)


# In[ ]:

#uploads a Pandas dataframe into a Database table

df = pandas.read_csv(StringIO(CasJobs_TestTableCSV), index_col=None)
result = CasJobs.uploadPandasDataFrameToTable(dataFrame=df, tableName=CasJobs_TestTableName2, context="MyDB")
table = CasJobs.executeQuery(sql="select * from " + CasJobs_TestTableName2, context="MyDB", format="pandas")
print(result)
print(table)


# In[ ]:

# drop or delete table just created:

result2 = CasJobs.executeQuery(sql="DROP TABLE " + CasJobs_TestTableName2, context=CasJobs_TestDatabase, format="pandas")
print(result2)


# In[ ]:

#upload csv data string into a database table:

result3 = CasJobs.uploadCSVDataToTable(csvData=CasJobs_TestTableCSV, tableName=CasJobs_TestTableName2, context="MyDB")
df2 = CasJobs.executeQuery(sql="select * from " + CasJobs_TestTableName2, context="MyDB", format="pandas")
print(result3)
print(df2)


# In[ ]:

# drop or delete table just created:

result4 = CasJobs.executeQuery(sql="DROP TABLE " + CasJobs_TestTableName2, context=CasJobs_TestDatabase, format="pandas")
print(result4)


# In[ ]:

# *******************************************************************************************************
#  SkyServer section:
# *******************************************************************************************************


# In[ ]:

#help(SkyServer)


# In[ ]:

#defining sql query and SDSS data relelease:

SkyServer_TestQuery = "select top 1 specobjid, ra, dec from specobj order by specobjid"
SkyServer_DataRelease = "DR13"


# In[ ]:

#Exectute sql query

df = SkyServer.sqlSearch(sql=SkyServer_TestQuery, dataRelease=SkyServer_DataRelease)
print(df)


# In[ ]:

#get an image cutout

img = SkyServer.getJpegImgCutout(ra=197.614455642896, dec=18.438168853724, width=2, height=2, scale=0.4, 
                                 dataRelease=SkyServer_DataRelease,opt="OG",
                                 query="SELECT TOP 100 p.objID, p.ra, p.dec, p.r FROM fGetObjFromRectEq(197.6,18.4,197.7,18.5) n, PhotoPrimary p WHERE n.objID=p.objID")
plt.imshow(img)


# In[ ]:

# do a radial search of objects:

df = SkyServer.radialSearch(ra=258.25, dec=64.05, radius=0.1, dataRelease=SkyServer_DataRelease)
print(df)


# In[ ]:

#do rectangular search of objects:

df = SkyServer.rectangularSearch(min_ra=258.3, max_ra=258.31, min_dec=64,max_dec=64.01, dataRelease=SkyServer_DataRelease)
print(df)


# In[ ]:

#do an object search based on RA,Dec coordinates:

object = SkyServer.objectSearch(ra=258.25, dec=64.05, dataRelease=SkyServer_DataRelease)
print(object)


# In[ ]:

# *******************************************************************************************************
# SciDrive section:
# *******************************************************************************************************


# In[ ]:

#help(SciDrive)


# In[ ]:

#define name of directory to be created in SciDrive:
SciDrive_Directory = "SciScriptPython"
#define name, path and content of a file to be first created and then uploaded into SciDrive:
SciDrive_FileName = "TestFile.csv"
SciDrive_FilePath = "./TestFile.csv"
SciDrive_FileContent = "Column1,Column2\n4.5,5.5\n"


# In[ ]:

#create a folder or container in SciDrive

responseCreate = SciDrive.createContainer(SciDrive_Directory)
print(responseCreate)


# In[ ]:

#get the public url to access the directory content in SciDrive

url = SciDrive.publicUrl(SciDrive_Directory)
print(url)


# In[ ]:

#Delete folder or container in SciDrive:

responseDelete = SciDrive.delete(SciDrive_Directory)
print(responseDelete)


# In[ ]:

#create a local file:

file = open(SciDrive_FileName, "w")
file.write(SciDrive_FileContent)
file.close()


# In[ ]:

#uploading a file to SciDrive:

responseUpload = SciDrive.upload(path=SciDrive_Directory + "/" + SciDrive_FileName, localFilePath=SciDrive_FilePath)
print(responseUpload)


# In[ ]:

#download file:

stringio = SciDrive.download(path=SciDrive_Directory + "/" + SciDrive_FileName, format="StringIO")
fileContent = stringio.read()
print(fileContent)


# In[ ]:

#upload string data:

responseUpload = SciDrive.upload(path=SciDrive_Directory + "/" + SciDrive_FileName, data=SciDrive_FileContent)
fileContent = SciDrive.download(path=SciDrive_Directory + "/" + SciDrive_FileName, format="text")
print(fileContent)


# In[ ]:

#delete folder in SciDrive:

responseDelete = SciDrive.delete(SciDrive_Directory)
print(responseDelete)


# In[ ]:

#delete local file:

os.system("rm " + SciDrive_FilePath)


# In[ ]:

# *******************************************************************************************************
# SkyQuery section:
# *******************************************************************************************************


# In[ ]:

#help(SkyQuery)


# In[ ]:

#list all databses or datasets available

datasets = SkyQuery.listAllDatasets()
print(datasets)


# In[ ]:

#get info about the user's personal database or dataset

info = SkyQuery.getDatasetInfo("MyDB")
print(info)


# In[ ]:

#list tables inside dataset

tables = SkyQuery.listDatasetTables("MyDB")
print(tables)


# In[ ]:

#list available job queues

queueList = SkyQuery.listQueues()
print(queueList)


# In[ ]:

#list available job queues and related info

quick = SkyQuery.getQueueInfo('quick')
long= SkyQuery.getQueueInfo('long')
print(quick)
print(long)


# In[ ]:

#Define query

SkyQuery_Query = "select 4.5 as Column1, 5.5 as Column2"


# In[ ]:

#submit a query as a job

jobId = SkyQuery.submitJob(query=SkyQuery_Query, queue="quick")
print(jobId)


# In[ ]:

#get status of a submitted job

jobId = SkyQuery.submitJob(query=SkyQuery_Query, queue="quick")
jobDescription = SkyQuery.getJobStatus(jobId=jobId)
print(jobDescription)


# In[ ]:

# wait for a job to be finished and then get the status

jobId = SkyQuery.submitJob(query=SkyQuery_Query, queue="quick")
jobDescription = SkyQuery.waitForJob(jobId=jobId, verbose=True)
print("jobDescription=")
print(jobDescription)


# In[ ]:

# cancel a job that is running, and then get its status

jobId = SkyQuery.submitJob(query=SkyQuery_Query, queue="long")
isCanceled = SkyQuery.cancelJob(jobId)
print(isCanceled)
print("job status:")
print(SkyQuery.getJobStatus(jobId=jobId))


# In[ ]:

#define csv table to be uploaded to into MyDB in SkyQuery

SkyQuery_TestTableName = "TestTable_SciScript_R"
SkyQuery_TestTableCSV = u"Column1,Column2\n4.5,5.5\n"


# In[ ]:

#uploading the csv table:

result = SkyQuery.uploadTable(uploadData=SkyQuery_TestTableCSV, tableName=SkyQuery_TestTableName, datasetName="MyDB", format="csv")
print(result)


# In[ ]:

#downloading table:

table = SkyQuery.getTable(tableName=SkyQuery_TestTableName, datasetName="MyDB", top=10)
print(table)


# In[ ]:

#list tables inside dataset

tables = SkyQuery.listDatasetTables("MyDB")
print(tables)


# In[ ]:

#get dataset table info:

info = SkyQuery.getTableInfo(tableName="webuser." + SkyQuery_TestTableName, datasetName="MyDB")
print(info)


# In[ ]:

#get dataset table columns info

columns = SkyQuery.listTableColumns(tableName="webuser." + SkyQuery_TestTableName, datasetName="MyDB")
print(columns)


# In[ ]:

#drop (or delete) table from dataset.

result = SkyQuery.dropTable(tableName=SkyQuery_TestTableName, datasetName="MyDB");
print(result)

