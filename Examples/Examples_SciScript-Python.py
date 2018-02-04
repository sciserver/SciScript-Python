
# coding: utf-8

# In[ ]:

import SciServer
from SciServer import Authentication, LoginPortal, Config, CasJobs, SkyQuery, SciDrive, SkyServer, Files, Jobs
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

Authentication_login_sharedWithName = '***'
Authentication_login_sharedWithPassword = '***'


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

os.remove(CasJobs_TestFitsFile)


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

result4 = CasJobs.executeQuery(sql="DROP TABLE " + CasJobs_TestTableName2, context="MyDB", format="pandas")
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

#list content and metadata of top level directory in SciDrive

dirList = SciDrive.directoryList("")
print(dirList)


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

#list content and metadata of directory in SciDrive

dirList = SciDrive.directoryList(SciDrive_Directory)
print(dirList)


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

os.remove(SciDrive_FilePath)


# In[ ]:

# *******************************************************************************************************
# SkyQuery section:
# *******************************************************************************************************


# In[ ]:

#help(SkyQuery)


# In[ ]:

token1 = Authentication.login(Authentication_loginName, Authentication_loginPassword);


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

# get list of jobs

quickJobsList = SkyQuery.listJobs('quick')
longJobsList = SkyQuery.listJobs('long')
print(quickJobsList)
print(longJobsList)


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


# In[ ]:

# *******************************************************************************************************
# Files section:
# *******************************************************************************************************


# In[ ]:

#help(Files)


# In[ ]:

# defining the FileService name

Files_FileServiceName = "FileServiceJHU"
Files_RootVolumeName1 = "volumes"
Files_UserVolumeName1 = Authentication_loginName + "_UserVolume555"
Files_RootVolumeName2 = "volumes"
Files_UserVolumeName2 = Authentication_loginName + "_UserVolume999"
Files_NewDirectoryName1 = "myNewDirectory555"
Files_NewDirectoryName2 = "myNewDirectory999"
Files_LocalFileName = "MyNewFile.txt"
Files_LocalFileContent = "#ID,Column1,Column2\n1,4.5,5.5"


# In[ ]:

token1 = Authentication.login(Authentication_loginName, Authentication_loginPassword);


# In[ ]:

# get available FileServices

fileServices = Files.getFileServices();
print(fileServices)


# In[ ]:

# get names of file services available to the user.

fileServiceNames = Files.getFileServicesNames();
print(fileServiceNames)


# In[ ]:

# get FileService from Name

fileService = Files.getFileServiceFromName(Files_FileServiceName);
print(fileService)


# In[ ]:

# get the API endpoint URL of a FileService

fileServiceAPIUrl = Files.__getFileServiceAPIUrl(fileService);
print(fileServiceAPIUrl)


# In[ ]:

# get root volumes

rootVolumes = Files.getRootVolumes(fileService)
print(rootVolumes)


# In[ ]:

# create user volume

Files.createUserVolume(fileService, Files_RootVolumeName1, Files_UserVolumeName1)
Files.createUserVolume(fileService, Files_RootVolumeName1, Files_UserVolumeName2)


# In[ ]:

# create a directory in the persistent volume

Files.createDir(fileService, Files_RootVolumeName1, Files_UserVolumeName1, Files_NewDirectoryName1);
Files.createDir(fileService, Files_RootVolumeName2, Files_UserVolumeName2, Files_NewDirectoryName2);


# In[ ]:

# upload a text string into a file in the remote directory

Files.upload(fileService, Files_RootVolumeName1, Files_UserVolumeName1, 
             Files_NewDirectoryName1 + "/" + Files_LocalFileName, 
             data=Files_LocalFileContent);


# In[ ]:

# List content of remote directory

dirList = Files.dirList(fileService, Files_RootVolumeName1, Files_UserVolumeName1, Files_NewDirectoryName1, level=2)
print(dirList)


# In[ ]:

# download a remote text file into a local directory

Files.download(fileService, Files_RootVolumeName1, Files_UserVolumeName1, 
               Files_NewDirectoryName1 + "/" + Files_LocalFileName, 
               localFilePath=Files_LocalFileName);


# In[ ]:

# Delete remote file

Files.delete(fileService, Files_RootVolumeName1, Files_UserVolumeName1, Files_NewDirectoryName1 + "/" + Files_LocalFileName)


# In[ ]:

# upload a local file into a remote directory

Files.upload(fileService, Files_RootVolumeName1, Files_UserVolumeName1,
             Files_NewDirectoryName1 + "/" + Files_LocalFileName, localFilePath=Files_LocalFileName, quiet=False);


# In[ ]:

# copy remote file into remote directory

Files.move(fileService, Files_RootVolumeName1, Files_UserVolumeName1,Files_NewDirectoryName1 + "/" + Files_LocalFileName, 
           fileService, Files_RootVolumeName2, Files_UserVolumeName2,Files_UserVolumeName2 + "/" + Files_NewDirectoryName2 + "/" + Files_LocalFileName
          );


# In[ ]:

# sharing user volume with another user

Files.shareUserVolume(fileService, Files_RootVolumeName2, Files_UserVolumeName2, 
                      sharedWith=Authentication_login_sharedWithName, type="USER", 
                      userVolumeOwner=Authentication_loginName,
                      allowedActions=["read"])


# In[ ]:

# let the other user log-in

token1 = Authentication.login(Authentication_login_sharedWithName, Authentication_login_sharedWithPassword);


# In[ ]:

# the other user downloads the remote text file (from the shared user volume) into a local string variable

string = Files.download(fileService, Files_RootVolumeName2, Files_UserVolumeName2, 
                        Files_NewDirectoryName2 + "/" + Files_LocalFileName, 
                        format="txt", userVolumeOwner=Authentication_loginName);
print(string)


# In[ ]:

# the original user logs in again

token1 = Authentication.login(Authentication_loginName, Authentication_loginPassword);


# In[ ]:

# delete folders

Files.delete(fileService, Files_RootVolumeName1, Files_UserVolumeName1, Files_NewDirectoryName1)
Files.delete(fileService, Files_RootVolumeName2, Files_UserVolumeName2, Files_NewDirectoryName2)


# In[ ]:

# delete user volumes

Files.deleteUserVolume(fileService, Files_RootVolumeName1, Files_UserVolumeName1)
Files.deleteUserVolume(fileService, Files_RootVolumeName1, Files_UserVolumeName2)


# In[ ]:

# *******************************************************************************************************
# Jobs section:
# *******************************************************************************************************


# In[ ]:

#help(Jobs)


# In[ ]:

Jobs_DockerComputeDomainName = 'Small Jobs Batch Domain'

Jobs_FileServiceName = "FileServiceJHU"
Jobs_RootVolumeName = "volumes"
Jobs_UserVolumeName = Authentication_loginName + "_JobsTestVolume"
Jobs_DirectoryName = "JobsTestDirectory"
Jobs_NotebookName = 'TestNotebook.ipynb'
Jobs_ShellCommand = 'ls -laht'
Jobs_DockerImageName = 'Python (astro)'

Jobs_UserVolumes = [{'name':'persistent'},{'name':'scratch', 'needsWriteAccess':True}]
Jobs_DataVolumes = [{'name':'SDSS_DAS'}]
Jobs_Parameters =  'param1=1\nparam2=2\nparam3=3'
Jobs_Alias = 'MyNewJob'

Jobs_SqlQuery = "select 1;"
Jobs_RBBComputeDomainName = 'Manga (long)'
Jobs_DatabaseContextName = "manga"
Jobs_QueryResultsFile = 'myQueryResults'


# In[ ]:

# get docker compute domains

dockerComputeDomains = Jobs.getDockerComputeDomains();
print(dockerComputeDomains)


# In[ ]:

# get names of docker compute domains available to the user

dockerComputeDomainsNames = Jobs.getDockerComputeDomainsNames()
print(dockerComputeDomainsNames)


# In[ ]:

# get docker compute domain from name

dockerComputeDomain = Jobs.getDockerComputeDomainFromName(Jobs_DockerComputeDomainName)
print(dockerComputeDomain)


# In[ ]:

# uploading Jupyter notebook to remote directory

fileService = Files.getFileServiceFromName(Jobs_FileServiceName);
Files.createUserVolume(fileService, Jobs_RootVolumeName, Jobs_UserVolumeName)
Files.upload(fileService, Jobs_RootVolumeName, Jobs_UserVolumeName, 
             Jobs_DirectoryName + "/" + Jobs_NotebookName, 
             localFilePath=Jobs_NotebookName);


# In[ ]:

# submit a Jupyter notebook job.

jobId = Jobs.submitNotebookJob('/home/idies/workspace/' + Jobs_UserVolumeName + '/' + Jobs_DirectoryName + '/' + Jobs_NotebookName,
                             dockerComputeDomain,
                             Jobs_DockerImageName, 
                             Jobs_UserVolumes, Jobs_DataVolumes,
                             Jobs_Alias)
print(jobId)


# In[ ]:

# get job status

jobStatus = Jobs.getJobStatus(jobId)
print(jobStatus)


# In[ ]:

# get job description

job = Jobs.getJobDescription(jobId)
print(job)


# In[ ]:

# wait until job is finsihed and get job status

jobId = Jobs.submitNotebookJob('/home/idies/workspace/' + Jobs_UserVolumeName + '/' + Jobs_DirectoryName + '/' + Jobs_NotebookName,
                             dockerComputeDomain,
                             Jobs_DockerImageName, 
                             Jobs_UserVolumes, Jobs_DataVolumes,
                             Jobs_Alias)
jobStatus = Jobs.waitForJob(jobId)
print(jobStatus)


# In[ ]:

# cancel job

jobId = Jobs.submitNotebookJob('/home/idies/workspace/' + Jobs_UserVolumeName + '/' + Jobs_DirectoryName + '/' + Jobs_NotebookName,
                             dockerComputeDomain,
                             Jobs_DockerImageName, 
                             Jobs_UserVolumes, Jobs_DataVolumes,
                             Jobs_Alias)
Jobs.cancelJob(jobId)


# In[ ]:

Files.deleteUserVolume(fileService, Jobs_RootVolumeName, Jobs_UserVolumeName, quiet=True)


# In[ ]:

# submit shell command job

jobId = Jobs.submitShellCommandJob(Jobs_ShellCommand,
                                    dockerComputeDomain,
                                    Jobs_DockerImageName,
                                    Jobs_UserVolumes, Jobs_DataVolumes,
                                    Jobs_Alias)


# In[ ]:

# get relational database (RDB) compute domains

rdbComputeDomains = Jobs.getRDBComputeDomains();
print(rdbComputeDomains)


# In[ ]:

# get names of relational database (RDB) compute domains

rdbComputeDomainsNames = Jobs.getRDBComputeDomainsNames()
print(rdbComputeDomainsNames)


# In[ ]:

# get relational database (RDB) compute domain from name

rdbComputeDomain = Jobs.getRDBComputeDomainFromName(Jobs_RBBComputeDomainName);
print(rdbComputeDomain)


# In[ ]:

# submit RDB (relatinal database) query job

jobId = Jobs.submitRDBQueryJob(Jobs_SqlQuery, rdbComputeDomain, Jobs_DatabaseContextName, Jobs_QueryResultsFile, Jobs_Alias)
print(jobId)

