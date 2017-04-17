#!/usr/bin/python
from SciServer import Authentication, LoginPortal, Config, CasJobs, SkyServer, SkyQuery, SciDrive
import unittest2 as unittest
import os;
import pandas;
import sys;
import json;
from io import StringIO
from io import BytesIO
import skimage

# Define login Name and password before running the tests:
Authentication_loginName = '***';
Authentication_loginPassword = '***'


#skyserver
SkyServer_TestQuery = "select top 1 specobjid, ra, dec from specobj order by specobjid"
SkyServer_DataRelease = "DR13"
SkyServer_QueryResultCSV = "specobjid,ra,dec\n299489677444933632,146.71421,-1.0413043\n"
SkyServer_RadialSearchResultCSV = 'objid,run,rerun,camcol,field,obj,type,ra,dec,u,g,r,i,z,Err_u,Err_g,Err_r,Err_i,Err_z\n1237671939804561654,6162,301,3,133,246,3,258.250804,64.051445,23.339820,22.319400,21.411050,21.119710,20.842770,0.664019,0.116986,0.076410,0.080523,0.238198\n'
SkyServer_RectangularSearchResultCSV = 'objid,run,rerun,camcol,field,obj,type,ra,dec,u,g,r,i,z,Err_u,Err_g,Err_r,Err_i,Err_z\n1237671939804628290,6162,301,3,134,1346,6,258.304721,64.006203,25.000800,24.500570,22.485400,21.103450,20.149990,0.995208,0.565456,0.166184,0.071836,0.124986\n'
SkyServer_ObjectSearchResultObjID = 1237671939804561654

SkyQuery_TestTableName = "TestTable_SciScript_R"
SkyQuery_TestTableCSV = u"Column1,Column2\n4.5,5.5\n"
SkyQuery_TestTableCSVdownloaded = "#ID,Column1,Column2\n1,4.5,5.5\n"
SkyQuery_Query = "select 4.5 as Column1, 5.5 as Column2"

CasJobs_TestTableName1 = "MyNewtable1"
CasJobs_TestTableName2 = "MyNewtable2"
CasJobs_TestDatabase = "MyDB"
CasJobs_TestQuery = "select 4 as Column1, 5 as Column2 "
CasJobs_TestTableCSV = u"Column1,Column2\n4,5\n"
CasJobs_TestFitsFile = "SciScriptTestFile.fits"
CasJobs_TestCSVFile = "SciScriptTestFile.csv"

SciDrive_Directory = "/SciScriptPython"
SciDrive_FileName = "TestFile.csv"
SciDrive_FileContent = "Column1,Column2\n4.5,5.5\n"


#class TestVariables:
#    SkyQuery_jobId = None;
#    def __init__(self):
#        pass


class TestAuthentication(unittest.TestCase):


    def setUp(self):
        pass

    # *******************************************************************************************************
    # Authentication section

    def test_Authentication_allMethods(self):


        newToken1 = "myToken1"
        newToken2 = "myToken2"

        token1 = Authentication.login(Authentication_loginName, Authentication_loginPassword);
        token2 = Authentication.getToken()
        token3 = Authentication.getKeystoneToken()
        token4 = Authentication.token.value
        user = Authentication.getKeystoneUserWithToken(token1)
        iden = Authentication.identArgIdentifier()

        self.assertEqual(iden, "--ident=")

        self.assertNotEqual(token1, "")
        self.assertIsNot(token1, None)
        self.assertEqual(token1, token2)
        self.assertEqual(token1, token3)
        self.assertEqual(token1, token4)

        self.assertEqual(user.userName, Authentication_loginName)
        self.assertIsNot(user.id, None)
        self.assertNotEqual(user.id, "")

        Authentication.setToken(newToken1)
        self.assertEqual(newToken1, Authentication.getToken())
        Authentication.setKeystoneToken(newToken2)
        self.assertEqual(newToken2, Authentication.getKeystoneToken())


class TestLoginPortal(unittest.TestCase):

    def setUp(self):
        pass

        # *******************************************************************************************************
        # Authentication section

    def test_LoginPortal_allMethods(self):


        newToken1 = "myToken1"
        newToken2 = "myToken2"

        token1 = LoginPortal.login(Authentication_loginName, Authentication_loginPassword);
        token2 = LoginPortal.getToken()
        token3 = LoginPortal.getKeystoneToken()
        user = LoginPortal.getKeystoneUserWithToken(token1)
        iden = LoginPortal.identArgIdentifier()

        self.assertEqual(iden, "--ident=")

        self.assertNotEqual(token1, "")
        self.assertIsNot(token1, None)
        self.assertEqual(token1, token2)
        self.assertEqual(token1, token3)

        self.assertIsNot(user.userName, None)
        self.assertNotEqual(user.userName, "")
        self.assertIsNot(user.id, None)
        self.assertNotEqual(user.id, "")

        LoginPortal.setKeystoneToken(newToken1)
        self.assertEqual(newToken1, LoginPortal.getKeystoneToken())


class TestCasJobs(unittest.TestCase):

    token1 = Authentication.login(Authentication_loginName, Authentication_loginPassword);

    def setUp(self):
        pass

    # *******************************************************************************************************
    # CasJobs section:


    def test_CasJobs_getSchemaName(self):
        casJobsId = CasJobs.getSchemaName()
        self.assertNotEqual(casJobsId,"")

    def test_CasJobs_getTables(self):
        tables = CasJobs.getTables(context="MyDB")

    def test_CasJobs_executeQuery(self):
        df = CasJobs.executeQuery(sql=CasJobs_TestQuery, context=CasJobs_TestDatabase, format="pandas")
        self.assertEqual(CasJobs_TestTableCSV, df.to_csv(index=False))

    def test_CasJobs_submitJob(self):
        jobId = CasJobs.submitJob(sql=CasJobs_TestQuery + " into MyDB." + CasJobs_TestTableName1, context=CasJobs_TestDatabase)
        jobDescription = CasJobs.waitForJob(jobId=jobId, verbose=True)
        df = CasJobs.executeQuery(sql="DROP TABLE " + CasJobs_TestTableName1, context="MyDB", format="csv")
        self.assertNotEqual(jobId, "")

    def test_CasJobs_getJobStatus(self):
        jobId = CasJobs.submitJob(sql=CasJobs_TestQuery, context=CasJobs_TestDatabase)
        jobDescription = CasJobs.getJobStatus(jobId)
        self.assertEqual(jobDescription["JobID"], jobId)

    def test_CasJobs_cancelJob(self):
        jobId = CasJobs.submitJob(sql=CasJobs_TestQuery, context=CasJobs_TestDatabase)
        isCanceled = CasJobs.cancelJob(jobId=jobId)
        self.assertEqual(isCanceled, True)

    def test_CasJobs_waitForJob(self):
        jobId = CasJobs.submitJob(sql=CasJobs_TestQuery, context=CasJobs_TestDatabase)
        jobDescription = CasJobs.waitForJob(jobId=jobId, verbose=True)
        self.assertGreaterEqual(jobDescription["Status"], 3)

    def test_CasJobs_writeFitsFileFromQuery(self):
        #CasJobs.getFitsFileFromQuery
        try:
            result = CasJobs.writeFitsFileFromQuery(fileName=CasJobs_TestFitsFile, queryString=CasJobs_TestQuery, context="MyDB")
            self.assertEqual(result, True)
            self.assertEqual(os.path.isfile(CasJobs_TestFitsFile), True)
        finally:
            try:
                os.remove(CasJobs_TestFitsFile)
            except:
                pass;

    def test_CasJobs_getPandasDataFrameFromQuery(self):
        #CasJobs.getPandasDataFrameFromQuery
        df = CasJobs.getPandasDataFrameFromQuery(queryString=CasJobs_TestQuery, context=CasJobs_TestDatabase)
        self.assertEqual(df.to_csv(index=False), CasJobs_TestTableCSV)

    def test_CasJobs_getNumpyArrayFromQuery(self):
        #CasJobs.getNumpyArrayFromQuery
        array = CasJobs.getNumpyArrayFromQuery(queryString=CasJobs_TestQuery, context=CasJobs_TestDatabase)
        newArray = pandas.read_csv(StringIO(CasJobs_TestTableCSV), index_col=None).as_matrix()
        self.assertEqual(array.all(), newArray.all())

    def test_CasJobs_uploadPandasDataFrameToTable_uploadCSVDataToTable(self):
        try:

            df = pandas.read_csv(StringIO(CasJobs_TestTableCSV), index_col=None)

            result = CasJobs.uploadPandasDataFrameToTable(dataFrame=df, tableName=CasJobs_TestTableName2, context="MyDB")
            table = CasJobs.executeQuery(sql="select * from " + CasJobs_TestTableName2, context="MyDB", format="pandas")
            result2 = CasJobs.executeQuery(sql="DROP TABLE " + CasJobs_TestTableName2, context="MyDB", format="csv")
            self.assertEqual(result, True)
            self.assertItemsEqual(table, df)

            result = CasJobs.uploadCSVDataToTable(csvData=CasJobs_TestTableCSV, tableName=CasJobs_TestTableName2, context="MyDB")
            df2 = CasJobs.executeQuery(sql="select * from " + CasJobs_TestTableName2, context="MyDB", format="pandas")
            result2 = CasJobs.executeQuery(sql="DROP TABLE " + CasJobs_TestTableName2, context="MyDB", format="csv")
            self.assertEqual(result, True)
            self.assertItemsEqual(df, df2)

        finally:
            try:
                csv = CasJobs.executeQuery(sql="DROP TABLE " + CasJobs_TestTableName2, context="MyDB",format="csv")
            except:
                pass;


class TestSkyServer(unittest.TestCase):

    token1 = Authentication.login(Authentication_loginName, Authentication_loginPassword);

    def setUp(self):
        pass

    # *******************************************************************************************************
    #  SkyServer section:

    def test_SkyServer_sqlSearch(self):
        #sql search
        df = SkyServer.sqlSearch(sql=SkyServer_TestQuery, dataRelease=SkyServer_DataRelease)
        self.assertEqual(SkyServer_QueryResultCSV, df.to_csv(index=False))

    def test_SkyServer_getJpegImgCutout(self):
        #image cutout
        img = SkyServer.getJpegImgCutout(ra=197.614455642896, dec=18.438168853724, width=512, height=512, scale=0.4, dataRelease=SkyServer_DataRelease,opt="OG",query="SELECT TOP 100 p.objID, p.ra, p.dec, p.r FROM fGetObjFromRectEq(197.6,18.4,197.7,18.5) n, PhotoPrimary p WHERE n.objID=p.objID")
        im = skimage.io.imread("./TestGalaxy.jpeg")
        self.assertEqual(img.tobytes(), im.tobytes())

    def test_SkyServer_radialSearch(self):
        # radial search
        df = SkyServer.radialSearch(ra=258.25, dec=64.05, radius=0.1, dataRelease=SkyServer_DataRelease)
        self.maxDiff = None;
        self.assertEqual(SkyServer_RadialSearchResultCSV, df.to_csv(index=False, float_format="%.6f"))

    def test_SkyServer_rectangularSearch(self):
        #rectangular search
        df = SkyServer.rectangularSearch(min_ra=258.3, max_ra=258.31, min_dec=64,max_dec=64.01, dataRelease=SkyServer_DataRelease)
        self.maxDiff = None;
        self.assertEqual(SkyServer_RectangularSearchResultCSV, df.to_csv(index=False, float_format="%.6f"))

    def test_SkyServer_objectSearch(self):
        #object search
        object = SkyServer.objectSearch(ra=258.25, dec=64.05, dataRelease=SkyServer_DataRelease)
        self.maxDiff = None;
        self.assertEqual(SkyServer_ObjectSearchResultObjID, object[0]["Rows"][0]["id"])

class TestSciDrive(unittest.TestCase):

    token1 = Authentication.login(Authentication_loginName, Authentication_loginPassword);

    def setUp(self):
        pass

    # *******************************************************************************************************
    # SciDrive section:


    def test_SciDrive_createContainer_directoryList_delete(self):
       try:
           responseDelete = SciDrive.delete(SciDrive_Directory)
       except:
           pass;

       try:
            responseCreate = SciDrive.createContainer(SciDrive_Directory)
            self.assertEqual(responseCreate, True)

            dirList = SciDrive.directoryList(SciDrive_Directory)
            self.assertTrue(dirList["path"].__contains__(SciDrive_Directory));

       finally:
            responseDelete = SciDrive.delete(SciDrive_Directory)
            self.assertEqual(responseDelete, True)

    def test_SciDrive_publicUrl(self):
            try:
                responseDelete = SciDrive.delete(SciDrive_Directory)
            except:
                pass;
            responseCreate = SciDrive.createContainer(SciDrive_Directory)
            url = SciDrive.publicUrl(SciDrive_Directory)
            responseDelete = SciDrive.delete(SciDrive_Directory)
            isUrl = url.startswith("http")
            self.assertEqual(responseCreate, True)
            self.assertEqual(isUrl, True)
            self.assertEqual(responseDelete, True)

    def test_SciDrive_upload_download_delete(self):
        try:

            if (sys.version_info > (3, 0)): #python3
                file = open(SciDrive_FileName, "w")
            else: #python2
                file = open(SciDrive_FileName, "wb")

            file.write(SciDrive_FileContent)
            file.close()

            responseUpload = SciDrive.upload(path=SciDrive_Directory + "/" + SciDrive_FileName, localFilePath=SciDrive_FileName)

            stringio = SciDrive.download(path=SciDrive_Directory + "/" + SciDrive_FileName, format="StringIO")
            fileContent = stringio.read()
            responseDelete = SciDrive.delete(SciDrive_Directory)
            self.assertEqual(responseUpload["path"], SciDrive_Directory + "/" + SciDrive_FileName)
            self.assertEqual(fileContent, SciDrive_FileContent)
            self.assertEqual(responseDelete, True)

            responseUpload = SciDrive.upload(path=SciDrive_Directory + "/" + SciDrive_FileName, data=SciDrive_FileContent)
            fileContent = SciDrive.download(path=SciDrive_Directory + "/" + SciDrive_FileName, format="text")
            responseDelete = SciDrive.delete(SciDrive_Directory)
            self.assertEqual(responseUpload["path"], SciDrive_Directory + "/" + SciDrive_FileName)
            self.assertEqual(fileContent, SciDrive_FileContent)
            self.assertEqual(responseDelete, True)

        finally:
            try:
                os.remove(SciDrive_FileName)
            except:
                pass;


class TestSkyQuery(unittest.TestCase):

    token1 = Authentication.login(Authentication_loginName, Authentication_loginPassword);

    def setUp(self):
        pass

    # *******************************************************************************************************
    # SkyQuery section:

    #-- submitting jobs:

    def test_SkyQuery_listQueues(self):
        queueList = SkyQuery.listQueues()

    def test_SkyQuery_getQueueInfo(self):
        queueInfo = SkyQuery.getQueueInfo('quick')
        queueInfo = SkyQuery.getQueueInfo('long')

    def test_SkyQuery_submitJob(self):
        jobId = SkyQuery.submitJob(query=SkyQuery_Query, queue="quick")
        self.assertNotEqual(jobId, "")

    def test_SkyQuery_getJobStatus(self):
        jobId = SkyQuery.submitJob(query=SkyQuery_Query, queue="quick")
        jobDescription = SkyQuery.getJobStatus(jobId=jobId)

    def test_SkyQuery_waitForJob(self):
        jobId = SkyQuery.submitJob(query=SkyQuery_Query, queue="quick")
        jobDescription = SkyQuery.waitForJob(jobId=jobId, verbose=True)
        self.assertEqual(jobDescription["status"], "completed")

    def test_SkyQuery_cancelJob(self):
        isCanceled = SkyQuery.cancelJob(SkyQuery.submitJob(query=SkyQuery_Query, queue="long"))
        self.assertEqual(isCanceled, True)

    #-- uploading and downloading csv tables:

    def test_SkyQuery_uploadTable_getTable_getTableInfo_listTableColumns_dropTable(self):
        try:
            result = SkyQuery.dropTable(tableName=SkyQuery_TestTableName, datasetName="MyDB");
        except:
            pass;

        result = SkyQuery.uploadTable(uploadData=SkyQuery_TestTableCSV, tableName=SkyQuery_TestTableName, datasetName="MyDB", format="csv")
        self.assertEqual(result, True)

        table = SkyQuery.getTable(tableName=SkyQuery_TestTableName, datasetName="MyDB", top=10)
        self.assertEqual(SkyQuery_TestTableCSVdownloaded, table.to_csv(index=False));

        info = SkyQuery.getTableInfo(tableName="webuser." + SkyQuery_TestTableName, datasetName="MyDB")

        columns = SkyQuery.listTableColumns(tableName="webuser." + SkyQuery_TestTableName, datasetName="MyDB")

        result = SkyQuery.dropTable(tableName=SkyQuery_TestTableName, datasetName="MyDB");
        self.assertEqual(result, True)

    #-- getting database info

    def test_SkyQuery_listJobs(self):
        quickJobsList = SkyQuery.listJobs('quick')
        longJobsList = SkyQuery.listJobs('long')

    def test_SkyQuery_listAllDatasets(self):
        datasets = SkyQuery.listAllDatasets()

    def test_SkyQuery_getDatasetInfo(self):
        info = SkyQuery.getDatasetInfo("MyDB")

    def test_SkyQuery_listDatasetTables(self):
        tables = SkyQuery.listDatasetTables("MyDB")





class TestComputeJobs(unittest.TestCase):

    token1 = Authentication.login(Authentication_loginName, Authentication_loginPassword);

    def setUp(self):
        pass

    # *******************************************************************************************************
    # ComputeJobs section

    def test_ComputeJobs_getComputeDomains(self):
        pass

    def test_ComputeJobs_submitNotebookJob(self):
        pass

    def test_ComputeJobs_submitShellCommandJob(self):
        pass

    def test_ComputeJobs_getJobDescription(self):
        pass

    def test_ComputeJobs_getJobStatus(self):
        pass

    def test_ComputeJobs_getJobsList(self):
        pass

    def test_ComputeJobs_getJobDirectory(self):
        pass

    def test_ComputeJobs_cancelJob(self):
        pass

    def test_ComputeJobs_waitForJob(self):
        pass


class TestFileService(unittest.TestCase):

    token1 = Authentication.login(Authentication_loginName, Authentication_loginPassword);

    def setUp(self):
        pass

    # *******************************************************************************************************
    # FileService section

    def test_FileService_createDir(self):
        pass

    def test_FileService_delete(self):
        pass

    def test_FileService_upload(self):
        pass

    def test_FileService_download(self):
        pass

if __name__ == '__main__':
    #unittest.main()
    unittest.TestLoader.sortTestMethodsUsing = lambda x, y: cmp(x,y);
    testLoader = unittest.TestLoader()
    testLoader.sortTestMethodsUsing = lambda x, y: 0;
    suite = testLoader.loadTestsFromTestCase(TestAuthentication); unittest.TextTestRunner(verbosity=2).run(suite)
    suite = testLoader.loadTestsFromTestCase(TestLoginPortal); unittest.TextTestRunner(verbosity=2).run(suite)
    suite = testLoader.loadTestsFromTestCase(TestCasJobs); unittest.TextTestRunner(verbosity=2).run(suite)
    suite = testLoader.loadTestsFromTestCase(TestSkyServer); unittest.TextTestRunner(verbosity=2).run(suite)
    suite = testLoader.loadTestsFromTestCase(TestSciDrive); unittest.TextTestRunner(verbosity=2).run(suite)
    suite = testLoader.loadTestsFromTestCase(TestSkyQuery); unittest.TextTestRunner(verbosity=2).run(suite)

