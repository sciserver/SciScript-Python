#!/usr/bin/python
from SciServer import Authentication, LoginPortal, Config, CasJobs, SkyServer, SkyQuery, SciDrive
import unittest2 as unittest
import os;
import pandas;

# Define login Name and password before running the tests
loginName = '***';
loginPassword = '***'


testFitsFile = "./MyFile.fits"
testCSVFile = "./MyFile.csv"


class TestSciScript(unittest.TestCase):

    def setUp(self):
        pass

    def test_Authentication(self):


        token1 = Authentication.login(loginName, loginPassword);
        self.assertIsNot(token1, None)
        self.assertNotEqual(token1, "")

        self.assertEqual(token1, Authentication.getToken())
        self.assertEqual(token1, Authentication.getKeystoneToken())

        newToken = "myToken"
        Authentication.setToken(newToken)
        self.assertEqual(newToken, Authentication.getToken())

        newToken = "myToken2"
        Authentication.setKeystoneToken(newToken)
        self.assertEqual(newToken, Authentication.getKeystoneToken())

        Authentication.setToken(token1)


    def test_LoginPortal(self):
        t=1

    def test_CasJobs(self):

        testQueryTableName = "MyNewtable"
        testDatabase = "MyDB"
        testQuery = "select 4 as Column1, 5 as Column2 "
        testQueryResultCSV = "Column1,Column2\n4,5\n"
        #CasJobs.getSchemaName
        wsid = CasJobs.getSchemaName()

        #CasJobs.getTables
        tables = CasJobs.getTables("MyDB")

        # CasJobs.submitJob
        dd = CasJobs.executeQuery("IF OBJECT_ID('dbo." + testQueryTableName + "', 'U') IS NOT NULL DROP TABLE " + testQueryTableName, context=testDatabase, format="csv")
        jobid = CasJobs.submitJob(testQuery + " into " + testQueryTableName , context=testDatabase)

        #CasJobs.getJobStatus
        status = CasJobs.getJobStatus(jobid)

        # CasJobs.submitJob
        CasJobs.waitForJob(jobid, verbose=False)

        #CasJobs.executeQuery
        csv = CasJobs.executeQuery("select * from " + testQueryTableName, context=testDatabase, format="csv")
        self.assertEqual(csv, testQueryResultCSV)
        csv2 = CasJobs.executeQuery("drop table " + testQueryTableName, context=testDatabase, format="csv")

        #CasJobs.getFitsFileFromQuery
        result = CasJobs.getFitsFileFromQuery(testFitsFile, testQuery)
        self.assertEqual(result, True)
        self.assertEqual(os.path.isfile(testFitsFile), True)
        self.assertEqual(os.path.getsize(testFitsFile), 8640)

        #CasJobs.getPandasDataFrameFromQuery
        df = CasJobs.getPandasDataFrameFromQuery(testQuery, context=testDatabase)
        #self.assertEqual(df.to_csv(index_label=None).encode("utf8"), testQueryResultCSV)
        self.assertEqual(df.to_csv(index_label=False, index=False).encode("utf8"), testQueryResultCSV)

        #CasJobs.getNumpyArrayFromQuery
        array = CasJobs.getNumpyArrayFromQuery(testQuery, context=testDatabase)
        self.assertEqual(df.as_matrix().all(), array.all())

        #CasJobs.uploadPandasDataFrameToTable
        result = CasJobs.uploadPandasDataFrameToTable(df,testQueryTableName)
        self.assertEqual(result, True)
        self.assertEqual(CasJobs.executeQuery("select * from " + testQueryTableName, context=testDatabase, format="pandas").as_matrix().all(), array.all())

        #cleaning up
        dd = CasJobs.executeQuery("IF OBJECT_ID('dbo." + testQueryTableName + "', 'U') IS NOT NULL DROP TABLE " + testQueryTableName, context=testDatabase, format="csv")


    def test_SkyServer(self):

        testQuery_SkyServer = "select top 1 specobjid, ra, dec from specobj order by specobjid"
        dataRelease_SkyServer = "DR13"
        testQueryResultCSV_SkyServer = "specobjid,ra,dec\n299489677444933632,146.71421,-1.0413043\n"
        testRadialSearchResultCSV_SkyServer = 'objid,run,rerun,camcol,field,obj,type,ra,dec,u,g,r,i,z,Err_u,Err_g,Err_r,Err_i,Err_z\n1237671939804561654,6162,301,3,133,246,3,258.250803912,64.0514446092,23.33982,22.3194,21.41105,21.11971,20.84277,0.6640186,0.1169861,0.07641038,0.08052275,0.2381976\n'
        testRectangularSearchResultCSV_SkyServer = 'objid,run,rerun,camcol,field,obj,type,ra,dec,u,g,r,i,z,Err_u,Err_g,Err_r,Err_i,Err_z\n0,1237671939804628290,6162,301,3,134,1346,6,258.304721275,64.0062025053,25.0008,24.50057,22.4854,21.10345,20.14999,0.9952077,0.5654563,0.1661842,0.07183576,0.124986\n'
        testObjectSearchResultCSV_SkyServer = "[{u'Rows': [{u'specId': None, u'name': u'', u'apid': u'', u'objId': u'0x112d1812608500f6', u'specObjId': u'', u'id': 1237671939804561654}], u'TableName': u'objectInfo'}, {u'Rows': [{u'mjd': 53879, u'camcol': 3, u'run': 6162, u'specObjId': None, u'objid': 1237671939804561654, u'field': 133, u'otype': u'GALAXY', u'survey': u'', u'ra': 258.250803912329, u'clean': 0, u'rerun': 301, u'obj': 246, u'dec': 64.0514446092202, u'mode': 1}], u'TableName': u'MetaData'}, {u'Rows': [{u'parentID': 1237671939804561638, u'GalaxyZoo_Morph': u'-', u'nChild': 0, u'camcol': 3, u'mjdNum': 53879, u'otype': u'GALAXY', u'field': 133, u'ra': 258.250803912329, u'u': 23.33982, u'run': 6162, u'petrorad_r': u'     1.51 &plusmn      0.161', u'photoZ_KD': u'  0.369 &plusmn   0.1088', u'objId': u'0x112d1812608500f6', u'fieldId': u'0x112d181260850000', u'err_u': 0.6640186, u'Other observations': 1, u'z': 20.84277, u'g': 22.3194, u'err_r': 0.07641038, u'i': 21.11971, u'err_z': 0.2381976, u'err_g': 0.1169861, u'r': 21.41105, u'flags': u'DEBLEND_NOPEAK DEBLENDED_AT_EDGE STATIONARY BINNED1 DEBLENDED_AS_PSF NOTCHECKED INTERP COSMIC_RAY NOPETRO NODEBLEND CHILD BLENDED EDGE ', u'mode': u'PRIMARY', u'clean': 0, u'rerun': 301, u'extinction_r': u'   0.06', u'dec': 64.0514446092202, u'err_i': 0.08052275}], u'TableName': u'ImagingData'}, {u'Rows': [{u'tablename': u'PhotoObjAll', u'name': u'err_g', u'unit': u'mag'}, {u'tablename': u'PhotoObjAll', u'name': u'err_i', u'unit': u'mag'}, {u'tablename': u'PhotoObjAll', u'name': u'err_r', u'unit': u'mag'}, {u'tablename': u'PhotoObjAll', u'name': u'err_u', u'unit': u'mag'}, {u'tablename': u'PhotoObjAll', u'name': u'err_z', u'unit': u'mag'}, {u'tablename': u'PhotoObjAll', u'name': u'extinction_r', u'unit': u'mag'}, {u'tablename': u'PhotoObjAll', u'name': u'g', u'unit': u'mag'}, {u'tablename': u'PhotoObjAll', u'name': u'i', u'unit': u'mag'}, {u'tablename': u'PhotoObjAll', u'name': u'mjd', u'unit': u'days'}, {u'tablename': u'PhotoObjAll', u'name': u'mode', u'unit': u''}, {u'tablename': u'PhotoObjAll', u'name': u'nChild', u'unit': u''}, {u'tablename': u'PhotoObjAll', u'name': u'nDetect', u'unit': u''}, {u'tablename': u'PhotoObjAll', u'name': u'parentID', u'unit': u''}, {u'tablename': u'PhotoObjAll', u'name': u'petroRad_r', u'unit': u'arcsec'}, {u'tablename': u'PhotoObjAll', u'name': u'r', u'unit': u'mag'}, {u'tablename': u'PhotoObjAll', u'name': u'u', u'unit': u'mag'}, {u'tablename': u'PhotoObjAll', u'name': u'z', u'unit': u'mag'}], u'TableName': u'ImagingDataUnits'}, {u'Rows': [], u'TableName': u'CrossId_USNO'}, {u'Rows': [{u'Minor axis (arcsec)': 0, u'Catalog': u'FIRST', u'Major axis (arcsec)': 0, u'Peak flux (mJy)': u'    0.00 &plusmn;     0.00'}], u'TableName': u'CrossId_FIRST'}, {u'Rows': [], u'TableName': u'CrossId_ROSAT'}, {u'Rows': [], u'TableName': u'CrossId_RC3'}, {u'Rows': [], u'TableName': u'CrossId_WISE'}, {u'Rows': [], u'TableName': u'CrossId_TWOMASS'}, {u'Rows': [{u'redshift': None, u'spectralClass': u'', u'fiberid': None, u'dec': 64.0514446092202, u'plateId': u'', u'otype': u'GALAXY', u'field': 133, u'ra': 258.250803912329, u'specObjId': u'', u'flags': u'DEBLEND_NOPEAK DEBLENDED_AT_EDGE STATIONARY BINNED1 DEBLENDED_AS_PSF NOTCHECKED INTERP COSMIC_RAY NOPETRO NODEBLEND CHILD BLENDED EDGE ', u'plate': None, u'run': 6162, u'run2d': u'', u'err_u': u'   0.66', u'fieldId': u'0x112d181260850000', u'mjd': None, u'z': u'  20.84', u'g': u'  22.32', u'err_r': u'   0.08', u'i': u'  21.12', u'err_z': u'   0.24', u'err_g': u'   0.12', u'r': u'  21.41', u'u': u'  23.34', u'rerun': 301, u'camcol': 3, u'err_i': u'   0.08', u'objId': u'0x112d1812608500f6'}], u'TableName': u'QuickLookMetaData'}, {u'Rows': [], u'TableName': u'SpectralData'}, {u'Rows': [], u'TableName': u'ApogeeData'}, {u'Rows': [], u'TableName': u'ApogeeVisits'}, {u'Rows': [], u'TableName': u'MangaData'}]"

        #sql search
        df = SkyServer.sqlSearch(sql=testQuery_SkyServer, dataRelease=dataRelease_SkyServer)
        self.assertEqual(testQueryResultCSV_SkyServer, df.to_csv(index_label=False, index=False))

        #image cutout
        img = SkyServer.getJpegImgCutout(ra=197.614455642896, dec=18.438168853724, width=512, height=512, scale=0.4, dataRelease=dataRelease_SkyServer,
                                     opt="OG",
                                     query="SELECT TOP 100 p.objID, p.ra, p.dec, p.r FROM fGetObjFromRectEq(197.6,18.4,197.7,18.5) n, PhotoPrimary p WHERE n.objID=p.objID")

        #radial search
        df = SkyServer.radialSearch(ra=258.25, dec=64.05, radius=0.1, dataRelease=dataRelease_SkyServer)
        self.assertEqual(df.to_csv(index_label=False, index=False), testRadialSearchResultCSV_SkyServer)

        #rectangular search
        df = SkyServer.rectangularSearch(min_ra=258.3, max_ra=258.31, min_dec=64,max_dec=64.01, dataRelease=dataRelease_SkyServer)
        self.assertEqual(df.to_csv(index_label=False), testRectangularSearchResultCSV_SkyServer)

        #object search
        object = SkyServer.objectSearch(ra=258.25, dec=64.05, dataRelease=dataRelease_SkyServer)
        self.assertEqual(object.__str__(), testObjectSearchResultCSV_SkyServer)



    def test_SciDrive(self):

        SciDriveDir = "SciScriptPythonTestDirectory"
        SciDriveFile = "Test.fits"

        try:
            response = SciDrive.delete(SciDriveDir)
        except:
            pass;

        #create container
        response = SciDrive.createContainer(SciDriveDir)

        #upload
        response = SciDrive.upload(SciDriveDir + "/" + SciDriveFile, testFitsFile)

        #public url
        url = SciDrive.publicUrl(SciDriveDir + "/" + SciDriveFile)

        #download
        stringio = SciDrive.download(SciDriveDir + "/" + SciDriveFile);fits = stringio.read();

        #clean up of file:
        os.system("rm " + testFitsFile)


    def test_SkyQuery(self):

        status = SkyQuery.getJobStatus(SkyQuery.submitJob("select 1 as foo"))

        #isCanceled = SkyQuery.cancelJob(SkyQuery.submitJob("select 1 as foo"))

        #queueList = SkyQuery.listQueues()

        #queueInfo = SkyQuery.getQueueInfo('quick')

        #jobId = SkyQuery.submitJob('select 1 as foo', "quick")

        #jobsList = SkyQuery.listJobs('quick')

        #datasets = SkyQuery.listAllDatasets()

        #info = SkyQuery.getDatasetInfo("MyDB")

        #tables = SkyQuery.listDatasetTables("MyDB")

        #info = SkyQuery.getTableInfo("MyDB", "myTable")

if __name__ == '__main__':
    #unittest.main()
    suite = unittest.TestLoader().loadTestsFromTestCase(TestSciScript)
    unittest.TextTestRunner(verbosity=2).run(suite)


