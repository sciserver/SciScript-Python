# Python v3.4

#mhd1024 = 3,
#isotropic1024coarse = 4,
#isotropic1024fine = 5,
#channel = 6,
#mixing = 7

#import Turbulence. as WebServices
#import SciServer.py2.Session as Session
#import SciServer.py2.CasJobs as CasJobs
#import SciServer.py2.Config as Config
#import SciServer.py2.Keystone as Keystone
#import Turbulence.py2.zindex as zi
import WebServices
import Session
import CasJobs
import Config
import Keystone
import zindex as zi

import numpy as np

import tables
import pandas as pd
import base64
import time

ATOMLENGTH = 8


class Field:
    """A field corresponding to a turbulence dataset.
    Id corresponds to the charname from the datafields table.  The known ones are(u,p,b,a,d).
    The current values for the bit codes are {'u' : 1, 'p' : 2, 'b' : 4, 'a' : 8, 'd' : 16}.
    The current accessor methods are {'u' : 'rawvelocity', 'p' : 'rawpressure', 'b' : 'rawmagneticfield', 'a' : 'rawvectorpotential', 'd' : 'rawdesnity'}.
    Vorticity is currently a problem since it has an id but no accessor method."""

    def __init__(self, id, bitCode, dimensions, columnName, accessorMethod):
        self.id = id
        self.bitCode = bitCode
        self.accessorMethod = accessorMethod
        self.columnName = columnName
        self.dimensions = dimensions


class VelocityField(Field):
    """Convinience class for a field.
    Should be removed in the future in favor of getting this information from the database with the Turbulence.Datasets.getDataset method."""

    def __init__(self):
        super().__init__('u', 1, 3, "velocity", "rawvelocity")


class PressureField(Field):
    """Convinience class for a field.
    Should be removed in the future in favor of getting this information from the database with the Turbulence.Datasets.getDataset method."""

    def __init__(self):
        super().__init__('p', 2, 1, "pressure", "rawpressure")


class MagneticField(Field):
    """Convinience class for a field.
    Should be removed in the future in favor of getting this information from the database with the Turbulence.Datasets.getDataset method."""

    def __init__(self):
        super().__init__('b', 4, 3, "magnetic", "rawmagneticfield")


class VectorPotentialField(Field):
    """Convinience class for a field.
    Should be removed in the future in favor of getting this information from the database with the Turbulence.Datasets.getDataset method."""

    def __init__(self):
        super().__init__('a', 8, 1, "vectorpotential", "rawvectorpotential")


class DensityField(Field):
    """Convinience class for a field.
    Should be removed in the future in favor of getting this information from the database with the Turbulence.Datasets.getDataset method."""

    def __init__(self):
        super().__init__('d', 16, 1, "density", "rawdesnity")


class FieldReader:
    """A reader to access a dataset field."""

    def __init__(self, dataset, field, t, twidth, x, y, z, xwidth, ywidth, zwidth, strideTime=1, strideSpace=1,
                 filter=None, token=""):

        self.dataset = dataset
        self.field = field
        self.t = t
        self.twidth = twidth
        self.x = x
        self.y = y
        self.z = z
        self.xwidth = xwidth
        self.ywidth = ywidth
        self.zwidth = zwidth
        self.strideTime = strideTime
        self.strideSpace = strideSpace
        self.filter = filter

        self.checkBoundries(t, x, y, z, xwidth, ywidth, zwidth)

        self.time = t * self.dataset.dt
        self.timeStep = 0

        self.closed = False

        self.token = token

    def read(self):
        """Makes a call to the turbulence web services to get the data.
        Each call to the read method will return one time step.
        When the last time step has been read the closed attribute will be set to false."""

        if (self.filter != None):
            border = self.filter.border
        else:
            border = 0

        b_x = self.x - border
        b_y = self.y - border
        b_z = self.z - border

        b_xwidth = self.xwidth + 2 * border
        b_ywidth = self.ywidth + 2 * border
        b_zwidth = self.zwidth + 2 * border

        if (b_x < 0 or b_y < 0 or b_z < 0 or (b_xwidth > self.dataset.xsize - b_x) or (
            b_ywidth > self.dataset.ysize - b_y) or (b_zwidth > self.dataset.zsize - b_z)):
            border = 0
            b_x = self.x
            b_xwidth = self.xwidth
            b_y = self.y
            b_ywidth = self.ywidth
            b_z = self.z
            b_zwidth = self.zwidth

        npArr = Turbulence.WebServices.getNumpyArrayFromRawResponse(self.dataset.name, self.field.accessorMethod,
                                                                    self.time, b_x, b_y, b_z, b_xwidth, b_ywidth,
                                                                    b_zwidth, self.field.dimensions, token=self.token)

        if (self.filter != None):
            npArr = self.filter.filter(npArr)

        #print npArr.shape
        if (border > 0):
            resultStrided = npArr[border:-border:self.strideSpace, border:-border:self.strideSpace,
                            border:-border:self.strideSpace, :]
        else:
            resultStrided = npArr[::self.strideSpace, ::self.strideSpace, ::self.strideSpace, :]
        #print resultStrided.shape

        self.timeStep += self.strideTime
        self.time = (self.t + self.timeStep) * self.dataset.dt

        if self.timeStep >= self.twidth:
            self.closed = True
        else:
            self.closed = False

        return resultStrided

    def reset(self):
        """Resets the reader so that the read() method can be called from the initial time."""
        self.closed = False
        self.timeStep = 0
        self.time = self.t * self.dataset.dt

    def seekable(self):
        return False

    def writable(self):
        return False

    def readable(self):
        return true

    def checkBoundries(self, t, x, y, z, xwidth, ywidth, zwidth):
        errorFlag = False
        errorMessage = ""

        if t < 0 or t >= (self.dataset.tsize):
            errorFlag = True
            errorMessage += "Time value t = " + str(t) + " is out of bounds.  It should be between 0 and " + str(
                self.dataset.tsize) + ".\n"
        if x < 0 or x >= self.dataset.xsize:
            errorFlag = True
            errorMessage = "Position x = " + str(x) + " is out of bounds.  It should be between 0 and " + str(
                self.dataset.xsize) + ".\n"
        if y < 0 or y >= self.dataset.ysize:
            errorFlag = True
            errorMessage = "Position y = " + str(y) + " is out of bounds.  It should be between 0 and " + str(
                self.dataset.ysize) + ".\n"
        if z < 0 or z >= self.dataset.zsize:
            errorFlag = True
            errorMessage = "Position z = " + str(z) + " is out of bounds.  It should be between 0 and " + str(
                self.dataset.zsize) + ".\n"
        if xwidth < 1 or (xwidth - x) > self.dataset.xsize:
            errorFlag = True
            errorMessage = "Width xwidth = " + str(xwidth) + " is out of bounds.  It should be between 0 and " + str(
                (self.dataset.xsize - x)) + ".\n"
        if ywidth < 1 or (ywidth - y) > self.dataset.ysize:
            errorFlag = True
            errorMessage = "Width ywidth = " + str(ywidth) + " is out of bounds.  It should be between 0 and " + str(
                (self.dataset.ysize - y)) + ".\n"
        if zwidth < 1 or (zwidth - z) > self.dataset.zsize:
            errorFlag = True
            errorMessage = "Width zwidth = " + str(zwidth) + " is out of bounds.  It should be between 0 and " + str(
                (self.dataset.zsize - z)) + ".\n"

        if errorFlag:
            raise ValueError(errorMessage)


class Dataset:
    """Parametrizes a turbulence dataset."""

    def __init__(self, name, dt, tsize, xsize, ysize, zsize, fields, strideTime=1, strideSpace=1, filter=None, uid=-1):
        self.dt = dt
        self.name = name
        self.tsize = tsize
        self.xsize = xsize
        self.ysize = ysize
        self.zsize = zsize
        self.fields = fields
        self.uid = uid

        self.strideSpace = strideTime
        self.strideTime = strideSpace
        self.filter = filter


    def getFieldReader(self, field, t=0, twidth=0, x=0, y=0, z=0, xwidth=0, ywidth=0, zwidth=0, token=""):
        """"Returns a FieldReader for the given field."""
        if (twidth == 0):
            twidth = self.tsize
        if (xwidth == 0):
            xwidth = self.xsize
        if (ywidth == 0):
            ywidth = self.ysize
        if (zwidth == 0):
            zwidth = self.zsize

        reader = FieldReader(self, field, t, twidth, x, y, z, xwidth, ywidth, zwidth, self.strideTime, self.strideSpace,
                             self.filter, token)
        return reader

    def getFilteredDataset(self, strideTime, strideSpace, filter):
        """"Returns a DatasetObject with the same fields, etc.
        When the read() method is called on this dataset, it will pass the results through the given Filter object."""
        ds = Dataset(self.name, self.dt, self.tsize, self.xsize, self.ysize, self.zsize, self.fields, strideTime,
                     strideSpace, filter, self.uid)

        return ds

    def uploadToDatabase(self, newDatasetName, t=0, twidth=0, x=0, y=0, z=0, xwidth=0, ywidth=0, zwidth=0):
        """"Uploads the dataset to the myscratch:turbulence database.
        It needs to populate another table with the metadata including parameters such as the name, the time step, dt, and the size."""

        turbTestToken = SciServer.Keystone.getToken(SciServer.Config.CasJobsTurbulenceUser,
                                                    SciServer.Config.CasJobsTurbulencePassword,
                                                    SciServer.Config.CasJobsTurbulenceTenant)
        schemaName = SciServer.CasJobs.SciServer.CasJobs.getSchemaName(turbTestToken)

        #Here we should populate the metadata tables.  TurbInfoTesting.pynb has examples of how to populate the DatabaseMap, datafield, and dataset tables.
        #The datafield and dataset tables are pretty self explanatory, but the DatabaseMap table is a little trickier.  Stephen or Chichi or Sue can tell you about it in detail.
        #queryString = "INSERT INTO DatabaseMap (DatasetID, DatasetName, ProductionMachineName, ProductionDatabaseName, HotSpareMachineName,HotSpareDatabaseName, CodeDatabaseName, BackupLocation, HotSpareActive, SliceNum, PartitionNum, minLim, maxLim) VALUES (9, 'testtest', 'turbscratch', 'turbscratchdb', null, null, 'mhdlib', null, 0, 1, 1, 0, 64);"
        #queryString = "INSERT INTO datafields (name, dataset_id, charname, components, longname, tablename) VALUES ('testtest', 9, 'u', 3, 'velocity', 'itest');"
        #queryString = "INSERT INTO datasets (id, name, minLim, maxLim, maxTime, dt, timeinc, timeoff, thigh, xhigh, yhigh, zhigh) VALUES ("
        #queryString += str(dataset_id) + ", 'testtest', 0,640001, 4.0, 0.03, 3, 0, 3, 66, 66, 66);"


        if (twidth == 0):
            twidth = self.tsize
        if (xwidth == 0):
            xwidth = self.xsize
        if (ywidth == 0):
            ywidth = self.ysize
        if (zwidth == 0):
            zwidth = self.zsize

        coTSize = int(twidth / self.strideTime)
        coXSize = int(xwidth / self.strideSpace)
        coYSize = int(ywidth / self.strideSpace)
        coZSize = int(zwidth / self.strideSpace)

        for field in self.fields:

            arrHeader = bytearray(24)
            arrHeader[0] = 1
            arrHeader[1] = 69
            arrHeader[5] = 2 * field.dimensions
            arrHeader[8] = field.dimensions
            arrHeader[10] = 8
            arrHeader[12] = 8
            arrHeader[14] = 8
            b64Header = base64.b64encode(arrHeader)

            fieldReader = self.getFieldReader(field, t, twidth, x, y, z, xwidth, ywidth, zwidth, token=turbTestToken)

            tableName = newDatasetName + "_" + field.columnName

            print "Creating table ", tableName, " to database context ", SciServer.Config.CasJobsTurbulenceContext
            sqlCreate = "CREATE TABLE " + SciServer.Config.CasJobsTurbulenceContext + "." + tableName + "(timestep int NOT NULL,zindex bigint NOT NULL, data varbinary(" + str(
                8 * 8 * 8 * 4 * field.dimensions + 24) + ") NOT NULL)"

            #createResponse = SciServer.Databases.getReaderFromMyDB(sqlCreate)
            createResponse = SciServer.CasJobs.executeQuery(sqlCreate,
                                                            context=SciServer.Config.CasJobsTurbulenceContext,
                                                            token=turbTestToken)

            if (createResponse == 500):
                print "There was an error creating the table: ", createResponse, ". Does the table already exist?  If so, this will append to the existing data."

            print "adding index", tableName, " to database context ", SciServer.Config.CasJobsTurbulenceContext
            sqlIndex = "ALTER TABLE " + SciServer.Config.CasJobsTurbulenceContext + "." + tableName + " add constraint pk_" + tableName + " primary key clustered (timestep, zindex)"

            indexResponse = SciServer.CasJobs.executeQuery(sqlIndex, context=SciServer.Config.CasJobsTurbulenceContext,
                                                           token=turbTestToken)
            if (createResponse == 500):
                print "There was an error creating the index: ", indexResponse

            columnNames = "timestep,zindex,data"

            timeStep = 0
            while not fieldReader.closed:
                #timeStep = fieldReader.timeStep
                #print "Timestep = ", timeStep
                ts1 = time.time()
                resultStrided = fieldReader.read()
                ts2 = time.time()
                print "Finished reading data in", str(ts2 - ts1), "seconds"
                #csvStr = columnNames
                totalTs1 = time.time()
                parseTs1 = time.time()
                csvData = []
                csvData.append(columnNames)
                uploadChunk = 32 * 32 * 32
                i = 0
                if resultStrided.shape != (coZSize, coYSize, coXSize, field.dimensions):
                    print "Hey! Yo! The dimensions aren't right. ", resultStrided.shape, "!=", (
                    coXSize, coYSize, coZSize, field.dimensions)
                else:
                    for xindex in range(0, coXSize, ATOMLENGTH):
                        for yindex in range(0, coYSize, ATOMLENGTH):
                            for zindex in range(0, coZSize, ATOMLENGTH):
                                i = i + 1
                                arrAtom = resultStrided[zindex:(zindex + ATOMLENGTH), yindex:(yindex + ATOMLENGTH),
                                          xindex:(xindex + ATOMLENGTH)]
                                b64Atom = "{base64}" + b64Header.decode() + base64.b64encode(arrAtom.tobytes()).decode()

                                #flatIndex = xindex + yindex*coXSize + zindex*coXSize*coYSize
                                flatIndex = np.bitwise_and(zi.morton3d(zindex, yindex, xindex), zi.mask())
                                csvData.append("\n")
                                csvData.append(str(timeStep))
                                csvData.append(",")
                                csvData.append(str(flatIndex))
                                csvData.append(",")
                                csvData.append("{base64}")
                                csvData.append(b64Header.decode())
                                csvData.append(base64.b64encode(arrAtom.tobytes()).decode())
                                if (i >= uploadChunk):
                                    csvStr = ''.join(csvData)
                                    parseTs2 = time.time()
                                    print "Finished parsing chunk in", str(parseTs2 - parseTs1), "seconds"
                                    ts1 = time.time()
                                    response = SciServer.CasJobs.uploadCVSDataToTable(str.encode(csvStr), tableName,
                                                                                      SciServer.Config.CasJobsTurbulenceContext,
                                                                                      token=turbTestToken)
                                    ts2 = time.time()
                                    print "Finished writing chunk in", str(ts2 - ts1), "seconds"
                                    i = 0;
                                    parseTs1 = time.time()
                                    csvData = []
                                    csvData.append(columnNames)

                csvStr = ''.join(csvData)
                parseTs2 = time.time()
                print "Finished parsing chunk in", str(parseTs2 - parseTs1), "seconds"
                ts1 = time.time()
                response = SciServer.CasJobs.uploadCVSDataToTable(str.encode(csvStr), tableName,
                                                                  SciServer.Config.CasJobsTurbulenceContext,
                                                                  token=turbTestToken)
                ts2 = time.time()
                print "Finished writing chunk in", str(ts2 - ts1), "seconds"
                totalTs2 = time.time()
                print "Total parsing and writing time:", str(totalTs2 - totalTs1), "seconds"

                timeStep += 1

            #Create Z-index table
            print "Creating Z-index table for:", field.columnName
            createZIndexQuery = "DECLARE @thigh INT SELECT @thigh = MAX(timestep) FROM " + schemaName + "." + tableName + " EXEC dbo.spCreateZindexTable '" + schemaName + "','" + tableName + "',@thigh"
            response = SciServer.CasJobs.executeQuery(createZIndexQuery,
                                                      context=SciServer.Config.CasJobsTurbulenceContext,
                                                      token=turbTestToken)

        print"Adding new dataset to turbinfo:", newDatasetName
        addDatasetQuery = "EXEC dbo.spAddUserDataset '" + newDatasetName + "','" + schemaName + "'," + str(
            self.uid) + "," + str(x) + "," + str(y) + "," + str(z) + "," + str(xwidth) + "," + str(ywidth) + "," + str(
            zwidth) + "," + str(t) + "," + str(twidth)
        response = SciServer.CasJobs.executeQuery(addDatasetQuery, context='turbinfo', token=turbTestToken)

        return True

    def writeToHFD5File(self, name, t=0, twidth=0, x=0, y=0, z=0, xwidth=0, ywidth=0, zwidth=0):
        """Writes the dataset to a HDF5 file.  The format is identical to that which is returned from the turbulence cuttout service."""
        if (twidth == 0):
            twidth = self.tsize
        if (xwidth == 0):
            xwidth = self.xsize
        if (ywidth == 0):
            ywidth = self.ysize
        if (zwidth == 0):
            zwidth = self.zsize

        h5file = tables.open_file(name, mode="w")

        #Bitfield indicating which fields are present
        bitCode = 0
        for field in self.fields:
            bitCode = bitCode | field.bitCode

        h5file.create_array(h5file.root, "_contents", obj=[bitCode])

        h5file.create_array(h5file.root, "_dataset", obj=[self.uid])

        h5file.create_array(h5file.root, "_size", obj=[int(twidth / self.strideTime), int(xwidth / self.strideSpace),
                                                       int(ywidth / self.strideSpace), int(zwidth / self.strideSpace)])

        h5file.create_array(h5file.root, "_start", obj=[t, x, y, z])

        for field in self.fields:
            fieldReader = self.getFieldReader(field, t, twidth, x, y, z, xwidth, ywidth, zwidth)

            arrayNameIndex = 0
            while not fieldReader.closed:
                resultStrided = fieldReader.read()
                tableName = fieldReader.field.id + str.zfill(str(arrayNameIndex), 4) + "0"
                h5file.create_array(h5file.root, tableName, obj=resultStrided)

                arrayNameIndex += 1

        return h5file


class isotropic1024fine(Dataset):
    """Parametrizes a dataset.
    This can be removed now that we can use the getDataset() method to get the parameters from the database."""

    def __init__(self):
        vField = VelocityField()
        vField.columnName = "vel"
        pField = PressureField()
        pField.columnName = "pr"
        fields = [vField, pField]
        super().__init__("isotropic1024fine", 0.0002, 100, 1024, 1024, 1024, fields, uid=5)


class isotropic1024coarse(Dataset):
    """Parametrizes a dataset.
    This can be removed now that we can use the getDataset() method to get the parameters from the database."""

    def __init__(self):
        vField = VelocityField()
        vField.columnName = "vel"
        pField = PressureField()
        pField.columnName = "pr"
        fields = [vField, pField]
        super().__init__("isotropic1024coarse", 0.002, 100, 1024, 1024, 1024, fields, uid=7)


def getDataset(name):
    """Queries the turbinfo database for the parameters of the named dataset.
    Returns a Dataset object."""

    turbTestToken = SciServer.Keystone.getToken(SciServer.Config.CasJobsTurbulenceUser,
                                                SciServer.Config.CasJobsTurbulencePassword,
                                                SciServer.Config.CasJobsTurbulenceTenant)

    context = "turbinfo"

    queryString = "SELECT DatasetID,dt,thigh,xhigh,yhigh,zhigh FROM datasets WHERE name='" + name + "'"

    cvsResponse = SciServer.CasJobs.executeQuery(queryString, context, token=turbTestToken)

    pFrame = pd.read_csv(cvsResponse)
    print pFrame

    dataset_id = pFrame.DatasetID.values[0]
    dt = pFrame.dt.values[0]
    thigh = pFrame.thigh.values[0]
    xhigh = pFrame.xhigh.values[0]
    yhigh = pFrame.yhigh.values[0]
    zhigh = pFrame.zhigh.values[0]

    #class Dataset:
    #def __init__(self, name, dt, tsize, xsize,ysize,zsize,fields, strideTime=1, strideSpace=1, filter=None, uid=-1):

    #get the fields
    queryString = "SELECT * FROM datafields WHERE DatasetID='" + str(dataset_id) + "'"

    cvsResponse = SciServer.CasJobs.executeQuery(queryString, context, token=turbTestToken)

    pFrame = pd.read_csv(cvsResponse)
    print pFrame

    fields = []

    #for idx, row in data.iterrows():
    for idx, pFrameField in pFrame.iterrows():
        charname = pFrameField.charname
        components = pFrameField.components
        tablename = pFrameField.tablename
        columnname = pFrameField["name"]

        #we still need to figure out the bitcode and the accessorMethod
        if charname == 'u' or charname == 'p' or charname == 'b' or charname == 'a' or charname == 'd':
            bitCodes = {'u': 1, 'p': 2, 'b': 4, 'a': 8, 'd': 16}
            bitCode = bitCodes[charname]

            accessorMethods = {'u': 'rawvelocity', 'p': 'rawpressure', 'b': 'rawmagneticfield',
                               'a': 'rawvectorpotential', 'd': 'rawdesnity'}
            accessorMethod = accessorMethods[charname]

            #class Field:
            #def __init__(self,id, bitCode, dimensions, columnName, accessorMethod):

            field = Turbulence.Datasets.Field(charname, bitCode, components, columnname, accessorMethod)

            fields.append(field)

    #make the DataSet
    ds = Turbulence.Datasets.Dataset(name, dt, thigh, xhigh, yhigh, zhigh, fields, uid=dataset_id, )

    return ds