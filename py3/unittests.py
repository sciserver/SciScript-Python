__author__ = 'gerard'

# unit tests

from random import randint

import pandas

from SciServer import SciDrive, LoginPortal, CasJobs


def testLoginPortal(user,password):
    token= LoginPortal.login(user,password)
    print("token=",token)
    user= LoginPortal.getKeystoneUserWithToken(token)
    print("user=",user.id,user.userName)
    return token

def testCasJobsQuery(token):
    sql="""select top 10 galaxyId,snapnum,stellarmass from MRIIscPlanck1"""

    gals= CasJobs.executeQuery(sql,context="Henriques2015a",format="pandas", token=token)
    print("PANDAS")
    print(gals)
    gals= CasJobs.executeQuery(sql,context="Henriques2015a",format="csv", token=token)
    print("CSV")
    print(gals)
    gals= CasJobs.executeQuery(sql,context="Henriques2015a",format="json", token=token)
    print("JSON")
    print(gals)
    gals= CasJobs.executeQuery(sql,context="Henriques2015a",format="readable", token=token)
    print("READABLE")
    gals=pandas.read_csv(gals)
    print(gals)
    gals= CasJobs.executeQuery(sql,context="Henriques2015a",format="blabal", token=token)
    print("ERROR")
    print(gals)
    return gals

def testCasJobsTables():
    schema= CasJobs.getSchemaName()
    print("schema=",schema)
    tables= CasJobs.getTables("MyDB")
    print(tables)

def testCasJobsSubmit(token):
    tbl="mriiscplanck1_"+str(randint(0,1000000))

    sql="""select top 10 galaxyId,snapnum,stellarmass into """+tbl+""" from MRIIscPlanck1"""

    jobId= CasJobs.submitJob(sql,context="Henriques2015a",token=token)
    print("jobId=",jobId)
    jobDesc = CasJobs.waitForJob(jobId)
    print( jobDesc)

def testUploadDataFrame(df,token, tablename):
    response= CasJobs.uploadPandasDataFrameToTable(df,tablename,token=token)


def testSciDrive(df,file_name, token):
    df.to_csv(file_name, sep=',')
    f=open(file_name,'rb')
    data=f.read()
    f.close()

    container="test_"+str(randint(0,1000000))

    SciDrive.createContainer(container,token=token)
    sdFile= container+"/"+file_name
    SciDrive.upload(sdFile, data)

    data1 = SciDrive.download(sdFile,token)
    print(data1.read())

def testTurbulenceToken():
    token = LoginPortal.getToken(py3.Turbulence.Config.CasJobsTurbulenceUser, py3.Turbulence.Config.CasJobsTurbulencePassword)
    return token




def testSciServer():
    user="G*******"
    password="********"
    token=testLoginPortal(user,password)
    # testCasJobsTables()
    testCasJobsQuery(token)

    # r=testSciDrive(gals,"galaxies.csv",token)
    # print(r)
    # i=randint(0,1000000)
    # testUploadDataFrame(gals, token,"FooBar_"+str(i))
    # testCasJobsSubmit(token)

def testTurbulence():
    token=testTurbulenceToken()
    print("turbulence token = ",token)
    schemaName = CasJobs.getSchemaName(token)
    print("schemaname=",schemaName)


def main():
    testSciServer()
#    testTurbulence()



if __name__ == "__main__":
    main()