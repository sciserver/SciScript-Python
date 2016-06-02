__author__ = 'gerard'

# unit tests

from SciServer import CasJobs, LoginPortal, SciDrive
import pandas
from random import randint


def testLoginPortal(user,password):
    token=LoginPortal.getToken(user,password)
    print("token=",token)
    user=LoginPortal.getKeystoneUserWithToken(token)
    print("user=",user.id,user.userName)
    return token

def testCasJobsQuery(token):
    sql="""select top 10 galaxyId,snapnum,stellarmass from MRIIscPlanck1"""

    queryResponse=CasJobs.executeQuery(sql,context="Henriques2015a",token=token)
    gals= pandas.read_csv(queryResponse,index_col=None)
    print(gals)
    return gals

def testCasJobsTables():
    schema=CasJobs.getSchemaName()
    print("schema=",schema)
    tables=CasJobs.getTables("MyDB")
    print(tables)

def testCasJobsSubmit(token):
    sql="""select top 10 galaxyId,snapnum,stellarmass into mriiscplanck1_a from MRIIscPlanck1"""

    jobId=CasJobs.submitJob(sql,context="Henriques2015a",token=token)
    print("jobId=",jobId)
    jobDesc = CasJobs.waitForJob(jobId)
    print( jobDesc)

def testUploadDataFrame(df,token, tablename):
    response=CasJobs.uploadPandasDataFrameToTable(df,tablename,token=token)


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

def main():
    user=""
    password="*****"

    token=testLoginPortal()
    testCasJobsTables()
    gals=testCasJobsQuery(token)
    r=testSciDrive(gals,"galaxies.csv",token)
    print(r)
    i=randint(0,1000000)
    testUploadDataFrame(gals, token,"FooBar_"+str(i))
    testCasJobsSubmit(token)

if __name__ == "__main__":
    main()