# URLs for accessing SciServer web services (API endpoints)
CasJobsRESTUri = "http://skyserver.sdss.org/CasJobs/RestApi"
AuthenticationURL = "http://portal.sciserver.org/login-portal/keystone/v3/tokens"
SciDriveHost = 'http://www.scidrive.org'
SkyQueryUrl = 'http://voservices.net/skyquery/Api/V1'
DataRelease = 'DR13'
SkyServerWSurl = 'http://skyserver.sdss.org'
version = "sciserver-v1.9.3" #sciserver release version
KeystoneTokenPath =  "/home/idies/keystone.token" #this path to the file containing the user's keystone token is hardcoded in the sciserver-compute environment


# returns TRUE if the library is running inside the SciServer-Compute, and FALSE if not
import os;
def isSciServerComputeEnvironment():
    if os.path.isfile(KeystoneTokenPath):
        return True
    else:
        return False
