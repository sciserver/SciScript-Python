"""
The SciServer.Config module contains important parameters for the correct functioning of the SciServer package.\n
Although these parameters must be set/defined by the admin or user before the installation of the package, they can also be accessed and changed on-the-fly while on the python session.\n

- **Config.CasJobsRESTUri**: defines the base URL of the CasJobs web API (string). E.g., "https://skyserver.sdss.org/CasJobs/RestApi"

- **Config.AuthenticationURL**: defines the base URL of the Authentication web service API (string). E.g., "https://portal.sciserver.org/login-portal/keystone/v3/tokens"

- **Config.SciDriveHost**: defines the base URL of the SciDrive web service API (string). E.g., "https://www.scidrive.org"

- **Config.SkyQueryUrl**: defines the base URL of the SkyQuery web service API (string). E.g., "http://voservices.net/skyquery/Api/V1"

- **Config.SkyServerWSurl**: defines the base URL of the SkyServer web service API (string). E.g., "https://skyserver.sdss.org"

- **Config.DataRelease**: defines the SDSS data release (string), to be used to build the full SkyServer API url along with Config.SkyServerWSurl. E.g., "DR13"

- **Config.KeystoneTokenPath**: defines the local path (string) to the file containing the user's authentication token in the SciServer-Compute environment. E.g., "/home/idies/keystone.token". Unlikely to change since it is hardcoded in SciServer-Compute.

- **Config.version**: defines the SciServer release version tag (string), to which this package belongs. E.g., "sciserver-v1.9.3"
"""
# URLs for accessing SciServer web services (API endpoints)
CasJobsRESTUri = "https://skyserver.sdss.org/CasJobs/RestApi"
AuthenticationURL = "https://portal.sciserver.org/login-portal/keystone/v3/tokens"
SciDriveHost = "https://www.scidrive.org"
SkyQueryUrl = "http://voservices.net/skyquery/Api/V1"
SkyServerWSurl = "https://skyserver.sdss.org"
DataRelease = "DR13"
KeystoneTokenPath =  "/home/idies/keystone.token" #this path to the file containing the user's keystone token is hardcoded in the sciserver-compute environment
version = "sciserver-v1.10.2" #sciserver release version



# returns TRUE if the library is running inside the SciServer-Compute, and FALSE if not
import os;
def isSciServerComputeEnvironment():
    """
    Checks whether the library is being run within the SciServer-Compute environment.

    :return: Returns True if the library is being run within the SciServer-Compute environment, and False if not.
    """
    if os.path.isfile(KeystoneTokenPath):
        return True
    else:
        return False
