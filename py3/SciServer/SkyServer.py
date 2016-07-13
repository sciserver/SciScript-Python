import requests
import pandas
import skimage.io
import urllib

from io import StringIO
from io import BytesIO

from SciServer import LoginPortal, Config

def sqlSearch(sql, limit="10", token=""):
    """Runs a SQL query against the SDSS database. If a token is supplied, then it will run on behalf of the token's user.
    'sql': a string containing the sql query
    'limit': maximum number of rows in the result table (string). If set to '0', then the function will return all rows.
    'token': Sciserver's authentication token for the user.
    """
    url = Config.SkyServerWSurl + '/' + Config.DataRelease + '/SearchTools/SqlSearch?'
    url = url + 'format=csv&'
    url = url + 'cmd=' + sql + '&'
    url = url + 'limit=' + limit + '&'
    url = urllib.quote_plus(url)
    acceptHeader = "text/plain"
    headers = {'Content-Type': 'application/json', 'Accept': acceptHeader}

    if (token != ""):
        headers['X-Auth-Token'] = token
    else:
        Token = ""
        try:
            Token = LoginPortal.getToken()
        except:
            Token = ""
        if(Token != ""):
            headers['X-Auth-Token'] = Token

    try:
        response = requests.get(url,headers=headers)
        if response.status_code != 200:
            return {"Error":{"ErrorCode":response.status_code,"Message":response.content.decode()}}

        r=response.content.decode();
        return pandas.read_csv(StringIO(r), comment='#')
    except requests.exceptions.RequestException as e:
        return e


def getJpegImgCutout(ra, dec, scale=0.7, width=512, height=512, token = ""):
    """Gets a rectangular image cutout from a region of the sky in SDSS, centered at (ra,dec).
    'ra': Right Ascension of the image's center.
    'dec': Declination of the image's center.
    'scale': scale of the image, measured in [arcsec/pix]
    'width': Right Ascension of the image's center.
    'ra': Right Ascension of the image's center.
    'height': Height of the image, measured in [pix].
    'token': Sciserver's authentication token for the user.
    """
    url = Config.SkyServerWSurl + '/' + Config.DataRelease + '/ImgCutout/getjpeg?'
    url = url + 'ra=' + str(ra) + '&'
    url = url + 'dec=' + str(dec) + '&'
    url = url + 'scale=' + str(scale) + '&'
    url = url + 'width=' + str(width) + '&'
    url = url + 'height=' + str(height) + '&'
    #url = urllib.quote_plus(url)
    acceptHeader = "text/plain"
    headers = {'Content-Type': 'application/json', 'Accept': acceptHeader}

    if (token != ""):
        headers['X-Auth-Token'] = token
    else:
        Token = ""
        try:
            Token = LoginPortal.getToken()
        except:
            Token = ""
        if(Token != ""):
            headers['X-Auth-Token'] = Token

    try:
        response = requests.get(url,headers=headers)
        if response.status_code != 200:
            return {"Error":{"ErrorCode":response.status_code,"Message":response.content.decode()}}

        return skimage.io.imread( BytesIO( response.content  ) )
    except requests.exceptions.RequestException as e:
        return e

def radialSearch(ra, dec, radius=1, coordType="equatorial", whichPhotometry="optical", limit="10", token=""):
    """Runs a query in the SDSS database that searches for all objects within a certain radius from a point in the sky, and retrieves the result table as a Panda's dataframe.
    'ra': Right Ascension of the image's center.
    'dec': Declination of the image's center.
    'radius': Search radius around the (ra,dec) coordinate in the sky. Measured in arcminutes.
    'coordType': Type of celestial coordinate system. Can be set to "equatorial" or "galactic".
    'whichPhotometry': Type of retrieved data. Can be set to "optical" or "infrared".
    'limit': Maximum number of rows in the result table (string). If set to "0", then the function will return all rows.
    'token': Sciserver's authentication token for the user.
    """
    url = Config.SkyServerWSurl + '/' + Config.DataRelease + '/SearchTools/RadialSearch?'
    url = url + 'format=csv&'
    url = url + 'ra=' + str(ra) + '&'
    url = url + 'dec=' + str(dec) + '&'
    url = url + 'radius=' + str(radius) + '&'
    url = url + 'coordType=' + coordType + '&'
    url = url + 'whichPhotometry=' + whichPhotometry + '&'
    url = url + 'limit=' + limit + '&'
    #url = urllib.quote_plus(url)
    acceptHeader = "text/plain"
    headers = {'Content-Type': 'application/json', 'Accept': acceptHeader}

    if (token != ""):
        headers['X-Auth-Token'] = token
    else:
        Token = ""
        try:
            Token = LoginPortal.getToken()
        except:
            Token = ""
        if(Token != ""):
            headers['X-Auth-Token'] = Token

    try:
        response = requests.get(url,headers=headers)
        if response.status_code != 200:
            return {"Error":{"ErrorCode":response.status_code,"Message":response.content.decode()}}

        r=response.content.decode();
        return pandas.read_csv(StringIO(r), comment='#')
    except requests.exceptions.RequestException as e:
        return e


def rectangularSearch(min_ra, max_ra, min_dec, max_dec, coordType="equatorial", whichPhotometry="optical", limit="10", token=""):
    """Runs a query in the SDSS database that searches for all objects within a certain rectangular box defined on the the sky, and retrieves the result table as a Panda's dataframe.
    'min_ra': Minimum value of Right Ascension coordinate that defines the box boundaries on the sky.
    'max_ra': Maximum value of Right Ascension coordinate that defines the box boundaries on the sky.
    'min_dec': Minimum value of Declination coordinate that defines the box boundaries on the sky.
    'max_dec': Maximum value of Declination coordinate that defines the box boundaries on the sky.
    'coordType': Type of celestial coordinate system. Can be set to "equatorial" or "galactic".
    'whichPhotometry': Type of retrieved data. Can be set to "optical" or "infrared".
    'limit': Maximum number of rows in the result table (string). If set to "0", then the function will return all rows.
    'token': Sciserver's authentication token for the user.
    """
    url = Config.SkyServerWSurl + '/' + Config.DataRelease + '/SearchTools/RectangularSearch?'
    url = url + 'format=csv&'
    url = url + 'min_ra=' + str(min_ra) + '&'
    url = url + 'max_ra=' + str(max_ra) + '&'
    url = url + 'min_dec=' + str(min_dec) + '&'
    url = url + 'max_dec=' + str(max_dec) + '&'
    url = url + 'coordType=' + coordType + '&'
    url = url + 'whichPhotometry=' + whichPhotometry + '&'
    url = url + 'limit=' + limit + '&'
    #url = urllib.quote_plus(url)
    acceptHeader = "text/plain"
    headers = {'Content-Type': 'application/json', 'Accept': acceptHeader}

    if (token != ""):
        headers['X-Auth-Token'] = token
    else:
        Token = ""
        try:
            Token = LoginPortal.getToken()
        except:
            Token = ""
        if(Token != ""):
            headers['X-Auth-Token'] = Token

    try:
        response = requests.get(url,headers=headers)
        if response.status_code != 200:
            return {"Error":{"ErrorCode":response.status_code,"Message":response.content.decode()}}

        r=response.content.decode();
        return pandas.read_csv(StringIO(r), comment='#')
    except requests.exceptions.RequestException as e:
        return e

