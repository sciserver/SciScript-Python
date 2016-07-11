import json
import time

import sys
from io import StringIO

import requests
import pandas

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
    #url = URLencode(url)
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
        postResponse = requests.post(url,headers=headers)
        if postResponse.status_code != 200:
            return {"Error":{"ErrorCode":postResponse.status_code,"Message":postResponse.content.decode()}}

        r=postResponse.content.decode();
        return pandas.read_csv(StringIO(r), comment='#')
    except requests.exceptions.RequestException as e:
        return e
