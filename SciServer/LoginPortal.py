__author__ = 'gerard'
#Python v3.4

import json
from io import StringIO
from SciServer import Config,Session

import requests

class KeystoneUser:
    id = "KeystoneID"
    userName = "User Name"

def getKeystoneUserWithToken(token):

    loginURL = Config.LoginPortalURL +token


    try:
        getResponse = requests.get(loginURL)
        responseJson = json.loads((getResponse.content.decode()))

        ksu = KeystoneUser()
        ksu.userName = responseJson["token"]["user"]["name"]
        ksu.id = responseJson["token"]["user"]["id"]

        return ksu

    except Exception as e:
        return e

def getToken(UserName, Password):

    loginURL = Config.LoginPortalURL

    authJson = {"auth":{"identity":{"password":{"user":{"name":UserName,"password":Password}}}}}

    data = json.dumps(authJson).encode()

    headers={'Content-Type': "application/json"}


    try:
        postResponse = requests.post(loginURL,data=data,headers=headers)
        token= postResponse.headers['X-Subject-Token']
        Session.setKeystoneToken(token)
        return token
    except Exception as e:
        print("Exeption message: ", e)
        return None
