__author__ = 'gerard'
#Python v3.4

import json
import sys
import requests
import os.path

from SciServer import Config

class KeystoneUser:
    id = "KeystoneID"
    userName = "User Name"

def getKeystoneUserWithToken(token):

    loginURL = Config.LoginPortalURL
    if ~loginURL.endswith("/"):
        loginURL = loginURL + "/"
    loginURL = loginURL + token
    try:
        getResponse = requests.get(loginURL)
        responseJson = json.loads((getResponse.content.decode()))

        ksu = KeystoneUser()
        ksu.userName = responseJson["token"]["user"]["name"]
        ksu.id = responseJson["token"]["user"]["id"]

        return ksu

    except Exception as e:
        return e

def login(UserName, Password):

    loginURL = Config.LoginPortalURL

    authJson = {"auth":{"identity":{"password":{"user":{"name":UserName,"password":Password}}}}}

    data = json.dumps(authJson).encode()

    headers={'Content-Type': "application/json"}


    try:
        postResponse = requests.post(loginURL,data=data,headers=headers)
        token= postResponse.headers['X-Subject-Token']
        setKeystoneToken(token)
        return token
    except Exception as e:
        print("Exeption message: ", e)
        return None

def getToken():
    """
    Retrieve the token stored in the docker container. If not found there, will look at system argument --ident.
    Will only work when running on docker.
    """
    # This code block defined your token and makes it available as a
#   system variable for the length of your current session.
#
# This will usually be the first code block in any script you write.
    tokenFile='/home/idies/keystone.token'
    if os.path.isfile(tokenFile) :
        with open(tokenFile, 'r') as f:
            token = f.read().rstrip('\n')
        setKeystoneToken(token)
        return token
    else:
        return getKeystoneToken()

def identArgIdentifier():
    return "--ident="

def getKeystoneToken():
    """
    Returns the users keystone token passed into the python instance with the --ident argument.
    """
    token = ""
    ident = identArgIdentifier()
    for arg in sys.argv:
        if (arg.startswith(ident)):
            token = arg[len(ident):]

    if (token == ""):
        raise EnvironmentError("Keystone token is not in the command line argument --ident.")

    return token


def setKeystoneToken(token):
    """
    Set the token as the --ident argument
    :param token:
    :return:
    """
    sys.argv.append(identArgIdentifier()+token)