#Python v3.4

import json

import SciServer.Config
import SciServer.Session

import urllib

class KeystoneUser:
    id = "KeystoneID"
    userName = "User Name"

def getKeystoneUserWithToken(token):

    KeystoneUrl = SciServer.Config.KeystoneServerUri + SciServer.Config.KeystoneApiPathV3 + "/auth/tokens"

    scriptingToken = getToken(SciServer.Config.ScriptingKeystoneUserName, SciServer.Config.ScriptingKeystonePassword, SciServer.Config.ScriptingKeystoneTenant)

    req = urllib.request.Request(KeystoneUrl, method='GET')
    req.add_header('X-Auth-Token', scriptingToken)
    req.add_header('X-Subject-Token', token)

    try:
        getResponse = urllib.request.urlopen(req)
        print("getKeystoneUserWithToken GET response: ", getResponse.status, getResponse.reason)

        responseMessage = getResponse.read().decode()

        responseJson = json.loads(responseMessage)

        ksu = KeystoneUser()
        ksu.userName = responseJson["token"]["user"]["name"]
        ksu.id = responseJson["token"]["user"]["id"]

        return ksu
    
    except Exception as e:
        print("Exeption message: ", e)
        return -1

def getToken(UserName, Password, Tenant):
       
    KeystoneUrl = SciServer.Config.KeystoneServerUri + SciServer.Config.KeystoneApiPathV2 + "/tokens"

    authJson = {"auth": {"tenantName": Tenant,"passwordCredentials": {"username": UserName,"password": Password}}}

    data = json.dumps(authJson).encode()

    req = urllib.request.Request(KeystoneUrl, data=data, method='POST')
    req.add_header('Content-Type', "application/json")

    try:
        postResponse = urllib.request.urlopen(req)

        print("getReaderFromMyDB POST response: ", postResponse.status, postResponse.reason)

        responseMessage = postResponse.read().decode()

        responseJson = json.loads(responseMessage)

        return responseJson["access"]["token"]["id"]
    
    except Exception as e:
        print("Exeption message: ", e)
        return -1
    