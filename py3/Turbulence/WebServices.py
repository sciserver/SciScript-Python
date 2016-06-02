#Python v3.4

import requests

import tables
import numpy as np

from py3 import SciServer

import py3.SciServer.Session
from py3.Turbulence import Config


def getCutout(dataset, fields, ti, nt, xi, nx, yi, ny, zi, nz, turbAuthToken):
    cutoutUrl = Config.TurbulenceCutoutRootURL + dataset+ "/" + fields +"/" + str(ti) + "," + str(nt) +"/" + str(xi) + "," + str(nx) + "/" + str(yi) + "," + str(ny) + "/" + str(zi) + "," + str(nz)

    getResponse = requests.get(cutoutUrl)

    return getResponse

def getPyTablesCutout(dataset, fields, ti, nt, xi, nx, yi, ny, zi, nz, turbAuthToken):

    response = getCutout(dataset, fields, ti, nt, xi, nx, yi, ny, zi, nz, turbAuthToken)

    h5file = tables.openFile("InMemoryCutout.h5", "r", driver="H5FD_CORE", driver_core_backing_store=0, driver_core_image=response.read())

    return h5file

#data should be bytes
def callTurbulenceRESTService(dataset, operation, parameters, data, token):
    
    TurbUrl = SciServer.Config.TurbulenceRESTUri + "/" + dataset + "/" + operation + "?" + parameters

    if(data is None):
        data=b""

    headers = {'X-Auth-Token': token}

    try:
        postResponse = requests.post(TurbUrl,data=data,headers=headers)
        print("getReaderFromMyDB POST response: ", postResponse.status_code, postResponse.reason)

        return postResponse.content
    
    except Exception as e:
        print("Exeption message: ", e)
        return -1

def getRawResponse(dataset, function, time, x, y, z, xwidth, ywidth, zwidth, token=""):
    #sciServerUser = SciServer.Session.getSciServerUser()

    if token == "":
        token = py3.SciServer.Session.getKeystoneToken()

    parameters = "time=" + str(time) +"&x=" + str(x) + "&xwidth=" + str(xwidth) + "&y=" + str(y) + "&ywidth=" + str(ywidth) + "&z=" + str(z) + "&zwidth=" + str(zwidth)

    postResponse = callTurbulenceRESTService(dataset, function, parameters, None, token)

    return postResponse

def getNumpyArrayFromRawResponse(dataset, function, time, x, y, z, xwidth, ywidth, zwidth, dimensions, token=""):
    response = getRawResponse(dataset, function, time, x, y, z, xwidth, ywidth, zwidth)

    if response != -1:
        #arr = np.empty([xwidth , ywidth , zwidth, dimensions], dtype = np.float32)

        #response.readinto(arr)
        arr = np.frombuffer(response.read(),dtype = np.float32).reshape([xwidth , ywidth , zwidth, dimensions])
        
        return arr
    else:
        raise Exception("The response was invalid.")