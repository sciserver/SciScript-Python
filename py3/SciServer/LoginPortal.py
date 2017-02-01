__author__ = 'mtaghiza, gerard'
#Python v3.4

import json
import sys
import requests
import os.path

import SciServer.Authentication;

class KeystoneUser:
    id = "KeystoneID"
    userName = "User Name"

def getKeystoneUserWithToken(token):
  return SciServer.Authentication.getKeystoneToken();
  
def login(UserName, Password):
    return SciServer.Authentication.login(UserName, Password);

def getToken():
    """
    Retrieve the token stored in the docker container. If not found there, will look at system argument --ident.
    Will only work when running on docker.
    """
    # This code block defined your token and makes it available as a
#   system variable for the length of your current session.
#
# This will usually be the first code block in any script you write.
    return SciServer.Authentication.getToken();

def identArgIdentifier():
    return SciServer.Authentication.identArgIdentifier();

def getKeystoneToken():
    """
    Returns the users keystone token passed into the python instance with the --ident argument.
    """
    return SciServer.Authentication.getKeystoneToken();


def setKeystoneToken(token):
    """
    Set the token as the --ident argument
    :param token:
    :return:
    """
    SciServer.Authentication.setKeystoneToken();
 
