__author__ = 'mtaghiza, gerard'
#Python v3.4

import json
import sys
import requests
import os.path
import warnings
import SciServer.Authentication;


def deprecated(func):
    '''This is a decorator which can be used to mark functions
     as deprecated. It will result in a warning being emitted
     when the function is used.'''
    def new_func(*args, **kwargs):
        warnings.warn("Call to deprecated function {}.".format(func.__name__),category = DeprecationWarning)
        return func(*args, **kwargs)

    new_func.__name__ = func.__name__
    new_func.__doc__ = func.__doc__
    new_func.__dict__.update(func.__dict__)
    return new_func



@deprecated
class KeystoneUser:
    id = "KeystoneID"
    userName = "User Name"

@deprecated
def getKeystoneUserWithToken(token):
  return SciServer.Authentication.getKeystoneToken();

@deprecated
def login(UserName, Password):
    return SciServer.Authentication.login(UserName, Password);

@deprecated
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

@deprecated
def identArgIdentifier():
    return SciServer.Authentication.identArgIdentifier();

@deprecated
def getKeystoneToken():
    """
    Returns the users keystone token passed into the python instance with the --ident argument.
    """
    return SciServer.Authentication.getKeystoneToken();

@deprecated
def setKeystoneToken(token):
    """
    Set the token as the --ident argument
    :param token:
    :return:
    """
    SciServer.Authentication.setKeystoneToken();
