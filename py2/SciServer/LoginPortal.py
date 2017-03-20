__author__ = 'mtaghiza, gerard'
#Python v3.4

import json
import sys
import requests
import os.path
import warnings
import SciServer.Authentication;


class KeystoneUser:
    """
    .. warning:: Deprecated. Use SciServer.Authentication.KeystoneUser instead.\n

    The class KeystoneUser stores the 'id' and 'name' of the user.
    """
    warnings.warn("Using SciServer.LoginPortal.KeystoneUser is deprecated. Use SciServer.Authentication.KeystoneUser instead.", DeprecationWarning, stacklevel=2);
    id = "KeystoneID"
    userName = "User Name"

def getKeystoneUserWithToken(token):
    """
    .. warning:: Deprecated. Use SciServer.Authentication.getKeystoneUserWithToken instead.\n

    Returns the name and Keystone id of the user corresponding to the specified token.

    :param token: Sciserver's authentication token (string) for the user.
    :return: Returns a KeystoneUser object, which stores the name and id of the user.
    :raises: Throws an exception if the HTTP request to the Authentication URL returns an error.
    :example: token = Authentication.getKeystoneUserWithToken(Authentication.getToken())

    .. seealso:: Authentication.getToken, Authentication.login, Authentication.setToken.
    """
    warnings.warn("Using SciServer.LoginPortal.getKeystoneUserWithToken is deprecated. Use SciServer.Authentication.getKeystoneUserWithToken instead.", DeprecationWarning, stacklevel=2);
    return SciServer.Authentication.getKeystoneUserWithToken(token);

def login(UserName, Password):
    """
    .. warning:: Deprecated. Use SciServer.Authentication.login instead.\n

    Logs the user into SciServer and returns the authentication token.
    This function is useful when SciScript-Python library methods are executed outside the SciServer-Compute environment.
    In this case, the session authentication token does not exist (and therefore can't be automatically recognized),
    so the user has to use Authentication.login in order to log into SciServer manually and get the authentication token.
    Authentication.login also sets the token value in the python instance argument variable "--ident", and as the local object Authentication.token (of class Authentication.Token).

    :param UserName: name of the user (string)
    :param Password: password of the user (string)
    :return: authentication token (string)
    :raises: Throws an exception if the HTTP request to the Authentication URL returns an error.
    :example: token = Authentication.login('loginName','loginPassword')

    .. seealso:: Authentication.getKeystoneUserWithToken, Authentication.getToken, Authentication.setToken, Authentication.token.
    """
    warnings.warn("Using SciServer.LoginPortal.login is deprecated. Use SciServer.Authentication.login instead.", DeprecationWarning, stacklevel=2);
    return SciServer.Authentication.login(UserName, Password);

def getToken():
    """
    .. warning:: Deprecated. Use SciServer.Authentication.getToken instead.\n

    Returns the SciServer authentication token of the user. First, will try to return Authentication.token.value.
    If Authentication.token.value is not set, Authentication.getToken will try to return the token value in the python instance argument variable "--ident".
    If this variable does not exist, will try to return the token stored in Config.KeystoneTokenFilePath. Will return a None value if all previous steps fail.

    :return: authentication token (string)
    :example: token = Authentication.getToken()

    .. seealso:: Authentication.getKeystoneUserWithToken, Authentication.login, Authentication.setToken, Authentication.token.

    """
    warnings.warn("Using SciServer.LoginPortal.getToken is deprecated. Use SciServer.Authentication.getToken instead.", DeprecationWarning, stacklevel=2);
    return SciServer.Authentication.getToken();

def identArgIdentifier():
    """
    .. warning:: Deprecated. Use SciServer.Authentication.identArgIdentifier instead.\n

    Returns the name of the python instance argument variable where the user token is stored.

    :return: name (string) of the python instance argument variable where the user token is stored.
    :example: name = Authentication.identArgIdentifier()

    .. seealso:: Authentication.getKeystoneUserWithToken, Authentication.login, Authentication.getToken, Authentication.token.
    """
    warnings.warn("Using SciServer.Authentication.identArgIdentifier is deprecated. Use SciServer.Authentication.identArgIdentifier instead.", DeprecationWarning, stacklevel=2);
    return SciServer.Authentication.identArgIdentifier();

def getKeystoneToken():
    """
    .. warning:: Deprecated. Use Authentication.getToken instead.\n

    Returns the users keystone token passed into the python instance with the --ident argument.

    :return: authentication token (string)
    :example: token = Authentication.getKeystoneToken()

    .. seealso:: Authentication.getKeystoneUserWithToken, Authentication.login, Authentication.setToken, Authentication.token, Authentication.getToken.
    """
    warnings.warn("Using SciServer.LoginPortal.getKeystoneToken is deprecated. Use SciServer.Authentication.getToken instead.", DeprecationWarning, stacklevel=2);
    return SciServer.Authentication.getKeystoneToken();

def setKeystoneToken(token):
    """
    .. warning:: Deprecated. Use Authentication.setToken instead.\n

    Sets the token as the --ident argument

    :param _token: authentication token (string)
    :example: Authentication.setKeystoneToken("myToken")

    .. seealso:: Authentication.getKeystoneUserWithToken, Authentication.login, Authentication.setToken, Authentication.token, Authentication.getToken.
    """
    warnings.warn("Using SciServer.LoginPortal.getKeystoneToken is deprecated. Use SciServer.Authentication.setToken instead.", DeprecationWarning, stacklevel=2);
    SciServer.Authentication.setKeystoneToken(token);
