# Python v3.4

import sys


def identArgIdentifier():
    return "--ident="


def getKeystoneToken():
    """Returns the users keystone token passed into the python instance with the --ident argument."""

    token = ""
    ident = identArgIdentifier()
    for arg in sys.argv:
        if (arg.startswith(ident)):
            token = arg[len(ident):]

    if (token == ""):
        raise EnvironmentError("Keystone token is not in the command line argument --ident.")

    return token


def setKeystoneToken(token):
    sys.argv.append(identArgIdentifier()+token)