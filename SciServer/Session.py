#Python v3.4

import json

import os
import sys

import SciServer.Config

import re

def getKeystoneToken():
	"""Returns the users keystone token passed into the python instance with the --ident argument."""

	token = ""
	identArgIdentifier = "--ident="

	for arg in sys.argv:
		if(arg.startswith(identArgIdentifier)):
			token = arg[len(identArgIdentifier):]

	if(token == ""):
		raise EnvironmentError("Keystone token is not in the command line argument --ident.")
			
	return token
