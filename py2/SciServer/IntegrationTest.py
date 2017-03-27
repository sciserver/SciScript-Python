import json
import time

import sys
from io import StringIO

import requests
import pandas

from SciServer import Authentication, Config





userNames = ['matlab', 'recount'];
userPasswords = ['matlab', 'recount'];
userTokens = [];

for i in range(len(userNames)):
    Authentication.login(userNames[i],userPasswords[i]);
    token = Authentication.getKeystoneToken();
    userTokens.append(token);

