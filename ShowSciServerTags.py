#!/usr/bin/python
import sys
import os


def sysPrint(myString): # print string to screen, depending on python version
  if sys.version_info > (3, 0):
    print(myString)
  else:
    print myString

sysPrint("\n---1) Updating local Git repository...\n\n")
os.system("git tag -d $(git tag)") #deletes local tags
os.system("git fetch --all") #fetches all remotes into local repo, including tags.
os.system("git checkout master")

sysPrint("\n--2) Listing available SciServer release version tags:\n\n")
tags = os.popen("git tag --list \"*sciserver*\"").read().split("\n")
if len(tags)==0:
  sysPrint("No SciServer tags available.\n\n")
else:
    os.system("git tag --list \"*sciserver*\"")

sysPrint("\n*** Refer to http://www.sciserver.org/support/updates for particular release tag details.")