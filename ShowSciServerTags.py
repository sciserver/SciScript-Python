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
os.system("git reset --hard origin/master") #resets the local master branch to what was just fetched.
os.system("git clean -df") #removes all untracked files

sysPrint("\n--2) Listing available SciServer version Tags:\n\n")
tags = os.popen("git tag").read().split("\n")
if len(tags)==0:
  sysPrint("No SciServer Tags available.\n\n")
else:
    os.system("git tag")
