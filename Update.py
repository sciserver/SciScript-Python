#!/usr/bin/python
import sys
import os

commandLineArguments = sys.argv

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
#os.system("git clean -df") #removes all untracked files


if len(commandLineArguments) == 0:
    sysPrint("\n---2) Checking out latest SciScript code from local master branch...\n\n")
    os.system("git checkout master")

else:

    sciserverTag = commandLineArguments[0]
    sysPrint("\n---2) Checking out latest SciScript code tagged as \"" + sciserverTag + "\"...\n\n")
    os.system("git checkout tags/" + sciserverTag)


os.chdir("./py2")

sysPrint("\n---3) Building the SciServer package for Python 2...\n\n")
os.system("python2 setup.py install")

os.chdir("../py3")

sysPrint("\n---4) Building the SciServer package for Python 3...\n\n")
os.system("python3 setup.py install")
