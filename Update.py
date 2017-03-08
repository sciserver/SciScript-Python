#!/usr/bin/python
import sys
import os

commandLineArguments = sys.argv

def sysPrint(myString): # print string to screen, depending on python version
  if sys.version_info > (3, 0):
    print(myString)
  else:
    print myString

# Checks whether the library is being run within the SciServer-Compute environment. Returns True if the library is being run within the SciServer-Compute environment, and False if not.
def isSciServerComputeEnvironment():
    """
    Checks whether the library is being run within the SciServer-Compute environment. Returns True if the library is being run within the SciServer-Compute environment, and False if not.
    """
    if os.path.isfile("/home/idies/keystone.token"):
        return True
    else:
        return False



sysPrint("\n---1) Updating local Git repository...\n\n")
os.system("git tag -d $(git tag)") #deletes local tags
os.system("git fetch --all") #fetches all remotes into local repo, including tags.
os.system("git checkout master")
os.system("git reset --hard origin/master") #resets the local master branch to what was just fetched.
os.system("git clean -df") #removes all untracked files


if len(commandLineArguments) == 0:
    sysPrint("\n---2) Checking out latest SciScript code from local master branch...\n\n")
    os.system("git checkout master")
else:
    sciserverTag = commandLineArguments[0]
    sysPrint("\n---2) Checking out latest SciScript code tagged as \"" + sciserverTag + "\"...\n\n")
    os.system("git checkout tags/" + sciserverTag)

os.chdir("./py2")

sysPrint("\n---3) Building the SciServer package for Python 2...\n\n")

if isSciServerComputeEnvironment():
  os.system("source activate py27")# activating python = python2 in anaconda
  os.system("python setup.py install")
  os.system("source activate root")# deactivating python = python2 in anaconda, now changing back to the anaconda default of python = python3
else:
  os.system("python2 setup.py install")

os.chdir("../py3")

sysPrint("\n---4) Building the SciServer package for Python 3...\n\n")

if isSciServerComputeEnvironment():
  os.system("python setup.py install")# In anaconda, the default is python = python3
else:
  os.system("python3 setup.py install")



