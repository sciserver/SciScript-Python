#!/usr/bin/python
import sys
import os

commandLineArguments = sys.argv

# Checks whether the library is being run within the SciServer-Compute environment. Returns True if the library is being run within the SciServer-Compute environment, and False if not.
def isSciServerComputeEnvironment():
    """
    Checks whether the library is being run within the SciServer-Compute environment. Returns True if the library is being run within the SciServer-Compute environment, and False if not.
    """
    if os.path.isfile("/home/idies/keystone.token"):
        return True
    else:
        return False


os.system('printf "\n===============================================================================================================\n"')
os.system('printf " ---1) Updating local Git repository...\n\n"')
os.system("git tag -d $(git tag)") #deletes local tags
os.system("git fetch --all") #fetches all remotes into local repo, including tags.
os.system("git checkout master")
os.system("git reset --hard origin/master") #resets the local master branch to what was just fetched.
os.system("git clean -df") #removes all untracked files

os.system("cp -f Install.py ../Install_IntermediateCopy5551234.py") #copies the install file one level up, so that if the commit checked out in step 2) does not have it, then we can copy it back in there.
os.system("cp -f ShowSciServerTags.py ../ShowSciServerTags_IntermediateCopy5551234.py") #copies the install file one level up, so that if the commit checked out in step 2) does not have it, then we can copy it back in 

if len(commandLineArguments) <= 1:
    os.system('printf "\n===============================================================================================================\n"')
    os.system('printf " ---2) Checking out latest SciScript code from local master branch...\n\n"')
    os.system("git checkout master")
else:
    sciserverTag = commandLineArguments[1]
    os.system('printf "\n===============================================================================================================\n"')
    os.system('printf " ---2) Checking out latest SciScript code tagged as \"" + sciserverTag + "\"...\n\n"')
    os.system("git checkout tags/" + sciserverTag)

hasInstallFile = os.popen("ls Install.py").read()
if len(hasInstallFile) > 0:
    os.system("rm -f ../Install_IntermediateCopy5551234.py") #removes the copy of the install file one level up
    os.system("rm -f ../ShowSciServerTags_IntermediateCopy5551234.py") #removes the copy of the install file one level up
else:
    os.system("mv -f ../Install_IntermediateCopy5551234.py ./Install.py") #copies the install file back from one level up.
    os.system("mv -f ../ShowSciServerTags_IntermediateCopy5551234.py ./ShowSciServerTags.py") #copies the install file back from one level up.

hasInstallFile = os.popen("ls ShowSciServerTags.py").read()
if len(hasInstallFile) > 0:
    os.system("rm -f ../ShowSciServerTags_IntermediateCopy5551234.py") #removes the copy of the install file one level up
else:
    os.system("mv -f ../ShowSciServerTags_IntermediateCopy5551234.py ./ShowSciServerTags.py") #copies the install file back from one level up.

os.chdir("./py2")

os.system('printf "\n===============================================================================================================\n"')
os.system('printf " ---3) Building the SciServer package for Python 2...\n\n"')

if isSciServerComputeEnvironment():
  os.system("source activate py27 ; python setup.py install ; source activate root") # activating python = python2 in anaconda, and then changing back to the anaconda default of python = python3
else:
  os.system("python2 setup.py install")

os.chdir("../py3")

os.system('printf "\n===============================================================================================================\n"')
os.system('printf " ---4) Building the SciServer package for Python 3...\n\n"')

if isSciServerComputeEnvironment():
  os.system("python setup.py install")# In anaconda, the default is python = python3
else:
  os.system("python3 setup.py install")



