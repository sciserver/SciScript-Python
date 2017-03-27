#!/usr/bin/python
import sys
import os

os.system('printf "\n---1) Updating local Git repository...\n\n"')
os.system("git tag -d $(git tag)") #deletes local tags
os.system("git fetch --all") #fetches all remotes into local repo, including tags.
os.system("git checkout master")

os.system('printf "\n--2) Listing available SciServer release version tags:\n\n"')
tags = os.popen("git tag --list \"*sciserver*\"").read().split("\n")
if len(tags)==0:
  os.system('printf "No SciServer tags available.\n\n"')
else:
  os.system("git tag --list \"*sciserver*\"")

os.system('printf "\n*** Refer to http://www.sciserver.org/support/updates for particular release tag details.\n\n"')