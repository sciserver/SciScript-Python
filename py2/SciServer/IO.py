l__author__ = 'mtaghiza'
#Python v3.4

import sys
import os.path


def getJobDirectory():
    """
    Retrieve the token stored in the docker container. If not found there, will look at system argument --ident.
    Will only work when running on docker.
    """
    # This code block defined your token and makes it available as a
#   system variable for the length of your current session.
#
# This will usually be the first code block in any script you write.
    jobDirectoryFile='/home/idies/io.jobDirectory'
    if os.path.isfile(jobDirectoryFile) :
        with open(jobDirectoryFile, 'r') as f:
            jobDirectory = f.read().rstrip('\n')
        return jobDirectory
    else:
	jobDirectory = '/home/idies/workspace/scratch/defaultJobDirectory/'
	os.system('mkdir -p ' + jobDirectory)
        return jobDirectory