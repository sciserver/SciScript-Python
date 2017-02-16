__author__ = 'mtaghiza'
#Python v3.4

import sys
import os;
import os.path;


def getJobDirectory():
    """
    Gets the Jobs directory, as written inside the /home/idies/jobs.path file. This directory is used as the working directory in the execution of asynchronous jobs.
    """
    jobDirectoryFile='/home/idies/jobs.path'
    if os.path.isfile(jobDirectoryFile):
        jobDirectory = None;
        with open(jobDirectoryFile, 'r') as f:
            jobDirectory = f.read().rstrip('\n')

        return jobDirectory
    else:
        raise OSError("Jobs directory not found.")