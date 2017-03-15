# SciScript-Python

## Python libraries for Jupyter Notebooks

This python package provides functions for quick access of [SciServer](http://www.sciserver.org) APIs (web services) and tools.
[SciServer](http://www.sciserver.org) provides a new online framework for data-intensive scientifc computing in the cloud,
where the motto is to bring the computation close where the data is stored, and allow seamless access and sharing of big data sets within the scientific community.

Some SciServer tools you can access with this package:

 * [Login Portal](http://portal.sciserver.org): Single sign-on portal to all SciServer applications.

 * [CasJobs](http://skyserver.sdss.org/CasJobs): Database storage and querying.

 * [SciDrive](http://www.scidrive.org/): Drag-and-drop file storage and sharing.

 * [SkyServer](http://skyserver.sdss.org/): Access to the SDSS astronomical survey.

 * [SkyQuery](http://www.voservices.net/skyquery): Cross-match of astronomical source catalogs.

Maintainer: Manuchehr Taghizadeh-Popp.

Authors: Gerard Lemson, Manuchehr Taghizadeh-Popp.


## 1) Cloning the code locally:

1.1- Run `git clone http://github.com/sciserver/SciScript-Python.git`

## 2) Setting configuration parameters:

2.1.- Open `./py2/Config.py` and `./py3/Config.py`, and edit the API URLs and parameters to match those of your SciServer installation, according to the instructions and descriptions found therein.

## 3) Installation:

There are 2 possibilities: automatic or manual installation.

### a) Automatic Installation and Update:

3.a.1- Run `python ShowSciServerTags.py` in order to see the version tags that label each SciServer release containing new SciScript code.

3.a.2- To install or update, run `python Install.py tag`, where `tag` is the version tag of the SciServer release containing the SciScript version you want to install or update to (see previous step). If `tag` is not specified, then the latest version will be installed.

### b) Manual Installation:

3.b.1- To install python 2 code, run `python setup.py install` while in the `./py2` directory.

3.b.2- To install python 3 code, run `python3 setup.py install` while in the `./py3` directory.

## 5) Creating HTML documentation:

5.1.- Run `make html` while in the `./docs_sphinx/` directory. The html files will be created in `./docs_sphinx/_build/`

## 6) Unit Tests:

6.1.- Open `UnitTests.py` and edit the `loginName` and `loginPassword` parameters in order to run the Tests under the credentials of a (test) user.

6.2.- Run `python UnitTests.py` in order to run the unit tests for the SciScript-Python modules. Be sure all that all tests end with an `OK` status.
