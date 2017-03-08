# SciScript-Python

## Python libraries for Jupyter Notebooks

This python package provides functions for quick access of SciServer APIs (web services) and tools.
[SciServer](http://www.sciserver.org) provides a new online framework for data-intensive scientifc computing in the cloud,
where the motto is to bring the computation close where the data is stored, and allow seamless access and sharing of big data sets within the scientific community.

Some SciServer tools you can access with this package:\n

..* [Login Portal](http://portal.sciserver.org): Single sign-on portal to all SciServer applications.

..* [CasJobs](http://skyserver.sdss.org/CasJobs): Database storage and querying.

..* [SciDrive](http://www.scidrive.org/): Drag-and-drop file storage and sharing.

..* [SkyServer] <http://skyserver.sdss.org/): Access to the SDSS astronomical survey.

..* [SkyQuery] <http://www.voservices.net/skyquery): Cross-match of astronomical source catalogs.

Maintainer: Manuchehr Taghizadeh-Popp.

Authors: Gerard Lemson, Manuchehr Taghizadeh-Popp.


### Cloning the code locally:
    `git clone http://github.com/sciserver/SciScript-Python.git`

### Manual Installation Process:

1.- To install python 2 code, run `python setup.py install` while in the `./py2` directory.

2.- To install python 3 code, run `python3 setup.py install` while in the `./py3` directory.


### Automatic Update/Installation process:
  
1.- Run `python ShowSciServerTags.py` in order to see the version tags that label each SciServer release containing new SciScript code.

2.- To update or install, run `python Update.py tag`, where `tag` is the version tag of the SciServer release containing the SciScript version you want to install or update to (see previous step). If `tag` is not specified, then the latest version will be installed.


### Creating HTML documentation:

1.- Run `make html` while in the `./docs_sphinx/` directory. The html files will be created in `./docs_sphinx/_build/`


