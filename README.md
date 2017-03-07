# SciScript-R

## Python libraries for Jupyter Notebooks


### Cloning the code locally:
    `git clone http://github.com/sciserver/SciScript-Python.git`

### Manual Installation Process:

1.- To install python 2 code, run `python setup.py install` while in the `./py2` directory.

2.- To install python 3 code, run `python setup.py install` while in the `./py3` directory.


### Automatic Update/Installation process:
  
1.- Run `python ShowSciServerTags.py` in order to see the version tags that label each SciServer release containing new SciScript code.

2.- To update or install, run `python Update.py tag`, where `tag` is the version tag of the SciServer release containing the SciScript version you want to install or update to (see previous step). If `tag` is not specified, then the latest version will be installed.


### Creating HTML documentation:

1.- Run 'make html' while in the `./docs_sphinx/' directory. The html files will be created in `./docs_sphinx/_build/`


