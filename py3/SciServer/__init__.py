"""
This python package provides functions for quick access of SciServer APIs (web services) and tools.
SciServer (http://www.sciserver.org) provides a new online framework for data-intensive scientifc computing in the cloud,
where the motto is to bring the computation close where the data is stored, and allow seamless access and sharing of big data sets within the scientific community.

Some SciServer tools you can access with this package:\n

* `Login Portal <http://portal.sciserver.org>`_: Single sign-on portal to all SciServer applications.
\t\tAlthough some tools accept anonymous access, you can use Authentication.login to login and access the tools and your own data and environment (after registering in the Login Portal). If you are running this package in a Jupyter Notebook in the SciServer-Compute environment, the use of Authentication.login is not necessary since it's done automatically.

* `CasJobs <http://skyserver.sdss.org/CasJobs/>`_: Database storage and querying.
\t\tYou can have access big databases, as well as save your data tables in your own database called 'MyDB'. The user can run synchronous or asynchronous SQL queries and get the result back as an R data-frame (using CasJobs.executeQuery or CasJobs.submitJob, respectively). Uploading of CSV files or R data-frames into a database table can be done using CasJobs.uploadCSVToTable and CasJobs.uploadDataFrameToTable, respectively.

* `SciDrive <http://www.scidrive.org/>`_: Drag-and-drop file storage and sharing.
\t\tYou can create directories in SciDrive using SciDrive.createContainer, upload a file to SciDrive using SciDrive.upload, and share its URL with your collaborators by using SciDrive.publicUrl.

* `SkyServer <http://skyserver.sdss.org/>`_: Access to the SDSS astronomical survey.
\t\tYou can query the SDSS database using SkyServer.sqlSearch, run cone searches using SkyServer.radialSearch, or get cutout images from the sky using SkyServer.getJpegImgCutout, between other tasks.

* `SkyQuery <http://www.voservices.net/skyquery>`_: Cross-match of astronomical source catalogs.
\t\tYou can use this scalable database system for uploading your own catalogs and cross-matching them against huge astronomical source catalogs, or even cross-matching huge catalogs against each other!. Use SkyQuery.submitJob to run the cross-match, and use SkyQuery.listAllDatasets, SkyQuery.listDatasetTables and SkyQuery.listTableColumns to browse the catalogs and database schema.

**References**

* SciServer-Python repository: http://www.github.com/sciserver/SciScript-Python

* SciServer: http://www.sciserver.org


**Maintainer**: Manuchehr Taghizadeh-Popp <mtaghiza@jhu.edu>\n

**Authors**: Gerard Lemson <glemson1@jhu.edu>, Manuchehr Taghizadeh-Popp <mtaghiza@jhu.edu>

**Version**: sciserver-v1.9.5


"""
__author__ = 'gerard,mtaghiza'
