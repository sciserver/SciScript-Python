"""
This python package provides functions for quick access of SciServer APIs (web services) and tools.
SciServer (http://www.sciserver.org) provides a new online framework for data-intensive scientifc computing in the cloud,
where the motto is to bring the computation close where the data is stored, and allow seamless access and sharing of big data sets within the scientific community.

Some SciServer tools you can access with this package:\n

* `Login Portal <https://portal.sciserver.org>`_: Single sign-on portal to all SciServer applications.
\t\tAlthough some tools accept anonymous access, you can use Authentication.login to login and access the tools and your own data and environment (after registering in the Login Portal). If you are running this package in a Jupyter Notebook in the SciServer-Compute environment, the use of Authentication.login is not necessary since it's done automatically.

* `CasJobs <https://skyserver.sdss.org/CasJobs/>`_: Database storage and querying.
\t\tYou can have access big databases, as well as save your data tables in your own database called 'MyDB'. The user can run synchronous or asynchronous SQL queries and get the result back as an R data-frame (using CasJobs.executeQuery or CasJobs.submitJob, respectively). Uploading of CSV files or R data-frames into a database table can be done using CasJobs.uploadCSVToTable and CasJobs.uploadDataFrameToTable, respectively.

* `SciDrive <https://www.scidrive.org/>`_: Drag-and-drop file storage and sharing.
\t\tYou can create directories in SciDrive using SciDrive.createContainer, upload a file to SciDrive using SciDrive.upload, and share its URL with your collaborators by using SciDrive.publicUrl.

* `SkyServer <https://skyserver.sdss.org/>`_: Access to the SDSS astronomical survey.
\t\tYou can query the SDSS database using SkyServer.sqlSearch, run cone searches using SkyServer.radialSearch, or get cutout images from the sky using SkyServer.getJpegImgCutout, between other tasks.

* `SkyQuery <https://www.voservices.net/skyquery>`_: Cross-match of astronomical source catalogs.
\t\tYou can use this scalable database system for uploading your own catalogs and cross-matching them against huge astronomical source catalogs, or even cross-matching huge catalogs against each other!. Use SkyQuery.submitJob to run the cross-match, and use SkyQuery.listAllDatasets, SkyQuery.listDatasetTables and SkyQuery.listTableColumns to browse the catalogs and database schema.

* `Compute Jobs <https://apps.sciserver.org/compute/jobs>`_: Submission of Jupyter notebooks or shell commands as jobs
\t\tYou can execute whole Jupyter notebooks and shell commands as asynchronous batch jobs, as well as synchronous jobs.

* `SciQuery Jobs <http://apps.sciserver.org/sciquery-ui>`_: Submission of SQL queries.
\t\tYou can execute synchronous or s synchronous SQL queries against Postgres and other database backends.

* `Files <https://apps.sciserver.org/dashboard/files>`_: Interaction with the SciServer file system.
\t\tYou can upload, and download data into your own file space in SciServer, as well as share data with your collaborators, between other things.


**References**

* SciServer-Python repository: http://www.github.com/sciserver/SciScript-Python

* SciServer: http://www.sciserver.org


**Maintainer**: Manuchehr Taghizadeh-Popp <mtaghiza@jhu.edu>\n

**Authors**: Gerard Lemson <glemson1@jhu.edu>, Manuchehr Taghizadeh-Popp <mtaghiza@jhu.edu>

**Version**: sciserver-v2.0.13


"""
__author__ = 'gerard,mtaghiza'
