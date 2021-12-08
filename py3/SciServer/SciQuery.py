from SciServer import Authentication, Config, Files, Jobs
import pandas as pd
import numpy as np
import json
import requests
from functools import lru_cache
from datetime import datetime

def _get_default_rdb_domain():
    rdb_domains = Jobs.getRDBComputeDomainsNames()
    if len(rdb_domains) > 0:
        return rdb_domains[0]
    else:
        raise Exception("There are no rdbComputeDomains available for the user.");


class OutputTargetType:
    """
    Contains a set of allowed database output types.
    """
    FILE_JSON = "FILE_JSON"
    FILE_CSV = "FILE_CSV"
    DATABASE_TABLE = "TABLE"


class FileOutput:
    """
    Defines the output of a database query to a file.
    """
    def __init__(self, target_name: str = "result.json", target_type: str = OutputTargetType.FILE_JSON,
                 statement_indexes: list = [1]):
        """
        :param target_name: name of the file (string), such as "result.json"
        :param target_type:  type (string) of the file containing the query result(s) (e.g., "FILE_JSON"). As set of possible values is given by the static members of class 'SciQuery.OutputTargetType'
        :param statement_indexes:  list of integers or integer. Each integer value denotes the index or position (>=1) of the sql statements whithin the input query, whose resultset is going to be written into this OutputTarget
        """
        if type(target_name) != str or type(target_type) != str:
            raise ValueError("Invalid type(s) for input parameter(s) 'target_name' or 'target_type'")
        self.target_name = target_name
        self.target_type = target_type
        self.set_statement_indexes(statement_indexes)

    def set_statement_indexes(self, statement_indexes: list = [1]):
        """
        Sets the index(es) of the sql statement(s) whithin the input query, whose resultset(s) is(are) going to be written into this OutputTarget.
        :param statement_indexes: list of integers, which are the indices (starting with 1) of the sql statements whithin the input query, whose resultsets are going to be written into this OutputTarget.
        """
        if type(statement_indexes) != list:
            statement_indexes = [statement_indexes]
        for index in statement_indexes:
            if type(index) != int or index <= 0:
                raise ValueError("Invalid type for input parameter 'statement_indexes'")
        self.statement_indexes = [i for i in sorted(set(statement_indexes))]
        return self

    @classmethod
    def get_default(cls):
        """
        Gets a OutputTarget object filled with default values: JSON output file where only the 1st SQL statement of the query is written in it.
        """
        cls.target_name = "result.json"
        cls.target_type = OutputTargetType.FILE_JSON
        cls.statement_indexes = [1]
        return cls

    def __str__(self):
        return "File Output of target_name = {}, target_type= {}, statement_indexes = {}".format(self.target_name,
                                                                                                 self.target_type,
                                                                                                 self.statement_indexes)

    def __repr__(self):
        return "FileOutput(target_name = {}, target_type= {}, statement_indexes = {})".format(self.target_name,
                                                                                              self.target_type,
                                                                                              self.statement_indexes)


class DatabaseTableOutput:
    """
    Defines the output of a database query to a database table
    """
    def __init__(self, table: str = "resultTable", database: str = "", rdb_domain: str = "", schema: str = "",
                 statement_indexes: list = [1]):
        """
        :param table: name of the database table (string), such as "resultTable"
        :param database: name of the database (string) where the output table in created. If it is owned explicitly by a user, then it should follow the pattern "mydb:username"
        :param rdb_domain: name (string) of the relational database (RDB) compute domain that contains the database. Name of such domains available to the user is returned by the function Jobs.getRDBComputeDomainNames().
        :param schema: database schema (string)
        :param statement_indexes:  list of integers or integer. Each integer value denotes the index or position (>=1) of the sql statements whithin the input query, whose resultset is going to be written into this OutputTarget
        """
        if type(table) != str or type(rdb_domain) != str or type(schema) != str:
            raise ValueError("Invalid type(s) for input parameter(s) 'target_name' or 'target_type'")

        self.table = table
        self.database = database if database != "" else "mydb:" + Authentication.keystoneUser.userName
        self.rdb_domain = rdb_domain if rdb_domain != "" else _get_default_rdb_domain()
        self.schema = schema
        self.set_statement_indexes(statement_indexes)
        self.target_name = ".".join([rdb_domain, database, schema, table])
        self.target_type = OutputTargetType.DATABASE_TABLE

    def set_statement_indexes(self, statement_indexes: list = [1]):
        """
        Sets the index(es) of the sql statement(s) whithin the input query, whose resultset(s) is(are) going to be written into this OutputTarget.
        :param statement_indexes: list of integers, which are the indices (starting with 1) of the sql statements whithin the input query, whose resultsets are going to be written into this OutputTarget.
        """
        if type(statement_indexes) != list:
            statement_indexes = [statement_indexes]
        for index in statement_indexes:
            if type(index) != int or index <= 0:
                raise ValueError("Invalid type for input parameter 'statement_indexes'")
        self.statement_indexes = [i for i in sorted(set(statement_indexes))]
        return self

    @classmethod
    def get_default(cls):
        """
        Gets a OutputTarget object filled with default values: JSON output file where only the 1st SQL statement of the query is written in it.
        """
        cls.target_name = "resultTable"
        cls.database = "mydb:" + Authentication.keystoneUser.userName
        cls.rdb_domain = _get_default_rdb_domain()
        cls.schema = ""
        cls.target_type = OutputTargetType.DATABASE_TABLE
        cls.statement_indexes = [1]
        return cls

    def __str__(self):
        return "Database Table Output of table = {}, database= {}, rdb_domain = {}, schema = {}, statement_indexes = {}".format(
            self.table, self.database, self.rdb_domain, self.schema, self.statement_indexes)

    def __repr__(self):
        return "DatabaseTableOutput(table = {}, database= {}, rdb_domain = {}, schema = {}, statement_indexes = {})".format(
            self.table, self.database, self.rdb_domain, self.schema, self.statement_indexes)


class RDBJob:
    """
    Contains the definition of an RDB job
    """
    def __init__(self, job):
        """
        :param job: can be the job ID (string), or the
        """
        if type(job) != dict:
            job = Jobs.getJobDescription(job)
        for k, v in job.items():
            setattr(self, k, v)

    def get_output_targets(self):
        output_targets = {}
        for t in self.targets:
            i = (t['location'], t['type'])
            if i not in output_targets:
                output_targets[i] = [t['resultNumber']]
            else:
                output_targets[i] = output_targets[i].append(t['resultNumber'])

        targets = []
        for k in output_targets:
            if k[1] == OutputTargetType.DATABASE_TABLE:
                p = k[0].split(".")
                targets.append(DatabaseTableOutput(table=p[3], database=p[1], rdb_domain=p[0], schema=p[2],
                                                   statement_indexes=output_targets[k]))
            else:
                targets.append(FileOutput(target_name=k[0], target_type=k[1], statement_indexes=output_targets[k]))

        # output_targets = [ OutputTarget(l,t,output_targets[(l,t)]) for l,t in output_targets]
        # return output_targets
        return targets

    def get_results_folder_path(self):
        path = ":".join(self.resultsFolderURI.split(":")[1:])
        if not path.startswith(Config.ComputeWorkDir):
            path = Config.ComputeWorkDir + path[1:]
        return path

    def get_output_target_path(self, output_target):
        if output_target.target_type == OutputTargetType.DATABASE_TABLE:
            raise ValueError("Output target is not a file but a database")
        return self.get_results_folder_path() + output_target.target_name

    def get_fileservice_folder_path(self):
        path = ":".join(self.resultsFolderURI.split(":")[1:])
        if path.startswith(Config.ComputeWorkDir):
            path = "/" + path.replace(Config.ComputeWorkDir, "", 1)
        return path

    def get_start_time(self):
        return datetime.fromtimestamp(self.startTime / 1000.0)

    def get_end_time(self):
        return datetime.fromtimestamp(self.endTime / 1000.0)

    def get_duration(self):
        return self.duration

    def __str__(self):
        return "RDB Job of id = {}".format(self.id)

    def __repr__(self):
        return "RDBJob(id = {})".format(self.id)



@lru_cache(128)
def _get_file_service(file_service_id=""):
    print(file_service_id)
    file_services = Files.getFileServices(verbose=False)
    for file_service in file_services:
        if file_service["name"] == file_service_id or file_service["identifier"] == file_service_id:
            return file_service

    if len(file_services) > 0:
        return file_services[0]
    else:
        raise Exception("No fileservices available for the user")


def submitQueryJob(sqlQuery,
                   rdbComputeDomain=None,
                   databaseContextName=None,
                   output_targets=FileOutput.get_default(),
                   resultsFolderPath="",
                   jobAlias="",
                   file_service_name=""):
    """
    Submits a sql query for execution (as an asynchronous job) inside a relational database (RDB) compute domain.

    :param sqlQuery: sql query (string)
    :param rdbComputeDomain: object (dictionary) that defines a relational database (RDB) compute domain. A list of
            these kind of objects available to the user is returned by the function Jobs.getRDBComputeDomains().
    :param databaseContextName: database context name (string) on which the sql query is executed.
    :param output_targets: object of type SciQuery.OutputTarget defining the output of one or multiple statements
    within the input query. Could also be a list of OutputTarget objects.
    :param resultsFolderPath: full path to results folder (string) where query output tables are written into.
    E.g.: /home/idies/workspace/rootVOlume/username/userVolume/jobsFolder . If not set,
    then a default folder will be set automatically.
    :param jobAlias: alias (string) of job, defined by the user.
    :param file_service_name: name or uuid (string) of FileService where the results folder (resultsFolderPath) is
    going to be created. If not defined, then the first available FileService is chosen by default.
    :return: the ID (integer) that labels the job.
    :raises: Throws an exception if the HTTP request to the Authentication URL returns an error. Throws an exception if
     the HTTP request to the SciQuery API returns an error, or if the volumes defined by the user are not available in
     the Docker compute domain.
    :example: job_id = SciQuery.submitQueryJob('select 1';,None, None, 'myQueryResults', 'myNewJob')

    .. seealso:: Jobs.submitNotebookJob, Jobs.submitShellCommandJob, Jobs.getJobStatus, Jobs.getDockerComputeDomains,
    Jobs.cancelJob
    """

    token = Authentication.getToken()
    if token is not None and token != "":

        if Config.isSciServerComputeEnvironment():
            taskName = "Compute.SciScript-Python.Sciquery.submitQueryJob"
        else:
            taskName = "SciScript-Python.Sciquery.submitQueryJob"

        if rdbComputeDomain is None:
            rdbComputeDomains = Jobs.getRDBComputeDomains();
            if len(rdbComputeDomains) > 0:
                rdbComputeDomain = rdbComputeDomains[0];
            else:
                raise Exception("There are no rdbComputeDomains available for the user.");

        if databaseContextName is None:
            databaseContexts = rdbComputeDomain.get('databaseContexts');
            if len(databaseContexts) > 0:
                databaseContextName = databaseContexts[0].get('name')
            else:
                raise Exception("rbdComputeDomain has no database contexts available for the user.");

        if type(output_targets) != list:
            output_targets = [output_targets]

        targets = []
        for target in output_targets:
            for index in target.statement_indexes:
                targets.append({'location': target.target_name, 'type': target.target_type, 'resultNumber': index})

        rdbDomainId = rdbComputeDomain.get('id');

        file_service = _get_file_service(file_service_name)
        resultsFolderPath = file_service['identifier'] + ":" + resultsFolderPath

        dockerJobModel = {
            "inputSql": sqlQuery,
            "submitterDID": jobAlias,
            "databaseContextName": databaseContextName,
            "rdbDomainId": rdbDomainId,
            "targets": targets,
            "resultsFolderURI": resultsFolderPath
        }

        print(dockerJobModel)

        data = json.dumps(dockerJobModel).encode()
        url = Config.SciqueryURL + "/api/jobs/" + str(rdbDomainId) + "?TaskName=" + taskName;
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        print(url)
        res = requests.post(url, data=data, headers=headers, stream=True)

        if res.status_code < 200 or res.status_code >= 300:
            raise Exception("Error when submitting a job to the SciQuery API.\nHttp Response from SciQuery API " +
                            "returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return res.content.decode()
    else:
        raise Exception("User token is not defined. First log into SciServer.")


def execute_query(query,
                  rdb_compute_domain=None,
                  database_context=None,
                  results_folder_path="",
                  job_alias="",
                  poll_time=0.2,
                  file_service_name=""):
    output_target = FileOutput("result1.json", OutputTargetType.FILE_JSON).set_statement_indexes([1])

    jobId = submitQueryJob(sqlQuery=query, rdbComputeDomain=rdb_compute_domain, databaseContextName=database_context,
                           output_targets=output_target,
                           resultsFolderPath=results_folder_path,
                           jobAlias=job_alias, file_service_name=file_service_name)

    job_status = Jobs.waitForJob(jobId, verbose=False, pollTime=poll_time)
    job = RDBJob(jobId)
    if job.status > 32:
        messages = ". ".join(job.messages) if len(job.messages) > 0 else ""
        if (job.status == 64):
            raise Exception("Query ended with an error. " + messages)
        if (job.status == 128):
            raise Exception("Query was cancelled. " + messages)

    if Config.isSciServerComputeEnvironment():
        file_path = job.get_output_target_path(output_target)
        with open(file_path, ) as f:
            j = json.load(f)
    else:
        file_service_id = job.resultsFolderURI.split(":")[0]
        file_service = _get_file_service(file_service_id)
        path = job.get_fileservice_folder_path(output_target)
        s = Files.download(file_service, path, format="txt", quiet=True)
        j = json.loads(s)

    data = np.asarray(j['Result'][0]['Data'])
    column_names = j['Result'][0]['ColumnNames']
    name = j['Result'][0]['TableName']

    df = pd.DataFrame(data=data, columns=column_names)
    df.name = name
    return df
