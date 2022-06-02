from SciServer import Authentication, Config, Files, Jobs
import pandas as pd
import json
import requests
import cachetools.func
from collections.abc import Iterable
from datetime import datetime
import warnings
from pathlib import Path
from typing import Union, List
import time


class OutputType:
    """
    Contains a set of allowed database output types.
    """
    FILE_JSON = "FILE_JSON"
    FILE_CSV = "FILE_CSV"
    DATABASE_TABLE = "TABLE"


@cachetools.func.ttl_cache(maxsize=128, ttl=120)
def _get_file_service(file_service: str = None):
    file_services = Files.getFileServices(verbose=False)
    if file_service is None:
        if len(file_services) > 0:
            return file_services[0]
        raise Exception("No file services available for the user.")
    else:
        for fs in file_services:
            if file_service == fs.get("name") or file_service == fs.get("identifier"):
                return fs
        raise Exception("Unable to find fileService")


class Output:
    """
    Base class for output objects
    """
    def __init__(self,
                 name: str = "output.json",
                 output_type: str = OutputType.FILE_JSON,
                 statement_indexes: Union[int, List[int]] = 1):

        if type(name) != str:
            raise TypeError("Invalid type for input parameter 'name'.")
        if type(output_type) != str:
            raise TypeError("Invalid type for input parameter 'output_type'.")
        if type(statement_indexes) not in [list, int]:
            raise TypeError("Invalid type for input parameter 'statement_indexes'.")
        self.name = name
        self.output_type = output_type
        self.statement_indexes = None
        self.set_statement_indexes(statement_indexes)

    def set_statement_indexes(self, statement_indexes: Union[int, List[int]] = 1):
        """
        Sets the index(es) of the sql statement(s) within the input query, whose result-set(s) is(are) going to be
        written into this Output.
        :param statement_indexes: integer or list of integers, which are the indices (starting with 1) of the sql
        statements within the input query, whose resultsets are going to be written into this Output.
        """
        if not isinstance(statement_indexes, Iterable):
            statement_indexes = [statement_indexes]
        for index in statement_indexes:
            if type(index) != int or index <= 0:
                raise TypeError("Invalid type for input parameter 'statement_indexes'")
        self.statement_indexes = [i for i in sorted(set(statement_indexes))]

    def __str__(self):
        return "Output of name = {}, type= {}, statement_indexes = {}".format(self.name, self.output_type,
                                                                              self.statement_indexes)
    def __repr__(self):
        return "Output(name = {}, type= {}, statement_indexes = {})".format(self.name, self.output_type,
                                                                            self.statement_indexes)


class FileOutput(Output):
    """
    Defines the output of a database query into a file.
    """

    def __init__(self,
                 name: str = "output.json",
                 output_type: str = OutputType.FILE_JSON,
                 statement_indexes: Union[int, List[int]] = 1,
                 file_service: str = None):
        """
        :param name: name of the file (string), such as "result.json"
        :param output_type: type (string) of the file containing the query result(s) (e.g., "FILE_JSON").
        As set of possible values is given by the static members of class 'SciQuery.OutputTargetType'
        :param statement_indexes:  list of integers or integer. Each integer value denotes the index or position (>=1)
        of the sql statements within the input query, whose resultset is going to be written into this OutputTarget
        :param file_service: string denoting name or identifier of file service where the output file is written into.
        """
        if file_service:
            file_service = FileOutput.find_file_service(file_service)
            self.file_service_name = file_service['name']
            self.file_service_identifier = file_service['identifier']
        else:
            self.file_service_name = None
            self.file_service_identifier = None

        if not name:
            raise NameError("Input parameter name cannot be empty or None")
        name = name.rstrip("/")
        file_path = Path(name)
        if name == file_path.name: # means no path included in 'name' input parameter
            self.file_base_path = None
            self.file = name
            self.path = None
            self.file_service_path = None
        else:
            if not name.startswith(Config.ComputeWorkDir):
                file_path = Path(Config.ComputeWorkDir + name) # in case it is relative path
            self.file_base_path = str(file_path.parent)
            self.file_base_path = self.file_base_path if self.file_base_path.endswith("/") \
                else self.file_base_path + "/"
            self.file = file_path.name
            self.path = self.file_base_path + self.file
            self.file_service_path = self.path.replace(Config.ComputeWorkDir, "", 1)
            name = file_path.name

        super().__init__(name, output_type, statement_indexes)

    def get_path(self) -> str:
        if self.path:
            return self.path
        else:
            raise Exception("Attribute 'file_base_path' is not set.")


    @classmethod
    def get_default(cls):
        """
        Gets an Output object filled with default values: JSON output file where only the 1st SQL statement of
        the query is written in it.
        """
        return cls("result.json", OutputType.FILE_JSON, 1)

    @staticmethod
    def build_file_base_path(top_volume: str = "Temporary",
                             user_volume: str = "scratch",
                             user_volume_owner_name: str = "",
                             relative_path: str = "sciqueryjobs",
                             add_date_ending: bool = False) -> str:
        if not top_volume:
            raise NameError("Input parameter top_volume cannot be empty or None")

        if add_date_ending:
            now = datetime.now()
            date1 = now.strftime("%Y-%m-%d")
            date2 = now.strftime("%Hh%Mm%S.%fs")
            relative_path = "{0}/{1}/{2}".format(relative_path.rstrip('/'), date1, date2)

        if user_volume:
            if not user_volume_owner_name:
                user_volume_owner_name = SciQuery.get_user().userName
            path = str(Path(Config.ComputeWorkDir, top_volume, user_volume_owner_name, user_volume, relative_path))
        else:
            path = str(Path(Config.ComputeWorkDir, top_volume, relative_path))
        return path if path.endswith("/") else path + "/"

    @staticmethod
    def find_file_service(file_service: Union[str, dict] = None) -> dict:
        if isinstance(file_service, dict):
            file_service = file_service.get("identifier")
        return _get_file_service(file_service)

    def __str__(self):
        return f"File Output of name = {self.name}, type= {self.output_type}, statement_indexes = " \
               f"{self.statement_indexes}"

    def __repr__(self):
        return f"FileOutput(name= {self.name}, type= {self.output_type}, statement_indexes = {self.statement_indexes})"


class DatabaseTableOutput(Output):
    """
    Defines the output of a database query into a database table
    """

    def __init__(self,
                 table: str = "resultTable",
                 database: str = None,
                 statement_indexes: Union[int, List[int]] = 1,
                 rdb_compute_domain: str = None,
                 schema: str = ""):

        """
        :param table: name of the output database table (string), such as "resultTable"
        :param database: name of the database (string) where the output table in created. If it is owned explicitly by
        a user, then it should follow the pattern "mydb:<username>"
        :param statement_indexes:  list of integers or integer. Each integer value denotes the index or position (>=1)
        :param rdb_compute_domain: name (string) of the relational database (RDB) compute domain that contains the
        database, or object of class RDBComputeDomain corresponding to it.
        Name of such domains available to the user is returned by the function Jobs.getRDBComputeDomainNames().
        :param schema: database schema (string)
        of the sql statements within the input query, whose resultset is going to be written into this OutputTarget
        """
        if type(table) != str or type(schema) != str:
            raise TypeError("Input parameter(s) 'table' or 'schema' should be of type string.")

        domain = RDBComputeDomains.get_default_rdb_compute_domain() if not rdb_compute_domain else rdb_compute_domain
        if not database:
            if type(domain) == str:
                database = SciQuery.get_rdb_compute_domains().get_rdb_compute_domain(domain).get_default_database().name
            else:
                database = domain.get_default_database().name
                domain = domain.name


        self.table = table
        self.database = database
        self.rdb_compute_domain_name = domain
        self.schema = schema
        name = ".".join([self.rdb_compute_domain_name, self.database, self.schema, self.table])
        super().__init__(name, OutputType.DATABASE_TABLE, statement_indexes)

    @classmethod
    def get_default(cls):
        """
        Gets a OutputTarget object filled with default values: JSON output file where only the 1st SQL statement of
        the query is written in it.
        """
        return cls(table = "resultTable",
                   database = RDBComputeDomains.get_default_rdb_compute_domain().get_default_database().name,
                   rdb_compute_domain = RDBComputeDomains.get_default_rdb_compute_domain().name,
                   schema = "",
                   statement_indexes = [1])

    def __str__(self):
        return "Database Table Output of table= {}, database= {}, rdb_compute_domain_name= {}, schema= {}, " \
               "statement_indexes= {}".format(self.table, self.database, self.rdb_compute_domain_name, self.schema,
                                              self.statement_indexes)

    def __repr__(self):
        return "DatabaseTableOutput(table= {}, database= {}, rdb_compute_domain_name= {}, schema= {}, " \
               "statement_indexes= {})".format(self.table, self.database, self.rdb_compute_domain_name, self.schema,
                                               self.statement_indexes)


class Outputs(list):
    """
    Contains a list of output objects, defining database query resultset outputs.
    """

    def __init__(self, *outputs):
        super().__init__()
        for output in outputs:
            outs = output if isinstance(output, Iterable) else [output]
            for out in outs:
                self.append(out)

    def append(self, obj):
        if isinstance(obj, Output):
            super().append(obj)
        else:
            raise NameError("Input object is not a subclass of the 'Output' class.")

    def get_target_list(self, file_base_path: str = None, file_service: str = None):
        targets = []
        fs = FileOutput.find_file_service(file_service)
        for output in self:
            for index in output.statement_indexes:
                location = output.name
                if output.output_type != OutputType.DATABASE_TABLE: # for files
                    if not output.file_service_identifier:
                        file_service_identifier = fs.get('identifier')
                    else:
                        file_service_identifier = output.file_service_identifier

                    if not output.file_base_path:
                        if file_base_path:
                            location = file_base_path.rstrip("/") + "/" + output.file
                        else:
                            location = FileOutput.build_file_base_path().rstrip("/") + "/" + output.file
                    else:
                        location = output.file_base_path.rstrip("/") + "/" + output.file

                    location = file_service_identifier + ":" + location;
                targets.append({'location': location, 'type': output.output_type, 'resultNumber': index})
        return targets

    @staticmethod
    def get_default():
        return Outputs(FileOutput(name="result.json", output_type=OutputType.FILE_JSON, statement_indexes = [1]))


class RDBJob:
    """
    Contains the definition of a job consisting on a query run in a Relational Database (RDB)
    """
    _JOB_STATUS_MAP = {1: "PENDING", 2: "QUEUED", 4: "ACCEPTED", 8: "STARTED", 16: "FINISHED", 32: "SUCCESS",
                       64: "ERROR", 128: "CANCELED"}

    def __init__(self, job):
        """
        :param job: can be the job ID (string), or a dictionary containing all the attributes of an RDBJob object.
        """
        if type(job) != dict:
            job = Jobs.getJobDescription(job)

        self.id = job.get('id')
        self.alias = job.get('submitterDID') if job.get('submitterDID') is not None else job.get('alias')
        self._submitter_trust_id = job.get('submitterTrustId') if job.get('submitterTrustId') is not None else \
            job.get('_submitter_trust_id')
        self._run_by_uuid = job.get('runByUUID') if job.get('runByUUID') is not None else job.get('_run_by_uuid')
        self.submission_time = self._get_datetime(job.get('submissionTime') if job.get('submissionTime') is not None
                                                  else job.get('submission_time'))
        self.start_time = self._get_datetime(job.get('startTime') if job.get('startTime') is not None
                                             else job.get('start_time'))
        self.end_time = self._get_datetime(job.get('endTime') if job.get('endTime') is not None
                                           else job.get('end_time'))
        self.duration = job.get('duration')
        self.timeout = job.get('timeout')
        self._messages = job.get('messages') if job.get('messages') is not None else job.get('_messages')
        self.message_list = [m.get("content") for m in self._messages] if self._messages is not None else []
        self.status = job.get('status')
        self.status_string = RDBJob.get_job_status(job.get('status'))
        self._results_folder_uri = job.get('resultsFolderURI') if job.get('resultsFolderURI') is not None \
            else job.get('_results_folder_uri')
        self._type = job.get('type') if job.get('type') is not None else job.get('_type')
        self.user_name = job.get('username') if job.get('username') is not None else job.get('user_name')
        self.input_sql = job.get('inputSql') if job.get('inputSql') is not None else job.get('input_sql')
        self.targets = job.get('targets')
        self.database_name = job.get('databaseContextName') \
            if job.get('databaseContextName') is not None else job.get('database_name')
        self._rdb_resource_context_uuid = job.get('rdbResourceContextUUID') \
            if job.get('rdbResourceContextUUID') is not None else job.get('_rdb_resource_context_uuid')
        self.rdb_compute_domain_name = job.get('rdbDomainName') if job.get('rdbDomainName') is not None \
            else job.get('rdb_compute_domain_name')
        self.rdb_compute_domain_id = job.get('rdbDomainId') if job.get('rdbDomainId') is not None \
            else job.get('rdb_compute_domain_id')
        self.outputs = self._get_outputs()
        self.get_job_status = self._get_job_status_string

    def get_metadata(self, result_format="pandas") -> pd.DataFrame:
        data = []
        column_names = []
        if result_format == "pandas":
            for attr, value in self.__dict__.items():
                import inspect
                if not attr.startswith("_") and not inspect.ismethod(value): # no private members nor methods
                    data.append(value)
                    column_names.append(attr)
            df = pd.DataFrame([data], columns=column_names)
            df.name = str(RDBJob)
            return df
        elif result_format == "dict":
            return self.__dict__
        else:
            raise Exception("Invalid value for input parameter 'result_format'.")

    @staticmethod
    def get_job_status(status: int) -> str:
        return RDBJob._JOB_STATUS_MAP.get(status)

    @staticmethod
    def get_job(job_id: int):
        return RDBJob(Jobs.getJobDescription(job_id))

    def cancel(self):
        Jobs.cancelJob(self.id)

    def refresh(self):
        self.__init__(Jobs.getJobDescription(self.id))

    def _get_job_status_string(self) -> str:
        return RDBJob.get_job_status(self.status)

    def _get_outputs(self) -> Outputs:
        output_targets = {}
        for t in self.targets:
            i = (t['location'], t['type'])
            if i not in output_targets:
                output_targets[i] = [t['resultNumber']]
            else:
                output_targets[i].append(t['resultNumber'])

        outputs = Outputs()
        for k in output_targets:
            if k[1] == OutputType.DATABASE_TABLE:
                p = k[0].split(".")
                outputs.append(DatabaseTableOutput(table=p[3], database=p[1], rdb_compute_domain=p[0], schema=p[2],
                                                   statement_indexes=output_targets[k]))
            else:
                file_parts = k[0].split(":")
                file_service_identifier = file_parts[0]
                name = file_parts[1]
                outputs.append(FileOutput(name=name, output_type=k[1], statement_indexes=output_targets[k],
                                          file_service=file_service_identifier))

        return outputs

    def _get_output_from_index(self, ind: int):
        if ind > len(self.outputs) - 1:
            raise ValueError("Index is outside of the index range in the outputs list.")
        return self.outputs[ind]

    def get_output_path(self, output: Union[Output, int] = 0) -> str:
        out = self._get_output_from_index(output) if isinstance(output, int) else output
        if out.output_type == OutputType.DATABASE_TABLE:
            raise TypeError("Output is not a file but a database")
        return out.get_path()

    def get_output_as_string(self, output: Union[Output, int, str] = None):
        if not isinstance(output, str):
            out = self._get_output_from_index(output) if isinstance(output, int) else output
            file_path = self.get_output_path(out)
        else:
            file_path = output

        if Config.isSciServerComputeEnvironment():
            with open(file_path, ) as f:
                data = f.read()
        else:
            if isinstance(output, str):
                raise Exception(f"Cannot find file_path {output} in local file system.")

            fs = FileOutput.find_file_service(out.file_service_identifier)
            path = out.file_service_path
            data = Files.download(fs, path, format="txt", quiet=True)

        return data

    def get_json_output(self, output: Union[Output, int, str] = 0) -> dict:
        data_dict = json.loads(self.get_output_as_string(output))
        return data_dict.get("Result")

    def get_dataframe_from_output(self, output: Union[Output, int] = 0, result_index: int = 0) -> list:
        out = self._get_output_from_index(output) if isinstance(output, int) else output
        if out.output_type == OutputType.FILE_JSON:
            result = self.get_json_output(out)[result_index]
            df = pd.DataFrame(result['Data'], columns=result['ColumnNames'])
            df.name = result['TableName']
        elif out.output_type == OutputType.FILE_CSV:
            df = pd.read_csv(out.get_path(), skiprows=1)
        elif out.output_type == OutputType.DATABASE_TABLE:
            sq = SciQuery(rdb_compute_domain=out.rdb_compute_domain_name, database=out.database)
            query = f"select * from {out.table};"
            df = sq.execute_query(query)
        else:
            raise Exception(f"Output type {out.output_type} not supported")
        return df

    def _get_datetime(self, time):
        return datetime.fromtimestamp(time / 1000.0) if time is not None else None

    def __str__(self):
        return "RDB Job of id={}".format(self.id)

    def __repr__(self):
        return "RDBJob(id={})".format(self.id)


class Database:
    """
    Defines a database context where users can run sql queries.
    """

    def __init__(self, rdb_compute_domain: Union[str, int, dict], database: Union[str, int, dict]):
        """
        :param rdb_compute_domain: Parameter that identifies the relation database domain or environment that
        contains the database. Could be either its name (string), ID (integer), or a dictionary containing
        the attributes of the domain.
        :param database: defines the database. Can be either the database name (string), ID (integer), or a dictionary
        containing all the attributes of an object of class Database.
        """
        if type(database) not in [str, int, dict]:
            raise TypeError("Invalid type for input parameter 'database'.")

        if type(rdb_compute_domain) not in [str, int, dict]:
            raise TypeError("Invalid type for input parameter 'rdb_compute_domain'.")

        if type(rdb_compute_domain) != dict:
            domain = RDBComputeDomain(rdb_compute_domain)
        else:
            domain = rdb_compute_domain

        if type(database) != dict:
            dbs = domain.get('databaseContexts') or domain.get('databases')
            if type(database) == str:
                database = [db for db in dbs if db.get('name') == database]
            else:
                database = [db for db in dbs if db.get('id') == database]

            if len(database) == 0:
                raise NameError("Unable to find database.")
            else:
                database = database[0]

        self.id = database.get('id')
        self._racm_id = database.get('_racm_id') if database.get('_racm_id') is not None else database.get('racmId')
        self.name = database.get('name') if database.get('name') is not None else database.get('contextName')
        self.description = database.get('description')
        self.vendor = database.get('vendor')
        self.schemas = database.get('dbSchemas') if database.get('dbSchemas') is not None else database.get('schemas')
        self.rdb_compute_domain_name = domain.get('name') if domain.get('name') is not None else \
            domain.get('displayName')
        self.rdb_compute_domain_id = domain.get('id')

    def get_metadata(self) -> pd.DataFrame:
        data = []
        column_names = ['database_name', 'database_description', 'database_vendor', 'database_id',
                        'rdb_compute_domain_name', 'rdb_compute_domain_id']
        data.append([self.name, self.description, self.vendor, self.id, self.rdb_compute_domain_name,
                     self.rdb_compute_domain_id])
        df = pd.DataFrame(data=data, columns=column_names, index=[self.id])
        df = df.astype({"rdb_compute_domain_id": int, "database_id": int})
        return df

    def __str__(self):
        return "Database of name={}, id={} and rdb_compute_domain_name={}".format(self.name, self.id,
                                                                                  self.rdb_compute_domain_name)

    def __repr__(self):
        return "Database(name={}, id={}, rdb_compute_domain_name={})".format(self.name, self.id,
                                                                                self.rdb_compute_domain_name)


class RDBComputeDomain:
    """
    Defines a domain or environment with databases that users are able to query.
    """

    def __init__(self, rdb_compute_domain: Union[str, int, dict]):
        """
        Creates an instance of an RDBComputeDomain, which defines a domain or environment with databases that users.
        are able to query.
        :param rdb_compute_domain: Parameter that identifies the domain. Could be either its name (string),
        ID (integer), or a dictionary containing all the attributes of the domain.
        """
        if type(rdb_compute_domain) not in [str, int, dict]:
            raise TypeError("Invalid type for input parameter 'rdb_compute_domain'.")

        if type(rdb_compute_domain) != dict:
            domains = SciQuery.get_rdb_compute_domains("dict")
            if type(rdb_compute_domain) == str:
                domain = [d for d in domains if d.get('name') == rdb_compute_domain]
            elif type(rdb_compute_domain) == int:
                domain = [d for d in domains if d.get('id') == rdb_compute_domain]
            else:
                raise TypeError("Invalid type for input parameter 'rdb_compute_domain'.")

            if len(domain) > 0:
                rdb_compute_domain = domain[0]
            else:
                raise NameError("Unable to find rdbComputeDomain {0}.".format(rdb_compute_domain))

        self.id = rdb_compute_domain.get('id')
        self._racm_id = rdb_compute_domain.get('_racm_id') if rdb_compute_domain.get('_racm_id') is not None else \
            rdb_compute_domain.get('racmId')
        self.name = rdb_compute_domain.get('name') if rdb_compute_domain.get('name') is not None else \
            rdb_compute_domain.get('displayName')
        self.description = rdb_compute_domain.get('description')
        dbs = []
        databases = rdb_compute_domain.get('dbContexts') if \
            rdb_compute_domain.get('dbContexts') is not None else rdb_compute_domain.get('databases')
        for db_name, db_dict in databases.items():
            dbs.append(Database(rdb_compute_domain, db_dict))
        self.databases = dbs

    def get_database_names(self) -> list:
        """
        Gets a list of the names of databases in an RDBComputeDomain

        :return: list of database names (strings)
        :example: dbnames = SciQuery.get_database_names(rdbComputeDomainName);
        .. seealso:: SciQuery.get_databases_metadata
        """
        return [db.name for db in self.databases]

    def get_database(self, database: Union[str, int, dict, Database]) -> Database:
        if type(database) == str:
            dbs = [db for db in self.databases if db.name == database]
        elif type(database) == int:
            dbs = [db for db in self.databases if db.id == database]
        elif type(database) == dict:
            dbs = [db for db in self.databases if db.id == database.get('id')]
        elif isinstance(database, Database):
            return self.get_database(database.id)
        else:
            raise TypeError("Invalid type for input parameter 'database'.")
        if len(dbs) == 0:
            raise NameError("Database not found in list of available databases.")
        else:
            return dbs[0]

    def get_default_database(self) -> Database:
        dbs = [db for db in self.databases if db.name == SciQuery.get_mydb_name()]
        if len(dbs) > 0:
            return dbs[0]
        elif len(self.databases) > 0:
            return self.databases[0]
        else:
            raise Exception("No default database available.")

    def get_metadata(self, do_include_databases: bool = False) -> pd.DataFrame:

        column_names = ['rdb_compute_domain_name', 'rdb_compute_domain_description', 'rdb_compute_domain_id']
        data = [[self.name, self.description, self.id]]
        domain_metadata = pd.DataFrame(data=data, columns=column_names)

        if do_include_databases:
            db_metadata = self.get_databases_metadata()
            # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.join.html
            domain_metadata = pd.merge(domain_metadata, db_metadata, how="outer",
                                       left_on=["rdb_compute_domain_id", "rdb_compute_domain_name"],
                                       right_on=["rdb_compute_domain_id", "rdb_compute_domain_name"])
            domain_metadata.sort_values(by=['rdb_compute_domain_name', 'database_name'], inplace=True)
            domain_metadata = domain_metadata.astype({"database_id": int})
        else:
            domain_metadata.sort_values(by=['rdb_compute_domain_name'], inplace=True)

        domain_metadata = domain_metadata.astype({"rdb_compute_domain_id": int})
        return domain_metadata

    def get_databases_metadata(self) -> pd.DataFrame:
        """
        Gets metadata of the databases in this RDBComputeDomain.

        :return: pandas dataframe with associated metadata.
        .. seealso:: SciQuery.get_database_names
        """
        dfs = [db.get_metadata() for db in self.databases]
        dfs = pd.concat(dfs, ignore_index=True)
        dfs.sort_values(by="database_name", inplace=True)
        return dfs

    def __str__(self):
        return "RDBComputeDomain of name={} and id={}".format(self.name, self.id)

    def __repr__(self):
        return "RDBComputeDomain(name={}, id={})".format(self.name, self.id)


class RDBComputeDomains(list):
    """
    Defines a list of RDBComputeDomains, which are domains or environments with databases that users are able to query.
    """
    def __init__(self, rdb_compute_domains: Union[Iterable, RDBComputeDomain]):
        """
        :param rdb_compute_domains: Parameter that identifies a list of RDBComputeDomain objects.
        Could be either single RDBComputeDomain object, or an iterable containing multiple RDBComputeDomain objects.
        """
        super().__init__()
        domains = rdb_compute_domains if isinstance(rdb_compute_domains, Iterable) else [rdb_compute_domains]
        for d in domains:
            if isinstance(d, RDBComputeDomain):
                self.append(d)
            else:
                raise NameError("Input object is not of class RDBComputeDomain.")

    def get_rdb_compute_domain(self, rdb_compute_domain: Union[str, int, dict, RDBComputeDomain]) -> RDBComputeDomain:
        if type(rdb_compute_domain) == str:
            domains = [d for d in self if d.name == rdb_compute_domain]
        elif type(rdb_compute_domain) == int:
            domains = [d for d in self if d.id == rdb_compute_domain]
        elif type(rdb_compute_domain) == dict:
            domains = [d for d in self if d.id == rdb_compute_domain.get('id')]
        elif isinstance(rdb_compute_domain, RDBComputeDomain):
            return self.get_rdb_compute_domain(rdb_compute_domain.id)
        else:
            raise TypeError("Invalid type for input parameter 'rdb_compute_domain'.")

        if len(domains) == 0:
            raise NameError("RDBComputeDomain not found in list of available rdbComputeDomains")
        else:
            return domains[0]

    def get_default_rdb_compute_domain(self) -> RDBComputeDomain:
        domains = [domain for domain in self if len(domain.databases) > 0]
        if len(domains) > 0:
            doms = [dom for dom in domains if dom.get_default_database().name == SciQuery.get_mydb_name()]
            if len(doms) > 0:
                return doms[0]
            return domains[0]
        else:
            raise Exception("No RDBComputeDomain available with a database.")


class SciQuery:
    """
    Instance of the SciQuery app for querying relational databases.
    """
    def __init__(self,
                 rdb_compute_domain: Union[str, int, dict, RDBComputeDomain] = None,
                 database: Union[str, int, dict, Database] = None,
                 file_service: Union[str, dict] = None,
                 results_base_path: str = None,
                 outputs: Outputs = None,
                 verbose: bool = True,
                 hard_fail: bool = False):
        """
        Creates instance of SciQuery class.

        :param rdb_compute_domain: defines a domain or environment of multiple databases where users can run queries.
        Can be either the domain's name (string), ID (integer), an object of class RDBComputeDomain, or a dictionary
        containing all the attributes of an object of class RDBComputeDomain. If set to None, a default value will be
        assigned to it.
        :param database: defines the database where the queries are executed in.
        Can be either the database name (string), ID (integer), an object of class Database, or a dictionary containing
        all the attributes of an object of class Database. If set to None, a default value will be assigned to it.
        :param file_service: a File Service defines an available file system where query result sets can be written
        into. This parameter can be it name or identifier (string), or a dictionary defining a file service.
        If set to None, a default value will be assigned to it.
        :param results_base_path: base path (string) of the directory where the query results are written into.
        Can be constructed by using FileOutput.build_file_base_path(). If set to None, a default value will be assigned
        to it at the moment of running a sql query.
        :param outputs: Defines the query(ies) output(s). Can be an object derived from the Output base class (such as
        FileOutput or DatabaseTableOutput), or a list of those. If set to None, a default value (json file output)
        will be assigned to it.
        :param verbose: Boolean parameter. If True, warning messages will be written in case of errors, in the case when
        the hard_fail parameter is set to False. If False, nothing will be written.
        :param hard_fail: Boolean parameter. If True, exceptions will be raised in case of errors during instantiation.
        If False, then no exceptions are raised, and warnings might be showed instead
        (depending on the value of the verbose parameter).
        """

        self.user = SciQuery.get_user()
        self.verbose = verbose
        self.hard_fail = hard_fail
        self._file_service = None
        self._results_base_path = None
        self._outputs = None
        self._rdb_compute_domains = None
        self._rdb_compute_domain = None
        self._database = None
        self.refresh_date = None
        self.set(rdb_compute_domain, database, file_service, results_base_path, outputs, verbose, hard_fail)

    @staticmethod
    def get_token() -> str:
        token = Authentication.getToken()
        if token is None or token == "":
            raise Exception("User not has not logged into SciServer. Use 'Authentication.login'.")
        return token

    @staticmethod
    def get_user() -> Authentication.KeystoneUser:
        return Authentication.getKeystoneUserWithToken(SciQuery.get_token())

    def set(self,
            rdb_compute_domain: Union[str, int, dict, RDBComputeDomain] = None,
            database: Union[str, int, dict, Database] = None,
            file_service: Union[str, dict] = None,
            results_base_path: str = None,
            outputs: Outputs = None,
            verbose: bool = None,
            hard_fail: bool = None):
        """
        Sets or refreshes the parameters in the SciQuery object, all at once.

        :param rdb_compute_domain: defines a domain or environment of multiple databases where users can run queries.
        Can be either the domain's name (string), ID (integer), an object of class RDBComputeDomain, or a dictionary
        containing all the attributes of an object of class RDBComputeDomain. If set to None, the current value
        is refreshed.
        :param database: defines the database where the queries are executed in.
        Can be either the database name (string), ID (integer), an object of class Database, or a dictionary containing
        all the attributes of an object of class Database. If set to None, the current value is refreshed.
        :param file_service: a File Service defines an available file system where query result sets can be written
        into. This parameter can be it name or identifier (string), or a dictionary defining a file service.
        If set to None, the current value is refreshed.
        :param results_base_path: base path (string) of the directory where the query results are written into.
        Can be constructed by using FileOutput.build_file_base_path().
        :param outputs: Defines the query(ies) output(s). Can be a list of Output objects,
        or a single object of class Outputs. If set to None, a default value (json file output) will be assigned to it.
        :param verbose: Boolean parameter. If True, warning messages will be written in case of errors, in the case when
        the hard_fail parameter is set to False. If False, nothing will be written.
        :param hard_fail: Boolean parameter. If True, exceptions will be raised in case of errors during instantiation.
        If False, then no exceptions are raised, and warnings might be showed instead
        (depending on the value of the verbose parameter).
        """

        self.verbose = verbose if verbose else self.verbose
        self.hard_fail = hard_fail if hard_fail else self.hard_fail

        # set or refresh current _rdb_compute_domains
        try:
            self.rdb_compute_domains = SciQuery.get_rdb_compute_domains('class')
        except Exception as ex:
            self._handle_exception(NameError(ex), "Unable to set or refresh rdb_compute_domains.")
            # nothing else to do:
            return

        try:
            if self.rdb_compute_domain is None:
                self.rdb_compute_domain = rdb_compute_domain if rdb_compute_domain else \
                    self.get_default_rdb_compute_domain()
            else:
                self.rdb_compute_domain = rdb_compute_domain if rdb_compute_domain else self.rdb_compute_domain
        except Exception as ex:
            self._handle_exception(NameError(ex), "Unable to set or refresh rdb_compute_domain.")

        try:
            if self.database is None:
                self.database = database if database else self.get_default_database()
            else:
                self.database = database if database else self.database
        except Exception as ex:
            self._handle_exception(NameError(ex), "Unable to set or refresh database.")

        try:
            if self.file_service is None:
                self.file_service = file_service if file_service else self.get_default_file_service()
            else:
                self.file_service = file_service if file_service else self.file_service
        except Exception as ex:
            self._handle_exception(NameError(ex), "Unable to set or refresh file_service.")

        try:
            if self.outputs is None:
                self.outputs = outputs if outputs else self.get_default_outputs()
            else:
                self.outputs = outputs if outputs else self.outputs
        except Exception as ex:
            self._handle_exception(NameError(ex), "Unable to set or refresh outputs.")

        try:
            self.results_base_path = results_base_path
        except Exception as ex:
            self._handle_exception(NameError(ex), "Unable to set results_base_path.")

        self.refresh_date = datetime.now()

    def _handle_exception(self, exception: Exception, extra_message: str = ""):

        message = extra_message + " Error: " + str(exception) if extra_message else str(exception)
        if self.hard_fail:
            exception.message = message
            raise exception
        elif self.verbose:
            warnings.warn(message)

    def refresh(self):
        self.set(verbose=self.verbose, hard_fail=self.hard_fail)

    @staticmethod
    def get_mydb_name(owner_name: str = None) -> str:
        if not owner_name:
            owner_name = SciQuery.get_user().userName
        return "mydb:" + owner_name

    @staticmethod
    def get_rdb_compute_domains(result_format: str = 'class') -> RDBComputeDomains:
        """
        Gets a list of all registered Relational Database (RDB) compute domains that the user has access to.

        :param result_format: If set to "class", then the returned value will be of class RDBComputeDomains.
        If set to "dict", then the return value will be a list of dictionaries, each of them containing the attributes
        of an RDBComputeDomain object.
        :return: an object of class RDBComputeDomains, or a list of dictionaries, each of them containing the attributes
        of an RDBComputeDomain object.
        :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that
        purpose). Throws an exception if the HTTP request to the JOBM API returns an error.
        :example: rdb_compute_domains = SciQuery.get_rdb_compute_domains();
        """
        token = SciQuery.get_user().token

        if Config.isSciServerComputeEnvironment():
            task_name = "Compute.SciScript-Python.SciQuery.get_rdb_compute_domains"
        else:
            task_name = "SciScript-Python.SciQuery.get_rdb_compute_domains"

        url = Config.SciqueryURL + "/api/info/domain?TaskName=" + task_name
        headers = {'X-Auth-Token': token, "Content-Type": "application/json"}
        res = requests.get(url, headers=headers, stream=True)
        if res.status_code != 200:
            raise Exception(
                "Error when getting RDB Compute Domains from JOBM API.\nHttp Response from JOBM API returned"
                " status code " + str(res.status_code) + ":\n" + res.content.decode())
        else:
            arr = json.loads(res.content.decode())
            if result_format == 'class':
                return RDBComputeDomains([RDBComputeDomain(d) for d in arr])
            else:
                return arr

    # rdb_compute_domains ---------------------------------------------------

    @property
    def rdb_compute_domains(self) -> RDBComputeDomains:
        return self._rdb_compute_domains

    @rdb_compute_domains.setter
    def rdb_compute_domains(self, rdb_compute_domains: RDBComputeDomains):
        if not isinstance(rdb_compute_domains, RDBComputeDomains):
            raise Exception("'rdb_compute_domains' should be of class RDBComputeDomains.")
        self._rdb_compute_domains = rdb_compute_domains

    # rdb_compute_domain ---------------------------------------------------

    @property
    def rdb_compute_domain(self) -> RDBComputeDomain:
        return self._rdb_compute_domain

    @rdb_compute_domain.setter
    def rdb_compute_domain(self, rdb_compute_domain: Union[str, int, dict, RDBComputeDomain]):
        if rdb_compute_domain is None:
            raise Exception("'rdb_compute_domain' cannot be set to None.")
        self._rdb_compute_domain = self.get_rdb_compute_domain(rdb_compute_domain)

    def get_rdb_compute_domain(self, rdb_compute_domain: Union[str, int, dict, RDBComputeDomain] = None) \
            -> RDBComputeDomain:
        """
        Returns an object of class RDBComputeDomain, either defined by the input name or identifiers, or that
        which is set in the SciQuery instance.

        :param rdb_compute_domain: defines a domain or environment of multiple databases where users can run queries.
        Can be either the domain's name (string), ID (integer), an object of class RDBComputeDomain, or a dictionary
        containing all the attributes of an object of class RDBComputeDomain. If set to None, then the currently set
        value of rdb_compute_domain in the SciQuery object is returned.
        :return: Object of class RDBComputeDomain.
        """
        if rdb_compute_domain is None:
            return self._rdb_compute_domain
        return self.rdb_compute_domains.get_rdb_compute_domain(rdb_compute_domain)

    def get_default_rdb_compute_domain(self):
        return self.rdb_compute_domains.get_default_rdb_compute_domain()

    # database ---------------------------------------------------

    @property
    def database(self) -> Database:
        return self._database

    @database.setter
    def database(self, database: Union[str, int, dict, Database]):
        if database is None:
            raise Exception("'database' cannot be set to None.")
        self._database = self.get_database(database, self.rdb_compute_domain)

    def get_database(self,
                     database: Union[str, int, dict, Database] = None,
                     rdb_compute_domain: Union[str, int, dict, RDBComputeDomain] = None) -> Database:
        """
        Returns an object of class Database, either defined by the input name or identifiers, or that
        which is set in the SciQuery instance.


        :param database: identifies the database, which this function returns as an object of class Database.
        Can be either the database name (string), ID (integer), an object of class Database, or a dictionary containing
        all the attributes of an object of class Database. If set to None, then the currently set value of database in
        the SciQuery object is returned.
        :param rdb_compute_domain: defines a domain or environment of multiple databases where users can run queries,
        and that contains the database. Can be either the domain's name (string), ID (integer), an object of class
        RDBComputeDomain, or a dictionary containing all the attributes of an object of class RDBComputeDomain.
        If set to None, then the currently set value of rdb_compute_domain in the SciQuery object is internally used.
        :return: Object of class Database
        """
        if database is None:
            return self._database
        return self.get_rdb_compute_domain(rdb_compute_domain).get_database(database)

    def get_default_database(self, rdb_compute_domain: Union[str, int, dict, RDBComputeDomain] = None) -> Database:
        domain = self.get_default_rdb_compute_domain() if rdb_compute_domain is None \
            else self.get_rdb_compute_domain(rdb_compute_domain)
        return domain.get_default_database()

    # file_service ---------------------------------------------------

    @property
    def file_service(self) -> dict:
        return self._file_service

    @file_service.setter
    def file_service(self, file_service: Union[str, dict]):
        if file_service is None:
            raise Exception("'file_service' cannot be set to None.")
        self._file_service = self.get_file_service(file_service)

    def get_file_service(self, file_service: Union[str, dict] = None) -> dict:
        """
        Returns the definition of a file service as a dictionary, either defined by the input name or identifiers,
        or that which is set in the SciQuery instance.

        :param file_service: name or identifier (string) of a file service, or the dictionary with its definition.
        If set to None, then the currently set value of file_service in the SciQuery object is returned.
        :return: dictionary with the definition of a file service.
        """
        if file_service is None:
            return self._file_service
        return FileOutput.find_file_service(file_service)

    def get_default_file_service(self) -> dict:
        return FileOutput.find_file_service()

    # results_base_path ---------------------------------------------------

    @property
    def results_base_path(self) -> str:
        return self._results_base_path

    @results_base_path.setter
    def results_base_path(self, results_base_path: str):
        # if results_base_path is None or not results_base_path.startswith(Config.ComputeWorkDir):
        #    raise Exception(f"The string 'results_base_path' must start with {Config.ComputeWorkDir}")
        self._results_base_path = results_base_path

    def get_results_base_path(self) -> str:
        return self._results_base_path

    def get_default_results_base_path(self, add_date_ending=True) -> str:
        return FileOutput.build_file_base_path(add_date_ending=add_date_ending)

    # outputs -------------------------------------------------------------

    @property
    def outputs(self) -> Outputs:
        return self._outputs

    @outputs.setter
    def outputs(self, outputs: Union[Outputs, Output]):
        if outputs is None:
            raise Exception("'outputs' cannot be set to None.")
        self._outputs = self.get_outputs(outputs)

    def get_outputs(self, outputs: Union[Outputs, Output] = None) -> Outputs:
        """
        Returns an object of class Outputs, either defined by the inputs parameters, or that
        which is set in the SciQuery instance.

        :param outputs: object of class Outputs, or iterable of output objects. If set to None, then the currently
        set value of outputs in the SciQuery object is returned.
        :return: object of class Outputs.
        """
        if outputs is None:
            return self.outputs
        return Outputs(outputs)

    def get_default_outputs(self) -> Outputs:
        return Outputs.get_default()

    # ---------------------------------------------------------------------------------------------
    # Running Queries -----------------------------------------------------------------------------
    # ---------------------------------------------------------------------------------------------

    def submit_query_job(self,
                         sql_query: str,
                         database: Union[str, int, dict, Database] = None,
                         outputs: Union[Outputs, Output] = None,
                         results_base_path: str = None,
                         rdb_compute_domain: Union[str, int, dict, RDBComputeDomain] = None,
                         file_service: str = None,
                         job_alias: str = "") -> int:
        """
        Submits a sql query for execution (as an asynchronous job) inside a relational database (RDB) compute domain.

        :param sql_query: sql query (string)
        :param database: defines the database where the sql query is executed in.
        Can be either the database name (string), ID (integer), an object of class Database, or a dictionary containing
        all the attributes of an object of class Database. If set to None, then the current value of the database field
        in this SciQuery instance will be used.
        :param outputs: Defines the query(ies) output(s). Can be an object derived from the Output base class (such as
        FileOutput or DatabaseTableOutput), or a list of those. If set to None, then the current value of the outputs
        field in this SciQuery instance will be used.
        :param results_base_path: full path to results folder (string) where query output tables are written into.
        E.g.: /home/idies/workspace/rootVOlume/username/userVolume/jobsFolder . If set to None, then its current value
        in this SciQuery instance will be used. If that value is None, then a default folder will be set automatically.
        :param rdb_compute_domain: defines a domain or environment of multiple databases where users can run queries,
        and that contains the database. Can be either the domain's name (string), ID (integer), an object of class
        RDBComputeDomain, or a dictionary containing all the attributes of an object of class RDBComputeDomain.
        If set to None, then the currently set value of rdb_compute_domain in the SciQuery object is internally used.
        :param file_service: a File Service defines an available file system where query result sets can be written
        into. This parameter can be its name or identifier (string), or a dictionary defining a file service.
        If set to None, then the currently set value of file_service in the SciQuery object is internally used.
        :param job_alias: alias (string) of job, defined by the user.
        :return: the ID (string) that labels the job.
        """

        domain = self.get_rdb_compute_domain(rdb_compute_domain)
        db = self.get_database(database, domain)
        fs = self.get_file_service(file_service)
        outputs = self.get_outputs(outputs)
        results_base_path = results_base_path if results_base_path else self.get_results_base_path()
        if not results_base_path:
            results_base_path = self.get_default_results_base_path()

        targets = outputs.get_target_list(results_base_path, fs.get('identifier'))

        job_model = {
            "inputSql": sql_query,
            "submitterDID": job_alias,
            "databaseContextName": db.name,
            "rdbDomainId": domain.id,
            "targets": targets,
            "resultsFolderURI": fs['identifier'] + ":" + results_base_path
        }

        if Config.isSciServerComputeEnvironment():
            task_name = "Compute.SciScript-Python.SciQuery.submit_query_job"
        else:
            task_name = "SciScript-Python.SciQuery.submit_query_job"

        data = json.dumps(job_model).encode()
        url = Config.SciqueryURL + "/api/jobs/" + str(domain._racm_id) + "?TaskName=" + task_name;
        headers = {'X-Auth-Token': self.user.token, "Content-Type": "application/json"}
        res = requests.post(url, data=data, headers=headers, stream=True)
        if res.status_code < 200 or res.status_code >= 300:
            raise Exception("Error when submitting a job to the SciQuery API.\nHttp Response from SciQuery API " +
                            "returned status code " + str(res.status_code) + ":\n" + res.content.decode());
        else:
            return res.content.decode()

    def execute_query(self,
                      sql_query,
                      database: Union[str, int, dict, Database] = None,
                      results_base_path: str = None,
                      rdb_compute_domain: Union[str, int, dict, RDBComputeDomain] = None,
                      job_alias: str = "",
                      poll_time: float = 1.0,
                      file_service: str = None) -> pd.DataFrame:
        """
        Returns the query result (as a Pandas data frame) of a sql query submitted as a job to a
        relational database (RDB) compute domain.

        :param sql_query: sql query (string)
        :param database: defines the database where the sql query is executed in.
        Can be either the database name (string), ID (integer), an object of class Database, or a dictionary containing
        all the attributes of an object of class Database. If set to None, then the current value of the database field
        in this SciQuery instance will be used.
        :param results_base_path: full path to results folder (string) where query output tables are written into.
        E.g.: /home/idies/workspace/rootVOlume/username/userVolume/jobsFolder . If set to None, then its current value in
        this SciQuery instance will be used. If that value is None, then a default folder will be set automatically.
        :param rdb_compute_domain: defines a domain or environment of multiple databases where users can run queries,
        and that contains the database. Can be either the domain's name (string), ID (integer), an object of class
        RDBComputeDomain, or a dictionary containing all the attributes of an object of class RDBComputeDomain.
        If set to None, then the currently set value of rdb_compute_domain in the SciQuery object is internally used.
        :param job_alias: alias (string) of job, defined by the user.
        :param poll_time: time (float) in seconds between consecutive requests for updates in the jobs status.
        :param file_service: a File Service defines an available file system where query result sets can be written
        into. This parameter can be its name or identifier (string), or a dictionary defining a file service.
        If set to None, then the currently set value of file_service in the SciQuery object is internally used.
        :return: Pandas data frame containing the result of the query.
        """
        output = FileOutput("result1.json", OutputType.FILE_JSON, 1)
        job_alias = job_alias if job_alias else "synchronous query"
        job_id = self.submit_query_job(sql_query=sql_query, rdb_compute_domain=rdb_compute_domain, database=database,
                                       outputs=output,
                                       results_base_path=results_base_path,
                                       job_alias=job_alias,
                                       file_service=file_service)

        job = self.wait_for_job(job_id, verbose=False, poll_time=poll_time)
        if job.status > 32:
            messages = ". ".join(job.message_list)
            if (job.status == 64):
                raise Exception("Query ended with an error. " + messages)
            if (job.status == 128):
                raise Exception("Query was cancelled. " + messages)

        df = job.get_dataframe_from_output(0)
        return df

    @staticmethod
    def get_jobs_list(top=5, open=None, start=None, end=None, result_format="pandas") \
            -> Union[pd.DataFrame, list]:
        """
        Gets the list of SciQuery Jobs submitted by the user.

        :param top: top number of jobs (integer) returned. If top=None, then all jobs are returned.
        :param open: If set to 'True', then only returns jobs that have not finished executing and wrapped up
        (status <= FINISHED). If set to 'False' then only returns jobs that are still running. If set to 'None',
        then returns both finished and unfinished jobs.
        :param start: The earliest date (inclusive) to search for jobs, in string format yyyy-MM-dd hh:mm:ss.SSS.
        If set to 'None', then there is no lower bound on date.
        :param end: The latest date (inclusive) to search for jobs, in string format yyyy-MM-dd hh:mm:ss.SSS.
        If set to 'None', then there is no upper bound on date.
        :param result_format: string defining the return format. "pandas" for a pandas dataframe and "list"
        for a list of RDBJob objects.
        :return: pandas dataframe, or list of RDBJob objects or, each containing the definition of a submitted job.
        """
        job_dict_list = Jobs.getJobsList(top=top, open=open, start=start, end=end, type='rdb')
        jobs = []
        for job_dict in job_dict_list:
            j = RDBJob(job_dict)
            jobs.append(j if format == "list" else j.get_metadata())
        if result_format == "pandas":
            jobs = pd.concat(jobs, ignore_index=True)
        return jobs

    @staticmethod
    def get_job(job_id):
        """
        Gets the definition of the job as a RDBJob object.

        :param job_id: Id of job
        :return: RDBJob object containing the description or definition of the job.
        """
        return RDBJob.get_job(job_id)

    @staticmethod
    def get_job_status(job_id):
        """
        Gets a dictionary with the job status as an integer value, together with its semantic meaning. The integer value is
        a power of 2, that is, 1:PENDING, 2:QUEUED, 4:ACCEPTED, 8:STARTED, 16:FINISHED, 32:SUCCESS, 64:ERROR, 128:CANCELED

        :param job_id: Id of job (integer).
        :return: dictionary with the integer value of the job status, as well as its semantic meaning.
        """
        return Jobs.getJobStatus(job_id)

    @staticmethod
    def cancel_job(job):
        """
        Cancels the execution of a job.

        :param job_id: Id of the job (integer)
        :raises: Throws an exception if the HTTP request to the Authentication URL returns an error. Throws an exception if
        the HTTP request to the JOBM API returns an error.
        :example: SciQuery.cancelJob(jobId);

        .. seealso:: SciQuery.get_job_status, SciQuery.getJobDescription
        """
        if isinstance(job, str):
            Jobs.cancelJob(job)
        elif isinstance(job, RDBJob):
            job.cancel()
        else:
            raise NameError("Invalid type for input parameter 'job'.")

    def wait_for_job(self, job_id, verbose = False, poll_time=1.0):
        """
        Queries the job status regularly and waits until the job is completed.

        :param job_id: id of job (integer)
        :param verbose: if True, will print "wait" messages on the screen while the job is still running. If False, it
        will suppress the printing of messages on the screen.
        :param poll_time: idle time interval (integer, in seconds) before querying again for the job status. Minimum
        value allowed is 0.1 seconds.
        :return: After the job is finished, returns a dictionary object containing the job status.
        :raises: Throws an exception if the user is not logged into SciServer (use Authentication.login for that
        purpose).
        Throws an exception if the HTTP request to the JOBM API returns an error.
        :example: jobStatus = SciQuery.wait_for_job(jobId)

        .. seealso:: SciQuery.get_job_status, SciQuery.getJobDescription
        """
        #return Jobs.waitForJob(jobId=jobId, verbose=verbose, pollTime=poll_time)
        min_poll_time = 0.1 # in seconds
        while True:
            if verbose:
                print("Waiting...")
            job_desc = Jobs.getJobDescription(job_id)
            if job_desc.get("status") >= 32:
                if verbose:
                    print("Done!")
                return RDBJob(job_desc)
            else:
                time.sleep(max(min_poll_time, poll_time))

    ### METADATA

    def get_rdb_compute_domains_metadata(self, do_include_databases=False):
        """
        Gets metadata related to all relational database (RDB) compute domains (RDBComputeDomains) available.

        :param do_include_databases: Boolean parameter. If True, it will return metadata related to all available
        databases in each RDBComputeDomain as well.
        :return: pandas dataframe containing associated metadata.
        """
        dfs = []
        for domain in self.rdb_compute_domains:
            dfs.append(domain.get_metadata(do_include_databases))

        df = pd.concat(dfs, ignore_index=True)
        df.sort_values(by="rdb_compute_domain_name", inplace=True)
        return df

    def get_rdb_compute_domain_names(self):
        """
        Returns the names of the RDB compute domains available to the user.

        :return: an array of strings, each being the name of a rdb compute domain available to the user.
        """
        return [d.name for d in self.rdb_compute_domains]

    def get_rdb_compute_domain_metadata(self, rdb_compute_domain=None, do_include_databases=False):
        return self.get_rdb_compute_domain(rdb_compute_domain).get_metadata(do_include_databases)

    def get_databases_metadata(self, rdb_compute_domain=None):
        """
        Gets metadata (name and description) of databases in an RDBComputeDomain.
        """
        rdb_compute_domain = self.get_rdb_compute_domain(rdb_compute_domain)
        if isinstance(rdb_compute_domain, Iterable):
            dfs = [d.get_databases_metadata() for d in rdb_compute_domain]
            return pd.concat(dfs, ignore_index=True)
        else:
            return rdb_compute_domain.get_databases_metadata()

    def get_database_metadata(self, database=None, rdb_compute_domain=None):
        rdb_compute_domain = self.get_rdb_compute_domain(rdb_compute_domain)
        database = self.get_database(database, rdb_compute_domain)
        return database.get_metadata()

    def get_database_names(self, rdb_compute_domain=None):
        """
        Gets a list of the names of databases in an RDBComputeDomain

        :return: array of database names (strings)
        """
        rdb_compute_domain = self.get_rdb_compute_domain(rdb_compute_domain)
        return rdb_compute_domain.get_database_names()

    def _get_metadata(self, rdb_compute_domain, database, resource_name="", metadata_type=""):
        """
        Utility function for the use of other metadata functions.
        """

        rdb_compute_domain = self.get_rdb_compute_domain(rdb_compute_domain)
        database = self.get_database(database, rdb_compute_domain.id)

        if metadata_type not in [t for t in dir(_MetadataType) if not t.startswith("__")]:
            raise TypeError("Invalid type for input parameter 'metadata_type'.")

        if Config.isSciServerComputeEnvironment():
            task_name = "Compute.SciScript-Python.Sciquery.get_metadata_" + metadata_type
        else:
            task_name = "SciScript-Python.Sciquery.get_metadata_" + metadata_type

        url = Config.SciqueryURL + "/api/metadata/{0}/{1}/".format(rdb_compute_domain._racm_id, database.name);
        if metadata_type == _MetadataType.TABLES:
            url += "tables"
        elif metadata_type == _MetadataType.VIEWS:
            url += "views"
        elif metadata_type == _MetadataType.ROUTINES:
            url += "routines"
        elif metadata_type == _MetadataType.COLUMNS:
            url += "{0}/{1}".format(resource_name, "columns")
        elif metadata_type == _MetadataType.PARAMETERS:
            url += "{0}/{1}".format(resource_name, "parameters")
        elif metadata_type == _MetadataType.CONSTRAINTS:
            url += "{0}/{1}".format(resource_name, "constraints")
        else:
            raise ValueError("Wrong metadata_type parameter value of " + metadata_type)

        url += "?taskName=" + task_name
        headers = {'X-Auth-Token': self.user.token}
        res = requests.get(url, headers=headers, stream=True)

        if res.status_code < 200 or res.status_code >= 300:
            raise Exception("Error when getting metadata from SciQuery API.\nHttp Response from SciQuery API " +
                            "returned status code " + str(res.status_code) + ":\n" + res.content.decode())
        else:
            res = json.loads(res.content.decode())
            result = res['Result'][0]
            df = pd.DataFrame(result['Data'], columns=[c.upper() for c in result['ColumnNames']])
            df.name = result['TableName']
            return df

    def get_tables_metadata(self, database=None, rdb_compute_domain=None):
        """
        Gets metadata related to tables in a particular database belonging to an RDBComputeDomain.
        """
        return self._get_metadata(rdb_compute_domain, database, metadata_type=_MetadataType.TABLES)

    def get_table_names(self, database=None, rdb_compute_domain=None):
        """
        Gets a list of the names of tables in a particular database belonging to an RDBComputeDomain.
        """
        tables = self.get_tables_metadata(database, rdb_compute_domain)
        return [name for name in tables['TABLE_NAME']]

    def get_views_metadata(self, database=None, rdb_compute_domain=None):
        """
        Gets metadata related to views in a particular database belonging to an RDBComputeDomain.
        """
        return self._get_metadata(rdb_compute_domain, database, metadata_type=_MetadataType.VIEWS)

    def get_view_names(self, database=None, rdb_compute_domain=None):
        """
        Gets a list of the names of views in a particular database belonging to an RDBComputeDomain.
        """
        tables = self.get_views_metadata(database, rdb_compute_domain)
        return [name for name in tables['TABLE_NAME']]

    def get_routines_metadata(self, database=None, rdb_compute_domain=None):
        """
        Gets metadata related to routines or functions in a particular database belonging to an RDBComputeDomain.
        """
        return self._get_metadata(rdb_compute_domain, database, metadata_type=_MetadataType.ROUTINES)

    def get_routine_names(self, database=None, rdb_compute_domain=None):
        """
        Gets a list of the names of routines or functions in a particular database belonging to an RDBComputeDomain.
        """
        routines = self.get_routines_metadata(database, rdb_compute_domain)
        return [routine_name for routine_name in routines['ROUTINE_NAME']]

    def get_columns_metadata(self, table_name, database=None, rdb_compute_domain=None):
        """
        Gets metadata related to columns in a particular database table belonging to an RDBComputeDomain.
        """
        return self._get_metadata(rdb_compute_domain, database, table_name, metadata_type=_MetadataType.COLUMNS)

    def get_column_names(self, table_name, database=None, rdb_compute_domain=None):
        """
        Gets a list of the names of table columns in a particular database belonging to an RDBComputeDomain.
        """
        columns = self.get_columns_metadata(table_name, database, rdb_compute_domain)
        return [columnName for columnName in columns['COLUMN_NAME']]

    def get_constraints_metadata(self, table_name, database=None, rdb_compute_domain=None):
        """
        Gets metadata related to table constraints in a particular database table belonging to an RDBComputeDomain.
        """
        return self._get_metadata(rdb_compute_domain, database, table_name, metadata_type=_MetadataType.CONSTRAINTS)

    def get_constraint_names(self, table_name, database=None, rdb_compute_domain=None):
        """
        Gets a list of the names of table constraints in a particular database belonging to an RDBComputeDomain.
        """
        constraints = self.get_constraints_metadata(table_name, database, rdb_compute_domain)
        return [constraintName for constraintName in constraints['CONSTRAINT_NAME']]

    def get_routine_parameters_metadata(self, routine_name, database=None, rdb_compute_domain=None):
        """
        Gets metadata related to routine parameters in a particular database belonging to an RDBComputeDomain.
        """
        return self._get_metadata(rdb_compute_domain, database, routine_name, metadata_type=_MetadataType.PARAMETERS)

    def get_routine_parameter_names(self, routine_name, database=None, rdb_compute_domain=None):
        """
        Gets a list of the names of routine parameters in a particular database belonging to an RDBComputeDomain.
        """
        parameters = self.get_routine_parameters_metadata(routine_name, database, rdb_compute_domain)
        return [name for name in parameters['PARAMETER_NAME']]

    def __str__(self):
        return "SciQuery instance with rdb_compute_domains = {})".format(self._rdb_compute_domains)

    def __repr__(self):
        return "SciQuery(rdb_compute_domains = {})".format(self._rdb_compute_domains)


class _MetadataType:
    """
    Contains a set of metadata types.
    """
    TABLES = "TABLES"
    VIEWS = "VIEWS"
    COLUMNS = "COLUMNS"
    ROUTINES = "ROUTINES"
    CONSTRAINTS = "CONSTRAINTS"
    PARAMETERS = "PARAMETERS"
