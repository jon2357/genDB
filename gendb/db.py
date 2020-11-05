# Python Libaries
from pathlib import Path
import pyodbc
import re
import csv
import json
from datetime import datetime

### User Modules
from .log import get_logger

logger = get_logger(f"{__package__}.{__name__}")


### Define SQL Base Class
base_environment = {
    "server": None,
    "driver": None,
    "database": None,
    "trusted_connection": None,
    "user": None,
    "pass": None,
}

# adds class to convert datetime non-json serialize-able objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


def checkNumeric(inDriver, field):
    # check if database is TSQL
    if re.search("SQL Server", inDriver):
        return "ISNUMERIC({}) = 1".format(field)

    # check if database is SQLite
    if re.search("SQLite", inDriver):
        # "(typeof({}) = 'integer' OR typeof({}) = 'real' OR {} GLOB '[1-9]')"
        # "(typeof({}) = 'integer' OR typeof({}) = 'real' OR {} REGEXP '/^\d*\.?\d+$/' )"
        # return "(typeof({}) = 'integer' OR typeof({}) = 'real')".format(field, field, field)
        return None

    # check if database is Postgres
    if re.search("PostgreSQL", inDriver):
        return None

    # check if database is Oracle
    if re.search("Oracle", inDriver):
        return None


def convertString(inDriver, field, value):
    if re.search("SQL Server", inDriver):
        if isinstance(value, int):
            return "TRY_CONVERT(bigint, {})".format(field)
        if isinstance(value, float):
            return "TRY_CONVERT(dec(38,2), {})".format(field)

    if re.search("SQLite", inDriver):
        if isinstance(value, int):
            return "CAST({} AS INT)".format(field)
        if isinstance(value, float):
            return "CAST({} AS REAL)".format(field)

    if re.search("PostgreSQL", inDriver):
        return field

    if re.search("Oracle", inDriver):
        return field


class SQLServer:
    def __init__(self, env=base_environment, sql="", inVars=None, dbg=False):
        self.__env = base_environment
        self.env = env
        logger.info(f'Initializing Database Class: ENV: {self.env["server"]} ')
        self.sql = sql
        self.vars = inVars
        self.result = list()
        self._conditionVars = list()
        self.settings = {"timeout": 45}
        self.debug = dbg

    ###### Properties ######
    @property
    def env(self):  # Getter
        return self.__env

    @env.setter
    def env(self, inEnv):
        # Check if inEnv is a dictionary and contains specific keys ("server","driver","database","trusted_connection","user","pass")
        acceptedFields = base_environment.keys()
        if not isinstance(inEnv, dict):
            logger.error(f"Updating ENV: passed in variable is not a dictionary - {inEnv}")
            return

        allow = True
        for key in inEnv.keys():
            if key.lower() not in acceptedFields:
                logger.error(f"Updating ENV: passed in dictionary key is not allowed - {key}")
                allow = False
                break

        if allow:
            inDictLowerCase = {k.lower(): v for k, v in inEnv.items()}
            logger.info(f"Updating:ENV: {inDictLowerCase.keys()}")
            self.__env.update(inDictLowerCase)

    @property
    def sql(self):  # Getter
        return self.__sqlScript

    # Setter checks if we are passing in a sql filename or a query
    @sql.setter
    def sql(self, inSQLString):
        if ".sql" in inSQLString[0:255].lower():
            fullPath = inSQLString
            logger.info(f"Loading SQL Code File: {fullPath}")
            with open(fullPath, "r") as fd:
                self.__sqlScript = fd.read()
        else:
            logger.info(f"Loading SQL Code String: {inSQLString[0:255]}")
            self.__sqlScript = inSQLString

    @property
    def vars(self):  # Getter
        return self.__sqlVars

    @vars.setter
    def vars(self, inVars):
        logger.info(f"Adding Variables: {inVars}")
        # Should add a check to make sure the passed in values are single value, tuple, or list
        if inVars and (not isinstance(inVars, list)):
            self.__sqlVars = [inVars]
        else:
            self.__sqlVars = inVars

    @property
    def result(self):  # Getter
        return self.__result

    @result.setter
    def result(self, inRes):
        self.__result = inRes

    ###### Methods ######
    # examples
    # db.add_conditional("Number_Field", ">", 700)
    # db.add_conditional("Number_Field", "!=", None)
    def add_conditional(self, field, inType, value):
        logger.info(f"Updating Conditionals: {field} {inType} {value}")
        chkConditional = ["=", ">", "<", ">=", "<=", "<>", "!=", "in", "not in", "like", "not like"]
        if inType.lower() not in chkConditional:
            logger.error(
                f"Error! Incompatible Conditional: {inType}. Allowed options: {chkConditional}"
            )
            return

        if not isinstance(field, str) or (
            isinstance(field, str) and (len(field) > 128 or re.search("\W", field) is not None)
        ):
            logger.error(
                f"Error! Incompatible Field: {field}. Must only contain alphanumeric and underscore"
            )
            return

        addString = ""
        # Check if we are search for NULL or excluding NULL values
        if value == None:
            if inType == "=":
                addString = f"{field} is null"
            elif inType in ["!=", "<>"]:
                addString = f"{field} is not null"

        # Check if value is in a list
        elif isinstance(value, list) and inType.lower() in ["in", "not in"]:
            # Extend array with the values in the list
            self._conditionVars.extend(value)
            qString = "(" + ",".join(["?"] * len(value)) + ")"
            addString = f"{field} {inType} {qString}"

        # Check if value is numeric
        elif isinstance(value, (int, float)):
            # Add search value to array
            self._conditionVars.append(value)
            qString = "?"

            numCheckStr = checkNumeric(self.env["driver"], field)
            numConvertStr = convertString(self.env["driver"], field, value)
            if numCheckStr != None:
                fieldStr = "{} AND {}".format(numCheckStr, numConvertStr)
            else:
                fieldStr = "{}".format(numConvertStr)

            addString = f"{fieldStr} {inType} {qString}"

        # If value is not in a list, not numeric, and not null, search without any data type checks
        else:
            # Add search value to array
            self._conditionVars.append(value)
            qString = "?"
            addString = f"{field} {inType} {qString}"

        self.sql = f"{self.sql} AND {addString}"

    def add_conditional_dict(self, conditionDict):
        logger.info(f"Updating Conditional From Dictionary: {len(conditionDict)}")
        # For adding conditions with a dictionary of values
        # format: searchParameter = {field: (conditional,value)}
        if conditionDict and isinstance(conditionDict, dict):
            for key, value in conditionDict.items():
                (conditional, data) = value
                SQLServer.add_conditional(key, conditional, data)

    def conn_parameters(self):
        connectionParams = list()
        printableParams = list()
        for key in base_environment.keys():
            if self.env[key] and self.env[key] != "":
                connectionParams.append(f"{key}={self.env[key]}")
                if key not in ("user", "pass"):
                    printableParams.append((key, self.env[key]))

        return {"connection": ";".join(connectionParams) + ";", "details": printableParams}

    def run_query(self):
        params = self.conn_parameters()
        if self.vars:
            self.vars.extend(self._conditionVars)
        else:
            self.vars = self._conditionVars

        results = []

        try:
            logger.info(f"Query: Creating Connection")
            logger.debug(params["details"])
            # logger.debug(params["connection"])
            conn = pyodbc.connect(r"" + params["connection"])
            conn.timeout = self.settings["timeout"]
            print("Getting Cursor")
            cursor = conn.cursor()

            logger.debug(f"SQL Statement: {self.sql}")
            logger.debug(f"SQL Variables: {self.vars}")
            if self.debug:
                print("**************** SQL *************** ")
                print(self.sql)
                print("**************** Vars *************** ")
                print(self.vars)
                print("************************************ ")

            print("Running Query: Executing script")
            logger.info(f"Query: Executing SQL Statement")
            if self.vars:
                cursor.execute(self.sql, self.vars)
            else:
                cursor.execute(self.sql)

            print("Processing the Data: Getting Columns")
            logger.info(f"Query: Getting Fieldnames")
            logger.debug(f"cursor.description: {cursor.description}")
            if self.debug:
                print(cursor.description)
            columns = []
            if cursor.description:
                columns = [column[0] for column in cursor.description]

            print("Processing the Data: Looping over returned results")
            logger.info(f"Query: Getting Row Data")
            for row in cursor.fetchall():
                if self.debug:
                    print(row)
                results.append(dict(zip(columns, row)))

            if not results:
                print("Result: No results returned")
                logger.info(f"Total Result Count: 0")
            else:
                print("Result: Total Count: " + str(len(results)))
                logger.info(f"Total Result Count: {len(results)}")
                if self.debug:
                    print("Index 0 below:")
                    print(results[0])

            print("Closing the connection")
            conn.close()

            self.result = results

        except Exception as e:
            print("Error")
            logger.error(type(e))
            logger.error(e, exc_info=True)
            return

    def get_fields(self):
        try:
            if len(self.result) == 0:
                return None
            else:
                iter(self.result)
                iter(self.result[0])
                return self.result[0].keys()
        except Exception as e:
            logger.error(f"Results are not iterable: {len(self.result)}")
            logger.error(e, exc_info=True)
            print("not iterable")

    def get_field_data(self, field):
        # Returns a list of data from a specific field
        logger.info(f"Getting Field data for: {field}")
        if len(self.result) > 1 and field in self.result[0]:
            return [d[field] for d in self.result if field in d]

    def select_fields(self, fieldList):
        logger.info(f"Selecting Fields: {fieldList}")
        newList = list()
        for inD in self.result:
            modDict = dict((k, inD[k]) for k in fieldList if k in inD)
            newList.append(modDict)
        return newList

    def rename_fields(self, keyDict):
        logger.info(f"Renaming Fields: {keyDict}")
        # keyDict = {"Old_Name": "New_Name"}
        def rename_keys(d, keys):
            return dict([(keys.get(k, k), v) for k, v in d.items()])

        newList = list()
        for inD in self.result:
            newDict = rename_keys(inD, keyDict)
            newList.append(newDict)
        self.result = newList

    def export_csv(self, fullFilePath, selectedFields=None):
        # export results as a CSV with the ability to export selected fields
        outputFileNameLoc = Path(fullFilePath).with_suffix(".csv")
        print(f"Printing to file: {outputFileNameLoc}")
        logger.info(f"Exporting CSV File: {outputFileNameLoc}")
        logger.info(f"Exporting CSV Selected Fields: {selectedFields}")

        if selectedFields:
            res = SQLServer.select_fields(self, selectedFields)
        else:
            res = self.result
        with open(outputFileNameLoc, "w", encoding="utf8", newline="") as output_file:
            dict_writer = csv.DictWriter(output_file, res[0].keys())
            dict_writer.writeheader()
            dict_writer.writerows(res)

    def export_json(self, fullFilePath, selectedFields=None):
        # Export into json format with the ability to export selected fields
        outputFileNameLoc = Path(fullFilePath).with_suffix(".json")
        print(f"Printing to file: {outputFileNameLoc}")
        logger.info(f"Exporting JSON File: {outputFileNameLoc}")
        logger.info(f"Exporting JSON Selected Fields: {selectedFields}")

        if selectedFields:
            res = SQLServer.select_fields(self, selectedFields)
        else:
            res = self.result
        with open(outputFileNameLoc, "w") as outfile:
            json.dump(res, outfile, cls=DateTimeEncoder)
