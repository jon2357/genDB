# Python Libaries
from pathlib import Path
import pyodbc
import re
import csv
import json
from datetime import datetime

from .log import get_logger

logger = get_logger(__file__)

base_environment = {
    "server": None,
    "driver": None,
    "database": None,
    "trusted_connection": "yes",
    "user": None,
    "pass": None,
}

# adds class to convert datetime non-json serialize-able objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


class SQLServer:
    def __init__(self, env=base_environment, sql="", inVars=None, dbg=False):
        self.__env = base_environment
        self.env = env
        self.sql = sql
        self.vars = inVars
        self.result = list()
        self._conditionVars = list()
        self.settings = {"timeout": 45}
        self.debug = dbg
        print(f'Initializing Database Class: ENV: {env["server"]}')
        logger.info(f'Initializing Database Class: ENV: {env["server"]}')

    ###### Properties ######
    @property
    def env(self):  # Getter
        return self.__env

    @env.setter
    def env(self, inEnv):
        # Check if inEnv is a dictionary and contains specific keys ("server","driver","database","trusted_connection","user","pass")
        acceptedFields = ["server", "driver", "database", "trusted_connection", "user", "pass"]
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
            logger.info(f"Updating:ENV: {inEnv.keys()}")
            self.__env.update(inEnv)

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
        if value == None:
            if inType == "=":
                addString = f"{field} is null"
            elif inType in ["!=", "<>"]:
                addString = f"{field} is not null"
        elif isinstance(value, list) and inType.lower() in ["in", "not in"]:
            qString = "(" + ",".join(["?"] * len(value)) + ")"
            addString = f"{field} {inType} {qString}"
            self._conditionVars.extend(value)
        elif isinstance(value, int):
            addString = f"ISNUMERIC({field}) = 1 AND TRY_CONVERT(bigint, {field}) {inType} ?"
            self._conditionVars.append(value)
        elif isinstance(value, float):
            addString = f"ISNUMERIC({field}) = 1 AND TRY_CONVERT(dec(38,2), {field}) {inType} ?"
            self._conditionVars.append(value)
        else:
            addString = f"{field} {inType} ?"
            self._conditionVars.append(value)

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
        # Get required connection parameters
        srv = {
            "server": self.env["server"],
            "driver": self.env["driver"],
            "database": self.env["database"],
            "trusted_connection": self.env["trusted_connection"],
            "user": self.env["user"],
            "pass": self.env["pass"],
        }

        # Set up connection paramters
        params = dict()

        # Generate parameter string for pyodbc
        params["connection"] = (
            "Driver="
            + srv["driver"]
            + ";Server="
            + srv["server"]
            + ";Database="
            + srv["database"]
            + ";"
        )
        # Setup if we are using a trusted connection (local system credentials)
        if srv["trusted_connection"] == "yes":
            params["auth"] = "Trusted_Connection=" + srv["trusted_connection"] + ";"
        else:
            params["auth"] = "UID=" + srv["user"] + ";PWD=" + srv["pass"] + ";"

        return params

    def run_query(self):
        params = self.conn_parameters()
        if self.vars:
            self.vars.extend(self._conditionVars)
        else:
            self.vars = self._conditionVars

        results = []

        try:
            print("Starting Connection")
            conn = pyodbc.connect(r"" + params["connection"] + params["auth"])
            conn.timeout = self.settings["timeout"]
            print("Getting Cursor")
            cursor = conn.cursor()

            if self.debug:
                logger.debug(f"SQL Statement: {len(self.sql)}")
                logger.debug(f"SQL Variables: {self.vars}")
                print("**************** SQL *************** ")
                print(self.sql)
                print("**************** Vars *************** ")
                print(self.vars)
                print("************************************ ")

            print("Running Query: Executing script")
            if self.vars:
                cursor.execute(self.sql, self.vars)
            else:
                cursor.execute(self.sql)

            print("Processing the Data: Getting Columns")
            logger.debug(f"cursor.description: {cursor.description}")
            if self.debug:
                print(cursor.description)
            columns = []
            if cursor.description:
                columns = [column[0] for column in cursor.description]

            print("Processing the Data: Looping over returned results")
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

        except Exception as inst:
            logger.error(type(inst))
            logger.error(inst, exc_info=True)
            return

    def get_fields(self):
        try:
            iter(self.result)
            iter(self.result[0])
            return self.result[0].keys()
        except TypeError as e:
            logger.error(f"Results are not iterable: {len(self.results)}")
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
