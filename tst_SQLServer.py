# "DRIVER={ODBC Driver 17 for SQL Server};Server=localhost\SQLEXPRESS;Database=master;trusted_connection=yes;"
# SQL Server must be running for test to work
from gendb.db import SQLServer


test_env = {
    "driver": "{ODBC Driver 17 for SQL Server}",
    "server": "localhost\SQLEXPRESS",
    "database": "master",
    "trusted_connection": "yes",
}
sql = "SELECT * FROM testTable WHERE 1 = 1 "


def main():

    db = SQLServer(env=test_env, sql=sql, dbg=True)
    db.add_conditional("testInt", "!=", 10)
    db.run_query()
    print(db.get_fields())


if __name__ == "__main__":
    main()