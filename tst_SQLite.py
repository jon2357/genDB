# import sys, os
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# print(sys.path)
from gendb.db import SQLServer

# Requires installing a third party SQLite driver: https://www.devart.com/odbc/sqlite/download.html
test_env = {
    "driver": "{Devart ODBC Driver for SQLite}",
    "database": "./gendb/tests/testDB.db",
    "server": "localhost",
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