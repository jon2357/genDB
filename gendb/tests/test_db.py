import sys, os
import sys, os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
print(sys.path)
from db import SQLServer


def main():
    test_env = {"server": "SQLite3 ODBC Driver", "database": "testDB.db"}
    sql = "SELECT * FROM testtable"
    db = SQLServer(env=test_env, sql=sql, dbg=True)
    print(db.result)


if __name__ == "__main__":
    main()