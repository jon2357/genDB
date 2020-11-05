# python ./gendb/test/createTestSQLite.py
import os
import sqlite3
from sqlite3 import Error

from pathlib import Path

db_file = Path(__file__).resolve().parent / "testDB.db"


def deleteDB():
    print("Attempting to delete SQLite database")
    if os.path.exists(db_file):
        os.remove(db_file)
        print("File Removed!")


### Create a test data database
def generateDB():
    dropTestTable = "DROP TABLE IF EXISTS testTable;"

    createTestTable = """ 
    CREATE TABLE IF NOT EXISTS testTable ( 
        testVarCharNull text,
        testVarChar text NOT NULL,
        testIntNull integer,
        testInt integer NOT NULL,
        testFloatNull real,
        testFloat real NOT NULL
    ); """

    updateTestTable = """
    INSERT INTO testTable(
        testVarCharNull,
        testVarChar,
        testIntNull,
        testInt,
        testFloatNull,
        testFloat
    ) VALUES(?,?,?,?,?,?) """

    testData = [
        ("vc1", "vc10", 1, 10, 100.1, 100.1),
        ("vc2", "vc20", 2, 20, 200.1, 200.1),
        ("vc3", "vc30", 3, 30, 300.1, 300.1),
        ("991", "992", 4, 40, 400.1, 400.1),
        ("991.1", "992.1", 5, 50, 500.1, 500.1),
        (None, "992", None, 60, None, 600.1),
    ]

    conn = None
    try:
        # Delete file if found
        deleteDB()

        # Create connection / database
        print("Estabolishing a connection")
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

        print("Dropping the Table")
        c.execute(dropTestTable)

        print("Creating the Table")
        c.execute(createTestTable)

        print("Adding Data")
        for t in testData:
            c.execute(updateTestTable, t)

        conn.commit()

        print("Display Data")
        c.execute("SELECT * FROM testtable")
        rows = c.fetchall()
        for row in rows:
            print(row)

    except Error as e:
        print(e)


if __name__ == "__main__":
    generateDB()