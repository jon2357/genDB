import sqlite3
from sqlite3 import Error

### Create a test data database
def generateDB():
    db_file = "testDB.db"
    createTestTable = """ CREATE TABLE IF NOT EXISTS testtable ( 
        id integer PRIMARY KEY, 
        fldText text NOT NULL, 
        fldIntNull integer NOT NULL, 
        fldInt integer); """

    updateTestTable = """ INSERT INTO testtable(fldText,fldIntNull,fldInt) VALUES(?,?,?) """

    testData = [
        ("name1", 10, 1),
        ("name2", 20, 2),
        ("name3", 30, None),
    ]

    conn = None
    try:
        print("Estabolishing a connection")
        conn = sqlite3.connect(db_file)
        c = conn.cursor()

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