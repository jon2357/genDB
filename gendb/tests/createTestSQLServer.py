# python ./gendb/tests/createTestSQLServer.py
import pyodbc

connectStr = "DRIVER={ODBC Driver 17 for SQL Server};Server=localhost\SQLEXPRESS;Database=master;trusted_connection=yes;"

createTable = """ 
IF OBJECT_ID('[master].[dbo].[testTable]', 'U') IS NOT NULL 
  DROP TABLE [master].[dbo].[testTable]; 

CREATE TABLE [master].[dbo].[testTable](
	[testVarCharNull] [nvarchar](50) NULL,
	[testVarChar] [nvarchar](50) NOT NULL,
	[testIntNull] [int] NULL,
	[testInt] [int] NOT NULL,
	[testFloatNull] [float] NULL,
	[testFloat] [float] NOT NULL
) ON [PRIMARY]

"""

updateTestTable = """
INSERT INTO [master].[dbo].[testTable](
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

print("Opening Connection")
cnxn = pyodbc.connect(connectStr)
cursor = cnxn.cursor()

# Create Table
print("Creating Table")
cursor.execute(createTable)

# Add Values
print("Adding Data")
for t in testData:
    cursor.execute(updateTestTable, t)

# Save changes
print("Commit Changes")
cnxn.commit()

print("Display Data")
cursor.execute("SELECT * FROM [master].[dbo].[testTable]")
rows = cursor.fetchall()
for row in rows:
    print(row)