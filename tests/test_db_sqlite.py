## Add parent folder to python module search path
# (If test is in a subdirectory, this breaks the relative imports of the package)
# (If test is in the package directory, it seems to work well)
# Assumes you are currently running the SQL database and have run the 'createTestSQLite.py' script
import sys, os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pathlib import Path

## Import modules to test
from gendb.db import SQLServer

TEST_OUTPUT_DIR = Path(__file__).resolve().parent / "test_output"
if not TEST_OUTPUT_DIR.is_dir():
    TEST_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

test_env = {
    "driver": "{Devart ODBC Driver for SQLite}",
    "database": Path(__file__).resolve().parent / "testDB.db",
    "server": "localhost",
    "trusted_connection": "yes",
}


def test_sql_query_return_all_results():
    sql = "SELECT * FROM testTable WHERE 1 = 1 "
    inVars = None

    db = SQLServer(env=test_env, sql=sql, inVars=inVars, dbg=False)
    db.run_query()

    assert len(db.result) == 6


def test_sql_query_return_1_result_pass_var_on_init():
    sql = "SELECT * FROM testTable WHERE 1 = 1 LIMIT ?"
    inVars = 1

    db = SQLServer(env=test_env, sql=sql, inVars=inVars, dbg=False)
    db.run_query()

    assert len(db.result) == 1
    assert db.vars == [1]


def test_sql_query_return_3_results_pass_var_after_init():
    sql = "SELECT * FROM testTable WHERE 1 = 1 LIMIT ?"
    inVars = None

    db = SQLServer(env=test_env, sql=sql, inVars=inVars, dbg=False)
    db.vars = 3
    db.run_query()

    assert len(db.result) == 3
    assert db.vars == [3]


def test_sql_query_add_conditional_integer_field_search_integer():
    sql = "SELECT * FROM testTable WHERE 1 = 1 "
    inVars = None

    db = SQLServer(env=test_env, sql=sql, inVars=inVars, dbg=False)
    db.add_conditional("testInt", "!=", 10)
    db.add_conditional("testIntNull", "<", 4)
    db.run_query()
    print(db.sql)
    print(db.result)

    str2match = "AND CAST(testInt AS INT) != ? AND CAST(testIntNull AS INT) < ?"
    assert (db.sql.find(str2match) == -1) == False
    assert db.vars == [10, 4]
    assert len(db.result) == 2


def test_sql_query_add_conditional_integer_field_equal_null():
    sql = "SELECT * FROM testTable WHERE 1 = 1 "
    inVars = None

    db = SQLServer(env=test_env, sql=sql, inVars=inVars, dbg=False)
    db.add_conditional("testIntNull", "=", None)
    db.run_query()
    print(db.sql)

    str2match = "testIntNull is null"
    assert (db.sql.find(str2match) == -1) == False
    assert len(db.result) == 1


def test_sql_query_add_conditional_integer_field_not_equal_null():
    sql = "SELECT * FROM testTable WHERE 1 = 1 "
    inVars = None

    db = SQLServer(env=test_env, sql=sql, inVars=inVars, dbg=False)
    db.add_conditional("testIntNull", "!=", None)
    db.run_query()
    print(db.sql)

    str2match = "testIntNull is not null"

    assert (db.sql.find(str2match) == -1) == False
    assert len(db.result) == 5


def test_sql_query_add_conditional_string_field_search_string():
    sql = "SELECT * FROM testTable WHERE 1 = 1 "
    inVars = None

    db = SQLServer(env=test_env, sql=sql, inVars=inVars, dbg=False)
    db.add_conditional("testVarChar", "=", "vc10")
    db.run_query()
    print(db.sql)

    str2match = "testVarChar = ?"
    assert (db.sql.find(str2match) == -1) == False
    assert db.vars == ["vc10"]
    assert len(db.result) == 1


def test_sql_query_add_conditional_string_field_search_int():
    sql = "SELECT * FROM testTable WHERE 1 = 1 "
    inVars = None

    db = SQLServer(env=test_env, sql=sql, inVars=inVars, dbg=False)
    db.add_conditional("testVarChar", "=", 992)
    db.run_query()
    print(db.sql)
    print(db.result)

    str2match = "AND CAST(testVarChar AS INT) = ?"
    assert (db.sql.find(str2match) == -1) == False
    assert db.vars == [992]
    assert (
        len(db.result) == 3
    )  # For SQL forcing the column to be an integer will remove the decimal


def test_sql_query_get_fields():
    sql = "SELECT * FROM testTable WHERE 1 = 1 "
    inVars = None
    db = SQLServer(env=test_env, sql=sql, inVars=inVars, dbg=False)
    db.run_query()
    assert len(db.get_fields()) == len(db.result[0])


def test_sql_query_get_single_field_data():
    sql = "SELECT * FROM testTable WHERE 1 = 1 "
    inVars = None
    db = SQLServer(env=test_env, sql=sql, inVars=inVars, dbg=False)
    db.run_query()
    assert isinstance(db.get_field_data("testInt"), list) == True
    assert len(db.get_field_data("testInt")) == len(db.result)


def test_sql_query_select_fields():
    sql = "SELECT * FROM testTable WHERE 1 = 1 "
    inVars = None
    db = SQLServer(env=test_env, sql=sql, inVars=inVars, dbg=False)
    db.run_query()

    fieldList = ["testVarChar", "testInt"]
    subResults = db.select_fields(fieldList)
    assert set(fieldList) == set(subResults[0].keys())


def test_sql_query_rename_result_fields():
    sql = "SELECT * FROM testTable WHERE 1 = 1 "
    inVars = None
    db = SQLServer(env=test_env, sql=sql, inVars=inVars, dbg=False)
    db.run_query()

    original = db.select_fields(["testVarChar", "testInt"])
    renameFields = {"testVarChar": "rename1", "testInt": "rename2"}
    db.rename_fields(renameFields)
    newF = db.select_fields(["rename1", "rename2"])
    assert len(newF) == len(original)


def test_sql_query_export_csv():
    sql = "SELECT * FROM testTable WHERE 1 = 1 "
    inVars = None
    db = SQLServer(env=test_env, sql=sql, inVars=inVars, dbg=False)
    db.run_query()

    fullFilePath = TEST_OUTPUT_DIR / "sqlite_test.csv"
    db.export_csv(fullFilePath)

    fullFilePath = TEST_OUTPUT_DIR / "sqlite_test_selected.csv"
    db.export_csv(fullFilePath, ["testVarChar", "testInt"])


def test_sql_query_export_json():
    sql = "SELECT * FROM testTable WHERE 1 = 1 "
    inVars = None
    db = SQLServer(env=test_env, sql=sql, inVars=inVars, dbg=False)
    db.run_query()

    fullFilePath = TEST_OUTPUT_DIR / "sqlite_test.json"
    db.export_json(fullFilePath)

    fullFilePath = TEST_OUTPUT_DIR / "sqlite_test_selected.json"
    db.export_json(fullFilePath, ["testVarChar", "testInt"])
