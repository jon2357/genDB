# Generalized Dynamic Database Search

General SQL Database Class for Dynamically Querying with Conditionals.

Wraps the pyodbc library to create a unified interface for adding dynamic conditional database search without knowing the underlying structure and field types during the search. Uses both value parameterization and SQL sanitization.

## Logging

Can be configured with environment variables

- LOG_LEVEL
- LOG_FORMAT
- LOG_FILE
- LOG_FOLDER

## Additional Software for Testing

### SQLite3

The 3rd part SQLite3 ODBC driver can be found at: <https://www.devart.com/odbc/sqlite/download.html>

### SQL Server 2019 Express Edition

For TSQL testing SQL Server 2019 Express was used, and can be found at: <https://www.microsoft.com/en-us/sql-server/sql-server-downloads>

### Postgres

Under Development
