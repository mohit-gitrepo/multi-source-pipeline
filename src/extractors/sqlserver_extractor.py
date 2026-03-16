"""
SQL Server Extractor.

Extracts data from SQL Server source databases using pyodbc.
Supports Windows Authentication and SQL Server Authentication.

LEARNING NOTE:
    pyodbc is the standard Python library for connecting to SQL Server.
    It uses ODBC drivers installed on the machine to communicate
    with the database — same concept as ODBC DSN connections in SSMS.
"""

import logging  # Standard Python library for logging messages (replaces print statements)
import os       # Used to read environment variables via os.getenv()

import pyodbc                    # SQL Server connection library
from dotenv import load_dotenv   # Reads .env file and loads values into os.environ

# Import our base class and return type from the extractors package
from extractors.base_extractor import BaseExtractor, ExtractResult

# Load .env file — must be called before any os.getenv() calls
# In production (CI/CD server), variables come from the environment directly
load_dotenv()

# Module-level logger — uses the filename as the logger name automatically
# __name__ resolves to "extractors.sqlserver_extractor" at runtime
logger = logging.getLogger(__name__)


class SqlServerExtractor(BaseExtractor):
    """Extracts data from a SQL Server database.

    Inherits from BaseExtractor which enforces that we implement
    get_connection() and extract() — this is the contract pattern.

    Supports both Windows Authentication (default when no user/password
    in .env) and SQL Server Authentication (when user/password provided).

    LEARNING NOTE:
        Inheriting from BaseExtractor means this class IS-A BaseExtractor.
        The pipeline orchestrator can treat SqlServerExtractor and
        PostgresExtractor identically — it just calls .extract() on both.

    Example:
        extractor = SqlServerExtractor("ECommerceDB")
        result = extractor.extract("Orders", "stg_orders")
    """

    def get_connection(self) -> pyodbc.Connection:
        """Create a SQL Server connection for the configured database.

        LEARNING NOTE:
            A connection string is how pyodbc knows where to connect.
            It contains: driver, server, database, and auth method.
            We build it dynamically based on environment variables
            so credentials never appear in the code itself.

            ODBC Driver 17 for SQL Server must be installed on the machine.
            On Windows this is typically already present. On Linux/Mac
            it must be installed separately.

            Trusted_Connection=yes means Windows Authentication —
            uses the currently logged-in Windows user's credentials.
            This is what you use at Koch — no username/password needed.

        Returns:
            Active pyodbc connection object.

        Raises:
            pyodbc.Error: If connection cannot be established.
        """
        # Read connection details from environment variables
        # Second argument to os.getenv() is the default if variable is not set
        host = os.getenv("SQLSERVER_HOST", "localhost")
        user = os.getenv("SQLSERVER_USER")          # None if not set in .env
        password = os.getenv("SQLSERVER_PASSWORD")  # None if not set in .env

        if user and password:
            # SQL Server Authentication — explicit username and password
            # Used when connecting to remote servers or non-Windows environments
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                # Double braces {{ }} in f-string produce literal { } in output
                f"SERVER={host};DATABASE={self.database};"
                # self.database comes from BaseExtractor.__init__()
                f"UID={user};PWD={password};"
            )
        else:
            # Windows Authentication — uses currently logged-in Windows account
            # This is the default for local development at Koch
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={host};DATABASE={self.database};"
                f"Trusted_Connection=yes;"
            )

        # Debug level — only visible when logging level is set to DEBUG
        # Use DEBUG for connection details, INFO for business events
        self.logger.debug("Connecting to SQL Server: %s / %s", host, self.database)

        # timeout=30 means if connection not established in 30 seconds, raise error
        # Without timeout, a failed connection could hang forever
        return pyodbc.connect(conn_str, timeout=30)

    def extract(
        self,
        table: str,
        target_table: str,
        query: str | None = None,  # None means "no custom query provided"
    ) -> ExtractResult:
        """Extract all rows from a SQL Server table.

        LEARNING NOTE:
            This method follows the Extract step of ELT.
            It connects, runs a query, captures results and metadata,
            then returns everything packaged as an ExtractResult.

            We wrap everything in try/except so a single table failure
            does not crash the entire pipeline — the error is stored
            in the result object and the pipeline continues with other tables.

            cursor.description is a pyodbc feature that returns column
            metadata after a query executes — we use it to capture
            column names dynamically without hardcoding them anywhere.

        Args:
            table: Source table name in SQL Server (e.g. "Orders").
            target_table: Staging table name in analytics_warehouse
                          (e.g. "stg_ss_orders").
            query: Optional custom SQL query. If None, defaults to
                   SELECT * FROM [table]. Square brackets handle
                   table names with spaces or reserved words.

        Returns:
            ExtractResult containing rows, column names, row count,
            source metadata, and error message if extraction failed.
        """
        # Initialise the result object with source metadata upfront
        # Even if extraction fails, we return this with error field populated
        result = ExtractResult(
            source_engine="sqlserver",       # Identifies which engine this came from
            source_database=self.database,   # e.g. "ECommerceDB"
            source_table=table,              # e.g. "Orders"
            target_table=target_table,       # e.g. "stg_ss_orders"
        )

        # Use custom query if provided, otherwise select everything
        # Square brackets around table name handle reserved words and spaces
        # e.g. a table called "Order" would fail without brackets (reserved word)
        sql = query or f"SELECT * FROM [{table}]"

        try:
            # Get a connection — calls get_connection() defined above
            conn = self.get_connection()

            # A cursor is the object that executes SQL and holds the result set
            # Think of it like the cursor in SSMS — points to the result
            cursor = conn.cursor()

            self.logger.info(
                "Extracting %s.%s → %s", self.database, table, target_table
            )

            # Execute the SQL — results are now held in the cursor buffer
            cursor.execute(sql)

            # cursor.description contains column metadata AFTER execute()
            # Each item in description is a tuple — index [0] is column name
            # This gives us column names dynamically without hardcoding
            # Example: [('OrderID',), ('CustomerName',), ('Price',)]
            result.columns = [col[0] for col in cursor.description]
            # List comprehension — same as:
            # columns = []
            # for col in cursor.description:
            #     columns.append(col[0])

            # fetchall() pulls ALL rows from the result set into memory at once
            # For very large tables consider fetchmany(batch_size) instead
            # For this pipeline fetchall() is acceptable for our table sizes
            result.rows = cursor.fetchall()

            # Store the count — len() on a list is O(1), instant
            result.row_count = len(result.rows)

            # Always close connection when done — releases it back to the pool
            # Not closing connections causes connection pool exhaustion
            # which eventually blocks all new connections to SQL Server
            conn.close()

            self.logger.info(
                "Extracted %s rows from %s.%s",
                f"{result.row_count:,}",  # :, formats with commas → 1,000,000
                self.database,
                table,
            )

        except pyodbc.Error as e:
            # Catch ONLY pyodbc-specific errors — not all exceptions
            # Specific exception handling is a production standard —
            # bare "except:" or "except Exception:" hides real bugs
            result.error = str(e)   # Store error in result — do not raise
            self.logger.error(
                "Failed to extract %s.%s: %s", self.database, table, e
            )
            # Pipeline receives result with error set and decides what to do
            # This is called "fail gracefully" — one bad table, not full crash

        return result