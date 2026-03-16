"""
PostgreSQL Extractor.

Extracts data from PostgreSQL source databases using psycopg3.
Follows the same contract as SqlServerExtractor — inherits from
BaseExtractor and implements get_connection() and extract().

LEARNING NOTE — psycopg3 vs psycopg2:
    psycopg2 was the standard PostgreSQL driver for Python for many years.
    psycopg3 (imported as just "psycopg") is the modern rewrite — faster,
    supports async, and has a cleaner API. We use psycopg3 throughout
    this project. The key difference you will notice: psycopg3 uses
    %s placeholders for parameters (same as psycopg2), but the connection
    API is slightly different.

LEARNING NOTE — Why a separate extractor for PostgreSQL:
    SQL Server uses pyodbc with ODBC drivers and Windows Authentication.
    PostgreSQL uses psycopg with its own protocol and connection format.
    The engines are completely different under the hood — but by using
    BaseExtractor, the pipeline orchestrator treats them identically.
    This is the power of abstraction.
"""

import logging  # Standard Python logging — same pattern as all other files
import os       # os.getenv() to read environment variables

import psycopg               # PostgreSQL driver (psycopg3 — installed as psycopg[binary])
from dotenv import load_dotenv  # Reads .env file into os.environ

# Import the base class and shared return type
from extractors.base_extractor import BaseExtractor, ExtractResult

# Load .env file values into environment before any os.getenv() calls
load_dotenv()

# Module-level logger — will show as "PostgresExtractor" in log output
# because self.logger in BaseExtractor uses self.__class__.__name__
logger = logging.getLogger(__name__)


class PostgresExtractor(BaseExtractor):
    """Extracts data from a PostgreSQL database using psycopg3.

    LEARNING NOTE — Key Differences vs SqlServerExtractor:
        1. Driver: psycopg vs pyodbc
        2. Connection: psycopg.connect() with keyword args vs connection string
        3. Error type: psycopg.Error vs pyodbc.Error
        4. Table quoting: no brackets needed — PostgreSQL uses double quotes
           but is case-insensitive for unquoted identifiers by default
        5. No Trusted_Connection — PostgreSQL uses its own pg_hba.conf
           for authentication, not Windows Authentication

    Everything else — the pattern, the return type, the error handling —
    is identical to SqlServerExtractor. That is the point of BaseExtractor.

    Example:
        extractor = PostgresExtractor("dba_lab")
        result = extractor.extract("orders", "stg_pg_dbalab_orders")
    """

    def get_connection(self) -> psycopg.Connection:
        """Create a PostgreSQL connection for the configured database.

        LEARNING NOTE — psycopg.connect() vs pyodbc connection string:
            pyodbc uses a single connection string with semicolon-separated
            key=value pairs (like a DSN). psycopg uses individual keyword
            arguments — host, port, dbname, user, password separately.
            Both approaches achieve the same result — just different APIs.

        LEARNING NOTE — connect_timeout:
            Same concept as timeout=30 in pyodbc — if PostgreSQL does not
            respond within this many seconds, raise an error instead of
            hanging indefinitely. Critical for pipelines running unattended.

        LEARNING NOTE — PG_PORT default 5432:
            5432 is the default PostgreSQL port — same as 1433 is the
            default SQL Server port. We read it from .env so it can be
            changed without modifying code (e.g. if running PG on 5433).
            int() converts the string from os.getenv() to an integer
            because psycopg expects port as a number, not a string.

        Returns:
            Active psycopg connection object.

        Raises:
            psycopg.OperationalError: If connection cannot be established.
                                      Subclass of psycopg.Error.
        """
        # Log which database we are connecting to before attempting
        # Debug level — only visible when LOG_LEVEL=DEBUG in environment
        self.logger.debug(
            "Connecting to PostgreSQL: %s:%s / %s",
            os.getenv("PG_HOST", "localhost"),
            os.getenv("PG_PORT", "5432"),
            self.database,  # self.database set in BaseExtractor.__init__()
        )

        # psycopg.connect() takes individual keyword arguments — not a string
        # Each parameter maps directly to a PostgreSQL connection parameter
        return psycopg.connect(
            host=os.getenv("PG_HOST", "localhost"),
            # int() required — os.getenv always returns a string, psycopg wants int
            port=int(os.getenv("PG_PORT", "5432")),
            # dbname is the PostgreSQL parameter name for the database
            # Note: psycopg uses "dbname", not "database"
            dbname=self.database,
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASSWORD", ""),
            # If connection not established within 30 seconds, raise error
            connect_timeout=30,
        )

    def extract(
        self,
        table: str,
        target_table: str,
        query: str | None = None,  # None = no custom query, use SELECT *
    ) -> ExtractResult:
        """Extract all rows from a PostgreSQL table.

        LEARNING NOTE — Identical Pattern to SqlServerExtractor.extract():
            The body of this method is structurally identical to the SQL
            Server version. The only differences are:
            1. get_connection() returns a psycopg connection (not pyodbc)
            2. Table names use plain names (not [brackets] like SQL Server)
            3. We catch psycopg.Error instead of pyodbc.Error

            This demonstrates why the base class pattern is valuable —
            both extractors implement the same logic, just with
            engine-specific details swapped out.

        LEARNING NOTE — PostgreSQL Table Name Quoting:
            SQL Server uses [square brackets] to quote identifiers.
            PostgreSQL uses "double quotes" for case-sensitive identifiers.
            For standard lowercase table names (which PostgreSQL normalises
            to lowercase by default), no quoting is needed at all.
            We use plain table names here — works for all our source tables.

        LEARNING NOTE — cursor.description in psycopg3:
            Works identically to pyodbc — after cursor.execute(), the
            cursor.description attribute holds column metadata.
            Each item is a Column object — index [0] is the column name.
            The list comprehension [desc[0] for desc in cursor.description]
            extracts just the names — same pattern as SQL Server extractor.

        Args:
            table: Source table name in PostgreSQL (e.g. "orders").
                   PostgreSQL is case-insensitive for unquoted names —
                   "orders", "Orders", "ORDERS" all refer to the same table.
            target_table: Staging table name in analytics_warehouse
                          (e.g. "stg_pg_dbalab_orders").
            query: Optional custom SQL. If None, defaults to SELECT * FROM table.
                   Use this for filtered extractions e.g. last 30 days only.

        Returns:
            ExtractResult with rows, columns, row_count, metadata, and
            error field populated if extraction failed.
        """
        # Initialise result with source metadata — populated even on failure
        # The loader checks result.error to decide whether to skip loading
        result = ExtractResult(
            source_engine="postgres",        # Identifies engine in audit columns
            source_database=self.database,   # e.g. "dba_lab"
            source_table=table,              # e.g. "orders"
            target_table=target_table,       # e.g. "stg_pg_dbalab_orders"
        )

        # Default query: select all rows from the table
        # Unlike SQL Server, no brackets needed around PostgreSQL table names
        # for standard lowercase names
        sql = query or f"SELECT * FROM {table}"

        try:
            # Establish connection using get_connection() defined above
            conn = self.get_connection()

            # Create a cursor — the object that sends queries and holds results
            # In psycopg3, cursor() creates a standard synchronous cursor
            cursor = conn.cursor()

            self.logger.info(
                "Extracting %s.%s → %s", self.database, table, target_table
            )

            # Send the SQL query to PostgreSQL and execute it
            # Results are buffered on the server until we fetch them
            cursor.execute(sql)

            # cursor.description available after execute() — column metadata
            # In psycopg3, each item in description is a Column namedtuple
            # desc[0] = column name (same position as pyodbc cursor.description)
            result.columns = [desc[0] for desc in cursor.description]
            # Equivalent to:
            # result.columns = []
            # for desc in cursor.description:
            #     result.columns.append(desc[0])

            # Fetch ALL rows into memory as a list of tuples
            # Each tuple is one row — values in same order as result.columns
            # e.g. (1, 'Amit Sharma', 'Bengaluru', datetime(2024, 1, 15))
            result.rows = cursor.fetchall()

            # Count rows — len() on a list is O(1), no additional DB call needed
            result.row_count = len(result.rows)

            # Close connection to release it — critical for connection pool health
            # PostgreSQL has a max_connections limit (default 100)
            # Unclosed connections consume slots and block new connections
            conn.close()

            self.logger.info(
                "Extracted %s rows from %s.%s",
                f"{result.row_count:,}",  # Comma-formatted number for readability
                self.database,
                table,
            )

        except psycopg.Error as e:
            # psycopg.Error is the base class for ALL psycopg exceptions:
            # - psycopg.OperationalError: connection failures
            # - psycopg.ProgrammingError: bad SQL syntax
            # - psycopg.IntegrityError: constraint violations
            # Catching psycopg.Error handles all of them in one clause
            # without hiding non-database errors (like AttributeError)
            result.error = str(e)
            self.logger.error(
                "Failed to extract %s.%s: %s", self.database, table, e
            )
            # Return result with error set — pipeline logs it and continues
            # This is "fail gracefully" — one bad table, not a full crash

        return result
