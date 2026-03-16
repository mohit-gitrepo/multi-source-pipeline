"""
Warehouse Loader.

Loads extracted data into the analytics_warehouse PostgreSQL database.
This is the Load step of ELT — takes ExtractResult objects produced
by the extractors and writes them into staging tables.

LEARNING NOTE — The L in ELT:
    In ELT, we Load raw data first, then Transform inside the warehouse.
    This file handles the Load step — creating the target database,
    creating staging tables dynamically, and bulk-loading rows.

    The key design decisions here:
    1. analytics_warehouse is created automatically if it does not exist
    2. Staging tables are DROPPED and RECREATED on every run (full refresh)
    3. We use PostgreSQL COPY for bulk loading — much faster than INSERT
    4. Audit columns are added to every row for data lineage tracking

LEARNING NOTE — Why analytics_warehouse is a separate database:
    We never load into the source databases (ECommerceDB, hr_db, etc.)
    because that would mix raw operational data with analytical data.
    A dedicated warehouse database keeps concerns separated — sources
    stay clean, the warehouse holds pipeline outputs only.
"""

import io        # io.StringIO — creates an in-memory text buffer (used for COPY)
import logging   # Standard Python logging
import os        # os.getenv() to read environment variables
from datetime import datetime  # Used for timestamps in audit columns

import psycopg               # PostgreSQL driver
from dotenv import load_dotenv  # Reads .env file

# ExtractResult is the input type for the loader — output of extractors
from extractors.base_extractor import ExtractResult

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Audit column definitions
# ---------------------------------------------------------------------------

# These columns are appended to EVERY staging table automatically
# They answer "where did this data come from and when did it arrive?"
# This is called data lineage — critical for debugging and compliance
AUDIT_COLUMNS = """
    _source_engine      VARCHAR(20),
    _source_database    VARCHAR(128),
    _source_table       VARCHAR(128),
    _loaded_at          TIMESTAMP DEFAULT NOW()
"""
# _source_engine:   "sqlserver" or "postgres"
# _source_database: "ECommerceDB", "hr_db", etc.
# _source_table:    "Orders", "employees", etc.
# _loaded_at:       timestamp when this row was loaded — auto-filled by PostgreSQL


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def get_warehouse_connection() -> psycopg.Connection:
    """Create a connection to the analytics_warehouse database.

    LEARNING NOTE:
        This is a standalone function, not a class method — it does not
        belong to an extractor. The loader is a separate concern from
        extraction, so we keep it as module-level functions.

        WAREHOUSE_DB defaults to "analytics_warehouse" if not set in .env.
        This makes it easy to switch to a different warehouse DB name
        (e.g. "analytics_warehouse_dev" for development) without changing code.

    Returns:
        Active psycopg connection to analytics_warehouse.

    Raises:
        psycopg.OperationalError: If connection fails.
    """
    return psycopg.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        # WAREHOUSE_DB is a separate env variable — not the same as source DBs
        dbname=os.getenv("WAREHOUSE_DB", "analytics_warehouse"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", ""),
        connect_timeout=30,
    )


# ---------------------------------------------------------------------------
# Warehouse setup
# ---------------------------------------------------------------------------

def create_warehouse_db() -> None:
    """Create analytics_warehouse database if it does not exist.

    LEARNING NOTE — Why connect to "postgres" first:
        In PostgreSQL you cannot create a database while connected to it.
        You also cannot create a database while connected to a database
        that does not exist yet. So we connect to the built-in "postgres"
        database (which always exists) to issue the CREATE DATABASE command.

    LEARNING NOTE — autocommit=True:
        CREATE DATABASE in PostgreSQL cannot run inside a transaction.
        By default psycopg wraps everything in a transaction.
        Setting autocommit=True disables that — each statement commits
        immediately. Required specifically for CREATE DATABASE.

    LEARNING NOTE — Idempotent design:
        We check if the database exists before creating it.
        Running this function multiple times is safe — it never fails
        if the database already exists. This is called idempotency —
        the same operation produces the same result regardless of
        how many times it runs. Critical for pipeline reliability.
    """
    # Connect to the built-in postgres database — always exists
    conn = psycopg.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname="postgres",  # Built-in admin database — always present
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", ""),
        autocommit=True,    # Required for CREATE DATABASE — cannot be in a transaction
    )

    cursor = conn.cursor()

    # Check if analytics_warehouse already exists in pg_database system catalog
    # pg_database is a PostgreSQL system table that lists all databases
    # %s is a parameterized placeholder — never use f-string for SQL values
    cursor.execute(
        "SELECT 1 FROM pg_database WHERE datname = %s",
        (os.getenv("WAREHOUSE_DB", "analytics_warehouse"),),
        # Note the trailing comma — this makes it a tuple, required by psycopg
    )

    if not cursor.fetchone():
        # Database does not exist — create it
        # We use f-string here for the DB name because %s cannot be used
        # for identifiers (table/database names) — only for values
        cursor.execute(
            f"CREATE DATABASE {os.getenv('WAREHOUSE_DB', 'analytics_warehouse')}"
        )
        logger.info("Created database: analytics_warehouse")
    else:
        logger.info("analytics_warehouse already exists — skipping creation")

    conn.close()


def create_staging_schema(conn: psycopg.Connection) -> None:
    """Create the staging schema in the warehouse if it does not exist.

    LEARNING NOTE — What is a schema:
        In PostgreSQL, a schema is a namespace inside a database.
        It groups related tables together. Our warehouse has one schema
        called "staging" which holds all raw extracted tables.

        Full table reference: analytics_warehouse.staging.stg_ss_orders
        Database → Schema → Table

        The default schema is "public" — we use "staging" to clearly
        separate raw pipeline data from any future transformed data.

    LEARNING NOTE — IF NOT EXISTS:
        Makes this operation idempotent — safe to run on every pipeline
        execution without failing if the schema already exists.

    Args:
        conn: Active connection to analytics_warehouse.
    """
    cursor = conn.cursor()
    # IF NOT EXISTS makes this safe to run multiple times
    cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.commit()  # Commit the transaction — psycopg does not auto-commit
    logger.info("Staging schema ready")


# ---------------------------------------------------------------------------
# Dynamic table creation
# ---------------------------------------------------------------------------

def infer_pg_type(value) -> str:
    """Infer a PostgreSQL column type from a Python value.

    LEARNING NOTE — Why dynamic type inference:
        When extractors use SELECT *, we do not know the column types
        in advance. Python represents database values as native Python
        types after fetching. We reverse-map Python types to PostgreSQL
        types to create the staging table with appropriate column types.

        This is not perfect — for example, all integers map to BIGINT
        regardless of whether the source was TINYINT or BIGINT.
        For production pipelines you would use more precise type mapping
        based on cursor.description type codes. For our learning pipeline,
        this approach is practical and sufficient.

    LEARNING NOTE — isinstance() checks:
        isinstance(value, bool) must come BEFORE isinstance(value, int)
        because in Python, bool is a subclass of int —
        isinstance(True, int) returns True. If we checked int first,
        booleans would be incorrectly mapped to BIGINT.

    Args:
        value: A single Python value sampled from the first extracted row.

    Returns:
        PostgreSQL type string e.g. "TEXT", "BIGINT", "TIMESTAMP".
    """
    # Order matters — bool check MUST come before int check
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"        # Safe choice — covers all integer sizes
    if isinstance(value, float):
        return "DOUBLE PRECISION"  # PostgreSQL double precision = Python float
    if isinstance(value, datetime):
        return "TIMESTAMP"
    # Default: TEXT handles everything else — strings, decimals, None
    # TEXT in PostgreSQL has no length limit — safe fallback
    return "TEXT"


def create_staging_table(
    conn: psycopg.Connection,
    result: ExtractResult,
) -> None:
    """Drop and recreate a staging table based on extracted metadata.

    LEARNING NOTE — Full refresh strategy:
        We DROP the table and recreate it on every pipeline run.
        This is called a full refresh — simpler and safer than
        incremental loading because we never have to handle duplicates,
        deletes, or schema changes from the previous run.

        The downside: larger tables take longer to reload every time.
        For production at scale you would implement incremental loading
        using watermarks (last_updated timestamp). Our pipeline uses
        full refresh because it is appropriate for our table sizes.

    LEARNING NOTE — Dynamic column definitions:
        We build the CREATE TABLE SQL dynamically from the extracted
        column names and inferred types. This means the loader works
        on ANY table from ANY source without any configuration.
        Adding a new source just requires adding it to the manifest
        in pipeline.py — no loader changes needed.

    LEARNING NOTE — Audit columns:
        AUDIT_COLUMNS are appended to every table. They add
        _source_engine, _source_database, _source_table, _loaded_at.
        These columns are not in the source data — we add them
        during the load to track data lineage.

    Args:
        conn: Active connection to analytics_warehouse.
        result: ExtractResult containing column names and sample rows
                used to infer column types for CREATE TABLE.
    """
    # Full table reference includes schema prefix
    table_name = f"staging.{result.target_table}"
    cursor = conn.cursor()

    # Drop existing table — full refresh strategy
    # IF EXISTS prevents error if table does not exist yet (first run)
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    if not result.rows:
        # No rows extracted — cannot infer column types, skip table creation
        logger.warning(
            "No rows in result for %s — skipping table creation", table_name
        )
        return

    # Use the first row as a sample to infer column types
    # row[0] is a tuple of values — one value per column
    sample_row = result.rows[0]

    # Build column definitions: "column_name" TYPE
    col_defs = []
    for col_name, sample_value in zip(result.columns, sample_row):
        # zip() pairs column names with their corresponding sample values
        # e.g. zip(["OrderID", "Price"], (1, 99.99)) → [("OrderID", 1), ("Price", 99.99)]
        pg_type = infer_pg_type(sample_value)
        # Lower-case column names — PostgreSQL convention
        # Double quotes handle column names that are reserved words
        col_defs.append(f'    "{col_name.lower()}" {pg_type}')

    # Append audit columns at the end of every staging table
    col_defs.append(f"    {AUDIT_COLUMNS.strip()}")
    # Join all column definitions with commas and newlines
    columns_sql = ",\n".join(col_defs)

    # Execute CREATE TABLE with dynamically built column list
    cursor.execute(f"CREATE TABLE {table_name} (\n{columns_sql}\n)")
    conn.commit()

    logger.info(
        "Created staging table: %s (%d source columns + 4 audit columns)",
        table_name,
        len(result.columns),
    )


# ---------------------------------------------------------------------------
# Bulk loading
# ---------------------------------------------------------------------------

def load_to_warehouse(result: ExtractResult) -> int:
    """Load an ExtractResult into the analytics_warehouse staging area.

    LEARNING NOTE — Why PostgreSQL COPY instead of INSERT:
        INSERT sends each row as a separate SQL statement — for 1 million
        rows that is 1 million round trips to the database. Very slow.

        COPY is PostgreSQL's native bulk load mechanism — it streams
        the entire dataset as tab-separated text in a single operation.
        For large datasets COPY can be 10-50x faster than INSERT.

        We use psycopg3's cursor.copy() context manager which handles
        the COPY protocol automatically — we just write rows to it.

    LEARNING NOTE — io.StringIO (in-memory buffer):
        We build the entire dataset as a tab-separated string in memory
        using io.StringIO — a file-like object that lives in RAM.
        Then we pass it to COPY in one shot.

        Tab-separated format (tab between values, newline between rows) is
        the default format for PostgreSQL COPY FROM STDIN.

    LEARNING NOTE — Null handling in COPY format:
        PostgreSQL COPY represents NULL values as \\N (backslash-N).
        We check for None (Python's null) and replace with \\N so
        PostgreSQL correctly interprets them as NULL in the database.

    LEARNING NOTE — Audit columns in COPY:
        We append source_engine, source_database, source_table to each
        row before writing to the buffer. _loaded_at is handled by
        PostgreSQL's DEFAULT NOW() — we do not need to supply it.

    Args:
        result: ExtractResult from an extractor — contains rows, columns,
                source metadata, and error state.

    Returns:
        Number of rows successfully loaded. Returns 0 on any failure.
    """
    # Skip loading if extraction failed — error is already logged by extractor
    if result.error:
        logger.error(
            "Skipping load for %s — extraction failed: %s",
            result.target_table, result.error,
        )
        return 0

    # Skip if no rows — nothing to load
    if not result.rows:
        logger.warning("No rows to load for %s", result.target_table)
        return 0

    try:
        conn = get_warehouse_connection()

        # Create (or recreate) the staging table for this extraction
        create_staging_table(conn, result)

        table_name = f"staging.{result.target_table}"

        # Build the full column list: source columns + audit columns
        # _loaded_at is omitted — handled by DEFAULT NOW() in the table definition
        all_columns = result.columns + [
            "_source_engine",
            "_source_database",
            "_source_table",
        ]

        # Format column names for SQL — lowercase, double-quoted
        col_list = ", ".join(f'"{c.lower()}"' for c in all_columns)

        # ---------------------------------------------------------------------------
        # Build tab-separated buffer for COPY
        # ---------------------------------------------------------------------------
        buffer = io.StringIO()  # In-memory file-like object — no disk I/O

        for row in result.rows:
            # Append the 3 audit values to each row's data values
            audit_values = [
                result.source_engine,    # e.g. "sqlserver"
                result.source_database,  # e.g. "ECommerceDB"
                result.source_table,     # e.g. "Orders"
            ]
            all_values = list(row) + audit_values
            # list(row) converts the tuple from fetchall() to a list
            # so we can concatenate it with audit_values list

            # Escape each value for COPY tab-separated format
            escaped = []
            for v in all_values:
                if v is None:
                    # PostgreSQL COPY uses \\N to represent NULL
                    escaped.append("\\N")
                else:
                    # Convert value to string, remove tabs and newlines
                    # Tab (\t) is the COPY delimiter — must be removed from values
                    # Newline (\n) is the COPY row separator — must be removed too
                    escaped.append(
                        str(v).replace("\t", " ").replace("\n", " ")
                    )

            # Join values with tab delimiter and end row with newline
            buffer.write("\t".join(escaped) + "\n")

        # Move buffer position back to start so COPY reads from the beginning
        buffer.seek(0)

        # ---------------------------------------------------------------------------
        # Execute COPY — bulk load from buffer into staging table
        # ---------------------------------------------------------------------------
        cursor = conn.cursor()

        # cursor.copy() is psycopg3's COPY context manager
        # COPY ... FROM STDIN means read data from our buffer (not a file)
        with cursor.copy(
            f"COPY {table_name} ({col_list}) FROM STDIN"
        ) as copy:
            # Write the entire buffer content to COPY in one call
            copy.write(buffer.read())

        # Commit the transaction — data is not visible until committed
        conn.commit()
        conn.close()

        logger.info(
            "Loaded %s rows → %s",
            f"{result.row_count:,}",
            table_name,
        )
        return result.row_count

    except psycopg.Error as e:
        logger.error("Load failed for %s: %s", result.target_table, e)
        return 0  # Return 0 — pipeline summary will show this table as failed
    