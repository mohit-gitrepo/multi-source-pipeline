"""
Multi-Source Schema Verification Script.

Connects to both SQL Server and PostgreSQL sources and prints
all databases, tables, and row counts in a single run.

Usage:
    python verify_sources.py

Environment variables required:
    SQLSERVER_HOST, SQLSERVER_USER, SQLSERVER_PASSWORD
    PG_HOST, PG_PORT, PG_USER, PG_PASSWORD
"""

import logging
from dataclasses import dataclass

import pyodbc
import psycopg

from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TableInfo:
    """Holds metadata for a single table."""
    database_name: str
    table_name: str
    row_count: int


# ---------------------------------------------------------------------------
# SQL Server
# ---------------------------------------------------------------------------

SQLSERVER_DATABASES = [
    "ECommerceDB",
    "EmployeeManagementDB",
    "WarehouseManagementDB",
    "DBA_CapacityRepo",
    "CapacityForecast",
]

ROW_COUNT_SQL_SERVER = """
    SELECT
        t.NAME AS table_name,
        SUM(p.rows) AS row_count
    FROM sys.tables t
    INNER JOIN sys.partitions p
        ON t.object_id = p.object_id
    WHERE p.index_id IN (0, 1)
    GROUP BY t.NAME
    ORDER BY t.NAME
"""

TABLES_SQL_SERVER = """
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
"""


def get_sqlserver_connection(database: str) -> pyodbc.Connection:
    """Create a SQL Server connection for a given database.

    Args:
        database: Target database name.

    Returns:
        Active pyodbc connection.

    Raises:
        pyodbc.Error: If connection fails.
    """
    host = os.getenv("SQLSERVER_HOST", "localhost")
    user = os.getenv("SQLSERVER_USER")
    password = os.getenv("SQLSERVER_PASSWORD")

    if user and password:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={host};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
        )
    else:
        # Windows Authentication fallback
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={host};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )

    return pyodbc.connect(conn_str, timeout=10)


def inspect_sqlserver() -> list[TableInfo]:
    """Inspect all configured SQL Server databases.

    Returns:
        List of TableInfo objects across all SQL Server sources.
    """
    results: list[TableInfo] = []

    for db in SQLSERVER_DATABASES:
        try:
            conn = get_sqlserver_connection(db)
            cursor = conn.cursor()

            cursor.execute(TABLES_SQL_SERVER)
            tables = [row.TABLE_NAME for row in cursor.fetchall()]

            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
                    row_count = cursor.fetchone()[0]
                except pyodbc.Error as e:
                    logger.warning("Could not count rows in %s.%s: %s", db, table, e)
                    row_count = -1

                results.append(TableInfo(
                    database_name=db,
                    table_name=table,
                    row_count=row_count,
                ))

            conn.close()
            logger.info("SQL Server: inspected %s (%d tables)", db, len(tables))

        except pyodbc.Error as e:
            logger.error("SQL Server: could not connect to %s — %s", db, e)

    return results


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

POSTGRES_DATABASES = [
    "sales_db",
    "hr_db",
    "inventory_db",
    "dba_lab",
]

ROW_COUNT_POSTGRES = """
    SELECT relname AS table_name, n_live_tup AS row_count
    FROM pg_stat_user_tables
    ORDER BY relname
"""


def get_postgres_connection(database: str) -> psycopg.Connection:
    """Create a PostgreSQL connection for a given database.

    Args:
        database: Target database name.

    Returns:
        Active psycopg connection.

    Raises:
        psycopg.OperationalError: If connection fails.
    """
    return psycopg.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=database,
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", ""),
        connect_timeout=10,
    )


def inspect_postgres() -> list[TableInfo]:
    """Inspect all configured PostgreSQL databases.

    Returns:
        List of TableInfo objects across all PostgreSQL sources.
    """
    results: list[TableInfo] = []

    for db in POSTGRES_DATABASES:
        try:
            conn = get_postgres_connection(db)
            cursor = conn.cursor()
            cursor.execute(ROW_COUNT_POSTGRES)
            rows = cursor.fetchall()

            for table_name, row_count in rows:
                results.append(TableInfo(
                    database_name=db,
                    table_name=table_name,
                    row_count=row_count,
                ))

            conn.close()
            logger.info("PostgreSQL: inspected %s (%d tables)", db, len(rows))

        except psycopg.OperationalError as e:
            logger.error("PostgreSQL: could not connect to %s — %s", db, e)

    return results


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def print_section(title: str) -> None:
    """Print a formatted section header.

    Args:
        title: Section title text.
    """
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def display_results(source: str, tables: list[TableInfo]) -> None:
    """Print table inspection results grouped by database.

    Args:
        source: Source engine label (e.g. 'SQL Server').
        tables: List of TableInfo objects to display.
    """
    print_section(source)

    if not tables:
        print("  No tables found or all connections failed.")
        return

    current_db = None
    for t in sorted(tables, key=lambda x: (x.database_name, x.table_name)):
        if t.database_name != current_db:
            current_db = t.database_name
            print(f"\n  Database: {current_db}")
            print(f"  {'Table':<40} {'Rows':>12}")
            print(f"  {'-'*40} {'-'*12}")

        row_display = f"{t.row_count:,}" if t.row_count >= 0 else "error"
        print(f"  {t.table_name:<40} {row_display:>12}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Run schema verification across all configured sources."""
    logger.info("Starting multi-source schema verification")

    sqlserver_tables = inspect_sqlserver()
    postgres_tables = inspect_postgres()

    display_results("SQL SERVER SOURCES", sqlserver_tables)
    display_results("POSTGRESQL SOURCES", postgres_tables)

    total = len(sqlserver_tables) + len(postgres_tables)
    print(f"\n{'=' * 60}")
    print(f"  Total tables discovered: {total}")
    print(f"{'=' * 60}\n")

    logger.info("Verification complete — %d tables discovered", total)


if __name__ == "__main__":
    main()