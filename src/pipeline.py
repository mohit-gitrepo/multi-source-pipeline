"""
Multi-Source ELT Pipeline — Main Orchestrator.

This is the entry point for the entire pipeline. It coordinates
all extractors and the loader to move data from SQL Server and
PostgreSQL sources into the analytics_warehouse staging schema.

LEARNING NOTE — What an Orchestrator Does:
    An orchestrator is the conductor of a pipeline — it does not
    do the actual extraction or loading itself. It:
    1. Defines WHAT to extract (the manifest)
    2. Calls extractors in sequence
    3. Passes results to the loader
    4. Tracks success/failure per table
    5. Reports a summary at the end

    In production this file would be wrapped in an Airflow DAG
    so it runs on a schedule automatically. For now, we run it
    manually with: python src/pipeline.py

LEARNING NOTE — ELT Flow:
    Extract  → SqlServerExtractor / PostgresExtractor
    Load     → warehouse_loader.load_to_warehouse()
    Transform → dbt (next session — reads from staging tables)

Run:
    python src/pipeline.py

Environment variables required in .env:
    SQLSERVER_HOST, SQLSERVER_USER, SQLSERVER_PASSWORD
    PG_HOST, PG_PORT, PG_USER, PG_PASSWORD
    WAREHOUSE_DB (default: analytics_warehouse)
"""

import logging   # Standard Python logging
import sys       # sys.exit() to terminate with error code on critical failure
from datetime import datetime  # For tracking pipeline run duration

from dotenv import load_dotenv  # Load .env before anything else

# Import our extractor classes — each handles one source engine
from extractors.postgres_extractor import PostgresExtractor
from extractors.sqlserver_extractor import SqlServerExtractor

# Import loader functions — handles warehouse setup and data loading
from loaders.warehouse_loader import (
    create_staging_schema,   # Creates staging schema in warehouse
    create_warehouse_db,     # Creates analytics_warehouse DB if missing
    get_warehouse_connection, # Returns a connection to analytics_warehouse
    load_to_warehouse,       # Loads an ExtractResult into a staging table
)

load_dotenv()

# Configure root logger — applies to all loggers in all modules
# format: timestamp [LEVEL] logger_name — message
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Extraction Manifest
# ---------------------------------------------------------------------------

# LEARNING NOTE — What is a Manifest:
#   A manifest is a configuration list that defines WHAT the pipeline
#   should process. Each entry is a tuple of 4 values:
#   (extractor_class, source_database, source_table, target_table)
#
#   extractor_class:  Which extractor to use — SqlServerExtractor or PostgresExtractor
#   source_database:  The database to connect to on the source engine
#   source_table:     The table to extract from that database
#   target_table:     The staging table name to create in analytics_warehouse
#
#   ADDING A NEW TABLE: Just add one line to this list.
#   No changes needed in extractors or loader — the manifest drives everything.
#
#   NAMING CONVENTION for target tables:
#   stg_ss_  = staging, SQL Server source
#   stg_pg_  = staging, PostgreSQL source
#   Then database abbreviation + table name
#   e.g. stg_ss_orders = staging, SQL Server, Orders table

EXTRACTION_MANIFEST = [
    # ── SQL Server sources ──────────────────────────────────────────────────
    # ECommerceDB — orders, customers, products
    (SqlServerExtractor, "ECommerceDB",           "Orders",         "stg_ss_orders"),
    (SqlServerExtractor, "ECommerceDB",           "Customers",      "stg_ss_customers"),
    (SqlServerExtractor, "ECommerceDB",           "Products",       "stg_ss_products"),

    # EmployeeManagementDB — employees and departments
    (SqlServerExtractor, "EmployeeManagementDB",  "Employees",      "stg_ss_employees"),
    (SqlServerExtractor, "EmployeeManagementDB",  "Departments",    "stg_ss_departments"),

    # WarehouseManagementDB — products, suppliers, stock movements
    (SqlServerExtractor, "WarehouseManagementDB", "Products",       "stg_ss_wh_products"),
    (SqlServerExtractor, "WarehouseManagementDB", "Suppliers",      "stg_ss_suppliers"),
    (SqlServerExtractor, "WarehouseManagementDB", "StockMovements", "stg_ss_stock_movements"),

    # ── PostgreSQL sources ──────────────────────────────────────────────────
    # dba_lab — customers, products, orders (3 related tables — best for dbt joins)
    (PostgresExtractor,  "dba_lab",               "customers",      "stg_pg_dbalab_customers"),
    (PostgresExtractor,  "dba_lab",               "products",       "stg_pg_dbalab_products"),
    (PostgresExtractor,  "dba_lab",               "orders",         "stg_pg_dbalab_orders"),

    # hr_db — employees and departments
    (PostgresExtractor,  "hr_db",                 "employees",      "stg_pg_hr_employees"),
    (PostgresExtractor,  "hr_db",                 "departments",    "stg_pg_hr_departments"),

    # inventory_db — products and stock movements
    (PostgresExtractor,  "inventory_db",           "products",       "stg_pg_inv_products"),
    (PostgresExtractor,  "inventory_db",           "stock_movements","stg_pg_inv_movements"),

    # sales_db — orders only
    (PostgresExtractor,  "sales_db",               "orders",         "stg_pg_sales_orders"),
]
# Total: 16 extraction tasks across 2 engines and 7 source databases


# ---------------------------------------------------------------------------
# Summary display
# ---------------------------------------------------------------------------

def print_summary(results: list[dict]) -> None:
    """Print a formatted pipeline run summary table to the console.

    LEARNING NOTE — Why a summary table:
        When 16 tables are extracted and loaded, you need a quick way
        to see which succeeded and which failed — without scrolling
        through hundreds of log lines. This function formats the
        results into a readable table printed at the end of the run.

        In production, this summary would also be sent to Slack or
        email via an alerting integration in the Airflow DAG.

    Args:
        results: List of result dicts. Each dict has:
                 - target_table (str): staging table name
                 - rows_loaded (int): number of rows loaded, -1 on failure
                 - status (str): "✓ OK" or "✗ FAIL"
                 - duration_s (float): seconds taken for this task
    """
    print(f"\n{'=' * 65}")
    print(f"  PIPELINE RUN SUMMARY")
    print(f"{'=' * 65}")
    # :<35 = left-align in 35 chars, :>12 = right-align in 12 chars
    print(f"  {'TARGET TABLE':<35} {'ROWS':>12}  {'STATUS':<10}")
    print(f"  {'-' * 35} {'-' * 12}  {'-' * 10}")

    total_rows = 0
    for r in results:
        # Format row count with commas, show — for failures
        row_str = f"{r['rows_loaded']:,}" if r["rows_loaded"] > 0 else "—"
        print(f"  {r['target_table']:<35} {row_str:>12}  {r['status']:<10}")
        # max() prevents negative numbers from reducing the total
        total_rows += max(r["rows_loaded"], 0)

    print(f"  {'-' * 35} {'-' * 12}")
    print(f"  {'TOTAL':<35} {total_rows:>12,}")
    print(f"{'=' * 65}\n")


# ---------------------------------------------------------------------------
# Main pipeline function
# ---------------------------------------------------------------------------

def run_pipeline() -> None:
    """Execute the full ELT pipeline across all configured sources.

    LEARNING NOTE — Pipeline Execution Flow:
        Step 1: Setup — create warehouse DB and staging schema if needed
        Step 2: Loop through EXTRACTION_MANIFEST — extract and load each table
        Step 3: Track results per table — success, failure, row count, duration
        Step 4: Print summary table showing all 16 results

    LEARNING NOTE — sys.exit(1) on warehouse setup failure:
        If we cannot create the warehouse or staging schema, there is no
        point continuing — every load would fail. sys.exit(1) terminates
        the process with exit code 1, which signals failure to the calling
        process (Airflow, CI/CD, shell script). Exit code 0 = success,
        any non-zero = failure.

    LEARNING NOTE — datetime.now() for timing:
        We capture start time at the beginning and calculate elapsed time
        at the end. str(elapsed).split(".")[0] removes microseconds from
        the timedelta string — e.g. "0:02:34" instead of "0:02:34.123456".

    Raises:
        SystemExit: If warehouse setup fails — no point running extractions.
    """
    start = datetime.now()  # Record pipeline start time
    logger.info("Pipeline started — %d extraction tasks", len(EXTRACTION_MANIFEST))

    # ── Step 1: Warehouse Setup ─────────────────────────────────────────────
    # Create analytics_warehouse database if it does not exist
    # Create staging schema inside the warehouse
    # These are idempotent — safe to run on every pipeline execution
    try:
        create_warehouse_db()              # Creates DB if missing

        conn = get_warehouse_connection()  # Connect to the new/existing warehouse
        create_staging_schema(conn)        # Create staging schema if missing
        conn.close()

    except Exception as e:
        # If warehouse setup fails, nothing else can work — exit immediately
        # This is the only place we use bare Exception — because any failure
        # here (connection, permission, disk space) is equally fatal
        logger.error("Warehouse setup failed: %s", e)
        sys.exit(1)  # Exit code 1 = failure — Airflow/CI will catch this

    # ── Step 2 & 3: Extract + Load each table ──────────────────────────────
    results = []  # Collect results for the summary table

    for extractor_class, source_db, source_table, target_table in EXTRACTION_MANIFEST:
        # LEARNING NOTE — Tuple unpacking:
        #   Each manifest entry is a 4-tuple. Python unpacks it into
        #   4 named variables in one line — cleaner than indexing [0], [1], etc.

        task_start = datetime.now()  # Time each individual extraction task
        logger.info("─" * 55)       # Visual separator in logs between tasks

        # Instantiate the correct extractor for this source engine
        # extractor_class is either SqlServerExtractor or PostgresExtractor
        # Passing source_db calls BaseExtractor.__init__(database=source_db)
        extractor = extractor_class(source_db)

        # Extract: connect to source, run SELECT *, return ExtractResult
        extract_result = extractor.extract(source_table, target_table)

        # Load: create staging table, bulk load via COPY, return row count
        rows_loaded = load_to_warehouse(extract_result)

        # Calculate how long this task took
        duration = (datetime.now() - task_start).total_seconds()

        # Determine status based on whether extraction had an error
        status = "✓ OK" if not extract_result.error else "✗ FAIL"

        # Store result for summary display
        results.append({
            "target_table": target_table,
            "rows_loaded": rows_loaded,
            "status": status,
            "duration_s": round(duration, 2),
        })

    # ── Step 4: Summary ────────────────────────────────────────────────────
    elapsed = datetime.now() - start  # Total pipeline run time
    logger.info("─" * 55)
    logger.info(
        "Pipeline complete in %s",
        str(elapsed).split(".")[0],  # Remove microseconds — e.g. "0:03:42"
    )

    # Print the formatted summary table to console
    print_summary(results)


# ---------------------------------------------------------------------------
# Entry point guard
# ---------------------------------------------------------------------------

# LEARNING NOTE — if __name__ == "__main__":
#   When Python runs a file directly (python pipeline.py), __name__ is "__main__".
#   When a file is imported as a module (from pipeline import run_pipeline),
#   __name__ is the module name, not "__main__".
#
#   This guard ensures run_pipeline() only executes when the file is run
#   directly — NOT when it is imported. Without this guard, importing
#   pipeline.py in a test file would immediately trigger the full pipeline run.
#
#   This is a universal Python best practice — every script should have it.

if __name__ == "__main__":
    run_pipeline()