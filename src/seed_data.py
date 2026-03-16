"""
Multi-Source Data Seeding Script — Million Scale.

Populates all empty tables across SQL Server and PostgreSQL
with realistic data at production scale using batch inserts
for performance.

Counts:
    ECommerceDB.Customers        →  1,000,000
    ECommerceDB.Products         →    100,000
    WarehouseMgmt.Suppliers      →      1,000
    WarehouseMgmt.StockMovements →  2,000,000
    hr_db.departments            →         10
    hr_db.employees              →    500,000
    inventory_db.products        →    100,000
    inventory_db.stock_movements →  2,000,000
    dba_lab.customers            →    500,000
    dba_lab.products             →    100,000
    dba_lab.orders               →  5,000,000
    sales_db.orders              →  5,000,000

Usage:
    python seed_data.py

Environment variables required:
    SQLSERVER_HOST, SQLSERVER_USER, SQLSERVER_PASSWORD
    PG_HOST, PG_PORT, PG_USER, PG_PASSWORD
"""

import logging
import os
import random
from datetime import datetime, timedelta
from typing import Generator

import psycopg
import pyodbc
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BATCH_SIZE = 10_000  # rows per commit — balances memory and speed

CITIES = ["Mumbai", "Delhi", "Bengaluru", "Chennai", "Hyderabad",
          "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Surat",
          "Nagpur", "Lucknow", "Bhopal", "Indore", "Visakhapatnam"]
COUNTRIES = ["India", "USA", "UK", "Germany", "Singapore",
             "Australia", "Canada", "UAE", "Japan", "France"]
DEPARTMENTS = ["Engineering", "Sales", "Marketing", "Finance",
               "HR", "Operations", "Legal", "Product", "Support", "Analytics"]
CATEGORIES = ["Electronics", "Clothing", "Food", "Furniture",
              "Sports", "Books", "Toys", "Automotive", "Beauty", "Garden"]
SUPPLIERS = [f"Supplier_{i}" for i in range(1, 1001)]
MOVEMENT_TYPES = ["IN", "OUT"]
STATUSES = ["completed", "pending", "cancelled", "refunded", "processing"]
FIRST_NAMES = ["Amit", "Priya", "Rahul", "Sneha", "Vikram", "Anjali",
               "Suresh", "Deepa", "Rajesh", "Kavita", "Arun", "Meena",
               "Kiran", "Pooja", "Sanjay", "Neha", "Manoj", "Divya"]
LAST_NAMES = ["Sharma", "Patel", "Singh", "Kumar", "Verma", "Gupta",
              "Joshi", "Mehta", "Nair", "Reddy", "Rao", "Shah",
              "Iyer", "Pillai", "Banerjee", "Das", "Mishra", "Tiwari"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def random_date(start_days_ago: int = 1825, end_days_ago: int = 0) -> datetime:
    """Generate a random datetime within a range.

    Args:
        start_days_ago: How many days ago the range starts.
        end_days_ago: How many days ago the range ends.

    Returns:
        Random datetime within the specified range.
    """
    delta_seconds = (start_days_ago - end_days_ago) * 86400
    dt = (datetime.now()
          - timedelta(days=end_days_ago)
          - timedelta(seconds=random.randint(0, delta_seconds)))
    # SQL Server datetime does not support microseconds — strip them
    return dt.replace(microsecond=0)


def random_name() -> str:
    """Generate a random full name.

    Returns:
        Random first + last name string.
    """
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def batched(data: list, size: int) -> Generator:
    """Split a list into batches of given size.

    Args:
        data: List to split.
        size: Batch size.

    Yields:
        Successive batches as lists.
    """
    for i in range(0, len(data), size):
        yield data[i:i + size]


def log_progress(table: str, inserted: int, total: int) -> None:
    """Log insertion progress.

    Args:
        table: Table name being populated.
        inserted: Rows inserted so far.
        total: Total rows to insert.
    """
    pct = (inserted / total) * 100
    logger.info("%s: %s / %s rows (%.0f%%)",
                table, f"{inserted:,}", f"{total:,}", pct)


def table_is_empty_ss(conn: pyodbc.Connection, table: str) -> bool:
    """Check whether a SQL Server table has zero rows.

    Args:
        conn: Active SQL Server connection.
        table: Table name to check.

    Returns:
        True if table is empty.
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
    return cursor.fetchone()[0] == 0


def table_is_empty_pg(conn: psycopg.Connection, table: str) -> bool:
    """Check whether a PostgreSQL table has zero rows.

    Args:
        conn: Active psycopg connection.
        table: Table name to check.

    Returns:
        True if table is empty.
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    return cursor.fetchone()[0] == 0


# ---------------------------------------------------------------------------
# SQL Server connection
# ---------------------------------------------------------------------------

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
            f"SERVER={host};DATABASE={database};"
            f"UID={user};PWD={password};"
        )
    else:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={host};DATABASE={database};"
            f"Trusted_Connection=yes;"
        )
    return pyodbc.connect(conn_str, timeout=30)


# ---------------------------------------------------------------------------
# PostgreSQL connection
# ---------------------------------------------------------------------------

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
        connect_timeout=30,
    )


# ---------------------------------------------------------------------------
# Batch insert helpers
# ---------------------------------------------------------------------------

def sqlserver_batch_insert(
    conn: pyodbc.Connection,
    label: str,
    sql: str,
    rows: list,
) -> None:
    """Insert rows into SQL Server in batches with progress logging.

    Args:
        conn: Active SQL Server connection.
        label: Label for progress logging.
        sql: Parameterized INSERT statement using ? placeholders.
        rows: All rows to insert.
    """
    cursor = conn.cursor()
    cursor.fast_executemany = False
    total = len(rows)
    inserted = 0

    for batch in batched(rows, BATCH_SIZE):
        cursor.executemany(sql, batch)
        conn.commit()
        inserted += len(batch)
        log_progress(label, inserted, total)


def postgres_batch_insert(
    conn: psycopg.Connection,
    label: str,
    sql: str,
    rows: list,
) -> None:
    """Insert rows into PostgreSQL in batches with progress logging.

    Args:
        conn: Active psycopg connection.
        label: Label for progress logging.
        sql: Parameterized INSERT statement using %s placeholders.
        rows: All rows to insert.
    """
    cursor = conn.cursor()
    total = len(rows)
    inserted = 0

    for batch in batched(rows, BATCH_SIZE):
        cursor.executemany(sql, batch)
        conn.commit()
        inserted += len(batch)
        log_progress(label, inserted, total)


# ---------------------------------------------------------------------------
# SQL Server seeders
# ---------------------------------------------------------------------------

def seed_ecommerce(conn: pyodbc.Connection) -> None:
    """Seed Customers and Products in ECommerceDB.

    Args:
        conn: Active SQL Server connection to ECommerceDB.
    """
    if table_is_empty_ss(conn, "Customers"):
        total = 1_000_000
        logger.info("Generating %s customer rows...", f"{total:,}")
        rows = [
            (random_name(), f"user{i}@email.com",
             random.choice(CITIES), random.choice(COUNTRIES),
             random_date(1825, 30).strftime('%Y-%m-%d'))
            for i in range(1, total + 1)
        ]
        sqlserver_batch_insert(
            conn, "ECommerceDB.Customers",
            "INSERT INTO Customers (CustomerName, Email, City, Country, CreatedDate) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )
    else:
        logger.info("ECommerceDB.Customers already has data — skipping")

    if table_is_empty_ss(conn, "Products"):
        total = 100_000
        logger.info("Generating %s product rows...", f"{total:,}")
        rows = [
            (f"Product_{i}", random.choice(CATEGORIES),
             round(random.uniform(10.0, 50000.0), 2), random.choice(SUPPLIERS))
            for i in range(1, total + 1)
        ]
        sqlserver_batch_insert(
            conn, "ECommerceDB.Products",
            "INSERT INTO Products (ProductName, Category, UnitPrice, SupplierName) "
            "VALUES (?, ?, ?, ?)",
            rows,
        )
    else:
        logger.info("ECommerceDB.Products already has data — skipping")


def seed_employee_departments(conn: pyodbc.Connection) -> None:
    """Seed Departments in EmployeeManagementDB.

    Args:
        conn: Active SQL Server connection to EmployeeManagementDB.
    """
    if table_is_empty_ss(conn, "Departments"):
        rows = [
            (dept, random_name(), random.choice(CITIES),
             round(random.uniform(500_000, 10_000_000), 2))
            for dept in DEPARTMENTS
        ]
        sqlserver_batch_insert(
            conn, "EmployeeManagementDB.Departments",
            "INSERT INTO Departments (DepartmentName, ManagerName, LocationCity, Budget) "
            "VALUES (?, ?, ?, ?)",
            rows,
        )
    else:
        logger.info("EmployeeManagementDB.Departments already has data — skipping")


def seed_warehouse(conn: pyodbc.Connection) -> None:
    """Seed Suppliers and StockMovements in WarehouseManagementDB.

    Args:
        conn: Active SQL Server connection to WarehouseManagementDB.
    """
    if table_is_empty_ss(conn, "Suppliers"):
        rows = [
            (s, f"contact@{s.lower().replace(' ', '')}.com",
             random.choice(COUNTRIES), round(random.uniform(1.0, 5.0), 2))
            for s in SUPPLIERS
        ]
        sqlserver_batch_insert(
            conn, "WarehouseManagementDB.Suppliers",
            "INSERT INTO Suppliers (SupplierName, ContactEmail, Country, Rating) "
            "VALUES (?, ?, ?, ?)",
            rows,
        )
    else:
        logger.info("WarehouseManagementDB.Suppliers already has data — skipping")

    if table_is_empty_ss(conn, "StockMovements"):
        cursor = conn.cursor()
        cursor.execute("SELECT ProductID FROM Products")
        product_ids = [row[0] for row in cursor.fetchall()]
        cursor.execute("SELECT SupplierID FROM Suppliers")
        supplier_ids = [row[0] for row in cursor.fetchall()]

        if not product_ids or not supplier_ids:
            logger.error("Products or Suppliers missing — run their seeds first")
            return

        total = 2_000_000
        logger.info("Generating %s stock movement rows...", f"{total:,}")
        rows = [
            (random.choice(product_ids), random.choice(MOVEMENT_TYPES),
             random.randint(1, 1000), random_date(730, 0),
             random.choice(supplier_ids))
            for _ in range(total)
        ]
        sqlserver_batch_insert(
            conn, "WarehouseManagementDB.StockMovements",
            "INSERT INTO StockMovements "
            "(ProductID, MovementType, Quantity, MovementDate, SupplierID) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )
    else:
        logger.info("WarehouseManagementDB.StockMovements already has data — skipping")


# ---------------------------------------------------------------------------
# PostgreSQL seeders
# ---------------------------------------------------------------------------

def seed_hr_db(conn: psycopg.Connection) -> None:
    """Seed departments and employees in hr_db.

    Args:
        conn: Active psycopg connection to hr_db.
    """
    if table_is_empty_pg(conn, "departments"):
        rows = [
            (dept, random_name(), random.choice(CITIES),
             round(random.uniform(500_000, 10_000_000), 2))
            for dept in DEPARTMENTS
        ]
        postgres_batch_insert(
            conn, "hr_db.departments",
            "INSERT INTO departments (department_name, manager_name, location, budget) "
            "VALUES (%s, %s, %s, %s)",
            rows,
        )
    else:
        logger.info("hr_db.departments already has data — skipping")

    if table_is_empty_pg(conn, "employees"):
        cursor = conn.cursor()
        cursor.execute("SELECT department_id FROM departments")
        dept_ids = [row[0] for row in cursor.fetchall()] or [None]

        total = 500_000
        logger.info("Generating %s employee rows...", f"{total:,}")
        rows = [
            (random_name(), random.choice(DEPARTMENTS),
             random.randint(300_000, 5_000_000),
             random_date(5000, 30).date(),
             random.choice(dept_ids))
            for _ in range(total)
        ]
        postgres_batch_insert(
            conn, "hr_db.employees",
            "INSERT INTO employees "
            "(emp_name, department, salary, hire_date, department_id) "
            "VALUES (%s, %s, %s, %s, %s)",
            rows,
        )
    else:
        logger.info("hr_db.employees already has data — skipping")


def seed_inventory_db(conn: psycopg.Connection) -> None:
    """Seed products and stock_movements in inventory_db.

    Args:
        conn: Active psycopg connection to inventory_db.
    """
    if table_is_empty_pg(conn, "products"):
        total = 100_000
        logger.info("Generating %s inventory product rows...", f"{total:,}")
        rows = [
            (f"Product_{i}", random.randint(0, 50_000),
             random.choice(CITIES), random_date(365, 0))
            for i in range(1, total + 1)
        ]
        postgres_batch_insert(
            conn, "inventory_db.products",
            "INSERT INTO products (product_name, stock, warehouse, last_updated) "
            "VALUES (%s, %s, %s, %s)",
            rows,
        )
    else:
        logger.info("inventory_db.products already has data — skipping")

    if table_is_empty_pg(conn, "stock_movements"):
        cursor = conn.cursor()
        cursor.execute("SELECT product_id FROM products")
        product_ids = [row[0] for row in cursor.fetchall()]

        total = 2_000_000
        logger.info("Generating %s stock movement rows...", f"{total:,}")
        rows = [
            (random.choice(product_ids), random.choice(MOVEMENT_TYPES),
             random.randint(1, 1000), random_date(730, 0),
             f"Movement {i}")
            for i in range(1, total + 1)
        ]
        postgres_batch_insert(
            conn, "inventory_db.stock_movements",
            "INSERT INTO stock_movements "
            "(product_id, movement_type, quantity, movement_date, notes) "
            "VALUES (%s, %s, %s, %s, %s)",
            rows,
        )
    else:
        logger.info("inventory_db.stock_movements already has data — skipping")


def seed_dba_lab(conn: psycopg.Connection) -> None:
    """Seed customers, products, and orders in dba_lab.

    Args:
        conn: Active psycopg connection to dba_lab.
    """
    if table_is_empty_pg(conn, "customers"):
        total = 500_000
        logger.info("Generating %s dba_lab customer rows...", f"{total:,}")
        rows = [
            (random_name(), random.choice(CITIES), random_date(1825, 0))
            for _ in range(total)
        ]
        postgres_batch_insert(
            conn, "dba_lab.customers",
            "INSERT INTO customers (name, city, created_at) VALUES (%s, %s, %s)",
            rows,
        )
    else:
        logger.info("dba_lab.customers already has data — skipping")

    if table_is_empty_pg(conn, "products"):
        total = 100_000
        logger.info("Generating %s dba_lab product rows...", f"{total:,}")
        rows = [
            (f"Product_{i}", random.choice(CATEGORIES),
             round(random.uniform(10.0, 50000.0), 2))
            for i in range(1, total + 1)
        ]
        postgres_batch_insert(
            conn, "dba_lab.products",
            "INSERT INTO products (product_name, category, price) "
            "VALUES (%s, %s, %s)",
            rows,
        )
    else:
        logger.info("dba_lab.products already has data — skipping")

    if table_is_empty_pg(conn, "orders"):
        cursor = conn.cursor()
        cursor.execute("SELECT customer_id FROM customers LIMIT 50000")
        cust_ids = [row[0] for row in cursor.fetchall()]
        cursor.execute("SELECT product_id FROM products LIMIT 10000")
        prod_ids = [row[0] for row in cursor.fetchall()]

        total = 5_000_000
        logger.info("Generating %s dba_lab order rows...", f"{total:,}")
        rows = [
            (random.choice(cust_ids), random.choice(prod_ids),
             random.randint(1, 50), round(random.uniform(10.0, 50000.0), 2),
             random.choice(STATUSES), random_date(1825, 0))
            for _ in range(total)
        ]
        postgres_batch_insert(
            conn, "dba_lab.orders",
            "INSERT INTO orders "
            "(customer_id, product_id, quantity, price, status, order_time) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            rows,
        )
    else:
        logger.info("dba_lab.orders already has data — skipping")


def seed_sales_db(conn: psycopg.Connection) -> None:
    """Seed orders in sales_db.

    Args:
        conn: Active psycopg connection to sales_db.
    """
    if table_is_empty_pg(conn, "orders"):
        total = 5_000_000
        logger.info("Generating %s sales_db order rows...", f"{total:,}")
        rows = [
            (random_name(), f"Product_{random.randint(1, 100_000)}",
             random.randint(1, 50), round(random.uniform(10.0, 50000.0), 2),
             random_date(1825, 0))
            for _ in range(total)
        ]
        postgres_batch_insert(
            conn, "sales_db.orders",
            "INSERT INTO orders "
            "(customer_name, product_name, quantity, price, order_date) "
            "VALUES (%s, %s, %s, %s, %s)",
            rows,
        )
    else:
        logger.info("sales_db.orders already has data — skipping")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Seed all empty tables across SQL Server and PostgreSQL at million scale."""
    logger.info("Starting million-scale data seeding")
    start = datetime.now()

    logger.info("=" * 55)
    logger.info("SQL SERVER SEEDING")
    logger.info("=" * 55)

    for db, seeder in [
        ("ECommerceDB", seed_ecommerce),
        ("EmployeeManagementDB", seed_employee_departments),
        ("WarehouseManagementDB", seed_warehouse),
    ]:
        try:
            conn = get_sqlserver_connection(db)
            seeder(conn)
            conn.close()
        except pyodbc.Error as e:
            logger.error("%s seeding failed: %s", db, e)

    logger.info("=" * 55)
    logger.info("POSTGRESQL SEEDING")
    logger.info("=" * 55)

    for db, seeder in [
        ("hr_db", seed_hr_db),
        ("inventory_db", seed_inventory_db),
        ("dba_lab", seed_dba_lab),
        ("sales_db", seed_sales_db),
    ]:
        try:
            conn = get_postgres_connection(db)
            seeder(conn)
            conn.close()
        except psycopg.OperationalError as e:
            logger.error("%s seeding failed: %s", db, e)

    elapsed = datetime.now() - start
    logger.info("Seeding complete in %s", str(elapsed).split(".")[0])


if __name__ == "__main__":
    main()