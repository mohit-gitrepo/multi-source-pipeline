"""
Base Extractor — Abstract Base Class.

Defines the contract that ALL source extractors must follow.
Every extractor in this pipeline (SQL Server, PostgreSQL, and
any future source) must inherit from this class and implement
get_connection() and extract().

LEARNING NOTE — Why This File Exists:
    Without this base class, SqlServerExtractor and PostgresExtractor
    would be completely independent classes with no guaranteed structure.
    The pipeline orchestrator would need to know which class it is
    dealing with and call different methods for each.

    With this base class, the pipeline just calls extractor.extract()
    on anything — it does not care if it is SQL Server or PostgreSQL.
    This is called polymorphism — one interface, many implementations.
"""

import logging  # Standard Python logging library

# abc = Abstract Base Classes — Python's built-in module for defining contracts
# ABC  = the class we inherit from to make our class abstract
# abstractmethod = decorator that marks a method as "must be implemented by subclass"
from abc import ABC, abstractmethod

# dataclass decorator automatically generates __init__, __repr__, __eq__
# based on the fields we define — removes boilerplate constructor code
# field() is used when a field needs a factory function instead of a direct default
from dataclasses import dataclass, field

# Module-level logger
logger = logging.getLogger(__name__)


@dataclass  # This decorator makes ExtractResult a dataclass — see LEARNING NOTE below
class ExtractResult:
    """Holds the result of a single table extraction.

    LEARNING NOTE — Why a dataclass instead of a dictionary:
        We could return a plain dictionary: {"rows": [...], "columns": [...]}
        But dictionaries have no type hints, no validation, and no
        autocomplete in your IDE. A dataclass gives us:
        - Named fields with types (self-documenting)
        - IDE autocomplete when accessing result.rows, result.columns
        - A clean __repr__ for debugging
        - Immutability options if needed later

    LEARNING NOTE — Why error is None by default:
        None means "no error occurred". If extraction succeeds, error stays None.
        If extraction fails, we set error = str(exception_message).
        The pipeline checks: if result.error is not None → log and skip loading.

    Attributes:
        source_engine: Which DB engine this came from (sqlserver / postgres).
        source_database: Source database name (e.g. ECommerceDB).
        source_table: Source table name (e.g. Orders).
        target_table: Target staging table name in analytics_warehouse.
        rows: Extracted rows — list of tuples, one tuple per row.
        columns: Column names in the same order as values in each row tuple.
        row_count: Number of rows extracted — stored separately for fast access.
        error: Error message string if extraction failed, None if successful.
    """

    # Required fields — must be provided when creating an ExtractResult
    # These are positional — order matters when not using keyword arguments
    source_engine: str      # e.g. "sqlserver" or "postgres"
    source_database: str    # e.g. "ECommerceDB"
    source_table: str       # e.g. "Orders"
    target_table: str       # e.g. "stg_ss_orders"

    # Optional fields with defaults — do not need to be provided at creation
    # field(default_factory=list) creates a NEW empty list for each instance
    # IMPORTANT: Never use rows: list = [] directly in a dataclass —
    # all instances would share the SAME list object (Python gotcha)
    rows: list = field(default_factory=list)
    columns: list = field(default_factory=list)

    # row_count defaults to 0 — updated after fetchall() completes
    row_count: int = 0

    # error defaults to None — means "no error"
    # str | None means this field accepts either a string or None
    error: str | None = None


class BaseExtractor(ABC):
    """Abstract base class for all source extractors.

    LEARNING NOTE — What ABC Does:
        ABC stands for Abstract Base Class. When a class inherits from ABC,
        Python treats it as a template — it cannot be instantiated directly.

        Try running: extractor = BaseExtractor("mydb")
        Python raises: TypeError: Can't instantiate abstract class

        This forces developers to always use a concrete subclass like
        SqlServerExtractor or PostgresExtractor — never BaseExtractor itself.

    LEARNING NOTE — What @abstractmethod Does:
        Any method decorated with @abstractmethod MUST be implemented
        by every subclass. If a subclass skips one, Python raises a
        TypeError when you try to create an instance of that subclass.

        This is a compile-time safety check — catches missing implementations
        before the pipeline even runs.

    LEARNING NOTE — Design Pattern:
        This is the Template Method / Strategy pattern.
        BaseExtractor defines WHAT must be done (connect, extract).
        Each subclass defines HOW to do it for its specific engine.
        The pipeline orchestrator only knows about the WHAT — not the HOW.
    """

    def __init__(self, database: str) -> None:
        """Initialise the extractor for a given source database.

        LEARNING NOTE:
            This __init__ runs for ALL subclasses automatically because
            they call super().__init__() — or in this case Python calls
            it implicitly when no __init__ is defined in the subclass.

            We store database as self.database so every subclass method
            can access it via self.database without receiving it again
            as a parameter.

            We create self.logger here so every subclass automatically
            gets a logger named after its own class — SqlServerExtractor
            gets a logger called "SqlServerExtractor", not "BaseExtractor".

        Args:
            database: Name of the source database to connect to.
                      e.g. "ECommerceDB", "hr_db"
        """
        # Store database name as instance variable — accessible in all methods
        self.database = database

        # self.__class__.__name__ returns the actual subclass name at runtime
        # So SqlServerExtractor gets logger "SqlServerExtractor"
        # And PostgresExtractor gets logger "PostgresExtractor"
        # This makes log output clearly show which extractor produced each message
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod  # Decorator — subclass MUST implement this or TypeError is raised
    def get_connection(self):
        """Return an active connection to the source database.

        LEARNING NOTE:
            This method has no body (just a docstring and pass implicitly).
            It is a declaration — "every extractor must be able to connect."
            The HOW (connection string, driver, auth) is left to each subclass.

            SqlServerExtractor implements this with pyodbc.
            PostgresExtractor implements this with psycopg.
            A future MySQLExtractor would implement this with mysql-connector.

        Returns:
            Active database connection object.
            Type varies by engine — pyodbc.Connection or psycopg.Connection.

        Raises:
            Exception: If connection cannot be established.
                       Specific exception type depends on the engine.
        """

    @abstractmethod  # Decorator — subclass MUST implement this or TypeError is raised
    def extract(self, table: str, target_table: str,
                query: str | None = None) -> ExtractResult:
        """Extract data from a source table.

        LEARNING NOTE:
            Again, no body — just a declaration.
            The pipeline orchestrator calls this method on every extractor.
            It always returns an ExtractResult — regardless of engine.

            This consistent return type is what allows the loader to work
            with results from both SQL Server and PostgreSQL identically.
            The loader just receives ExtractResult objects — it does not
            know or care which engine produced them.

        Args:
            table: Source table name to extract from.
            target_table: Staging table name in analytics_warehouse.
            query: Optional custom SQL. If None, extracts all rows.

        Returns:
            ExtractResult containing extracted data and metadata.
        """