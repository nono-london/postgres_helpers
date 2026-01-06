"""
Synchronous PostgreSQL connector with single connection.

This module provides a synchronous interface to PostgreSQL using psycopg2
with a single connection. For better performance in multi-threaded applications,
consider using PostgresConnectorPool instead.

Usage:
    from postgres_helpers.postgres_sync import PostgresConnector

    # As context manager (recommended)
    with PostgresConnector() as db:
        results = db.fetch_all_as_dicts("SELECT * FROM users")

    # Manual connection management
    db = PostgresConnector()
    try:
        results = db.fetch_all_as_dicts("SELECT * FROM users")
    finally:
        db.close_connection()

    # With transactions
    with db.transaction() as cursor:
        cursor.execute("INSERT INTO orders ...")
        cursor.execute("UPDATE inventory ...")
"""

import logging
from contextlib import contextmanager
from os import environ
from pathlib import Path
from typing import Union, Optional, List, Dict, Any, Tuple, Iterator

import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import Error
from psycopg2.errors import (
    UniqueViolation,
    ForeignKeyViolation,
    CheckViolation
)
from psycopg2.extensions import connection

from postgres_helpers.app_config import load_postgres_details_to_env
from postgres_helpers.exceptions import (
    PostgresHelperError,
    ConnectionError,
    QueryExecutionError,
    UniqueViolationError,
    ForeignKeyViolationError,
    CheckViolationError,
    TransactionError
)
from postgres_helpers.results import (
    QueryResult,
    ExecuteManyResult,
    InsertResult,
    UpsertResult,
    ConnectionInfo
)

logger = logging.getLogger(f"postgres_helpers:{Path(__file__).name}")


class PostgresConnector:
    """
    Synchronous PostgreSQL connector with single connection.

    This class manages a single database connection. For multi-threaded
    applications, consider using PostgresConnectorPool instead.

    Args:
        db_host: Database host (falls back to POSTGRES_DB_HOST env var)
        db_port: Database port (falls back to POSTGRES_DB_PORT env var)
        db_user: Database user (falls back to POSTGRES_DB_USER env var)
        db_password: Database password (falls back to POSTGRES_DB_PASS env var)
        db_name: Database name (falls back to POSTGRES_DB_NAME env var)
        connect_timeout: Connection timeout in seconds (default: 6)
        application_name: Name shown in pg_stat_activity (optional)

    Example:
        with PostgresConnector() as db:
            users = db.fetch_all_as_dicts("SELECT * FROM users")
    """

    def __init__(
            self,
            db_host: Optional[str] = None,
            db_port: Optional[str] = None,
            db_user: Optional[str] = None,
            db_password: Optional[str] = None,
            db_name: Optional[str] = None,
            connect_timeout: int = 6,
            application_name: Optional[str] = None
    ):
        if None in [db_host, db_port, db_name, db_user, db_password]:
            load_postgres_details_to_env()

        self.db_host = environ["POSTGRES_DB_HOST"] if db_host is None else db_host
        self.db_port = environ["POSTGRES_DB_PORT"] if db_port is None else str(db_port)
        self.db_user: str = environ["POSTGRES_DB_USER"] if db_user is None else db_user
        self.db_password: str = environ["POSTGRES_DB_PASS"] if db_password is None else db_password
        self.db_name: str = environ["POSTGRES_DB_NAME"] if db_name is None else db_name

        self.connect_timeout: int = connect_timeout
        self.application_name = application_name.replace(' ', '_') if application_name else None

        self.db_connection: Optional[connection] = None

    # =========================================================================
    # Context Manager Support
    # =========================================================================

    def __enter__(self) -> "PostgresConnector":
        """Enter context manager - opens connection."""
        self.open_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager - closes connection."""
        self.close_connection()

    # =========================================================================
    # Connection Lifecycle
    # =========================================================================

    def open_connection(self) -> None:
        """
        Open database connection if not already open.

        Raises:
            ConnectionError: If connection fails.
        """
        if self.db_connection is not None and not self.db_connection.closed:
            return

        try:
            self.db_connection = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
                connect_timeout=self.connect_timeout,
                application_name=self.application_name
            )
            # Enable autocommit by default for single queries
            self.db_connection.autocommit = True

        except Exception as ex:
            logger.error(f"Failed to connect: {ex}")
            raise ConnectionError(
                f"Failed to connect to PostgreSQL: {ex}",
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                original_error=ex
            )

    def close_connection(self) -> None:
        """
        Close database connection.

        Safe to call multiple times.
        """
        if self.db_connection is not None and not self.db_connection.closed:
            try:
                self.db_connection.close()
            except Exception as ex:
                logger.error(f"Error closing connection: {ex}")
            finally:
                self.db_connection = None

    def is_connected(self) -> bool:
        """Check if connection is open."""
        return self.db_connection is not None and not self.db_connection.closed

    def get_connection_info(self) -> ConnectionInfo:
        """Get information about the current connection."""
        info = ConnectionInfo(
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
            user=self.db_user,
            is_connected=self.is_connected()
        )

        if self.is_connected():
            try:
                cursor = self.db_connection.cursor()
                cursor.execute("SELECT version()")
                result = cursor.fetchone()
                info.server_version = result[0].split(",")[0].strip() if result else ""
                cursor.close()
            except Exception:
                pass

        return info

    # =========================================================================
    # Transaction Support
    # =========================================================================

    @contextmanager
    def transaction(self) -> Iterator:
        """
        Context manager for database transactions.

        Automatically commits on success, rolls back on exception.

        Yields:
            psycopg2 cursor with active transaction.

        Example:
            with db.transaction() as cursor:
                cursor.execute("INSERT INTO orders ...")
                cursor.execute("UPDATE inventory ...")
        """
        self.open_connection()

        # Disable autocommit for transaction
        original_autocommit = self.db_connection.autocommit
        self.db_connection.autocommit = False

        cursor = self.db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            yield cursor
            self.db_connection.commit()
        except Exception as ex:
            self.db_connection.rollback()
            logger.error(f"Transaction error, rolled back: {ex}")
            raise TransactionError(f"Transaction failed: {ex}", original_error=ex)
        finally:
            cursor.close()
            self.db_connection.autocommit = original_autocommit

    # =========================================================================
    # Error Handling Helper
    # =========================================================================

    def _convert_exception(
            self,
            ex: Exception,
            query: Optional[str] = None,
            params: Optional[tuple] = None
    ) -> PostgresHelperError:
        """Convert psycopg2 exceptions to postgres_helpers exceptions."""
        safe_query = query[:200] + "..." if query and len(query) > 200 else query

        if isinstance(ex, UniqueViolation):
            return UniqueViolationError(
                f"Unique constraint violation: {ex}",
                query=safe_query,
                params=params,
                original_error=ex
            )
        elif isinstance(ex, ForeignKeyViolation):
            return ForeignKeyViolationError(
                f"Foreign key constraint violation: {ex}",
                query=safe_query,
                params=params,
                original_error=ex
            )
        elif isinstance(ex, CheckViolation):
            return CheckViolationError(
                f"Check constraint violation: {ex}",
                query=safe_query,
                params=params,
                original_error=ex
            )
        elif isinstance(ex, Error):
            return QueryExecutionError(
                f"Query execution failed: {ex}",
                query=safe_query,
                params=params,
                original_error=ex
            )
        else:
            return QueryExecutionError(
                f"Unexpected error: {ex}",
                query=safe_query,
                params=params,
                original_error=ex
            )

    # =========================================================================
    # Query Execution Methods
    # =========================================================================

    def execute_one_query(
            self,
            sql_query: str,
            sql_variables: Optional[tuple] = None,
            close_connection: bool = False
    ) -> QueryResult:
        """
        Execute a single SQL query (INSERT, UPDATE, DELETE, etc.).

        Args:
            sql_query: The SQL query to execute.
            sql_variables: Query parameters as a tuple.
            close_connection: If True, close connection after execution.

        Returns:
            QueryResult with rows_affected and status_message.
        """
        self.open_connection()

        cursor = self.db_connection.cursor()

        try:
            cursor.execute(sql_query, sql_variables)

            return QueryResult(
                rows_affected=cursor.rowcount,
                status_message=cursor.statusmessage,
                success=True
            )

        except Exception as ex:
            logger.error(f"execute_one_query failed: {ex}")
            raise self._convert_exception(ex, sql_query, sql_variables)

        finally:
            cursor.close()
            if close_connection:
                self.close_connection()

    def execute_many_query(
            self,
            sql_query: str,
            tuples_list: List[tuple],
            close_connection: bool = False
    ) -> ExecuteManyResult:
        """
        Execute a query multiple times with different parameters.

        Args:
            sql_query: The SQL query to execute.
            tuples_list: List of parameter tuples.
            close_connection: If True, close connection after execution.

        Returns:
            ExecuteManyResult with execution statistics.
        """
        self.open_connection()

        cursor = self.db_connection.cursor()

        try:
            cursor.executemany(sql_query, tuples_list)

            return ExecuteManyResult(
                success=True,
                total_statements=len(tuples_list),
                rows_affected=cursor.rowcount
            )

        except Exception as ex:
            logger.error(f"execute_many_query failed: {ex}")
            raise self._convert_exception(ex, sql_query)

        finally:
            cursor.close()
            if close_connection:
                self.close_connection()

    # =========================================================================
    # Fetch Methods
    # =========================================================================

    def fetch_all_as_dicts(
            self,
            sql_query: str,
            sql_variables: Optional[tuple] = None,
            close_connection: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Fetch all rows as a list of dictionaries.

        Args:
            sql_query: SELECT query to execute.
            sql_variables: Query parameters as a tuple.
            close_connection: If True, close connection after execution.

        Returns:
            List of dicts where keys are column names.
        """
        self.open_connection()

        cursor = self.db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            cursor.execute(sql_query, sql_variables)
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

        except Exception as ex:
            logger.error(f"fetch_all_as_dicts failed: {ex}")
            raise self._convert_exception(ex, sql_query, sql_variables)

        finally:
            cursor.close()
            if close_connection:
                self.close_connection()

    def fetch_all_as_df(
            self,
            sql_query: str,
            sql_variables: Optional[tuple] = None,
            close_connection: bool = False
    ) -> pd.DataFrame:
        """
        Fetch all rows as a pandas DataFrame.

        Args:
            sql_query: SELECT query to execute.
            sql_variables: Query parameters as a tuple.
            close_connection: If True, close connection after execution.

        Returns:
            DataFrame with columns matching the query result.
        """
        results = self.fetch_all_as_dicts(
            sql_query=sql_query,
            sql_variables=sql_variables,
            close_connection=close_connection
        )
        return pd.DataFrame(results) if results else pd.DataFrame()

    def fetch_one_as_dict(
            self,
            sql_query: str,
            sql_variables: Optional[tuple] = None,
            close_connection: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a single row as a dictionary.

        Args:
            sql_query: SELECT query (should return 0 or 1 row).
            sql_variables: Query parameters as a tuple.
            close_connection: If True, close connection after execution.

        Returns:
            Dict with column names as keys, or None if no row found.
        """
        self.open_connection()

        cursor = self.db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            cursor.execute(sql_query, sql_variables)
            row = cursor.fetchone()
            return dict(row) if row else None

        except Exception as ex:
            logger.error(f"fetch_one_as_dict failed: {ex}")
            raise self._convert_exception(ex, sql_query, sql_variables)

        finally:
            cursor.close()
            if close_connection:
                self.close_connection()

    def fetch_value(
            self,
            sql_query: str,
            sql_variables: Optional[tuple] = None,
            close_connection: bool = False
    ) -> Optional[Any]:
        """
        Fetch a single value from the first column of the first row.

        Args:
            sql_query: SELECT query (should select one column).
            sql_variables: Query parameters as a tuple.
            close_connection: If True, close connection after execution.

        Returns:
            The value, or None if no row found.
        """
        self.open_connection()

        cursor = self.db_connection.cursor()

        try:
            cursor.execute(sql_query, sql_variables)
            row = cursor.fetchone()
            return row[0] if row else None

        except Exception as ex:
            logger.error(f"fetch_value failed: {ex}")
            raise self._convert_exception(ex, sql_query, sql_variables)

        finally:
            cursor.close()
            if close_connection:
                self.close_connection()

    # =========================================================================
    # Convenience Insert Methods
    # =========================================================================

    def insert_into_with_dict(
            self,
            table_name: str,
            parameters_dict: Dict[str, Any],
            on_duplicate_ignore: bool = True,
            close_connection: bool = False
    ) -> InsertResult:
        """
        Insert a row using a dictionary of column: value pairs.

        Args:
            table_name: Name of the table to insert into.
            parameters_dict: Dict mapping column names to values.
            on_duplicate_ignore: If True, ignore duplicate key errors.
            close_connection: If True, close connection after execution.

        Returns:
            InsertResult with insertion details.
        """
        placeholder = ", ".join(["%s"] * len(parameters_dict))
        columns = '"' + '","'.join(parameters_dict.keys()) + '"'
        conflict_clause = " ON CONFLICT DO NOTHING" if on_duplicate_ignore else ""

        query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholder}){conflict_clause}'
        params = tuple(parameters_dict.values())

        self.open_connection()
        cursor = self.db_connection.cursor()

        try:
            cursor.execute(query, params)

            return InsertResult(
                rows_affected=cursor.rowcount,
                success=True,
                was_duplicate=(cursor.rowcount == 0 and on_duplicate_ignore)
            )

        except UniqueViolation as ex:
            if on_duplicate_ignore:
                return InsertResult(rows_affected=0, success=True, was_duplicate=True)
            raise self._convert_exception(ex, query, params)

        except Exception as ex:
            logger.error(f"insert_into_with_dict failed: {ex}")
            raise self._convert_exception(ex, query, params)

        finally:
            cursor.close()
            if close_connection:
                self.close_connection()

    def insert_with_dict_returning(
            self,
            table_name: str,
            parameters_dict: Dict[str, Any],
            on_duplicate_ignore: bool = True,
            close_connection: bool = False
    ) -> InsertResult:
        """
        Insert a row and return the inserted row data.

        Args:
            table_name: Name of the table to insert into.
            parameters_dict: Dict mapping column names to values.
            on_duplicate_ignore: If True, ignore duplicate key errors.
            close_connection: If True, close connection after execution.

        Returns:
            InsertResult with returning_row containing the full inserted row.
        """
        placeholder = ", ".join(["%s"] * len(parameters_dict))
        columns = '"' + '","'.join(parameters_dict.keys()) + '"'
        conflict_clause = " ON CONFLICT DO NOTHING" if on_duplicate_ignore else ""

        query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholder}){conflict_clause} RETURNING *'
        params = tuple(parameters_dict.values())

        self.open_connection()
        cursor = self.db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            cursor.execute(query, params)
            row = cursor.fetchone()

            if row:
                row_dict = dict(row)
                return InsertResult(
                    rows_affected=1,
                    success=True,
                    was_duplicate=False,
                    returning_row=row_dict,
                    last_inserted_id=row_dict.get('id')
                )
            else:
                return InsertResult(rows_affected=0, success=True, was_duplicate=True)

        except UniqueViolation as ex:
            if on_duplicate_ignore:
                return InsertResult(rows_affected=0, success=True, was_duplicate=True)
            raise self._convert_exception(ex, query, params)

        except Exception as ex:
            logger.error(f"insert_with_dict_returning failed: {ex}")
            raise self._convert_exception(ex, query, params)

        finally:
            cursor.close()
            if close_connection:
                self.close_connection()

    def insert_into_with_dict_update(
            self,
            table_name: str,
            parameters_dict: Dict[str, Any],
            constraint_key: Optional[str] = None,
            on_duplicate_update: bool = True,
            close_connection: bool = False
    ) -> UpsertResult:
        """
        Insert a row, or update it if it already exists (upsert).

        Args:
            table_name: Name of the table.
            parameters_dict: Dict mapping column names to values.
            constraint_key: Name of the unique constraint.
            on_duplicate_update: If True, update on conflict.
            close_connection: If True, close connection after execution.

        Returns:
            UpsertResult with operation details.
        """
        placeholder = ", ".join(["%s"] * len(parameters_dict))
        columns = '"' + '","'.join(parameters_dict.keys()) + '"'

        if on_duplicate_update:
            if constraint_key is None:
                constraint_key = f"{table_name}_pkey"
            set_clause = ", ".join(
                f'"{key}" = EXCLUDED."{key}"' for key in parameters_dict.keys()
            )
            conflict_clause = (
                f" ON CONFLICT ON CONSTRAINT {constraint_key}"
                f" DO UPDATE SET {set_clause}"
            )
        else:
            conflict_clause = " ON CONFLICT DO NOTHING"

        query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholder}){conflict_clause}'
        params = tuple(parameters_dict.values())

        self.open_connection()
        cursor = self.db_connection.cursor()

        try:
            cursor.execute(query, params)

            return UpsertResult(
                rows_affected=cursor.rowcount,
                success=True,
                was_inserted=(cursor.rowcount > 0),
                was_updated=False
            )

        except Exception as ex:
            logger.error(f"insert_into_with_dict_update failed: {ex}")
            raise self._convert_exception(ex, query, params)

        finally:
            cursor.close()
            if close_connection:
                self.close_connection()

    def insert_into_with_dict_update_returning(
            self,
            table_name: str,
            parameters_dict: Dict[str, Any],
            constraint_key: Optional[str] = None,
            close_connection: bool = False
    ) -> UpsertResult:
        """
        Upsert a row and return the result with accurate insert/update detection.

        Args:
            table_name: Name of the table.
            parameters_dict: Dict mapping column names to values.
            constraint_key: Name of the unique constraint.
            close_connection: If True, close connection after execution.

        Returns:
            UpsertResult with accurate was_inserted/was_updated flags.
        """
        placeholder = ", ".join(["%s"] * len(parameters_dict))
        columns = '"' + '","'.join(parameters_dict.keys()) + '"'

        if constraint_key is None:
            constraint_key = f"{table_name}_pkey"

        set_clause = ", ".join(
            f'"{key}" = EXCLUDED."{key}"' for key in parameters_dict.keys()
        )

        query = (
            f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholder})'
            f' ON CONFLICT ON CONSTRAINT {constraint_key}'
            f' DO UPDATE SET {set_clause}'
            f' RETURNING *, xmax'
        )
        params = tuple(parameters_dict.values())

        self.open_connection()
        cursor = self.db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            cursor.execute(query, params)
            row = cursor.fetchone()

            if row:
                row_dict = dict(row)
                xmax = row_dict.pop('xmax', 0)

                return UpsertResult(
                    rows_affected=1,
                    success=True,
                    was_inserted=(xmax == 0),
                    was_updated=(xmax > 0),
                    returning_row=row_dict
                )
            else:
                return UpsertResult(rows_affected=0, success=True)

        except Exception as ex:
            logger.error(f"insert_into_with_dict_update_returning failed: {ex}")
            raise self._convert_exception(ex, query, params)

        finally:
            cursor.close()
            if close_connection:
                self.close_connection()

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def get_postgresql_version(self, close_connection: bool = False) -> str:
        """Get the PostgreSQL server version."""
        result = self.fetch_all_as_dicts(
            "SELECT version()",
            close_connection=close_connection
        )
        version = result[0]["version"].split(",")[0].strip()
        logger.info(f"Connected to: {version}")
        return version

    def table_exists(
            self,
            table_name: str,
            schema: str = "public",
            close_connection: bool = False
    ) -> bool:
        """Check if a table exists."""
        result = self.fetch_value(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
            """,
            (schema, table_name),
            close_connection=close_connection
        )
        return bool(result)


# =============================================================================
# Main (for testing)
# =============================================================================

if __name__ == "__main__":
    with PostgresConnector(application_name="test_sync") as db:
        version = db.get_postgresql_version()
        print(f"Connected to: {version}")

        results = db.fetch_all_as_df("SELECT version()")
        print(results)