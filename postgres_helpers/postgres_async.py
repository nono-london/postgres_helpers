"""
Async PostgreSQL connector with single connection.

This module provides an async interface to PostgreSQL using asyncpg with
a single connection. For better performance in concurrent applications,
consider using PostgresConnectorAsyncPool instead.

Usage:
    from postgres_helpers.postgres_async import PostgresConnectorAsync

    # As context manager (recommended)
    async with PostgresConnectorAsync() as db:
        results = await db.fetch_all_as_dicts("SELECT * FROM users")

    # Manual connection management
    db = PostgresConnectorAsync()
    try:
        results = await db.fetch_all_as_dicts("SELECT * FROM users")
    finally:
        await db.close_connection()

    # With transactions
    async with db.transaction() as conn:
        await conn.execute("INSERT INTO orders ...")
        await conn.execute("UPDATE inventory ...")
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from os import environ
from pathlib import Path
from typing import Union, Optional, List, Dict, Any, Tuple, AsyncIterator

import asyncpg
import pandas as pd
from asyncpg.connection import Connection

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


class PostgresConnectorAsync:
    """
    Async PostgreSQL connector with single connection.

    This class manages a single database connection. For concurrent
    applications, consider using PostgresConnectorAsyncPool instead.

    Args:
        db_host: Database host (falls back to POSTGRES_DB_HOST env var)
        db_port: Database port (falls back to POSTGRES_DB_PORT env var)
        db_user: Database user (falls back to POSTGRES_DB_USER env var)
        db_password: Database password (falls back to POSTGRES_DB_PASS env var)
        db_name: Database name (falls back to POSTGRES_DB_NAME env var)
        application_name: Name shown in pg_stat_activity (optional)
        command_timeout: Default query timeout in seconds (optional)

    Example:
        async with PostgresConnectorAsync() as db:
            users = await db.fetch_all_as_dicts("SELECT * FROM users")
    """

    def __init__(
            self,
            db_host: Optional[str] = None,
            db_port: Optional[str] = None,
            db_user: Optional[str] = None,
            db_password: Optional[str] = None,
            db_name: Optional[str] = None,
            application_name: Optional[str] = None,
            command_timeout: Optional[float] = None
    ):
        if None in [db_host, db_port, db_name, db_user, db_password]:
            load_postgres_details_to_env()

        self.db_host = environ["POSTGRES_DB_HOST"] if db_host is None else db_host
        self.db_port = environ["POSTGRES_DB_PORT"] if db_port is None else str(db_port)
        self.db_user: str = environ["POSTGRES_DB_USER"] if db_user is None else db_user
        self.db_password: str = environ["POSTGRES_DB_PASS"] if db_password is None else db_password
        self.db_name: str = environ["POSTGRES_DB_NAME"] if db_name is None else db_name

        self.command_timeout: Optional[float] = command_timeout
        self.server_settings = {'application_name': application_name} if application_name else None

        self.db_connection: Optional[Connection] = None

    # =========================================================================
    # Context Manager Support
    # =========================================================================

    async def __aenter__(self) -> "PostgresConnectorAsync":
        """Enter async context manager - opens connection."""
        await self.open_connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager - closes connection."""
        await self.close_connection()

    # =========================================================================
    # Connection Lifecycle
    # =========================================================================

    async def open_connection(self) -> None:
        """
        Open database connection if not already open.

        Raises:
            ConnectionError: If connection fails.
        """
        if self.db_connection is not None and not self.db_connection.is_closed():
            return

        try:
            self.db_connection = await asyncpg.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name,
                command_timeout=self.command_timeout,
                server_settings=self.server_settings
            )
        except Exception as ex:
            logger.error(f"Failed to connect: {ex}")
            raise ConnectionError(
                f"Failed to connect to PostgreSQL: {ex}",
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                original_error=ex
            )

    async def close_connection(self) -> None:
        """
        Close database connection.

        Safe to call multiple times.
        """
        if self.db_connection is not None and not self.db_connection.is_closed():
            try:
                await self.db_connection.close()
            except Exception as ex:
                logger.error(f"Error closing connection: {ex}")
            finally:
                self.db_connection = None

    def is_connected(self) -> bool:
        """Check if connection is open."""
        return self.db_connection is not None and not self.db_connection.is_closed()

    async def get_connection_info(self) -> ConnectionInfo:
        """Get information about the current connection."""
        info = ConnectionInfo(
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
            user=self.db_user,
            is_connected=self.is_connected()
        )

        if self.db_connection and not self.db_connection.is_closed():
            try:
                info.server_version = str(self.db_connection.get_server_version())
            except Exception:
                pass

        return info

    # =========================================================================
    # Transaction Support
    # =========================================================================

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Connection]:
        """
        Context manager for database transactions.

        Automatically commits on success, rolls back on exception.

        Yields:
            asyncpg.Connection with active transaction.

        Example:
            async with db.transaction() as conn:
                await conn.execute("INSERT INTO orders ...")
                await conn.execute("UPDATE inventory ...")
        """
        await self.open_connection()

        try:
            async with self.db_connection.transaction():
                yield self.db_connection
        except asyncpg.PostgresError as ex:
            logger.error(f"Transaction error: {ex}")
            raise TransactionError(f"Transaction failed: {ex}", original_error=ex)

    # =========================================================================
    # Error Handling Helper
    # =========================================================================

    def _convert_exception(
            self,
            ex: Exception,
            query: Optional[str] = None,
            params: Optional[Tuple] = None
    ) -> PostgresHelperError:
        """Convert asyncpg exceptions to postgres_helpers exceptions."""
        safe_query = query[:200] + "..." if query and len(query) > 200 else query

        if isinstance(ex, asyncpg.UniqueViolationError):
            return UniqueViolationError(
                f"Unique constraint violation: {ex}",
                query=safe_query,
                params=params,
                original_error=ex,
                constraint_name=getattr(ex, 'constraint_name', None),
                detail=getattr(ex, 'detail', None)
            )
        elif isinstance(ex, asyncpg.ForeignKeyViolationError):
            return ForeignKeyViolationError(
                f"Foreign key constraint violation: {ex}",
                query=safe_query,
                params=params,
                original_error=ex,
                constraint_name=getattr(ex, 'constraint_name', None),
                detail=getattr(ex, 'detail', None)
            )
        elif isinstance(ex, asyncpg.CheckViolationError):
            return CheckViolationError(
                f"Check constraint violation: {ex}",
                query=safe_query,
                params=params,
                original_error=ex,
                constraint_name=getattr(ex, 'constraint_name', None)
            )
        elif isinstance(ex, asyncpg.PostgresError):
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

    async def execute_one_query(
            self,
            sql_query: str,
            sql_variables: Optional[Tuple] = None,
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

        Raises:
            ConnectionError: If connection fails.
            UniqueViolationError: If unique constraint is violated.
            QueryExecutionError: For other query errors.
        """
        await self.open_connection()

        try:
            result = await self.db_connection.execute(
                sql_query,
                *(sql_variables if sql_variables else ())
            )

            rows_affected = -1
            try:
                rows_affected = int(result.split()[-1])
            except (ValueError, IndexError):
                pass

            return QueryResult(
                rows_affected=rows_affected,
                status_message=result,
                success=True
            )

        except Exception as ex:
            logger.error(f"execute_one_query failed: {ex}")
            raise self._convert_exception(ex, sql_query, sql_variables)

        finally:
            if close_connection:
                await self.close_connection()

    async def execute_many_query(
            self,
            sql_query: str,
            tuples: List[Tuple],
            close_connection: bool = False
    ) -> ExecuteManyResult:
        """
        Execute a query multiple times with different parameters.

        Args:
            sql_query: The SQL query to execute.
            tuples: List of parameter tuples.
            close_connection: If True, close connection after execution.

        Returns:
            ExecuteManyResult with execution statistics.
        """
        await self.open_connection()

        try:
            await self.db_connection.executemany(sql_query, tuples)

            return ExecuteManyResult(
                success=True,
                total_statements=len(tuples)
            )

        except Exception as ex:
            logger.error(f"execute_many_query failed: {ex}")
            raise self._convert_exception(ex, sql_query)

        finally:
            if close_connection:
                await self.close_connection()

    # =========================================================================
    # Fetch Methods
    # =========================================================================

    async def fetch_all_as_dicts(
            self,
            sql_query: str,
            sql_variables: Optional[Tuple] = None,
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
        await self.open_connection()

        try:
            results = await self.db_connection.fetch(
                sql_query,
                *(sql_variables if sql_variables else ())
            )
            return [dict(r.items()) for r in results]

        except Exception as ex:
            logger.error(f"fetch_all_as_dicts failed: {ex}")
            raise self._convert_exception(ex, sql_query, sql_variables)

        finally:
            if close_connection:
                await self.close_connection()

    async def fetch_all_as_df(
            self,
            sql_query: str,
            sql_variables: Optional[Tuple] = None,
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
        results = await self.fetch_all_as_dicts(
            sql_query=sql_query,
            sql_variables=sql_variables,
            close_connection=close_connection
        )
        return pd.DataFrame(results) if results else pd.DataFrame()

    async def fetch_one_as_dict(
            self,
            sql_query: str,
            sql_variables: Optional[Tuple] = None,
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
        await self.open_connection()

        try:
            result = await self.db_connection.fetchrow(
                sql_query,
                *(sql_variables if sql_variables else ())
            )
            return dict(result.items()) if result else None

        except Exception as ex:
            logger.error(f"fetch_one_as_dict failed: {ex}")
            raise self._convert_exception(ex, sql_query, sql_variables)

        finally:
            if close_connection:
                await self.close_connection()

    async def fetch_value(
            self,
            sql_query: str,
            sql_variables: Optional[Tuple] = None,
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
        await self.open_connection()

        try:
            return await self.db_connection.fetchval(
                sql_query,
                *(sql_variables if sql_variables else ())
            )

        except Exception as ex:
            logger.error(f"fetch_value failed: {ex}")
            raise self._convert_exception(ex, sql_query, sql_variables)

        finally:
            if close_connection:
                await self.close_connection()

    # =========================================================================
    # Convenience Insert Methods
    # =========================================================================

    async def insert_into_with_dict(
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
        columns = '"' + '","'.join(parameters_dict.keys()) + '"'
        placeholders = ", ".join(f"${i + 1}" for i in range(len(parameters_dict)))
        conflict_clause = " ON CONFLICT DO NOTHING" if on_duplicate_ignore else ""

        query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders}){conflict_clause}'
        params = tuple(parameters_dict.values())

        await self.open_connection()

        try:
            result = await self.db_connection.execute(query, *params)

            rows_affected = 0
            try:
                rows_affected = int(result.split()[-1])
            except (ValueError, IndexError):
                pass

            return InsertResult(
                rows_affected=rows_affected,
                success=True,
                was_duplicate=(rows_affected == 0 and on_duplicate_ignore)
            )

        except asyncpg.UniqueViolationError as ex:
            if on_duplicate_ignore:
                return InsertResult(rows_affected=0, success=True, was_duplicate=True)
            raise self._convert_exception(ex, query, params)

        except Exception as ex:
            logger.error(f"insert_into_with_dict failed: {ex}")
            raise self._convert_exception(ex, query, params)

        finally:
            if close_connection:
                await self.close_connection()

    async def insert_with_dict_returning(
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
        columns = '"' + '","'.join(parameters_dict.keys()) + '"'
        placeholders = ", ".join(f"${i + 1}" for i in range(len(parameters_dict)))
        conflict_clause = " ON CONFLICT DO NOTHING" if on_duplicate_ignore else ""

        query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders}){conflict_clause} RETURNING *'
        params = tuple(parameters_dict.values())

        await self.open_connection()

        try:
            row = await self.db_connection.fetchrow(query, *params)

            if row:
                row_dict = dict(row.items())
                return InsertResult(
                    rows_affected=1,
                    success=True,
                    was_duplicate=False,
                    returning_row=row_dict,
                    last_inserted_id=row_dict.get('id')
                )
            else:
                return InsertResult(rows_affected=0, success=True, was_duplicate=True)

        except asyncpg.UniqueViolationError as ex:
            if on_duplicate_ignore:
                return InsertResult(rows_affected=0, success=True, was_duplicate=True)
            raise self._convert_exception(ex, query, params)

        except Exception as ex:
            logger.error(f"insert_with_dict_returning failed: {ex}")
            raise self._convert_exception(ex, query, params)

        finally:
            if close_connection:
                await self.close_connection()

    async def insert_into_with_dict_update(
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
            constraint_key: Name of the unique constraint for conflict detection.
            on_duplicate_update: If True, update on conflict.
            close_connection: If True, close connection after execution.

        Returns:
            UpsertResult with operation details.
        """
        columns = '"' + '","'.join(parameters_dict.keys()) + '"'
        placeholders = ", ".join(f"${i + 1}" for i in range(len(parameters_dict)))

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

        query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders}){conflict_clause}'
        params = tuple(parameters_dict.values())

        await self.open_connection()

        try:
            result = await self.db_connection.execute(query, *params)

            rows_affected = 0
            try:
                rows_affected = int(result.split()[-1])
            except (ValueError, IndexError):
                pass

            return UpsertResult(
                rows_affected=rows_affected,
                success=True,
                was_inserted=(rows_affected > 0),
                was_updated=False
            )

        except Exception as ex:
            logger.error(f"insert_into_with_dict_update failed: {ex}")
            raise self._convert_exception(ex, query, params)

        finally:
            if close_connection:
                await self.close_connection()

    async def insert_into_with_dict_update_returning(
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
        columns = '"' + '","'.join(parameters_dict.keys()) + '"'
        placeholders = ", ".join(f"${i + 1}" for i in range(len(parameters_dict)))

        if constraint_key is None:
            constraint_key = f"{table_name}_pkey"

        set_clause = ", ".join(
            f'"{key}" = EXCLUDED."{key}"' for key in parameters_dict.keys()
        )

        query = (
            f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders})'
            f' ON CONFLICT ON CONSTRAINT {constraint_key}'
            f' DO UPDATE SET {set_clause}'
            f' RETURNING *, xmax'
        )
        params = tuple(parameters_dict.values())

        await self.open_connection()

        try:
            row = await self.db_connection.fetchrow(query, *params)

            if row:
                row_dict = dict(row.items())
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
            if close_connection:
                await self.close_connection()

    # =========================================================================
    # Utility Methods
    # =========================================================================

    async def get_postgresql_version(self, close_connection: bool = False) -> str:
        """Get the PostgreSQL server version."""
        result = await self.fetch_all_as_dicts(
            "SELECT version()",
            close_connection=close_connection
        )
        version = result[0]["version"].split(",")[0].strip()
        logger.info(f"Connected to: {version}")
        return version

    async def table_exists(
            self,
            table_name: str,
            schema: str = "public",
            close_connection: bool = False
    ) -> bool:
        """Check if a table exists."""
        result = await self.fetch_value(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = $1 AND table_name = $2
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
    async def main():
        async with PostgresConnectorAsync(application_name="test_async") as db:
            version = await db.get_postgresql_version()
            print(f"Connected to: {version}")

            results = await db.fetch_all_as_df("SELECT version()")
            print(results)


    asyncio.run(main())