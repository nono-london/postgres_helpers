"""
Async PostgreSQL connector with connection pooling.

This module provides an async interface to PostgreSQL using asyncpg with
connection pooling for better performance in concurrent applications.

Usage:
    from postgres_helpers.postgres_async_pool import PostgresConnectorAsyncPool

    # Basic usage
    db = PostgresConnectorAsyncPool()
    results = await db.fetch_all_as_dicts("SELECT * FROM users WHERE id = $1", (1,))
    await db.close_pool()

    # As context manager (recommended)
    async with PostgresConnectorAsyncPool() as db:
        results = await db.fetch_all_as_dicts("SELECT * FROM users")

    # With transactions
    async with db.transaction() as conn:
        await conn.execute("INSERT INTO orders ...")
        await conn.execute("UPDATE inventory ...")
        # Auto-commits on success, auto-rollbacks on exception

Error Handling:
    from postgres_helpers.exceptions import (
        PostgresHelperError,
        UniqueViolationError,
        QueryExecutionError
    )

    try:
        await db.insert_into_with_dict("users", {"email": "exists@example.com"})
    except UniqueViolationError:
        print("Email already registered")
    except PostgresHelperError as e:
        print(f"Database error: {e}")
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from os import getenv
from pathlib import Path
from typing import (
    Union, Optional, List, Dict, Tuple, Any, AsyncIterator
)

import asyncpg
from asyncpg.pool import Pool
from asyncpg.connection import Connection
import pandas as pd

from postgres_helpers.app_config import load_postgres_details_to_env
from postgres_helpers.exceptions import (
    PostgresHelperError,
    ConnectionError,
    PoolError,
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


class PostgresConnectorAsyncPool:
    """
    Async PostgreSQL connector with connection pooling.

    This class manages a pool of database connections for efficient
    concurrent database access. It supports context manager protocol
    for automatic cleanup.

    Args:
        pool_size_max: Maximum number of connections in the pool (default: 5)
        pool_size_min: Minimum number of connections to maintain (default: 3)
        db_host: Database host (falls back to POSTGRES_DB_HOST env var)
        db_port: Database port (falls back to POSTGRES_DB_PORT env var)
        db_user: Database user (falls back to POSTGRES_DB_USER env var)
        db_password: Database password (falls back to POSTGRES_DB_PASS env var)
        db_name: Database name (falls back to POSTGRES_DB_NAME env var)
        application_name: Name shown in pg_stat_activity (optional)
        command_timeout: Default query timeout in seconds (optional)

    Example:
        # Using context manager (recommended)
        async with PostgresConnectorAsyncPool(pool_size_max=10) as db:
            users = await db.fetch_all_as_dicts("SELECT * FROM users")

        # Manual management
        db = PostgresConnectorAsyncPool()
        try:
            users = await db.fetch_all_as_dicts("SELECT * FROM users")
        finally:
            await db.close_pool()
    """

    def __init__(
            self,
            pool_size_max: int = 5,
            pool_size_min: int = 3,
            db_host: Optional[str] = None,
            db_port: Optional[str] = None,
            db_user: Optional[str] = None,
            db_password: Optional[str] = None,
            db_name: Optional[str] = None,
            application_name: Optional[str] = None,
            command_timeout: Optional[float] = None
    ):
        # Load env vars if any connection param is missing
        if None in [db_host, db_port, db_name, db_user, db_password]:
            load_postgres_details_to_env()

        self.db_host = getenv("POSTGRES_DB_HOST") if db_host is None else db_host
        self.db_port = getenv("POSTGRES_DB_PORT") if db_port is None else str(db_port)
        self.db_user: str = getenv("POSTGRES_DB_USER") if db_user is None else db_user
        self.db_password: str = getenv("POSTGRES_DB_PASS") if db_password is None else db_password
        self.db_name: str = getenv("POSTGRES_DB_NAME") if db_name is None else db_name

        # Pool configuration
        self.pool_size_max: int = pool_size_max
        self.pool_size_min: int = pool_size_min
        self.command_timeout: Optional[float] = command_timeout

        # Pool instance
        self.db_connection_pool: Optional[Pool] = None

        # Server settings for application name visibility in pg_stat_activity
        self.server_settings = {'application_name': application_name} if application_name else None

    # =========================================================================
    # Context Manager Support
    # =========================================================================

    async def __aenter__(self) -> "PostgresConnectorAsyncPool":
        """Enter async context manager - creates pool."""
        await self._create_pool_connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager - closes pool."""
        await self.close_pool()

    # =========================================================================
    # Pool Lifecycle
    # =========================================================================

    async def _create_pool_connection(self) -> None:
        """
        Create the connection pool if it doesn't exist.

        Raises:
            PoolError: If pool creation fails.
        """
        if self.db_connection_pool is not None:
            return

        # Ensure min < max
        if self.pool_size_min >= self.pool_size_max:
            self.pool_size_min = max(1, self.pool_size_max - 1)

        try:
            self.db_connection_pool = await asyncpg.create_pool(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name,
                max_size=self.pool_size_max,
                min_size=self.pool_size_min,
                command_timeout=self.command_timeout,
                server_settings=self.server_settings
            )
        except Exception as ex:
            logger.error(f"Failed to create connection pool: {ex}")
            raise PoolError(
                f"Failed to create connection pool: {ex}",
                pool_size_min=self.pool_size_min,
                pool_size_max=self.pool_size_max,
                original_error=ex
            )

    async def close_pool(self) -> None:
        """
        Close the connection pool and release all connections.

        Safe to call multiple times. After closing, the pool can be
        recreated by calling any query method.
        """
        if self.db_connection_pool is not None:
            await self.db_connection_pool.close()
            self.db_connection_pool = None
            logger.debug("Connection pool closed")

    def is_pool_active(self) -> bool:
        """Check if the connection pool is active."""
        return self.db_connection_pool is not None

    async def get_pool_status(self) -> ConnectionInfo:
        """
        Get information about the current pool and connection.

        Returns:
            ConnectionInfo with pool statistics.
        """
        info = ConnectionInfo(
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
            user=self.db_user,
            is_connected=self.is_pool_active()
        )

        if self.db_connection_pool:
            info.pool_size = self.db_connection_pool.get_size()
            info.pool_free = self.db_connection_pool.get_idle_size()

            # Get server version
            try:
                async with self.db_connection_pool.acquire() as conn:
                    info.server_version = str(conn.get_server_version())
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

        Provides a connection with an active transaction. The transaction
        is automatically committed on successful exit, or rolled back if
        an exception occurs.

        Yields:
            asyncpg.Connection: A connection with an active transaction.

        Raises:
            TransactionError: If transaction management fails.

        Example:
            async with db.transaction() as conn:
                # All queries here are in the same transaction
                await conn.execute("INSERT INTO orders (customer_id) VALUES ($1)", customer_id)
                order_id = await conn.fetchval("SELECT lastval()")
                await conn.execute(
                    "INSERT INTO order_items (order_id, product_id) VALUES ($1, $2)",
                    order_id, product_id
                )
                # Commits automatically here
                # If any exception occurs, rollback happens automatically
        """
        await self._create_pool_connection()

        try:
            async with self.db_connection_pool.acquire() as conn:
                async with conn.transaction():
                    yield conn
        except asyncpg.PostgresError as ex:
            logger.error(f"Transaction error: {ex}")
            raise TransactionError(
                f"Transaction failed: {ex}",
                original_error=ex
            )

    @asynccontextmanager
    async def acquire_connection(self) -> AsyncIterator[Connection]:
        """
        Acquire a connection from the pool without starting a transaction.

        Use this when you need raw connection access but don't need
        transaction management.

        Yields:
            asyncpg.Connection: A connection from the pool.

        Example:
            async with db.acquire_connection() as conn:
                await conn.copy_to_table('my_table', source=file)
        """
        await self._create_pool_connection()
        async with self.db_connection_pool.acquire() as conn:
            yield conn

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

        # Truncate query for security (don't log full queries with sensitive data)
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
            sql_variables: Optional[Tuple] = None
    ) -> QueryResult:
        """
        Execute a single SQL query (INSERT, UPDATE, DELETE, CREATE, etc.).

        Args:
            sql_query: The SQL query to execute.
            sql_variables: Query parameters as a tuple.

        Returns:
            QueryResult with rows_affected, status_message, etc.

        Raises:
            PoolError: If pool creation fails.
            UniqueViolationError: If unique constraint is violated.
            ForeignKeyViolationError: If foreign key constraint is violated.
            QueryExecutionError: For other query errors.

        Example:
            result = await db.execute_one_query(
                "UPDATE users SET active = $1 WHERE last_login < $2",
                (False, datetime(2023, 1, 1))
            )
            print(f"Deactivated {result.rows_affected} users")
        """
        await self._create_pool_connection()

        try:
            async with self.db_connection_pool.acquire() as conn:
                result = await conn.execute(
                    sql_query,
                    *(sql_variables if sql_variables else ())
                )

            # Parse result string (e.g., "UPDATE 5" or "INSERT 0 1")
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

    async def execute_many_query(
            self,
            sql_query: str,
            tuples: List[Tuple]
    ) -> ExecuteManyResult:
        """
        Execute a query multiple times with different parameters.

        This is more efficient than calling execute_one_query in a loop
        as it uses a single transaction.

        Note: If one statement fails, the entire batch is rolled back.

        Args:
            sql_query: The SQL query to execute (with $1, $2, ... placeholders).
            tuples: List of parameter tuples, one per execution.

        Returns:
            ExecuteManyResult with execution statistics.

        Raises:
            PoolError: If pool creation fails.
            QueryExecutionError: If any statement fails (batch is rolled back).

        Example:
            await db.execute_many_query(
                "INSERT INTO logs (level, message) VALUES ($1, $2)",
                [
                    ("INFO", "User logged in"),
                    ("WARNING", "Invalid password attempt"),
                    ("INFO", "User logged out")
                ]
            )
        """
        await self._create_pool_connection()

        try:
            async with self.db_connection_pool.acquire() as conn:
                await conn.executemany(sql_query, tuples)

            return ExecuteManyResult(
                success=True,
                total_statements=len(tuples)
            )

        except Exception as ex:
            logger.error(f"execute_many_query failed: {ex}")
            raise self._convert_exception(ex, sql_query)

    # =========================================================================
    # Fetch Methods
    # =========================================================================

    async def fetch_all_as_dicts(
            self,
            sql_query: str,
            sql_variables: Optional[Tuple] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch all rows as a list of dictionaries.

        Args:
            sql_query: SELECT query to execute.
            sql_variables: Query parameters as a tuple.

        Returns:
            List of dicts where keys are column names.
            Returns empty list if no rows found.

        Raises:
            PoolError: If pool creation fails.
            QueryExecutionError: If query execution fails.

        Example:
            users = await db.fetch_all_as_dicts(
                "SELECT id, name, email FROM users WHERE active = $1",
                (True,)
            )
            for user in users:
                print(f"{user['name']}: {user['email']}")
        """
        await self._create_pool_connection()

        try:
            async with self.db_connection_pool.acquire() as conn:
                results = await conn.fetch(
                    sql_query,
                    *(sql_variables if sql_variables else ())
                )

            return [dict(r.items()) for r in results]

        except Exception as ex:
            logger.error(f"fetch_all_as_dicts failed: {ex}")
            raise self._convert_exception(ex, sql_query, sql_variables)

    async def fetch_all_as_df(
            self,
            sql_query: str,
            sql_variables: Optional[Tuple] = None
    ) -> pd.DataFrame:
        """
        Fetch all rows as a pandas DataFrame.

        Args:
            sql_query: SELECT query to execute.
            sql_variables: Query parameters as a tuple.

        Returns:
            DataFrame with columns matching the query result.
            Returns empty DataFrame if no rows found.

        Raises:
            PoolError: If pool creation fails.
            QueryExecutionError: If query execution fails.

        Example:
            df = await db.fetch_all_as_df(
                "SELECT date, amount FROM transactions WHERE user_id = $1",
                (user_id,)
            )
            total = df['amount'].sum()
        """
        results = await self.fetch_all_as_dicts(
            sql_query=sql_query,
            sql_variables=sql_variables
        )

        return pd.DataFrame(results) if results else pd.DataFrame()

    async def fetch_one_as_dict(
            self,
            sql_query: str,
            sql_variables: Optional[Tuple] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a single row as a dictionary.

        Args:
            sql_query: SELECT query to execute (should return 0 or 1 row).
            sql_variables: Query parameters as a tuple.

        Returns:
            Dict with column names as keys, or None if no row found.

        Raises:
            PoolError: If pool creation fails.
            QueryExecutionError: If query execution fails.

        Example:
            user = await db.fetch_one_as_dict(
                "SELECT * FROM users WHERE id = $1",
                (user_id,)
            )
            if user:
                print(f"Found: {user['name']}")
        """
        await self._create_pool_connection()

        try:
            async with self.db_connection_pool.acquire() as conn:
                result = await conn.fetchrow(
                    sql_query,
                    *(sql_variables if sql_variables else ())
                )

            return dict(result.items()) if result else None

        except Exception as ex:
            logger.error(f"fetch_one_as_dict failed: {ex}")
            raise self._convert_exception(ex, sql_query, sql_variables)

    async def fetch_value(
            self,
            sql_query: str,
            sql_variables: Optional[Tuple] = None
    ) -> Optional[Any]:
        """
        Fetch a single value from the first column of the first row.

        Args:
            sql_query: SELECT query (should select one column, return 0 or 1 row).
            sql_variables: Query parameters as a tuple.

        Returns:
            The value, or None if no row found.

        Raises:
            PoolError: If pool creation fails.
            QueryExecutionError: If query execution fails.

        Example:
            count = await db.fetch_value(
                "SELECT COUNT(*) FROM users WHERE active = $1",
                (True,)
            )
            print(f"Active users: {count}")
        """
        await self._create_pool_connection()

        try:
            async with self.db_connection_pool.acquire() as conn:
                return await conn.fetchval(
                    sql_query,
                    *(sql_variables if sql_variables else ())
                )

        except Exception as ex:
            logger.error(f"fetch_value failed: {ex}")
            raise self._convert_exception(ex, sql_query, sql_variables)

    # =========================================================================
    # Convenience Insert Methods
    # =========================================================================

    async def insert_into_with_dict(
            self,
            table_name: str,
            parameters_dict: Dict[str, Any],
            on_duplicate_ignore: bool = True
    ) -> InsertResult:
        """
        Insert a row using a dictionary of column: value pairs.

        Args:
            table_name: Name of the table to insert into.
            parameters_dict: Dict mapping column names to values.
            on_duplicate_ignore: If True, silently ignore duplicate key errors.

        Returns:
            InsertResult with insertion details.

        Raises:
            PoolError: If pool creation fails.
            UniqueViolationError: If duplicate and on_duplicate_ignore=False.
            QueryExecutionError: For other errors.

        Example:
            result = await db.insert_into_with_dict(
                "users",
                {"email": "new@example.com", "name": "New User"},
                on_duplicate_ignore=True
            )
            if result.was_inserted:
                print("User created!")
            elif result.was_duplicate:
                print("User already exists")
        """
        # Build query
        columns = '"' + '","'.join(parameters_dict.keys()) + '"'
        placeholders = ", ".join(f"${i + 1}" for i in range(len(parameters_dict)))
        conflict_clause = " ON CONFLICT DO NOTHING" if on_duplicate_ignore else ""

        query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders}){conflict_clause}'
        params = tuple(parameters_dict.values())

        await self._create_pool_connection()

        try:
            async with self.db_connection_pool.acquire() as conn:
                result = await conn.execute(query, *params)

            rows_affected = 0
            try:
                rows_affected = int(result.split()[-1])
            except (ValueError, IndexError):
                pass

            return InsertResult(
                rows_affected=rows_affected,
                status_message=result,
                success=True,
                was_duplicate=(rows_affected == 0 and on_duplicate_ignore)
            )

        except asyncpg.UniqueViolationError as ex:
            if on_duplicate_ignore:
                return InsertResult(
                    rows_affected=0,
                    success=True,
                    was_duplicate=True
                )
            raise self._convert_exception(ex, query, params)

        except Exception as ex:
            logger.error(f"insert_into_with_dict failed: {ex}")
            raise self._convert_exception(ex, query, params)

    async def insert_with_dict_returning(
            self,
            table_name: str,
            parameters_dict: Dict[str, Any],
            on_duplicate_ignore: bool = True
    ) -> InsertResult:
        """
        Insert a row and return the inserted row data.

        Uses RETURNING * to fetch the inserted row, including any
        default values or generated columns.

        Args:
            table_name: Name of the table to insert into.
            parameters_dict: Dict mapping column names to values.
            on_duplicate_ignore: If True, silently ignore duplicate key errors.

        Returns:
            InsertResult with returning_row containing the full inserted row.

        Raises:
            PoolError: If pool creation fails.
            UniqueViolationError: If duplicate and on_duplicate_ignore=False.
            QueryExecutionError: For other errors.

        Example:
            result = await db.insert_with_dict_returning(
                "users",
                {"email": "new@example.com", "name": "New User"}
            )
            if result.returning_row:
                print(f"Created user ID: {result.returning_row['id']}")
                print(f"Created at: {result.returning_row['created_at']}")
        """
        # Build query
        columns = '"' + '","'.join(parameters_dict.keys()) + '"'
        placeholders = ", ".join(f"${i + 1}" for i in range(len(parameters_dict)))
        conflict_clause = " ON CONFLICT DO NOTHING" if on_duplicate_ignore else ""

        query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders}){conflict_clause} RETURNING *'
        params = tuple(parameters_dict.values())

        await self._create_pool_connection()

        try:
            async with self.db_connection_pool.acquire() as conn:
                row = await conn.fetchrow(query, *params)

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
                # No row returned = conflict ignored
                return InsertResult(
                    rows_affected=0,
                    success=True,
                    was_duplicate=True
                )

        except asyncpg.UniqueViolationError as ex:
            if on_duplicate_ignore:
                return InsertResult(
                    rows_affected=0,
                    success=True,
                    was_duplicate=True
                )
            raise self._convert_exception(ex, query, params)

        except Exception as ex:
            logger.error(f"insert_with_dict_returning failed: {ex}")
            raise self._convert_exception(ex, query, params)

    async def insert_into_with_dict_update(
            self,
            table_name: str,
            parameters_dict: Dict[str, Any],
            constraint_key: Optional[str] = None,
            on_duplicate_update: bool = True
    ) -> UpsertResult:
        """
        Insert a row, or update it if it already exists (upsert).

        Uses PostgreSQL's ON CONFLICT ... DO UPDATE syntax.

        Args:
            table_name: Name of the table.
            parameters_dict: Dict mapping column names to values.
            constraint_key: Name of the unique constraint to use for conflict
                           detection. Defaults to "{table_name}_pkey".
            on_duplicate_update: If True, update on conflict. If False,
                                behaves like insert_into_with_dict.

        Returns:
            UpsertResult with information about what happened.

        Raises:
            PoolError: If pool creation fails.
            QueryExecutionError: For query errors.

        Example:
            result = await db.insert_into_with_dict_update(
                "user_settings",
                {"user_id": 123, "theme": "dark", "language": "en"},
                constraint_key="user_settings_user_id_key"
            )
            if result.was_inserted:
                print("New settings created")
            else:
                print("Settings updated")
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

        await self._create_pool_connection()

        try:
            async with self.db_connection_pool.acquire() as conn:
                result = await conn.execute(query, *params)

            rows_affected = 0
            try:
                rows_affected = int(result.split()[-1])
            except (ValueError, IndexError):
                pass

            # Note: We can't easily distinguish insert vs update without RETURNING
            # For a more accurate result, use insert_into_with_dict_update_returning
            return UpsertResult(
                rows_affected=rows_affected,
                status_message=result,
                success=True,
                was_inserted=(rows_affected > 0),  # Approximation
                was_updated=False  # Can't determine without xmax
            )

        except Exception as ex:
            logger.error(f"insert_into_with_dict_update failed: {ex}")
            raise self._convert_exception(ex, query, params)

    async def insert_into_with_dict_update_returning(
            self,
            table_name: str,
            parameters_dict: Dict[str, Any],
            constraint_key: Optional[str] = None
    ) -> UpsertResult:
        """
        Upsert a row and return the result with accurate insert/update detection.

        Uses RETURNING with xmax to determine if the row was inserted or updated.

        Args:
            table_name: Name of the table.
            parameters_dict: Dict mapping column names to values.
            constraint_key: Name of the unique constraint. Defaults to "{table_name}_pkey".

        Returns:
            UpsertResult with accurate was_inserted/was_updated flags and returning_row.

        Example:
            result = await db.insert_into_with_dict_update_returning(
                "user_settings",
                {"user_id": 123, "theme": "dark"}
            )
            if result.was_inserted:
                print(f"Created new settings: {result.returning_row}")
            elif result.was_updated:
                print(f"Updated settings: {result.returning_row}")
        """
        columns = '"' + '","'.join(parameters_dict.keys()) + '"'
        placeholders = ", ".join(f"${i + 1}" for i in range(len(parameters_dict)))

        if constraint_key is None:
            constraint_key = f"{table_name}_pkey"

        set_clause = ", ".join(
            f'"{key}" = EXCLUDED."{key}"' for key in parameters_dict.keys()
        )

        # Include xmax to detect insert vs update
        # xmax = 0 means insert, xmax > 0 means update
        query = (
            f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders})'
            f' ON CONFLICT ON CONSTRAINT {constraint_key}'
            f' DO UPDATE SET {set_clause}'
            f' RETURNING *, xmax'
        )
        params = tuple(parameters_dict.values())

        await self._create_pool_connection()

        try:
            async with self.db_connection_pool.acquire() as conn:
                row = await conn.fetchrow(query, *params)

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
                return UpsertResult(
                    rows_affected=0,
                    success=True
                )

        except Exception as ex:
            logger.error(f"insert_into_with_dict_update_returning failed: {ex}")
            raise self._convert_exception(ex, query, params)

    # =========================================================================
    # Utility Methods
    # =========================================================================

    async def get_postgresql_version(self) -> str:
        """
        Get the PostgreSQL server version.

        Returns:
            Version string like "PostgreSQL 15.2".

        Example:
            version = await db.get_postgresql_version()
            print(version)  # "PostgreSQL 15.2"
        """
        result = await self.fetch_all_as_dicts("SELECT version()")
        version = result[0]["version"].split(",")[0].strip()
        logger.info(f"Connected to: {version}")
        return version

    async def table_exists(self, table_name: str, schema: str = "public") -> bool:
        """
        Check if a table exists.

        Args:
            table_name: Name of the table.
            schema: Schema name (default: "public").

        Returns:
            True if the table exists.

        Example:
            if await db.table_exists("users"):
                print("Table exists")
        """
        result = await self.fetch_value(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = $1 AND table_name = $2
            )
            """,
            (schema, table_name)
        )
        return bool(result)


# =============================================================================
# Main (for testing)
# =============================================================================

if __name__ == "__main__":
    async def main():
        async with PostgresConnectorAsyncPool(application_name="test_pool") as db:
            version = await db.get_postgresql_version()
            print(f"Connected to: {version}")

            results = await db.fetch_all_as_df("SELECT version()")
            print(results)


    asyncio.run(main())