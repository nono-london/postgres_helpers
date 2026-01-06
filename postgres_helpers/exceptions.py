"""
Custom exceptions for postgres_helpers library.

Usage:
    from postgres_helpers.exceptions import (
        PostgresHelperError,
        ConnectionError,
        PoolError,
        QueryExecutionError,
        UniqueViolationError,
        QueryTimeoutError
    )

All exceptions inherit from PostgresHelperError for easy catching:
    try:
        await db.execute_one_query(...)
    except PostgresHelperError as e:
        # Handle any postgres_helpers error
        pass
"""

from typing import Optional, Any


class PostgresHelperError(Exception):
    """
    Base exception for all postgres_helpers errors.

    Catch this to handle any error from the library.
    """
    pass


class ConnectionError(PostgresHelperError):
    """
    Failed to establish a database connection.

    Attributes:
        host: The database host that was attempted
        port: The database port that was attempted
        database: The database name that was attempted
        original_error: The underlying exception from the driver
    """

    def __init__(
            self,
            message: str,
            host: Optional[str] = None,
            port: Optional[str] = None,
            database: Optional[str] = None,
            original_error: Optional[Exception] = None
    ):
        self.host = host
        self.port = port
        self.database = database
        self.original_error = original_error
        super().__init__(message)

    def __str__(self) -> str:
        base = super().__str__()
        if self.host:
            base += f" (host={self.host}, port={self.port}, database={self.database})"
        return base


class PoolError(PostgresHelperError):
    """
    Connection pool related error.

    This includes errors during pool creation, acquisition timeout,
    or pool exhaustion.

    Attributes:
        pool_size_min: Minimum pool size configured
        pool_size_max: Maximum pool size configured
        original_error: The underlying exception from the driver
    """

    def __init__(
            self,
            message: str,
            pool_size_min: Optional[int] = None,
            pool_size_max: Optional[int] = None,
            original_error: Optional[Exception] = None
    ):
        self.pool_size_min = pool_size_min
        self.pool_size_max = pool_size_max
        self.original_error = original_error
        super().__init__(message)


class QueryExecutionError(PostgresHelperError):
    """
    Error executing a SQL query.

    This is the base class for query-related errors. Use this to catch
    any query execution problem.

    Attributes:
        query: The SQL query that failed (may be truncated for security)
        params: The parameters passed to the query
        original_error: The underlying exception from the driver
    """

    def __init__(
            self,
            message: str,
            query: Optional[str] = None,
            params: Optional[tuple] = None,
            original_error: Optional[Exception] = None
    ):
        self.query = query
        self.params = params
        self.original_error = original_error
        super().__init__(message)

    def __str__(self) -> str:
        base = super().__str__()
        if self.original_error:
            base += f" | Original error: {self.original_error.__class__.__name__}: {self.original_error}"
        return base


class UniqueViolationError(QueryExecutionError):
    """
    Duplicate key / unique constraint violation.

    Raised when an INSERT or UPDATE would violate a UNIQUE constraint.

    Attributes:
        constraint_name: Name of the violated constraint (if available)
        detail: PostgreSQL detail message (if available)
    """

    def __init__(
            self,
            message: str,
            query: Optional[str] = None,
            params: Optional[tuple] = None,
            original_error: Optional[Exception] = None,
            constraint_name: Optional[str] = None,
            detail: Optional[str] = None
    ):
        super().__init__(message, query, params, original_error)
        self.constraint_name = constraint_name
        self.detail = detail


class ForeignKeyViolationError(QueryExecutionError):
    """
    Foreign key constraint violation.

    Raised when an INSERT, UPDATE, or DELETE would violate a FOREIGN KEY constraint.

    Attributes:
        constraint_name: Name of the violated constraint (if available)
        detail: PostgreSQL detail message (if available)
    """

    def __init__(
            self,
            message: str,
            query: Optional[str] = None,
            params: Optional[tuple] = None,
            original_error: Optional[Exception] = None,
            constraint_name: Optional[str] = None,
            detail: Optional[str] = None
    ):
        super().__init__(message, query, params, original_error)
        self.constraint_name = constraint_name
        self.detail = detail


class CheckViolationError(QueryExecutionError):
    """
    CHECK constraint violation.

    Raised when an INSERT or UPDATE would violate a CHECK constraint.

    Attributes:
        constraint_name: Name of the violated constraint (if available)
    """

    def __init__(
            self,
            message: str,
            query: Optional[str] = None,
            params: Optional[tuple] = None,
            original_error: Optional[Exception] = None,
            constraint_name: Optional[str] = None
    ):
        super().__init__(message, query, params, original_error)
        self.constraint_name = constraint_name


class QueryTimeoutError(QueryExecutionError):
    """
    Query execution timeout.

    Raised when a query takes longer than the configured timeout.

    Attributes:
        timeout_seconds: The timeout value that was exceeded
    """

    def __init__(
            self,
            message: str,
            query: Optional[str] = None,
            params: Optional[tuple] = None,
            original_error: Optional[Exception] = None,
            timeout_seconds: Optional[float] = None
    ):
        super().__init__(message, query, params, original_error)
        self.timeout_seconds = timeout_seconds


class TransactionError(PostgresHelperError):
    """
    Transaction-related error.

    Raised when there's an issue with transaction management
    (begin, commit, rollback).

    Attributes:
        original_error: The underlying exception from the driver
    """

    def __init__(
            self,
            message: str,
            original_error: Optional[Exception] = None
    ):
        self.original_error = original_error
        super().__init__(message)