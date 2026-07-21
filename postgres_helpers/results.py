"""
Standardized result objects for postgres_helpers library.

These dataclasses provide consistent return types across all connector variants
(sync, async, pool, non-pool).

Usage:
    from postgres_helpers.results import QueryResult, FetchResult

    result = await db.execute_one_query("INSERT INTO ...")
    print(f"Affected {result.rows_affected} rows")
    print(f"Status: {result.status_message}")
"""

from dataclasses import dataclass, field
from typing import Optional, Any, List, Dict


@dataclass
class QueryResult:
    """
    Standardized result for write operations (INSERT, UPDATE, DELETE, etc.).

    This replaces the inconsistent tuple returns across different connectors.

    Attributes:
        rows_affected: Number of rows affected by the query.
                      Returns -1 if the count couldn't be determined.
        status_message: PostgreSQL status message (e.g., "INSERT 0 1", "UPDATE 5").
        last_inserted_id: The ID of the last inserted row (if RETURNING id was used
                         or if return_last_inserted_id=True).
        returning_row: Full row data if RETURNING * or RETURNING columns was used.
        success: True if the query executed without errors.

    Example:
        result = await db.execute_one_query(
            "INSERT INTO users (name) VALUES ($1) RETURNING id",
            ("John",)
        )
        if result.success:
            print(f"Created user with ID: {result.last_inserted_id}")
        print(f"Status: {result.status_message}")  # "INSERT 0 1"
    """
    rows_affected: int = 0
    status_message: str = ""
    last_inserted_id: Optional[Any] = None
    returning_row: Optional[Dict[str, Any]] = None
    success: bool = True

    @property
    def was_inserted(self) -> bool:
        """Check if at least one row was inserted."""
        return self.rows_affected > 0 and "INSERT" in self.status_message.upper()

    @property
    def was_updated(self) -> bool:
        """Check if at least one row was updated."""
        return self.rows_affected > 0 and "UPDATE" in self.status_message.upper()

    @property
    def was_deleted(self) -> bool:
        """Check if at least one row was deleted."""
        return self.rows_affected > 0 and "DELETE" in self.status_message.upper()


@dataclass
class ExecuteManyResult:
    """
    Result for batch operations (executemany).

    Note: PostgreSQL's executemany doesn't return per-statement row counts,
    so rows_affected may not reflect the actual total in all cases.

    Attributes:
        success: True if all statements executed without errors.
        total_statements: Number of statements that were executed.
        rows_affected: Total rows affected (may be -1 if not determinable).

    Example:
        result = await db.execute_many_query(
            "INSERT INTO logs (msg) VALUES ($1)",
            [("msg1",), ("msg2",), ("msg3",)]
        )
        print(f"Executed {result.total_statements} inserts")
    """
    success: bool = True
    total_statements: int = 0
    rows_affected: int = -1  # executemany often can't determine this


@dataclass
class InsertResult:
    """
    Result specifically for insert operations with convenience attributes.

    Attributes:
        rows_affected: Number of rows inserted (0 if conflict ignored).
        success: True if no error occurred (even if 0 rows due to conflict).
        was_duplicate: True if insert was skipped due to duplicate key.
        returning_row: The inserted row data (if RETURNING was used).
        last_inserted_id: The ID of the inserted row.

    Example:
        result = await db.insert_into_with_dict(
            "users",
            {"email": "test@example.com", "name": "Test"},
            on_duplicate_ignore=True
        )
        if result.was_duplicate:
            print("User already exists")
        elif result.success:
            print(f"Created user: {result.returning_row}")
    """
    rows_affected: int = 0
    status_message: str = ""
    success: bool = True
    was_duplicate: bool = False
    returning_row: Optional[Dict[str, Any]] = None
    last_inserted_id: Optional[Any] = None

    @property
    def was_inserted(self) -> bool:
        """Check if the row was actually inserted."""
        return self.rows_affected > 0 and not self.was_duplicate


@dataclass
class UpsertResult:
    """
    Result for upsert (INSERT ... ON CONFLICT UPDATE) operations.

    Attributes:
        rows_affected: Number of rows affected.
        success: True if no error occurred.
        was_inserted: True if a new row was inserted.
        was_updated: True if an existing row was updated.
        returning_row: The inserted/updated row data (if RETURNING was used).

    Example:
        result = await db.insert_into_with_dict_update(
            "users",
            {"email": "test@example.com", "name": "Updated Name"},
            constraint_key="users_email_key"
        )
        if result.was_inserted:
            print("New user created")
        elif result.was_updated:
            print("Existing user updated")
    """
    rows_affected: int = 0
    status_message: str = ""
    success: bool = True
    was_inserted: bool = False
    was_updated: bool = False
    returning_row: Optional[Dict[str, Any]] = None

    # Note: Distinguishing insert vs update in upsert requires RETURNING with xmax
    # xmax = 0 means insert, xmax > 0 means update
    # This is set by the connector if it can determine it


@dataclass
class ConnectionInfo:
    """
    Information about the current database connection.

    Useful for debugging and monitoring.

    Attributes:
        host: Database server host.
        port: Database server port.
        database: Database name.
        user: Connected user.
        server_version: PostgreSQL server version string.
        is_connected: Whether currently connected.
        pool_size: Current pool size (for pool connectors).
        pool_free: Number of free connections in pool.
    """
    host: str = ""
    port: str = ""
    database: str = ""
    user: str = ""
    server_version: str = ""
    is_connected: bool = False
    pool_size: Optional[int] = None
    pool_free: Optional[int] = None