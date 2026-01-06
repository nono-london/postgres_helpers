"""
Tests for the refactored PostgresConnectorAsyncPool.

These tests demonstrate the new features:
- Context manager support
- Transaction context manager
- Consistent return types (QueryResult, InsertResult, etc.)
- Exception handling
"""

import pytest
from postgres_helpers.postgres_async_pool import PostgresConnectorAsyncPool
from postgres_helpers.results import (
    QueryResult,
    InsertResult,
    UpsertResult,
    ExecuteManyResult
)
from postgres_helpers.exceptions import (
    UniqueViolationError,
    QueryExecutionError,
    PostgresHelperError
)


# =============================================================================
# Basic Connection Tests
# =============================================================================

@pytest.mark.asyncio
async def test_context_manager():
    """Test that context manager properly opens and closes pool."""
    async with PostgresConnectorAsyncPool() as db:
        assert db.is_pool_active()
        result = await db.fetch_all_as_dicts("SELECT 1 as value")
        assert result == [{"value": 1}]

    # Pool should be closed after exiting context
    assert not db.is_pool_active()


@pytest.mark.asyncio
async def test_fetch_all_as_dicts():
    """Test fetching results as list of dicts."""
    async with PostgresConnectorAsyncPool() as db:
        results = await db.fetch_all_as_dicts("SELECT version()")
        assert len(results) == 1
        assert "version" in results[0]


@pytest.mark.asyncio
async def test_fetch_all_as_df():
    """Test fetching results as DataFrame."""
    async with PostgresConnectorAsyncPool() as db:
        df = await db.fetch_all_as_df("SELECT 1 as a, 2 as b")
        assert len(df) == 1
        assert list(df.columns) == ["a", "b"]


@pytest.mark.asyncio
async def test_fetch_one_as_dict():
    """Test fetching single row."""
    async with PostgresConnectorAsyncPool() as db:
        result = await db.fetch_one_as_dict("SELECT 42 as answer")
        assert result == {"answer": 42}

        # Test no rows
        result = await db.fetch_one_as_dict("SELECT 1 WHERE false")
        assert result is None


@pytest.mark.asyncio
async def test_fetch_value():
    """Test fetching single value."""
    async with PostgresConnectorAsyncPool() as db:
        value = await db.fetch_value("SELECT 42")
        assert value == 42

        value = await db.fetch_value("SELECT 1 WHERE false")
        assert value is None


@pytest.mark.asyncio
async def test_get_postgresql_version():
    """Test version retrieval."""
    async with PostgresConnectorAsyncPool() as db:
        version = await db.get_postgresql_version()
        assert "PostgreSQL" in version


@pytest.mark.asyncio
async def test_pool_status():
    """Test pool status retrieval."""
    async with PostgresConnectorAsyncPool(pool_size_min=2, pool_size_max=5) as db:
        status = await db.get_pool_status()
        assert status.is_connected
        assert status.pool_size is not None


# =============================================================================
# Query Execution Tests
# =============================================================================

@pytest.mark.asyncio
async def test_execute_one_query_returns_query_result():
    """Test that execute_one_query returns QueryResult."""
    async with PostgresConnectorAsyncPool() as db:
        # Create temp table
        await db.execute_one_query("""
            CREATE TEMP TABLE test_exec (id SERIAL PRIMARY KEY, name TEXT)
        """)

        # Insert and check result type
        result = await db.execute_one_query(
            "INSERT INTO test_exec (name) VALUES ($1)",
            ("test",)
        )

        assert isinstance(result, QueryResult)
        assert result.success
        assert result.rows_affected == 1
        assert "INSERT" in result.status_message


@pytest.mark.asyncio
async def test_execute_many_query():
    """Test batch execution."""
    async with PostgresConnectorAsyncPool() as db:
        await db.execute_one_query("""
            CREATE TEMP TABLE test_many (id SERIAL PRIMARY KEY, value INT)
        """)

        result = await db.execute_many_query(
            "INSERT INTO test_many (value) VALUES ($1)",
            [(1,), (2,), (3,), (4,), (5,)]
        )

        assert isinstance(result, ExecuteManyResult)
        assert result.success
        assert result.total_statements == 5

        # Verify data
        count = await db.fetch_value("SELECT COUNT(*) FROM test_many")
        assert count == 5


# =============================================================================
# Insert Methods Tests
# =============================================================================

@pytest.mark.asyncio
async def test_insert_into_with_dict():
    """Test dictionary-based insert."""
    async with PostgresConnectorAsyncPool() as db:
        await db.execute_one_query("""
            CREATE TEMP TABLE test_dict_insert (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE,
                name TEXT
            )
        """)

        # Successful insert
        result = await db.insert_into_with_dict(
            "test_dict_insert",
            {"email": "test@example.com", "name": "Test User"}
        )

        assert isinstance(result, InsertResult)
        assert result.success
        assert result.rows_affected == 1
        assert result.was_inserted
        assert not result.was_duplicate


@pytest.mark.asyncio
async def test_insert_into_with_dict_duplicate_ignored():
    """Test that duplicates are properly ignored."""
    async with PostgresConnectorAsyncPool() as db:
        await db.execute_one_query("""
            CREATE TEMP TABLE test_dup (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE
            )
        """)

        # First insert
        await db.insert_into_with_dict("test_dup", {"email": "test@example.com"})

        # Duplicate insert with ignore
        result = await db.insert_into_with_dict(
            "test_dup",
            {"email": "test@example.com"},
            on_duplicate_ignore=True
        )

        assert result.success
        assert result.rows_affected == 0
        assert result.was_duplicate
        assert not result.was_inserted


@pytest.mark.asyncio
async def test_insert_into_with_dict_duplicate_raises():
    """Test that duplicates raise exception when not ignored."""
    async with PostgresConnectorAsyncPool() as db:
        await db.execute_one_query("""
            CREATE TEMP TABLE test_dup_raise (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE
            )
        """)

        await db.insert_into_with_dict("test_dup_raise", {"email": "test@example.com"})

        with pytest.raises(UniqueViolationError):
            await db.insert_into_with_dict(
                "test_dup_raise",
                {"email": "test@example.com"},
                on_duplicate_ignore=False
            )


@pytest.mark.asyncio
async def test_insert_with_dict_returning():
    """Test insert with RETURNING."""
    async with PostgresConnectorAsyncPool() as db:
        await db.execute_one_query("""
            CREATE TEMP TABLE test_returning (
                id SERIAL PRIMARY KEY,
                name TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        result = await db.insert_with_dict_returning(
            "test_returning",
            {"name": "Test"}
        )

        assert result.success
        assert result.returning_row is not None
        assert "id" in result.returning_row
        assert "created_at" in result.returning_row
        assert result.returning_row["name"] == "Test"
        assert result.last_inserted_id == result.returning_row["id"]


@pytest.mark.asyncio
async def test_insert_into_with_dict_update():
    """Test upsert functionality."""
    async with PostgresConnectorAsyncPool() as db:
        await db.execute_one_query("""
            CREATE TEMP TABLE test_upsert (
                id INT PRIMARY KEY,
                value TEXT
            )
        """)

        # Insert
        result = await db.insert_into_with_dict_update(
            "test_upsert",
            {"id": 1, "value": "original"},
            constraint_key="test_upsert_pkey"
        )

        assert isinstance(result, UpsertResult)
        assert result.success
        assert result.rows_affected == 1

        # Update (same id)
        result = await db.insert_into_with_dict_update(
            "test_upsert",
            {"id": 1, "value": "updated"},
            constraint_key="test_upsert_pkey"
        )

        assert result.success

        # Verify
        row = await db.fetch_one_as_dict("SELECT * FROM test_upsert WHERE id = 1")
        assert row["value"] == "updated"


@pytest.mark.asyncio
async def test_insert_into_with_dict_update_returning():
    """Test upsert with accurate insert/update detection."""
    async with PostgresConnectorAsyncPool() as db:
        await db.execute_one_query("""
            CREATE TEMP TABLE test_upsert_ret (
                id INT PRIMARY KEY,
                value TEXT
            )
        """)

        # Insert (should be was_inserted=True)
        result = await db.insert_into_with_dict_update_returning(
            "test_upsert_ret",
            {"id": 1, "value": "original"},
            constraint_key="test_upsert_ret_pkey"
        )

        assert result.was_inserted
        assert not result.was_updated
        assert result.returning_row["value"] == "original"

        # Update (should be was_updated=True)
        result = await db.insert_into_with_dict_update_returning(
            "test_upsert_ret",
            {"id": 1, "value": "updated"},
            constraint_key="test_upsert_ret_pkey"
        )

        assert not result.was_inserted
        assert result.was_updated
        assert result.returning_row["value"] == "updated"


# =============================================================================
# Transaction Tests
# =============================================================================

@pytest.mark.asyncio
async def test_transaction_commit():
    """Test that successful transaction commits."""
    async with PostgresConnectorAsyncPool() as db:
        await db.execute_one_query("""
            CREATE TEMP TABLE test_tx (id SERIAL PRIMARY KEY, value INT)
        """)

        async with db.transaction() as conn:
            await conn.execute("INSERT INTO test_tx (value) VALUES (1)")
            await conn.execute("INSERT INTO test_tx (value) VALUES (2)")

        # Should be committed
        count = await db.fetch_value("SELECT COUNT(*) FROM test_tx")
        assert count == 2


@pytest.mark.asyncio
async def test_transaction_rollback_on_error():
    """Test that failed transaction rolls back."""
    async with PostgresConnectorAsyncPool() as db:
        await db.execute_one_query("""
            CREATE TEMP TABLE test_tx_rb (id SERIAL PRIMARY KEY, value INT UNIQUE)
        """)

        try:
            async with db.transaction() as conn:
                await conn.execute("INSERT INTO test_tx_rb (value) VALUES (1)")
                await conn.execute("INSERT INTO test_tx_rb (value) VALUES (1)")  # Duplicate!
        except Exception:
            pass

        # Should be rolled back - no rows
        count = await db.fetch_value("SELECT COUNT(*) FROM test_tx_rb")
        assert count == 0


@pytest.mark.asyncio
async def test_transaction_manual_rollback():
    """Test manual rollback via exception."""
    async with PostgresConnectorAsyncPool() as db:
        await db.execute_one_query("""
            CREATE TEMP TABLE test_manual_rb (id SERIAL PRIMARY KEY, value INT)
        """)

        class MyRollbackException(Exception):
            pass

        try:
            async with db.transaction() as conn:
                await conn.execute("INSERT INTO test_manual_rb (value) VALUES (100)")
                raise MyRollbackException("Oops, rollback!")
        except MyRollbackException:
            pass

        # Should be rolled back
        count = await db.fetch_value("SELECT COUNT(*) FROM test_manual_rb")
        assert count == 0


# =============================================================================
# Exception Tests
# =============================================================================

@pytest.mark.asyncio
async def test_exception_hierarchy():
    """Test that all exceptions inherit from PostgresHelperError."""
    async with PostgresConnectorAsyncPool() as db:
        await db.execute_one_query("""
            CREATE TEMP TABLE test_ex (id INT PRIMARY KEY)
        """)
        await db.insert_into_with_dict("test_ex", {"id": 1})

        # Can catch specific exception
        try:
            await db.insert_into_with_dict("test_ex", {"id": 1}, on_duplicate_ignore=False)
        except UniqueViolationError as e:
            assert e.original_error is not None

        # Can also catch base exception
        try:
            await db.insert_into_with_dict("test_ex", {"id": 1}, on_duplicate_ignore=False)
        except PostgresHelperError:
            pass  # Caught!


@pytest.mark.asyncio
async def test_query_execution_error():
    """Test that syntax errors raise QueryExecutionError."""
    async with PostgresConnectorAsyncPool() as db:
        with pytest.raises(QueryExecutionError):
            await db.execute_one_query("SELEKT * FORM users")  # Intentional typo


# =============================================================================
# Utility Tests
# =============================================================================

@pytest.mark.asyncio
async def test_table_exists():
    """Test table existence check."""
    async with PostgresConnectorAsyncPool() as db:
        # Create temp table
        await db.execute_one_query("CREATE TEMP TABLE test_exists (id INT)")

        # Note: TEMP tables are in pg_temp schema
        exists = await db.fetch_value("""
            SELECT EXISTS (
                SELECT FROM pg_tables WHERE tablename = 'test_exists'
            )
        """)
        assert exists


if __name__ == "__main__":
    pytest.main([__file__, "-v"])