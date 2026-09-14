import pytest
import logging
from postgres_helpers.app_config import logging_config
from postgres_helpers.exceptions import QueryExecutionError, UniqueViolationError
from postgres_helpers.postgres_async import PostgresConnectorAsync
from pathlib import Path

logging_config(log_level=logging.INFO,
               log_file_name=Path(__file__).name.replace('.py','.log'))


@pytest.mark.asyncio
async def test_async_fetch():
    sql_string = """
        SELECT version()
    """
    db_conn = PostgresConnectorAsync(application_name="test async fetch")
    results = await db_conn.fetch_all_as_dicts(sql_query=sql_string, close_connection=True)

    assert len(results) > 0


@pytest.mark.asyncio
async def test_insert_many_by_batch():
    # temp table lives on the single connection, so nothing is left behind
    async with PostgresConnectorAsync(application_name="test async batch") as db:
        await db.execute_one_query(
            "CREATE TEMP TABLE batch_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)"
        )
        insert_sql = "INSERT INTO batch_test (id, name, value) VALUES %s"

        rows = [(i, f"name_{i}", i * 2) for i in range(10)]
        result = await db.insert_many_by_batch(insert_sql, rows, page_size=3)
        assert result.success
        assert result.total_statements == 10
        assert result.rows_affected == 10
        assert await db.fetch_value("SELECT sum(value) FROM batch_test") == 90

        # rows skipped by ON CONFLICT DO NOTHING are not counted
        rows = [(i, f"name_{i}", 0) for i in range(8, 13)]
        result = await db.insert_many_by_batch(insert_sql + " ON CONFLICT DO NOTHING", rows, page_size=2)
        assert result.rows_affected == 3

        # 3 columns cap a page at 32767 // 3 = 10922 rows, whatever page_size says
        rows = [(i, None, i) for i in range(1000, 13000)]
        result = await db.insert_many_by_batch(insert_sql, rows, page_size=20_000)
        assert result.rows_affected == 12000

        # a failing page rolls back the pages already sent
        rows = [(100, "ok", 1), (101, "ok", 1), (0, "duplicate", 1)]
        with pytest.raises(UniqueViolationError):
            await db.insert_many_by_batch(insert_sql, rows, page_size=2)
        assert await db.fetch_value("SELECT count(*) FROM batch_test") == 12013

        with pytest.raises(QueryExecutionError):
            await db.insert_many_by_batch(
                "INSERT INTO batch_test (id, name, value) VALUES (%s, %s, %s)", [(200, "x", 1)]
            )

        result = await db.insert_many_by_batch(insert_sql, [])
        assert result.rows_affected == 0


