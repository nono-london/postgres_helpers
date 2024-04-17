import pytest
import logging
from postgres_helpers.app_config import logging_config
from postgres_helpers.postgres_async import PostgresConnectorAsync
from pathlib import Path

logging_config(log_level=logging.INFO,
               log_file_name=Path(__file__).name.replace('.py','.log'),
               force_local_folder=True)


@pytest.mark.asyncio
async def test_async_fetch():
    sql_string = """
        SELECT version()
    """
    db_conn = PostgresConnectorAsync(application_name="test async fetch")
    results = await db_conn.fetch_all_as_dicts(sql_query=sql_string, close_connection=True)

    assert len(results) > 0


