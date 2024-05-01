from postgres_helpers.app_config import logging_config
from postgres_helpers.postgres_async_pool import PostgresConnectorAsyncPool
import pytest
logging_config()

@pytest.mark.asyncio
async def test_pool_async_dicts():
    sql_string = """
        SELECT version()
    """
    my_postgres = PostgresConnectorAsyncPool()
    results = await my_postgres.fetch_all_as_dicts(sql_query=sql_string)

    assert len(results) > 0

@pytest.mark.asyncio
async def test_pool_async_pd():
    sql_string = """
            SELECT version()
        """
    my_postgres = PostgresConnectorAsyncPool()
    results = await my_postgres.fetch_all_as_df(sql_query=sql_string)

    assert len(results) > 0

@pytest.mark.asyncio
async def test_pool_size_min():
    sql_string = """
                SELECT version()
            """
    my_postgres = PostgresConnectorAsyncPool(pool_size_max=3, pool_size_min=4)
    results = await my_postgres.fetch_all_as_df(sql_query=sql_string)

    assert len(results) > 0

if __name__ == '__main__':
    test_pool_async_dicts()
    test_pool_async_pd()
