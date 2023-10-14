import asyncio

from postgres_helpers.postgres_async_pool import PostgresConnectorAsyncPool


def test_pool_async_dicts():
    sql_string = """
        SELECT version()
    """
    my_postgres = PostgresConnectorAsyncPool()
    results = asyncio.get_event_loop().run_until_complete(
        my_postgres.fetch_all_as_dicts(sql_query=sql_string))

    assert len(results) > 0


def test_pool_async_pd():
    sql_string = """
            SELECT version()
        """
    my_postgres = PostgresConnectorAsyncPool()
    results = asyncio.get_event_loop().run_until_complete(
        my_postgres.fetch_all_as_df(sql_query=sql_string))

    assert len(results) > 0


if __name__ == '__main__':
    test_pool_async_dicts()
    test_pool_async_pd()
