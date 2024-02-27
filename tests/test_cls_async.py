import asyncio

from postgres_helpers.app_config import logging_config
from postgres_helpers.postgres_async import PostgresConnectorAsync

logging_config()


def test_async_fetch():
    sql_string = """
        SELECT version()
    """
    my_postgres = PostgresConnectorAsync()
    results = asyncio.get_event_loop().run_until_complete(
        my_postgres.fetch_all_as_dicts(sql_query=sql_string, close_connection=True))

    assert len(results) > 0


if __name__ == '__main__':
    test_async_fetch()
