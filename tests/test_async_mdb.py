from postgres_helpers.async_postgres_class import AsyncPostGresConnector
import asyncio
import pytest


@pytest.mark.asyncio
async def test_async_fetch():
    sql_string = """
        SELECT yahoo_ticker FROM d_security_ticker LIMIT 10
    """
    my_postgres = AsyncPostGresConnector()

    results = await my_postgres.fetch_all_as_dicts(sql_query=sql_string, close_connection=True)
    assert len(results)==10


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(test_async_fetch())


