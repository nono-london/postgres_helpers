import asyncio

import pytest

from postgres_helpers.async_postgres_class import AsyncPostGresConnector


@pytest.mark.asyncio
async def test_async_fetch():
    sql_string = """
        SELECT version()
    """
    my_postgres = AsyncPostGresConnector()

    results = await my_postgres.fetch_all_as_dicts(sql_query=sql_string, close_connection=True)
    assert len(results) > 0


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(test_async_fetch())
