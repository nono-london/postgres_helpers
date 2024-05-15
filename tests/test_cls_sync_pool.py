from dotenv import load_dotenv

from postgres_helpers.postgres_sync_pool import PostgresConnectorPool


def test_fetch_as_dict():
    load_dotenv()
    my_postgres = PostgresConnectorPool()
    sql_string = """
        SELECT version()
    """
    my_results = my_postgres.fetch_all_as_dicts(
        sql_query=sql_string,
    )

    assert len(my_results) > 0


def test_fetch_as_df():
    load_dotenv()
    my_postgres = PostgresConnectorPool()
    sql_string = """
        SELECT version()
    """
    result_df = my_postgres.fetch_all_as_df(
        sql_query=sql_string,
    )

    assert len(result_df) > 0


if __name__ == '__main__':
    pass
