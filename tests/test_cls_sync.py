from postgres_helpers.postgres_sync import PostgresConnector


def test_connector():

    my_postgres = PostgresConnector()
    sql_string = """
        SELECT version()
    """
    results = my_postgres.fetch_all_as_dicts(sql_query=sql_string, close_connection_after=True)
    my_postgres.close_connection()
    assert len(results) > 0


if __name__ == '__main__':
    test_connector()
