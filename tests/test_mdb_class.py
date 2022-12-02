""" postgresql_mdb_libs tests"""
from postgres_helpers.sync_postgres_class import PostGresConnector


def test_connector():
    my_postgres = PostGresConnector()
    my_postgres.open_connection()
    sql_string = """
        SELECT yahoo_ticker FROM d_security_ticker LIMIT 5
    """
    results = my_postgres.fetch_all_as_dict_list(sql_query=sql_string, close_connection_after=True)
    my_postgres.close_connection()
    assert len(results) > 0


if __name__ == '__main__':
    test_connector()
