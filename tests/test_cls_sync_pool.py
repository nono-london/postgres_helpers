import pytest
from dotenv import load_dotenv

from postgres_helpers.exceptions import UniqueViolationError
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


def test_create_insert_delete():
    load_dotenv()
    database_name = 'test_db'
    table_name = 'test_table'

    my_postgres = PostgresConnectorPool()
    sql_string = f"""
        CREATE DATABASE {database_name}
    """

    result = my_postgres.execute_one_query(sql_query=sql_string)
    assert result.status_message == 'CREATE DATABASE'
    print(f'create database query result:', result)
    my_postgres.db_connection_pool.closeall()

    my_postgres = PostgresConnectorPool(db_name=database_name)

    sql_string = f"""
             CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            value INTEGER)
        """
    result = my_postgres.execute_one_query(sql_query=sql_string)
    assert result.status_message == 'CREATE TABLE'
    print(f'create table query result:', result)

    result = my_postgres.insert_with_dict_returning(table_name=table_name,
                                                    parameters_dict={'name': 'test_name', 'value': 123}
                                                    )

    print(f'test returning insert:', result)
    assert result.returning_row['name'] == 'test_name'
    assert result.returning_row['value'] == 123

    my_postgres = PostgresConnectorPool()

    sql_string = f"""
                 DROP DATABASE IF EXISTS {database_name}
            """
    result = my_postgres.execute_one_query(sql_query=sql_string)
    print(f'drop database :', result)


def test_insert_many_by_batch():
    load_dotenv()
    table_name = 'test_insert_many_by_batch'

    my_postgres = PostgresConnectorPool()
    my_postgres.execute_one_query(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name TEXT)")
    try:
        rows = [(i, f"name_{i}") for i in range(10)]
        result = my_postgres.insert_many_by_batch(
            f"INSERT INTO {table_name} (id, name) VALUES %s", rows, page_size=3
        )
        assert result.rows_affected == 10
        assert my_postgres.fetch_value(f"SELECT count(*) FROM {table_name}") == 10

        # a failing page rolls back the pages already sent
        rows = [(100, "ok"), (101, "ok"), (0, "duplicate")]
        with pytest.raises(UniqueViolationError):
            my_postgres.insert_many_by_batch(f"INSERT INTO {table_name} (id, name) VALUES %s", rows, page_size=2)
        assert my_postgres.fetch_value(f"SELECT count(*) FROM {table_name}") == 10
    finally:
        my_postgres.execute_one_query(f"DROP TABLE IF EXISTS {table_name}")
        my_postgres.close_pool()


if __name__ == '__main__':
    test_create_insert_delete()
