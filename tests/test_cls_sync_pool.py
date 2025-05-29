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


def test_create_insert_delete():
    load_dotenv()
    database_name = 'test_db'
    table_name = 'test_table'

    my_postgres = PostgresConnectorPool()
    sql_string = f"""
        CREATE DATABASE {database_name}
    """

    result = my_postgres.execute_one_query(sql_query=sql_string)
    assert result[2] == 'CREATE DATABASE'
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
    assert result[2] == 'CREATE TABLE'
    print(f'create table query result:', result)

    result = my_postgres.insert_with_dict_returning(table_name=table_name,
                                                    parameters_dict={'name': 'test_name', 'value': 123}
                                                    )

    print(f'test returning insert:', result)
    assert result[1:] == ('test_name', 123)

    my_postgres = PostgresConnectorPool()

    sql_string = f"""
                 DROP DATABASE IF EXISTS {database_name}
            """
    result = my_postgres.execute_one_query(sql_query=sql_string)
    print(f'drop database :', result)


if __name__ == '__main__':
    test_create_insert_delete()
