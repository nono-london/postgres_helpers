import pytest
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

from postgres_helpers.exceptions import QueryExecutionError, UniqueViolationError
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


@pytest.mark.parametrize(
    "populate_table, minimum_id, expected_rows",
    [
        pytest.param(False, 0, [], id="empty-table"),
        pytest.param(True, 9, [], id="no-matching-rows"),
        pytest.param(True, 0, [(1, "first")], id="populated-table"),
    ],
)
def test_dataframe_columns_for_empty_and_populated_results(
    populate_table, minimum_id, expected_rows
):
    with PostgresConnectorPool(pool_size_min=1, pool_size_max=1) as db:
        db.execute_one_query(
            "CREATE TEMP TABLE dataframe_columns_test (id integer, label text)"
        )
        if populate_table:
            db.execute_one_query("INSERT INTO dataframe_columns_test VALUES (1, 'first')")

        frame = db.fetch_all_as_df(
            "SELECT id AS identifier, label FROM pg_temp.dataframe_columns_test "
            "WHERE id > %s ORDER BY id",
            (minimum_id,),
        )
        assert list(frame.columns) == ["identifier", "label"]
        assert frame.shape == (len(expected_rows), 2)
        assert list(frame.itertuples(index=False, name=None)) == expected_rows
        assert db.is_pool_active()
        assert db.fetch_value("SELECT 42") == 42
    assert not db.is_pool_active()


def test_dataframe_missing_table_raises_query_error():
    with PostgresConnectorPool(pool_size_min=1, pool_size_max=1) as db:
        db.execute_one_query(
            "CREATE TEMP TABLE dataframe_columns_test (id integer, label text)"
        )
        with pytest.raises(QueryExecutionError) as error:
            db.fetch_all_as_df(
                "SELECT * FROM pg_temp.dataframe_columns_missing",
            )
        assert error.value.original_error.pgcode == "42P01"
        assert db.is_pool_active()
        assert db.fetch_value("SELECT 42") == 42
    assert not db.is_pool_active()



def test_upsert_returning_insert_and_update_flags():
    with PostgresConnectorPool(pool_size_min=1, pool_size_max=1) as db:
        db.execute_one_query(
            "CREATE TEMP TABLE upsert_flags_test (id integer PRIMARY KEY, name text)"
        )
        inserted = db.insert_into_with_dict_update_returning(
            "upsert_flags_test", {"id": 1, "name": "first"}
        )
        assert inserted.success and inserted.rows_affected == 1
        assert inserted.returning_row == {"id": 1, "name": "first"}
        assert inserted.was_inserted is True
        assert inserted.was_updated is False

        updated = db.insert_into_with_dict_update_returning(
            "upsert_flags_test", {"id": 1, "name": "updated"}
        )
        assert updated.success and updated.rows_affected == 1
        assert updated.returning_row == {"id": 1, "name": "updated"}
        assert updated.was_inserted is False
        assert updated.was_updated is True
        assert db.fetch_value("SELECT count(*) FROM pg_temp.upsert_flags_test") == 1
        assert db.fetch_value("SELECT name FROM pg_temp.upsert_flags_test") == "updated"


@pytest.mark.parametrize(
    "invalid_xmax",
    [
        pytest.param(None, id="missing-value"),
        pytest.param("invalid", id="invalid-text"),
        pytest.param(float("inf"), id="overflow"),
    ],
)
def test_upsert_returning_invalid_xmax_preserves_row(monkeypatch, caplog, invalid_xmax):
    class InvalidXmaxCursor(RealDictCursor):
        def fetchone(self):
            row = super().fetchone()
            row["xmax"] = invalid_xmax
            return row

    monkeypatch.setattr("postgres_helpers.postgres_sync_pool.RealDictCursor", InvalidXmaxCursor)
    with PostgresConnectorPool(pool_size_min=1, pool_size_max=1) as db:
        db.execute_one_query(
            "CREATE TEMP TABLE upsert_flags_test (id integer PRIMARY KEY, name text)"
        )
        result = db.insert_into_with_dict_update_returning(
            "upsert_flags_test", {"id": 1, "name": "first"}
        )
        assert result.success and result.rows_affected == 1
        assert result.returning_row == {"id": 1, "name": "first"}
        assert result.was_inserted is False
        assert result.was_updated is False
        assert "invalid xmax" in caplog.text
        assert db.fetch_value("SELECT name FROM pg_temp.upsert_flags_test") == "first"


if __name__ == '__main__':
    test_create_insert_delete()
