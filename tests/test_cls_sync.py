import pytest
from dotenv import load_dotenv

from postgres_helpers.app_config import logging_config
from postgres_helpers.exceptions import QueryExecutionError, UniqueViolationError
from postgres_helpers.postgres_sync import PostgresConnector

load_dotenv()
logging_config()


def test_connector():
    my_postgres = PostgresConnector()
    sql_string = """
        SELECT version()
    """
    results = my_postgres.fetch_all_as_dicts(sql_query=sql_string, close_connection=True)
    my_postgres.close_connection()
    assert len(results) > 0


def test_insert_many_by_batch():
    # temp table lives on the single connection, so nothing is left behind
    with PostgresConnector() as db:
        db.execute_one_query("CREATE TEMP TABLE batch_test (id INTEGER PRIMARY KEY, name TEXT)")

        rows = [(i, f"name_{i}") for i in range(10)]
        result = db.insert_many_by_batch("INSERT INTO batch_test (id, name) VALUES %s", rows, page_size=3)
        assert result.success
        assert result.total_statements == 10
        assert result.rows_affected == 10
        assert db.fetch_value("SELECT count(*) FROM batch_test") == 10

        # rows skipped by ON CONFLICT DO NOTHING are not counted
        rows = [(i, f"name_{i}") for i in range(8, 13)]
        result = db.insert_many_by_batch(
            "INSERT INTO batch_test (id, name) VALUES %s ON CONFLICT DO NOTHING", rows, page_size=2
        )
        assert result.rows_affected == 3

        # a failing page rolls back the pages already sent
        rows = [(100, "ok"), (101, "ok"), (0, "duplicate")]
        with pytest.raises(UniqueViolationError):
            db.insert_many_by_batch("INSERT INTO batch_test (id, name) VALUES %s", rows, page_size=2)
        assert db.fetch_value("SELECT count(*) FROM batch_test") == 13
        assert db.db_connection.autocommit

        result = db.insert_many_by_batch("INSERT INTO batch_test (id, name) VALUES %s", [])
        assert result.rows_affected == 0


@pytest.mark.parametrize("close_connection", [False, True])
@pytest.mark.parametrize(
    "populate_table, minimum_id, expected_rows",
    [
        pytest.param(False, 0, [], id="empty-table"),
        pytest.param(True, 9, [], id="no-matching-rows"),
        pytest.param(True, 0, [(1, "first")], id="populated-table"),
    ],
)
def test_dataframe_columns_for_empty_and_populated_results(
    populate_table, minimum_id, expected_rows, close_connection
):
    with PostgresConnector() as db:
        db.execute_one_query(
            "CREATE TEMP TABLE dataframe_columns_test (id integer, label text)"
        )
        if populate_table:
            db.execute_one_query("INSERT INTO dataframe_columns_test VALUES (1, 'first')")

        frame = db.fetch_all_as_df(
            "SELECT id AS identifier, label FROM pg_temp.dataframe_columns_test "
            "WHERE id > %s ORDER BY id",
            (minimum_id,),
            close_connection=close_connection,
        )
        assert list(frame.columns) == ["identifier", "label"]
        assert frame.shape == (len(expected_rows), 2)
        assert list(frame.itertuples(index=False, name=None)) == expected_rows
        assert db.is_connected() == (not close_connection)
        assert db.fetch_value("SELECT 42") == 42
    assert not db.is_connected()


@pytest.mark.parametrize("close_connection", [False, True])
def test_dataframe_missing_table_raises_query_error(close_connection):
    with PostgresConnector() as db:
        db.execute_one_query(
            "CREATE TEMP TABLE dataframe_columns_test (id integer, label text)"
        )
        with pytest.raises(QueryExecutionError) as error:
            db.fetch_all_as_df(
                "SELECT * FROM pg_temp.dataframe_columns_missing",
                close_connection=close_connection,
            )
        assert error.value.original_error.pgcode == "42P01"
        assert db.is_connected() == (not close_connection)
        assert db.fetch_value("SELECT 42") == 42
    assert not db.is_connected()


if __name__ == '__main__':
    test_connector()
