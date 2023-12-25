import logging
from pathlib import Path

logger = logging.getLogger(f"postgres_helpers:{Path(__file__).name}")
from os import environ
from typing import Union, Optional, List, Tuple

# https://zetcode.com/python/psycopg2/
# https://pynative.com/python-postgresql-tutorial/
import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2 import Error
from psycopg2.errors import UniqueViolation
from psycopg2.extensions import connection

from postgres_helpers.app_config import load_postgres_details_to_env


class PostgresConnector:
    """Class to handle Postgresql"""

    def __init__(
        self,
        db_host: Optional[str] = None,
        db_port: Optional[str] = None,
        db_user: Optional[str] = None,
        db_password: Optional[str] = None,
        db_name: Optional[str] = None,
        connect_timeout: int = 6,
    ):
        if None in [
            db_host,
            db_port,
            db_name,
            db_user,
            db_password,
        ]:
            load_postgres_details_to_env()

        self.db_host = environ["POSTGRES_DB_HOST"] if db_host is None else db_host
        self.db_port = environ["POSTGRES_DB_PORT"] if db_port is None else str(db_port)
        self.db_user: str = environ["POSTGRES_DB_USER"] if db_user is None else db_user
        self.db_password: str = (
            environ["POSTGRES_DB_PASS"] if db_password is None else db_password
        )
        self.db_name: str = environ["POSTGRES_DB_NAME"] if db_name is None else db_name

        self.db_connection: Union[connection, None] = None
        self.db_version: Union[str, None] = None
        self.connect_timeout: int = connect_timeout

    def open_connection(self) -> bool:
        # check if there is an existing connection which is already opened
        if self.db_connection is not None and not self.db_connection.closed:
            return True

        try:
            self.db_connection = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
                connect_timeout=self.connect_timeout,
            )
            # allowing autocommit so that when sql error, no need to close conn or rollback
            self.db_connection.autocommit = True
            return True

        except (Exception, Error) as ex:
            logger.error(f"Error connecting with database: {ex}")
            return False

    def close_connection(self) -> bool:
        """Returns True is successfully closed connection"""
        if self.db_connection is None:
            return True

        if not self.db_connection.closed:
            self.db_connection.close()
            self.db_connection = None
            return True

        return False

    def get_postgresql_version(self) -> str:
        result = self.fetch_all_as_dicts(
            "SELECT version()",
        )
        self.db_version = result[0]["version"].split(",")[0].strip()
        logger.info(f"Connected to Postgres version: {self.db_version}")
        return self.db_version

    def execute_one_query(
        self,
        sql_query: str,
        sql_variables: tuple = None,
        return_last_inserted_id: bool = False,
        close_connection: bool = True,
    ) -> tuple:
        """return a tuple of (last_inserted_row_id, rows_affected, status_message)
        rows_affected return -1 if SQL error: duplicate key etc., 0 if nothing changed, # of records affected
        """

        self.open_connection()

        db_cursor = self.db_connection.cursor()
        try:
            db_cursor.execute(sql_query, sql_variables)
            # needs to commit for data to be stored
            # self.db_connection.commit()

        except UniqueViolation as ex:
            logger.warning(f"Error with UNIQUE KEY: {ex}")
            # needs to rollback or close connection when unhandled SQL error
            # https://stackoverflow.com/questions/2979369/databaseerror-current-transaction-is-aborted-commands-ignored-until-end-of-tra
            # self.db_connection.rollback()
        except Exception as ex:
            logger.error(
                f"Error while inserting data {sql_variables}\n"
                f"With SQL query: {sql_query}\n"
                f"Error is: {ex}\n"
                f"Error class name is: {ex.__class__.__name__}"
            )

        rows_affected = db_cursor.rowcount
        status_message = db_cursor.statusmessage

        last_row_id = -1
        if return_last_inserted_id:
            try:
                last_row_id = db_cursor.fetchone()[0]
            except (Exception, Error) as ex:
                logger.error(f"Error while recovering last inserted id: {ex}")
                last_row_id = -1

        db_cursor.close()

        if close_connection:
            self.close_connection()

        return last_row_id, rows_affected, status_message

    def execute_many_query(self, sql_query: str, tuples_list: list) -> tuple:
        """Return a tuple of (rows_affected, status_message)
        tuples_list is a list of parameters as tuples
        the query will fail if there is an SQL error, not using try/except

        """
        self.open_connection()
        db_cursor = self.db_connection.cursor()

        db_cursor.executemany(sql_query, tuples_list)

        # self.db_connection.commit()
        # commit shouldn't be needed anymore using autocommit
        # https://stackoverflow.com/questions/9222256/how-do-i-know-if-i-have-successfully-created-a-table-python-psycopg2
        rows_affected = db_cursor.rowcount
        status_message = db_cursor.statusmessage
        db_cursor.close()
        self.close_connection()

        return rows_affected, status_message

    def fetch_all_as_dicts(
        self, sql_query: str, sql_variables: tuple = None, close_connection: bool = True
    ) -> List[Tuple]:
        self.open_connection()

        db_cursor = self.db_connection.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )

        db_cursor.execute(sql_query, sql_variables)
        rows_found = db_cursor.fetchall()
        db_cursor.close()

        if close_connection:
            self.close_connection()

        return rows_found

    def fetch_all_as_df(
        self, sql_query: str, sql_variables: tuple = None, close_connection: bool = True
    ) -> Union[None, pd.DataFrame]:
        self.open_connection()
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql.html
        results: list = self.fetch_all_as_dicts(
            sql_query=sql_query, sql_variables=sql_variables
        )

        result_df: pd.DataFrame = pd.DataFrame(
            results,
        )

        if close_connection:
            self.close_connection()

        return result_df

    def insert_into_with_dict(
        self,
        table_name: str,
        parameters_dict: dict,
        close_connection: bool = True,
        on_duplicate_ignore: bool = True,
    ) -> int:
        """Returns the number of rows that have been affected by the query"""
        # https://stackoverflow.com/questions/14071038/add-an-element-in-each-dictionary-of-a-list-list-comprehension
        # Building the sql query
        placeholder = ", ".join(["%s"] * len(parameters_dict))
        columns = '"' + '","'.join(parameters_dict.keys()) + '"'
        if on_duplicate_ignore:
            ignore_query = " ON CONFLICT DO NOTHING"
        else:
            ignore_query = ""
        query: str = 'INSERT INTO "{table}" ({columns}) VALUES ({values}) {update_query} '.format(
            table=table_name,
            columns=columns,
            values=placeholder,
            update_query=ignore_query,
        )

        row_updated: int = 0
        try:
            self.open_connection()

            db_cursor = self.db_connection.cursor()
            parameters_tuple = tuple(list(parameters_dict.values()))
            db_cursor.execute(query, parameters_tuple)
            row_updated = db_cursor.rowcount
            self.db_connection.commit()
            db_cursor.close()

        except Exception as ex:
            logger.error(
                f"Error: {ex}\n"
                f"parameters: {parameters_dict}"
                f"Sql string: {query}"
                f"Failed to insert into MySQL table {table_name}"
            )

        finally:
            if close_connection:
                self.close_connection()

        return row_updated

    def insert_into_with_dict_update(
        self,
        table_name: str,
        parameters_dict: dict,
        constraint_key: Optional[str] = None,
        on_duplicate_update: bool = True,
        close_connection: bool = True,
    ) -> int:
        """Returns the number of rows that have been affected by the query"""
        # https://stackoverflow.com/questions/35305946/python-sql-insert-into-on-duplicate-update-with-dictionary
        # https://stackoverflow.com/questions/14071038/add-an-element-in-each-dictionary-of-a-list-list-comprehension

        placeholder: str = ", ".join(["%s"] * len(parameters_dict))
        update_query: str = ""

        if on_duplicate_update:
            if constraint_key is None:
                constraint_key = f"{table_name}_pkey"
            set_query = ", ".join(
                [str(key) + f" = EXCLUDED.{key}" for key in parameters_dict.keys()]
            )
            update_query = (
                f" ON CONFLICT ON CONSTRAINT {constraint_key}"
                f" DO UPDATE "
                f" SET {set_query} "
            )

        columns = '"' + '","'.join(parameters_dict.keys()) + '"'
        query = 'INSERT INTO "{table}" ({columns}) VALUES ({values}) {update_query} '.format(
            table=table_name,
            columns=columns,
            values=placeholder,
            update_query=update_query,
        )

        parameters_tuple = tuple(list(parameters_dict.values()))

        updated_rows: int = 0
        try:
            self.open_connection()
            db_cursor = self.db_connection.cursor()
            db_cursor.execute(query, parameters_tuple)
            updated_rows = db_cursor.rowcount
            self.db_connection.commit()
            db_cursor.close()

        except Exception as ex:
            logger.error(
                f"Error with insert in Postgres: {ex}\n"
                f"Parameters used are:\n{parameters_tuple}\n"
                f"Query is:\n{query}"
            )

        if close_connection:
            self.close_connection()

        return updated_rows

    def insert_into_with_dict_update_no_try(
        self,
        table_name: str,
        parameters_dict: dict,
        constraint_key: Optional[str] = None,
        on_duplicate_update: bool = True,
        close_connection: bool = True,
    ) -> int:
        """Returns the number of rows that have been affected by the query"""
        # https://stackoverflow.com/questions/35305946/python-sql-insert-into-on-duplicate-update-with-dictionary
        # https://stackoverflow.com/questions/14071038/add-an-element-in-each-dictionary-of-a-list-list-comprehension

        placeholder: str = ", ".join(["%s"] * len(parameters_dict))
        update_query: str = ""

        if on_duplicate_update:
            if constraint_key is None:
                constraint_key = f"{table_name}_pkey"
            set_query = ", ".join(
                [str(key) + f" = EXCLUDED.{key}" for key in parameters_dict.keys()]
            )
            update_query = (
                f" ON CONFLICT ON CONSTRAINT {constraint_key}"
                f" DO UPDATE "
                f" SET {set_query} "
            )

        columns = '"' + '","'.join(parameters_dict.keys()) + '"'
        query = 'INSERT INTO "{table}" ({columns}) VALUES ({values}) {update_query} '.format(
            table=table_name,
            columns=columns,
            values=placeholder,
            update_query=update_query,
        )

        parameters_tuple = tuple(list(parameters_dict.values()))

        self.open_connection()
        db_cursor = self.db_connection.cursor()
        try:
            db_cursor.execute(query, parameters_tuple)
        except Exception as ex:
            logger.critical(f"Query: {db_cursor.query}\n" f"Raised Error is: {ex}")
            raise ex

        updated_rows = db_cursor.rowcount
        self.db_connection.commit()
        db_cursor.close()

        if close_connection:
            self.close_connection()

        return updated_rows


if __name__ == "__main__":
    my_postgres = PostgresConnector()
    sql_string = """
        SELECT version()
    """
    my_results = my_postgres.fetch_all_as_dicts(
        sql_query=sql_string, close_connection=True
    )
    my_postgres.close_connection()
    print(my_results)
