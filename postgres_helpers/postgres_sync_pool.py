import logging
from os import environ
from typing import (Union, Optional, List, Tuple)

import pandas as pd
from dotenv import load_dotenv
from psycopg2 import Error
from psycopg2.errors import UniqueViolation
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool

logger = logging.getLogger(f"postgres_helpers:{__name__}")


class PostgresConnectorPool:
    """Class to handle Postgresql"""

    def __init__(
            self,
            db_host: Optional[str] = None,
            db_port: Optional[str] = None,
            db_user: Optional[str] = None,
            db_password: Optional[str] = None,
            db_name: Optional[str] = None,
            connect_timeout: int = 6,
            pool_size_min: int = 2,
            pool_size_max: int = 5,
            application_name: Optional[str] = None,
    ):
        self.pool_size_min: int = pool_size_min
        self.pool_size_max: int = pool_size_max

        self.db_host = environ["POSTGRES_DB_HOST"] if db_host is None else db_host
        self.db_port = environ["POSTGRES_DB_PORT"] if db_port is None else str(db_port)
        self.db_user: str = environ["POSTGRES_DB_USER"] if db_user is None else db_user
        self.db_password: str = (
            environ["POSTGRES_DB_PASS"] if db_password is None else db_password
        )
        self.db_name: str = environ["POSTGRES_DB_NAME"] if db_name is None else db_name

        self.db_connection_pool: Union[SimpleConnectionPool, None] = None
        self.db_version: Union[str, None] = None
        self.connect_timeout: int = connect_timeout

        self.application_name = application_name if application_name is None else application_name.strip().replace(" ",
                                                                                                                   "_")

    def _create_pool_connection(self):
        """Create a pool connection if None, Raise exception on error"""
        if self.db_connection_pool is None:
            # check pool size min is lower than pool size max
            if self.pool_size_min >= self.pool_size_max:
                self.pool_size_min = max(1, self.pool_size_max - 1)
            try:
                self.db_connection_pool = SimpleConnectionPool(
                    minconn=self.pool_size_min,
                    maxconn=self.pool_size_max,
                    host=self.db_host,
                    port=self.db_port,
                    user=self.db_user,
                    password=self.db_password,
                    dbname=self.db_name,
                    application_name=self.application_name
                    # server_settings=self.server_settings if self.server_settings else None
                )
            except Exception as ex:
                logger.error(f"Error while creating Pool with Postgres: {ex}")
                raise Exception(f"Error while creating Pool with Postgres: {ex}")

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
    ) -> tuple:
        """return a tuple of (last_inserted_row_id, rows_affected, status_message)
        rows_affected return -1 if SQL error: duplicate key etc., 0 if nothing changed, # of records affected
        """

        self._create_pool_connection()
        conn = self.db_connection_pool.getconn()
        conn.autocommit = True

        db_cursor = conn.cursor()

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
                f"Error while inserting data {sql_variables}-"
                f"with SQL query: {sql_query}-"
                f"error: {ex}-"
                f"error Class: {ex.__class__.__name__}"
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
        self.db_connection_pool.putconn(conn)

        return last_row_id, rows_affected, status_message

    def execute_many_query(self, sql_query: str, tuples_list: list) -> tuple:
        """Return a tuple of (rows_affected, status_message)
        tuples_list is a list of parameters as tuples
        the query will fail if there is an SQL error, not using try/except

        """
        self._create_pool_connection()
        conn = self.db_connection_pool.getconn()
        conn.autocommit = True

        db_cursor = conn.cursor()

        db_cursor.executemany(sql_query, tuples_list)

        # self.db_connection.commit()
        # commit shouldn't be needed anymore using autocommit
        # https://stackoverflow.com/questions/9222256/how-do-i-know-if-i-have-successfully-created-a-table-python-psycopg2
        rows_affected = db_cursor.rowcount
        status_message = db_cursor.statusmessage
        db_cursor.close()
        self.db_connection_pool.putconn(conn)

        return rows_affected, status_message

    def fetch_all_as_dicts(
            self, sql_query: str, sql_variables: tuple = None
    ) -> List[Tuple]:
        """

        """

        self._create_pool_connection()
        conn = self.db_connection_pool.getconn()

        db_cursor = conn.cursor(
            cursor_factory=RealDictCursor
        )

        db_cursor.execute(sql_query, sql_variables)
        rows_found = db_cursor.fetchall()
        db_cursor.close()
        self.db_connection_pool.putconn(conn)

        return rows_found

    def fetch_all_as_df(
            self, sql_query: str, sql_variables: tuple = None
    ) -> Union[None, pd.DataFrame]:

        results: list = self.fetch_all_as_dicts(
            sql_query=sql_query, sql_variables=sql_variables
        )

        result_df: pd.DataFrame = pd.DataFrame(
            results,
        )

        return result_df

    def insert_into_with_dict(
            self,
            table_name: str,
            parameters_dict: dict,
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
            self._create_pool_connection()
            conn = self.db_connection_pool.getconn()
            conn.autocommit = True
            db_cursor = conn.cursor()
            parameters_tuple = tuple(list(parameters_dict.values()))
            db_cursor.execute(query, parameters_tuple)
            row_updated = db_cursor.rowcount

            db_cursor.close()

        except Exception as ex:
            logger.error(
                f"Error: {ex}-"
                f"parameters: {parameters_dict}-"
                f"Sql string: {query}-"
                f"Failed to insert into MySQL table {table_name}"
            )

        return row_updated

    def insert_with_dict_returning(
            self,
            table_name: str,
            parameters_dict: dict,
            on_duplicate_ignore: bool = True,
    ) -> dict | None:
        """Returns the row that has been inserted, None if an error occurred"""
        # https://stackoverflow.com/questions/14071038/add-an-element-in-each-dictionary-of-a-list-list-comprehension
        # Building the sql query
        placeholder = ", ".join(["%s"] * len(parameters_dict))
        columns = '"' + '","'.join(parameters_dict.keys()) + '"'
        if on_duplicate_ignore:
            ignore_query = " ON CONFLICT DO NOTHING"
        else:
            ignore_query = ""
        query: str = 'INSERT INTO "{table}" ({columns}) VALUES ({values}) {update_query}  RETURNING *'.format(
            table=table_name,
            columns=columns,
            values=placeholder,
            update_query=ignore_query,
        )

        try:
            self._create_pool_connection()
            conn = self.db_connection_pool.getconn()
            conn.autocommit = True
            db_cursor = conn.cursor()
            parameters_tuple = tuple(list(parameters_dict.values()))
            db_cursor.execute(query, parameters_tuple)
            created_rows = db_cursor.fetchone()

            db_cursor.close()
            return created_rows

        except Exception as ex:
            logger.error(
                f"Error: {ex}-"
                f"parameters: {parameters_dict}-"
                f"Sql string: {query}-"
                f"Failed to insert into MySQL table {table_name}"
            )

        return None

    def insert_into_with_dict_update(
            self,
            table_name: str,
            parameters_dict: dict,
            constraint_key: Optional[str] = None,
            on_duplicate_update: bool = True,
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
        self._create_pool_connection()
        conn = self.db_connection_pool.getconn()
        conn.autocommit = True
        try:
            db_cursor = conn.cursor()
            db_cursor.execute(query, parameters_tuple)
            updated_rows = db_cursor.rowcount
            db_cursor.close()
        except Exception as ex:
            logger.error(
                f"Error with insert in Postgres: {ex}-"
                f"Parameters used are:\n{parameters_tuple}-"
                f"Query is:\n{query}"
            )

        self.db_connection_pool.putconn(conn)
        return updated_rows

    def insert_into_with_dict_update_no_try(
            self,
            table_name: str,
            parameters_dict: dict,
            constraint_key: Optional[str] = None,
            on_duplicate_update: bool = True,
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

        conn = self.db_connection_pool.getconn()
        conn.autocommit = True
        db_cursor = conn.cursor()

        try:
            db_cursor.execute(query, parameters_tuple)
        except Exception as ex:
            logger.critical(f"Query: {db_cursor.query}\n" f"Raised Error is: {ex}")
            raise ex

        updated_rows = db_cursor.rowcount

        db_cursor.close()

        self.db_connection_pool.putconn(conn)

        return updated_rows


if __name__ == "__main__":
    load_dotenv()
    my_postgres = PostgresConnectorPool()
    sql_string = """
        SELECT version()
    """
    my_results = my_postgres.fetch_all_as_dicts(
        sql_query=sql_string,
    )
    # my_postgres.get_postgresql_version()

    print(my_results)
