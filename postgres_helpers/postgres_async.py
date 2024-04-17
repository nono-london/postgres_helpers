# https://magicstack.github.io/asyncpg/current/api/index.html
import logging
from pathlib import Path

logger = logging.getLogger(f"postgres_helpers:{Path(__file__).name}")

import asyncio
from os import environ
from typing import Union, Optional, List, Dict

import asyncpg
import pandas as pd
from asyncpg.connection import Connection

from postgres_helpers.app_config import load_postgres_details_to_env


# to create async class
# https://bbc.github.io/cloudfit-public-docs/asyncio/asyncio-part-3.html


class PostgresConnectorAsync:
    def __init__(
            self,
            db_host: Optional[str] = None,
            db_port: Optional[str] = None,
            db_user: Optional[str] = None,
            db_password: Optional[str] = None,
            db_name: Optional[str] = None,
            application_name: Optional[str] = None
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

        # shows application name within PGAdmin4 fos instance
        self.server_settings = {'application_name': application_name} if application_name else None

        self.db_connection: Union[Connection, None] = None

    async def open_connection(self):
        """Returns True if successfully connected to mdb"""
        if self.db_connection is None or self.db_connection.is_closed():
            try:
                self.db_connection = await asyncpg.connect(
                    host=self.db_host,
                    port=self.db_port,
                    user=self.db_user,
                    password=self.db_password,
                    database=self.db_name,
                    server_settings=self.server_settings if self.server_settings else None
                )

            except Exception as ex:
                logger.error(f"Error while connecting PostgreSQL in Async: {ex}")
                raise Exception(f"Error while connecting PostgreSQL in Async: {ex}")

    async def close_connection(self) -> bool:
        """Returns True if successfully disconnected from mdb"""
        if not self.db_connection:
            return True

        if self.db_connection is not None and self.db_connection.is_closed() is False:
            try:
                await self.db_connection.close()
                return True
            except Exception as ex:
                logger.error(f"Error while closing connection: {ex}")
                return False

    async def execute_one_query(
            self, sql_query: str,
            sql_variables: tuple = None,
            close_connection: bool = True
    ) -> Union[None, int]:
        """
        :param sql_query: a query to execute once
        :param sql_variables: variables of the query in a Tuple
        :param close_connection: closes the connection after execution or leave it opened
        :return: the number of rows affected or -1 if an error occurred
        """
        # check that we have a connection, raise error if not
        await self.open_connection()

        result = await self.db_connection.execute(
            sql_query,
            *sql_variables if sql_variables else ()
        )

        if close_connection:
            await self.close_connection()
        # https://www.postgresql.org/docs/current/protocol-message-formats.html
        # parse and send number of affected rows by query

        affected_rows: int = -1
        try:
            affected_rows = int(result.split(" ")[-1])
        except ValueError as ex:
            logger.error(f"Error while parsing Results: {result}, Error: {ex}")

        return affected_rows

    async def fetch_all_as_dicts(
            self, sql_query: str, sql_variables: tuple = None, close_connection: bool = True
    ) -> Union[List[Dict], None]:
        """
        Return results of a query as a pd.DataFrame
        :param sql_query: the SELECT query
        :param sql_variables: variables of the query in a Tuple
        :param close_connection: closes the connection after execution or leave it opened
        :return: a List of Dicts if there was no error, else None
        """

        # check that we have a connection, raise error if not
        await self.open_connection()

        results = await self.db_connection.fetch(
            sql_query,
            *sql_variables if sql_variables else ()
        )
        results = [dict(r.items()) for r in results]
        if close_connection:
            await self.close_connection()
        return results

    async def fetch_all_as_df(
            self, sql_query: str, sql_variables: tuple = None, close_connection: bool = True
    ) -> Union[pd.DataFrame, None]:
        """
        Return results of a query as a pd.DataFrame
        :param sql_query: the SELECT query
        :param sql_variables: variables of the query in a Tuple
        :param close_connection: closes the connection after execution or leave it opened
        :return: a pd.DataFrame if there was no error, else None
        """
        # Uses fetch_all_as_dicts and dataframe results
        results = await self.fetch_all_as_dicts(sql_query=sql_query,
                                                sql_variables=sql_variables,
                                                close_connection=close_connection)

        result_df: pd.DataFrame = pd.DataFrame(results)
        if close_connection:
            await self.close_connection()

        return result_df


if __name__ == "__main__":
    sql_string = """
        SELECT version()
    """
    my_postgres = PostgresConnectorAsync()
    my_results = asyncio.new_event_loop().run_until_complete(
        my_postgres.fetch_all_as_dicts(sql_query=sql_string,
                                       close_connection=True)
    )

    print(my_results)
