import asyncio
import logging
from os import getenv
from pathlib import Path
from typing import Union, Optional, List, Dict

import asyncpg
import pandas as pd
from asyncpg.pool import Pool

from postgres_helpers.app_config import load_postgres_details_to_env

logger = logging.getLogger(f"postgres_helpers:{Path(__file__).name}")


# to create async class
# https://bbc.github.io/cloudfit-public-docs/asyncio/asyncio-part-3.html


class PostgresConnectorAsyncPool:
    def __init__(
            self,

            pool_size_max: int = 30,
            db_host: Optional[str] = None,
            db_port: Optional[str] = None,
            db_user: Optional[str] = None,
            db_password: Optional[str] = None,
            db_name: Optional[str] = None,
            application_name: Optional[str] = None,
            asyncio_loop=None

    ):
        if None in [
            db_host,
            db_port,
            db_name,
            db_user,
            db_password,
        ]:
            load_postgres_details_to_env()

        self.db_host = getenv("POSTGRES_DB_HOST") if db_host is None else db_host
        self.db_port = getenv("POSTGRES_DB_PORT") if db_port is None else str(db_port)
        self.db_user: str = getenv("POSTGRES_DB_USER") if db_user is None else db_user
        self.db_password: str = (
            getenv("POSTGRES_DB_PASS") if db_password is None else db_password
        )
        self.db_name: str = getenv("POSTGRES_DB_NAME") if db_name is None else db_name

        self.pool_size_max: int = pool_size_max

        self.db_connection_pool: Union[Pool, None] = None
        # shows application name within PGAdmin4 fos instance
        self.server_settings = {'application_name': application_name} if application_name else None

    async def _create_pool_connection(self):
        """Create a pool connection if None, Raise exception on error"""
        try:
            if self.db_connection_pool is None:
                self.db_connection_pool = await asyncpg.create_pool(
                    host=self.db_host,
                    port=self.db_port,
                    user=self.db_user,
                    password=self.db_password,
                    database=self.db_name,
                    max_size=self.pool_size_max,
                    server_settings=self.server_settings if self.server_settings else None
                )
        except Exception as ex:
            logger.error(f"Error while creating Pool Async with PostgreSQL: {ex}")
            raise Exception(f"Error while creating Pool Async with PostgreSQL: {ex}")

    async def execute_one_query(
            self, sql_query: str, sql_variables: tuple = None
    ) -> Union[None, int]:
        """
        Execute an SQL query once
        :param sql_query: a query to execute once
        :param sql_variables: variables of the query in a Tuple
        :return: the number of rows affected or -1 if an error occurred
        """

        # check that we have a connection pool, raise error if not
        await self._create_pool_connection()

        async with self.db_connection_pool.acquire() as conn:
            result = await conn.execute(
                sql_query,
                *sql_variables if sql_variables is not None else ()
            )

        # https://www.postgresql.org/docs/current/protocol-message-formats.html
        # parse and send number of affected rows by query

        affected_rows: int = -1
        try:
            affected_rows = int(result.split(" ")[-1])
        except ValueError as ex:
            logger.error(f"Error while parsing Results: {result}, Error: {ex}")

        return affected_rows

    async def execute_many_query(self, sql_query: str, tuples_list: list) -> int:
        """Return teh number of rows rows_affected,
        the query will fail if there is an SQL error, not using try/except
        :param sql_query: the SQL query to execute many times
        :param tuples_list: a list of parameters as tuples
        """
        # check that we have a connection pool, raise error if not
        await self._create_pool_connection()

        async with self.db_connection_pool.acquire() as conn:
            db_cursor = conn.cursor()
            db_cursor.executemany(sql_query, tuples_list)
            rows_affected = db_cursor.rowcount
            status_message = db_cursor.statusmessage
            db_cursor.close()

        return rows_affected

    async def fetch_all_as_dicts(
            self, sql_query: str, sql_variables: Optional[tuple] = None
    ) -> Union[List[Dict], None]:
        """Fetch query data in a list of dicts
        :param sql_query: a sql statement
        :type sql_query: str
        :param sql_variables: a tuple with variables
        :type sql_variables: tuple
        :returns: a list of dicts
        :rtype: List[Dict]
        """

        # check that we have a connection pool, raise error if not
        await self._create_pool_connection()

        async with self.db_connection_pool.acquire() as conn:
            results = await conn.fetch(
                sql_query,
                *sql_variables if sql_variables is not None else ()
            )

            results = [dict(r.items()) for r in results]

        return results

    async def fetch_all_as_df(
            self, sql_query: str, sql_variables: tuple = None
    ) -> Union[pd.DataFrame, None]:
        """Fetch query data in a pd Dataframe
        :param sql_query: a sql statement
        :param sql_variables: a tuple with variables
        :returns: a pd.Dataframe of the results
        """

        # check that we have a connection pool, raise error if not
        await self._create_pool_connection()

        results = await self.fetch_all_as_dicts(sql_query=sql_query,
                                                sql_variables=sql_variables)

        result_df: Union[pd.DataFrame, None] = None
        if results:
            result_df = pd.DataFrame([dict(r.items()) for r in results])

        return result_df


if __name__ == "__main__":
    sql_string = """
        SELECT version()
    """
    loop = asyncio.new_event_loop()
    my_postgres = PostgresConnectorAsyncPool(asyncio_loop=loop,
                                             application_name="test pool")
    my_results = loop.run_until_complete(
        my_postgres.fetch_all_as_df(sql_query=sql_string)
    )

    print(my_results)
