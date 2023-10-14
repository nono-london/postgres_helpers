# https://magicstack.github.io/asyncpg/current/api/index.html
import asyncio
from os import getenv
from typing import (Union, Optional, List, Dict)

import asyncpg
import pandas as pd
from asyncpg.pool import Pool

from postgres_helpers.app_config import load_postgres_details_to_env


# to create async class
# https://bbc.github.io/cloudfit-public-docs/asyncio/asyncio-part-3.html

class PostgresConnectorAsyncPool:
    def __init__(self, pool_size_max: int = 30,
                 db_host: Optional[str] = None, db_port: Optional[str] = None,
                 db_user: Optional[str] = None, db_password: Optional[str] = None,
                 db_name: Optional[str] = None,
                 ):

        if None in [db_host, db_port, db_name, db_user, db_password, ]:
            load_postgres_details_to_env()

        self.db_host = getenv('POSTGRES_DB_HOST') if db_host is None else db_host
        self.db_port = getenv('POSTGRES_DB_PORT') if db_port is None else str(db_port)
        self.db_user: str = getenv('POSTGRES_DB_USER') if db_user is None else db_user
        self.db_password: str = getenv('POSTGRES_DB_PASS') if db_password is None else db_password
        self.db_name: str = getenv('POSTGRES_DB_NAME') if db_name is None else db_name

        self.pool_size_max: int = pool_size_max

        self.db_connection_pool: Union[Pool, None] = None
        asyncio.get_event_loop().run_until_complete(self._create_pool_connection())

    async def _create_pool_connection(self) -> bool:
        """Returns True if successfully connected to mdb"""
        try:
            self.db_connection_pool = await asyncpg.create_pool(host=self.db_host,
                                                                port=self.db_port,
                                                                user=self.db_user,
                                                                password=self.db_password,
                                                                database=self.db_name,
                                                                max_size=self.pool_size_max

                                                                )
            return True
        except Exception as ex:
            print(f"Error while connecting PostgreSQL in Async:\n{ex}")
            return False

    async def execute_one_query(self, sql_query: str,
                                sql_variables: tuple = None) -> Union[None, int]:
        """Fetch query data in a pd Dataframe"""
        with self.db_connection_pool.acquire() as conn:
            if sql_variables is None:
                result: str = await conn.execute(sql_query)
            else:
                result: str = await conn.execute(sql_query, *sql_variables)

        # https://www.postgresql.org/docs/current/protocol-message-formats.html
        # parse and send number of affected rows by query

        affected_rows: int = -1
        try:
            affected_rows = int(result.split(" ")[-1])
        except ValueError as ex:
            print(f"Result of query was: {result}\n"
                  f"Error was: {ex}")

        return affected_rows

    async def execute_many_query(self, sql_query: str, tuples_list: list) -> tuple:
        """Return a tuple of (rows_affected, status_message)
            tuples_list is a list of parameters as tuples
            the query will fail if there is an SQL error, not using try/except

        """
        async with self.db_connection_pool.acquire() as conn:
            db_cursor = conn.cursor()
            db_cursor.executemany(sql_query, tuples_list)
            rows_affected = db_cursor.rowcount
            status_message = db_cursor.statusmessage
            db_cursor.close()

        return rows_affected, status_message

    async def fetch_all_as_df(self, sql_query: str,
                              sql_variables: tuple = None) -> Union[None, pd.DataFrame]:
        """Fetch query data in a pd Dataframe
        :param sql_query: a sql statement
        :type sql_query: str
        :param sql_variables: a tuple with variables
        :type sql_variables: tuple
        :returns: a pd.Dataframe of the results
        :rtype: pd.Dataframe
        """

        async with self.db_connection_pool.acquire() as conn:
            if sql_variables is None:
                results = await conn.fetch(sql_query, )
            else:
                results = await conn.fetch(sql_query, *sql_variables)

        result_df: pd.DataFrame = pd.DataFrame([dict(r.items()) for r in results])

        return result_df

    async def fetch_all_as_dicts(self, sql_query: str,
                                 sql_variables: Optional[tuple] = None) -> Union[None, List[Dict]]:
        """Fetch query data in a list of dicts
        :param sql_query: a sql statement
        :type sql_query: str
        :param sql_variables: a tuple with variables
        :type sql_variables: tuple
        :returns: a list of dicts
        :rtype: List[Dict]
        """
        async with self.db_connection_pool.acquire() as conn:
            if sql_variables is None:
                results = await conn.fetch(sql_query, )
            else:
                results = await conn.fetch(sql_query, *sql_variables)
            results = [dict(r.items()) for r in results]

        return results


if __name__ == '__main__':
    sql_string = """
        SELECT version()
    """
    my_postgres = PostgresConnectorAsyncPool()
    my_results = asyncio.get_event_loop().run_until_complete(
        my_postgres.fetch_all_as_df(sql_query=sql_string))

    print(my_results)
