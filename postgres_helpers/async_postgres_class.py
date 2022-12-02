# https://magicstack.github.io/asyncpg/current/api/index.html
from typing import Union

import asyncpg
import pandas as pd
from asyncpg.connection import Connection

from postgres_helpers.app_config_secret import (POSTGRES_DB_USER, POSTGRES_DB_HOST,
                                                POSTGRES_DB_PASS, POSTGRES_DB_NAME,
                                                POSTGRES_DP_PORT)


# to create async class
# https://bbc.github.io/cloudfit-public-docs/asyncio/asyncio-part-3.html

class AsyncPostGresConnector:
    def __init__(self, mdb_host: str = POSTGRES_DB_HOST, mdb_port: int = POSTGRES_DP_PORT,
                 mdb_user: str = POSTGRES_DB_USER, mdb_password: str = POSTGRES_DB_PASS,
                 mdb_name: str = POSTGRES_DB_NAME):
        self.mdb_host: str = mdb_host
        self.mdb_port: int = mdb_port
        self.mdb_user: str = mdb_user
        self.mdb_password: str = mdb_password
        self.mdb_name: str = mdb_name
        self.mdb_connection: Union[Connection, None] = None

    async def open_connection(self) -> bool:
        """Returns True if successfully connected to mdb"""
        if self.mdb_connection is None or self.mdb_connection.is_closed():
            try:
                self.mdb_connection = await asyncpg.connect(host=self.mdb_host,
                                                            port=self.mdb_port,
                                                            user=self.mdb_user,
                                                            password=self.mdb_password,
                                                            database=self.mdb_name)
                return True
            except Exception as ex:
                print(f"Error while connecting PostgreSQL in Async:\n{ex}")
                return False
        elif self.mdb_connection.is_closed() is False:
            return True
        else:
            return False

    async def close_connection(self) -> bool:
        """Returns True if successfully disconnected from mdb"""
        if not self.mdb_connection:
            return True

        if self.mdb_connection is not None and self.mdb_connection.is_closed() is False:
            try:
                await self.mdb_connection.close()
                return True
            except Exception as ex:
                print(f"Error while closing connection:\n{ex}")
                return False

    async def fetch_all_as_pd(self, sql_query: str,
                              sql_variables: tuple = None, close_connection: bool = True) -> Union[None, pd.DataFrame]:
        """Fetch query data in a pd Dataframe"""
        if await self.open_connection() is False:
            return None
        if sql_variables:
            results = await self.mdb_connection.fetch(sql_query, *sql_variables)
        else:
            results = await self.mdb_connection.fetch(sql_query)
        result_df: pd.DataFrame = pd.DataFrame([dict(r.items()) for r in results])
        if close_connection:
            await self.close_connection()
        return result_df

    async def fetch_all_as_dicts(self, sql_query: str,
                                 sql_variables: tuple = None, close_connection: bool = True) -> Union[None, list]:
        """Fetch query data in a pd Dataframe"""
        if await self.open_connection() is False:
            print("No connection was established")
            return None
        if sql_variables:
            results: list = await self.mdb_connection.fetch(sql_query, *sql_variables)
        else:
            results: list = await self.mdb_connection.fetch(sql_query)
        results = [dict(r.items()) for r in results]
        if close_connection:
            await self.close_connection()
        return results

    async def execute_one_query(self, sql_query: str,
                                sql_variables: tuple = None, close_connection: bool = True) -> Union[None, int]:
        """Fetch query data in a pd Dataframe"""
        if await self.open_connection() is False:
            print("No connection was established")
            return None
        if sql_variables:
            result: str = await self.mdb_connection.execute(sql_query, *sql_variables)
        else:
            result: str = await self.mdb_connection.execute(sql_query, )

        if close_connection:
            await self.close_connection()
        # https://www.postgresql.org/docs/current/protocol-message-formats.html
        # parse and send numbe rof affected rows by query
        return int(result.split(" ")[-1])


if __name__ == '__main__':
    pass
