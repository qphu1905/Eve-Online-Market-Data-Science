import asyncio
import concurrent.futures
import logging
from urllib.parse import quote_plus

import httpx
import pandas as pd
import sqlalchemy as db
from dotenv import dotenv_values


def create_database_engine() -> db.engine.Engine:
    """Create mariadb database engine
    :parameter: None
    :return: db_engine: sqlalchemy.engine.Engine
    """

    db_engine = db.create_engine(f'mariadb+mariadbconnector://{MARIADB_USERNAME}:%s@{MARIADB_SERVER_ADDRESS}/eve_online_database' % quote_plus(MARIADB_PASSWORD))
    return db_engine

def query_region_id(db_engine, table_marketRegionsDim):
    with db_engine.begin() as con:
        stmt = db.select(table_marketRegionsDim.c.regionID)
        result = con.execute(stmt)
    return [row.regionID for row in result]

def query_type_id(db_engine, table_itemsDim):
    with db_engine.begin() as con:
        stmt = db.select(table_itemsDim.c.typeID)
        result = con.execute(stmt)
    return [row.typeID for row in result]


async def fetch_market_history(region_id):
    """Fetch market history from region with region id: region_id and write to csv file"""
    limits = httpx.Limits(max_connections=None, max_keepalive_connections=None)
    async with httpx.AsyncClient(limits=limits) as client:
        async with asyncio.TaskGroup() as tg:
            urls = [f'https://evetycoon.com/api/v1/market/history/{region_id}/{type_id}' for type_id in type_ids]
            responses = [await tg.create_task(client.get(url)) for url in urls]
            data = [response.json() for response in responses]
            data = [entry for entries in data for entry in entries]
            df = pd.DataFrame(data)
            df.to_csv('marketHistory.csv', index=False, mode='a')


def create_aio_loop(region_id):
    """Synchronous function for multiprocess that run async fetch function"""
    print(f'Fetching region history: {region_id}')
    asyncio.run(fetch_market_history(region_id))


async def main():
    """Create multiple processes, each fetching market data of a region, in batches of 10 processes/region per batch"""
    loop = asyncio.get_running_loop()
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        for region_id in region_ids:
            loop.run_in_executor(executor, create_aio_loop, region_id)


if __name__ == '__main__':
    #load environment variables
    env = dotenv_values()

    MARIADB_SERVER_ADDRESS = env['MARIADB_SERVER_ADDRESS']
    MARIADB_USERNAME = env['MARIADB_USERNAME']
    MARIADB_PASSWORD = env['MARIADB_PASSWORD']

    db_engine = create_database_engine()
    db_metadata = db.MetaData()
    table_marketRegionsDim = db.Table('marketRegionsDim', db_metadata, autoload_with=db_engine)
    table_itemsDim = db.Table('itemsDim', db_metadata, autoload_with=db_engine)
    region_ids = query_region_id(db_engine, table_marketRegionsDim)
    type_ids = query_type_id(db_engine, table_itemsDim)

    asyncio.run(main())

