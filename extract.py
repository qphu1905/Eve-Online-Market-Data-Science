import pandas as pd
import sqlalchemy as db
from dotenv import dotenv_values
from urllib.parse import quote_plus
import requests
import asyncio
import httpx
from aiolimiter import AsyncLimiter

#load environment variables
env = dotenv_values()

MARIADB_SERVER_ADDRESS = env['MARIADB_SERVER_ADDRESS']
MARIADB_USERNAME = env['MARIADB_USERNAME']
MARIADB_PASSWORD = env['MARIADB_PASSWORD']

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

async def fetch_market_history(region_id, type_id, client, limiter):
    async with limiter:
        response = await client.get(f'https://evetycoon.com/api/v1/market/history/{region_id}/{type_id}')
    return response.json()
async def main():
    rate_limiter = AsyncLimiter(1000,1)
    async with httpx.AsyncClient() as client:
        task = []
        for type_id in type_ids[:]:
            task.append(fetch_market_history(10000002, type_id, client, rate_limiter))
        data = await asyncio.gather(*task)
    data = [entry for entries in data for entry in entries]
    df = pd.DataFrame(data)
    print(df)
    return df
db_engine = create_database_engine()
db_metadata = db.MetaData()
table_marketRegionsDim = db.Table('marketRegionsDim', db_metadata, autoload_with=db_engine)
table_itemsDim = db.Table('itemsDim', db_metadata, autoload_with=db_engine)
table_marketHistory = db.Table('marketHistory', db_metadata, autoload_with=db_engine)

region_ids = query_region_id(db_engine, table_marketRegionsDim)
type_ids = query_type_id(db_engine, table_itemsDim)

result = asyncio.run(main())
