import asyncio
import concurrent.futures
import datetime
from urllib.parse import quote_plus

import httpx
import pandas as pd
import sqlalchemy as db
from dotenv import dotenv_values


def create_database_engine(username, password, server_address, ) -> db.engine.Engine:
    """Create mariadb database engine
    :parameter: None
    :return: db_engine: sqlalchemy.engine.Engine
    """

    db_engine = db.create_engine(f'mariadb+mariadbconnector://{username}:%s@{server_address}/eve_online_database' % quote_plus(password))
    return db_engine


def query_region_id(list_region_name):
    response = httpx.post('https://esi.evetech.net/latest/universe/ids/?datasource=tranquility', json=list_region_name)
    list_region_ids = [region['id'] for region in response.json()['regions']]
    return list_region_ids


def query_type_id(db_engine, table_itemsDim):
    with db_engine.begin() as con:
        stmt = db.select(table_itemsDim.c.typeID)
        result = con.execute(stmt)
    return [row.typeID for row in result]


async def fetch(region_id, type_id, client, retry=2):
    while retry:
        try:
            url = f'https://esi.evetech.net/latest/markets/{region_id}/history/?datasource=tranquility&type_id={type_id}'
            response = await client.get(url)
            headers = response.headers
            response.raise_for_status()
            data = response.json()
            if data is None:
                print(url)
                print(response)
                print(headers)
                return []
            else:
                for entry in data:
                    entry['regionID'] = region_id
                    entry['typeID'] = type_id
                return data
        except httpx.HTTPError as exc:
            print(headers)
            # retry == 1 is the last retry attempt
            # because the retry loop terminates if retry == 0
            if retry == 1:
                print(exc)
                return []
            else:
                if response.status_code == 500:
                    print(f'{exc}. Retrying...')
                    retry -= 1
                elif response.status_code == 502:
                    print(f'{exc}. Retrying...')
                    retry -= 1
                elif response.status_code == 420:
                    error_limit_reset_time = int(headers['x-esi-error-limit-reset'])
                    print(f'{exc}.\nRetrying after {error_limit_reset_time} seconds...')
                    await asyncio.sleep(error_limit_reset_time)
                else:
                    print(f'{exc}. Retrying...')
                    retry -= 1
        except Exception as e:
            print(e)
            raise asyncio.CancelledError


async def fetch_market_history(region_id, type_ids):
    """Fetch market history from region with region id: region_id and write to csv file"""
    limits = httpx.Limits(max_connections=100)
    async with httpx.AsyncClient(limits=limits, timeout=90) as client:
        async with asyncio.TaskGroup() as tg:
            data = [await tg.create_task(fetch(region_id=region_id, type_id=type_id, client=client)) for type_id in type_ids]
            data = [entry for entries in data for entry in entries]
            df = pd.DataFrame(data)
            filename = f'/data/marketHistory_{datetime.date.today()}.csv'
            df.to_csv(filename, index=False, header=False, mode='a')
            print(f'{region_id} market history written to csv.')


def create_aio_loop(region_id, type_ids):
    """Synchronous function for multiprocess that runs async fetch function"""
    print(f'Fetching region history: {region_id}')
    asyncio.run(fetch_market_history(region_id, type_ids))


async def create_subprocess(region_ids, type_ids):
    """Create multiple processes, each fetching market data of a region"""
    loop = asyncio.get_running_loop()
    with concurrent.futures.ProcessPoolExecutor(max_workers=len(region_ids)) as executor:
        for region_id in region_ids:
            loop.run_in_executor(executor, create_aio_loop, region_id, type_ids)


def main():
    #load environment variables
    env = dotenv_values()

    MARIADB_SERVER_ADDRESS = env['MARIADB_SERVER_ADDRESS']
    MARIADB_USERNAME = env['MARIADB_USERNAME']
    MARIADB_PASSWORD = env['MARIADB_PASSWORD']

    db_engine = create_database_engine(MARIADB_USERNAME, MARIADB_PASSWORD, MARIADB_SERVER_ADDRESS)
    db_metadata = db.MetaData()
    table_itemsDim = db.Table('itemsDim', db_metadata, autoload_with=db_engine)

    region_ids = query_region_id(['The Forge', 'Domain', 'Sinq Laison', 'Heimatar', 'Metropolis', 'Perrigen Falls', 'Tenerifis'])
    type_ids = query_type_id(db_engine, table_itemsDim)

    asyncio.run(create_subprocess(region_ids, type_ids))


if __name__ == '__main__':
    main()

