import sys

import pandas as pd
import sqlalchemy as db
from dotenv import dotenv_values
from urllib.parse import quote_plus


def create_database_engine(username, password, server_address) -> db.engine.Engine:
    """Create mariadb database engine
    :parameter: None
    :return: db_engine: sqlalchemy.engine.Engine
    """

    db_engine = db.create_engine(f'mariadb+mariadbconnector://{username}:%s@{server_address}/eve_online_database' % quote_plus(password))
    return db_engine


def clear_ingest(db_engine: db.engine.Engine):
    """Delete all entry in temporary ingest table"""

    print("Clearing ingest table")
    with db_engine.connect() as connection:
        query = db.text("TRUNCATE marketHistoryDataIngest")
        connection.execute(query)
    print("Ingest table cleared")


def load_csv_to_ingest(filename: str, db_engine: db.engine.Engine):
    """Load market data in csv file into temporary ingest table"""
    print('Loading to staging')
    names = ['date', 'regionID', 'typeID', 'average', 'highest', 'lowest', 'orderCount', 'volume',
             '2dLaggedReturn',
             '2dMomentum',
             '2dPriceMovingAverage',
             '2dPriceMovingAverageStd',
             '2dHighestMovingAverage',
             '2dLowestMovingAverage',
             '2dRelativePriceStrength',
             '2dVolumeMovingAverage',
             '2dRelativeVolumeStrength',
             '5dLaggedReturn',
             '5dMomentum',
             '5dPriceMovingAverage',
             '5dPriceMovingAverageStd',
             '5dHighestMovingAverage',
             '5dLowestMovingAverage',
             '5dRelativePriceStrength',
             '5dVolumeMovingAverage',
             '5dRelativeVolumeStrength',
             '10dLaggedReturn',
             '10dMomentum',
             '10dPriceMovingAverage',
             '10dPriceMovingAverageStd',
             '10dHighestMovingAverage',
             '10dLowestMovingAverage',
             '10dRelativePriceStrength',
             '10dVolumeMovingAverage',
             '10dRelativeVolumeStrength',
             '20dLaggedReturn',
             '20dMomentum',
             '20dPriceMovingAverage',
             '20dPriceMovingAverageStd',
             '20dHighestMovingAverage',
             '20dLowestMovingAverage',
             '20dRelativePriceStrength',
             '20dVolumeMovingAverage',
             '20dRelativeVolumeStrength',
             '50dLaggedReturn',
             '50dMomentum',
             '50dPriceMovingAverage',
             '50dPriceMovingAverageStd',
             '50dHighestMovingAverage',
             '50dLowestMovingAverage',
             '50dRelativePriceStrength',
             '50dVolumeMovingAverage',
             '50dRelativeVolumeStrength',
             '100dLaggedReturn',
             '100dMomentum',
             '100dPriceMovingAverage',
             '100dPriceMovingAverageStd',
             '100dHighestMovingAverage',
             '100dLowestMovingAverage',
             '100dRelativePriceStrength',
             '100dVolumeMovingAverage',
             '100dRelativeVolumeStrength']

    with pd.read_csv(filename, header=None, index_col=False, names=names, chunksize=50000) as reader:
        for df in reader:
            df.to_sql('marketHistoryDataIngest', con=db_engine, if_exists='append', index=False)
    print('Loaded to staging')


def load_ingest_to_prod(ingest, prod, db_engine: db.engine.Engine):
    """Load data from ingest table into prod table
       Ignore duplicate"""
    print('loading to prod')
    ingest_columns = [column.name for column in ingest.c]
    with db_engine.begin() as conn:
        stmt = db.insert(prod).from_select(ingest_columns, db.select(ingest)).prefix_with('IGNORE')
        conn.execute(stmt)
    print('loaded to prod')


def main():
    #load environment variables
    env = dotenv_values()

    MARIADB_SERVER_ADDRESS = env['MARIADB_SERVER_ADDRESS']
    MARIADB_USERNAME = env['MARIADB_USERNAME']
    MARIADB_PASSWORD = env['MARIADB_PASSWORD']

    db_engine = create_database_engine(MARIADB_USERNAME, MARIADB_PASSWORD, MARIADB_SERVER_ADDRESS)
    db_metadata = db.MetaData()
    table_ingest = db.Table('marketHistoryDataIngest', db_metadata, autoload_with=db_engine)
    table_prod = db.Table('marketHistory', db_metadata, autoload_with=db_engine)

    filename = f'/data/ingest.csv'
    try:
        clear_ingest(db_engine)
        load_csv_to_ingest(filename, db_engine)
        load_ingest_to_prod(table_ingest, table_prod, db_engine)
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == '__main__':
    main()