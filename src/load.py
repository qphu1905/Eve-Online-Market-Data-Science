import pandas as pd
import sqlalchemy as db
from dotenv import dotenv_values
from urllib.parse import quote_plus
import datetime


def create_database_engine(username, password, server_address) -> db.engine.Engine:
    """Create mariadb database engine
    :parameter: None
    :return: db_engine: sqlalchemy.engine.Engine
    """

    db_engine = db.create_engine(f'mariadb+mariadbconnector://{username}:%s@{server_address}/eve_online_database' % quote_plus(password))
    return db_engine


def clear_ingest(db_engine: db.engine.Engine, ingest):
    """Delete all entry in temporary ingest table"""

    with db_engine.connect() as connection:
        stmt = ingest.delete()
        connection.execute(stmt)


def load_csv_to_ingest(filename: str, db_engine: db.engine.Engine):
    """Load market data in csv file into temporary ingest table"""

    names= ['date', 'regionID', 'typeID', 'average', 'highest', 'lowest', 'orderCount', 'volume', '1dLaggedReturn', 'highLowRatio', '5dPriceMovingAverage', '20dPriceMovingAverage', '50dPriceMovingAverage', '5dVolumeMovingAverage', '20dVolumeMovingAverage', '50dVolumeMovingAverage', '20dDonchianHigh', '20dDonchianLow', '55dDonchianHigh', '55dDonchianLow']
    with pd.read_csv(filename, header=None, index_col=False, names=names, chunksize=50000) as reader:
        for df in reader:
            df.to_sql('marketHistoryDataIngest', con=db_engine, if_exists='append', index=False)


def load_ingest_to_prod(db_engine: db.engine.Engine, ingest, prod):
    """Load data from ingest table into prod table
       Ignore duplicate"""

    ingest_columns = [column.name for column in ingest.c]
    with db_engine.begin() as conn:
        stmt = db.insert(prod).from_select(ingest_columns, db.select(ingest)).prefix_with('IGNORE')
        conn.execute(stmt)


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

    filename = f'/data/marketHistory_{datetime.date.today()}.csv'
    clear_ingest(db_engine, table_ingest)
    load_csv_to_ingest(filename, db_engine)
    load_ingest_to_prod(db_engine, table_ingest, table_prod)


if __name__ == '__main__':
    main()