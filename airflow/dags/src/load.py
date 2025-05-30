import pandas as pd
import sqlalchemy as db
from dotenv import dotenv_values
from urllib.parse import quote_plus
import datetime


def create_database_engine(username, password, server_address, ) -> db.engine.Engine:
    """Create mariadb database engine
    :parameter: None
    :return: db_engine: sqlalchemy.engine.Engine
    """

    db_engine = db.create_engine(f'mariadb+mariadbconnector://{username}:%s@{server_address}/eve_online_database' % quote_plus(password))
    return db_engine


def load(filename: str, db_engine: db.engine.Engine):
    names= ['date', 'regionID', 'typeID', 'average', 'highest', 'lowest', 'orderCount', 'volume', '5dMovingAverage', '20dMovingAverage', '50dMovingAverage', '20dDonchianHigh', '20dDonchianLow', '55dDonchianHigh', '55dDonchianLow']
    df = pd.read_csv(filename, header=None, index_col=False, names=names)
    df.to_sql('marketHistory', con=db_engine, if_exists='append', index=False)


def main():
    #load environment variables
    env = dotenv_values()

    MARIADB_SERVER_ADDRESS = env['MARIADB_SERVER_ADDRESS']
    MARIADB_USERNAME = env['MARIADB_USERNAME']
    MARIADB_PASSWORD = env['MARIADB_PASSWORD']

    db_engine = create_database_engine(MARIADB_USERNAME, MARIADB_PASSWORD, MARIADB_SERVER_ADDRESS)
    filename = f'../../data/marketHistory_{datetime.date.today()}.csv'
    load(filename, db_engine)


if __name__ == '__main__':
    main()