import concurrent.futures
import pickle
import sys
import traceback
from asyncio import CancelledError
from datetime import datetime
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
import sqlalchemy as db
from dotenv import dotenv_values
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import precision_score

env = dotenv_values()

MARIADB_USERNAME = env['MARIADB_USERNAME']
MARIADB_PASSWORD = env['MARIADB_PASSWORD']
MARIADB_SERVER_ADDRESS = env['MARIADB_SERVER_ADDRESS']

database_credentials = (MARIADB_USERNAME, MARIADB_PASSWORD, MARIADB_SERVER_ADDRESS)


def create_database_engine(credentials) -> db.engine.Engine:
    """Create mariadb database engine
    :parameter: None
    :return: db_engine: sqlalchemy.engine.Engine
    """

    username = credentials[0]
    password = credentials[1]
    server_address = credentials[2]

    db_engine = db.create_engine(f'mariadb+mariadbconnector://{username}:%s@{server_address}/eve_online_database' % quote_plus(password))
    return db_engine


def create_predictors(horizon):
    horizons = [2, 5, 10, 20, 50, 100]

    horizons_to_extract = [i for i in horizons if i <= horizon][-2:]

    predictors = []
    for horizon in horizons_to_extract:
        predictors.append(f'{horizon}dLaggedReturn')
        predictors.append(f'{horizon}dMomentum')
        predictors.append(f'{horizon}dPriceMovingAverage')
        predictors.append(f'{horizon}dPriceMovingAverageStd')
        predictors.append(f'{horizon}dHighestMovingAverage')
        predictors.append(f'{horizon}dLowestMovingAverage')
        predictors.append(f'{horizon}dRelativePriceStrength')
        predictors.append(f'{horizon}dVolumeMovingAverage')
        predictors.append(f'{horizon}dRelativeVolumeStrength')

    return predictors


def fetch_training_data(region_id, type_id, predictors, horizon, db_engine):

    info_log_file = f"logs/{region_id}/{horizon}/log_info.txt"
    success_log_file = f"logs/{region_id}/{horizon}/log_success.txt"
    errors_log_file = f"logs/{region_id}/{horizon}/log_errors.txt"

    columns_list = predictors.copy()
    columns_list.extend(['date', 'average'])
    conditions_list = [f'regionID = {region_id} ', f'typeID = {type_id}']

    query = "SELECT {columns} FROM marketHistory WHERE {conditions}".format(columns=', '.join(columns_list), conditions=' AND '.join(conditions_list))

    with open(info_log_file, 'a') as log:
        log.write(f'<{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}> Fetching training data for item {type_id} from region {region_id}.\n')

    try:
        with db_engine.begin() as con:
            stmt = db.text(query)
            result = con.execute(stmt)
        with open(info_log_file, 'a') as log:
            log.write(f'<{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}> Training data for item {type_id} from region {region_id} fetched from database.\n')

    except Exception as e:
        with open(errors_log_file, 'a') as log:
            log.write(f'<{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}> Fetching of training data for item {type_id} from region {region_id} failed with error {e}.\n')
            print(traceback.format_exc())
            con.rollback()
            con.close()
            sys.exit(1)

    if result:
        df = pd.DataFrame(result)
        if len(df) >= 365:

            next_window_price = f'next_{horizon}d_price'
            next_window_target= f'target_next_{horizon}d_price'
            df[next_window_price] = df['average'].shift(-horizon)
            df[next_window_target] = (df[next_window_price] > df['average']).astype(int)

            df.set_index('date', inplace=True)
            df.dropna(inplace=True)
            with open(info_log_file, 'a') as log:
                log.write(f'<{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}> Training data for item {type_id} from region {region_id} ready for training.\n')
            return df

        else:
            with open(info_log_file, 'a') as log:
                log.write(f'<{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}> Training data for item {type_id} from region {region_id} insufficient.\n')
            return pd.DataFrame([])


def query_type_id(db_engine, table_itemsDim):
    with db_engine.begin() as con:
        stmt = db.select(table_itemsDim.c.typeID)
        result = con.execute(stmt)
    return [row.typeID for row in result]


def predict(train, test, predictors, model, horizon):
    model.fit(train[predictors], train[f'target_next_{horizon}d_price'])
    predictions = model.predict_proba(test[predictors])[:,1]
    predictions = np.where(predictions >= 0.7, 1, 0)
    predictions = pd.Series(predictions, index=test.index, name=f"prediction_next_{horizon}d")
    combined = pd.concat([test[f'target_next_{horizon}d_price'], predictions], axis=1)
    return combined


def backtest(data: pd.DataFrame, model, predictors, horizon, start=180, step=30):
    all_predictions = []

    for i in range(start, len(data), step):
        train = data.iloc[0:i]
        test = data.iloc[i:i+step]
        predictions = predict(train, test, predictors, model, horizon)
        all_predictions.append(predictions)
    return pd.concat(all_predictions)


def train_model(predictors, horizon, region_id, type_id):

    info_log_file = f"logs/{region_id}/{horizon}/log_info.txt"
    success_log_file = f"logs/{region_id}/{horizon}/log_success.txt"
    errors_log_file = f"logs/{region_id}/{horizon}/log_errors.txt"

    model_output = f"models/{region_id}/{horizon}/{region_id}_{type_id}_{horizon}d.pkl"

    model = RandomForestClassifier(n_estimators=200, min_samples_split=50, random_state=0)

    try:
        db_engine = create_database_engine(database_credentials)
        data = fetch_training_data(region_id, type_id, predictors, horizon, db_engine)
        db_engine.dispose()

        with open(info_log_file, "a") as log:
            log.write(f"<{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}> Training model for {type_id} from region {region_id}.\n")

        if not data.empty:

            predictions = backtest(data, model, predictors, horizon, start=180, step=horizon)
            precision = precision_score(predictions[f'target_next_{horizon}d_price'], predictions[f'prediction_next_{horizon}d'])
            if precision > 0.5:
                with open(model_output, 'wb') as file:
                    pickle.dump(model, file)

                with open(success_log_file, "a") as log:
                    log.write(f"<{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}> Model for {type_id} from region {region_id} with horizon {horizon}d trained successfully with precision score {precision}.\n")

            elif precision <= 0.5:
                with open(success_log_file, "a") as log:
                    log.write(f"<{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}> Model for {type_id} from region {region_id} with horizon {horizon}d trained successfully but was not saved due to low precision score: {precision}.\n")

        elif data.empty:
            with open(success_log_file, "a") as log:
                log.write(f"<{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}> Model for {type_id} from region {region_id} with horizon {horizon}d has not been trained due to insufficient data.\n")

    except Exception as e:
        with open(errors_log_file, "a") as log:
            log.write(f"<{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}> Error training model for {type_id} from region {region_id} with horizon {horizon}d with error: {e}.\n")
            print(traceback.format_exc())
            sys.exit(1)


def main():

    db_engine = create_database_engine(database_credentials)
    db_metadata = db.MetaData()
    table_items_dim = db.Table('itemsDim', db_metadata, autoload_with=db_engine)
    type_ids = query_type_id(db_engine, table_items_dim)
    # type_ids = [34]
    region_ids = [10000002, 10000030, 10000032, 10000042, 10000043, 10000061, 10000066]
    region_ids = [10000002]

    horizons = [2, 5, 10, 20, 50, 100]

    with concurrent.futures.ProcessPoolExecutor(max_workers=16, max_tasks_per_child=1) as executor:
            for region_id in region_ids:
                for type_id in type_ids:
                    for horizon in horizons:
                        predictor_list = create_predictors(horizon)
                        executor.submit(train_model, predictor_list, horizon, region_id, type_id)


if __name__ == "__main__":
    main()