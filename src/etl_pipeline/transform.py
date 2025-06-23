import sys
import traceback

import pandas as pd
import numpy as np
import datetime


def read_csv(filename: str) -> pd.DataFrame:
    """Read data from csv file"""
    names = ['average', 'date', 'highest', 'lowest', 'orderCount', 'volume', 'regionID', 'typeID']
    new_names = ['date', 'regionID', 'typeID', 'average', 'highest', 'lowest', 'orderCount', 'volume']

    df = pd.read_csv(filename, header=None, names=names)
    df = df[new_names]
    df = df.astype({"date": str,
                    "regionID": int,
                    "typeID": int,
                    "average": float,
                    "highest": float,
                    "lowest": float,
                    "orderCount": int,
                    "volume": int})
    df.sort_values(by=['regionID', 'typeID', 'date',], inplace=True)

    return df


def get_period_data(df: pd.DataFrame, period: int=100) -> pd.DataFrame:
    """Filter data from only the last 56 days to perform transformation"""

    start_date = (datetime.date.today() - datetime.timedelta(days=period)).strftime("%Y-%m-%d")
    df = df[df['date'] >= start_date]
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Perform transformation on data"""

    grouped_df = df.groupby(['regionID', 'typeID'], group_keys=False)

    windows = [2, 5, 10, 20, 50, 100]

    for window in windows:
        lagged_return = f'{window}dLaggedReturn'
        momentum = f'{window}dMomentum'
        price_moving_average = f'{window}dPriceMovingAverage'
        price_moving_average_std = f'{window}dPriceMovingAverageStd'
        highest_moving_average = f'{window}dHighestMovingAverage'
        lowest_moving_average = f'{window}dLowestMovingAverage'
        relative_price_strength = f'{window}dRelativePriceStrength'

        volume_moving_average = f'{window}dVolumeMovingAverage'
        relative_volume_strength = f'{window}dRelativeVolumeStrength'

        df[lagged_return] = grouped_df['average'].transform(lambda x: np.log(x / x.shift(window)))
        df[momentum] = grouped_df['average'].transform(lambda x: x - x.shift(window))

        df[price_moving_average] = grouped_df['average'].transform(lambda x: x.rolling(window=window).mean().round(5))
        df[price_moving_average_std] = grouped_df['average'].transform(lambda x: x.rolling(window=window).std().round(5))
        df[highest_moving_average] = grouped_df['highest'].transform(lambda x: x.rolling(window=window).max().round(5))
        df[lowest_moving_average] = grouped_df['lowest'].transform(lambda x: x.rolling(window=window).min().round(5))
        df[relative_price_strength] = grouped_df[['average', price_moving_average]].apply(lambda x: x.average / x[price_moving_average]).round(5)

        df[volume_moving_average] = grouped_df['volume'].transform(lambda x: x.rolling(window=window).mean().round(5))
        df[relative_volume_strength] = grouped_df[['volume', volume_moving_average]].apply(lambda x: x.average / x[volume_moving_average]).round(5)

    df.dropna(inplace=True)
    return df


def main():
    filename = f'/data/ingest.csv'
    try:
        df = read_csv(filename)
        df = get_period_data(df)
        df = transform(df)
        df.to_csv(filename, mode='w', index=False, header=False)
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)


if __name__ == '__main__':
    main()