import pandas as pd
import numpy as np
import datetime


def read_csv(filename: str) -> pd.DataFrame:
    """Read data from csv file"""

    df = pd.read_csv(filename, header=None, names=['average', 'date', 'highest', 'lowest', 'orderCount', 'volume', 'regionID', 'typeID'])
    df = df[['date', 'regionID', 'typeID', 'average', 'highest', 'lowest', 'orderCount', 'volume']]
    df = df.astype({"date": str, "regionID": int, "typeID": int, "average": float, "highest": float, "lowest": float, "orderCount": int, "volume": int})
    df.sort_values(by=['regionID', 'typeID', 'date',], inplace=True)
    return df


def get_56_days_data(df: pd.DataFrame) -> pd.DataFrame:
    """Filter data from only the last 56 days to perform transformation"""

    start_date = (datetime.date.today() - datetime.timedelta(days=56)).strftime("%Y-%m-%d")
    df = df[df['date'] >= start_date]
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Perform transformation on data"""

    grouped_df = df.groupby(['regionID', 'typeID'], group_keys=False)

    df['1dReturn'] = grouped_df['average'].transform(lambda x: np.log(x / x.shift(1)))
    df['highLowRatio'] = grouped_df[['highest', 'lowest']].apply(lambda x: x.highest / x.lowest)

    df['5dMovingAverage'] = grouped_df['average'].transform(lambda x: x.rolling(window=5, min_periods=1).mean().round(2))
    df['20dMovingAverage'] = grouped_df['average'].transform(lambda x: x.rolling(window=20, min_periods=1).mean().round(2))
    df['50dMovingAverage'] = grouped_df['average'].transform(lambda x: x.rolling(window=50, min_periods=1).mean().round(2))

    df['5dVolumeMovingAverage'] = grouped_df['volume'].transform(lambda x: x.rolling(window=5, min_periods=1).mean().round(2))
    df['20dVolumeMovingAverage'] = grouped_df['volume'].transform(lambda x: x.rolling(window=20, min_periods=1).mean().round(2))
    df['50dVolumeMovingAverage'] = grouped_df['volume'].transform(lambda x: x.rolling(window=50, min_periods=1).mean().round(2))

    df['20dDonchianHigh'] = grouped_df['highest'].transform(lambda x: x.rolling(window=20, min_periods=1).max().round(2))
    df['20dDonchianLow'] = grouped_df['lowest'].transform(lambda x: x.rolling(window=20, min_periods=1).min().round(2))
    df['55dDonchianHigh'] = grouped_df['highest'].transform(lambda x: x.rolling(window=55, min_periods=1).max().round(2))
    df['55dDonchianLow'] = grouped_df['lowest'].transform(lambda x: x.rolling(window=55, min_periods=1).min().round(2))

    df.dropna(inplace=True)
    return df


def main():
    filename = f'/data/marketHistory_{datetime.date.today()}.csv'
    df = read_csv(filename)
    df = get_56_days_data(df)
    df = transform(df)
    df.to_csv(filename, mode='w', index=False, header=False)


if __name__ == '__main__':
    main()