import pandas as pd
import datetime

def read_csv(filename: str) -> pd.DataFrame:
    df = pd.read_csv(filename, header=None, names=['average', 'date', 'highest', 'lowest', 'orderCount', 'volume', 'regionID', 'typeID'])
    df = df[['date', 'regionID', 'typeID', 'average', 'highest', 'lowest', 'orderCount', 'volume']]
    df.sort_values(by=['regionID', 'typeID', 'date',], inplace=True)
    return df


def get_last_56_days_data(df: pd.DataFrame) -> pd.DataFrame:
    date_56_days_ago = (pd.to_datetime('today') - datetime.timedelta(days=56)).strftime('%Y-%m-%d')
    df = df[df['date'] >= date_56_days_ago]
    print(df)
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    grouped_df = df.groupby(['regionID', 'typeID'])
    df['5dMovingAverage'] = grouped_df['average'].transform(lambda x: x.rolling(window=5, min_periods=1).mean().round(2))
    df['20dMovingAverage'] = grouped_df['average'].transform(lambda x: x.rolling(window=20, min_periods=1).mean().round(2))
    df['50dMovingAverage'] = grouped_df['average'].transform(lambda x: x.rolling(window=50, min_periods=1).mean().round(2))
    df['20dDonchianHigh'] = grouped_df['highest'].transform(lambda x: x.rolling(window=20, min_periods=1).max().round(2))
    df['20dDonchianLow'] = grouped_df['lowest'].transform(lambda x: x.rolling(window=20, min_periods=1).min().round(2))
    df['55dDonchianHigh'] = grouped_df['highest'].transform(lambda x: x.rolling(window=55, min_periods=1).max().round(2))
    df['55dDonchianLow'] = grouped_df['lowest'].transform(lambda x: x.rolling(window=55, min_periods=1).min().round(2))
    df = df[df['date'] == (pd.to_datetime('today') - datetime.timedelta(days=1)).strftime('%Y-%m-%d')]
    print(df)
    return df

if __name__ == '__main__':
    filename = f'marketHistory_{datetime.date.today()}.csv'
    df = read_csv(filename)
    df = get_last_56_days_data(df)
    df = transform(df)
    df.to_csv(filename, mode='w', index=False, header=False)
