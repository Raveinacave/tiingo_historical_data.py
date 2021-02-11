from tiingo import TiingoClient
from datetime import timedelta, datetime, date
import pandas as pd
from dateutil import parser
import os.path
import math


config = {}

# To reuse the same HTTP Session across API calls (and have better performance), include a session key.
config['session'] = True

# API Key for tiingo
config['api_key'] = "APIKEYHERE"

# Kline sizes dict to make csv naming clearer
kline_sizes = {"1m": "1m", "5m": "1m", "60min": "1h", "daily": "1d", "weekly": "1w"}

# Initialize
client = TiingoClient(config)

def minutes_of_new_data(symbol, data):
    if len(data) > 0:  date_time_str = str(parser.parse(data["date"].iloc[-1]))[:19]
    else: 
        date_time_str = '2000-02-12 00:00:00'
    old = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S').replace(microsecond=0, second=0, minute=0)   
    new = datetime.now().replace(microsecond=0, second=0, minute=0)
    print(old,new)
    return old, new

def get_ticker_EOD_price(ticker,startDate=None, endDate=None,fmt='json'):
        # By default, return latest EOD Composite Price for a stock ticker.
        #    On average, each feed contains 3 data sources.

        #     Supported tickers + Available Day Ranges are here:
        #     https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip

        #     Args:
        #         ticker (string): Unique identifier for stock ticker
        #         startDate (string): Start of ticker range in YYYY-MM-DD format
        #         endDate (string): End of ticker range in YYYY-MM-DD format
        #         fmt (string): 'csv' or 'json'
        #         frequency (string): Resample frequency
    frequency = "daily"
    url = client._get_url(ticker, frequency)
    params = {
        'format': fmt if fmt != "object" else 'json',  # conversion local
        'resampleFreq': frequency,
        'columns': ['date,open,high,low,close,volume,adjOpen,adjHigh,adjLow,adjClose,adjVolume,divCash,splitFactor']
    }

    if startDate:
        params['startDate'] = startDate
    if endDate:
        params['endDate'] = endDate
    
    # TODO: evaluate whether to stream CSV to cache on disk, or
    # load as array in memory, or just pass plain text
    response = client._request('GET', url, params=params)
    if fmt == "json":
        return response.json()
    elif fmt == "object":
        data = response.json()
        return [dict_to_object(item, "TickerPrice") for item in data]
    else:
        return response.content.decode("utf-8")



def get_all_tiingo(symbol, kline_size, save = True):
    filename = 'TIINGO-%s-%s-data.csv' % (symbol, kline_sizes[kline_size])
    datapath = 'data'
    filename = os.path.join(".." + os.sep, datapath + os.sep, filename)
    if os.path.isfile(filename): data_df = pd.read_csv(filename)
    else: data_df = pd.DataFrame()
    oldest_point, newest_point = minutes_of_new_data(symbol, data_df)
    print('Downloading new data available for %s' % (symbol))
    klines = client.get_ticker_price(symbol, fmt='json', startDate=oldest_point, endDate=newest_point, frequency=kline_size)
    data = pd.DataFrame(klines, columns = ['date', 'close' , 'high', 'low', 'open'])
    if len(data_df) > 0:
        temp_df = pd.DataFrame(data)
        merged_df = temp_df.merge(data_df, how='left', indicator=True)
        merged_df = merged_df[merged_df._merge == 'left_only'].iloc[:,:-1]
        data_df = pd.concat([data_df, merged_df], ignore_index=True)
    else: data_df = data
    data_df.set_index('date', inplace=True)
    if save: data_df.to_csv(filename)
    print(klines)
    print('All caught up on ' + symbol + "!" )
    return data_df

def get_all_tiingo_daily(symbol, kline_size="daily", save = True):
    filename = 'TIINGO-%s-%s-data.csv' % (symbol, kline_sizes[kline_size])
    datapath = 'data'
    filename = os.path.join(".." + os.sep, datapath + os.sep, filename)
    if os.path.isfile(filename): data_df = pd.read_csv(filename)
    else: data_df = pd.DataFrame()
    oldest_point, newest_point = minutes_of_new_data(symbol, data_df)
    print('Downloading new data available for %s' % (symbol))
    klines = get_ticker_EOD_price(symbol, fmt='json', startDate=oldest_point, endDate=newest_point)
    data = pd.DataFrame(klines, columns = ['date', 'close' , 'high', 'low', 'open'])
    if len(data_df) > 0:
        temp_df = pd.DataFrame(data)
        merged_df = temp_df.merge(data_df, how='left', indicator=True)
        merged_df = merged_df[merged_df._merge == 'left_only'].iloc[:,:-1]
        data_df = pd.concat([data_df, merged_df], ignore_index=True)
    else: data_df = data
    data_df.set_index('date', inplace=True)
    if save: data_df.to_csv(filename)
    print(klines)
    print('All caught up on ' + symbol + '!')
    return data_df

# symbols = ["AAPL","GOOGL","TSLA"]
# for symbol in symbols:
#     get_all_tiingo_daily(symbol, save = True)
#     get_all_tiingo(symbol,'60min',save=True)
def get_all_stock_tickers():
    filename = 'TIINGO-STOCKTICKERS.csv'
    datapath = 'data'
    filename = os.path.join(".." + os.sep, datapath + os.sep, filename)
    if os.path.isfile(filename): data_df = pd.read_csv(filename)
    else: data_df = pd.DataFrame()
    tickers = client.list_stock_tickers()
    data_df = pd.DataFrame(tickers, columns = ['ticker', 'exchange' , 'assetType', 'priceCurrency', 'startDate', 'endDate'])
    data_df.set_index('ticker', inplace=True)
    data_df.to_csv(filename)
    print(tickers)
    print('All stock tickers loaded into csv')
    return data_df

def get_all_etf_tickers():
    filename = 'TIINGO-ETFTICKERS.csv'
    datapath = 'data'
    filename = os.path.join(".." + os.sep, datapath + os.sep, filename)
    if os.path.isfile(filename): data_df = pd.read_csv(filename)
    else: data_df = pd.DataFrame()
    tickers = client.list_etf_tickers()
    data_df = pd.DataFrame(tickers, columns = ['ticker', 'exchange' , 'assetType', 'priceCurrency', 'startDate', 'endDate'])
    data_df.set_index('ticker', inplace=True)
    data_df.to_csv(filename)
    print(tickers)
    print('All etf tickers loaded into csv')
    return data_df

def get_all_fund_tickers():
    filename = 'TIINGO-FUNDTICKERS.csv'
    datapath = 'data'
    filename = os.path.join(".." + os.sep, datapath + os.sep, filename)
    data_df = pd.DataFrame()
    tickers = client.list_fund_tickers()
    data_df = pd.DataFrame(tickers, columns = ['ticker', 'exchange' , 'assetType', 'priceCurrency', 'startDate', 'endDate'])
    data_df.set_index('ticker', inplace=True)
    data_df.to_csv(filename)
    print(tickers)
    print('All fund tickers loaded into csv')
    return data_df
