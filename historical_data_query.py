import requests
import pandas as pd
import numpy as np
from datetime import date
import math

ADDRESS = 'https://ftx.com/api/markets/'
MAX_BARS = 5000

def historical_data_query(market, start_date, end_date, resolution):
    """
    Queries the historical data for any instrument listed on FTX

    Parameters
    ----------
    market : 
    start_date :
    end_date :
    resolution :

    Return
    ---------
    df : DataFrame containing OHLC of instrument queried on FTX.
    """
    end_date = pd.Timestamp(end_date).timestamp()
    start_date = pd.Timestamp(start_date).timestamp()
    
    # Constants
    bars_to_query = (end_date - start_date) / resolution
    number_of_queries = math.floor(bars_to_query / MAX_BARS) + 1
    final_query = bars_to_query % MAX_BARS
    
    # Initialization
    i = number_of_queries

    if (bars_to_query > 5000):
        print('Total of {} lines to be loaded..'.format(int(bars_to_query)))
        while (number_of_queries) > 0:
            if (i == number_of_queries):
                response = requests.get(ADDRESS + '{}/candles?resolution={}&end_time={}&limit={}'.format(
                    market, 
                    resolution,
                    int(end_date),
                    MAX_BARS))
                df = pd.DataFrame.from_dict(response.json()['result'])
                end_date = df['time'][0]
                print('Loaded total of {} lines of {} historical data (resolution = {}s)..'.format(len(df), market, resolution))
            elif number_of_queries > 1:
                response = requests.get(ADDRESS + '{}/candles?resolution={}&end_time={}&limit={}'.format(
                    market,
                    resolution,
                    int(end_date / 1000),
                    MAX_BARS))      
                df = df.append(pd.DataFrame.from_dict(response.json()['result'])).sort_values('startTime').reset_index(drop=True)
                end_date = df['time'][0]
                print('Loaded total of {} lines of {} historical data (resolution = {}s)..'.format(len(df), market, resolution))
            else: 
                response = requests.get(ADDRESS + '{}/candles?resolution={}&end_time={}&limit={}'.format(
                    market,
                    resolution,
                    int(end_date / 1000),
                    int(bars_to_query % MAX_BARS)))

                df = df.append(pd.DataFrame.from_dict(response.json()['result'])).sort_values('startTime').reset_index(drop=True)
                end_date = df['time'][0]
                print('Loaded total of {} lines of {} historical data (resolution = {}s)..'.format(len(df), market, resolution))
            number_of_queries -= 1
            if number_of_queries == 0:
                print('Finished load of {} lines of {} historical data (resolution = {}s)!'.format(len(df), market, resolution))
                break
    else:
        response = requests.get(ADDRESS + '{}/candles?resolution={}&end_time={}&limit={}'.format(
                market,
                resolution,
                int(end_date),
                int(bars_to_query)))
        df = pd.DataFrame.from_dict(response.json()['result'])
        print('Finished load of {} lines of {} historical data (resolution = {}s)!'.format(len(df), market, resolution))

    return df

if __name__ == "__main__":
    df = historical_data_query(market="BTC-PERP", start_date="2021-01-01", end_date="2021-09-26", resolution=3600)
    print(df)