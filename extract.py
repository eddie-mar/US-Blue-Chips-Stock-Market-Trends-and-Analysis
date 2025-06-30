import argparse
import os

import pandas as pd
import pyspark
import requests

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import year

load_dotenv()
API_KEY = os.environ.get('ALPHA_VANTAGE_KEY')

def blue_chips_stocks(file):
    blue_chips = {}

    with open(file, 'r') as f:
        for line in f:
            kv = line.rstrip().split('\t', 1)   # 'NVDA NVIDIA Corporation' -> split using \t, one splitting only
            blue_chips[kv[0]] = kv[1]
    
    return blue_chips

def stock_data_csv(blue_chips, API_KEY):
    BASE_URL = 'https://www.alphavantage.co/query?'
    function = 'TIME_SERIES_DAILY'
    output_size = 'full'    # compact if 100 data points only
    columns = ['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']

    # initiate csv file
    df = pd.DataFrame(columns=columns)
    df.to_csv('us_blue_chips.csv', index=False)

    # list failed stocks api call
    skipped = []
    i = 0

    for symbol in blue_chips.keys():
        if i == 25:     # top 25 blue chips
            break
        
        try:
            response = requests.get(f'{BASE_URL}function={function}&symbol={symbol}&outputsize={output_size}&apikey={API_KEY}')
            response = response.json()['Time Series (Daily)']
            print(f'Extracted data for {symbol}')
        except Exception as e:
            print(f'Response error for {symbol}:\n{e}')
            skipped.append(symbol)
            continue

        df = pd.DataFrame.from_dict(response, orient='index')
        df = df.reset_index()
        df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.astype({'Open':'float64', 'High':'float64', 'Low':'float64', 'Close':'float64', 'Volume':'int64'})
        df.insert(1, 'Symbol', symbol)
        df.to_csv('us_blue_chips.csv', header=False, index=False, mode='a')     # append data
        i += 1
        print(f'Appended {symbol} data to csv file')

    return 'us_blue_chips.csv'


def finalize_data(stock_csv_file, start, end, repartition):
    if not repartition:
        df = pd.read_csv(stock_csv_file)
        df['Date'] = pd.to_datetime(df['Date'])
        df = df[(df['Date'].dt.year >= start) & (df['Date'].dt.year <= end)]
        df = df.reset_index()
        df.to_csv('us_blue_chips.csv', index=False, columns=['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume'])

    else:
        # not really necessary to repartition since small file only. I just did so I can practice pyspark
        spark = SparkSession.builder.master('local[*]').appName('test').getOrCreate()
        df_spark = spark.read.option('header', 'true').option('inferschema', 'true').csv(stock_csv_file)
        df_spark = df_spark.filter((year(df_spark['Date']) >= start) & (year(df_spark['Date']) <= end))
        df_spark = df_spark.repartition(4)
        df_spark.write.parquet('stocks/data_raw/', mode='overwrite')
        print('Data repartitioned into parquet files. Located at ./stocks/data_raw/')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract US Blue Chips Stock data')

    if API_KEY is None:
        parser.add_argument('--api_key', required=True, help='Alpha Vantage API key. Get free at https://www.alphavantage.co/support/#api-key')
    parser.add_argument('--blue_chips_lst', required=True, help='Text file containing blue chips list')
    parser.add_argument('--repartition', required=False, default=True, help='To repartition data or just return csv file')
    parser.add_argument('--start_year', required=False, default=2000, help='Extract data starting year')
    parser.add_argument('--end_year', required=False, default=2024, help='Extract data until year')

    args = parser.parse_args()
    
    blue_chips_lst = os.path.join(os.getcwd(), args.blue_chips_lst)
    if not os.path.exists(blue_chips_lst):
        raise Exception('Missing blue chips list text file')
    
    blue_chips = blue_chips_stocks(args.blue_chips_lst)
    
    if API_KEY is None:
        key = args.api_key
    else:
        key = API_KEY

    csv_file = stock_data_csv(blue_chips, key)
    print('CSV file containing US Blue Chips stocks generated')

    finalize_data(csv_file, args.start_year, args.end_year, args.repartition)
    print('Extraction completed')
    

