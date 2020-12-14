INPUT_FILE_PATH_STOCKS = "robinhood_stock_positions.json"
INPUT_FILE_PATH_CRYPTO = "robinhood_crypto_positions.json"


import pyotp
import robin_stocks
import json
import pandas as pd
import functools
print = functools.partial(print, flush=True)  # Prevent print statements from buffering till end of execution


def get_dicts_from_json_file(data_file_path):
  
  data_file = open(data_file_path, "r")
  data_dicts = json.load(data_file)
  data_file.close()

  return data_dicts


def json_to_dict(json_input):
  
  dicts = json.loads(json_input)

  return dicts


def process_stock_positions_data(stock_positions_dicts):
  
    df = pd.DataFrame(stock_positions_dicts, index=None)
    df = df.transpose()
    
    df['type'] = 'stock'

    df.index.name = 'ticker'

    df.sort_values('name', inplace=True)

    return df


def process_crypto_positions_data(crypto_positions_dicts):

    df = pd.DataFrame(crypto_positions_dicts, index=None)

    symbols = []
    names = []
    for currency_dict in df['currency']:
      symbols.append(currency_dict['code'])
      names.append(currency_dict['name'])

    # Setup columns
    df.drop(df.columns.difference(['quantity']), 1, inplace=True)
    df['ticker'] = [symbol + 'USDT' for symbol in symbols]
    df['name'] = names
    df['equity'] = '?'
    df['type'] = 'crypto'

    df = df[['ticker', 'name', 'quantity', 'equity', 'type']]  # Rearrange columns

    df.set_index('ticker', inplace=True)
    df.sort_values('name', inplace=True)
    df.drop('USDUSDT', inplace=True)

    return df