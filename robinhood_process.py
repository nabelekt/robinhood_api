RH_DATA_JSON_FILE_PATH_STOCKS = "robinhood_stock_positions.json"
RH_DATA_JSON_FILE_PATH_CRYPTO = "robinhood_crypto_positions.json"


import robinhood_fetch as rh_fetch
import json
import pandas as pd
import functools
print = functools.partial(print, flush=True)  # Prevent print statements from buffering till end of execution
import argparse


def process_stock_positions_data(stock_positions_dicts):
  
    df = pd.DataFrame(stock_positions_dicts, index=None)
    df = df.transpose()
    
    df['type'] = 'stock'

    df.index.name = 'ticker'

    df = sort_by(df, 'name')

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


def process_positions_data(stock_positions_dicts=None, crypto_positions_dicts=None):

    if stock_positions_dicts is None:
        stock_positions_dicts  = get_dicts_from_json_file(RH_DATA_JSON_FILE_PATH_STOCKS)
    stock_positions_df = process_stock_positions_data(stock_positions_dicts)
    stock_positions_df = prep_stock_df_for_compare(stock_positions_df)

    if crypto_positions_dicts is None:
        crypto_positions_dicts = get_dicts_from_json_file(RH_DATA_JSON_FILE_PATH_CRYPTO)
    crypto_positions_df = process_crypto_positions_data(crypto_positions_dicts)

    positions_df = pd.concat([stock_positions_df, crypto_positions_df])

    return positions_df


def prep_stock_df_for_compare(df):

    df.drop(df.columns.difference(['name', 'quantity', 'equity', 'type']), 1, inplace=True)
    df = sort_by(df, 'ticker')
    df = df[['name', 'quantity', 'equity', 'type']]  # Rearrange columns
    df['equity'] = df['equity'].replace('[\$,]', '', regex=True).astype(float)  # Convert currency strings to float values

    return df


def prep_stock_df_for_output(df):

    df.drop(df.columns.difference(['name', 'quantity', 'equity', 'percentage']), 1, inplace=True)
    df = df[['name', 'quantity', 'equity', 'percentage']]  # Rearrange columns
    df['percentage'] = df['percentage'].astype(float)  # Convert string values to float values
    df = sort_by(df, 'percentage', ascending=False)
    df['equity'] = df['equity'].replace('[\$,]', '', regex=True).astype(float)  # Convert currency strings to float values

    return df


def sort_by(df, column_label, ascending=True):
    
    if ascending:
        df.sort_values(column_label, inplace=True, ascending=True)
    else:
        df.sort_values(column_label, inplace=True, ascending=False)
    
    return df


def write_stock_positions_to_json_file(output_file_path):

    stock_positions = rh_fetch.get_stock_positions_dicts()
    write_to_json_file(stock_positions,  output_file_path)


def write_crypto_positions_to_json_file(output_file_path):

    crypto_positions = rh_fetch.get_crypto_positions_dicts()
    write_to_json_file(crypto_positions, output_file_path)


def write_stock_positions_to_csv_file(output_file_path):

    stock_positions_dicts = rh_fetch.get_stock_positions_dicts()
    stock_positions_df = process_stock_positions_data(stock_positions_dicts)
    stock_positions_df = prep_stock_df_for_output(stock_positions_df)

    print(f"Writing to {output_file_path} file... ", end="")
    stock_positions_df.to_csv(output_file_path)
    print("Done.")

    # write_to_json_file(stock_positions,  output_file_path)


def write_to_json_file(data_to_write, output_file_path):
    print(f"Writing to {output_file_path} file... ", end="")
    output_file = open(output_file_path, "w")
    output_file.write(json.dumps(data_to_write))
    output_file.close()
    print("Done.")


def get_dicts_from_json_file(data_file_path):
  
  data_file = open(data_file_path, "r")
  data_dicts = json.load(data_file)
  data_file.close()

  return data_dicts


def json_to_dict(json_input):
  
  dicts = json.loads(json_input)

  return dicts


def parse_and_check_input():

  parser = argparse.ArgumentParser(description='Output CSV file with Robinhood position information.')
  parser.add_argument('csv_output_file_path')
  args = parser.parse_args()

  return args


if __name__ == '__main__':

  print()

  args = parse_and_check_input()

  rh_fetch.login()

  write_stock_positions_to_csv_file(args.csv_output_file_path)

  print("\nExiting.\n")

