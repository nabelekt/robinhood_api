RH_DATA_JSON_FILE_PATH_STOCKS = "robinhood_stock_positions.json"
RH_DATA_JSON_FILE_PATH_CRYPTO = "robinhood_crypto_positions.json"


import sys
import robin_stocks
import json
import pandas as pd
import functools
print = functools.partial(print, flush=True)  # Prevent print statements from buffering till end of execution
import argparse

# Local modules and files:
import robinhood_fetch as rh_fetch


def process_stock_positions_data(stock_positions_dicts, get_quotes=False):
  
    df = pd.DataFrame(stock_positions_dicts, index=None)
    df = df.transpose()
    
    df['type']  = 'stock'
    
    if get_quotes:
        print("\nGetting stock quotes from Robinhood. This may take a few minutes... ", end="")
        df['quote'] = df.apply(lambda row: get_stock_quote(row), axis=1)
        print("Done.")
    else:
        df['quote'] = '?'

    df.index.name = 'ticker'

    df = sort_by(df, 'name')

    return df


def get_stock_quote(row):

    quote = robin_stocks.stocks.get_stock_quote_by_symbol(row.name)['last_trade_price']
    
    return quote


def process_crypto_positions_data(crypto_positions_dicts, get_quotes=False):

    df = pd.DataFrame(crypto_positions_dicts, index=None)

    symbols = []
    names = []
    for currency_dict in df['currency']:
      symbols.append(currency_dict['code'])
      names.append(currency_dict['name'])

    # Setup columns
    df.drop(df.columns.difference(['quantity']), 1, inplace=True)
    df['name']     = names
    df['type']     = 'crypto'
    df['ticker']   = symbols  # Changed later, but this is used in get_crypto_quote() below
    df = df[df['ticker'] != 'USD']  # Drop 'USD'/'USDUSDT' ticker from list of crypto positions
    if get_quotes:
        print("\nGetting crypto quotes from Robinhood... ", end="")
        df['quote']    = df.apply(lambda row: get_crypto_quote(row), axis=1)  # Used in get_crypto_equity() below
        print("Done.")
        df['quote']    = df['quote'].astype('float')
    else:
        df['quote'] = '?'
    df['quantity'] = df['quantity'].astype('float')
    df['equity']   = df.apply(lambda row: get_crypto_equity(row), axis=1)
    df['ticker']   = [symbol + 'USDT' for symbol in df['ticker']]

    df = df[['ticker', 'name', 'quantity', 'quote', 'equity', 'type']]  # Rearrange columns
    df.set_index('ticker', inplace=True)
    df.sort_values('name', inplace=True)

    return df


def remove_USDT_from_crypto_ticker(row):

  if row['type'] == 'crypto':
      row['ticker'] = row['ticker'][:-4]  # Remove 'USDT'

  return row


def get_crypto_quote(row):

    return robin_stocks.crypto.get_crypto_quote(row['ticker'], 'ask_price')


def get_crypto_equity(row):

    if row['quote'] != '?':
        equity = row['quantity'] * row['quote']
    else:
        equity = '?'

    return equity


def process_stock_dividends_data(stock_dividends_dicts):
  
    df = pd.DataFrame(stock_dividends_dicts, index=None)

    df = sort_by(df, 'paid_at')

    return df


def process_positions_data(stock_positions_dicts=None, crypto_positions_dicts=None, get_quotes=False):

    if stock_positions_dicts is None:
        stock_positions_dicts  = get_dicts_from_json_file(RH_DATA_JSON_FILE_PATH_STOCKS)
    stock_positions_df = process_stock_positions_data(stock_positions_dicts, get_quotes)
    stock_positions_df = prep_stock_positions_df_for_compare(stock_positions_df)

    if crypto_positions_dicts is None:
        crypto_positions_dicts = get_dicts_from_json_file(RH_DATA_JSON_FILE_PATH_CRYPTO)
    crypto_positions_df = process_crypto_positions_data(crypto_positions_dicts, get_quotes)

    positions_df = pd.concat([stock_positions_df, crypto_positions_df])

    return positions_df


def prep_stock_positions_df_for_compare(df):

    columns_to_keep_in_order = ['name', 'quantity', 'equity', 'quote', 'type']

    df.drop(df.columns.difference(columns_to_keep_in_order), 1, inplace=True)
    df = sort_by(df, 'ticker')
    df = df[columns_to_keep_in_order]  # Rearrange columns
    df['equity'] = df['equity'].replace(r'[\$,]', '', regex=True).astype(float)  # Convert currency strings to float values
    df['quantity'] = df['quantity'].astype(float)  # Convert strings to float values

    return df


def prep_stock_positions_df_for_output(df):

    columns_to_keep_in_order = ['name', 'quantity', 'equity', 'quote', 'percentage']

    df.drop(df.columns.difference(columns_to_keep_in_order), 1, inplace=True)  # Drop unwanted columns
    df = df[columns_to_keep_in_order]  # Rearrange columns
    df['percentage'] = df['percentage'].astype(float)  # Convert string values to float values
    df = sort_by(df, 'percentage', ascending=False)
    df['equity'] = df['equity'].replace(r'[\$,]', '', regex=True).astype(float)  # Convert currency strings to float values

    return df


def prep_stock_dividends_df_for_output(df):

    columns_to_keep_in_order = ['paid_at', 'record_date', 'symbol', 'amount', 'position', 'rate', 'withholding', 'drip_enabled', 'nra_withholding']

    df.drop(df.columns.difference(columns_to_keep_in_order), 1, inplace=True)  # Drop unwanted columns
    df = df[columns_to_keep_in_order]  # Rearrange columns

    return df


def sort_by(df, column_label, ascending=True):
    
    if ascending:
        df = df.sort_values(column_label, inplace=False, ascending=True)
    else:
        df = df.sort_values(column_label, inplace=False, ascending=False)
    
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
    stock_positions_df = prep_stock_positions_df_for_output(stock_positions_df)

    print(f"Writing to {output_file_path} file... ", end="")
    stock_positions_df.to_csv(output_file_path, index=False)
    print("Done.")


def write_stock_dividends_to_csv_file(output_file_path):

    stock_dividends_dicts = rh_fetch.get_stock_dividends_dicts()
    stock_dividends_df = process_stock_dividends_data(stock_dividends_dicts)
    stock_dividends_df = prep_stock_dividends_df_for_output(stock_dividends_df)

    print(f"Writing to {output_file_path} file... ", end="")
    stock_dividends_df.to_csv(output_file_path, index=False)
    print("Done.")


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

  parser = argparse.ArgumentParser(description='Output CSV file(s) with Robinhood position and/or dividend information.')
  parser.add_argument('--stock_pos_csv_path', '--sp')
  parser.add_argument('--stock_div_csv_path', '--sd')
  args = parser.parse_args()

  if (not args.stock_pos_csv_path and not args.stock_div_csv_path):
    print(f"No output specified.\n")
    parser.print_help()
    sys.exit(f"\nExiting.\n")

  return args


if __name__ == '__main__':

  print()

  args = parse_and_check_input()

  rh_fetch.login()

  if (args.stock_pos_csv_path):
    write_stock_positions_to_csv_file(args.stock_pos_csv_path)
  
  if (args.stock_div_csv_path):
    write_stock_dividends_to_csv_file(args.stock_div_csv_path)

  print("\nDone.\nExiting.\n")

