BT_DATA_JSON_FILE_PATH = "Investment Summary 5.csv"
# BT_FILE_PATH = "Investment Summary.csv"

SHOW_CANCELED_AND_FAILED_ORDERS = False


import pandas as pd
import io
import robinhood_process as rh_process
import robinhood_fetch   as rh_fetch
import robin_stocks
import pyotp
from datetime import datetime as dt
import dateutil.parser
import json
import print_control
from contextlib import redirect_stdout
import os


def process_banktivity_positions_data():

    input_file  = open(BT_DATA_JSON_FILE_PATH, "r")
    file_header = input_file.readlines()[7]
    input_file.seek(0)
    file_data   = input_file.readlines()[9:]
    input_file.close()

    file_data   = ''.join(file_data)
    data_str    = file_header + file_data
    data_str    = io.StringIO(data_str)

    pd.options.display.width = 0
    pd.set_option('display.max_columns', None)

    df = pd.read_csv(data_str, sep=",")

    df = df.set_index('Symbol')
    df.index.names = ['ticker']
    df.rename(columns={"Name": "name", "Close Shares": "quantity", "Close Value": "equity"}, inplace=True)

    df['type'] = '?'  # Unknown if security is stock or cryptocurrency

    df = rh_process.prep_stock_df_for_compare(df)

    return df


def compare_holdings_data(df_rh, df_bt):
    
    # Get tickers in Robinhood data and in Banktivity data
    tickers_rh = df_rh.index.tolist()
    tickers_bt = df_bt.index.tolist()
    
    # Find missing tickers
    missing_from_rh = list(set(tickers_bt).difference(tickers_rh))
    missing_from_bt = list(set(tickers_rh).difference(tickers_bt))

    missing_from_rh = df_bt.loc[missing_from_rh]
    missing_from_bt = df_rh.loc[missing_from_bt]
    
    # Keep just those rows where quantity or equity is not 0
    missing_from_rh = missing_from_rh.loc[(missing_from_rh['quantity'] != 0) | (missing_from_rh['equity'] != 0)]
    missing_from_bt = missing_from_bt.loc[(missing_from_bt['quantity'] != 0) | (missing_from_bt['equity'] != 0)]

    missing_from_rh = rh_process.sort_by(missing_from_rh, 'name')
    missing_from_bt = rh_process.sort_by(missing_from_bt, 'name')

    return [missing_from_rh, missing_from_bt]


def format_datetime_str(order_dt_str):
    
    order_dt     = dateutil.parser.isoparse(order_dt_str)
    order_dt_str = order_dt.strftime('%Y-%m-%d %H:%M:%S %Z')

    return order_dt_str


def parse_and_print_rh_order_data(ticker, order_set):
    
    if order_set == []:
        print(f'  {ticker:7s}', end="  ")
        print(f"No Robinhood order data for ticker '{ticker}'.")
        return False
    
    else:
        for order_idx, order in enumerate(order_set):
            if ticker != order['symbol']:
                print("ERROR: symbol mismatch", sys.exc_info()[0])
                raise

            for execution in order['executions']:
                
                if ((order['state'] == 'filled') or (SHOW_CANCELED_AND_FAILED_ORDERS)):
                    num_executions = len(order['executions'])

                    print(f'  {ticker:7s}', end="  ")
                    print(order['state'], end="  ")

                    side = order['side']
                    print(f'{side:4s}', end="  ")

                    if (order['state'] != 'filled'):
                        quantity = float(order['quantity'])
                    else:
                        quantity = float(execution['quantity'])
                    if order['type'] == 'stock':
                        print(f'{quantity:11,f}', end="  ")
                    else:
                        print(f'{quantity:17,.10f}', end="  ")

                    if order['state'] == 'filled':

                        if 'executed_notional' in order:  # Stock order data uses 'executed_notional: amount'
                            amount = float(order['executed_notional']['amount'])
                        else:  # Crypto order data uses 'rounded_executed_notional'
                            amount = float(order['rounded_executed_notional'])
                        print(f'{amount:8,.2f}', end="  ")
                        
                        if 'price' in execution:  # Stock order data puts price data in each execution
                            price =  float(execution['price'])
                        else:  # Crypto order data puts price with order data
                            price = float(order['price'])
                        print(f'{price:9,.3f}', end="  ")
                        
                        datetime_str = format_datetime_str(execution['timestamp'])
                        print(datetime_str, end="  ")
                        
                        print(num_executions, end="  ")
                    
                    if num_executions > 1:
                        print(" ** part of an order with multiple executions", end="")
        
                print()
        
        return True


def iterate_through_rh_orders(tickers, orders):
   
    there_was_order_data = False

    if len(orders) > 0:
        for ticker_idx, order_set in enumerate(orders):

            ticker = tickers[ticker_idx]

            this_order_set_had_data = parse_and_print_rh_order_data(ticker, order_set)

            if (this_order_set_had_data):
                there_was_order_data = True

            print()

        if (not SHOW_CANCELED_AND_FAILED_ORDERS and there_was_order_data):
            print("Only showing filled orders.")

        if (there_was_order_data):
            print("More transaction data is avaiable for orders shown.")

    else:
        print("There was no order data.")


def cleanup_bt_crypto_tickers(bt_crypto_tickers):

    for idx, ticker in enumerate(bt_crypto_tickers):
        if ticker.endswith('USDT'):
            bt_crypto_tickers[idx] = ticker[:-1]

    return bt_crypto_tickers


if __name__ == "__main__":

    print()

    rh_fetch.login()

    # Save Robinhood position data to files so that it only needs to be fetched once if script is run multiple times
    files_exist = os.path.exists(rh_process.RH_DATA_JSON_FILE_PATH_STOCKS) & os.path.exists(rh_process.RH_DATA_JSON_FILE_PATH_CRYPTO)
    fetch_new_info_input = 'n'
    if files_exist:
        fetch_new_info_input = input("Update data from Robinhood? Enter 'y' or 'n': ")
    if ((not files_exist) or (fetch_new_info_input == 'y')):
        if (fetch_new_info_input == 'y'):
            print()
        rh_process.write_stock_positions_to_json_file(rh_process.RH_DATA_JSON_FILE_PATH_STOCKS)
        rh_process.write_crypto_positions_to_json_file(rh_process.RH_DATA_JSON_FILE_PATH_CRYPTO)

    df_bt = process_banktivity_positions_data()
    df_rh = rh_process.process_positions_data()

    [missing_from_rh_df, missing_from_bt_df] = compare_holdings_data(df_rh, df_bt)
    
    stocks_are_missing_from_bt  = False
    cryptos_are_missing_from_bt = False
    if 'stock' in missing_from_bt_df.type.values:
        stocks_are_missing_from_bt  = True
    if 'crypto' in missing_from_bt_df.type.values:
        cryptos_are_missing_from_bt = True

    tickers_missing_from_bt_stock  = []
    tickers_missing_from_bt_crypto = []
    if stocks_are_missing_from_bt:
        tickers_missing_from_bt_stock = missing_from_bt_df[missing_from_bt_df['type'] == 'stock'].index.tolist()
    if cryptos_are_missing_from_bt:
        tickers_missing_from_bt_crypto = missing_from_bt_df[missing_from_bt_df['type'] == 'crypto'].index.tolist()
    # tickers_missing_from_bt_stock  = ['AAPL', 'TSLA', 'SPCE']  # For testing, give tickers here
    # tickers_missing_from_bt_crypto = ['BTCUSD', 'TEST', 'ETHUSD', 'LTCUSD']  # For testing, give tickers here

    tickers_missing_from_bt = tickers_missing_from_bt_stock + tickers_missing_from_bt_crypto

    tickers_missing_from_rh = missing_from_rh_df.index.tolist()

    print("\n----------------------------------------------------------------------\n")

    # Process and display data missing from Banktivity
    if tickers_missing_from_bt:
        print("Missing from Banktivity:\n")
        print(missing_from_bt_df)
                
        if tickers_missing_from_bt_stock:
            print("\nGetting missing stock order info... ")
            rh_stock_orders  = rh_fetch.get_rh_stock_orders(tickers_missing_from_bt_stock)
            print("\nRobinhood order data for stock tickers missing from Banktivity:")
            iterate_through_rh_orders(tickers_missing_from_bt, rh_stock_orders)
            print()
        else:
            print("\nNo Robinhood stock tickers missing from Banktivity.")
        
        if tickers_missing_from_bt_crypto:
            print("\nGetting missing crypto order info... ")
            tickers_missing_from_bt_crypto = cleanup_bt_crypto_tickers(tickers_missing_from_bt_crypto)
            rh_crypto_orders = rh_fetch.get_rh_crypto_orders(tickers_missing_from_bt_crypto)
            print("Robinhood order data for crypto tickers missing from Banktivity:")
            iterate_through_rh_orders(tickers_missing_from_bt_crypto, rh_crypto_orders)
        else:
            print("\nNo Robinhood crypto tickers missing from Banktivity.")
    
    else:
        print("No Robinhood data is missing from Banktivity.")


    print("\n----------------------------------------------------------------------\n")

    # Process and display data missing from Robinhood
    if tickers_missing_from_rh:
        print("Missing from Robinhood:\n")
        print(missing_from_rh_df)

        # TODO: print out Robinhood order data for missing tickers

    else:
        print("No Banktivity data is missing from Robinhood.")

    print("\n----------------------------------------------------------------------\n")

    print("Done. Exiting.\n")
