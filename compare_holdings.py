SHOW_CANCELED_AND_FAILED_ORDERS = False


import pandas as pd
import io
import robin_stocks
import pyotp
from datetime import datetime as dt
import json
import print_control
from contextlib import redirect_stdout
import os
import argparse
import sys
import time

# Local modules and files:
import robinhood_process as rh_process
import robinhood_fetch   as rh_fetch


def parse_and_check_input():

    parser = argparse.ArgumentParser(description='Reconcile Robinhood positions with what Banktivity is tracking')
    parser.add_argument('bt_csv_file_path', help="Path to CSV file exported from banktivity with position information.")
    parser.add_argument('--compare_equity', action='store_true')
    parser.add_argument('--equity_diff', help="If compare_equity is set, then RH orders where equity differences " \
                        "are greater than equity_diff will be displayed.")
    args = parser.parse_args()

    if not os.path.isfile(args.bt_csv_file_path):
        sys.exit(f"Input file '{args.bt_csv_file_path}' does not exist.\nExiting.\n")
    
    if args.compare_equity and not args.equity_diff:
        sys.exit(f"When '--compare_equity' is set, an '--equity_diff' argument must be provided.\nExiting.\n")

    if args.compare_equity:
        try:
            args.equity_diff = float(args.equity_diff)
        except ValueError:
            sys.exit(f"--equity_diff argument '{args.equity_diff}' is invalid. A floating-point value must be provided.\nExiting.\n")

    return args


def process_banktivity_positions_data(bt_holdings_csv_file_path):

    # Read in file data exported from Banktivity
    input_file  = open(bt_holdings_csv_file_path, "r")
    input_lines = input_file.readlines()
    input_file.close()

    # Find where securities information starts and get data from there
    securities_line_idx = input_lines.index('Securities\n')
    file_header = input_lines[securities_line_idx+1]
    file_data   = input_lines[securities_line_idx+3:]
    file_data   = ''.join(file_data)
    data_str    = file_header + file_data
    data_str    = io.StringIO(data_str)

    df = pd.read_csv(data_str, sep=",")

    df = df.set_index('Symbol')
    df.index.names = ['ticker']
    df.rename(columns={"Name": "name", "Close Shares": "quantity", "Close Value": "equity"}, inplace=True)

    df['type'] = '?'  # Unknown if security is stock or cryptocurrency
    df['quote'] =  '?'  # Datatypes need to be corrected before this can be set

    df = rh_process.prep_stock_positions_df_for_compare(df)

    df['quote'] = df['equity'] / df['quantity']

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


def parse_and_print_rh_order_data(ticker, order_set):
    # TODO: Some of this can probably now be replaced with:
    #    stock_orders_dicts = rh_fetch.get_stock_orders(tickers)
    #    stock_orders_df = process_stock_order_data(stock_positions_dicts)
    
    if order_set == []:
        print(f'  {ticker:7s}', end="  ")
        now = int(time.time())
        five_years_ago = int(now - (5*365.25*24*60*60))
        print(f"No Robinhood order data for '{ticker}'. This may be due to a merger, stock split, etc. See 5 year history at " \
              f"https://finance.yahoo.com/quote/{ticker}/history?period1={five_years_ago}&period2={now}" \
              f"\nSearching https://sec.report/Ticker/ may also be helpful.")
        return False
    
    else:
        for order_idx, order in enumerate(order_set):
            if ticker != order['symbol']:
                print(f"ERROR: symbol mismatch: {ticker} != {order['symbol']}, ", sys.exc_info()[0])
                raise  # Raise to help debug

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
                        print(f'{price:12,.3f}', end="  ")
                        
                        datetime_str = rh_process.format_datetime_str(execution['timestamp'])
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


def compare_equity(df_bt, df_rh):

    equity_tuples = [(df_rh['type'][ticker], ticker, df_rh['name'][ticker], df_bt['quantity'][ticker], 
        df_rh['quantity'][ticker], df_bt['quote'][ticker], df_rh['quote'][ticker], df_bt['equity'][ticker], df_rh['equity'][ticker]) for ticker in df_rh.index.tolist()]
    df = pd.DataFrame(equity_tuples, columns=['type', 'ticker', 'name', 'bt_quantity', 'rh_quantity', 'bt_price', 'rh_price', 'bt_equity', 'rh_equity'])

    df['equity_difference'] = df['bt_equity'] - df['rh_equity']

    df = df.reindex(df.equity_difference.abs().sort_values(ascending=False).index)  # Sort by absolute value
    df = df[df.equity_difference.abs() > 0]  # Get rid of rows with no difference

    print("Equity differences may be due to after-hours trading. Robinhood prices may be updated through after-hours, "
          "trading while Banktivity prices may only be updated through market close. 'rh_price' below, however, may "
          "only be updated through market close while 'rh_equity' will be current.")
    print("Equity differences:\n")
    print(df.to_string(index=False))

    return df


def get_equity_diff_tickers(equity_diff_df, equity_diff):

    equity_diff_df = equity_diff_df[equity_diff_df['equity_difference'].abs() >= equity_diff]
    equity_diff_df = rh_process.sort_by(equity_diff_df, 'name')

    equity_diff_stock_tickers  = equity_diff_df.loc[equity_diff_df['type'] == 'stock',  'ticker'].tolist()
    equity_diff_crypto_tickers = equity_diff_df.loc[equity_diff_df['type'] == 'crypto', 'ticker'].tolist()

    return [equity_diff_stock_tickers, equity_diff_crypto_tickers]


def main():

    print()

    args = parse_and_check_input()

    # Settings for dataframe printing
    pd.options.display.width = 0
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    # Do setup tasks
    rh_fetch.setup()

    # Login to Robinhood
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

    df_bt = process_banktivity_positions_data(args.bt_csv_file_path)
    df_rh = rh_process.process_positions_data(get_quotes=args.compare_equity)

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

    print("\n--------------------------------------------------------------------------------\n")

    # Process and display data missing from Banktivity
    if tickers_missing_from_bt:
        print("Missing from Banktivity:\n")
        print(missing_from_bt_df)
                
        if tickers_missing_from_bt_stock:
            print("\nGetting missing stock order info... ")
            rh_stock_orders  = rh_fetch.get_stock_orders(tickers_missing_from_bt_stock)
            print("\nRobinhood order data for stock tickers missing from Banktivity:")
            iterate_through_rh_orders(tickers_missing_from_bt, rh_stock_orders)
            print()
        else:
            print("\nNo Robinhood stock tickers missing from Banktivity.")
        
        if tickers_missing_from_bt_crypto:
            print("\nGetting missing crypto order info... ")
            tickers_missing_from_bt_crypto = cleanup_bt_crypto_tickers(tickers_missing_from_bt_crypto)
            rh_crypto_orders = rh_fetch.get_crypto_orders(tickers_missing_from_bt_crypto)
            print("Robinhood order data for crypto tickers missing from Banktivity:")
            iterate_through_rh_orders(tickers_missing_from_bt_crypto, rh_crypto_orders)
        else:
            print("\nNo Robinhood crypto tickers missing from Banktivity.")
    
    else:
        print("No Robinhood data is missing from Banktivity.")


    print("\n--------------------------------------------------------------------------------\n")

    # Process and display data missing from Robinhood
    if tickers_missing_from_rh:
        print("Missing from Robinhood:\n")
        print(missing_from_rh_df)

        # TODO: print out Robinhood order data for missing tickers

    else:
        print("No Banktivity data is missing from Robinhood.")

    print("\n--------------------------------------------------------------------------------\n")

    if args.compare_equity:

        # Remove missing tickers from dfs before comparing equity
        if tickers_missing_from_rh:
            df_bt.drop(tickers_missing_from_rh, inplace=True)
        if tickers_missing_from_bt:
            df_rh.drop(tickers_missing_from_bt, inplace=True)

        equity_diff_df = compare_equity(df_bt, df_rh)
        [equity_diff_tickers_stock, equity_diff_tickers_crypto] = get_equity_diff_tickers(equity_diff_df, args.equity_diff)

        if equity_diff_tickers_stock:        
            print(f"\nGetting stock order info for stock tickers where absolute value equity differences are greater than or equal to ${args.equity_diff}... ")
        rh_stock_orders  = rh_fetch.get_stock_orders(equity_diff_tickers_stock)

        if equity_diff_tickers_crypto:
            equity_diff_tickers_crypto = cleanup_bt_crypto_tickers(equity_diff_tickers_crypto)
            print(f"\nGetting crypto order info for crypto tickers where absolute value equity differences are greater than or equal to ${args.equity_diff}... ")
        rh_crypto_orders = rh_fetch.get_crypto_orders(equity_diff_tickers_crypto)

        if equity_diff_tickers_stock or equity_diff_tickers_crypto:
            print(f"\nRobinhood order data for securities where absolute value equity differences are greater than or equal to ${args.equity_diff}:\n")
            iterate_through_rh_orders(equity_diff_tickers_stock+equity_diff_tickers_crypto, rh_stock_orders+rh_crypto_orders)

        else:
            print(f"\nNo securities have equity differences greater than or equal to ${args.equity_diff}.")

        print("\n--------------------------------------------------------------------------------\n")


if __name__ == "__main__":

    main()

    print("Done. Exiting.\n")
