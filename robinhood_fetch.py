OUTPUT_FILE_PATH_STOCKS = "robinhood_stock_positions.json"
OUTPUT_FILE_PATH_CRYPTO = "robinhood_crypto_positions.json"


import pyotp
import robin_stocks
import json
import robinhood_creds as rh_creds
import functools
print = functools.partial(print, flush=True)  # Prevent print statements from buffering till end of execution


def rh_login():
    totp  = pyotp.TOTP(rh_creds.TOTP_code).now()
    login = robin_stocks.login(rh_creds.username, rh_creds.password, mfa_code=totp)


def get_stock_positions_dicts():
    print("Getting stock positions from Robinhood. This may take a few minutes... ", end="")
    stock_positions = robin_stocks.account.build_holdings(with_dividends=True)
    print("Done.")

    return stock_positions


def get_crypto_positions_dicts():
    print("Getting crypto positions from Robinhood... ", end="")    
    crypto_positions = robin_stocks.crypto.get_crypto_positions()
    print("Done.")

    return crypto_positions


def write_to_json_file(data_to_write, output_file_path):
    print(f"Writing to {output_file_path} file... ", end="")
    output_file = open(output_file_path, "w")
    output_file.write(json.dumps(data_to_write))
    output_file.close()
    print("Done.")


def get_rh_stock_orders(symbols):
    
    rh_login()
    
    # print_controller = print_control.Controller()
    # print_controller.disable_printing()
    
    # import os
    # import sys
    # f = open(os.devnull, 'w')
    # # sys.stdout = f

    # contextlib.redirect_stdout(f)

    # f = io.StringIO()
    # with redirect_stdout(f):

    #     orders = [robin_stocks.orders.find_stock_orders(symbol=symbol) for symbol in tickers]
    
    #     print(f)

    orders = []

    for symbol in symbols:
        order_set = robin_stocks.orders.find_stock_orders(symbol=symbol)
        for order in order_set:
            order['symbol'] = symbol
        orders.append(order_set)

    # print_controller.enable_printing()
    
    return orders


def get_rh_all_crypto_orders():
    
    rh_login()

    orders = robin_stocks.orders.get_all_crypto_orders()

    return orders


def get_rh_crypto_orders(symbols=None):
    
    all_orders = get_rh_all_crypto_orders()
    
    if symbols is None:
        wanted_orders = all_orders
    
    else:
        order_dict = dict((symbol, []) for symbol in symbols)  # Create dictionary with each symbol as a key
             
        for order in all_orders:
            symbol = get_crypto_order_symbol(order['currency_pair_id'])
            order['symbol'] = symbol
            try:
                order_dict[symbol].append(order)  # Append order to dicionary corresponding with symbol
            except KeyError:
                pass  # Discard orders that don't corespond to one of our tickers


        # Create list of lists from dictionary
        wanted_orders = list(order_dict.values())
    
    return wanted_orders


def get_crypto_order_symbol(currency_pair_id):

    return robin_stocks.crypto.get_crypto_quote_from_id(currency_pair_id, 'symbol')


def get_rh_crypto_order_info(order_id):

  rh_login()

  data = robin_stocks.orders.get_crypto_order_info(id)


def get_rh_crypto_positions():
    
    rh_login()

    positions = robin_stocks.crypto.get_crypto_positions()
    
    return positions


if __name__ == "__main__":

    print()

    rh_login()
    stock_positions = get_stock_positions_dicts()
    crypto_positions = get_crypto_positions_dicts()

    write_to_json_file(stock_positions,  OUTPUT_FILE_PATH_STOCKS)
    write_to_json_file(crypto_positions, OUTPUT_FILE_PATH_CRYPTO)

    print("\nDone. Exiting.\n")