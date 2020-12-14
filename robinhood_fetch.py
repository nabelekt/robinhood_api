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


def get_rh_stock_orders(tickers):
    
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

    orders = [robin_stocks.orders.find_stock_orders(symbol=symbol) for symbol in tickers]

    # print_controller.enable_printing()
    
    return orders


def get_rh_crypto_orders(tickers):
    
    rh_login()

    orders = robin_stocks.orders.get_all_crypto_orders()
    
    print(orders)
    return orders


if __name__ == "__main__":

    print()

    rh_login()
    stock_positions = get_stock_positions_dicts()
    crypto_positions = get_crypto_positions_dicts()

    write_to_json_file(stock_positions,  OUTPUT_FILE_PATH_STOCKS)
    write_to_json_file(crypto_positions, OUTPUT_FILE_PATH_CRYPTO)

    print("\nDone. Exiting.\n")