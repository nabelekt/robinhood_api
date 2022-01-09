import sys
import os
import pyotp
import robin_stocks
import json
import robinhood_creds as rh_creds
import functools
print = functools.partial(print, flush=True)  # Prevent print statements from buffering till end of execution


def setup():
    # Disable printing from robin_stocks module
    robin_stocks.helper.set_output(open(os.devnull,"w"))


def login():

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


def get_stock_dividends_dicts():

    print("Getting stock dividends from Robinhood... ", end="")
    stock_dividends = robin_stocks.account.get_dividends()
    print("Done.")

    print("Getting symbols for dividends from Robinhood. This may take a few miniutes... ", end="")
    for div_idx, dividend in enumerate(stock_dividends):
        stock_dividends[div_idx]['symbol'] = robin_stocks.stocks.get_symbol_by_url(stock_dividends[div_idx]['instrument'])
    print("Done.")

    return stock_dividends


def get_stock_orders(symbols):
    
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
        try:
            order_set = robin_stocks.orders.find_stock_orders(symbol=symbol)
        except IndexError as e:
            sys.exit(f"\nERROR: IndexError encountered. Make sure that all symbols are valid.\n\nExiting.\n")
        for order in order_set:
            order['symbol'] = symbol
        orders.append(order_set)

    # print_controller.enable_printing()
    
    return orders


def get_all_crypto_orders():
    
    orders = robin_stocks.orders.get_all_crypto_orders()

    return orders


def get_crypto_orders(symbols=None):
    
    all_orders = get_all_crypto_orders()
    
    if symbols is None:
        wanted_orders = all_orders
        # TODO: Do for all orders what we do for wanted orders below

    elif symbols == []:
        return []
    
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


def get_crypto_order_info(order_id):

  data = robin_stocks.orders.get_crypto_order_info(id)


def get_crypto_positions():
    
    positions = robin_stocks.crypto.get_crypto_positions()
    
    return positions
