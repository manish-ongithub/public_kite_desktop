from kiteconnect import KiteTicker
from kiteconnect import KiteConnect
import datetime
from copy import copy
import queue
import os
import pandas as pd
import json
import pdb
from pytz import timezone
import asyncio
import orders as ord
import tick_handler
import utilities as util

# Enter the user api_key and the access_token generated for that day
credentials = {'api_key': util.api_key, 'access_token': util.access_token}

# Initialize and get websocket
kite_ws = KiteTicker(credentials["api_key"], credentials["access_token"])
kite_connect = KiteConnect(api_key=credentials['api_key'])
kite_connect.set_access_token(credentials['access_token'])

#ord.OBJ_KITE_CONNECT = kite_connect
#list of tokens to receive ticks
#inst_list = kite_connect.instruments('NSE')
#mcx_list = kite_connect.instruments('MCX')
infy_rec = util.get_nse_instrument_details_by_name('INFY')
sbin_rec = util.get_nse_instrument_details_by_name('SBIN')
acc_rec = util.get_nse_instrument_details_by_name('ACC')
gold21febfut = util.get_mcx_instrument_details_by_name('GOLD21FEBFUT')

# Fetch all orders
orders_list = kite_connect.orders()
positions_list = kite_connect.positions()

ord.KITE_ORDERS_LIST = orders_list
ord.KITE_POSITIONS = positions_list

#instrument_list = {'SBIN':'779521','INFY':'408065','ACC':'5633'}
token_dict = {
                #infy_rec['instrument_token']:{'Symbol':infy_rec['name']},
                sbin_rec['instrument_token']:{'Symbol':sbin_rec['name']},
                #gold21febfut['instrument_token']:{'Symbol':'GOLD21FEBFUT'},
                #acc_rec['instrument_token']:{'Symbol':acc_rec['name']},
              }
util.lgr_kite_web.log(util.levels[0],'token_dict {}'.format(token_dict))
tick_handler.get_prev_candles_data_from_zerodha(token_dict)

class Event(object):
    """
    Event is base class providing an interface for all subsequent
    (inherited) events, that will trigger further events in the
    trading infrastructure.
    """
    pass

class TickEvent(Event):
    """
    Handles the event of receiving a new market ticks
    """
    def __init__(self, ticks):
        """
        Initialises the MarketEvent.
        """
        """
        for tick in ticks:
            #pdb.set_trace()
            #utc_time = tick['last_trade_time']
            #tick['last_trade_time'] = utc_time.astimezone(timezone('Asia/Kolkata'))
            #timestamp = tick["timestamp"]
            #tick['timestamp'] = timestamp.astimezone(timezone('Asia/Kolkata'))
        """
        self.type = 'TICK'
        self.data = ticks

# to be put in different class
EventQ = queue.Queue()


def on_ticks(ws, ticks):
    # print('on_ticks , ticks received')
    # print(ticks)
    # Callback to receive ticks.
    print('starting asyncio')
    asyncio.run(ord.Check_for_Orders_Queue(ticks,ord,kite_connect))
    print('asyncio run completed')
    tick = TickEvent(ticks)
    #EventQ.put(tick)
    asyncio.run(tick_handler.process_ticks(tick,ord))
    print('tickevent completed')


def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens
    instruments = list(token_dict.keys())
    print(instruments)
    # exit()
    ws.subscribe(instruments)
    # Set tick in `full` mode.
    ws.set_mode(ws.MODE_FULL, instruments)
    #get data from yahoo website
    #tick_handler.get_prev_candles_data_from_yahoo()



def on_order_update(ws, data):
    util.lgr_kite_web.log(util.levels[0],'in on_order_update')
    util.lgr_kite_web.log(util.levels[0],data)
    pdb.set_trace()


kite_ws.on_ticks = on_ticks                 # Triggered when ticks are received.
kite_ws.on_connect = on_connect             # Triggered when connection is established successfully
kite_ws.on_order_update = on_order_update   # Triggered when there is an order update for the connected user

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kite_ws.connect(threaded=True)
"""
ticks_list = [
  {"tradable": True, 'mode': 'full', 'instrument_token': 5633,'last_price': 1660.55, 'last_quantity': 4, 'average_price': 1656.29, 'volume': 1578664,
  'buy_quantity': 0, 'sell_quantity': 306,
  'ohlc': {'open': 1682.6, 'high': 1682.6, 'low': 1637.7, 'close': 1665.95},
  'change': -0.3241393799333768,
  'last_trade_time': datetime.datetime(2020, 12, 7, 15, 44, 2), 'oi': 0, 'oi_day_high': 0, 'oi_day_low': 0, 'timestamp': datetime.datetime(2020, 12, 7, 17, 10, 10), 'depth': {'buy': [{'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}], 'sell': [{'quantity': 306, 'price': 1660.55, 'orders': 8}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}]}},
  {"tradable": True, 'mode': 'full', 'instrument_token': 5633,
  'last_price': 1660.55, 'last_quantity': 4, 'average_price': 1656.29, 'volume': 1578664,
  'buy_quantity': 0, 'sell_quantity': 306,
  'ohlc': {'open': 1682.6, 'high': 1682.6, 'low': 1637.7, 'close': 1665.95},
  'change': -0.3241393799333768,
  'last_trade_time': datetime.datetime(2020, 12, 7, 15, 44, 2), 'oi': 0, 'oi_day_high': 0, 'oi_day_low': 0, 'timestamp': datetime.datetime(2020, 12, 7, 17, 10, 10), 'depth': {'buy': [{'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}], 'sell': [{'quantity': 306, 'price': 1660.55, 'orders': 8}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}]}},
  {"tradable": True, 'mode': 'full', 'instrument_token': 5633,
  'last_price': 1660.55, 'last_quantity': 4, 'average_price': 1656.29, 'volume': 1578664,
  'buy_quantity': 0, 'sell_quantity': 306,
  'ohlc': {'open': 1682.6, 'high': 1682.6, 'low': 1637.7, 'close': 1665.95},
  'change': -0.3241393799333768,
  'last_trade_time': datetime.datetime(2020, 12, 7, 15, 44, 2), 'oi': 0, 'oi_day_high': 0, 'oi_day_low': 0, 'timestamp': datetime.datetime(2020, 12, 7, 17, 10, 10), 'depth': {'buy': [{'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}], 'sell': [{'quantity': 306, 'price': 1660.55, 'orders': 8}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}]}},
  {"tradable": True, 'mode': 'full', 'instrument_token': 5633,
  'last_price': 1660.55, 'last_quantity': 4, 'average_price': 1656.29, 'volume': 1578664,
  'buy_quantity': 0, 'sell_quantity': 306,
  'ohlc': {'open': 1682.6, 'high': 1682.6, 'low': 1637.7, 'close': 1665.95},
  'change': -0.3241393799333768,
  'last_trade_time': datetime.datetime(2020, 12, 7, 15, 44, 2), 'oi': 0, 'oi_day_high': 0, 'oi_day_low': 0, 'timestamp': datetime.datetime(2020, 12, 7, 17, 10, 10), 'depth': {'buy': [{'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}], 'sell': [{'quantity': 306, 'price': 1660.55, 'orders': 8}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}]}},
  {"tradable": True, 'mode': 'full', 'instrument_token': 5633,
  'last_price': 1660.55, 'last_quantity': 4, 'average_price': 1656.29, 'volume': 1578664,
  'buy_quantity': 0, 'sell_quantity': 306,
  'ohlc': {'open': 1682.6, 'high': 1682.6, 'low': 1637.7, 'close': 1665.95},
  'change': -0.3241393799333768,
  'last_trade_time': datetime.datetime(2020, 12, 7, 15, 44, 2), 'oi': 0, 'oi_day_high': 0, 'oi_day_low': 0, 'timestamp': datetime.datetime(2020, 12, 7, 17, 10, 10), 'depth': {'buy': [{'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}], 'sell': [{'quantity': 306, 'price': 1660.55, 'orders': 8}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}]}},
  {"tradable": True, 'mode': 'full', 'instrument_token': 5633,
  'last_price': 1660.55, 'last_quantity': 4, 'average_price': 1656.29, 'volume': 1578664,
  'buy_quantity': 0, 'sell_quantity': 306,
  'ohlc': {'open': 1682.6, 'high': 1682.6, 'low': 1637.7, 'close': 1665.95},
  'change': -0.3241393799333768,
  'last_trade_time': datetime.datetime(2020, 12, 7, 15, 44, 2), 'oi': 0, 'oi_day_high': 0, 'oi_day_low': 0, 'timestamp': datetime.datetime(2020, 12, 7, 17, 10, 10), 'depth': {'buy': [{'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}], 'sell': [{'quantity': 306, 'price': 1660.55, 'orders': 8}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}, {'quantity': 0, 'price': 0.0, 'orders': 0}]}}
]
df_test = pd.DataFrame(ticks_list)
print(df_test)
#pdb.set_trace()
for index , row in df_test.iterrows():
    tks = [row.to_dict()]
    on_ticks(None,tks)

"""
if __name__ == "__main__":
    while(True):
        pass
