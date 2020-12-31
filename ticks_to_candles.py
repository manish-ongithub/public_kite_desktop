################## Ticks to candles in kiteconnect python ####################
# Author : Arun B
# Reference  : http://ezeetrading.in/Articles/Candles_formation_from_tick_data_zerodha.html
# Purpose : Convert ticks to candles by putting ticks in a queue. This redues time wasted in on_ticks function
################################################################################

from kiteconnect import KiteTicker
from kiteconnect import KiteConnect
import datetime
from copy import copy
import queue
import os
import pandas as pd
import json
import pdb
import utilities as util
from yahoo_finance_api import YahooFinance as yf
import strategy as st
from pytz import timezone
import asyncio
import orders as ord


# Enter the user api_key and the access_token generated for that day
credentials = {'api_key': util.api_key, 'access_token': util.access_token}

kite = KiteConnect(api_key=credentials['api_key'])
kite.set_access_token(credentials['access_token'])

inst_list = kite.instruments('NSE')

infy_rec = util.get_instrument_details_by_name('INFY',inst_list)
sbin_rec = util.get_instrument_details_by_name('SBIN',inst_list)

sbin1m = yf('SBIN.NS', result_range='1d', interval='1m', dropna='True').result
sbin5m = yf('SBIN.NS', result_range='1d', interval='5m', dropna='True').result
print(sbin1m.tail())
print(sbin5m.tail())
sbin1m = sbin1m[:-1]
sbin5m = sbin5m[:-1]

print(' ---------------- After removing last row ------------------')
print(sbin1m.tail())
print(sbin5m.tail())
"""
infy5m = yf('INFY.NS', result_range='1d', interval='5m', dropna='True').result

sbin15m = yf('SBIN.NS', result_range='5d', interval='15m', dropna='True').result
infy15m = yf('INFY.NS', result_range='5d', interval='15m', dropna='True').result
"""

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
        for tick in ticks:
            #pdb.set_trace()
            utc_time = tick['last_trade_time']
            tick['last_trade_time'] = utc_time.astimezone(timezone('Asia/Kolkata'))
            timestamp = tick["timestamp"]
            tick['timestamp'] = timestamp.astimezone(timezone('Asia/Kolkata'))

        self.type = 'TICK'
        self.data = ticks


class CandleEvent(Event):
    """
    Handles the event of receiving a 1min candle
    """

    def __init__(self, symbol, candle):
        self.type = 'CANDLE'
        self.symbol = symbol
        self.data = candle

    def print_self(self):
        print ("CANDLE:", self.data)


class CandleEvent15Min(Event):
    """
    Handles the event of 15 min candle.

    """

    def __init__(self, symbol, candle):
        """
        Initialises the 15mincandle event.
        """

        self.type = '15MinCANDLE'
        self.symbol = symbol
        self.data = candle

    def print_self(self):
        """
        Outputs the values within the Order.
        """
        print("CANDLE: = ", self.data)


# Initialise
kws = KiteTicker(credentials["api_key"], credentials["access_token"])

# token_dict = {256265:'NIFTY 50',260105:'NIFTY BANK'} #prev definition is wrong. It has to be nested dictionary
#token_dict = {256265: {'Symbol': 'NIFTY 50'}, 260105: {'Symbol': 'NIFTY BANK'}}  # you can put any F&O tokens here
token_dict = {infy_rec['instrument_token']:{'Symbol':infy_rec['name']},sbin_rec['instrument_token']:{'Symbol':sbin_rec['name']}}
print(token_dict)
# Creation of 1min and 15 min candles
candles_1 = {}
candles_15 = {}
candles_5 = {}

EventQ = queue.Queue()


def on_ticks(ws, ticks):
    #print('on_ticks , ticks received')
    #print(ticks)
    # Callback to receive ticks.
    print('starting asyncio')
    asyncio.run(ord.Check_for_Orders_Queue(ticks))
    print('asyncio run completed')
    tick = TickEvent(ticks)
    EventQ.put(tick)
    print('tickevent completed')


def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    instruments = list(token_dict.keys())
    print(instruments)
    # exit()
    ws.subscribe(instruments)

    # Set tick in `full` mode.
    ws.set_mode(ws.MODE_FULL, instruments)


kws.on_ticks = on_ticks
kws.on_connect = on_connect
# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect(threaded=True)


def main():
    while True:
        try:
            event = EventQ.get(False)
            #if event:
            #    print(event.data)
        except queue.Empty:
            continue
        else:
            if event.type == 'TICK':
                ticks = event.data

                for tick in ticks:
                    instrument = tick["instrument_token"]
                    # instrument = token_dict[instrument_token]
                    # print(instrument_token,instrument)
                    ltt = tick["timestamp"]
                    # print(tick)
                    ltt_min_1 = datetime.datetime(ltt.year, ltt.month, ltt.day, ltt.hour, ltt.minute)
                    ltt_min_2 = ltt_min_1 - datetime.timedelta(minutes=1)
                    #pdb.set_trace()
                    ltt_min_15 = datetime.datetime(ltt.year, ltt.month, ltt.day, ltt.hour, ltt.minute // 15 * 15)
                    ltt_min_215 = ltt_min_15 - datetime.timedelta(minutes=15)

                    ltt_min_5 = datetime.datetime(ltt.year, ltt.month, ltt.day, ltt.hour, ltt.minute // 5 * 5)
                    ltt_min_5_2 = ltt_min_5 - datetime.timedelta(minutes=5)

                    ltt_min_15 = ltt_min_5
                    ltt_min_215 = ltt_min_5_2
                    # print(ltt_min_1,end='\r')
                    # print(ltt_min_15,ltt_min_215)
                    # exit()
                    # For any other timeframe. Simply change ltt_min_1 variable defination.
                    # e.g.
                    # ltt_min_15=datetime.datetime(ltt.year, ltt.month, ltt.day, ltt.hour,ltt.minute//15*15)

                    ### Forming 1 Min Candles...

                    if instrument in candles_1:
                        if ltt_min_1 in candles_1[instrument]:
                            # print(tick)

                            candles_1[instrument][ltt_min_1]["high"] = max(candles_1[instrument][ltt_min_1]["high"],
                                                                           tick["last_price"])  # 1
                            candles_1[instrument][ltt_min_1]["low"] = min(candles_1[instrument][ltt_min_1]["low"],
                                                                          tick["last_price"])  # 2
                            candles_1[instrument][ltt_min_1]["close"] = tick["last_price"]  # 3
                            if tick["tradable"]:  # for F & O
                                candles_1[instrument][ltt_min_1]["volume"] = tick["volume"]
                                candles_1[instrument][ltt_min_1]["oi"] = tick['oi']
                                candles_1[instrument][ltt_min_1]["atp"] = tick['average_price']

                                if ltt_min_2 in candles_1[instrument]:
                                    candles_1[instrument][ltt_min_1]["vol"] = tick["volume"] - \
                                                                              candles_1[instrument][ltt_min_2][
                                                                                  "volume"]  # 3.5
                            else:
                                candles_1[instrument][ltt_min_1]["volume"] = 0
                                candles_1[instrument][ltt_min_1]["oi"] = 0
                                candles_1[instrument][ltt_min_1]["atp"] = 0

                        else:
                            # print(instrument,str(ltt_min_1),candles_1[instrument][ltt_min_1])
                            candles_1[instrument][ltt_min_1] = {}
                            candles_1[instrument][ltt_min_1]["high"] = copy(tick["last_price"])  # 8
                            candles_1[instrument][ltt_min_1]["low"] = copy(tick["last_price"])
                            candles_1[instrument][ltt_min_1]["open"] = copy(tick["last_price"])
                            candles_1[instrument][ltt_min_1]["close"] = copy(tick["last_price"])
                            candles_1[instrument][ltt_min_1]["volume"] = 0
                            candles_1[instrument][ltt_min_1]["vol"] = 0
                            candles_1[instrument][ltt_min_1]["oi"] = 0
                            candles_1[instrument][ltt_min_1]["atp"] = 0
                            if ltt_min_2 in candles_1[instrument]:
                                # print(candles_1)
                                candle = {"token": instrument, "Time": ltt_min_2,
                                          "open": candles_1[instrument][ltt_min_2]["open"],
                                          "high": candles_1[instrument][ltt_min_2]["high"],
                                          "low": candles_1[instrument][ltt_min_2]["low"],
                                          "close": candles_1[instrument][ltt_min_2]["close"],
                                          "volume": candles_1[instrument][ltt_min_2]["vol"],
                                          "oi": candles_1[instrument][ltt_min_2]["oi"],
                                          'atp': candles_1[instrument][ltt_min_2]['atp']
                                          }
                                candleevent = CandleEvent(instrument, candle)
                                EventQ.put(candleevent)
                    else:
                        candles_1[instrument] = {}
                        print("created dict for " + str(instrument))

                    ### Forming 15 Min Candles...
                    if instrument in candles_15:
                        if ltt_min_15 in candles_15[instrument]:
                            # print(tick)

                            candles_15[instrument][ltt_min_15]["high"] = max(candles_15[instrument][ltt_min_15]["high"],
                                                                             tick["last_price"])  # 1
                            candles_15[instrument][ltt_min_15]["low"] = min(candles_15[instrument][ltt_min_15]["low"],
                                                                            tick["last_price"])  # 2
                            candles_15[instrument][ltt_min_15]["close"] = tick["last_price"]  # 3
                            if tick["tradable"]:
                                candles_15[instrument][ltt_min_15]["volume"] = tick["volume"]
                                candles_15[instrument][ltt_min_15]["oi"] = tick['oi']
                                candles_15[instrument][ltt_min_15]["atp"] = tick['average_price']

                                if ltt_min_215 in candles_15[instrument]:
                                    candles_15[instrument][ltt_min_15]["vol"] = tick["volume"] - \
                                                                                candles_15[instrument][ltt_min_215][
                                                                                    "volume"]  # 3.5
                            else:
                                candles_15[instrument][ltt_min_15]["volume"] = 0
                                candles_15[instrument][ltt_min_15]["oi"] = 0
                                candles_15[instrument][ltt_min_15]["atp"] = 0

                        else:
                            # print(instrument,str(ltt_min_15),candles_15[instrument][ltt_min_15])
                            candles_15[instrument][ltt_min_15] = {}
                            candles_15[instrument][ltt_min_15]["high"] = copy(tick["last_price"])  # 8
                            candles_15[instrument][ltt_min_15]["low"] = copy(tick["last_price"])
                            candles_15[instrument][ltt_min_15]["open"] = copy(tick["last_price"])
                            candles_15[instrument][ltt_min_15]["close"] = copy(tick["last_price"])
                            candles_15[instrument][ltt_min_15]["volume"] = 0
                            candles_15[instrument][ltt_min_15]["vol"] = 0
                            candles_15[instrument][ltt_min_15]["oi"] = 0
                            candles_15[instrument][ltt_min_15]["atp"] = 0
                            if ltt_min_215 in candles_15[instrument]:
                                # print(candles_15)
                                candle = {"token": instrument, "Time": ltt_min_215,
                                          "open": candles_15[instrument][ltt_min_215]["open"],
                                          "high": candles_15[instrument][ltt_min_215]["high"],
                                          "low": candles_15[instrument][ltt_min_215]["low"],
                                          "close": candles_15[instrument][ltt_min_215]["close"],
                                          "volume": candles_15[instrument][ltt_min_215]["vol"],
                                          "oi": candles_15[instrument][ltt_min_215]["oi"],
                                          'atp': candles_15[instrument][ltt_min_215]['atp']
                                          }
                                candleevent = CandleEvent15Min(instrument, candle)
                                EventQ.put(candleevent)
                    else:
                        candles_15[instrument] = {}
                        print("created dict for " + str(instrument))

            elif event.type == "CANDLE":
                # print(event.type)
                # print(event.symbol,event.data)
                event.data.update(token_dict[event.symbol])
                df = pd.DataFrame(event.data, index=[0])
                print('1 min candle')
                if event.data['token'] == sbin_rec['instrument_token']:
                    global sbin1m
                    print('before appending ', len(sbin1m))
                    row = pd.Series({'Open':event.data['open'],'High':event.data['high'],'Low':event.data['low'],'Close':event.data['close'],'Volume':event.data['volume']}, name=event.data['Time'])
                    sbin1m = sbin1m.append(row)
                    print('added row to sbin1m ',len(sbin1m))
                    print(sbin1m.tail())
                    signal = st.check_stochastic(sbin1m)
                    print('signal returned by stochastic ',signal)
                    if signal is not st.hold_signal:
                        order_obj = {'price':event.data['close'],'instrument':event['token'],'order_type':st.signal}
                        ord.add_to_order_queue(order_obj)

                print(df)

            elif event.type == "15MinCANDLE":
                # print(event.symbol, event.data)
                event.data.update(token_dict[event.symbol])
                df = pd.DataFrame(event.data, index=[0])
                # print(df)
                print('5 min candle')
                if event.data['token'] == sbin_rec['instrument_token']:
                    print('before appending ', len(sbin5m))
                    row = pd.Series({'Open':event.data['open'],'High':event.data['high'],'Low':event.data['low'],'Close':event.data['close'],'Volume':event.data['volume']}, name=event.data['Time'])
                    sbin5m.append(row)
                    print('added row to sbin1m ', len(sbin5m))
                print(df)
                #CALCULATE indicator and place order
                # print('\n')




if __name__ == "__main__":
    main()