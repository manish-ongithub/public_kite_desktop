import asyncio
from yahoo_finance_api import YahooFinance as yf
import strategy as st
import datetime
import pdb
import pandas as pd
from copy import copy
import utilities as util

CANDLE_1MIN = "1m"
CANDLE_5MIN = "5m"
CANDLE_15MIN = "15m"
CANDLE_30MIN = "30m"

CANDLES_LIST = [CANDLE_1MIN,CANDLE_5MIN,CANDLE_15MIN,CANDLE_30MIN]
CANDLES_INT_LIST = [1,5,15,30]
CANDLES_INTERVAL_DURATION = {1:CANDLE_1MIN,5:CANDLE_5MIN,15:CANDLE_15MIN,30:CANDLE_30MIN}
DAYS_DURATION = "2d"

instrument_list = {'SBIN':'779521','INFY':'408065','ACC':'5633'}
#instrument_list = {'GOLD21FEBFUT':'55851015',}
#yahoo_instrument_list = ['SBIN.NS','INFY.NS']
yahoo_instrument_list = ['SBIN.NS']
INSTRUMENT_CANDLES_DATA = {}


def get_prev_candles_data_from_yahoo():
    #https://kite.zerodha.com/oms/instruments/historical/779521/minute?user_id=ZX6806&oi=1&from=2020-12-15&to=2020-12-15&ciqrandom=1608047524315
    #header {authorization:enctoken fVaDqQqCJ7uw7mDmdiFjxR3x9NLOotWvpyL0JaU10ov+aJ5glTtsMgVTxoLSjFxO/PyDEjjtMzxgDm+ZH2iTcfa61R9u5g==}
    for rec in yahoo_instrument_list:
        INSTRUMENT_CANDLES_DATA[instrument_list[rec]] = {}
        for interval in CANDLES_LIST:
            data = yf(rec, result_range=DAYS_DURATION, interval=interval, dropna='True').result
            if data is not None:
                #removing last candle from data
                data = data[:-1]
                INSTRUMENT_CANDLES_DATA[instrument_list[rec]][interval] = data
                print('data loaded for ',rec,' ',interval)
            else:
                print('data not loaded for ', rec, ' ', interval)


def get_prev_candles_data_from_zerodha(ticker_list):
    for rec in ticker_list.keys():
        token = rec
        INSTRUMENT_CANDLES_DATA[token] = {}
        for interval in CANDLES_INT_LIST:
            data = util.get_kite_data(token,interval,0)
            if data is not None:
                INSTRUMENT_CANDLES_DATA[token][CANDLES_INTERVAL_DURATION[interval]] = data
                util.lgr_tk_hndlr.log(util.levels[0],'data loaded for {} {}'.format (rec, interval))
            else:
                util.lgr_tk_hndlr.log(util.levels[3],'data not loaded for {} {}'.format(rec,interval))


#
"""
tick_handler.INSTRUMENT_CANDLES_DATA['779521']['5m'].loc['2020-12-11 15:25:00']
<class 'pandas.core.series.Series'>
Open         272.90
High         273.20
Low          272.40
Close        272.45
Volume    823682.00
row = tick_handler.INSTRUMENT_CANDLES_DATA['779521']['5m'].loc['2020-12-11 15:25:00']
modify row and assign to dataframe record
tick_handler.INSTRUMENT_CANDLES_DATA['779521']['5m'].loc['2020-12-11 15:25:00'] = row

datetime_object = datetime.datetime.strptime('2020-12-11 15:25:00', '%Y-%m-%d %H:%M:%S')
tick_handler.INSTRUMENT_CANDLES_DATA['779521']['5m'].loc[datetime_object]

datetime_object in  tick_handler.INSTRUMENT_CANDLES_DATA['779521']['5m'].index

"""


def add_tick_to_candles(instrument,candle_interval,curr_time_1,curr_time_prev_tick,tick):
    print('add_tick_to_candles ',instrument,candle_interval,curr_time_1)
    if instrument in INSTRUMENT_CANDLES_DATA.keys():
        datetime_index = INSTRUMENT_CANDLES_DATA[instrument][candle_interval].index
        util.lgr_tk_hndlr.log(util.levels[0],'before adding tick')
        print(INSTRUMENT_CANDLES_DATA[instrument][candle_interval].tail())
        if curr_time_1 in datetime_index:
            row = INSTRUMENT_CANDLES_DATA[instrument][candle_interval].loc[curr_time_1]
            high = max(row.High, tick["last_price"])    # 1
            low = min(row.Low, tick["last_price"])      # 2
            close = tick["last_price"]                  # 3
            if tick["tradable"]:                        # for F & O
                tk_volume = tick["volume"]
                total_volume = INSTRUMENT_CANDLES_DATA[instrument][candle_interval]['Volume'].sum() - row.Volume
                #total_volume = INSTRUMENT_CANDLES_DATA[instrument][candle_interval]['Volume'].sum()
                diff_volume = tk_volume - total_volume
                volume = diff_volume
                """
                if curr_time_prev_tick in datetime_index:
                    candles_1[instrument][ltt_min_1]["vol"] = tick["volume"] - \
                                                              candles_1[instrument][curr_time_prev_tick][
                                                                  "volume"]  # 3.5
                """

            else:
                volume = 0

            row.High = high
            row.Low = low
            row.Close = close
            row.Volume = volume
            row.OpenInterest = tick['oi']
            INSTRUMENT_CANDLES_DATA[instrument][candle_interval].loc[curr_time_1] = row
        else:
            util.lgr_tk_hndlr.log(util.levels[0],'tick not present {}'.format(curr_time_1))
            total_volume = INSTRUMENT_CANDLES_DATA[instrument][candle_interval]['Volume'].sum()
            diff_volume = tick["volume"] - total_volume

            row = pd.Series(
                {'Open': copy(tick["last_price"]), 'High': copy(tick["last_price"]), 'Low': copy(tick["last_price"]),
                 'Close': copy(tick["last_price"]), 'Volume': diff_volume,'OpenInterest':tick['oi']}, name=curr_time_1)

            INSTRUMENT_CANDLES_DATA[instrument][candle_interval] = INSTRUMENT_CANDLES_DATA[instrument][candle_interval].append(
                row)

        util.lgr_tk_hndlr.log(util.levels[0],' tick added '.format(candle_interval))
        print(INSTRUMENT_CANDLES_DATA[instrument][candle_interval].tail())
        #INSTRUMENT_CANDLES_DATA[instrument][candle_interval].to_csv('SBIN.CSV')


async def process_ticks(tick_event,order_class_object):
    util.lgr_tk_hndlr.log(util.levels[0],'inside process ticks')
    ticks = tick_event.data
    util.lgr_tk_hndlr.log(util.levels[0],'ticks length '.format(len(ticks)))
    for tick in ticks:
        instrument = tick["instrument_token"]
        ltt = tick["timestamp"]
        # print(tick)
        ltt_min_1 = datetime.datetime(ltt.year, ltt.month, ltt.day, ltt.hour, ltt.minute)
        ltt_min_1_prev = ltt_min_1 - datetime.timedelta(minutes=1)

        ltt_min_5 = datetime.datetime(ltt.year, ltt.month, ltt.day, ltt.hour, ltt.minute // 5 * 5)
        ltt_min_5_prev = ltt_min_5 - datetime.timedelta(minutes=5)

        ltt_min_15 = datetime.datetime(ltt.year, ltt.month, ltt.day, ltt.hour, ltt.minute // 15 * 15)
        ltt_min_15_prev = ltt_min_15 - datetime.timedelta(minutes=15)

        ltt_min_30 = datetime.datetime(ltt.year, ltt.month, ltt.day, ltt.hour, ltt.minute // 30 * 30)
        ltt_min_30_prev = ltt_min_30 - datetime.timedelta(minutes=30)

        add_tick_to_candles(instrument, CANDLE_1MIN,  ltt_min_1,  ltt_min_1_prev,  tick)
        add_tick_to_candles(instrument, CANDLE_5MIN,  ltt_min_5,  ltt_min_5_prev,  tick)
        #add_tick_to_candles(instrument, CANDLE_15MIN, ltt_min_15, ltt_min_15_prev, tick)
        #add_tick_to_candles(instrument, CANDLE_30MIN, ltt_min_30, ltt_min_30_prev, tick)

    run_strategy(ticks,order_class_object)


def run_strategy(ticks,order_class_object):
    util.lgr_tk_hndlr.log(util.levels[0], 'run_strategy ticks length {}'.format(len(ticks)))
    for tick in ticks:
        instrument = tick["instrument_token"]
        ltp = tick["last_price"]
        #pdb.set_trace()
        if instrument not in INSTRUMENT_CANDLES_DATA.keys():
            continue

        data = INSTRUMENT_CANDLES_DATA[instrument][CANDLE_5MIN]
        signal = st.check_stochastic(data)
        util.lgr_tk_hndlr.log(util.levels[0],'signal returned by stochastic {} instrument {}'.format(signal,instrument))

        #two lines for testing
        """signal = st.buy_signal
        order_obj = {'price': ltp, 'instrument': int(instrument), 'order_type': signal}
        order_class_object.add_to_order_queue(order_obj)
        return"""
        if signal is not st.hold_signal:
            order_obj = {'price': ltp, 'instrument': instrument, 'order_type': signal}
            #pdb.set_trace()
            order_class_object.add_to_order_queue(order_obj)
            INSTRUMENT_CANDLES_DATA[instrument][CANDLE_1MIN].to_csv('SBIN_STRATEGY.CSV')