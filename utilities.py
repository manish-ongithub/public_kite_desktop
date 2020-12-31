from datetime import datetime,timedelta
import requests
import pandas as pd
import logging
import logging.config
import logging.handlers

import pdb

loggingDict = {
        'version': 1,
        'formatters': {
            'detailed': {
                'class': 'logging.Formatter',
                'format': '%(asctime)s %(name)-15s %(levelname)-8s %(processName)-10s %(message)s'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'detailed'
            },
            'file': {
                'class': 'logging.FileHandler',
                'filename': 'mplog.log',
                'mode': 'w',
                'formatter': 'detailed',
            },
            'foofile': {
                'class': 'logging.FileHandler',
                'filename': 'mplog-foo.log',
                'mode': 'w',
                'formatter': 'detailed',
            },
            'errors': {
                'class': 'logging.FileHandler',
                'filename': 'mplog-errors.log',
                'mode': 'a',
                'level': 'DEBUG',
                'formatter': 'detailed',
            },
        },
        'loggers': {
            'kite_websockets': {
                'handlers': ['file']
            },
            'tick_handler': {
                'handlers': ['file']
            },
            'orders': {
                'handlers': ['file']
            },
            'strategy': {
                'handlers': ['file']
            },
            'utility':{
                'handlers': ['file']
            }
        },
        'root': {
            'level': 'DEBUG',
            'handlers': ['console', 'file', 'errors']
        },
    }

logging.config.dictConfig(loggingDict)
root = logging.getLogger()
root.setLevel(logging.DEBUG)
levels = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR,logging.CRITICAL]
loggers = ['kite_websockets', 'tick_handler', 'orders','strategy','utility']

lgr_kite_web = logging.getLogger('kite_websockets')
lgr_tk_hndlr = logging.getLogger('tick_handler')
lgr_orders = logging.getLogger('orders')
lgr_strategy = logging.getLogger('strategy')
lgr_utility = logging.getLogger('utility')

access_token = 'qiNfMgftmmopwvhcFHuhnFAcY129btz0'
api_key = 'oopkd7v8qyouimdm'
kite_authorization = 'enctoken 9pZjUdeYy5B7ZKUQ+2M3HMu073+I6PtegWZqO/dFGYPkHYLkGVOGXy7hcLoPARWuPE+mr7NxpBTUf064bwEDn8BoGkG3Zg=='
#enctoken Put+7fIgBoxp/vksugIpV/hYss7gw0K00O/vzksNRomM1sdWtA7q7mB3xarK89JhZhkF9lyWEJuzH6hN7XEpTNlE/4aruA==

kite_url_intraday = 'https://kite.zerodha.com/oms/instruments/historical/245249/60minute?user_id=ZX6806&oi=1&from=2019-12-23&to=2020-12-23&ciqrandom=1608713279669'
kite_url_day = 'https://kite.zerodha.com/oms/instruments/historical/245249/day?user_id=ZX6806&oi=1&from=2019-12-23&to=2020-12-23&ciqrandom=1608713470527'
#GOLD21FEBFUT
#https://kite.zerodha.com/oms/instruments/historical/55851015/5minute?user_id=ZX6806&oi=1&from=2020-11-24&to=2020-12-24&ciqrandom=1608806444864

df_nse = pd.read_csv('NSE_INSTRUMENTS.CSV')
df_mcx = pd.read_csv('MCX_INSTRUMENTS.CSV')


def get_nse_instrument_details_by_name(name):
    for index,rows in df_nse.iterrows():
        if rows['tradingsymbol'] == name:
            return rows.to_dict()

    return None


def get_mcx_instrument_details_by_name(name):
    for index,rows in df_mcx.iterrows():
        if rows['tradingsymbol'] == name:
            return rows.to_dict()

    return None



def get_nifty_50_instruments():
    pass


def kite_intraday_url(instrument,minutes,days_count):

    to_date = datetime.now().strftime("%Y-%m-%d")
    from_date = (datetime.now() - timedelta(days=days_count)).strftime("%Y-%m-%d")
    base_url = 'https://kite.zerodha.com/oms/instruments/historical/'+str(instrument)
    if minutes == 1:
        base_url += '/minute?user_id=ZX6806&oi=1&from='+from_date+'&to='+to_date+'&ciqrandom=1608713279669'
    else:
        base_url += '/'+str(minutes)+'minute?user_id=ZX6806&oi=1&from='+from_date+'&to='+to_date+'&ciqrandom=1608713279669'

    return base_url



def get_kite_data(instrument,minutes,days_count):
    #pdb.set_trace()
    intraday_url = kite_intraday_url(instrument,minutes,days_count)
    lgr_utility.log(levels[1], intraday_url)
    r = requests.get(intraday_url, headers={'authorization': kite_authorization})
    if r.status_code == 200:
        js = r.json()
        candles = js['data']['candles']
        reclist= []
        timestamplist = []
        for rec in candles:
            raw_timestamp = rec[0]
            str_timestamp = raw_timestamp[:22]+':'+raw_timestamp[22:]
            timestamp_obj = datetime.fromisoformat(str_timestamp)
            timestamp_obj = timestamp_obj.replace(tzinfo=None)
            c_rec = {'Timestamp':timestamp_obj,'Open':rec[1],'High':rec[2],'Low':rec[3],'Close':rec[4],'Volume':rec[5],'OpenInterest':rec[6]}
            reclist.append(c_rec)
            timestamplist.append(timestamp_obj)

        df = pd.DataFrame(reclist, columns=['Open', 'High', 'Low', 'Close', 'Volume', 'OpenInterest'],index=timestamplist)
        return df
    else:
        lgr_utility.log(levels[3],'error in fetching record {} {}'.format(r.status_code,r.json()))
        return None