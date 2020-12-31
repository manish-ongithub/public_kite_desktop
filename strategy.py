import talib
import backtrader as bt
import pdb

buy_signal = 1
sell_signal = -1
hold_signal = 0

uptrend = 1
downtrend = 0


def check_rsi(data):
    rsi = talib.RSI(data.Close, period=14)
    last_rsi_value = rsi[len(rsi) - 1]
    if last_rsi_value <= 30:
        return buy_signal
    elif last_rsi_value >= 70:
        return sell_signal
    else:
        return  hold_signal


def check_stochastic(data):
    fastk_period  = 14
    slowk_period  = 3
    slowk_maptype = 0
    slowd_period  = 3
    slowd_matype  = 0
    slowk, slowd = talib.STOCH(data.High, data.Low, data.Close, fastk_period=fastk_period, slowk_period=slowk_period, slowk_matype=slowk_maptype,
                               slowd_period=slowd_period,slowd_matype=slowd_matype)
    last_slowk = slowk[len(slowk) - 1]
    last_slowd = slowd[len(slowd) - 1]
    if last_slowk <= 20 and last_slowd <= 20:
        if last_slowk > last_slowd:
            return buy_signal
    if last_slowk >= 80  and last_slowd >= 80:
        if last_slowk < last_slowd:
            return sell_signal

    return hold_signal