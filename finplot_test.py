import finplot as fplt
import pandas as pd
import requests
from io import StringIO
from time import time
from tapy import Indicators
import yfinance as yf
import pdb
import numpy as np
import myutils

# load data and convert date
end_t = int(time())
start_t = end_t - 12*30*24*60*60 # twelve months
symbol = 'GUJGAS.NS'
interval = '1d'
url = 'https://query1.finance.yahoo.com/v7/finance/download/%s?period1=%s&period2=%s&interval=%s&events=history' % (symbol, start_t, end_t, interval)
r = requests.get(url, headers={'user-agent':'Mozilla/5.0'})

df = yf.download(tickers='GUJGASLTD.NS',period='5d',interval='5m')
ind= Indicators(df)
ind.awesome_oscillator(column_name='ao')
ind.bw_mfi(column_name='bw_mfi')
ind.smma(period=5, column_name='smma', apply_to='Close')
ind.sma(period=5, column_name='sma', apply_to='Close')
ind.alligator()
ind.fractals(column_name_high='fractals_high', column_name_low='fractals_low')
# df = pd.read_csv(StringIO(r.text))
lo_wicks = df[['Open','Close']].T.min() - df['Low']
df.loc[(lo_wicks>lo_wicks.quantile(0.99)), 'marker'] = df['Low']

ax,ax2 = fplt.create_plot('SBIN', rows=2)
axo = ax.overlay()
ind.df.replace([np.inf, -np.inf], np.nan, inplace=True)
ind.df.fillna(0)
ind.df['ts'] = ind.df.index.values.astype(np.int64) // 10 ** 9

ind.df.loc[ind.df['fractals_high'] == 0,'fractals_high'] = np.nan
ind.df.loc[ind.df['fractals_high'] > 0,'fractals_high'] = ind.df['High']+1
ind.df.loc[ind.df['fractals_low'] == 0,'fractals_low'] = np.nan
ind.df.loc[ind.df['fractals_low'] > 0,'fractals_low'] = ind.df['Low']-1

angularation = myutils.angularation(ind.df)
ind.df['angularation'] = angularation
ind.df.loc[ind.df['angularation'] == True,'angularation'] = ind.df['High']+2

fplt.set_y_range(-10, 10, ax=ax2)
fplt.plot(ind.df['fractals_high'], ax=ax, color='#4a5', style='^', legend='fractals_high')
fplt.plot(ind.df['angularation'], ax=ax, color='#0f0', style='^', legend='angularation')
fplt.plot(ind.df['fractals_low'], ax=ax, color='#f00', style="v", legend='fractals_low')
fplt.plot(ind.df[['alligator_jaws']], ax=ax, legend=['alligator_jaws'])
fplt.plot(ind.df[['alligator_teeth']], ax=ax, legend=['alligator_teeth'])
fplt.plot(ind.df[['alligator_lips']], ax=ax, legend=['alligator_lips'])
fplt.volume_ocv(ind.df[['Open','Close','ao']], ax=ax2)
fplt.volume_ocv(df[['Open','Close','Volume']], ax=axo)
fplt.candlestick_ochl(df[['Open', 'Close', 'High', 'Low']],ax=ax)

#######################################################
## update crosshair and legend when moving the mouse ##

hover_label = fplt.add_legend('', ax=ax)

def update_legend_text(x, y):
    row = df.loc[df[pd.Timestamp(x)]]
    # format html with the candle and set legend
    fmt = '<span style="color:#%s">%%.2f</span>' % ('0b0' if (row.Open<row.Close).all() else 'a00')
    rawtxt = '<span style="font-size:13px">%%s %%s</span> &nbsp; O%s C%s H%s L%s' % (fmt, fmt, fmt, fmt)
    hover_label.setText(rawtxt % (symbol, interval.upper(), row.Open, row.Close, row.High, row.Low))

def update_crosshair_text(x, y, xtext, ytext):
    ytext = '%s (Close%+.2f) (vol - %d)' % (ytext, (y - df.iloc[x].Close),df.iloc[x].Volume)
    return xtext, ytext

fplt.set_time_inspector(update_legend_text, ax=ax, when='hover')
fplt.add_crosshair_info(update_crosshair_text, ax=ax)

fplt.show()

"""
# df['Date'] = pd.to_datetime(df['Date']).astype('int64') # use finplot's internal representation, which is ns
df['Date'] = pd.to_datetime(df.index).astype('int64') # use finplot's internal representation, which is ns

ax,ax2 = fplt.create_plot('SBIN MACD', rows=2)

# plot macd with standard colors first
macd = df.Close.ewm(span=12).mean() - df.Close.ewm(span=26).mean()
signal = macd.ewm(span=9).mean()
df['macd_diff'] = macd - signal
fplt.volume_ocv(df[['Date','Open','Close','macd_diff']], ax=ax2, colorfunc=fplt.strength_colorfilter)
fplt.plot(macd, ax=ax2, legend='MACD')
fplt.plot(signal, ax=ax2, legend='Signal')

# change to b/w coloring templates for next plots
fplt.candle_bull_color = fplt.candle_bear_color = '#000'
fplt.volume_bull_color = fplt.volume_bear_color = '#333'
fplt.candle_bull_body_color = fplt.volume_bull_body_color = '#fff'

# plot price and volume
fplt.candlestick_ochl(df[['Date','Open','Close','High','Low']], ax=ax)

axo = ax.overlay()
fplt.volume_ocv(df[['Date','Open','Close','Volume']], ax=axo)
fplt.plot(df.Volume.ewm(span=24).mean(), ax=axo, color=1)



fplt.show()
"""