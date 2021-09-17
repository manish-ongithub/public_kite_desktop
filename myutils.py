import pandas as pd
import numpy as np


def angularation(data):
    '''
    :param two_candles_indes: index of next-to-last candle
    :return:True if there is a significant angularation, else False
    candles indicies: 0 - Open time, 1 - Open, 2 - High, 3 - Low, 4 - Close, 5 - Volume, 6 - Close time
    We measure distance between last bar mid price and alligator jaw and same distance -10 indexes from this point.
    It should be greater than 2.5
    '''
    # print(len(self.candles_all_time))
    angularation_val = []
    for idx, row in data.iterrows():
        if np.isnan(data.loc[idx, 'alligator_jaws']):
            angularation_val.append(np.nan)
            continue
        else:
            curr_loc = data.index.get_loc(idx)
            curr_jaw_val = data.loc[idx, 'alligator_jaws']
            curr_high = data.loc[idx, 'High']
            curr_low = data.loc[idx, 'Low']
            prev_10row = data.iloc[curr_loc - 10]
            # prev_jaw_val = data.loc[idx - 10, 'alligator_jaws']
            prev_high = prev_10row['High']
            prev_low = prev_10row['Low']
            dist1 = abs(float(curr_high) + float(curr_low) / 2  - curr_jaw_val)
            dist2 = abs(float(prev_high) + float(prev_low) / 2 - curr_jaw_val)
            if dist1 / dist2 > 1.012 or dist1 / dist2 < 0.9:
                # print('True')
                angularation_val.append(True)
            else:
                angularation_val.append(np.nan)

    return angularation_val

