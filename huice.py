# -*- coding: utf-8 -*-
"""
Created on Sun Oct 30 22:20:06 2016

@author: Administrator
"""

import pandas as pd
import os
import time
from multiprocessing import Pool
import sys
import numpy as np
import datetime

ISOTIMEFORMAT = '%Y-%m-%d %X'

input_path = 'D:/trading_data/stock data/'
output_file = 'D:/googledrive/stock/quant/gushequ_chaoduan/code_selection/result.csv'

cost = 1.5/1000
rate = 8.4/100

def profit(x,rate):
    if (x['date+1_open_qfq'] / x['close_qfq'] - 1 >= 9.9/100) and (x['date+1_high_qfq'] == x['date+1_low_qfq']):
        return 0
    elif x['date+2_high_qfq'] / x['date+1_close_qfq'] - 1 >= rate:
        return x['date+1_close_qfq'] * (1 + rate) / x['date+1_open_qfq'] - 1 - cost
    else:
        return x['date+2_close_qfq'] / x['date+1_open_qfq'] - 1 - cost

req_cols = ('code', 'date', 'open', 'high', 'low', 'close', 'volume', 'adjust_price_f')


def code_selection(file_name):
    
    print("Code processed: %s" % (file_name[2:8]) )
    
    
    code_hist_all = pd.read_csv(input_path + file_name)
        
    code_hist_all = code_hist_all.loc[:,req_cols]
    
    code_hist_all.loc[:,'code'] = [ code[2:] for code in code_hist_all.loc[:,'code']]
    code_hist_all = code_hist_all.sort_values('date').reset_index(drop = True)
    code_hist_all = code_hist_all.rename(columns={'adjust_price_f': 'close_qfq'})
    code_hist_all.loc[:,'open_qfq'] = code_hist_all.open / code_hist_all.close * code_hist_all.close_qfq
    code_hist_all.loc[:,'high_qfq'] = code_hist_all.high / code_hist_all.close * code_hist_all.close_qfq
    code_hist_all.loc[:,'low_qfq'] = code_hist_all.low / code_hist_all.close * code_hist_all.close_qfq
    
    code_hist_all.loc[:,'ma5_qfq'] = code_hist_all.loc[:,'close_qfq'].rolling(5).mean()        
    code_hist_all.loc[:,'ma10_qfq'] = code_hist_all.loc[:,'close_qfq'].rolling(10).mean()
    code_hist_all.loc[:,'ma20_qfq'] = code_hist_all.loc[:,'close_qfq'].rolling(20).mean()
    code_hist_all.loc[:,'High_in_5_days_qfq'] = code_hist_all.loc[:,'high_qfq'].rolling(5).max()
    code_hist_all.loc[:,'date-1_volume'] = code_hist_all.loc[:,'volume'].shift(1)
    code_hist_all.loc[:,'date-1_high_qfq'] = code_hist_all.loc[:,'high_qfq'].shift(1)
    code_hist_all.loc[:,'date-1_close_qfq'] = code_hist_all.loc[:,'close_qfq'].shift(1)
    code_hist_all.loc[:,'date-1_open_qfq'] = code_hist_all.loc[:,'open_qfq'].shift(1)
    code_hist_all.loc[:,'date-1_low_qfq'] = code_hist_all.loc[:,'low_qfq'].shift(1)
    code_hist_all.loc[:,'date-2_volume'] = code_hist_all.loc[:,'volume'].shift(2)
    code_hist_all.loc[:,'date-2_close_qfq'] = code_hist_all.loc[:,'close_qfq'].shift(2)
    code_hist_all.loc[:,'date-2_open_qfq'] = code_hist_all.loc[:,'open_qfq'].shift(2)
    code_hist_all.loc[:,'date-2_high_qfq'] = code_hist_all.loc[:,'high_qfq'].shift(2)
    code_hist_all.loc[:,'date-2_low_qfq'] = code_hist_all.loc[:,'low_qfq'].shift(2)
    code_hist_all.loc[:,'date+2_open_qfq'] = code_hist_all.loc[:,'open_qfq'].shift(-2)
    code_hist_all.loc[:,'date+2_close_qfq'] = code_hist_all.loc[:,'close_qfq'].shift(-2)
    code_hist_all.loc[:,'date+2_high_qfq'] = code_hist_all.loc[:,'high_qfq'].shift(-2)
    code_hist_all.loc[:,'date+2_low_qfq'] = code_hist_all.loc[:,'low_qfq'].shift(-2)
    code_hist_all.loc[:,'date+2'] = code_hist_all.loc[:,'date'].shift(-2)
    code_hist_all.loc[:,'date+1_open_qfq'] = code_hist_all.loc[:,'open_qfq'].shift(-1)
    code_hist_all.loc[:,'date+1_close_qfq'] = code_hist_all.loc[:,'close_qfq'].shift(-1)
    code_hist_all.loc[:,'date+1_high_qfq'] = code_hist_all.loc[:,'high_qfq'].shift(-1)
    code_hist_all.loc[:,'date+1_low_qfq'] = code_hist_all.loc[:,'low_qfq'].shift(-1)
    code_hist_all.loc[:,'date+1'] = code_hist_all.loc[:,'date'].shift(-1)
    code_hist_all.loc[:,'date-10_close_qfq'] = code_hist_all.loc[:,'close_qfq'].shift(10)

    code_hist_all = code_hist_all.loc[code_hist_all['date'] >= '2006-01-01',:]
    code_hist_all = code_hist_all.reset_index(drop = True)
   
#   replace all NaN value with empty string
   
    code_hist_all = code_hist_all.fillna(0)
  
    code_selection_result = code_hist_all.loc[

# standard restrictions from policy
    (code_hist_all.loc[:,'date+2_close_qfq'] != 0) 
    
# 以下block为策略逻辑    
    & (code_hist_all.loc[:,'date-1_volume'] < code_hist_all.loc[:,'date-2_volume'] * 2)  
    & (code_hist_all.loc[:,'open_qfq'] > 0) 
    & (code_hist_all.loc[:,'volume'] < code_hist_all.loc[:,'date-1_volume'] * 2) 
    & (code_hist_all.loc[:,'high_qfq'] == code_hist_all.loc[:,'High_in_5_days_qfq']) 
    & (code_hist_all.loc[:,'low_qfq'] < code_hist_all.loc[:,'date-1_high_qfq']) 
    & (code_hist_all.loc[:,'date-1_close_qfq'] > code_hist_all.loc[:,'date-1_open_qfq'] * 1.02) 
    & (code_hist_all.loc[:,'ma10_qfq'] > code_hist_all.loc[:,'ma20_qfq']) 
    & (code_hist_all.loc[:,'date-1_close_qfq'] > code_hist_all.loc[:,'date-2_close_qfq'] * 1.095) 
    & (code_hist_all.loc[:,'close_qfq'] < code_hist_all.loc[:,'high_qfq'] * 0.97)
    & (code_hist_all.loc[:,'date-10_close_qfq'] * 1.5 >= code_hist_all.loc[:,'close_qfq'])
    
# adhoc restrictions
    & (code_hist_all.loc[:,'date'] >= '2006-01-01')

#     这一句是通达信软件的逻辑, 不考虑刚上市20天以内的股票
    & (code_hist_all.loc[:,'ma20_qfq'] != 0) 

# delete last two rows
    & (code_hist_all.loc[:,'date+2'] != 0)
    
    ,:]
                
    if not code_selection_result.empty:
        code_selection_result.loc[:,'profit_8.4'] = code_selection_result.apply(lambda x: profit(x,rate), axis=1)

    return code_selection_result    
    
if __name__ == '__main__':
    
    pool = Pool(4)

    q = []
    result = pd.DataFrame()

    start_time = datetime.datetime.now()
    
    for file_name in os.listdir(input_path):
        
        q.append(pool.apply_async(code_selection, args=(file_name,)))

    pool.close()
    pool.join()    
    
    for i in q:
        result = result.append(i.get())
    
    print("Total Records:", len(result))
    print("Total Stocks:",len(os.listdir(input_path)))
        
    if os.path.exists(output_file):
        result.to_csv(output_file, mode='a', header=None, index=False)
    else:
        result.to_csv(output_file, index=False)
    
    end_time = datetime.datetime.now()
    
    print('Program start at: %s \nProgram finished at: %s' % (start_time, end_time))
    print('Time used:', end_time - start_time)