# -*- coding: utf-8 -*-
import pandas as pd
from config import STATS_HEADERS

def calculate_risk(row, y_data, dt_pct):
    """計算風險指標並回傳更新後的 row"""
    res = row.copy()
    
    # 更新 Yahoo 數據
    if y_data['price'] > 0:
        res['目前價'] = y_data['price']
        res['目前量'] = int(y_data['vol'] / 1000) # 轉張數
        res['PE'] = round(y_data['pe'], 2)
        res['PB'] = round(y_data['pb'], 2)
        res['成交值(億)'] = round((y_data['price'] * y_data['vol']) / 100000000, 2)
        
        # 計算警戒值
        hist = y_data['history']
        if len(hist) >= 7:
            ref_price = hist.iloc[-7]['Close']
            limit_price = round(ref_price * 1.32, 2)
            res['警戒價'] = limit_price
            if y_data['price'] > 0:
                res['差幅(%)'] = round(((limit_price - y_data['price']) / y_data['price']) * 100, 1)
        
        if len(hist) >= 5:
            avg_vol = hist['Volume'].mean()
            res['警戒量'] = int((avg_vol * 5) / 1000)

    # 更新 FinMind 當沖數據 (只有晚上有值，或者維持原值)
    if dt_pct > 0:
        res['當沖佔比(%)'] = dt_pct
        
    return res

def prepare_batch_update(original_records, updates):
    """準備寫回 Google Sheet 的資料格式 (依照 HEADER 排序)"""
    update_map = {str(r['代號']): r for r in updates}
    final_rows = []
    
    for row in original_records:
        code = str(row['代號'])
        if code in update_map:
            target = update_map[code]
            final_rows.append([target.get(h, '') for h in STATS_HEADERS])
        else:
            final_rows.append([row.get(h, '') for h in STATS_HEADERS])
            
    return final_rows
