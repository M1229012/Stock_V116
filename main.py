# -*- coding: utf-8 -*-
"""
V116.22 後端救援版 (All-FinMind Core)
修正重點：
1. [移除 Yahoo] 歷史股價、PE、PB 全部改用 FinMind 抓取，解決 Zeabur IP 被封鎖導致資料為 0 的問題。
2. [額度計算] 一檔股票需呼叫 4 次 API (股價、PER、PBR、當沖)，因此每小時限制處理 120 檔股票。
3. [資料修復] 執行後將自動修復 Google Sheet 中的 0 值。
"""

import os
import time
import pandas as pd
import numpy as np
import requests
import re
import gspread
import logging
import urllib3
from google.oauth2.service_account import Credentials
from google.auth import default
from datetime import datetime, timedelta, time as dt_time, date
from zoneinfo import ZoneInfo

# 自動安裝缺少的套件
try:
    import twstock
except ImportError:
    os.system('pip install twstock gspread google-auth requests pandas zoneinfo --quiet')
    import twstock

# ==========================================
# 1. 設定與常數
# ==========================================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

UNIT_LOT = 1000
# 🔥 關鍵：FinMind 一檔股票要抓 4 次 (Price, PER, PBR, DayTrading)
# 600 (上限) / 4 = 150。保險起見，設定每小時只跑 120 檔。
MAX_STOCKS_PER_RUN = 120 

STATS_HEADERS = [
    '代號', '名稱', '連續天數', '近30日注意次數', '近10日注意次數', '最近一次日期',
    '30日狀態碼', '10日狀態碼', '最快處置天數', '處置觸發原因', '風險等級', '觸發條件',
    '目前價', '警戒價', '差幅(%)', '目前量', '警戒量', '成交值(億)',
    '週轉率(%)', 'PE', 'PB', '當沖佔比(%)'
]

SHEET_NAME = "台股注意股資料庫_V33"
PARAM_SHEET_NAME = "個股參數"

try: TW_TZ = ZoneInfo("Asia/Taipei")
except: TW_TZ = ZoneInfo("UTC")

TARGET_DATE = datetime.now(TW_TZ)
IS_AFTER_9PM = TARGET_DATE.hour >= 21

# ==========================================
# 2. API 設定
# ==========================================
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"
FINMIND_TOKEN = os.getenv('FinMind_1') or os.getenv('FinMind_2')

_FINMIND_CACHE = {}
API_CALL_COUNT = 0

# ============================
# 3. FinMind 核心 (取代 Yahoo)
# ============================
def finmind_get(dataset, data_id=None, start_date=None, end_date=None):
    global API_CALL_COUNT
    cache_key = (dataset, data_id, start_date, end_date)
    if cache_key in _FINMIND_CACHE: return _FINMIND_CACHE[cache_key].copy()

    params = { "dataset": dataset, "data_id": str(data_id), "start_date": start_date, "end_date": end_date }
    headers = {"User-Agent": "Mozilla/5.0"}
    if FINMIND_TOKEN: headers["Authorization"] = f"Bearer {FINMIND_TOKEN}"

    for _ in range(3):
        API_CALL_COUNT += 1
        try:
            time.sleep(1.2) # 避免太快
            r = requests.get(FINMIND_API_URL, params=params, headers=headers, timeout=10, verify=False)
            if r.status_code == 200:
                j = r.json()
                df = pd.DataFrame(j.get("data", []))
                if not df.empty:
                    _FINMIND_CACHE[cache_key] = df
                return df
            elif r.status_code == 429:
                print("⚠️ FinMind 429 (Rate Limit).")
                return pd.DataFrame()
            time.sleep(2)
        except: time.sleep(1)
    return pd.DataFrame()

# [关键] 改用 FinMind 抓歷史股價 (取代 yfinance)
def fetch_history_data_finmind(stock_id, days=120):
    end_str = TARGET_DATE.strftime("%Y-%m-%d")
    start_str = (TARGET_DATE - timedelta(days=days)).strftime("%Y-%m-%d")
    
    df = finmind_get("TaiwanStockPrice", data_id=stock_id, start_date=start_str, end_date=end_str)
    
    if df.empty: return pd.DataFrame()
    
    # 欄位標準化以符合計算邏輯
    df = df.rename(columns={
        "date": "Date", "open": "Open", "max": "High", "min": "Low", "close": "Close", "Trading_Volume": "Volume"
    })
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.set_index('Date').sort_index()
    
    # 確保數值型態
    cols = ['Open', 'High', 'Low', 'Close', 'Volume']
    for c in cols: df[c] = pd.to_numeric(df[c], errors='coerce')
    
    return df

# [关键] 改用 FinMind 抓 PE/PB
def fetch_fundamental_finmind(stock_id):
    date_str = TARGET_DATE.strftime("%Y-%m-%d")
    # 往前抓幾天避免假日沒資料
    start_str = (TARGET_DATE - timedelta(days=5)).strftime("%Y-%m-%d")
    
    res = {'pe': 0.0, 'pb': 0.0}
    
    # PE
    df_pe = finmind_get("TaiwanStockPER", data_id=stock_id, start_date=start_str, end_date=date_str)
    if not df_pe.empty:
        res['pe'] = float(df_pe.iloc[-1]['PER'])
        
    # PB
    df_pb = finmind_get("TaiwanStockPBR", data_id=stock_id, start_date=start_str, end_date=date_str)
    if not df_pb.empty:
        res['pb'] = float(df_pb.iloc[-1]['PBR'])
        
    return res

# [關鍵] 抓當沖
def get_daytrade_finmind(stock_id, date_str):
    # 9點前不抓，除非強制
    if not IS_AFTER_9PM: return 0.0
    
    start = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
    d = finmind_get("TaiwanStockDayTrading", data_id=stock_id, start_date=start, end_date=date_str)
    p = finmind_get("TaiwanStockPrice", data_id=stock_id, start_date=start, end_date=date_str)
    
    if p.empty or d.empty: return 0.0
    
    try:
        m = pd.merge(p[['date','Trading_Volume']], d[['date','Volume']], on='date')
        if m.empty: return 0.0
        m = m.sort_values('date')
        last = m.iloc[-1]
        
        # 只要當天的佔比
        if last['Trading_Volume'] > 0:
            return round((last['Volume'] / last['Trading_Volume']) * 100, 2)
    except: pass
    return 0.0

# ============================
# 4. 計算邏輯
# ============================
def calculate_risk(stock_id, hist, fund, est_days, dt_pct, shares=1):
    res = {
        'curr_price': 0, 'limit_price': 0, 'gap_pct': 999.0, 
        'curr_vol': 0, 'limit_vol': 0, 'turnover_val': 0, 'turnover_rate': 0,
        'pe': fund['pe'], 'pb': fund['pb'], 'day_trade_pct': dt_pct,
        'risk_level': '低', 'trigger_msg': ''
    }
    
    if hist.empty: return res
    
    last = hist.iloc[-1]
    res['curr_price'] = last['Close']
    res['curr_vol'] = int(last['Volume'] / 1000)
    res['turnover_val'] = round((last['Close'] * last['Volume']) / 100000000, 2)
    
    if shares > 1:
        res['turnover_rate'] = round((last['Volume'] / shares) * 100, 2)
        
    # 簡易風險模擬 (還原您原本的邏輯)
    if est_days <= 1: res['risk_level'] = '高'
    elif est_days <= 2: res['risk_level'] = '中'
    
    # 警戒價 (Ref * 1.32)
    if len(hist) >= 7:
        ref = hist.iloc[-7]['Close']
        res['limit_price'] = round(ref * 1.32, 2)
        if res['curr_price'] > 0:
            res['gap_pct'] = round(((res['limit_price'] - res['curr_price']) / res['curr_price']) * 100, 1)
            
    # 警戒量 (60日均量 * 5)
    if len(hist) >= 60:
        avg_vol = hist.iloc[-60:]['Volume'].mean()
        res['limit_vol'] = int((avg_vol * 5) / 1000)
        
    return res

def get_ticker_suffix(market): return '.TWO' if '上櫃' in str(market) else '.TW'

# ============================
# 5. 主程式
# ============================
def connect_google_sheets():
    try:
        key = "/service_key.json" if os.path.exists("/service_key.json") else "service_key.json"
        if not os.path.exists(key): return None, None
        gc = gspread.service_account(filename=key)
        try: sh = gc.open(SHEET_NAME)
        except: sh = gc.create(SHEET_NAME)
        return sh, None
    except: return None, None

def get_or_create_ws(sh, title, headers=None):
    try: ws = sh.worksheet(title)
    except: 
        ws = sh.add_worksheet(title=title, rows="5000", cols="20")
        if headers: ws.append_row(headers)
    return ws

def main():
    print(f"🚀 啟動 V116.22 救援版 | {TARGET_DATE}")
    
    sh, _ = connect_google_sheets()
    if not sh: return

    ws_stats = get_or_create_ws(sh, "近30日熱門統計", headers=STATS_HEADERS)
    existing_data = ws_stats.get_all_records()
    
    today_str = TARGET_DATE.strftime("%Y-%m-%d")
    
    # 建立任務清單
    target_list = []
    for row in existing_data:
        code = str(row.get('代號'))
        last_date = str(row.get('最近一次日期'))
        # 如果日期不是今天，或者是今天但數值為0 (被之前的錯誤洗掉)，就重新抓
        try: price = float(row.get('目前價', 0))
        except: price = 0
        
        if last_date != today_str or price == 0:
            target_list.append({'code': code, 'data': row, 'mode': 'FULL'})
        elif IS_AFTER_9PM:
            # 9點後補當沖
            try: dt = float(row.get('當沖佔比(%)', 0))
            except: dt = 0
            if dt == 0:
                target_list.append({'code': code, 'data': row, 'mode': 'DT'})

    print(f"📋 待處理: {len(target_list)} 檔 (本次上限 {MAX_STOCKS_PER_RUN} 檔)")
    
    # 載入股本參數
    precise_db = {}
    try:
        ws_p = sh.worksheet(PARAM_SHEET_NAME)
        for r in ws_p.get_all_records():
            precise_db[str(r.get('代號'))] = r.get('發行股數', 1)
    except: pass

    # 執行
    updates = []
    processed = 0
    
    for item in target_list:
        if processed >= MAX_STOCKS_PER_RUN:
            print("🛑 達到本小時處理上限，停止並存檔。")
            break
            
        code = item['code']
        old_data = item['data']
        mode = item['mode']
        
        print(f"   [{processed+1}] {code} ...", end="\r")
        
        # 1. 抓歷史股價 (FinMind)
        hist = fetch_history_data_finmind(code)
        
        # 2. 抓基本面 (FinMind)
        fund = fetch_fundamental_finmind(code)
        
        # 3. 抓當沖 (FinMind)
        dt_val = get_daytrade_finmind(code, today_str)
        
        # 4. 計算
        shares = 1
        try: shares = int(str(precise_db.get(code, 1)).replace(',',''))
        except: pass
        
        est_days = 99
        try: est_days = int(old_data.get('最快處置天數', 99))
        except: pass
        
        res = calculate_risk(code, hist, fund, est_days, dt_val, shares)
        
        # 5. 更新
        new_row = old_data.copy()
        new_row['最近一次日期'] = today_str
        
        # 如果抓不到資料 (hist empty)，保留舊值或填0，避免錯誤
        if not hist.empty:
            new_row['目前價'] = res['curr_price']
            new_row['警戒價'] = res['limit_price']
            new_row['差幅(%)'] = res['gap_pct']
            new_row['目前量'] = res['curr_vol']
            new_row['警戒量'] = res['limit_vol']
            new_row['成交值(億)'] = res['turnover_val']
            new_row['週轉率(%)'] = res['turnover_rate']
            new_row['PE'] = res['pe']
            new_row['PB'] = res['pb']
            # 當沖只有在有值的時候才更新
            if res['day_trade_pct'] > 0:
                new_row['當沖佔比(%)'] = res['day_trade_pct']
        
        updates.append(new_row)
        processed += 1
        
    if updates:
        print(f"\n💾 正在寫入 {len(updates)} 筆資料...")
        update_map = {row['代號']: row for row in updates}
        final_rows = []
        for row in existing_data:
            code = str(row.get('代號'))
            target = update_map.get(code, row)
            final_rows.append([target.get(h, '') for h in STATS_HEADERS])
            
        ws_stats.clear()
        ws_stats.append_row(STATS_HEADERS, value_input_option='USER_ENTERED')
        ws_stats.append_rows(final_rows, value_input_option='USER_ENTERED')
        
    print(f"\n✅ 完成。API使用次數: {API_CALL_COUNT}")

if __name__ == "__main__":
    main()
