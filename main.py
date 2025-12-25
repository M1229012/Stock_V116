# -*- coding: utf-8 -*-
"""
V116.21 Backend Core (Public Safe Version)
功能：
1. [資料產出] 負責抓取所有股市數據，填入 STATS_HEADERS 指定的欄位。
2. [額度控管] 設定 MAX_API_CALLS = 450，超過即刻存檔下班，等待下小時 Cron Job 喚醒。
3. [分時策略] 
   - 15:00~20:59：全力抓取基本盤 (價/量/PE/PB)，當沖率暫填 0。
   - 21:00~23:59：檢查今日已更新但當沖為 0 者，補抓當沖數據。
4. [資安保護] 所有敏感金鑰皆透過環境變數讀取，程式碼內無敏感資訊。
"""

import os
import sys
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
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

# 自動安裝缺少的套件
try:
    import twstock
    import yfinance as yf
except ImportError:
    os.system('pip install twstock yfinance gspread google-auth python-dateutil requests pandas zoneinfo --quiet')
    import twstock
    import yfinance as yf

# ==========================================
# 1. 設定與常數
# ==========================================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger('yfinance')
logger.setLevel(logging.CRITICAL)
logger.disabled = True

UNIT_LOT = 1000
MAX_API_CALLS_PER_RUN = 450  # 🔥 額度上限：450次 (FinMind 限制約 600/hr)

# 後端產出欄位標準
STATS_HEADERS = [
    '代號', '名稱', '連續天數', '近30日注意次數', '近10日注意次數', '最近一次日期',
    '30日狀態碼', '10日狀態碼', '最快處置天數', '處置觸發原因', '風險等級', '觸發條件',
    '目前價', '警戒價', '差幅(%)', '目前量', '警戒量', '成交值(億)',
    '週轉率(%)', 'PE', 'PB', '當沖佔比(%)'
]

# Sheet 設定 (若需更高隱私，可將名稱改為 os.getenv('SHEET_NAME'))
SHEET_NAME = "台股注意股資料庫_V33"
PARAM_SHEET_NAME = "個股參數"

# 時區設定
try: TW_TZ = ZoneInfo("Asia/Taipei")
except: TW_TZ = ZoneInfo("UTC")

TARGET_DATE = datetime.now(TW_TZ)
IS_AFTER_9PM = TARGET_DATE.hour >= 21  # 判斷是否為晚上9點後

SAFE_CRAWL_TIME = dt_time(19, 0)
SAFE_MARKET_OPEN_CHECK = dt_time(16, 30)

# ==========================================
# 2. API 設定 (從環境變數讀取，安全)
# ==========================================
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"
# 🔥 這裡讀取環境變數，所以程式碼公開也沒關係
FINMIND_TOKEN = os.getenv('FinMind_1') or os.getenv('FinMind_2')

_FINMIND_CACHE = {}
API_CALL_COUNT = 0

# ============================
# 3. 工具函式
# ============================
CN_NUM = {"一":"1","二":"2","三":"3","四":"4","五":"5","六":"6","七":"7","八":"8","九":"9","十":"10"}
KEYWORD_MAP = {"起迄兩個營業日": 11, "當日沖銷": 13, "借券賣出": 12, "累積週轉率": 10, "週轉率": 4, "成交量": 9, "本益比": 6, "股價淨值比": 6, "溢折價": 8, "收盤價漲跌百分比": 1, "最後成交價漲跌": 1, "最近六個營業日累積": 1}

def normalize_clause_text(s: str) -> str:
    if not s: return ""
    s = str(s).replace("第ㄧ款", "第一款")
    for cn, dg in CN_NUM.items(): s = s.replace(f"第{cn}款", f"第{dg}款")
    return s.translate(str.maketrans("１２３４５６７８９０", "1234567890"))

def parse_clause_ids_strict(clause_text):
    if not isinstance(clause_text, str): return set()
    clause_text = normalize_clause_text(clause_text)
    ids = set(int(m) for m in re.findall(r'第\s*(\d+)\s*款', clause_text))
    if not ids:
        for k, v in KEYWORD_MAP.items():
            if k in clause_text: ids.add(v)
    return ids

def merge_clause_text(a, b):
    ids = parse_clause_ids_strict(a) | parse_clause_ids_strict(b)
    return "、".join([f"第{x}款" for x in sorted(ids)]) if ids else (a if len(a or "") >= len(b or "") else b)

def is_valid_accumulation_day(ids): return any(1 <= x <= 8 for x in ids)
def is_special_risk_day(ids): return any(9 <= x <= 14 for x in ids)
def get_ticker_suffix(market): return '.TWO' if any(k in str(market).upper() for k in ['上櫃', 'TWO', 'TPEX', 'OTC']) else '.TW'

def get_or_create_ws(sh, title, headers=None, rows=5000, cols=20):
    try:
        ws = sh.worksheet(title)
        if headers and ws.col_count < len(headers): ws.resize(rows=ws.row_count, cols=len(headers))
        return ws
    except:
        ws = sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))
        if headers: ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws

# ============================
# 4. API 核心 (計數器 + 單一 Token + 延遲)
# ============================
def finmind_get(dataset, data_id=None, start_date=None, end_date=None):
    global API_CALL_COUNT
    
    cache_key = (dataset, data_id, start_date, end_date)
    if cache_key in _FINMIND_CACHE: return _FINMIND_CACHE[cache_key].copy()

    # 額度保護
    if API_CALL_COUNT >= MAX_API_CALLS_PER_RUN:
        return pd.DataFrame()

    params = {"dataset": dataset}
    if data_id: params["data_id"] = str(data_id)
    if start_date: params["start_date"] = start_date
    if end_date: params["end_date"] = end_date
    
    headers = {"User-Agent": "Mozilla/5.0"}
    if FINMIND_TOKEN: headers["Authorization"] = f"Bearer {FINMIND_TOKEN}"

    for _ in range(3): # Retry
        API_CALL_COUNT += 1
        try:
            time.sleep(1.5) # 強制延遲
            r = requests.get(FINMIND_API_URL, params=params, headers=headers, timeout=10, verify=False)
            if r.status_code == 200:
                j = r.json()
                df = pd.DataFrame(j.get("data", []))
                if len(_FINMIND_CACHE) >= 2000: _FINMIND_CACHE.clear()
                _FINMIND_CACHE[cache_key] = df
                return df.copy()
            elif r.status_code == 429:
                print("⚠️ FinMind Rate Limit Reached.")
                API_CALL_COUNT = MAX_API_CALLS_PER_RUN + 1
                return pd.DataFrame()
            else:
                time.sleep(2)
        except: time.sleep(1)
    return pd.DataFrame()

# ============================
# 5. 資料處理邏輯
# ============================
def parse_roc_date(s):
    try: p=s.strip().split('/'); return date(int(p[0])+1911, int(p[1]), int(p[2]))
    except: return None

def parse_jail_period(s):
    if not s: return None, None
    d = s.split('～') if '～' in s else s.split('~')
    if len(d)<2 and '-' in s: d = s.split('-')
    if len(d)>=2:
        s_d, e_d = parse_roc_date(d[0]), parse_roc_date(d[1])
        if s_d and e_d: return s_d, e_d
    return None, None

def get_jail_map(sd, ed):
    print("🔒 下載處置名單...")
    jm = {}
    try:
        url = "https://www.twse.com.tw/rwd/zh/announcement/punish"
        r = requests.get(url, params={"startDate":sd.strftime("%Y%m%d"),"endDate":ed.strftime("%Y%m%d"),"response":"json"}, verify=False)
        for row in r.json().get("tables", [{}])[0].get("data", []):
            try:
                s, e = parse_jail_period(row[6])
                if s and e: jm.setdefault(row[2].strip(), []).append((s, e))
            except: continue
    except: pass
    return jm

def get_last_n_non_jail_trade_dates(code, cal, jm, ex, n=30):
    last_end = date(1900,1,1)
    if jm and code in jm: last_end = jm[code][-1][1]
    picked = []
    for d in reversed(cal):
        if d <= last_end: break
        if ex.get(code) and d in ex[code]: continue
        if jm and code in jm:
            is_j = False
            for s,e in jm[code]: 
                if s<=d<=e: is_j=True; break
            if is_j: continue
        picked.append(d)
        if len(picked)>=n: break
    return list(reversed(picked))

def fetch_history_data(code):
    try:
        time.sleep(1) 
        df = yf.Ticker(code).history(period="1y", auto_adjust=False)
        if not df.empty and df.index.tz: df.index = df.index.tz_localize(None)
        return df
    except: return pd.DataFrame()

def fetch_stock_fundamental(ticker_code):
    data = {'pe': 0, 'pb': 0}
    try:
        t = yf.Ticker(ticker_code)
        data['pe'] = t.info.get('trailingPE', t.info.get('forwardPE', 0)) or 0
        data['pb'] = t.info.get('priceToBook', 0) or 0
        data['pe'] = round(data['pe'], 2)
        data['pb'] = round(data['pb'], 2)
    except: pass
    return data

def get_daytrade_stats_finmind(stock_id, date_str):
    # 分流核心：9點前不抓當沖，省額度
    if not IS_AFTER_9PM: 
        return 0.0, 0.0
    
    start = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=15)).strftime("%Y-%m-%d")
    d = finmind_get("TaiwanStockDayTrading", data_id=stock_id, start_date=start, end_date=date_str)
    p = finmind_get("TaiwanStockPrice", data_id=stock_id, start_date=start, end_date=date_str)

    if p.empty or d.empty: return 0.0, 0.0
    try:
        m = pd.merge(p[['date','Trading_Volume']], d[['date','Volume']], on='date')
        if m.empty: return 0.0, 0.0
        m['date'] = pd.to_datetime(m['date']); m=m.sort_values('date')
        r6 = m.tail(6)
        if len(r6)<1: return 0.0, 0.0
        last = r6.iloc[-1]
        
        td = (last['Volume']/last['Trading_Volume']*100) if last['Trading_Volume']>0 else 0.0
        
        sum_vol = r6['Volume'].sum()
        sum_total = r6['Trading_Volume'].sum()
        avg = (sum_vol/sum_total*100) if sum_total>0 else 0.0
        
        return round(td, 2), round(avg, 2)
    except: return 0.0, 0.0

# ============================
# 6. 風險計算 (填滿所有欄位)
# ============================
def calculate_full_risk(stock_id, hist_df, fund_data, est_days, dt_today, dt_avg6, shares=1):
    res = {
        'risk_level': '低', 'trigger_msg': '', 'curr_price': 0, 
        'limit_price': 0, 'gap_pct': 999.0, 'curr_vol': 0, 'limit_vol': 0, 
        'turnover_val': 0, 'turnover_rate': 0, 
        'pe': fund_data.get('pe', 0), 'pb': fund_data.get('pb', 0), 
        'day_trade_pct': dt_today
    }

    if hist_df.empty: return res

    curr_close = float(hist_df.iloc[-1]['Close'])
    curr_vol_shares = float(hist_df.iloc[-1]['Volume'])
    
    res['curr_price'] = round(curr_close, 2)
    res['curr_vol'] = int(curr_vol_shares / 1000)
    res['turnover_val'] = round((curr_close * curr_vol_shares) / 100000000, 2)
    
    if shares > 1:
        res['turnover_rate'] = round((curr_vol_shares / shares) * 100, 2)
    
    if est_days <= 1: res['risk_level'] = '高'
    elif est_days <= 2: res['risk_level'] = '中'

    if len(hist_df) >= 7:
        ref_price = float(hist_df.iloc[-7]['Close'])
        res['limit_price'] = round(ref_price * 1.32, 2)
        if curr_close > 0:
            res['gap_pct'] = round(((res['limit_price'] - curr_close) / curr_close) * 100, 1)
            
    if len(hist_df) >= 60:
        avg_vol = hist_df['Volume'].iloc[-60:].mean()
        res['limit_vol'] = int((avg_vol * 5) / 1000)

    return res

# ============================
# 7. 主程式
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

def main():
    print(f"🚀 啟動 V116.21 | 時間: {TARGET_DATE} | 9PM模式: {IS_AFTER_9PM}")
    
    sh, _ = connect_google_sheets()
    if not sh: return

    # 1. 讀取現有資料
    ws_stats = get_or_create_ws(sh, "近30日熱門統計", headers=STATS_HEADERS)
    existing_data = ws_stats.get_all_records()
    
    today_str = TARGET_DATE.strftime("%Y-%m-%d")
    
    # 2. 建立更新檢查表 (分流邏輯)
    target_stocks_info = []
    
    for row in existing_data:
        code = str(row.get('代號'))
        if not code: continue
        
        last_date = str(row.get('最近一次日期'))
        dt_pct = row.get('當沖佔比(%)')
        try: dt_val = float(dt_pct)
        except: dt_val = 0.0
        
        mode = "SKIP"
        if last_date != today_str:
            mode = "FULL" # 需要更新股價
        elif IS_AFTER_9PM and dt_val == 0:
            mode = "DT_ONLY" # 股價已更新，只補當沖
            
        if mode != "SKIP":
            target_stocks_info.append({'code': code, 'mode': mode, 'data': row})

    print(f"📋 待處理: {len(target_stocks_info)} 檔 | 額度: {MAX_API_CALLS_PER_RUN}")

    # 3. 載入基本參數
    precise_db = {}
    try:
        ws_param = sh.worksheet(PARAM_SHEET_NAME)
        for r in ws_param.get_all_records():
            precise_db[str(r.get('代號'))] = {"market": r.get('市場','上市'), "shares": r.get('發行股數',1)}
    except: pass
    
    # 4. 開始執行
    updates = []
    processed = 0
    
    for item in target_stocks_info:
        if API_CALL_COUNT >= MAX_API_CALLS_PER_RUN:
            print("🛑 額度用盡，停止執行，等待下次排程。")
            break
            
        code = item['code']
        mode = item['mode']
        old_data = item['data']
        
        print(f"   [{processed+1}] {code} ({mode})...")
        
        suffix = get_ticker_suffix(precise_db.get(code, {}).get('market', '上市'))
        ticker = f"{code}{suffix}"
        
        # 股價與基本面 (Yahoo)
        hist = fetch_history_data(ticker)
        fund = fetch_stock_fundamental(ticker)

        # 當沖 (FinMind)
        dt_today, dt_avg6 = get_daytrade_stats_finmind(code, today_str)
        
        # 計算
        shares = 1
        try: shares = int(str(precise_db.get(code, {}).get('shares', 1)).replace(',',''))
        except: pass
        
        est_days = 99
        try: est_days = int(old_data.get('最快處置天數', 99))
        except: pass
        
        risk_res = calculate_full_risk(code, hist, fund, est_days, dt_today, dt_avg6, shares)
        
        # 更新欄位
        new_row = old_data.copy()
        new_row['最近一次日期'] = today_str
        for k, v in risk_res.items():
            # 對應 STATS_HEADERS 的欄位名稱做 mapping
            # (risk_res key) -> (Sheet Header)
            map_key = {
                'curr_price': '目前價', 'limit_price': '警戒價', 'gap_pct': '差幅(%)',
                'curr_vol': '目前量', 'limit_vol': '警戒量', 'turnover_val': '成交值(億)',
                'turnover_rate': '週轉率(%)', 'pe': 'PE', 'pb': 'PB', 'day_trade_pct': '當沖佔比(%)'
            }
            if k in map_key:
                new_row[map_key[k]] = v
        
        updates.append(new_row)
        processed += 1
        
    # 5. 寫回
    if updates:
        print("💾 儲存資料中...")
        update_map = {row['代號']: row for row in updates}
        final_rows = []
        for row in existing_data:
            code = str(row.get('代號'))
            # 如果有更新就用新的，沒有就用舊的
            target = update_map.get(code, row)
            # 轉成 list 準備寫入
            final_rows.append([target.get(h, '') for h in STATS_HEADERS])
                
        ws_stats.clear()
        ws_stats.append_row(STATS_HEADERS, value_input_option='USER_ENTERED')
        ws_stats.append_rows(final_rows, value_input_option='USER_ENTERED')
        
    print(f"\n✅ 執行結束。更新: {processed} 筆。API使用: {API_CALL_COUNT}")

if __name__ == "__main__":
    main()
