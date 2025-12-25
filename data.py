# -*- coding: utf-8 -*-
import requests
import pandas as pd
import gspread
import yfinance as yf
import time
import re
from datetime import datetime, timedelta, date
from config import *
from logic import parse_clause_ids_strict

_CURRENT_TOKEN_IDX = 0
_FINMIND_CACHE = {}

# --- 輔助函式: 型別安全轉換 ---
def _to_int(x):
    try:
        if x is None: return None
        if isinstance(x, str) and x.strip() == "": return None
        return int(float(x))
    except: return None

def _to_float(x):
    try:
        if x is None: return None
        if isinstance(x, str) and x.strip() == "": return None
        return float(x)
    except: return None

# --- 連線與工具 ---
def connect_google_sheets():
    try:
        key_path = "service_key.json"
        if not os.path.exists(key_path): return None
        gc = gspread.service_account(filename=key_path)
        try: sh = gc.open(SHEET_NAME)
        except: sh = gc.create(SHEET_NAME)
        return sh
    except: return None

def get_or_create_ws(sh, title, headers=None, rows=5000, cols=20):
    try:
        ws = sh.worksheet(title)
        return ws
    except:
        ws = sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))
        if headers: ws.append_row(headers)
        return ws

def load_precise_db_from_sheet(sh):
    try:
        ws = sh.worksheet(PARAM_SHEET_NAME)
        db = {}
        for r in ws.get_all_records():
            c = str(r.get('代號','')).strip()
            if c: db[c] = {'market': r.get('市場','上市'), 'shares': r.get('發行股數',1)}
        return db
    except: return {}

# --- 資料抓取: FinMind ---
def finmind_get(dataset, data_id=None, start_date=None, end_date=None):
    global _CURRENT_TOKEN_IDX
    cache_key = (dataset, data_id, start_date, end_date)
    if cache_key in _FINMIND_CACHE: return _FINMIND_CACHE[cache_key].copy()
    params = {"dataset": dataset}
    if data_id: params["data_id"] = str(data_id)
    if start_date: params["start_date"] = start_date
    if end_date: params["end_date"] = end_date
    if not FINMIND_TOKENS: return pd.DataFrame()
    
    for _ in range(4):
        headers = {"Authorization": f"Bearer {FINMIND_TOKENS[_CURRENT_TOKEN_IDX]}", "User-Agent": "Mozilla/5.0"}
        try:
            r = requests.get(FINMIND_API_URL, params=params, headers=headers, timeout=10)
            if r.status_code == 200:
                j = r.json()
                df = pd.DataFrame(j.get("data", []))
                if len(_FINMIND_CACHE) >= 2000: _FINMIND_CACHE.clear()
                _FINMIND_CACHE[cache_key] = df
                return df.copy()
            else:
                _CURRENT_TOKEN_IDX = (_CURRENT_TOKEN_IDX + 1) % len(FINMIND_TOKENS)
                time.sleep(2)
        except: time.sleep(1)
    return pd.DataFrame()

# --- 資料抓取: Yahoo History ---
def fetch_history_data(ticker):
    try:
        df = yf.Ticker(ticker).history(period="1y", auto_adjust=False)
        if not df.empty: df.index = df.index.tz_localize(None)
        return df
    except: return pd.DataFrame()

# --- 資料抓取: 基本面 (✅ 依照您的要求修改: 支援 Retry + None) ---
def fetch_stock_fundamental(code, ticker, precise_db, retries=3, sleep_sec=1.2):
    """
    回傳: shares, pe, pb (皆可能為 None)
    """
    # 1. 先從參數表拿 shares
    param_shares = None
    if str(code) in precise_db:
        param_shares = precise_db[str(code)].get('shares')
    
    shares = _to_int(param_shares)
    data = {"shares": shares, "pe": None, "pb": None}

    # 2. 嘗試抓取 Yahoo (帶 Retry)
    for attempt in range(1, retries + 1):
        try:
            t = yf.Ticker(ticker)
            # 嘗試獲取 info
            info = getattr(t, "info", None) or {}
            
            # shares: 參數表沒有才用 Yahoo
            if data["shares"] is None or data["shares"] <= 1:
                so = _to_int(info.get("sharesOutstanding"))
                if so and so > 1:
                    data["shares"] = so
                else:
                    # 再試 fast_info
                    fi = getattr(t, "fast_info", None) or {}
                    so2 = _to_int(fi.get("shares"))
                    if so2 and so2 > 1:
                        data["shares"] = so2
            
            # PE
            pe = _to_float(info.get("trailingPE"))
            if pe is None:
                pe = _to_float(info.get("forwardPE"))
            data["pe"] = pe
            
            # PB
            data["pb"] = _to_float(info.get("priceToBook"))
            
            # 成功取得資料就跳出
            return data

        except Exception:
            time.sleep(sleep_sec * attempt)
            
    return data

# --- 資料抓取: 當沖 (FinMind) ---
def get_daytrade_stats_finmind(code, date_str):
    end = date_str
    start = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=15)).strftime("%Y-%m-%d")
    df_dt = finmind_get("TaiwanStockDayTrading", code, start, end)
    df_p = finmind_get("TaiwanStockPrice", code, start, end)
    if df_dt.empty or df_p.empty: return 0.0, 0.0
    try:
        m = pd.merge(df_p[['date','Trading_Volume']], df_dt[['date','Volume']], on='date', how='inner')
        if m.empty: return 0.0, 0.0
        m = m.sort_values('date')
        last = m.iloc[-1]
        td = (last['Volume']/last['Trading_Volume']*100) if last['Trading_Volume']>0 else 0
        avg = m.tail(6); sum_v = avg['Volume'].sum(); sum_t = avg['Trading_Volume'].sum()
        avg_td = (sum_v/sum_t*100) if sum_t>0 else 0
        return round(td, 2), round(avg_td, 2)
    except: return 0.0, 0.0

# --- 爬蟲: 官方公告 ---
def get_daily_data(date_obj):
    date_str_nodash = date_obj.strftime("%Y%m%d")
    date_str = date_obj.strftime("%Y-%m-%d")
    rows = []
    print(f"📡 爬取公告 {date_str}...")

    # TWSE
    try:
        r = requests.get("https://www.twse.com.tw/rwd/zh/announcement/notice",
                         params={"startDate": date_str_nodash, "endDate": date_str_nodash, "response": "json"}, timeout=10)
        if r.status_code == 200 and 'data' in r.json():
            for i in r.json()['data']:
                code, name = str(i[1]).strip(), str(i[2]).strip()
                if len(code)==4 and code.isdigit():
                    raw = " ".join([str(x) for x in i])
                    ids = parse_clause_ids_strict(raw)
                    c_str = "、".join([f"第{k}款" for k in sorted(ids)]) or raw
                    rows.append({'日期':date_str, '市場':'TWSE', '代號':code, '名稱':name, '觸犯條款':c_str})
    except: pass

    # TPEx
    try:
        roc = f"{date_obj.year-1911}/{date_obj.month:02d}/{date_obj.day:02d}"
        r = requests.post("https://www.tpex.org.tw/www/zh-tw/bulletin/attention", data={'date':roc,'response':'json'}, headers={'User-Agent':'Mozilla/5.0','Referer':'https://www.tpex.org.tw/'}, timeout=10)
        if r.status_code == 200:
            res = r.json()
            target = res.get('data', [])
            if 'tables' in res: target = res['tables'][0].get('data', [])
            # Filter date
            final_target = [row for row in target if len(row)>5 and (str(row[5]).strip() in [roc, date_str])]
            for i in final_target:
                code, name = str(i[1]).strip(), str(i[2]).strip()
                if len(code)==4 and code.isdigit():
                    raw = " ".join([str(x) for x in i])
                    ids = parse_clause_ids_strict(raw)
                    c_str = "、".join([f"第{k}款" for k in sorted(ids)]) or raw
                    rows.append({'日期':date_str, '市場':'TPEx', '代號':code, '名稱':name, '觸犯條款':c_str})
    except: pass
    return rows

# --- 處置名單 & 大盤 & 排除日 ---
# (為了縮短篇幅，這部分與上一版完全相同，請確保您的 data.py 包含 update_market_monitoring_log, get_jail_map, get_official_trading_calendar 等)
# 請確認您的 data.py 保留了完整的 update_market_monitoring_log, get_jail_map, get_ticker_suffix (上一輪補的)
# 這裡僅列出最重要的 ticker_suffix 供確認
def get_ticker_suffix(market_type):
    m = str(market_type).upper().strip()
    if any(k in m for k in ['上櫃', 'TWO', 'TPEX', 'OTC']): return '.TWO'
    return '.TW'

# ... (其餘 get_jail_map, get_official_trading_calendar, update_market_monitoring_log 請保留原樣) ...
# ⚠️ 注意：請確保 data.py 內有完整的 update_market_monitoring_log, get_jail_map, get_official_trading_calendar 函式
# (我在這裡省略是為了避免回覆過長被截斷，實際檔案請保留)
