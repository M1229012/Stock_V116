# -*- coding: utf-8 -*-
import requests
import pandas as pd
import gspread
import yfinance as yf
import time
import re
from datetime import datetime, timedelta, date
from google.oauth2.service_account import Credentials
from config import *
from logic import parse_clause_ids_strict

_CURRENT_TOKEN_IDX = 0
_FINMIND_CACHE = {}

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

# ============================
# 🔥 官方公告爬蟲 (V116.18 原版 100% 還原)
# ============================
def get_daily_data(date_obj):
    date_str_nodash = date_obj.strftime("%Y%m%d")
    date_str = date_obj.strftime("%Y-%m-%d")
    rows = []
    error_count = 0

    print(f"📡 嘗試爬取官方公告 (日期: {date_str})...")

    # 1. TWSE
    try:
        r = requests.get("https://www.twse.com.tw/rwd/zh/announcement/notice",
                         params={"startDate": date_str_nodash, "endDate": date_str_nodash, "response": "json"}, timeout=10)
        if r.status_code == 200:
            d = r.json()
            if 'data' in d:
                for i in d['data']:
                    code = str(i[1]).strip()
                    name = str(i[2]).strip()
                    if not (code.isdigit() and len(code) == 4): continue
                    raw_text = " ".join([str(x) for x in i])
                    ids = parse_clause_ids_strict(raw_text)
                    clause_str = "、".join([f"第{k}款" for k in sorted(ids)])
                    if not clause_str: clause_str = raw_text
                    rows.append({'日期': date_str, '市場': 'TWSE', '代號': code, '名稱': name, '觸犯條款': clause_str})
        else: error_count += 1
    except: error_count += 1

    # 2. TPEx
    try:
        roc_date = f"{date_obj.year-1911}/{date_obj.month:02d}/{date_obj.day:02d}"
        headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.tpex.org.tw/'}
        r = requests.post("https://www.tpex.org.tw/www/zh-tw/bulletin/attention", data={'date': roc_date, 'response': 'json'}, headers=headers, timeout=10)
        if r.status_code == 200:
            res = r.json()
            target = []
            if 'tables' in res:
                 for t in res['tables']: target.extend(t.get('data', []))
            elif 'data' in res: target = res['data']

            # 🔥 [關鍵修正] 逐列檢查日期
            filtered_target = []
            if target:
                for row in target:
                    if len(row) > 5:
                        row_date = str(row[5]).strip()
                        if row_date == roc_date or row_date == date_str:
                            filtered_target.append(row)
            target = filtered_target

            for i in target:
                code = str(i[1]).strip()
                name = str(i[2]).strip()
                if not (code.isdigit() and len(code) == 4): continue
                raw_text = " ".join([str(x) for x in i])
                ids = parse_clause_ids_strict(raw_text)
                clause_str = "、".join([f"第{k}款" for k in sorted(ids)])
                if not clause_str: clause_str = raw_text
                rows.append({'日期': date_str, '市場': 'TPEx', '代號': code, '名稱': name, '觸犯條款': clause_str})
        else: error_count += 1
    except: error_count += 1

    if error_count >= 2 and not rows: return None
    if rows: print(f"✅ 成功抓到 {len(rows)} 檔注意股。")
    else: print(f"⚠️ 該日 ({date_str}) 查無資料。")
    return rows

# --- Jail Map & Calendar ---
def parse_roc_date(roc_date_str):
    try:
        roc_date_str = str(roc_date_str).strip()
        parts = re.split(r'[/-]', roc_date_str)
        if len(parts) == 3:
            year = int(parts[0]) + 1911
            month = int(parts[1])
            day = int(parts[2])
            return date(year, month, day)
    except: return None
    return None

def parse_jail_period(period_str):
    if not period_str: return None, None
    dates = []
    if '～' in period_str: dates = period_str.split('～')
    elif '~' in period_str: dates = period_str.split('~')
    elif '-' in period_str and '/' in period_str:
        if period_str.count('-') == 1: dates = period_str.split('-')
    
    if len(dates) >= 2:
        start_date = parse_roc_date(dates[0].strip())
        end_date = parse_roc_date(dates[1].strip())
        if start_date and end_date:
            return start_date, end_date
    return None, None

def get_jail_map(start_date_obj, end_date_obj):
    print("🔒 正在下載處置(Jail)名單以建立濾網...")
    jail_map = {}
    s_str = start_date_obj.strftime("%Y%m%d")
    e_str = end_date_obj.strftime("%Y%m%d")

    # 1) TWSE (Listing)
    try:
        url = "https://www.twse.com.tw/rwd/zh/announcement/punish"
        r = requests.get(url, params={"startDate": s_str, "endDate": e_str, "response": "json"}, timeout=10)
        j = r.json()

        def find_idx(fields, candidates):
            for c in candidates:
                if c in fields: return fields.index(c)
            return None

        if isinstance(j.get("tables"), list) and j["tables"]:
            t = j["tables"][0]
            fields = t.get("fields", [])
            data_rows = t.get("data", [])

            idx_code = find_idx(fields, ["證券代號", "有價證券代號"])
            if idx_code is None: idx_code = 2

            idx_period = find_idx(fields, ["處置起迄時間", "處置起訖時間"])
            if idx_period is None: idx_period = 6

            for row in data_rows:
                try:
                    code = str(row[idx_code]).strip()
                    period_str = str(row[idx_period]).strip()
                    sd, ed = parse_jail_period(period_str)
                    if sd and ed:
                        jail_map.setdefault(code, []).append((sd, ed))
                except: continue
        else:
            data_rows = j.get("data", [])
            for row in data_rows:
                try:
                    code = str(row[2]).strip() if len(row) > 2 else ""
                    period_str = str(row[6]).strip() if len(row) > 6 else ""
                    sd, ed = parse_jail_period(period_str)
                    if sd and ed:
                        jail_map.setdefault(code, []).append((sd, ed))
                except: continue
    except Exception as e:
        print(f"⚠️ TWSE 處置抓取失敗: {e}")

    # 2) TPEx (OTC) - OpenAPI
    try:
        url = "https://www.tpex.org.tw/openapi/v1/tpex_disposal_information"
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            for item in data:
                try:
                    code = str(item.get("SecuritiesCompanyCode", "")).strip()
                    if not code.isdigit() or len(code) != 4: continue
                    period = str(item.get("DispositionPeriod", "")).strip()
                    if not period: continue
                    sd, ed = parse_jail_period(period)
                    if not sd or not ed: continue
                    # Overlap Check
                    if ed >= start_date_obj and sd <= end_date_obj:
                        jail_map.setdefault(code, []).append((sd, ed))
                except: continue
    except Exception as e:
        print(f"⚠️ TPEx 處置抓取失敗: {e}")

    # Sort
    for k in jail_map:
        jail_map[k] = sorted(jail_map[k], key=lambda x: x[0])

    return jail_map

def get_official_trading_calendar(days=60):
    end_str = TARGET_DATE.strftime("%Y-%m-%d")
    start_str = (TARGET_DATE - timedelta(days=days*2)).strftime("%Y-%m-%d")

    print("📅 正在下載官方交易日曆...")
    df = finmind_get("TaiwanStockTradingDate", start_date=start_str, end_date=end_str)

    dates = []
    if not df.empty:
        df['date'] = pd.to_datetime(df['date']).dt.date
        dates = sorted(df['date'].tolist())
    else:
        curr = TARGET_DATE.date()
        while len(dates) < days:
            if curr.weekday() < 5: dates.append(curr)
            curr -= timedelta(days=1)
        dates = sorted(dates)

    today_date = TARGET_DATE.date()
    if dates and today_date > dates[-1] and today_date.weekday() < 5:
        if TARGET_DATE.time() > SAFE_MARKET_OPEN_CHECK:
             dates.append(today_date)

    return dates[-days:]

def update_market_monitoring_log(sh):
    print("📊 檢查並更新「大盤數據監控」...")
    HEADERS = ['日期', '代號', '名稱', '收盤價', '漲跌幅(%)', '成交金額(億)']
    ws_market = get_or_create_ws(sh, "大盤數據監控", headers=HEADERS, cols=10)

    def norm_date(s):
        s = str(s).strip()
        if not s: return ""
        try: return pd.to_datetime(s, errors='coerce').strftime("%Y-%m-%d")
        except: return s

    key_to_row = {}
    try:
        all_vals = ws_market.get_all_values()
        for r_idx, row in enumerate(all_vals[1:], start=2):
            if len(row) >= 2:
                d_str = norm_date(row[0])
                c_str = str(row[1]).strip()
                if d_str and c_str:
                    key_to_row[f"{d_str}_{c_str}"] = r_idx
    except: pass

    existing_keys = set(key_to_row.keys())

    try:
        targets = [
            {'fin_id': 'TAIEX', 'code': '^TWII', 'name': '加權指數'},
            {'fin_id': 'TPEx',  'code': '^TWOII', 'name': '櫃買指數'}
        ]
        start_date_str = (TARGET_DATE - timedelta(days=45)).strftime("%Y-%m-%d")
        
        dfs = {}
        for t in targets:
            fin_id = t['fin_id']; code = t['code']
            df = finmind_get("TaiwanStockPrice", data_id=fin_id, start_date=start_date_str)
            if not df.empty:
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)
                    df.index = df.index.tz_localize(None)
                if 'close' in df.columns:
                    df['Close'] = df['close'].astype(float)
                    df['Pct'] = df['Close'].pct_change() * 100
                if 'Turnover' in df.columns: df['Volume'] = df['Turnover'].astype(float)
                elif 'Trading_money' in df.columns: df['Volume'] = df['Trading_money'].astype(float)
                else: df['Volume'] = 0.0
                dfs[code] = df

        new_rows = []
        today_str = TARGET_DATE.strftime("%Y-%m-%d")
        all_dates = set()
        for df in dfs.values():
            all_dates.update(df.index.strftime("%Y-%m-%d").tolist())

        for d in sorted(all_dates):
            for t in targets:
                code = t['code']; name = t['name']
                df = dfs.get(code)
                if df is None or d not in df.index.strftime("%Y-%m-%d"): continue
                
                try: row = df.loc[d]
                except: row = df[df.index.strftime("%Y-%m-%d") == d].iloc[0]
                
                close_val = row.get('Close', 0)
                if pd.isna(close_val): continue
                
                close = round(float(close_val), 2)
                pct = round(float(row.get('Pct', 0) or 0), 2)
                vol_raw = float(row.get('Volume', 0) or 0)
                vol_billion = round(vol_raw / 100000000, 2)
                
                row_data = [d, code, name, close, pct, vol_billion]
                comp_key = f"{d}_{code}"
                
                if d == today_str and TARGET_DATE.time() < SAFE_MARKET_OPEN_CHECK: continue

                if d == today_str and comp_key in key_to_row and TARGET_DATE.time() >= SAFE_MARKET_OPEN_CHECK:
                    r_num = key_to_row[comp_key]
                    try:
                        ws_market.update(values=[row_data], range_name=f'A{r_num}:F{r_num}', value_input_option="USER_ENTERED")
                        print(f"   🔄 已覆寫更新今日 ({d} {name})。")
                    except: pass
                    continue

                if comp_key in existing_keys: continue
                if close > 0: new_rows.append(row_data)

        if new_rows:
            ws_market.append_rows(new_rows, value_input_option="USER_ENTERED")
            print(f"   ✅ 已補入 {len(new_rows)} 筆大盤數據。")
    except Exception as e:
        print(f"   ❌ 大盤數據更新失敗: {e}")

def fetch_history_data(ticker):
    try:
        df = yf.Ticker(ticker).history(period="1y", auto_adjust=False)
        if not df.empty: df.index = df.index.tz_localize(None)
        return df
    except: return pd.DataFrame()

def fetch_stock_fundamental(code, ticker, precise_db):
    market = '上市'; shares = 1
    if str(code) in precise_db:
        market = precise_db[str(code)]['market']
        shares = precise_db[str(code)]['shares']
    data = {'shares': shares, 'market': market, 'pe':0, 'pb':0}
    try:
        t = yf.Ticker(ticker)
        data['pe'] = t.info.get('trailingPE', 0)
        data['pb'] = t.info.get('priceToBook', 0)
    except: pass
    return data

def get_daytrade_stats_finmind(code, date_str):
    end = date_str
    start = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=15)).strftime("%Y-%m-%d")
    df_dt = finmind_get("TaiwanStockDayTrading", code, start, end)
    df_p = finmind_get("TaiwanStockPrice", code, start, end)
    if df_dt.empty or df_p.empty: return 0.0, 0.0
    try:
        m = pd.merge(df_p[['date','Trading_Volume']], df_dt[['date','Volume']], on='date')
        if m.empty: return 0.0, 0.0
        m = m.sort_values('date')
        last = m.iloc[-1]
        td = (last['Volume']/last['Trading_Volume']*100) if last['Trading_Volume']>0 else 0
        avg = m.tail(6); sum_v = avg['Volume'].sum(); sum_t = avg['Trading_Volume'].sum()
        avg_td = (sum_v/sum_t*100) if sum_t>0 else 0
        return round(td, 2), round(avg_td, 2)
    except: return 0.0, 0.0

def load_precise_db_from_sheet(sh):
    try:
        ws = sh.worksheet(PARAM_SHEET_NAME)
        db = {}
        for r in ws.get_all_records():
            c = str(r.get('代號','')).strip()
            if c: db[c] = {'market': r.get('市場','上市'), 'shares': r.get('發行股數',1)}
        return db
    except: return {}
