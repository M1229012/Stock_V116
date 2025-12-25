# -*- coding: utf-8 -*-
import requests
import pandas as pd
import gspread
import yfinance as yf
import time
import re
from datetime import datetime, timedelta, date
import config  # ✅ 引入 config 以使用全域狀態
from logic import parse_clause_ids_strict

# --- 輔助函式 ---
def _to_int_safe(x, default=0):
    try:
        if x is None: return default
        if isinstance(x, str) and x.strip() == "": return default
        return int(float(x))
    except: return default

def _to_float_safe(x, default=0.0):
    try:
        if x is None: return default
        if isinstance(x, str) and x.strip() == "": return default
        return float(x)
    except: return default

# --- 連線與工具 ---
def connect_google_sheets():
    try:
        key_path = "service_key.json"
        if not os.path.exists(key_path): return None
        gc = gspread.service_account(filename=key_path)
        try: sh = gc.open(config.SHEET_NAME)
        except: sh = gc.create(config.SHEET_NAME)
        return sh
    except: return None

def get_or_create_ws(sh, title, headers=None, rows=5000, cols=20):
    # ✅ 自動 resize 避免欄位錯位
    need_cols = max(cols, len(headers) if headers else 0)
    try:
        ws = sh.worksheet(title)
        try:
            if headers and ws.col_count < need_cols:
                ws.resize(rows=ws.row_count, cols=need_cols)
        except: pass
        return ws
    except:
        ws = sh.add_worksheet(title=title, rows=str(rows), cols=str(need_cols))
        if headers:
            ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws

def load_precise_db_from_sheet(sh):
    try:
        ws = sh.worksheet(config.PARAM_SHEET_NAME)
        data = ws.get_all_records()
        db = {}
        for row in data:
            code = str(row.get('代號','')).strip()
            if not code: continue
            try: shares = int(str(row.get('發行股數', 1)).replace(',', ''))
            except: shares = 1
            try: offset = float(row.get('類股漲幅修正', 0.0))
            except: offset = 0.0
            try: turn_avg = float(row.get('同類股平均週轉', 5.0))
            except: turn_avg = 5.0
            try: purity = float(row.get('成交量純度', 1.0))
            except: purity = 1.0
            market = str(row.get('市場', '上市')).strip()
            db[code] = {"market": market, "shares": shares, "sector_offset": offset, "sector_turn_avg": turn_avg, "vol_purity": purity}
        return db
    except: return {}

# --- FinMind (✅ 修正：完全使用 config 的全域變數) ---
def finmind_get(dataset, data_id=None, start_date=None, end_date=None):
    cache_key = (dataset, data_id, start_date, end_date)
    
    # 讀取 config cache
    if cache_key in config._FINMIND_CACHE:
        return config._FINMIND_CACHE[cache_key].copy()

    params = {"dataset": dataset}
    if data_id: params["data_id"] = str(data_id)
    if start_date: params["start_date"] = start_date
    if end_date: params["end_date"] = end_date
    if not config.FINMIND_TOKENS:
        return pd.DataFrame()

    for _ in range(4):
        # 使用 config index
        current_token = config.FINMIND_TOKENS[config.CURRENT_TOKEN_INDEX]
        headers = {
            "Authorization": f"Bearer {current_token}",
            "User-Agent": "Mozilla/5.0",
            "Connection": "close",
        }
        try:
            r = requests.get(config.FINMIND_API_URL, params=params, headers=headers, timeout=10)
            if r.status_code == 200:
                j = r.json()
                df = pd.DataFrame(j.get("data", []))
                # 更新 config cache
                if len(config._FINMIND_CACHE) >= 2000:
                    config._FINMIND_CACHE.clear()
                config._FINMIND_CACHE[cache_key] = df
                return df.copy()

            # 輪替 config index
            config.CURRENT_TOKEN_INDEX = (config.CURRENT_TOKEN_INDEX + 1) % len(config.FINMIND_TOKENS)
            time.sleep(2)
        except:
            time.sleep(1)

    return pd.DataFrame()

# --- 基本面抓取 (✅ 缺值回 0) ---
def fetch_stock_fundamental(code, ticker, precise_db, retries=3, sleep_sec=1.2):
    param_shares = 0
    if str(code) in precise_db:
        param_shares = _to_int_safe(precise_db[str(code)].get('shares'), 0)
    
    data = {"shares": param_shares, "pe": 0, "pb": 0}

    for attempt in range(1, retries + 1):
        try:
            t = yf.Ticker(ticker)
            info = getattr(t, "info", None) or {}
            
            if data["shares"] <= 1:
                so = _to_int_safe(info.get("sharesOutstanding"), 0)
                if so > 1: data["shares"] = so
                else:
                    fi = getattr(t, "fast_info", None) or {}
                    so2 = _to_int_safe(fi.get("shares"), 0)
                    if so2 > 1: data["shares"] = so2
            
            pe = _to_float_safe(info.get("trailingPE"), 0)
            if pe == 0: pe = _to_float_safe(info.get("forwardPE"), 0)
            data["pe"] = pe
            data["pb"] = _to_float_safe(info.get("priceToBook"), 0)
            return data
        except: time.sleep(sleep_sec * attempt)
            
    return data

# --- Yahoo History ---
def fetch_history_data(ticker_code):
    try:
        df = yf.Ticker(ticker_code).history(period="1y", auto_adjust=False)
        if df.empty: return pd.DataFrame()
        df.index = df.index.tz_localize(None)
        return df
    except: return pd.DataFrame()

# --- 官方公告爬蟲 (保留 UA 確保 Actions 可行) ---
def get_daily_data(date_obj):
    date_str_nodash = date_obj.strftime("%Y%m%d")
    date_str = date_obj.strftime("%Y-%m-%d")
    rows = []
    print(f"📡 爬取公告 {date_str}...")

    # TWSE
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Connection": "close",
        }
        r = requests.get("https://www.twse.com.tw/rwd/zh/announcement/notice",
                         params={"startDate": date_str_nodash, "endDate": date_str_nodash, "response": "json"}, 
                         headers=headers, timeout=10)
        
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
    except: pass

    # TPEx (保留合併多表邏輯)
    try:
        roc = f"{date_obj.year-1911}/{date_obj.month:02d}/{date_obj.day:02d}"
        headers_tpex = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.tpex.org.tw/'}
        r = requests.post("https://www.tpex.org.tw/www/zh-tw/bulletin/attention", 
                          data={'date': roc, 'response': 'json'}, 
                          headers=headers_tpex, 
                          timeout=10)
        if r.status_code == 200:
            res = r.json()
            target = []
            if 'tables' in res and isinstance(res['tables'], list):
                for t in res['tables']: target.extend(t.get('data', []))
            elif 'data' in res:
                target = res['data']
            
            final_target = [row for row in target if len(row)>5 and (str(row[5]).strip() in [roc, date_str])]
            for i in final_target:
                code, name = str(i[1]).strip(), str(i[2]).strip()
                if len(code)==4 and code.isdigit():
                    raw = " ".join([str(x) for x in i])
                    ids = parse_clause_ids_strict(raw)
                    c_str = "、".join([f"第{k}款" for k in sorted(ids)]) or raw
                    rows.append({'日期': date_str, '市場': 'TPEx', '代號': code, '名稱': name, '觸犯條款': c_str})
    except: pass
    
    if rows: print(f"✅ 成功抓到 {len(rows)} 檔注意股。")
    else: print(f"⚠️ 該日 ({date_str}) 查無資料。")
    return rows

# --- Jail Map, Ticker Suffix, Market Update ---
def get_ticker_suffix(market_type):
    m = str(market_type).upper().strip()
    if any(k in m for k in ['上櫃', 'TWO', 'TPEX', 'OTC']): return '.TWO'
    return '.TW'

def parse_roc_date(s):
    try:
        p = re.split(r'[/-]', str(s).strip())
        if len(p)==3: return date(int(p[0])+1911, int(p[1]), int(p[2]))
    except: return None
    return None

def parse_jail_period(s):
    if not s: return None, None
    d = s.split('～') if '～' in s else s.split('~')
    if len(d)<2 and '-' in s: d = s.split('-')
    if len(d)>=2:
        s1, s2 = parse_roc_date(d[0].strip()), parse_roc_date(d[1].strip())
        if s1 and s2: return s1, s2
    return None, None

def get_jail_map(sd, ed):
    print("🔒 正在下載處置(Jail)名單...")
    jm = {}
    s_str, e_str = sd.strftime("%Y%m%d"), ed.strftime("%Y%m%d")
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get("https://www.twse.com.tw/rwd/zh/announcement/punish", 
                         params={"startDate":s_str,"endDate":e_str,"response":"json"}, 
                         headers=headers, timeout=10)
        j = r.json()
        if isinstance(j.get("tables"), list) and j["tables"]:
            data_rows = j["tables"][0].get('data', [])
            for row in data_rows:
                try:
                    c = str(row[2]).strip()
                    s, e = parse_jail_period(str(row[6]))
                    if s and e: jm.setdefault(c,[]).append((s,e))
                except: continue
    except: pass
    
    try:
        r = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_disposal_information", timeout=10)
        for row in r.json():
            try:
                c = str(row.get("SecuritiesCompanyCode","")).strip()
                s, e = parse_jail_period(str(row.get("DispositionPeriod","")))
                if s and e and e>=sd and s<=ed: jm.setdefault(c,[]).append((s,e))
            except: continue
    except: pass
    for k in jm: jm[k].sort(key=lambda x:x[0])
    return jm

def get_official_trading_calendar(days=60):
    end = config.TARGET_DATE.strftime("%Y-%m-%d")
    start = (config.TARGET_DATE - timedelta(days=days*2)).strftime("%Y-%m-%d")
    print("📅 下載交易日曆...")
    df = finmind_get("TaiwanStockTradingDate", start_date=start, end_date=end)
    dates = []
    if not df.empty:
        df['date'] = pd.to_datetime(df['date']).dt.date
        dates = sorted(df['date'].tolist())
    else:
        curr = config.TARGET_DATE.date()
        while len(dates) < days:
            if curr.weekday()<5: dates.append(curr)
            curr -= timedelta(days=1)
        dates = sorted(dates)
    today = config.TARGET_DATE.date()
    if dates and today > dates[-1] and today.weekday()<5:
        if config.TARGET_DATE.time() > config.SAFE_MARKET_OPEN_CHECK: dates.append(today)
    return dates[-days:]

def get_daytrade_stats_finmind(stock_id, target_date_str):
    end_date = target_date_str
    start_date = (datetime.strptime(target_date_str, "%Y-%m-%d") - timedelta(days=15)).strftime("%Y-%m-%d")
    df_dt = finmind_get("TaiwanStockDayTrading", stock_id, start_date=start_date, end_date=end_date)
    df_p = finmind_get("TaiwanStockPrice", stock_id, start_date=start_date, end_date=end_date)
    if df_dt.empty or df_p.empty: return 0.0, 0.0
    try:
        merged = pd.merge(df_p[['date', 'Trading_Volume']], df_dt[['date', 'Volume']], on='date', how='inner')
        if merged.empty: return 0.0, 0.0
        merged['date'] = pd.to_datetime(merged['date'])
        merged = merged.sort_values('date')
        recent_6 = merged.tail(6)
        last_row = recent_6.iloc[-1]
        today_ratio = (last_row['Volume'] / last_row['Trading_Volume'] * 100.0) if last_row['Trading_Volume'] > 0 else 0.0
        sum_dt = recent_6['Volume'].sum()
        sum_total = recent_6['Trading_Volume'].sum()
        avg_6_ratio = (sum_dt / sum_total * 100.0) if sum_total > 0 else 0.0
        return round(today_ratio, 2), round(avg_6_ratio, 2)
    except: return 0.0, 0.0

def update_market_monitoring_log(sh):
    print("📊 更新大盤數據...")
    HEADERS = ['日期', '代號', '名稱', '收盤價', '漲跌幅(%)', '成交金額(億)']
    ws_market = get_or_create_ws(sh, "大盤數據監控", headers=HEADERS, cols=10)
    
    def norm_date(s):
        if not s: return ""
        try: return pd.to_datetime(s, errors='coerce').strftime("%Y-%m-%d")
        except: return str(s).strip()

    key_to_row = {}
    try:
        all_vals = ws_market.get_all_values()
        for r_idx, row in enumerate(all_vals[1:], start=2):
            if len(row) >= 2:
                key_to_row[f"{norm_date(row[0])}_{str(row[1]).strip()}"] = r_idx
    except: pass

    existing_keys = set(key_to_row.keys())
    
    try:
        targets = [
            {'fin_id': 'TAIEX', 'code': '^TWII', 'name': '加權指數'},
            {'fin_id': 'TPEx',  'code': '^TWOII', 'name': '櫃買指數'}
        ]
        start_date_str = (config.TARGET_DATE - timedelta(days=45)).strftime("%Y-%m-%d")
        today_str = config.TARGET_DATE.strftime("%Y-%m-%d")
        
        dfs = {}
        all_dates = set()
        for t in targets:
            df = finmind_get("TaiwanStockPrice", data_id=t['fin_id'], start_date=start_date_str)
            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                df.index = df.index.tz_localize(None)
                if 'close' in df.columns:
                    df['Close'] = df['close'].astype(float)
                    df['Pct'] = df['Close'].pct_change() * 100
                if 'Turnover' in df.columns: df['Volume'] = df['Turnover'].astype(float)
                elif 'Trading_money' in df.columns: df['Volume'] = df['Trading_money'].astype(float)
                else: df['Volume'] = 0.0
                dfs[t['code']] = df
                all_dates.update(df.index.strftime("%Y-%m-%d").tolist())

        new_rows = []
        for d in sorted(all_dates):
            for t in targets:
                code, name = t['code'], t['name']
                df = dfs.get(code)
                if df is None or d not in df.index.strftime("%Y-%m-%d"): continue
                try: 
                    row = df.loc[d] if d in df.index else df[df.index.strftime("%Y-%m-%d") == d].iloc[0]
                except: continue
                
                if pd.isna(row.get('Close')): continue
                close = round(float(row['Close']), 2)
                pct = round(float(row.get('Pct', 0) or 0), 2)
                vol = round(float(row.get('Volume', 0) or 0) / 100000000, 2)
                
                row_data = [d, code, name, close, pct, vol]
                comp_key = f"{d}_{code}"
                
                if d == today_str and config.TARGET_DATE.time() < config.SAFE_MARKET_OPEN_CHECK: continue
                
                if d == today_str and comp_key in key_to_row:
                    try:
                        r_num = key_to_row[comp_key]
                        ws_market.update(range_name=f'A{r_num}:F{r_num}', values=[row_data], value_input_option="USER_ENTERED")
                        print(f"   🔄 更新今日 {name}")
                    except: pass
                    continue

                if comp_key not in existing_keys:
                    new_rows.append(row_data)

        if new_rows:
            ws_market.append_rows(new_rows, value_input_option="USER_ENTERED")
            print(f"   ✅ 補入 {len(new_rows)} 筆大盤數據")
    except Exception as e:
        print(f"   ❌ 大盤更新失敗: {e}")
