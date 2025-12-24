# -*- coding: utf-8 -*-
import requests
import pandas as pd
import time
import yfinance as yf
import gspread
import logging
from google.auth import default
from google.colab import auth, userdata
from datetime import datetime, timedelta, date
from config import FINMIND_API_URL, PARAM_SHEET_NAME, SAFE_MARKET_OPEN_CHECK, SHEET_NAME
from utils import parse_clause_ids_strict, parse_jail_period, get_or_create_ws

# ==========================================
# 恢復 yfinance 靜音模式 (還原原始邏輯)
# ==========================================
logger = logging.getLogger('yfinance')
logger.setLevel(logging.CRITICAL)
logger.disabled = True

# FinMind Token 管理
try:
    token1 = userdata.get('FinMind_1')
    token2 = userdata.get('FinMind_2')
    FINMIND_TOKENS = [t for t in [token1, token2] if t]
except Exception as e:
    print(f"⚠️ 無法讀取 Secrets: {e}")
    FINMIND_TOKENS = []

CURRENT_TOKEN_INDEX = 0
_FINMIND_CACHE = {}

def finmind_get(dataset, data_id=None, start_date=None, end_date=None):
    global CURRENT_TOKEN_INDEX
    cache_key = (dataset, data_id, start_date, end_date)
    if cache_key in _FINMIND_CACHE:
        return _FINMIND_CACHE[cache_key].copy()

    params = {"dataset": dataset}
    if data_id: params["data_id"] = str(data_id)
    if start_date: params["start_date"] = start_date
    if end_date: params["end_date"] = end_date
    if not FINMIND_TOKENS: return pd.DataFrame()

    for _ in range(4):
        headers = {"Authorization": f"Bearer {FINMIND_TOKENS[CURRENT_TOKEN_INDEX]}", "User-Agent": "Mozilla/5.0", "Connection": "close"}
        try:
            r = requests.get(FINMIND_API_URL, params=params, headers=headers, timeout=10)
            if r.status_code == 200:
                j = r.json()
                df = pd.DataFrame(j["data"]) if "data" in j else pd.DataFrame()
                if len(_FINMIND_CACHE) >= 2000: _FINMIND_CACHE.clear()
                _FINMIND_CACHE[cache_key] = df
                return df.copy()
            elif r.status_code != 200:
                print(f"   ⚠️ Token {CURRENT_TOKEN_INDEX} 異常, 切換...")
                time.sleep(2)
                CURRENT_TOKEN_INDEX = (CURRENT_TOKEN_INDEX + 1) % len(FINMIND_TOKENS)
                continue
        except:
            time.sleep(1)
    return pd.DataFrame()

def connect_google_sheets():
    print("正在進行 Google 驗證...")
    try:
        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)
        try: sh = gc.open(SHEET_NAME)
        except: sh = gc.create(SHEET_NAME)
        return sh, None
    except: return None, None

def fetch_history_data(ticker_code):
    try:
        df = yf.Ticker(ticker_code).history(period="1y", auto_adjust=False)
        if df.empty: return pd.DataFrame()
        df.index = df.index.tz_localize(None)
        return df
    except: return pd.DataFrame()

def load_precise_db_from_sheet(sh):
    try:
        ws = sh.worksheet(PARAM_SHEET_NAME)
        data = ws.get_all_records()
        db = {}
        for row in data:
            code = str(row.get('代號', '')).strip()
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

def fetch_stock_fundamental(stock_id, ticker_code, precise_db):
    market = '上市'; shares = 0
    if str(stock_id) in precise_db:
        db = precise_db[str(stock_id)]
        market = db['market']; shares = db['shares']
    data = {'shares': shares, 'market_type': market, 'pe': -1, 'pb': -1}
    try:
        t = yf.Ticker(ticker_code)
        if ".TWO" in ticker_code: data['market_type'] = '上櫃'
        if data['shares'] <= 1:
            s = t.fast_info.get('shares', None)
            if s: data['shares'] = int(s)
        data['pe'] = t.info.get('trailingPE', t.info.get('forwardPE', 0))
        data['pb'] = t.info.get('priceToBook', 0)
        if data['pe']: data['pe'] = round(data['pe'], 2)
        if data['pb']: data['pb'] = round(data['pb'], 2)
    except: pass
    return data

def get_daytrade_stats_finmind(stock_id, target_date_str):
    end_date = target_date_str
    start_date = (datetime.strptime(target_date_str, "%Y-%m-%d") - timedelta(days=15)).strftime("%Y-%m-%d")
    price_df = finmind_get("TaiwanStockPrice", data_id=stock_id, start_date=start_date, end_date=end_date)
    dt_df = finmind_get("TaiwanStockDayTrading", data_id=stock_id, start_date=start_date, end_date=end_date)
    if price_df.empty or dt_df.empty: return 0.0, 0.0
    try:
        merged = pd.merge(price_df[['date', 'Trading_Volume']], dt_df[['date', 'Volume']], on='date', how='inner')
        if merged.empty: return 0.0, 0.0
        merged['date'] = pd.to_datetime(merged['date'])
        merged = merged.sort_values('date')
        recent_6 = merged.tail(6)
        if len(recent_6) < 6: return 0.0, 0.0
        last_row = recent_6.iloc[-1]
        today_ratio = (last_row['Volume'] / last_row['Trading_Volume'] * 100.0) if last_row['Trading_Volume'] > 0 else 0.0
        sum_dt = recent_6['Volume'].sum()
        sum_total = recent_6['Trading_Volume'].sum()
        avg_6_ratio = (sum_dt / sum_total * 100.0) if sum_total > 0 else 0.0
        return round(today_ratio, 2), round(avg_6_ratio, 2)
    except: return 0.0, 0.0

def is_market_open_by_finmind(date_str):
    df = finmind_get("TaiwanStockPrice", data_id="2330", start_date=date_str, end_date=date_str)
    return not df.empty

def get_official_trading_calendar(days=60, target_date_obj=None):
    if not target_date_obj: target_date_obj = datetime.now()
    end_str = target_date_obj.strftime("%Y-%m-%d")
    start_str = (target_date_obj - timedelta(days=days*2)).strftime("%Y-%m-%d")
    print("📅 正在下載官方交易日曆...")
    df = finmind_get("TaiwanStockTradingDate", start_date=start_str, end_date=end_str)
    dates = []
    if not df.empty:
        df['date'] = pd.to_datetime(df['date']).dt.date
        dates = sorted(df['date'].tolist())
    else:
        curr = target_date_obj.date()
        while len(dates) < days:
            if curr.weekday() < 5: dates.append(curr)
            curr -= timedelta(days=1)
        dates = sorted(dates)
    
    today_date = target_date_obj.date()
    today_str = today_date.strftime("%Y-%m-%d")
    is_late_enough = target_date_obj.time() > SAFE_MARKET_OPEN_CHECK

    if dates and today_date > dates[-1] and today_date.weekday() < 5:
        if is_late_enough:
            print(f"⚠️ 日曆缺漏今日 ({today_date})，時間已過 {SAFE_MARKET_OPEN_CHECK}，驗證開市中...")
            if is_market_open_by_finmind(today_str):
                print(f"✅ 驗證成功，補入今日。")
                dates.append(today_date)
            else:
                print(f"⛔ 驗證失敗，不補入。")
        else:
            print(f"⏳ 時間尚早，暫不強制補入今日。")
    return dates[-days:]

def get_daily_data(date_obj):
    date_str_nodash = date_obj.strftime("%Y%m%d")
    date_str = date_obj.strftime("%Y-%m-%d")
    rows = []
    error_count = 0
    print(f"📡 嘗試爬取官方公告 (日期: {date_str})...")
    # TWSE
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
    # TPEx
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

def get_jail_map(start_date_obj, end_date_obj):
    print("🔒 正在下載處置(Jail)名單...")
    jail_map = {}
    s_str = start_date_obj.strftime("%Y%m%d")
    e_str = end_date_obj.strftime("%Y%m%d")
    # TWSE
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
            idx_code = find_idx(fields, ["證券代號", "有價證券代號"]) or 2
            idx_period = find_idx(fields, ["處置起迄時間", "處置起訖時間"]) or 6
            for row in data_rows:
                try:
                    code = str(row[idx_code]).strip()
                    period_str = str(row[idx_period]).strip()
                    sd, ed = parse_jail_period(period_str)
                    if sd and ed: jail_map.setdefault(code, []).append((sd, ed))
                except: continue
        else:
            data_rows = j.get("data", [])
            for row in data_rows:
                try:
                    code = str(row[2]).strip() if len(row) > 2 else ""
                    period_str = str(row[6]).strip() if len(row) > 6 else ""
                    sd, ed = parse_jail_period(period_str)
                    if sd and ed: jail_map.setdefault(code, []).append((sd, ed))
                except: continue
    except Exception as e: print(f"⚠️ TWSE 處置失敗: {e}")
    # TPEx
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
                    sd, ed = parse_jail_period(period)
                    if not sd or not ed: continue
                    if ed >= start_date_obj and sd <= end_date_obj:
                        jail_map.setdefault(code, []).append((sd, ed))
                except: continue
    except Exception as e: print(f"⚠️ TPEx 處置失敗: {e}")
    for k in jail_map: jail_map[k] = sorted(jail_map[k], key=lambda x: x[0])
    return jail_map

def update_market_monitoring_log(sh, target_date_obj):
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
                if d_str and c_str: key_to_row[f"{d_str}_{c_str}"] = r_idx
    except: pass
    existing_keys = set(key_to_row.keys())
    try:
        targets = [{'fin_id': 'TAIEX', 'code': '^TWII', 'name': '加權指數'}, {'fin_id': 'TPEx', 'code': '^TWOII', 'name': '櫃買指數'}]
        start_date_str = (target_date_obj - timedelta(days=45)).strftime("%Y-%m-%d")
        dfs = {}
        for t in targets:
            df = finmind_get("TaiwanStockPrice", data_id=t['fin_id'], start_date=start_date_str)
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
                dfs[t['code']] = df
        new_rows = []
        today_str = target_date_obj.strftime("%Y-%m-%d")
        all_dates = set()
        for df in dfs.values(): all_dates.update(df.index.strftime("%Y-%m-%d").tolist())
        for d in sorted(all_dates):
            for t in targets:
                code = t['code']
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
                row_data = [d, code, t['name'], close, pct, vol_billion]
                comp_key = f"{d}_{code}"
                if d == today_str and target_date_obj.time() < SAFE_MARKET_OPEN_CHECK: continue
                if d == today_str and comp_key in key_to_row and target_date_obj.time() >= SAFE_MARKET_OPEN_CHECK:
                    r_num = key_to_row[comp_key]
                    try: ws_market.update(values=[row_data], range_name=f'A{r_num}:F{r_num}', value_input_option="USER_ENTERED")
                    except: pass
                    continue
                if comp_key in existing_keys: continue
                if close > 0: new_rows.append(row_data)
        if new_rows:
            ws_market.append_rows(new_rows, value_input_option="USER_ENTERED")
            print(f"   ✅ 已補入 {len(new_rows)} 筆大盤數據。")
        else: print("   ✅ 大盤數據已是最新。")
    except Exception as e: print(f"   ❌ 大盤更新失敗: {e}")
