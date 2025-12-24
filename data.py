# -*- coding: utf-8 -*-
import requests
import pandas as pd
import time
import yfinance as yf
import gspread
import logging
import os
import urllib3
from datetime import datetime, timedelta, date
from google.oauth2.service_account import Credentials
from google.auth import default

# 引入 config 與 utils
try:
    import config
    import utils
except ImportError:
    pass

# ==========================================
# 忽略 SSL 警告 (Zeabur 關鍵修復)
# ==========================================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 設定 yfinance 靜音
logger = logging.getLogger('yfinance')
logger.setLevel(logging.CRITICAL)
logger.disabled = True

# ==========================================
# Token 管理 (Zeabur 環境變數)
# ==========================================
FINMIND_TOKENS = []
env_token = os.getenv('FinMind_1')
if env_token: FINMIND_TOKENS.append(env_token)
env_token2 = os.getenv('FinMind_2')
if env_token2: FINMIND_TOKENS.append(env_token2)

# Colab Fallback
try:
    from google.colab import userdata
    t1 = userdata.get('FinMind_1')
    if t1 and t1 not in FINMIND_TOKENS: FINMIND_TOKENS.append(t1)
except: pass

CURRENT_TOKEN_INDEX = 0
_FINMIND_CACHE = {}

# ==========================================
# 連線與 API
# ==========================================
def connect_google_sheets():
    print("正在進行 Google 驗證...")
    try:
        # Zeabur / Local File Priority
        key_path = "/service_key.json"
        if not os.path.exists(key_path):
            key_path = "service_key.json"
            
        if os.path.exists(key_path):
            gc = gspread.service_account(filename=key_path)
        else:
            # Colab Default
            from google.colab import auth
            auth.authenticate_user()
            creds, _ = default()
            gc = gspread.authorize(creds)
            
        try: sh = gc.open(config.SHEET_NAME)
        except: sh = gc.create(config.SHEET_NAME)
        return sh, None
    except Exception as e:
        print(f"❌ Google Sheet 連線失敗: {e}")
        return None, None

def finmind_get(dataset, data_id=None, start_date=None, end_date=None):
    global CURRENT_TOKEN_INDEX
    cache_key = (dataset, data_id, start_date, end_date)
    if cache_key in _FINMIND_CACHE: return _FINMIND_CACHE[cache_key].copy()

    params = {"dataset": dataset}
    if data_id: params["data_id"] = str(data_id)
    if start_date: params["start_date"] = start_date
    if end_date: params["end_date"] = end_date
    
    tokens_to_try = FINMIND_TOKENS if FINMIND_TOKENS else [None]

    for _ in range(4):
        token = tokens_to_try[CURRENT_TOKEN_INDEX % len(tokens_to_try)]
        headers = {"User-Agent": "Mozilla/5.0", "Connection": "close"}
        if token: headers["Authorization"] = f"Bearer {token}"
            
        try:
            # [Fix] verify=False
            r = requests.get(config.FINMIND_API_URL, params=params, headers=headers, timeout=10, verify=False)
            if r.status_code == 200:
                j = r.json()
                df = pd.DataFrame(j["data"]) if "data" in j else pd.DataFrame()
                if len(_FINMIND_CACHE) >= 2000: _FINMIND_CACHE.clear()
                _FINMIND_CACHE[cache_key] = df
                return df.copy()
            elif r.status_code != 200 and token:
                time.sleep(2)
                CURRENT_TOKEN_INDEX += 1
                continue
        except: time.sleep(1)
    return pd.DataFrame()

# ==========================================
# 大盤監控更新 (V116.18 Logic)
# ==========================================
def update_market_monitoring_log(sh, target_date_obj):
    print("📊 檢查並更新「大盤數據監控」...")
    HEADERS = ['日期', '代號', '名稱', '收盤價', '漲跌幅(%)', '成交金額(億)']
    ws_market = utils.get_or_create_ws(sh, "大盤數據監控", headers=HEADERS, cols=10)

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
        targets = [
            {'fin_id': 'TAIEX', 'code': '^TWII', 'name': '加權指數'},
            {'fin_id': 'TPEx',  'code': '^TWOII', 'name': '櫃買指數'}
        ]
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

                if d == today_str and target_date_obj.time() < config.SAFE_MARKET_OPEN_CHECK:
                    if code == '^TWII': print(f"   ⏳ 今日 ({d}) 尚未收盤，跳過寫入。")
                    continue

                if d == today_str and comp_key in key_to_row and target_date_obj.time() >= config.SAFE_MARKET_OPEN_CHECK:
                    r_num = key_to_row[comp_key]
                    try:
                        ws_market.update(values=[row_data], range_name=f'A{r_num}:F{r_num}', value_input_option="USER_ENTERED")
                        print(f"   🔄 已覆寫更新今日 ({d} {t['name']}) 數據 (Row {r_num})。")
                    except Exception as e:
                        print(f"   ⚠️ 覆寫失敗 ({comp_key}): {e}")
                    continue

                if comp_key in existing_keys: continue
                if close > 0: new_rows.append(row_data)

        if new_rows:
            ws_market.append_rows(new_rows, value_input_option="USER_ENTERED")
            print(f"   ✅ 已補入 {len(new_rows)} 筆大盤數據。")
        else:
            print("   ✅ 大盤數據已是最新，無需新增。")
    except Exception as e:
        print(f"   ❌ 大盤數據更新失敗: {e}")

# ==========================================
# 爬蟲與資料抓取 (SSL Fix)
# ==========================================
def get_daily_data(date_obj):
    date_str_nodash = date_obj.strftime("%Y%m%d")
    date_str = date_obj.strftime("%Y-%m-%d")
    rows = []
    error_count = 0
    print(f"📡 嘗試爬取官方公告 (日期: {date_str})...")
    # TWSE
    try:
        r = requests.get("https://www.twse.com.tw/rwd/zh/announcement/notice",
                         params={"startDate": date_str_nodash, "endDate": date_str_nodash, "response": "json"}, 
                         timeout=10, verify=False)
        if r.status_code == 200:
            d = r.json()
            if 'data' in d:
                for i in d['data']:
                    code = str(i[1]).strip(); name = str(i[2]).strip()
                    if not (code.isdigit() and len(code) == 4): continue
                    raw_text = " ".join([str(x) for x in i])
                    ids = utils.parse_clause_ids_strict(raw_text)
                    clause_str = "、".join([f"第{k}款" for k in sorted(ids)])
                    if not clause_str: clause_str = raw_text
                    rows.append({'日期': date_str, '市場': 'TWSE', '代號': code, '名稱': name, '觸犯條款': clause_str})
        else: error_count += 1
    except: error_count += 1
    # TPEx
    try:
        roc_date = f"{date_obj.year-1911}/{date_obj.month:02d}/{date_obj.day:02d}"
        headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.tpex.org.tw/'}
        r = requests.post("https://www.tpex.org.tw/www/zh-tw/bulletin/attention", 
                          data={'date': roc_date, 'response': 'json'}, 
                          headers=headers, timeout=10, verify=False)
        if r.status_code == 200:
            res = r.json()
            target = []
            if 'tables' in res:
                 for t in res['tables']: target.extend(t.get('data', []))
            elif 'data' in res: target = res['data']
            
            filtered = []
            if target:
                for row in target:
                    if len(row) > 5:
                        rd = str(row[5]).strip()
                        if rd == roc_date or rd == date_str: filtered.append(row)
            target = filtered

            for i in target:
                code = str(i[1]).strip(); name = str(i[2]).strip()
                if not (code.isdigit() and len(code) == 4): continue
                raw_text = " ".join([str(x) for x in i])
                ids = utils.parse_clause_ids_strict(raw_text)
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
        r = requests.get("https://www.twse.com.tw/rwd/zh/announcement/punish", 
                         params={"startDate": s_str, "endDate": e_str, "response": "json"}, timeout=10, verify=False)
        j = r.json()
        if isinstance(j.get("tables"), list) and j["tables"]:
            data_rows = j["tables"][0].get("data", [])
            for row in data_rows:
                try:
                    # 簡易索引，假設格式穩定
                    code = str(row[2]).strip()
                    sd, ed = utils.parse_jail_period(str(row[6]).strip())
                    if sd and ed: jail_map.setdefault(code, []).append((sd, ed))
                except: continue
        else:
            for row in j.get("data", []):
                try:
                    code = str(row[2]).strip()
                    sd, ed = utils.parse_jail_period(str(row[6]).strip())
                    if sd and ed: jail_map.setdefault(code, []).append((sd, ed))
                except: continue
    except Exception as e: print(f"⚠️ TWSE 處置抓取失敗: {e}")
    # TPEx
    try:
        r = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_disposal_information", timeout=10, verify=False)
        if r.status_code == 200:
            for item in r.json():
                try:
                    code = str(item.get("SecuritiesCompanyCode", "")).strip()
                    if len(code) != 4: continue
                    sd, ed = utils.parse_jail_period(item.get("DispositionPeriod", ""))
                    if sd and ed:
                        if ed >= start_date_obj and sd <= end_date_obj:
                            jail_map.setdefault(code, []).append((sd, ed))
                except: continue
    except Exception as e: print(f"⚠️ TPEx 處置抓取失敗: {e}")
    for k in jail_map: jail_map[k] = sorted(jail_map[k], key=lambda x: x[0])
    return jail_map

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
    if dates and today_date > dates[-1] and today_date.weekday() < 5:
        if target_date_obj.time() > config.SAFE_MARKET_OPEN_CHECK:
            print(f"⚠️ 驗證今日 ({today_date}) 開市中...")
            if is_market_open_by_finmind(today_str):
                print("✅ 驗證成功，補入今日。")
                dates.append(today_date)
            else: print("⛔ 驗證失敗，不補入。")
        else: print("⏳ 時間尚早，暫不強制補入。")
    return dates[-days:]

def get_daytrade_stats_finmind(stock_id, target_date_str):
    end_date = target_date_str
    start_date = (datetime.strptime(target_date_str, "%Y-%m-%d") - timedelta(days=15)).strftime("%Y-%m-%d")
    p = finmind_get("TaiwanStockPrice", data_id=stock_id, start_date=start_date, end_date=end_date)
    d = finmind_get("TaiwanStockDayTrading", data_id=stock_id, start_date=start_date, end_date=end_date)
    if p.empty or d.empty: return 0.0, 0.0
    try:
        # [Fix] 轉換格式確保合併成功
        p['date'] = pd.to_datetime(p['date'])
        d['date'] = pd.to_datetime(d['date'])
        
        m = pd.merge(p[['date','Trading_Volume']], d[['date','Volume']], on='date', how='inner')
        if m.empty: return 0.0, 0.0
        m = m.sort_values('date')
        r6 = m.tail(6)
        if len(r6) < 6: return 0.0, 0.0
        last = r6.iloc[-1]
        today = (last['Volume']/last['Trading_Volume']*100) if last['Trading_Volume'] > 0 else 0.0
        avg6 = (r6['Volume'].sum()/r6['Trading_Volume'].sum()*100) if r6['Trading_Volume'].sum() > 0 else 0.0
        return round(today, 2), round(avg6, 2)
    except: return 0.0, 0.0

def fetch_history_data(ticker_code):
    try:
        df = yf.Ticker(ticker_code).history(period="1y", auto_adjust=False)
        if df.empty: return pd.DataFrame()
        # [Fix] 只有在已經有時區時才移除，防止報錯
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        return df
    except: return pd.DataFrame()

def load_precise_db_from_sheet(sh):
    try:
        ws = sh.worksheet(config.PARAM_SHEET_NAME)
        data = ws.get_all_records()
        db = {}
        for row in data:
            code = str(row.get('代號', '')).strip()
            if not code: continue
            try: shares = int(str(row.get('發行股數', 1)).replace(',', ''))
            except: shares = 1
            db[code] = {"market": str(row.get('市場', '上市')).strip(), "shares": shares}
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
