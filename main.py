# -*- coding: utf-8 -*-
"""
V116.18 台股注意股系統 (Zeabur Version - Ported from Final 1223)
修正重點：
1. [顯示] 修正「處置中」的顯示邏輯：最快處置天數顯示 "0"。
2. [邏輯] 修正「已達標」天數：當日達標視同進入處置（0天），而非剩餘1天。
3. [顯示] 修正「0天原因」：保留「已達標...」的詳細原因，不強制覆蓋為「處置中」。
4. [邏輯] 開放 TPEx (OTC) 股票進行處置預測。
5. [Zeabur] 自動適配環境變數與 SSL 憑證忽略。
"""

import os
import sys

# 自動安裝缺少的套件 (針對 Zeabur 環境保險起見)
try:
    import twstock
    import yfinance as yf
    import pandas as pd
    import numpy as np
    import requests
    import re
    import time
    import gspread
    import logging
    import urllib3
    from google.oauth2.service_account import Credentials
    from google.auth import default
    from datetime import datetime, timedelta, time as dt_time, date
    from dateutil.relativedelta import relativedelta
    from zoneinfo import ZoneInfo
except ImportError:
    os.system('pip install twstock yfinance gspread google-auth python-dateutil requests pandas zoneinfo --quiet')
    import twstock
    import yfinance as yf
    import pandas as pd
    import numpy as np
    import requests
    import re
    import time
    import gspread
    import logging
    import urllib3
    from google.oauth2.service_account import Credentials
    from google.auth import default
    from datetime import datetime, timedelta, time as dt_time, date
    from dateutil.relativedelta import relativedelta
    from zoneinfo import ZoneInfo

# ==========================================
# 1. 設定靜音模式與常數
# ==========================================
# 忽略 SSL 警告 (解決 Zeabur 連線失敗問題)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger('yfinance')
logger.setLevel(logging.CRITICAL)
logger.disabled = True

UNIT_LOT = 1000

# 定義統計表頭
STATS_HEADERS = [
    '代號', '名稱', '連續天數', '近30日注意次數', '近10日注意次數', '最近一次日期',
    '30日狀態碼', '10日狀態碼', '最快處置天數', '處置觸發原因', '風險等級', '觸發條件',
    '目前價', '警戒價', '差幅(%)', '目前量', '警戒量', '成交值(億)',
    '週轉率(%)', 'PE', 'PB', '當沖佔比(%)'
]

# ==========================================
# 📆 設定區
# ==========================================
SHEET_NAME = "台股注意股資料庫_V33"
PARAM_SHEET_NAME = "個股參數"
# Zeabur 預設時區可能為 UTC，強制指定台北時間
try:
    TW_TZ = ZoneInfo("Asia/Taipei")
except:
    TW_TZ = ZoneInfo("UTC") # Fallback

TARGET_DATE = datetime.now(TW_TZ)

SAFE_CRAWL_TIME = dt_time(19, 0)
SAFE_MARKET_OPEN_CHECK = dt_time(16, 30)

# ==========================================
# 🔑 FinMind 金鑰設定 (適配 Zeabur 環境變數)
# ==========================================
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"

FINMIND_TOKENS = []
# 1. 嘗試讀取環境變數 (Zeabur)
env_token = os.getenv('FinMind_1')
if env_token: FINMIND_TOKENS.append(env_token)
env_token2 = os.getenv('FinMind_2')
if env_token2: FINMIND_TOKENS.append(env_token2)

# 2. 嘗試讀取 Colab userdata (Fallback)
try:
    from google.colab import userdata
    t1 = userdata.get('FinMind_1')
    if t1 and t1 not in FINMIND_TOKENS: FINMIND_TOKENS.append(t1)
    t2 = userdata.get('FinMind_2')
    if t2 and t2 not in FINMIND_TOKENS: FINMIND_TOKENS.append(t2)
except: pass

CURRENT_TOKEN_INDEX = 0
_FINMIND_CACHE = {}

print(f"🚀 啟動 V116.18 台股注意股系統 (Zeabur Ported)")
print(f"🕒 系統時間 (Taiwan): {TARGET_DATE.strftime('%Y-%m-%d %H:%M:%S')}")

try: twstock.__update_codes()
except: pass

# ============================
# 🛠️ 工具函式 (含正規化與關鍵字修正)
# ============================
CN_NUM = {"一":"1","二":"2","三":"3","四":"4","五":"5","六":"6","七":"7","八":"8","九":"9","十":"10"}

# 🔥 [關鍵字對照表]
KEYWORD_MAP = {
    "起迄兩個營業日": 11,
    "當日沖銷": 13,
    "借券賣出": 12,
    "累積週轉率": 10,
    "週轉率": 4,
    "成交量": 9,
    "本益比": 6,
    "股價淨值比": 6,
    "溢折價": 8,
    "收盤價漲跌百分比": 1,
    "最後成交價漲跌": 1,
    "最近六個營業日累積": 1
}

def normalize_clause_text(s: str) -> str:
    if not s: return ""
    s = str(s)
    s = s.replace("第ㄧ款", "第一款")
    for cn, dg in CN_NUM.items():
        s = s.replace(f"第{cn}款", f"第{dg}款")
    s = s.translate(str.maketrans("１２３４５６７８９０", "1234567890"))
    return s

def parse_clause_ids_strict(clause_text):
    if not isinstance(clause_text, str): return set()
    clause_text = normalize_clause_text(clause_text)
    ids = set()
    
    # 1. 優先嘗試抓取標準格式
    matches = re.findall(r'第\s*(\d+)\s*款', clause_text)
    for m in matches:
        ids.add(int(m))
    
    # 2. 關鍵字補救
    if not ids:
        for keyword, code in KEYWORD_MAP.items():
            if keyword in clause_text:
                ids.add(code)
                
    return ids

# 🔥 [修正] 條款文字合併
def merge_clause_text(a, b):
    ids = set()
    ids |= parse_clause_ids_strict(a) if a else set()
    ids |= parse_clause_ids_strict(b) if b else set()
    if ids:
        return "、".join([f"第{x}款" for x in sorted(ids)])
    a = a or ""
    b = b or ""
    return a if len(a) >= len(b) else b

# 🔥 [關鍵] 判斷是否為「計分條款」(1~8款)
def is_valid_accumulation_day(ids):
    if not ids: return False
    return any(1 <= x <= 8 for x in ids)

# 🔥 [修正] 判斷是否為「特殊風險條款」(9~14款)
def is_special_risk_day(ids):
    if not ids: return False
    return any(9 <= x <= 14 for x in ids)

def get_ticker_suffix(market_type):
    m = str(market_type).upper().strip()
    keywords = ['上櫃', 'TWO', 'TPEX', 'OTC']
    if any(k in m for k in keywords):
        return '.TWO'
    return '.TW'

def get_or_create_ws(sh, title, headers=None, rows=5000, cols=20):
    need_cols = max(cols, len(headers) if headers else 0)
    try:
        ws = sh.worksheet(title)
        try:
            if headers and ws.col_count < need_cols:
                ws.resize(rows=ws.row_count, cols=need_cols)
        except: pass
        return ws
    except:
        print(f"⚠️ 工作表 '{title}' 不存在，正在建立 (cols={need_cols})...")
        ws = sh.add_worksheet(title=title, rows=str(rows), cols=str(need_cols))
        if headers:
            ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws

# ============================
# API 工具函數 (含 Zeabur SSL Fix)
# ============================
def finmind_get(dataset, data_id=None, start_date=None, end_date=None):
    global CURRENT_TOKEN_INDEX

    cache_key = (dataset, data_id, start_date, end_date)
    if cache_key in _FINMIND_CACHE:
        return _FINMIND_CACHE[cache_key].copy()

    params = {"dataset": dataset}
    if data_id: params["data_id"] = str(data_id)
    if start_date: params["start_date"] = start_date
    if end_date: params["end_date"] = end_date
    
    # 允許無 Token 嘗試 (免費版)
    tokens_to_try = FINMIND_TOKENS if FINMIND_TOKENS else [None]

    for _ in range(4):
        token = tokens_to_try[CURRENT_TOKEN_INDEX % len(tokens_to_try)]
        headers = {"User-Agent": "Mozilla/5.0", "Connection": "close"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
            
        try:
            # [Zeabur Fix] 加上 verify=False
            r = requests.get(FINMIND_API_URL, params=params, headers=headers, timeout=10, verify=False)
            if r.status_code == 200:
                j = r.json()
                df = pd.DataFrame(j["data"]) if "data" in j else pd.DataFrame()

                if len(_FINMIND_CACHE) >= 2000:
                    _FINMIND_CACHE.clear()
                _FINMIND_CACHE[cache_key] = df

                return df.copy()
            elif r.status_code != 200 and token:
                print(f"   ⚠️ Token {CURRENT_TOKEN_INDEX} 異常 (Status: {r.status_code})，切換下一組...")
                time.sleep(2)
                CURRENT_TOKEN_INDEX += 1
                continue
        except:
            time.sleep(1)
    return pd.DataFrame()

# ============================
# 🔥 大盤監控更新
# ============================
def update_market_monitoring_log(sh):
    print("📊 檢查並更新「大盤數據監控」...")
    HEADERS = ['日期', '代號', '名稱', '收盤價', '漲跌幅(%)', '成交金額(億)']
    ws_market = get_or_create_ws(sh, "大盤數據監控", headers=HEADERS, cols=10)

    def norm_date(s):
        s = str(s).strip()
        if not s: return ""
        try:
            return pd.to_datetime(s, errors='coerce').strftime("%Y-%m-%d")
        except:
            return s

    key_to_row = {}
    try:
        all_vals = ws_market.get_all_values()
        for r_idx, row in enumerate(all_vals[1:], start=2):
            if len(row) >= 2:
                d_str = norm_date(row[0])
                c_str = str(row[1]).strip()
                if d_str and c_str:
                    key_to_row[f"{d_str}_{c_str}"] = r_idx
    except:
        pass

    existing_keys = set(key_to_row.keys())

    try:
        targets = [
            {'fin_id': 'TAIEX', 'code': '^TWII', 'name': '加權指數'},
            {'fin_id': 'TPEx',  'code': '^TWOII', 'name': '櫃買指數'}
        ]

        start_date_str = (TARGET_DATE - timedelta(days=45)).strftime("%Y-%m-%d")

        dfs = {}
        for t in targets:
            fin_id = t['fin_id']
            code = t['code']
            df = finmind_get("TaiwanStockPrice", data_id=fin_id, start_date=start_date_str)

            if not df.empty:
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)
                    df.index = df.index.tz_localize(None)

                if 'close' in df.columns:
                    df['Close'] = df['close'].astype(float)
                    df['Pct'] = df['Close'].pct_change() * 100

                if 'Turnover' in df.columns:
                    df['Volume'] = df['Turnover'].astype(float)
                elif 'Trading_money' in df.columns:
                    df['Volume'] = df['Trading_money'].astype(float)
                else:
                    df['Volume'] = 0.0

                dfs[code] = df

        new_rows = []
        today_str = TARGET_DATE.strftime("%Y-%m-%d")
        all_dates = set()
        for df in dfs.values():
            all_dates.update(df.index.strftime("%Y-%m-%d").tolist())

        for d in sorted(all_dates):
            for t in targets:
                code = t['code']
                name = t['name']
                df = dfs.get(code)

                if df is None or d not in df.index.strftime("%Y-%m-%d"):
                    continue

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

                if d == today_str and TARGET_DATE.time() < SAFE_MARKET_OPEN_CHECK:
                    if code == '^TWII': print(f"   ⏳ 今日 ({d}) 尚未收盤，跳過寫入。")
                    continue

                if d == today_str and comp_key in key_to_row and TARGET_DATE.time() >= SAFE_MARKET_OPEN_CHECK:
                    r_num = key_to_row[comp_key]
                    try:
                        ws_market.update(values=[row_data], range_name=f'A{r_num}:F{r_num}', value_input_option="USER_ENTERED")
                        print(f"   🔄 已覆寫更新今日 ({d} {name}) 數據 (Row {r_num})。")
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

# ============================
# 🔥 處置資料抓取 (Jail) - 含 Zeabur SSL Fix
# ============================
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
        # [Zeabur Fix] 加上 verify=False
        r = requests.get(url, params={"startDate": s_str, "endDate": e_str, "response": "json"}, timeout=10, verify=False)
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
        # [Zeabur Fix] 加上 verify=False
        r = requests.get(url, timeout=10, verify=False)
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

def is_in_jail(stock_id, target_date, jail_map):
    if not jail_map or stock_id not in jail_map: return False
    periods = jail_map[stock_id]
    for start, end in periods:
        if start <= target_date <= end: return True
    return False

# 🔥 [新增] 找前一個交易日
def prev_trade_date(d, cal_dates):
    if not cal_dates: return None
    try:
        idx = cal_dates.index(d)
    except ValueError:
        idx = None
        for i in range(len(cal_dates)-1, -1, -1):
            if cal_dates[i] < d:
                idx = i
                break
        if idx is None: return None
    if idx - 1 >= 0: return cal_dates[idx - 1]
    return None

# 🔥 [新增] 建立官方排除日地圖
def build_exclude_map(cal_dates, jail_map):
    exclude_map = {}
    if not jail_map: return exclude_map
    
    cal_set = set(cal_dates)
    for code, periods in jail_map.items():
        s = set()
        for start, end in periods:
            # 2) 處置前一日
            pd = prev_trade_date(start, cal_dates)
            if pd: s.add(pd)
            # 1) 處置期間 (只放交易日)
            curr = start
            for d in cal_dates:
                if start <= d <= end: s.add(d)
        exclude_map[code] = s
    return exclude_map

def is_excluded(code, d, exclude_map):
    return bool(exclude_map) and (code in exclude_map) and (d in exclude_map[code])

# 🔥 [修正] 取最後 N 個「非處置」交易日 (含歸零邏輯)
def get_last_n_non_jail_trade_dates(stock_id, cal_dates, jail_map, exclude_map=None, n=30):
    # 🔥 [修正] 處置歸零邏輯：找出最近一次處置的結束日
    last_jail_end = date(1900, 1, 1)
    if jail_map and stock_id in jail_map:
        last_jail_end = jail_map[stock_id][-1][1]

    picked = []
    for d in reversed(cal_dates):
        # 🔥 [修正] 若日期早於等於最近一次處置結束日，代表那是前一次案底，需歸零(停止往前抓)
        if d <= last_jail_end:
            break

        if is_excluded(stock_id, d, exclude_map):
            continue
        if jail_map and is_in_jail(stock_id, d, jail_map):
            continue
        
        picked.append(d)
        if len(picked) >= n: break
    return list(reversed(picked))

# ============================
# 🔥 官方公告爬蟲 (注意股) - 含 Zeabur SSL Fix
# ============================
def get_daily_data(date_obj):
    date_str_nodash = date_obj.strftime("%Y%m%d")
    date_str = date_obj.strftime("%Y-%m-%d")
    rows = []
    error_count = 0

    print(f"📡 嘗試爬取官方公告 (日期: {date_str})...")

    # 1. TWSE
    try:
        # [Zeabur Fix] verify=False
        r = requests.get("https://www.twse.com.tw/rwd/zh/announcement/notice",
                         params={"startDate": date_str_nodash, "endDate": date_str_nodash, "response": "json"}, 
                         timeout=10, verify=False)
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
        # [Zeabur Fix] verify=False
        r = requests.post("https://www.tpex.org.tw/www/zh-tw/bulletin/attention", 
                          data={'date': roc_date, 'response': 'json'}, 
                          headers=headers, timeout=10, verify=False)
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

# ============================
# 📆 交易日曆與當沖數據
# ============================
def is_market_open_by_finmind(date_str):
    df = finmind_get("TaiwanStockPrice", data_id="2330", start_date=date_str, end_date=date_str)
    return not df.empty

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
    today_str = today_date.strftime("%Y-%m-%d")
    is_late_enough = TARGET_DATE.time() > SAFE_MARKET_OPEN_CHECK

    if dates and today_date > dates[-1] and today_date.weekday() < 5:
        if is_late_enough:
            print(f"⚠️ 日曆缺漏今日 ({today_date})，時間已過 {SAFE_MARKET_OPEN_CHECK}，驗證開市中...")
            if is_market_open_by_finmind(today_str):
                print(f"✅ 驗證成功 (2330有價)，補入今日。")
                dates.append(today_date)
            else:
                print(f"⛔ 驗證失敗 (2330無價)，判斷為休市或資料未更新，不補入。")
        else:
            print(f"⏳ 時間尚早，暫不強制補入今日日曆。")

    return dates[-days:]

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
    except:
        return 0.0, 0.0

# ============================
# 基礎資料
# ============================
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

# ============================
# 🔥 [核心重寫] 官方條文嚴格判定
# ============================
def calc_pct(curr, ref):
    return ((curr - ref) / ref) * 100 if ref != 0 else 0

def calculate_full_risk(stock_id, hist_df, fund_data, est_days, dt_today_pct, dt_avg6_pct):
    res = {'risk_level': '低', 'trigger_msg': '', 'curr_price': 0, 'limit_price': 0, 'gap_pct': 999.0, 'curr_vol': 0, 'limit_vol': 0, 'turnover_val': 0, 'turnover_rate': 0, 'pe': fund_data.get('pe', 0), 'pb': fund_data.get('pb', 0), 'day_trade_pct': dt_today_pct, 'is_triggered': False}

    if hist_df.empty or len(hist_df) < 7:
        if est_days <= 1: res['risk_level'] = '高'
        elif est_days <= 2: res['risk_level'] = '中'
        return res

    curr_close = float(hist_df.iloc[-1]['Close'])
    curr_vol_shares = float(hist_df.iloc[-1]['Volume'])
    curr_vol_lots = int(curr_vol_shares / UNIT_LOT)

    shares = fund_data.get('shares', 1)
    if shares > 1: turnover = (curr_vol_shares / shares) * 100
    else: turnover = -1.0

    turnover_val_money = curr_close * curr_vol_shares

    res['curr_price'] = round(curr_close, 2)
    res['curr_vol'] = curr_vol_lots
    res['turnover_rate'] = round(turnover, 2)
    res['turnover_val'] = round(turnover_val_money / 100000000, 2)

    triggers = []
    if curr_close < 5: return res

    window_7 = hist_df.tail(7)
    ref_6 = float(window_7.iloc[0]['Close'])

    # 第 2 條 (第一款)：漲跌幅異常
    rise_6 = calc_pct(curr_close, ref_6)
    price_diff_6 = abs(curr_close - ref_6)

    cond_1 = rise_6 > 32
    cond_2 = (rise_6 > 25) and (price_diff_6 >= 50)

    if cond_1: triggers.append(f"【第一款】6日漲{rise_6:.1f}%(>32%)")
    elif cond_2: triggers.append(f"【第一款】6日漲{rise_6:.1f}%且價差{price_diff_6:.0f}元")

    limit_p1 = ref_6 * 1.32
    limit_p2 = ref_6 * 1.25 if price_diff_6 >= 50 else 99999
    final_limit = min(limit_p1, limit_p2) if cond_2 else limit_p1
    res['limit_price'] = round(final_limit, 2)
    res['gap_pct'] = round(((final_limit - curr_close)/curr_close)*100, 1)

    # 第 3 條 (第二款)：波段漲幅
    if len(hist_df) >= 31:
        w = hist_df.tail(31)
        rise_30 = calc_pct(curr_close, float(w.iloc[0]['Close']))
        if rise_30 > 100: triggers.append(f"【第二款】30日漲{rise_30:.0f}%")
    if len(hist_df) >= 61:
        w = hist_df.tail(61)
        rise_60 = calc_pct(curr_close, float(w.iloc[0]['Close']))
        if rise_60 > 130: triggers.append(f"【第二款】60日漲{rise_60:.0f}%")
    if len(hist_df) >= 91:
        w = hist_df.tail(91)
        rise_90 = calc_pct(curr_close, float(w.iloc[0]['Close']))
        if rise_90 > 160: triggers.append(f"【第二款】90日漲{rise_90:.0f}%")

    # 第 4 條 (第三款)：價量異常
    if len(hist_df) >= 61:
        avg_vol_60 = hist_df['Volume'].iloc[-61:-1].mean()
        if avg_vol_60 > 0:
            vol_ratio = curr_vol_shares / avg_vol_60
            res['limit_vol'] = int(avg_vol_60 * 5 / 1000)
            if turnover >= 0.1 and curr_vol_lots >= 500:
                if rise_6 > 25 and vol_ratio > 5:
                    triggers.append(f"【第三款】漲{rise_6:.0f}%+量{vol_ratio:.1f}倍")

    # 第 5 條 (第四款)：價+週轉
    if turnover > 10 and rise_6 > 25:
        triggers.append(f"【第四款】漲{rise_6:.0f}%+轉{turnover:.0f}%")

    # 第 10 條 (第九款)：量能異常
    if len(hist_df) >= 61:
        avg_vol_60 = hist_df['Volume'].iloc[-61:-1].mean()
        avg_vol_6 = hist_df['Volume'].iloc[-6:].mean()
        is_exclude = (turnover < 0.1) or (curr_vol_lots < 500) or (turnover_val_money < 30000000)
        if not is_exclude and avg_vol_60 > 0:
            r1 = avg_vol_6 / avg_vol_60
            r2 = curr_vol_shares / avg_vol_60
            if r1 > 5: triggers.append(f"【第九款】6日均量放大{r1:.1f}倍")
            if r2 > 5: triggers.append(f"【第九款】當日量放大{r2:.1f}倍")

    # 第 11 條 (第十款)：累積週轉
    if turnover > 0:
        acc_vol_6 = hist_df['Volume'].iloc[-6:].sum()
        acc_turn = (acc_vol_6 / shares) * 100
        if turnover_val_money >= 500000000:
            if acc_turn > 50 and turnover > 10:
                triggers.append(f"【第十款】累轉{acc_turn:.0f}%")

    # 第 12 條 (第十一款)：價差異常
    if len(hist_df) >= 6:
        window_6 = hist_df.tail(6)
        high_6 = window_6['High'].max()
        low_6 = window_6['Low'].min()
        gap = high_6 - low_6
        threshold = 100
        if curr_close >= 500:
            tiers = int((curr_close - 500) / 500) + 1
            threshold = 100 + (tiers * 25)
        if gap >= threshold:
            triggers.append(f"【第十一款】6日價差{gap:.0f}元(>門檻{threshold})")

    # 第 14 條 (第十三款)：當沖
    if dt_avg6_pct > 60 and dt_today_pct > 60:
        dt_vol_est = curr_vol_shares * (dt_today_pct / 100.0)
        dt_vol_lots = dt_vol_est / 1000
        is_exclude = (turnover < 5) or (turnover_val_money < 500000000) or (dt_vol_lots < 5000)
        if not is_exclude:
            triggers.append(f"【第十三款】當沖{dt_today_pct}%(6日{dt_avg6_pct}%)")

    if triggers:
        res['is_triggered'] = True
        res['risk_level'] = '高'
        res['trigger_msg'] = "且".join(triggers)
    elif est_days <= 1: res['risk_level'] = '高'
    elif est_days <= 2: res['risk_level'] = '中'
    elif est_days >= 3: res['risk_level'] = '低'

    return res

# ============================
# 🔥 [新增] 現況處置檢查
# ============================
def check_jail_trigger_now(status_list, clause_list):
    status_list = list(status_list)
    clause_list = list(clause_list)

    if len(status_list) < 30:
        pad = 30 - len(status_list)
        status_list = [0]*pad + status_list
        clause_list = [""]*pad + clause_list

    c1_streak = 0
    for c in clause_list[-3:]:
        if 1 in parse_clause_ids_strict(c):
            c1_streak += 1

    valid_cnt_5 = 0
    valid_cnt_10 = 0
    valid_cnt_30 = 0

    total_len = len(status_list)
    for i in range(30):
        idx = total_len - 1 - i
        if idx < 0: break

        if status_list[idx] == 1:
            ids = parse_clause_ids_strict(clause_list[idx])
            # [Fix] 統一呼叫 is_valid_accumulation_day
            if is_valid_accumulation_day(ids):
                if i < 5:  valid_cnt_5  += 1
                if i < 10: valid_cnt_10 += 1
                valid_cnt_30 += 1

    reasons = []
    if c1_streak == 3: reasons.append("已觸發(連3第一款)")
    if valid_cnt_5 == 5: reasons.append("已觸發(連5)")
    if valid_cnt_10 >= 6: reasons.append(f"已觸發(10日{valid_cnt_10}次)")
    if valid_cnt_30 >= 12: reasons.append(f"已觸發(30日{valid_cnt_30}次)")

    return (len(reasons) > 0), " | ".join(reasons)

# ============================
# 🔥 處置預測 (Fix: 顯示優化 & 狀態判斷)
# ============================
def simulate_days_to_jail_strict(status_list, clause_list, *, stock_id=None, target_date=None, jail_map=None, enable_safe_filter=True):
    # 0) 若今天已在處置期：0 天（官方：處置中）
    if stock_id and target_date and jail_map and is_in_jail(stock_id, target_date, jail_map):
        return 0, "處置中"

    # 1) 若已達標：處置從次一營業日開始 => 改為 0 天 (代表即將處置/已達標)
    trigger_now, reason_now = check_jail_trigger_now(status_list, clause_list)
    if trigger_now:
        return 0, reason_now.replace("已觸發", "已達標，次一營業日處置")

    # [Fix] 10日安全過濾 (排除9~14款干擾)
    if enable_safe_filter:
        recent_valid_10 = 0
        check_len = min(len(status_list), 10)
        if check_len > 0:
            recent_statuses = status_list[-check_len:]
            recent_clauses = clause_list[-check_len:]
            for b, c in zip(recent_statuses, recent_clauses):
                if b == 1:
                    ids = parse_clause_ids_strict(c)
                    if is_valid_accumulation_day(ids):
                        recent_valid_10 += 1
        if recent_valid_10 == 0:
            return 99, "X"

    status_list = list(status_list)
    clause_list = list(clause_list)

    if len(status_list) < 30:
        pad = 30 - len(status_list)
        status_list = [0]*pad + status_list
        clause_list = [""]*pad + clause_list

    days = 0
    while days < 10:
        days += 1
        status_list.append(1)
        # [Modified] 模擬字串改為「第1款」
        clause_list.append("第1款")

        c1_streak = 0
        for c in clause_list[-3:]:
            if 1 in parse_clause_ids_strict(c): c1_streak += 1

        valid_cnt_5 = 0
        valid_cnt_10 = 0
        valid_cnt_30 = 0

        total_len = len(status_list)
        for i in range(30):
            idx = total_len - 1 - i
            if idx < 0: break

            if status_list[idx] == 1:
                ids = parse_clause_ids_strict(clause_list[idx])
                if is_valid_accumulation_day(ids):
                    if i < 5: valid_cnt_5 += 1
                    if i < 10: valid_cnt_10 += 1
                    valid_cnt_30 += 1

        reasons = []
        if c1_streak == 3: reasons.append(f"再{days}天處置")
        if valid_cnt_5 == 5: reasons.append(f"再{days}天處置(連5)")
        if valid_cnt_10 >= 6: reasons.append(f"再{days}天處置(10日{valid_cnt_10}次)")
        if valid_cnt_30 >= 12: reasons.append(f"再{days}天處置(30日{valid_cnt_30}次)")

        if reasons:
            # [Modified] 移除 +1 邏輯，直接回傳需要觸發的天數
            return days, " | ".join(reasons)

    return 99, ""

# ============================
# 🔥 Zeabur 專用連線 (自動切換)
# ============================
def connect_google_sheets():
    print("正在進行 Google 驗證...")
    try:
        # A. 優先嘗試讀取 Zeabur 注入的設定檔 /service_key.json
        key_path = "/service_key.json" # Zeabur 絕對路徑
        if not os.path.exists(key_path):
            key_path = "service_key.json" # 本地相對路徑
            
        if os.path.exists(key_path):
            gc = gspread.service_account(filename=key_path)
        else:
            # B. Fallback 到 Colab 的自動驗證 (雖然 Zeabur 不會用到，但保留相容性)
            auth.authenticate_user()
            creds, _ = default()
            gc = gspread.authorize(creds)
            
        try: sh = gc.open(SHEET_NAME)
        except: sh = gc.create(SHEET_NAME)
        return sh, None
    except Exception as e:
        print(f"❌ Google Sheet 連線失敗: {e}")
        return None, None

def main():
    sh, _ = connect_google_sheets()
    if not sh: return

    # [Fix] 大盤數據更新 (格式重構版 + FinMind修正)
    update_market_monitoring_log(sh)

    # 🔥 [關鍵修正] 交易日曆拉長至 240 天
    cal_dates = get_official_trading_calendar(240)
    target_trade_date_obj = cal_dates[-1]

    official_stocks = get_daily_data(target_trade_date_obj)

    is_today = (target_trade_date_obj == TARGET_DATE.date())
    is_early = (TARGET_DATE.time() < SAFE_CRAWL_TIME)
    is_pending = (official_stocks == [] and is_today and is_early)

    if official_stocks is None or is_pending:
        if len(cal_dates) >= 2:
            print("🔄 啟動「時光回朔機制」，退回上一個交易日 (T-1)...")
            cal_dates = cal_dates[:-1]
            target_trade_date_obj = cal_dates[-1]
            official_stocks = get_daily_data(target_trade_date_obj)
        else:
            print("❌ 交易日曆不足，無法回朔，維持原日期。")

    target_date_str = target_trade_date_obj.strftime("%Y-%m-%d")
    print(f"📅 最終鎖定運算日期: {target_date_str}")

    ws_log = get_or_create_ws(sh, "每日紀錄", headers=['日期','市場','代號','名稱','觸犯條款'])

    total_log_rows = 0
    try:
        col1 = ws_log.col_values(1)
        total_log_rows = len(col1)
    except: pass

    if official_stocks:
        print(f"💾 寫入資料庫...")
        existing_keys = set()

        def strict_date_str(raw):
            try:
                return pd.to_datetime(str(raw).strip()).strftime("%Y-%m-%d")
            except:
                return str(raw).strip()

        if total_log_rows < 2:
            try:
                existing_data = ws_log.get_all_values()
                if len(existing_data) > 1:
                    for row in existing_data[1:]:
                        if len(row) >= 3 and row[0] != '日期' and str(row[2]).isdigit():
                            d_std = strict_date_str(row[0])
                            existing_keys.add(f"{d_std}_{row[2]}")
                total_log_rows = len(existing_data)
            except: pass
        else:
            try:
                start_row = max(1, total_log_rows - 3000)
                raw_keys = ws_log.get(f'A{start_row}:E{total_log_rows}')
                if raw_keys:
                    for r in raw_keys:
                        if len(r) >= 3 and r[0] != '日期' and str(r[2]).isdigit():
                            d_std = strict_date_str(r[0])
                            existing_keys.add(f"{d_std}_{r[2]}")
            except: pass

        new_rows = []
        today_codes = set([s['代號'] for s in official_stocks])

        for stock in official_stocks:
            if stock['代號'] not in today_codes:
                continue

            key = f"{stock['日期']}_{stock['代號']}"
            if key not in existing_keys:
                new_rows.append([stock['日期'], stock['市場'], stock['代號'], stock['名稱'], stock['觸犯條款']])

        if new_rows:
            ws_log.append_rows(new_rows, value_input_option='USER_ENTERED')
            total_log_rows += len(new_rows)

    precise_db_cache = load_precise_db_from_sheet(sh)
    print("📊 正在同步大盤資料...")
    finmind_trade_date_str = target_date_str

    try:
        if total_log_rows < 2: raise ValueError("Too small")

        limit = 8000
        start_idx = max(1, total_log_rows - limit)
        raw_vals = ws_log.get(f'A{start_idx}:E{total_log_rows}')

        if not raw_vals or len(raw_vals) < 2:
            raise ValueError("Empty range")

        if start_idx > 1:
            headers = ws_log.get('A1:E1')
            if headers: raw_vals = headers + raw_vals

        df = pd.DataFrame(raw_vals[1:], columns=raw_vals[0])

        df.columns = [str(c).strip() for c in df.columns]
        req_cols = {'日期', '代號', '名稱', '觸犯條款'}
        if not req_cols.issubset(set(df.columns)):
             raise ValueError(f"Missing columns: {req_cols - set(df.columns)}")

        tmp_dates = pd.to_datetime(df['日期'], errors='coerce')
        min_ts = tmp_dates.dropna().min()
        
        # [Modified] 用 cal_dates 計算回朔日期，確保數據足夠
        start_date_90 = cal_dates[-90] if len(cal_dates) >= 90 else cal_dates[0]
        start_ts = pd.Timestamp(start_date_90)

        if pd.notna(min_ts) and min_ts > start_ts:
            print("⚠️ 緩衝區間不足，改為讀取全表...")
            all_vals = ws_log.get_all_values()

            if not all_vals or len(all_vals) < 2:
                df = pd.DataFrame(columns=['日期','市場','代號','名稱','觸犯條款'])
            else:
                df = pd.DataFrame(all_vals[1:], columns=all_vals[0])
                df.columns = [str(c).strip() for c in df.columns]
                df['日期'] = pd.to_datetime(df['日期'], errors='coerce').dt.date
        else:
            df['日期'] = tmp_dates.dt.date

    except Exception as e:
        print(f"⚠️ 讀取優化失敗 ({e})，降級為全表讀取...")
        all_vals = ws_log.get_all_values()
        if not all_vals or len(all_vals) < 2:
            df = pd.DataFrame(columns=['日期','市場','代號','名稱','觸犯條款'])
        else:
            df = pd.DataFrame(all_vals[1:], columns=all_vals[0])
            df.columns = [str(c).strip() for c in df.columns]
            df['日期'] = pd.to_datetime(df['日期'], errors='coerce').dt.date

    if not df.empty:
        df['代號'] = df['代號'].astype(str).str.strip()
        df = df[df['代號'].str.match(r'^\d{4}$', na=False)]
        df = df[pd.notna(df['日期'])]

    # 🔥 [關鍵修正] 建立 clause_map (同日合併)
    clause_map = {}
    for _, r in df.iterrows():
        try: 
            k = (str(r['代號']), r['日期'])
            new_text = str(r.get('觸犯條款', '') or '')
            old_text = clause_map.get(k, "")
            clause_map[k] = merge_clause_text(old_text, new_text)
        except: pass

    # [Modified] 篩選最近有出現的股票 (用 90 天比較保險)
    df_recent = df[df['日期'] >= start_date_90]
    stock_list = df_recent['代號'].unique()

    target_stocks = stock_list
    total_scan = len(target_stocks)
    
    # 🔥 [關鍵新增] 建立處置濾網 (Jail Mask) & 排除地圖 (Exclude Map)
    jail_lookback = target_trade_date_obj - timedelta(days=90)
    jail_map = get_jail_map(jail_lookback, target_trade_date_obj)
    exclude_map = build_exclude_map(cal_dates, jail_map)

    print(f"🔍 開始掃描 {total_scan} 檔股票 (V116.18 完整邏輯版 - 單次全執行)...")

    rows_stats = []

    for idx, code in enumerate(target_stocks):
        code = str(code).strip()

        name_series = df[df['代號']==code]['名稱']
        name = name_series.iloc[-1] if not name_series.empty else "未知"

        db_info = precise_db_cache.get(code, {})
        suffix = get_ticker_suffix(db_info.get('market', '上市'))
        ticker_code = f"{code}{suffix}"

        # 🔥 [關鍵修正] 使用「最近 N 個非處置交易日」作為日曆 (使用 exclude_map)
        stock_calendar_30_asc = get_last_n_non_jail_trade_dates(
            code, cal_dates, jail_map, exclude_map=exclude_map, n=30
        )

        bits = []
        clauses = []
        
        # 使用過濾後的日曆
        for d in stock_calendar_30_asc:
            c_str = clause_map.get((code, d), "")
            
            # Step 1: 官方排除日（處置執行前 + 處置期間）=> 不計次
            if is_excluded(code, d, exclude_map):
                bits.append(0)
                clauses.append(c_str) # 保留字串方便判斷延長
                continue
            
            # Step 2: 有公告條款才算注意日
            if c_str:
                bits.append(1)
                clauses.append(c_str)
            else:
                bits.append(0)
                clauses.append("")

        # 🔥 [關鍵新增] 建立顯示用的 valid_bits (只標記有效注意日 1~8款)
        valid_bits = []
        for i in range(len(bits)):
            if bits[i] == 1:
                ids = parse_clause_ids_strict(clauses[i])
                valid_bits.append(1 if is_valid_accumulation_day(ids) else 0)
            else:
                valid_bits.append(0)

        # 🔥 [關鍵修改] 狀態碼改用 valid_bits 顯示
        status_30 = "".join(map(str, valid_bits))
        if len(status_30) < 30: status_30 = status_30.zfill(30)

        # 🔥 [關鍵] TPEx 暫不預測處置 -> Removed/Modified to allow execution
        market = db_info.get('market', '上市')
        is_otc = (suffix == ".TWO") or any(k in str(market) for k in ['上櫃', 'TWO', 'TPEX', 'OTC'])
        
        # 定義預設值 (計算用)
        est_days_int = 99
        est_days_display = "X"
        reason_display = ""
        is_special_risk = False
        is_clause_13 = False

        # [修正] 移除對 OTC 的阻擋，允許模擬
        # 預測邏輯 (傳入 jail_map 和 target_date)
        est_days, reason_msg = simulate_days_to_jail_strict(
            bits, clauses,
            stock_id=code,
            target_date=target_trade_date_obj,
            jail_map=jail_map,
            enable_safe_filter=False 
        )

        # 🔥 [關鍵新增] 檢查當日是否觸發特殊風險 (9~14款)
        latest_ids = parse_clause_ids_strict(clauses[-1] if clauses else "")
        is_special_risk = is_special_risk_day(latest_ids)
        
        # 🔥 [修正] 檢查區間內是否曾觸發第13款 (影響處置天數 10->12)
        is_clause_13 = False
        for c in clauses:
            if 13 in parse_clause_ids_strict(c):
                is_clause_13 = True
                break

        # [Fix] 顯示處理與類型轉換
        # 修正：即使是特殊風險股，最快天數仍顯示標準計算結果 (X or int)，警示語放在原因欄
        if reason_msg == "X":
            est_days_int = 99
            est_days_display = "X" # 即使是特殊風險，這裡仍顯示 X
            if is_special_risk:
                reason_display = "籌碼異常(人工審核風險)"
                if is_clause_13: reason_display += " + 刑期可能延長"
            else:
                reason_display = ""
        elif est_days == 0:
             est_days_int = 0
             est_days_display = "0"
             # [Fix] 優先保留原因 (例如 "已達標...")，若為純處置才顯示 "處置中"
             reason_display = reason_msg
        else:
            est_days_int = int(est_days)
            est_days_display = str(est_days_int)
            reason_display = reason_msg
            if is_special_risk:
                reason_display += " | ⚠️留意人工處置風險"
            if is_clause_13:
                reason_display += " (若進處置將關12天)"

        hist = fetch_history_data(ticker_code)
        if hist.empty:
            alt_suffix = '.TWO' if suffix == '.TW' else '.TW'
            alt_ticker = f"{code}{alt_suffix}"
            hist = fetch_history_data(alt_ticker)
            if not hist.empty: ticker_code = alt_ticker

        fund = fetch_stock_fundamental(code, ticker_code, precise_db_cache)

        if (idx + 1) % 10 == 0:
            time.sleep(1.5)

        dt_today, dt_avg6 = get_daytrade_stats_finmind(code, finmind_trade_date_str)
        
        # 🔥 [Fix] 傳入 int 參數
        risk_res = calculate_full_risk(code, hist, fund, est_days_int, dt_today, dt_avg6)

        print(f"   [{idx+1}/{total_scan}] {code} {name}: 最快{est_days_display}天 {reason_display} | {risk_res['trigger_msg']} | 當沖:{dt_today}%")

        # 🔥 [Modified] 改用 valid_bits 計算連續天數 (排除 9~14 款)
        streak = 0
        for b in valid_bits[::-1]:
            if b == 1:
                streak += 1
            else:
                break

        # 🔥 [Modified] 改用 valid_bits 抓取最近一次日期
        last_trigger_date_str = "無"
        if len(valid_bits) > 0:
            for i in range(len(valid_bits)-1, -1, -1):
                if valid_bits[i] == 1:
                    last_trigger_date_str = stock_calendar_30_asc[i].strftime("%Y-%m-%d")
                    break
        
        # 🔥 [關鍵修正] 顯示用統計 (改用 valid_bits，確保與處置邏輯一致)
        cnt_30 = sum(valid_bits)
        cnt_10 = sum(valid_bits[-10:])

        rows_stats.append([
            code, name, streak, 
            cnt_30, # [Modified] 顯示有效注意次數
            cnt_10, # [Modified] 顯示有效注意次數
            last_trigger_date_str,
            status_30, status_30[-10:], est_days_display, reason_display, risk_res['risk_level'], risk_res['trigger_msg'],
            risk_res['curr_price'], risk_res['limit_price'], risk_res['gap_pct'],
            risk_res['curr_vol'], risk_res['limit_vol'], risk_res['turnover_val'],
            risk_res['turnover_rate'], risk_res['pe'], risk_res['pb'],
            risk_res['day_trade_pct']
        ])

    try:
        ws_stats = get_or_create_ws(sh, "近30日熱門統計", headers=STATS_HEADERS)

        # [Fix] 一次性全寫入，不分段
        print("💾 更新 [近30日熱門統計] (清空重寫)...")
        ws_stats.clear()
        ws_stats.append_row(STATS_HEADERS, value_input_option='USER_ENTERED')
        if rows_stats:
            ws_stats.append_rows(rows_stats, value_input_option='USER_ENTERED')

        print("\n✅ V116.18 執行完成！")
    except Exception as e:
        print(f"❌ 寫入失敗: {e}")

if __name__ == "__main__":
    main()
