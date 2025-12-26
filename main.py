# -*- coding: utf-8 -*-
"""
V116.18 台股注意股系統 (GitHub Action 單檔直上版 - TPEx 失敗防呆修正)
修正重點：
1. [防呆] get_daily_data: 若 TPEx 抓取失敗且無其他資料，回傳 None (而非空串列)。
2. [回補] backfill_daily_logs: 收到 None 時跳過該日處理，不寫入狀態表，確保下次能回補。
3. [保留] 包含所有先前修正 (Warm-up, 17:30時序, 21:00當沖, T-2修復)。
"""

import os
import twstock
import yfinance as yf
import pandas as pd
import numpy as np
import requests
import re
import time
import random
import gspread
import logging
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, time as dt_time, date
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

# ==========================================
# 1. 設定靜音模式與常數
# ==========================================
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
TW_TZ = ZoneInfo("Asia/Taipei")
TARGET_DATE = datetime.now(TW_TZ)

# ✅ [修正] 時間門檻與布林標記
SAFE_CRAWL_TIME = dt_time(17, 30)        # 其他資訊（注意股/統計）固定 17:30 後跑
DAYTRADE_PUBLISH_TIME = dt_time(21, 0)   # 當沖率 21:00 後才抓
SAFE_MARKET_OPEN_CHECK = dt_time(16, 30) # 用於判斷日曆是否該有今天

IS_NIGHT_RUN = TARGET_DATE.hour >= 20 # 保留舊變數兼容，主要邏輯改用下方兩個
IS_AFTER_SAFE = TARGET_DATE.time() >= SAFE_CRAWL_TIME
IS_AFTER_DAYTRADE = TARGET_DATE.time() >= DAYTRADE_PUBLISH_TIME

# 回補參數
MAX_BACKFILL_TRADING_DAYS = 40   # 最多回補幾個交易日(往前)
VERIFY_RECENT_DAYS = 2           # 強制驗證最近幾個交易日

# ==========================================
# 🔑 FinMind 金鑰設定 (GitHub Secret 適配)
# ==========================================
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"

token1 = os.getenv('FinMind_1')
token2 = os.getenv('FinMind_2')
FINMIND_TOKENS = [t for t in [token1, token2] if t]

CURRENT_TOKEN_INDEX = 0
_FINMIND_CACHE = {}

print(f"🚀 啟動 V116.18 台股注意股系統 (Fix: Fail-Safe Logic)")
print(f"🕒 系統時間 (Taiwan): {TARGET_DATE.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"⏰ 時序狀態: After 17:30? {IS_AFTER_SAFE} | After 21:00? {IS_AFTER_DAYTRADE}")

try: twstock.__update_codes()
except: pass

# ============================
# 🛠️ 工具函式
# ============================
CN_NUM = {"一":"1","二":"2","三":"3","四":"4","五":"5","六":"6","七":"7","八":"8","九":"9","十":"10"}

KEYWORD_MAP = {
    "起迄兩個營業日": 11, "當日沖銷": 13, "借券賣出": 12, "累積週轉率": 10, "週轉率": 4,
    "成交量": 9, "本益比": 6, "股價淨值比": 6, "溢折價": 8, "收盤價漲跌百分比": 1,
    "最後成交價漲跌": 1, "最近六個營業日累積": 1
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
    matches = re.findall(r'第\s*(\d+)\s*款', clause_text)
    for m in matches: ids.add(int(m))
    if not ids:
        for keyword, code in KEYWORD_MAP.items():
            if keyword in clause_text: ids.add(code)
    return ids

def merge_clause_text(a, b):
    ids = set()
    ids |= parse_clause_ids_strict(a) if a else set()
    ids |= parse_clause_ids_strict(b) if b else set()
    if ids: return "、".join([f"第{x}款" for x in sorted(ids)])
    a = a or ""; b = b or ""
    return a if len(a) >= len(b) else b

def is_valid_accumulation_day(ids):
    if not ids: return False
    return any(1 <= x <= 8 for x in ids)

def is_special_risk_day(ids):
    if not ids: return False
    return any(9 <= x <= 14 for x in ids)

def get_ticker_suffix(market_type):
    m = str(market_type).upper().strip()
    keywords = ['上櫃', 'TWO', 'TPEX', 'OTC']
    if any(k in m for k in keywords): return '.TWO'
    return '.TW'

def connect_google_sheets():
    try:
        if not os.path.exists("service_key.json"): return None, None
        gc = gspread.service_account(filename="service_key.json")
        try: sh = gc.open(SHEET_NAME)
        except: sh = gc.create(SHEET_NAME)
        return sh, None
    except: return None, None

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
        print(f"⚠️ 工作表 '{title}' 不存在，正在建立...")
        ws = sh.add_worksheet(title=title, rows=str(rows), cols=str(need_cols))
        if headers:
            ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws

def load_log_index(ws_log):
    existing_keys = set()
    date_counts = {}
    try:
        vals = ws_log.get_all_values()
        if not vals or len(vals) <= 1: return existing_keys, date_counts
        for r in vals[1:]:
            if len(r) >= 3 and str(r[0]).strip():
                d = str(r[0]).strip()
                code = str(r[2]).strip().replace("'", "")
                if code:
                    k = d + "_" + code
                    existing_keys.add(k)
                    date_counts[d] = date_counts.get(d, 0) + 1
    except: pass
    return existing_keys, date_counts

def load_status_index(ws_status):
    key_to_row = {}
    cnt_map = {}
    try:
        vals = ws_status.get_all_values()
        if not vals or len(vals) <= 1: return key_to_row, cnt_map
        for r_idx, row in enumerate(vals[1:], start=2):
            if len(row) >= 1 and str(row[0]).strip():
                d = str(row[0]).strip()
                key_to_row[d] = r_idx
                c = 0
                if len(row) >= 2:
                    try: c = int(str(row[1]).strip())
                    except: c = 0
                cnt_map[d] = c
    except: pass
    return key_to_row, cnt_map

def upsert_status(ws_status, key_to_row, date_str, count, now_str):
    row_data = [date_str, int(count), now_str]
    if date_str in key_to_row:
        r = key_to_row[date_str]
        try: ws_status.update(values=[row_data], range_name=f"A{r}:C{r}", value_input_option="USER_ENTERED")
        except: pass
    else:
        try: ws_status.append_row(row_data, value_input_option="USER_ENTERED")
        except: pass

def finmind_get(dataset, data_id=None, start_date=None, end_date=None):
    global CURRENT_TOKEN_INDEX
    cache_key = (dataset, data_id, start_date, end_date)
    if cache_key in _FINMIND_CACHE: return _FINMIND_CACHE[cache_key].copy()

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
                df = pd.DataFrame(j.get("data", [])) if "data" in j else pd.DataFrame()
                if len(_FINMIND_CACHE) >= 2000: _FINMIND_CACHE.clear()
                _FINMIND_CACHE[cache_key] = df
                return df.copy()
            elif r.status_code != 200:
                time.sleep(2)
                CURRENT_TOKEN_INDEX = (CURRENT_TOKEN_INDEX + 1) % len(FINMIND_TOKENS)
                continue
        except: time.sleep(1)
    return pd.DataFrame()

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
                key_to_row[f"{norm_date(row[0])}_{str(row[1]).strip()}"] = r_idx
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

        new_rows = []
        today_str = TARGET_DATE.strftime("%Y-%m-%d")
        all_dates = set()
        for df in dfs.values(): all_dates.update(df.index.strftime("%Y-%m-%d").tolist())

        for d in sorted(all_dates):
            for t in targets:
                code = t['code']; name = t['name']
                df = dfs.get(code)
                if df is None or d not in df.index.strftime("%Y-%m-%d"): continue
                try: row = df.loc[d]
                except: row = df[df.index.strftime("%Y-%m-%d") == d].iloc[0]
                
                if pd.isna(row.get('Close')): continue
                close = round(float(row['Close']), 2)
                pct = round(float(row.get('Pct', 0) or 0), 2)
                vol = round(float(row.get('Volume', 0) or 0) / 100000000, 2)
                
                row_data = [d, code, name, close, pct, vol]
                comp_key = f"{d}_{code}"

                if d == today_str and TARGET_DATE.time() < SAFE_MARKET_OPEN_CHECK: continue
                if d == today_str and comp_key in key_to_row and TARGET_DATE.time() >= SAFE_MARKET_OPEN_CHECK:
                    try:
                        r_num = key_to_row[comp_key]
                        ws_market.update(values=[row_data], range_name=f'A{r_num}:F{r_num}', value_input_option="USER_ENTERED")
                    except: pass
                    continue
                if comp_key in existing_keys: continue
                if close > 0: new_rows.append(row_data)

        if new_rows: ws_market.append_rows(new_rows, value_input_option="USER_ENTERED")
    except Exception as e: print(f" ❌ 大盤更新失敗: {e}")

def parse_roc_date(roc_date_str):
    try:
        p = re.split(r'[/-]', str(roc_date_str).strip())
        if len(p) == 3: return date(int(p[0]) + 1911, int(p[1]), int(p[2]))
    except: return None
    return None

def parse_jail_period(period_str):
    if not period_str: return None, None
    d = period_str.split('～') if '～' in period_str else period_str.split('~')
    if len(d)<2 and '-' in period_str: d = period_str.split('-')
    if len(d) >= 2:
        s, e = parse_roc_date(d[0].strip()), parse_roc_date(d[1].strip())
        if s and e: return s, e
    return None, None

def get_jail_map(start_date_obj, end_date_obj):
    print("🔒 正在下載處置(Jail)名單...")
    jail_map = {}
    s_str = start_date_obj.strftime("%Y%m%d")
    e_str = end_date_obj.strftime("%Y%m%d")

    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get("https://www.twse.com.tw/rwd/zh/announcement/punish", params={"startDate": s_str, "endDate": e_str, "response": "json"}, headers=headers, timeout=10)
        j = r.json()
        if isinstance(j.get("tables"), list) and j["tables"]:
            data_rows = j["tables"][0].get("data", [])
            for row in data_rows:
                try:
                    c = str(row[2]).strip()
                    s, e = parse_jail_period(str(row[6]))
                    if s and e: jail_map.setdefault(c, []).append((s, e))
                except: continue
    except: pass

    try:
        r = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_disposal_information", timeout=10)
        for item in r.json():
            try:
                # ✅ [修正] 增加四碼檢查
                c = str(item.get("SecuritiesCompanyCode", "")).strip()
                if not (c.isdigit() and len(c)==4): continue

                s, e = parse_jail_period(str(item.get("DispositionPeriod", "")).strip())
                if s and e and e >= start_date_obj and s <= end_date_obj:
                    jail_map.setdefault(c, []).append((s, e))
            except: continue
    except: pass

    for k in jail_map: jail_map[k] = sorted(jail_map[k], key=lambda x: x[0])
    return jail_map

def is_in_jail(stock_id, target_date, jail_map):
    if not jail_map or stock_id not in jail_map: return False
    for s, e in jail_map[stock_id]:
        if s <= target_date <= e: return True
    return False

def prev_trade_date(d, cal_dates):
    try:
        idx = cal_dates.index(d)
        if idx > 0:
            return cal_dates[idx - 1]
        return None
    except:
        for i in range(len(cal_dates)-1, -1, -1):
            if cal_dates[i] < d:
                return cal_dates[i]
        return None

def build_exclude_map(cal_dates, jail_map):
    exclude_map = {}
    if not jail_map: return exclude_map
    for code, periods in jail_map.items():
        s = set()
        for start, end in periods:
            pd = prev_trade_date(start, cal_dates)
            if pd: s.add(pd)
            for d in cal_dates:
                if start <= d <= end: s.add(d)
        exclude_map[code] = s
    return exclude_map

def is_excluded(code, d, exclude_map):
    return bool(exclude_map) and (code in exclude_map) and (d in exclude_map[code])

def get_last_n_non_jail_trade_dates(stock_id, cal_dates, jail_map, exclude_map=None, n=30):
    last_jail_end = date(1900, 1, 1)
    if jail_map and stock_id in jail_map: last_jail_end = jail_map[stock_id][-1][1]
    picked = []
    for d in reversed(cal_dates):
        if d <= last_jail_end: break
        if is_excluded(stock_id, d, exclude_map): continue
        if jail_map and is_in_jail(stock_id, d, jail_map): continue
        picked.append(d)
        if len(picked) >= n: break
    return list(reversed(picked))

# ✅ [新增] 獨立 TWSE 爬蟲函式
def fetch_twse_attention_rows(date_obj, date_str):
    date_str_nodash = date_obj.strftime("%Y%m%d")
    rows = []
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get("https://www.twse.com.tw/rwd/zh/announcement/notice",
                         params={"startDate": date_str_nodash, "endDate": date_str_nodash, "response": "json"}, headers=headers, timeout=10)
        if r.status_code == 200:
            d = r.json()
            if 'data' in d:
                for i in d['data']:
                    code = str(i[1]).strip(); name = str(i[2]).strip()
                    if len(code)==4 and code.isdigit():
                        raw = " ".join([str(x) for x in i])
                        ids = parse_clause_ids_strict(raw)
                        c_str = "、".join([f"第{k}款" for k in sorted(ids)]) or raw
                        rows.append({'日期': date_str, '市場': 'TWSE', '代號': code, '名稱': name, '觸犯條款': c_str})
    except: pass
    return rows

# ✅ [新增] 獨立 TPEx 爬蟲函式 (Final 1223 強力版 + Warm-up + Debug Info + 回傳 None)
def fetch_tpex_attention_rows(date_obj, date_str):
    roc_date = f"{date_obj.year - 1911}/{date_obj.month:02d}/{date_obj.day:02d}"
    url = "https://www.tpex.org.tw/www/zh-tw/bulletin/attention"
    
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.tpex.org.tw/",
        "Origin": "https://www.tpex.org.tw",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest", # ✅ 新增
    }
    payload = {"date": roc_date, "response": "json"}

    s = requests.Session()
    # ✅ warm-up：先打一次首頁拿 cookie
    try:
        s.get("https://www.tpex.org.tw/", headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    except:
        pass

    for attempt in range(1, 4):
        try:
            r = s.post(url, data=payload, headers=headers, timeout=12)

            if r.status_code != 200:
                print(f"⚠️ TPEx attention HTTP {r.status_code} (attempt {attempt})")
                time.sleep(0.6 + random.random())
                continue

            # ✅ 嘗試解析 JSON，失敗時印出 HTML 片段以便除錯
            try:
                res = r.json()
            except Exception as e:
                print(f"⚠️ TPEx decode JSON failed: {e}")
                print(f"   Response excerpt: {r.text[:200]}")
                time.sleep(0.6 + random.random())
                continue

            target = []
            if "tables" in res:
                for t in res["tables"]: target.extend(t.get("data", []))
            elif "data" in res:
                target = res["data"]

            filtered_target = []
            for row in target or []:
                if len(row) > 5:
                    row_date = str(row[5]).strip()
                    if row_date == roc_date or row_date == date_str:
                        filtered_target.append(row)

            rows = []
            for i in filtered_target:
                code = str(i[1]).strip()
                name = str(i[2]).strip()
                if not (code.isdigit() and len(code) == 4): continue

                raw_text = " ".join([str(x) for x in i])
                ids = parse_clause_ids_strict(raw_text)
                clause_str = "、".join([f"第{k}款" for k in sorted(ids)]) if ids else raw_text

                rows.append({
                    "日期": date_str, "市場": "TPEx", "代號": code, "名稱": name, "觸犯條款": clause_str
                })
            
            return rows

        except Exception as e:
            print(f"⚠️ TPEx attention exception (attempt {attempt}): {e}")
            time.sleep(0.6 + random.random())

    print("❌ TPEx attention failed after retries.")
    return None # ✅ 失敗回傳 None

# ✅ [修正] get_daily_data 改為整合呼叫 & 錯誤處理
def get_daily_data(date_obj):
    date_str = date_obj.strftime("%Y-%m-%d")
    rows = []
    print(f"📡 爬取公告 {date_str}...")
    
    rows.extend(fetch_twse_attention_rows(date_obj, date_str))
    
    tpex_rows = fetch_tpex_attention_rows(date_obj, date_str)
    tpex_failed = (tpex_rows is None)

    if tpex_failed:
        print("❌ TPEx 抓取失敗 (全部重試皆無回應)")
    else:
        rows.extend(tpex_rows)
    
    # ✅ 關鍵：兩邊都沒資料且 TPEx 失敗 → 回 None（不要當成 0 檔）
    if not rows and tpex_failed:
        return None

    if rows: print(f"✅ 抓到 {len(rows)} 檔")
    else: print(f"⚠️ 無資料")
    return rows

def backfill_daily_logs(sh, ws_log, cal_dates, target_trade_date_obj):
    now_str = TARGET_DATE.strftime("%Y-%m-%d %H:%M:%S")
    existing_keys, date_counts = load_log_index(ws_log)
    ws_status = get_or_create_ws(sh, "爬取狀態", headers=["日期", "抓到檔數", "最後更新時間"], cols=5)
    key_to_row, status_cnt = load_status_index(ws_status)
    status_is_new = (len(status_cnt) == 0)

    # 註解掉：不使用每日紀錄回填狀態表，由(D)負責
    # if not status_is_new: ...

    key_to_row, status_cnt = load_status_index(ws_status)
    window_dates = cal_dates[-MAX_BACKFILL_TRADING_DAYS:] if len(cal_dates) > MAX_BACKFILL_TRADING_DAYS else cal_dates[:]
    recent_dates = cal_dates[-VERIFY_RECENT_DAYS:] if len(cal_dates) >= VERIFY_RECENT_DAYS else cal_dates[:]
    dates_to_check = sorted(set(window_dates + recent_dates))

    rows_to_append = []
    status_updates = []

    print(f"🧩 回補檢查：共 {len(dates_to_check)} 個交易日（含最近 {VERIFY_RECENT_DAYS} 日強制驗證）")

    for d in dates_to_check:
        d_str = d.strftime("%Y-%m-%d")
        
        # ✅ 使用 SAFE_CRAWL_TIME (17:30) 判斷今日是否已過公告時間
        if d == TARGET_DATE.date() and TARGET_DATE.time() < SAFE_CRAWL_TIME: continue

        log_cnt = int(date_counts.get(d_str, 0))
        st_cnt = status_cnt.get(d_str, None)
        need_fetch = False

        if d in recent_dates: need_fetch = True
        if (st_cnt is not None) and (log_cnt < int(st_cnt)): need_fetch = True
        if (st_cnt is None) and (log_cnt == 0): need_fetch = True
        if (st_cnt is None) and (d in window_dates): need_fetch = True

        if not need_fetch: continue

        data = get_daily_data(d)
        
        # ✅ 修正：若回傳 None 代表抓取失敗，跳過不更新狀態
        if data is None:
            print(f"⚠️ {d_str} TPEx 抓取失敗(None)，本輪跳過狀態更新，留待下次回補")
            continue

        official_cnt = len(data)

        for s in data:
            k = f"{s['日期']}_{s['代號']}"
            if k not in existing_keys:
                rows_to_append.append([s['日期'], s['市場'], f"'{s['代號']}", s['名稱'], s['觸犯條款']])
                existing_keys.add(k)
                date_counts[s['日期']] = date_counts.get(s['日期'], 0) + 1

        status_updates.append((d_str, official_cnt, st_cnt))

    if rows_to_append:
        print(f"💾 回補寫入「每日紀錄」：{len(rows_to_append)} 筆")
        ws_log.append_rows(rows_to_append, value_input_option="USER_ENTERED")
    else:
        print("✅ 每日紀錄無需回補寫入")

    key_to_row, status_cnt = load_status_index(ws_status)
    for d_str, official_cnt, old_st_cnt in status_updates:
        write_cnt = official_cnt
        if official_cnt == 0:
            if old_st_cnt is not None and int(old_st_cnt) > 0: write_cnt = int(old_st_cnt)
            elif int(date_counts.get(d_str, 0)) > 0: write_cnt = int(date_counts[d_str])
        upsert_status(ws_status, key_to_row, d_str, write_cnt, now_str)

def is_market_open_by_finmind(date_str):
    df = finmind_get("TaiwanStockPrice", data_id="2330", start_date=date_str, end_date=date_str)
    return not df.empty

def get_official_trading_calendar(days=60):
    end = TARGET_DATE.strftime("%Y-%m-%d")
    start = (TARGET_DATE - timedelta(days=days*2)).strftime("%Y-%m-%d")
    print("📅 下載日曆...")
    df = finmind_get("TaiwanStockTradingDate", start_date=start, end_date=end)
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
            print(f"⚠️ 日曆缺漏今日 ({today_date})，驗證開市中...")
            if is_market_open_by_finmind(today_str):
                print(f"✅ 驗證成功 (2330有價)，補入今日。")
                dates.append(today_date)
            else:
                print(f"⛔ 驗證失敗 (2330無價)，判斷為休市或資料未更新，不補入。")
        else:
            print(f"⏳ 時間尚早，暫不強制補入今日日曆。")

    return dates[-days:]

# ✅ [修正] 抓不到資料時回傳 None (而非 0.0)，以便後續判斷為 Pending
def get_daytrade_stats_finmind(stock_id, target_date_str):
    end = target_date_str
    start = (datetime.strptime(target_date_str, "%Y-%m-%d") - timedelta(days=15)).strftime("%Y-%m-%d")
    df_dt = finmind_get("TaiwanStockDayTrading", stock_id, start_date=start, end_date=end)
    df_p = finmind_get("TaiwanStockPrice", stock_id, start_date=start, end_date=end)
    
    if df_dt.empty or df_p.empty: return None, None
    try:
        m = pd.merge(df_p[['date', 'Trading_Volume']], df_dt[['date', 'Volume']], on='date', how='inner')
        if m.empty: return None, None
        m = m.sort_values('date')
        last = m.iloc[-1]
        td = (last['Volume']/last['Trading_Volume']*100) if last['Trading_Volume']>0 else 0
        avg = m.tail(6); sum_v = avg['Volume'].sum(); sum_t = avg['Trading_Volume'].sum()
        avg_td = (sum_v/sum_t*100) if sum_t>0 else 0
        return round(td, 2), round(avg_td, 2)
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

def calc_pct(curr, ref):
    return ((curr - ref) / ref) * 100 if ref != 0 else 0

# ✅ [修正] 若當沖率為 None，只標記 Pending，不觸發 triggers，且不視為高風險
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
    rise_6 = calc_pct(curr_close, ref_6)
    price_diff_6 = abs(curr_close - ref_6)

    cond_1 = rise_6 > 32
    cond_2 = (rise_6 > 25) and (price_diff_6 >= 50)
    if cond_1: triggers.append(f"【第一款】6日漲{rise_6:.1f}%(>32%)")
    elif cond_2: triggers.append(f"【第一款】6日漲{rise_6:.1f}%且價差{price_diff_6:.0f}元")

    limit_p = ref_6 * 1.32
    if cond_2: limit_p = min(limit_p, ref_6 * 1.25)
    res['limit_price'] = round(limit_p, 2)
    res['gap_pct'] = round(((limit_p - curr_close)/curr_close)*100, 1)

    if len(hist_df)>=31 and calc_pct(curr_close, float(hist_df.iloc[-31]['Close'])) > 100: triggers.append("【第二款】30日漲>100%")
    if len(hist_df)>=61 and calc_pct(curr_close, float(hist_df.iloc[-61]['Close'])) > 130: triggers.append("【第二款】60日漲>130%")
    if len(hist_df)>=91 and calc_pct(curr_close, float(hist_df.iloc[-91]['Close'])) > 160: triggers.append("【第二款】90日漲>160%")

    if len(hist_df) >= 61:
        avg_vol_60 = hist_df['Volume'].iloc[-61:-1].mean()
        if avg_vol_60 > 0:
            vol_ratio = curr_vol_shares / avg_vol_60
            res['limit_vol'] = int(avg_vol_60 * 5 / 1000)
            if turnover >= 0.1 and curr_vol_lots >= 500:
                if rise_6 > 25 and vol_ratio > 5: triggers.append(f"【第三款】漲{rise_6:.0f}%+量{vol_ratio:.1f}倍")

    if turnover > 10 and rise_6 > 25: triggers.append(f"【第四款】漲{rise_6:.0f}%+轉{turnover:.0f}%")

    if len(hist_df) >= 61:
        avg_vol_60 = hist_df['Volume'].iloc[-61:-1].mean()
        avg_vol_6 = hist_df['Volume'].iloc[-6:].mean()
        is_exclude = (turnover < 0.1) or (curr_vol_lots < 500) or (turnover_val_money < 30000000)
        if not is_exclude and avg_vol_60 > 0:
            r1 = avg_vol_6 / avg_vol_60
            r2 = curr_vol_shares / avg_vol_60
            if r1 > 5: triggers.append(f"【第九款】6日均量放大{r1:.1f}倍")
            if r2 > 5: triggers.append(f"【第九款】當日量放大{r2:.1f}倍")

    if turnover > 0 and turnover_val_money >= 500000000:
        acc_turn = (hist_df['Volume'].iloc[-6:].sum() / shares) * 100
        if acc_turn > 50 and turnover > 10: triggers.append(f"【第十款】累轉{acc_turn:.0f}%")

    if len(hist_df) >= 6:
        gap = hist_df.iloc[-6:]['High'].max() - hist_df.iloc[-6:]['Low'].min()
        threshold = 100 + (int((curr_close - 500)/500)+1)*25 if curr_close >= 500 else 100
        if gap >= threshold: triggers.append(f"【第十一款】6日價差{gap:.0f}元(>門檻{threshold})")

    pending_msg = ""
    # ✅ 當沖率判斷：未公布則只標記 pending，不算觸發
    if dt_today_pct is None or dt_avg6_pct is None:
        pending_msg = "(當沖率待公布)"
    else:
        dt_vol_est = curr_vol_shares * (dt_today_pct / 100.0)
        dt_vol_lots = dt_vol_est / 1000
        is_exclude = (turnover < 5) or (turnover_val_money < 500000000) or (dt_vol_lots < 5000)
        if not is_exclude:
            if dt_avg6_pct > 60 and dt_today_pct > 60:
                triggers.append(f"【第十三款】當沖{dt_today_pct}%(6日{dt_avg6_pct}%)")

    # ✅ 最後輸出訊息：有觸發就加上 pending 註記（若有）
    if triggers:
        res['is_triggered'] = True
        res['risk_level'] = '高'
        res['trigger_msg'] = "且".join(triggers) + (f" {pending_msg}" if pending_msg else "")
    else:
        # 沒觸發就不要因 pending 升級風險
        res['trigger_msg'] = pending_msg
        if est_days <= 1: res['risk_level'] = '高'
        elif est_days <= 2: res['risk_level'] = '中'
        elif est_days >= 3: res['risk_level'] = '低'
    
    return res

def check_jail_trigger_now(status_list, clause_list):
    status_list = list(status_list); clause_list = list(clause_list)
    if len(status_list) < 30:
        pad = 30 - len(status_list)
        status_list = [0]*pad + status_list
        clause_list = [""]*pad + clause_list

    c1_streak = 0
    for c in clause_list[-3:]:
        if 1 in parse_clause_ids_strict(c): c1_streak += 1

    v5 = 0; v10 = 0; v30 = 0
    total = len(status_list)
    for i in range(30):
        idx = total - 1 - i
        if idx < 0: break
        if status_list[idx] == 1:
            ids = parse_clause_ids_strict(clause_list[idx])
            if is_valid_accumulation_day(ids):
                if i < 5: v5 += 1
                if i < 10: v10 += 1
                v30 += 1

    reasons = []
    if c1_streak == 3: reasons.append("已觸發(連3第一款)")
    if v5 == 5: reasons.append("已觸發(連5)")
    if v10 >= 6: reasons.append(f"已觸發(10日{v10}次)")
    if v30 >= 12: reasons.append(f"已觸發(30日{v30}次)")
    return (len(reasons) > 0), " | ".join(reasons)

def simulate_days_to_jail_strict(status_list, clause_list, *, stock_id=None, target_date=None, jail_map=None, enable_safe_filter=True):
    if stock_id and target_date and jail_map and is_in_jail(stock_id, target_date, jail_map):
        return 0, "處置中"

    trigger_now, reason_now = check_jail_trigger_now(status_list, clause_list)
    if trigger_now:
        return 0, reason_now.replace("已觸發", "已達標，次一營業日處置")

    if enable_safe_filter:
        recent_valid_10 = 0
        check_len = min(len(status_list), 10)
        if check_len > 0:
            for b, c in zip(status_list[-check_len:], clause_list[-check_len:]):
                if b == 1 and is_valid_accumulation_day(parse_clause_ids_strict(c)):
                    recent_valid_10 += 1
        if recent_valid_10 == 0: return 99, "X"

    status_list = list(status_list); clause_list = list(clause_list)
    if len(status_list) < 30:
        pad = 30 - len(status_list)
        status_list = [0]*pad + status_list
        clause_list = [""]*pad + clause_list

    days = 0
    while days < 10:
        days += 1
        status_list.append(1); clause_list.append("第1款")
        
        c1_streak = 0
        for c in clause_list[-3:]:
            if 1 in parse_clause_ids_strict(c): c1_streak += 1

        v5 = 0; v10 = 0; v30 = 0
        total = len(status_list)
        for i in range(30):
            idx = total - 1 - i
            if idx < 0: break
            if status_list[idx] == 1:
                ids = parse_clause_ids_strict(clause_list[idx])
                if is_valid_accumulation_day(ids):
                    if i < 5: v5 += 1
                    if i < 10: v10 += 1
                    v30 += 1
        
        reasons = []
        if c1_streak == 3: reasons.append(f"再{days}天處置")
        if v5 == 5: reasons.append(f"再{days}天處置(連5)")
        if v10 >= 6: reasons.append(f"再{days}天處置(10日{v10}次)")
        if v30 >= 12: reasons.append(f"再{days}天處置(30日{v30}次)")

        if reasons:
            return days, " | ".join(reasons)
            
    return 99, ""

# ============================
# Main
# ============================
def main():
    sh, _ = connect_google_sheets()
    if not sh: return

    update_market_monitoring_log(sh)

    cal_dates = get_official_trading_calendar(240)
    
    # ✅ [修正] main() 修正 T-2 回朔 Bug
    target_trade_date_obj = cal_dates[-1]
    is_today_trade = (target_trade_date_obj == TARGET_DATE.date())

    # 只有「日曆已包含今天」且「現在 < 17:30」才退回 T-1
    if is_today_trade and (not IS_AFTER_SAFE) and len(cal_dates) >= 2:
        print(f"⏳ 現在時間 {TARGET_DATE.strftime('%H:%M')} 早於 {SAFE_CRAWL_TIME}，且日曆包含今日，切換為 T-1 模式。")
        target_trade_date_obj = cal_dates[-2]

    target_date_str = target_trade_date_obj.strftime("%Y-%m-%d")
    print(f"📅 最終鎖定運算日期: {target_date_str}")

    ws_log = get_or_create_ws(sh, "每日紀錄", headers=['日期','市場','代號','名稱','觸犯條款'])
    
    # ✅ 執行回補 (包含檢查狀態表缺失)
    backfill_daily_logs(sh, ws_log, cal_dates, target_trade_date_obj)

    print("📊 讀取歷史 Log...")
    log_data = ws_log.get_all_records()
    df_log = pd.DataFrame(log_data)
    if not df_log.empty:
        df_log['代號'] = df_log['代號'].astype(str).str.strip().str.replace("'", "")
        df_log['日期'] = df_log['日期'].astype(str).str.strip()

    clause_map = {}
    for _, r in df_log.iterrows():
        key = (str(r['代號']), str(r['日期']))
        clause_map[key] = merge_clause_text(clause_map.get(key,""), str(r['觸犯條款']))

    jail_map = get_jail_map(target_trade_date_obj - timedelta(days=90), target_trade_date_obj)
    exclude_map = build_exclude_map(cal_dates, jail_map)

    start_dt_str = cal_dates[-90].strftime("%Y-%m-%d")
    df_recent = df_log[df_log['日期'] >= start_dt_str]
    target_stocks = df_recent['代號'].unique()
    
    precise_db = load_precise_db_from_sheet(sh)
    rows_stats = []
    
    print(f"🔍 掃描 {len(target_stocks)} 檔股票...")
    for idx, code in enumerate(target_stocks):
        code = str(code).strip()
        name = df_log[df_log['代號']==code]['名稱'].iloc[-1] if not df_log[df_log['代號']==code].empty else "未知"
        
        db_info = precise_db.get(code, {})
        m_type = str(db_info.get('market', '上市')).upper()
        suffix = '.TWO' if any(k in m_type for k in ['上櫃', 'TWO', 'TPEX', 'OTC']) else '.TW'
        ticker_code = f"{code}{suffix}"

        stock_calendar = get_last_n_non_jail_trade_dates(code, cal_dates, jail_map, exclude_map, 30)
        
        bits = []; clauses = []
        for d in stock_calendar:
            c = clause_map.get((code, d.strftime("%Y-%m-%d")), "")
            if is_excluded(code, d, exclude_map):
                bits.append(0); clauses.append(c); continue
            if c: bits.append(1); clauses.append(c)
            else: bits.append(0); clauses.append("")

        est_days, reason = simulate_days_to_jail_strict(
            bits, clauses, 
            stock_id=code, 
            target_date=target_trade_date_obj, 
            jail_map=jail_map,
            enable_safe_filter=False
        )
        
        latest_ids = parse_clause_ids_strict(clauses[-1] if clauses else "")
        is_special_risk = is_special_risk_day(latest_ids)
        is_clause_13 = False
        for c in clauses:
            if 13 in parse_clause_ids_strict(c):
                is_clause_13 = True
                break

        est_days_int = 99
        est_days_display = "X"
        reason_display = ""

        if reason == "X":
            est_days_int = 99
            est_days_display = "X"
            if is_special_risk:
                reason_display = "籌碼異常(人工審核風險)"
                if is_clause_13: reason_display += " + 刑期可能延長"
        elif est_days == 0:
            est_days_int = 0
            est_days_display = "0"
            reason_display = reason
        else:
            est_days_int = int(est_days)
            est_days_display = str(est_days_int)
            reason_display = reason
            if is_special_risk:
                reason_display += " | ⚠️留意人工處置風險"
            if is_clause_13:
                reason_display += " (若進處置將關12天)"

        hist = fetch_history_data(ticker_code)
        if hist.empty:
            alt_s = '.TWO' if suffix=='.TW' else '.TW'
            hist = fetch_history_data(f"{code}{alt_s}")
            if not hist.empty: ticker_code = f"{code}{alt_s}"

        fund = fetch_stock_fundamental(code, ticker_code, precise_db)
        
        # ✅ 當沖率抓取判斷：只有過了 21:00 才抓，否則給 None
        dt_today, dt_avg6 = None, None
        if IS_AFTER_DAYTRADE:
            dt_today, dt_avg6 = get_daytrade_stats_finmind(code, target_date_str)

        risk = calculate_full_risk(code, hist, fund, est_days_int, dt_today, dt_avg6)

        # streak
        valid_bits = [1 if b==1 and is_valid_accumulation_day(parse_clause_ids_strict(c)) else 0 for b,c in zip(bits, clauses)]
        streak = 0
        for v in reversed(valid_bits):
            if v: streak+=1
            else: break
            
        status_30 = "".join(map(str, valid_bits)).zfill(30)
        
        def safe(v):
            if v is None: return ""
            try: 
                if np.isnan(v): return ""
            except: pass
            return str(v)

        row = [
            f"'{code}", name, safe(streak), safe(sum(valid_bits)), safe(sum(valid_bits[-10:])),
            stock_calendar[-1].strftime("%Y-%m-%d") if stock_calendar else "",
            f"'{status_30}", f"'{status_30[-10:]}", est_days_display, safe(reason_display),
            safe(risk['risk_level']), safe(risk['trigger_msg']),
            safe(risk['curr_price']), safe(risk['limit_price']), safe(risk['gap_pct']),
            safe(risk['curr_vol']), safe(risk['limit_vol']), safe(risk['turnover_val']),
            safe(risk['turnover_rate']), safe(risk['pe']), safe(risk['pb']), safe(risk['day_trade_pct'])
        ]
        rows_stats.append(row)
        if (idx+1)%10==0: time.sleep(1)

    if rows_stats:
        print("💾 更新統計表...")
        ws_stats = get_or_create_ws(sh, "近30日熱門統計", headers=STATS_HEADERS)
        ws_stats.clear()
        ws_stats.append_row(STATS_HEADERS, value_input_option='USER_ENTERED')
        ws_stats.append_rows(rows_stats, value_input_option='USER_ENTERED')
        print("✅ 完成")

if __name__ == "__main__":
    main()
