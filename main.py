# -*- coding: utf-8 -*-
"""
V116.28 台股注意股系統 (修正『有公告紀錄就強制納入』累積邏輯)

本版相對於 V116.27 的修正重點：
[修正] 「每日紀錄出現 3 次但近30日熱門統計只記 2 次」的 bug：
  根本原因：
    - get_last_n_non_jail_trade_dates() 內部會用 cutoff_date 截斷歷史交易日，
      碰到「過去處置結束日」之後就 break，導致那一天前的日子全被丟棄。
    - bits 構造迴圈又會用 cutoff / exclude_map 再切一次。
    - V116.27 雖加了 force_include_target_attention，但只 force「最終運算日」那一天，
      歷史日子若有公告卻被 jail_map 區段或 cutoff 蓋到的，仍被切掉。

  關鍵原則：
    若 clause_map[(code, d)] 該日有公告 → 表示該日股票不在處置中
    (處置中的股票不會再被公告注意股)
    → 該日應「無條件納入累積」，不該被 jail_map / cutoff / exclude_map 切掉

  修正內容：
    1. 新增 get_last_n_trade_dates_with_attention()：
       重新蒐集 stock_calendar，凡是 clause_map 該日有值就強制納入，
       不再受 cutoff_date 截斷影響。
    2. main() 改用新函式取代舊的 get_last_n_non_jail_trade_dates()。
    3. bits 構造迴圈內，force_include 條件擴展到「該日有公告就 force」，
       不再只 force 最終運算日。
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
import traceback
import nest_asyncio
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, time as dt_time, date
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
from workalendar.asia import Taiwan

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

nest_asyncio.apply()

# ==========================================
# 1. 設定靜音模式與常數
# ==========================================
logger = logging.getLogger('yfinance')
logger.setLevel(logging.CRITICAL)
logger.disabled = True

UNIT_LOT = 1000

STATS_HEADERS = [
    '代號', '名稱', '連續天數', '近30日注意次數', '近10日注意次數', '最近一次日期',
    '30日狀態碼', '10日狀態碼', '最快處置天數', '處置觸發原因', '風險等級', '觸發條件',
    '目前價', '警戒價', '差幅(%)', '目前量', '警戒量', '成交值(億)',
    '週轉率(%)', 'PE', 'PB', '當沖佔比(%)'
]

# ==========================================
# 處置股技術追蹤表
# ==========================================
TECH_TRACK_SHEET_NAME = "處置股技術追蹤"
TECH_TRACK_HEADERS = [
    '計算日期', '代號', '名稱', '狀態', '訊號狀態', '符合條件', '訊號說明',
    '處置前10日漲跌幅(%)', '距離MA20(%)', '目前價', 'MA20',
    '回測後轉強', '曾回測MA20±5%', '回測MA20日期', '回測MA20收盤價',
    '處置前10日開盤價', '處置前一日收盤價',
    '市場', '處置期間', '處置開始日', '處置結束日', '更新時間',
]

TECH_PRE_10D_RISE_THRESHOLD = 20.0
TECH_MA20_GAP_THRESHOLD = 6.2
TECH_BREAKOUT_MA20_GAP_THRESHOLD = 10.0

TECH_TRACK_TRUE_BG = {"red": 1.0, "green": 0.93, "blue": 0.82}
TECH_TRACK_BREAKOUT_BG = {"red": 0.86, "green": 0.93, "blue": 1.0}
TECH_TRACK_FALSE_BG = {"red": 1.0, "green": 0.90, "blue": 0.90}

TECH_TRACK_COL_COUNT = len(TECH_TRACK_HEADERS)
TECH_TRACK_LAST_COL = "V"

# ==========================================
# 設定區
# ==========================================
SHEET_NAME = "台股注意股資料庫_V33"
PARAM_SHEET_NAME = "個股參數"
TW_TZ = ZoneInfo("Asia/Taipei")
TARGET_DATE = datetime.now(TW_TZ)

SAFE_CRAWL_TIME = dt_time(17, 30)
DAYTRADE_PUBLISH_TIME = dt_time(21, 0)
SAFE_MARKET_OPEN_CHECK = dt_time(16, 30)

IS_NIGHT_RUN = TARGET_DATE.hour >= 20
IS_AFTER_SAFE = TARGET_DATE.time() >= SAFE_CRAWL_TIME
IS_AFTER_DAYTRADE = TARGET_DATE.time() >= DAYTRADE_PUBLISH_TIME

MAX_BACKFILL_TRADING_DAYS = 40
VERIFY_RECENT_DAYS = 2

# ==========================================
# FinMind 金鑰設定
# ==========================================
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"

token1 = os.getenv('FinMind_1')
token2 = os.getenv('FinMind_2')
FINMIND_TOKENS = [t for t in [token1, token2] if t]

CURRENT_TOKEN_INDEX = 0
_FINMIND_CACHE = {}

print(f"啟動 V116.28 台股注意股系統 (修正『有公告就強制納入』累積邏輯)")
print(f"系統時間 (Taiwan): {TARGET_DATE.strftime('%Y-%m-%d %H:%M:%S')}")

try: twstock.__update_codes()
except: pass

# ============================
# 工具函式
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
    s = s.translate(str.maketrans("１2３４５６７８９０", "1234567890"))
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
        print(f"工作表 '{title}' 不存在，正在建立...")
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
    print("檢查並更新「大盤數據監控」...")
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
    except Exception as e: print(f" 大盤更新失敗: {e}")

# ============================
# 處置資料相關函式 (Jail)
# ============================
def parse_roc_date(roc_date_str):
    try:
        roc_date_str = str(roc_date_str).strip()
        parts = re.split(r"[/-]", roc_date_str)
        if len(parts) == 3:
            y = int(parts[0]) + 1911
            m = int(parts[1])
            d = int(parts[2])
            return date(y, m, d)
    except:
        return None
    return None

def parse_jail_period(period_str):
    if not period_str:
        return None, None

    s = str(period_str).strip()
    dates = []
    if "～" in s:
        dates = s.split("～")
    elif "~" in s:
        dates = s.split("~")
    elif "-" in s and "/" in s and s.count("-") == 1:
        dates = s.split("-")

    if len(dates) >= 2:
        sd = parse_roc_date(dates[0].strip())
        ed = parse_roc_date(dates[1].strip())
        if sd and ed:
            return sd, ed
    return None, None

def get_jail_map_from_sheet(sh):
    print("從 Google Sheet 讀取處置名單快取 (處置股90日明細)...")
    jail_map = {}
    try:
        ws = sh.worksheet("處置股90日明細")
        rows = ws.get_all_records()
        for r in rows:
            code = str(r.get('代號', '')).strip()
            if not code:
                code = str(r.get('Code', '')).strip()

            if not code: continue

            period = str(r.get('處置期間', '')).strip()
            if not period:
                period = str(r.get('Period', '')).strip()

            sd, ed = parse_jail_period(period)
            if sd and ed:
                jail_map.setdefault(code, []).append((sd, ed))
        print(f"快取讀取完成，共 {len(jail_map)} 檔處置股資料。")
    except Exception as e:
        print(f"讀取處置快取失敗 (可能是初次執行或工作表不存在): {e}")
    return jail_map

def is_in_jail(stock_id, target_date, jail_map):
    if not jail_map or stock_id not in jail_map:
        return False
    for s, e in jail_map[stock_id]:
        if s <= target_date <= e:
            return True
    return False

def prev_trade_date(d, cal_dates):
    try:
        idx = cal_dates.index(d)
        return cal_dates[idx - 1] if idx > 0 else None
    except:
        for i in range(len(cal_dates) - 1, -1, -1):
            if cal_dates[i] < d:
                return cal_dates[i]
        return None

def build_exclude_map(cal_dates, jail_map):
    exclude_map = {}
    if not jail_map:
        return exclude_map

    for code, periods in jail_map.items():
        s = set()
        for start, end in periods:
            for d in cal_dates:
                if start <= d <= end:
                    s.add(d)
        exclude_map[code] = s
    return exclude_map

def is_excluded(code, d, exclude_map):
    return bool(exclude_map) and (code in exclude_map) and (d in exclude_map[code])

def get_last_n_non_jail_trade_dates(stock_id, cal_dates, jail_map, exclude_map=None, n=30, target_date=None):
    cutoff_date = date(1900, 1, 1)

    if jail_map and stock_id in jail_map:
        past_jail_ends = [e for (s, e) in jail_map[stock_id] if e < target_date]
        if past_jail_ends:
            cutoff_date = max(cutoff_date, max(past_jail_ends))

        recent_jail_starts = [s for (s, e) in jail_map[stock_id] if s <= target_date]
        if recent_jail_starts:
            latest_start = max(recent_jail_starts)
            potential_cutoff = latest_start - timedelta(days=1)
            if potential_cutoff > cutoff_date:
                cutoff_date = potential_cutoff

    picked = []
    for d in reversed(cal_dates):
        if d <= cutoff_date:
            break

        if exclude_map and is_excluded(stock_id, d, exclude_map):
            continue

        if jail_map and is_in_jail(stock_id, d, jail_map):
            continue

        picked.append(d)
        if len(picked) >= n:
            break

    return list(reversed(picked))


# ===========================================================================
# [V116.28 核心新增函式]
# 取代 get_last_n_non_jail_trade_dates 用於 main 統計流程
# 邏輯：「該日有公告紀錄 → 證明該日股票不在處置中 → 強制納入累積」
#
# 為何重要：
#   舊版會用 cutoff_date / exclude_map 截斷歷史，導致明明在「每日紀錄」
#   裡有公告的日子，因為 jail_map 區段或截斷邏輯被誤切，造成累積次數少算。
#
#   例如某股 4/29、4/30、5/4 都有第1款，但 jail_map 內某段
#   結束於 4/29 → cutoff = 4/29 → 4/29 被 break 切掉 → 變 2 次。
#   然而 4/29 有公告本身就證明該日不在處置，邏輯矛盾。
#
# 新策略：
#   1. 蒐集 cal_dates 內所有 d <= target_date 的日子
#   2. 依規則切「真正應排除」的日子：
#      - 該日落在 jail_map 內任一處置區間 (is_in_jail) 且該日沒公告 → 排除
#      - 該日有公告 (clause_map_of_code 有值) → 強制納入，不論 jail_map
#   3. 取最後 n 天 (含目標日)
# ===========================================================================
def get_last_n_trade_dates_with_attention(
    stock_id,
    cal_dates,
    jail_map,
    clause_map_of_code,
    n=30,
    target_date=None,
):
    """蒐集計算累積次數用的歷史交易日清單。

    Parameters
    ----------
    stock_id : str
        股票代號
    cal_dates : list[date]
        交易日曆 (已過濾到 target_date 含以前)
    jail_map : dict
        處置區間對照
    clause_map_of_code : dict[date_str -> clause_text]
        該股的每日公告紀錄；key 是 'YYYY-MM-DD' 字串
    n : int
        最多取多少天 (預設 30)
    target_date : date
        最終運算日 (含)
    """
    if not target_date:
        target_date = TARGET_DATE.date()

    # 只看 target_date 含以前的日子
    candidate_dates = [d for d in cal_dates if d <= target_date]

    picked = []
    # 反向走，從最近的日子優先
    for d in reversed(candidate_dates):
        d_str = d.strftime("%Y-%m-%d")
        has_attention = bool(clause_map_of_code.get(d_str, ""))

        if has_attention:
            # 有公告 → 該日股票必然不在處置中（處置中不會公告）→ 強制納入
            picked.append(d)
        else:
            # 沒公告 → 走原本的處置區間判斷
            if jail_map and is_in_jail(stock_id, d, jail_map):
                # 處置中那天，視為「凍結期」，跳過 (但不 break，因為前面可能還有自由日)
                continue
            picked.append(d)

        if len(picked) >= n:
            break

    return list(reversed(picked))


def get_last_jail_end(stock_id, target_date, jail_map):
    last_end = None
    if not jail_map or stock_id not in jail_map: return None
    for s, e in jail_map[stock_id]:
        if e < target_date:
            last_end = e if (last_end is None or e > last_end) else last_end
    return last_end

# ============================
# 每日公告爬蟲區 (TWSE / TPEx)
# ============================
def fetch_twse_attention_rows(date_obj, date_str):
    date_str_nodash = date_obj.strftime("%Y%m%d")
    rows = []
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(
            "https://www.twse.com.tw/rwd/zh/announcement/notice",
            params={"startDate": date_str_nodash, "endDate": date_str_nodash, "response": "json"},
            headers=headers,
            timeout=10,
        )
        if r.status_code != 200:
            print(f"TWSE 抓取失敗：HTTP {r.status_code}，URL={r.url}")
            print(f"   回應內容前300字：{r.text[:300]}")
            return None

        try:
            d = r.json()
        except Exception as e:
            print(f"TWSE JSON 解析失敗：{type(e).__name__}: {e}")
            print(f"   URL={r.url}")
            print(f"   回應內容前300字：{r.text[:300]}")
            return None

        for i in d.get("data", []) or []:
            code = str(i[1]).strip()
            name = str(i[2]).strip()
            if len(code) == 4 and code.isdigit():
                raw = " ".join([str(x) for x in i])
                ids = parse_clause_ids_strict(raw)
                c_str = "、".join([f"第{k}款" for k in sorted(ids)]) or raw
                rows.append({"日期": date_str, "市場": "TWSE", "代號": code, "名稱": name, "觸犯條款": c_str})
    except Exception as e:
        print(f"TWSE 抓取例外：{type(e).__name__}: {e}")
        print(f"   日期={date_str}，查詢參數 startDate/endDate={date_str_nodash}")
        return None
    return rows

def fetch_tpex_attention_rows(date_obj, date_str):
    roc_date = f"{date_obj.year - 1911}/{date_obj.month:02d}/{date_obj.day:02d}"
    url = "https://www.tpex.org.tw/www/zh-tw/bulletin/attention"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.tpex.org.tw/",
        "Origin": "https://www.tpex.org.tw",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }
    payload = {"date": roc_date, "response": "json"}

    s = requests.Session()

    try:
        s.get("https://www.tpex.org.tw/", headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    except Exception as e:
        print(f"TPEx 初始化 Cookie 失敗：{type(e).__name__}: {e}")

    last_error = None
    for attempt in range(1, 4):
        try:
            r = s.post(url, data=payload, headers=headers, timeout=12)
            if r.status_code != 200:
                last_error = f"HTTP {r.status_code}"
                print(f"TPEx 第 {attempt} 次抓取失敗：HTTP {r.status_code}，URL={r.url}")
                print(f"   payload={payload}")
                print(f"   回應內容前300字：{r.text[:300]}")
                time.sleep(0.8)
                continue

            try:
                res = r.json()
            except Exception as e:
                last_error = f"JSON 解析失敗 {type(e).__name__}: {e}"
                print(f"TPEx 第 {attempt} 次 JSON 解析失敗：{type(e).__name__}: {e}")
                print(f"   URL={r.url}")
                print(f"   payload={payload}")
                print(f"   回應內容前300字：{r.text[:300]}")
                time.sleep(0.8)
                continue

            target = []
            if "tables" in res:
                for t in res["tables"]:
                    target.extend(t.get("data", []) or [])
            else:
                target = res.get("data", []) or []

            rows = []
            for i in target:
                if len(i) <= 5:
                    continue
                row_date = str(i[5]).strip()
                if row_date not in (roc_date, date_str):
                    continue

                code = str(i[1]).strip()
                name = str(i[2]).strip()
                if not (code.isdigit() and len(code) == 4):
                    continue

                raw = " ".join([str(x) for x in i])
                ids = parse_clause_ids_strict(raw)
                c_str = "、".join([f"第{k}款" for k in sorted(ids)]) if ids else raw

                rows.append({"日期": date_str, "市場": "TPEx", "代號": code, "名稱": name, "觸犯條款": c_str})

            return rows
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            print(f"TPEx 第 {attempt} 次抓取例外：{type(e).__name__}: {e}")
            print(f"   日期={date_str}，ROC日期={roc_date}，payload={payload}")
            time.sleep(0.8)

    print(f"TPEx 三次重試皆失敗，最後錯誤：{last_error}")
    return None

def get_daily_data(date_obj):
    date_str = date_obj.strftime("%Y-%m-%d")
    print(f"爬取公告 {date_str}...")

    twse_rows = fetch_twse_attention_rows(date_obj, date_str)
    tpex_rows = fetch_tpex_attention_rows(date_obj, date_str)

    if twse_rows is None or tpex_rows is None:
        failed_sources = []
        if twse_rows is None:
            failed_sources.append("上市 TWSE")
        if tpex_rows is None:
            failed_sources.append("上櫃 TPEx")

        print(f"抓取失敗：{', '.join(failed_sources)} 回傳 None，本輪不寫入狀態")
        return None

    rows = []
    rows.extend(twse_rows)
    rows.extend(tpex_rows)

    if rows:
        print(f"抓到 {len(rows)} 檔")
    else:
        print("無資料")
    return rows

def backfill_daily_logs(sh, ws_log, cal_dates, target_trade_date_obj):
    now_str = TARGET_DATE.strftime("%Y-%m-%d %H:%M:%S")
    existing_keys, date_counts = load_log_index(ws_log)
    ws_status = get_or_create_ws(sh, "爬取狀態", headers=["日期", "抓到檔數", "最後更新時間"], cols=5)
    key_to_row, status_cnt = load_status_index(ws_status)

    key_to_row, status_cnt = load_status_index(ws_status)
    window_dates = cal_dates[-MAX_BACKFILL_TRADING_DAYS:] if len(cal_dates) > MAX_BACKFILL_TRADING_DAYS else cal_dates[:]
    recent_dates = cal_dates[-VERIFY_RECENT_DAYS:] if len(cal_dates) >= VERIFY_RECENT_DAYS else cal_dates[:]
    dates_to_check = sorted(set(window_dates + recent_dates))

    rows_to_append = []
    status_updates = []

    print(f"回補檢查：共 {len(dates_to_check)} 個交易日（含最近 {VERIFY_RECENT_DAYS} 日強制驗證）")

    for d in dates_to_check:
        d_str = d.strftime("%Y-%m-%d")

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

        if data is None:
            print(f"{d_str} 抓取失敗(None)，跳過不更新狀態")

            if d in recent_dates or d == target_trade_date_obj:
                raise RuntimeError(
                    f"關鍵交易日 {d_str} 公告抓取失敗，已停止後續統計更新，避免錯誤資料被推播。"
                )

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
        print(f"回補寫入「每日紀錄」：{len(rows_to_append)} 筆")
        ws_log.append_rows(rows_to_append, value_input_option="USER_ENTERED")
    else:
        print("每日紀錄無需回補寫入")

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
    print("下載日曆...")
    df = finmind_get("TaiwanStockTradingDate", start_date=start, end_date=end)
    dates = []

    if not df.empty:
        df['date'] = pd.to_datetime(df['date']).dt.date
        dates = sorted(df['date'].tolist())
    else:
        cal = Taiwan()
        curr = TARGET_DATE.date()
        while len(dates) < days:
            if cal.is_working_day(curr):
                dates.append(curr)
            curr -= timedelta(days=1)
        dates = sorted(dates)

    today_date = TARGET_DATE.date()
    is_late_enough = TARGET_DATE.time() > SAFE_MARKET_OPEN_CHECK

    cal = Taiwan()
    is_today_work = cal.is_working_day(today_date)

    if dates and today_date > dates[-1] and is_today_work:
        if is_late_enough:
            print(f"日曆缺漏今日 ({today_date})，驗證開市中...")
            if is_market_open_by_finmind(today_date.strftime("%Y-%m-%d")):
                print(f"驗證成功 (2330有價)，補入今日。")
                dates.append(today_date)
            else:
                print(f"驗證失敗 (2330無價)，判斷為休市或資料未更新，不補入。")
        else:
            print(f"時間尚早，暫不強制補入今日日曆。")

    return dates[-days:]


def get_trading_calendar_between(start_date, end_date):
    """取得指定區間內的台股交易日，供即將出關判斷使用。"""
    start_date = start_date if isinstance(start_date, date) else pd.to_datetime(start_date).date()
    end_date = end_date if isinstance(end_date, date) else pd.to_datetime(end_date).date()
    if end_date < start_date:
        return []

    dates = []
    try:
        df = finmind_get(
            "TaiwanStockTradingDate",
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )
        if not df.empty and 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.date
            dates = sorted(set(df['date'].tolist()))
    except Exception as e:
        print(f"FinMind 交易日曆區間下載失敗，改用 Taiwan 行事曆備援：{e}")

    cal = Taiwan()
    if not dates:
        curr = start_date
    else:
        curr = max(dates) + timedelta(days=1)

    while curr <= end_date:
        if cal.is_working_day(curr):
            dates.append(curr)
        curr += timedelta(days=1)

    return sorted(set(dates))


def next_or_same_trade_date(d, cal_dates):
    if not d or not cal_dates:
        return None
    for td in cal_dates:
        if td >= d:
            return td
    return None


def trading_days_left_for_release(today_date, release_date, cal_dates):
    if not release_date or not cal_dates:
        return None

    base_trade_date = next_or_same_trade_date(today_date, cal_dates)
    release_trade_date = next_or_same_trade_date(release_date, cal_dates)

    if not base_trade_date or not release_trade_date:
        return None

    if release_trade_date < base_trade_date:
        return -1

    return sum(1 for td in cal_dates if base_trade_date < td <= release_trade_date)

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

def _safe_round(v, ndigits=2):
    try:
        if v is None or pd.isna(v):
            return ""
        return round(float(v), ndigits)
    except:
        return ""


def _fetch_technical_history(code, market, start_date, end_date):
    """抓取技術追蹤用股價資料；改用 FinMind 官方日線資料計算 MA20。"""
    code = str(code).replace("'", "").strip()
    source_label = f"FinMind:{code}"

    try:
        df = finmind_get(
            "TaiwanStockPrice",
            data_id=code,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )

        if df.empty:
            print(f"技術追蹤 FinMind 無股價資料 ({source_label})")
            return pd.DataFrame(), source_label

        required_cols = ['date', 'open', 'max', 'min', 'close']
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            print(f"技術追蹤 FinMind 欄位不足 ({source_label})：缺少 {missing_cols}")
            return pd.DataFrame(), source_label

        df = df.copy()
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        df = df.sort_values('date').drop_duplicates(subset=['date'], keep='last')
        df = df.set_index('date')

        df = df.rename(columns={
            'open': 'Open',
            'max': 'High',
            'min': 'Low',
            'close': 'Close',
            'Trading_Volume': 'Volume',
            'Trading_money': 'Trading_money',
        })

        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        if 'Volume' in df.columns:
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
        else:
            df['Volume'] = 0.0

        df = df.dropna(subset=['Open', 'Close', 'Low']).copy()
        if df.empty:
            print(f"技術追蹤 FinMind 股價資料清理後為空 ({source_label})")
            return pd.DataFrame(), source_label

        if getattr(df.index, 'tz', None) is not None:
            df.index = df.index.tz_localize(None)

        return df, source_label

    except Exception as e:
        print(f"技術追蹤 FinMind 股價抓取失敗 ({source_label}): {e}")
        return pd.DataFrame(), source_label


# ===========================================================================
# 計算每檔股票的技術追蹤資料
# ===========================================================================
def calc_jail_technical_track_row(market, code, name, period, status_label):
    now_str = TARGET_DATE.strftime("%Y-%m-%d %H:%M:%S")
    calc_date_str = TARGET_DATE.strftime("%Y-%m-%d")
    code = str(code).replace("'", "").strip()
    name = str(name).strip()
    period = str(period).strip()

    sd, ed = parse_jail_period(period)
    start_str = sd.strftime("%Y-%m-%d") if sd else ""
    end_str = ed.strftime("%Y-%m-%d") if ed else ""

    base_row = [
        calc_date_str, f"'{code}", name, status_label, "資料不足",
        "FALSE", "", "", "", "", "",
        "FALSE", "FALSE", "", "",
        "", "",
        market, period, start_str, end_str,
        now_str,
    ]

    if not sd or not ed:
        base_row[6] = "處置期間解析失敗：無法取得處置開始/結束日"
        return base_row

    fetch_start = sd - timedelta(days=120)
    fetch_end = TARGET_DATE.date() + timedelta(days=2)
    df, ticker_used = _fetch_technical_history(code, market, fetch_start, fetch_end)

    if df.empty or 'Open' not in df.columns or 'Close' not in df.columns or 'Low' not in df.columns:
        base_row[6] = f"無股價資料：{ticker_used} 無法取得 Open/Close/Low 欄位"
        return base_row

    df = df.dropna(subset=['Open', 'Close', 'Low']).copy()
    if df.empty:
        base_row[6] = f"股價資料為空：{ticker_used} 清除空值後無可用資料"
        return base_row

    pre_df = df[df.index.date < sd]
    if len(pre_df) < 10:
        base_row[6] = f"資料不足：處置前交易日只有 {len(pre_df)} 日，需至少 10 日"
        return base_row

    if len(df) < 20:
        base_row[6] = f"資料不足：股價資料只有 {len(df)} 日，需至少 20 日才能計算 MA20"
        return base_row

    df['MA20'] = df['Close'].rolling(20).mean()
    df['MA20_GAP_PCT'] = ((df['Close'] - df['MA20']) / df['MA20']) * 100
    df['LOW_MA20_GAP_PCT'] = ((df['Low'] - df['MA20']) / df['Low']) * 100

    pre_10_open = float(pre_df.tail(10)['Open'].iloc[0])
    pre_last_close = float(pre_df['Close'].iloc[-1])
    pre_10d_pct = ((pre_last_close - pre_10_open) / pre_10_open) * 100 if pre_10_open > 0 else 0.0

    current_price = float(df['Close'].iloc[-1])
    current_low = float(df['Low'].iloc[-1])
    ma20 = float(df['MA20'].iloc[-1]) if not pd.isna(df['MA20'].iloc[-1]) else 0.0
    ma20_gap_pct = ((current_price - ma20) / ma20) * 100 if ma20 > 0 else 0.0
    current_low_ma20_gap_pct = ((current_low - ma20) / current_low) * 100 if current_low > 0 else 0.0

    pre_rise_ok = pre_10d_pct >= TECH_PRE_10D_RISE_THRESHOLD
    current_retest_ok = pre_rise_ok and (abs(current_low_ma20_gap_pct) <= TECH_MA20_GAP_THRESHOLD)

    jail_df = df[(df.index.date >= sd) & (df.index.date <= ed)].copy()
    jail_df = jail_df.dropna(subset=['MA20', 'MA20_GAP_PCT', 'LOW_MA20_GAP_PCT'])
    retest_df = jail_df[jail_df['LOW_MA20_GAP_PCT'].abs() <= TECH_MA20_GAP_THRESHOLD]
    has_retested_ma20 = not retest_df.empty

    retest_date_str = ""
    retest_close = ""
    if has_retested_ma20:
        latest_retest = retest_df.iloc[-1]
        retest_date_str = retest_df.index[-1].strftime("%Y-%m-%d")
        retest_close = _safe_round(float(latest_retest['Close']), 2)

    breakout_ok = (
        pre_rise_ok
        and has_retested_ma20
        and ma20_gap_pct >= TECH_BREAKOUT_MA20_GAP_THRESHOLD
    )

    overall_match = current_retest_ok or breakout_ok

    if breakout_ok:
        signal_status = "回測後轉強"
        reason_text = (
            f"已達成「回測後轉強」｜"
            f"處置前10日漲幅 {pre_10d_pct:+.2f}% (>={TECH_PRE_10D_RISE_THRESHOLD:.0f}%)；"
            f"於 {retest_date_str} 處置期間內盤中低點回測 MA20；"
            f"目前收盤距離 MA20 {ma20_gap_pct:+.2f}% (>=+{TECH_BREAKOUT_MA20_GAP_THRESHOLD:.0f}%)"
        )
    elif current_retest_ok:
        signal_status = "目前回測月線"
        reason_text = (
            f"已達成「目前回測月線」｜"
            f"處置前10日漲幅 {pre_10d_pct:+.2f}% (>={TECH_PRE_10D_RISE_THRESHOLD:.0f}%)；"
            f"今日低點距離 MA20 {current_low_ma20_gap_pct:+.2f}% (在 +/-{TECH_MA20_GAP_THRESHOLD:.0f}% 內)"
        )
    else:
        signal_status = "未符合"
        if not pre_rise_ok:
            reason_text = (
                f"處置前10日漲幅 {pre_10d_pct:+.2f}% < {TECH_PRE_10D_RISE_THRESHOLD:.0f}% 門檻 → 不列入追蹤"
            )
        else:
            parts = [
                f"處置前10日漲幅 {pre_10d_pct:+.2f}% 已達標 (>={TECH_PRE_10D_RISE_THRESHOLD:.0f}%)，但兩訊號皆未成立："
            ]
            if abs(current_low_ma20_gap_pct) <= TECH_MA20_GAP_THRESHOLD:
                parts.append(f"O「目前回測月線」成立 (今日低點距離 MA20 {current_low_ma20_gap_pct:+.2f}%)")
            else:
                parts.append(
                    f"X「目前回測月線」不成立 (今日低點距離 MA20 {current_low_ma20_gap_pct:+.2f}%，"
                    f"超出 +/-{TECH_MA20_GAP_THRESHOLD:.0f}% 範圍)"
                )
            if not has_retested_ma20:
                parts.append(
                    f"X「回測後轉強」不成立 (處置期間內尚未出現盤中低點回測 MA20，"
                    f"今日低點距離 {current_low_ma20_gap_pct:+.2f}%)"
                )
            elif ma20_gap_pct < TECH_BREAKOUT_MA20_GAP_THRESHOLD:
                parts.append(
                    f"X「回測後轉強」不成立 ({retest_date_str} 已於處置期間內由盤中低點回測 MA20，"
                    f"但目前收盤距離 MA20 僅 {ma20_gap_pct:+.2f}%，未達 +{TECH_BREAKOUT_MA20_GAP_THRESHOLD:.0f}%)"
                )
            else:
                parts.append(f"O「回測後轉強」成立")
            reason_text = "；".join(parts)

    base_row[4]  = signal_status
    base_row[5]  = "TRUE" if overall_match else "FALSE"
    base_row[6]  = reason_text
    base_row[7]  = _safe_round(pre_10d_pct, 2)
    base_row[8]  = _safe_round(ma20_gap_pct, 2)
    base_row[9]  = _safe_round(current_price, 2)
    base_row[10] = _safe_round(ma20, 2)
    base_row[11] = "TRUE" if breakout_ok else "FALSE"
    base_row[12] = "TRUE" if has_retested_ma20 else "FALSE"
    base_row[13] = retest_date_str
    base_row[14] = retest_close
    base_row[15] = _safe_round(pre_10_open, 2)
    base_row[16] = _safe_round(pre_last_close, 2)
    return base_row


def build_jail_technical_tracking_rows(stock_latest_end, releasing_codes_map, today_date):
    rows = []
    sorted_stocks = sorted(stock_latest_end.items(), key=lambda x: (x[1]['date'], x[0]))

    for code, data in sorted_stocks:
        row_list = data.get('row_list', [])
        if len(row_list) < 4:
            continue

        market = str(row_list[0]).strip()
        code = str(row_list[1]).replace("'", "").strip()
        name = str(row_list[2]).strip()
        period = str(row_list[3]).strip()
        sd_date, ed_date = parse_jail_period(period)

        if not sd_date or not ed_date:
            continue

        if code in releasing_codes_map:
            status_label = "即將出關"
        elif sd_date <= today_date <= ed_date:
            status_label = "正在處置"
        else:
            continue

        rows.append(calc_jail_technical_track_row(market, code, name, period, status_label))
        if len(rows) % 10 == 0:
            time.sleep(1)

    return rows


def _tech_track_bg_color(is_match, is_breakout):
    if is_breakout:
        return TECH_TRACK_BREAKOUT_BG
    if is_match:
        return TECH_TRACK_TRUE_BG
    return TECH_TRACK_FALSE_BG


def apply_technical_tracking_sheet_formats(sh, ws, row_style_targets):
    try:
        sheet_id = ws.id
        format_requests = []

        column_formats = [
            (1,  2,  {"type": "TEXT"}),
            (5,  6,  {"type": "TEXT"}),
            (11, 13, {"type": "TEXT"}),
            (0,  1,  {"type": "DATE", "pattern": "yyyy-mm-dd"}),
            (13, 14, {"type": "DATE", "pattern": "yyyy-mm-dd"}),
            (19, 21, {"type": "DATE", "pattern": "yyyy-mm-dd"}),
            (21, 22, {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}),
            (7,  11, {"type": "NUMBER", "pattern": "0.00"}),
            (14, 15, {"type": "NUMBER", "pattern": "0.00"}),
            (15, 17, {"type": "NUMBER", "pattern": "0.00"}),
        ]

        for start_col, end_col, num_format in column_formats:
            format_requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": start_col,
                        "endColumnIndex": end_col,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": num_format
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            })

        if row_style_targets:
            for row_num, is_match, is_breakout in row_style_targets:
                format_requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_num - 1,
                            "endRowIndex": row_num,
                            "startColumnIndex": 0,
                            "endColumnIndex": TECH_TRACK_COL_COUNT,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": _tech_track_bg_color(is_match, is_breakout)
                            }
                        },
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })

        if format_requests:
            sh.batch_update({"requests": format_requests})
            print(f"  已套用 {len(format_requests)} 個格式設定 (含 {len(row_style_targets)} 列底色)")

    except Exception as e:
        print(f"技術追蹤工作表格式套用失敗：{type(e).__name__}: {e}")
        traceback.print_exc()


def upsert_jail_technical_tracking_sheet(sh, rows):
    ws = get_or_create_ws(sh, TECH_TRACK_SHEET_NAME, headers=TECH_TRACK_HEADERS, cols=TECH_TRACK_COL_COUNT)
    last_col = TECH_TRACK_LAST_COL

    def _is_retryable_sheet_error(e):
        msg = str(e)
        return any(code in msg for code in ['429', '500', '502', '503', '504'])

    def _run_sheet_write(action_desc, fn, max_retries=5):
        for attempt in range(max_retries):
            try:
                return fn()
            except gspread.exceptions.APIError as e:
                if _is_retryable_sheet_error(e) and attempt < max_retries - 1:
                    wait = (2 ** attempt) + random.uniform(0.5, 1.5)
                    print(f"{action_desc} 遇到 Google API 暫時性限制，{wait:.1f} 秒後重試 ({attempt + 1}/{max_retries})")
                    time.sleep(wait)
                    continue
                raise

    def _sheet_title_for_range(title):
        return str(title).replace("'", "''")

    def _batch_update_row_values(value_ranges, batch_size=80):
        if not value_ranges:
            return

        sheet_title = _sheet_title_for_range(ws.title)
        for start_idx in range(0, len(value_ranges), batch_size):
            chunk = value_ranges[start_idx:start_idx + batch_size]
            data = [
                {
                    "range": f"'{sheet_title}'!{item['range']}",
                    "values": item['values'],
                }
                for item in chunk
            ]
            body = {
                "valueInputOption": "USER_ENTERED",
                "data": data,
            }

            def _do_batch_update():
                if hasattr(sh, 'values_batch_update'):
                    return sh.values_batch_update(body)
                worksheet_data = [{"range": item['range'], "values": item['values']} for item in chunk]
                return ws.batch_update(worksheet_data, value_input_option='USER_ENTERED')

            _run_sheet_write(
                f"{TECH_TRACK_SHEET_NAME} 批次更新第 {start_idx + 1}-{start_idx + len(chunk)} 筆",
                _do_batch_update
            )

            if start_idx + batch_size < len(value_ranges):
                time.sleep(0.8)

    if not rows:
        print(f"{TECH_TRACK_SHEET_NAME} 無符合正在處置或即將出關的資料需要寫入。")
        apply_technical_tracking_sheet_formats(sh, ws, [])
        return

    all_values = ws.get_all_values()

    header_mismatch = bool(all_values) and all_values[0] != TECH_TRACK_HEADERS

    if not all_values:
        _run_sheet_write(
            f"{TECH_TRACK_SHEET_NAME} 建立表頭",
            lambda: ws.append_row(TECH_TRACK_HEADERS, value_input_option='USER_ENTERED')
        )
        all_values = [TECH_TRACK_HEADERS]
        existing_key_to_row = {}
    elif header_mismatch:
        print(f"{TECH_TRACK_SHEET_NAME} 偵測到 header 變動，執行 clear 重建...")
        _run_sheet_write(f"{TECH_TRACK_SHEET_NAME} 清空工作表", lambda: ws.clear())
        _run_sheet_write(
            f"{TECH_TRACK_SHEET_NAME} 重建表頭",
            lambda: ws.append_row(TECH_TRACK_HEADERS, value_input_option='USER_ENTERED')
        )
        all_values = [TECH_TRACK_HEADERS]
        existing_key_to_row = {}
    else:
        existing_key_to_row = {}
        for row_idx, row in enumerate(all_values[1:], start=2):
            if len(row) < 19:
                continue
            calc_date = str(row[0]).strip()
            row_code = str(row[1]).replace("'", "").strip()
            row_status = str(row[3]).strip()
            row_period = str(row[18]).strip()
            key = f"{calc_date}_{row_code}_{row_period}_{row_status}"
            existing_key_to_row[key] = row_idx

    rows_to_append = []
    row_style_targets = []
    update_value_ranges = []
    update_count = 0

    for row in rows:
        calc_date = str(row[0]).strip()
        row_code = str(row[1]).replace("'", "").strip()
        row_status = str(row[3]).strip()
        row_period = str(row[18]).strip()
        key = f"{calc_date}_{row_code}_{row_period}_{row_status}"

        is_match = str(row[5]).upper() == "TRUE"
        is_breakout = str(row[11]).upper() == "TRUE"

        if key in existing_key_to_row:
            r = existing_key_to_row[key]
            update_value_ranges.append({
                "range": f"A{r}:{last_col}{r}",
                "values": [row],
            })
            row_style_targets.append((r, is_match, is_breakout))
            update_count += 1
        else:
            rows_to_append.append(row)
            existing_key_to_row[key] = -1

    if update_value_ranges:
        _batch_update_row_values(update_value_ranges)

    if rows_to_append:
        append_start_row = len(all_values) + 1
        _run_sheet_write(
            f"{TECH_TRACK_SHEET_NAME} 批次新增 {len(rows_to_append)} 筆",
            lambda: ws.append_rows(rows_to_append, value_input_option='USER_ENTERED')
        )
        for offset, row in enumerate(rows_to_append):
            row_style_targets.append((
                append_start_row + offset,
                str(row[5]).upper() == "TRUE",
                str(row[11]).upper() == "TRUE"
            ))

    apply_technical_tracking_sheet_formats(sh, ws, row_style_targets)

    true_count = sum(1 for r in rows if str(r[5]).upper() == "TRUE")
    breakout_count = sum(1 for r in rows if str(r[11]).upper() == "TRUE")
    retest_count = sum(1 for r in rows if str(r[12]).upper() == "TRUE")
    print(
        f"{TECH_TRACK_SHEET_NAME} 更新完成："
        f"新增 {len(rows_to_append)} 筆、更新 {update_count} 筆、"
        f"符合條件 TRUE {true_count} 筆、曾回測MA20 {retest_count} 筆、"
        f"回測後轉強 TRUE {breakout_count} 筆。"
    )


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
    if dt_today_pct is None or dt_avg6_pct is None:
        pending_msg = "(當沖率待公布)"
    else:
        dt_vol_est = curr_vol_shares * (dt_today_pct / 100.0)
        dt_vol_lots = dt_vol_est / 1000
        is_exclude = (turnover < 5) or (turnover_val_money < 500000000) or (dt_vol_lots < 5000)
        if not is_exclude:
            if dt_avg6_pct > 60 and dt_today_pct > 60:
                triggers.append(f"【第十三款】當沖{dt_today_pct}%(6日{dt_avg6_pct}%)")

    if triggers:
        res['is_triggered'] = True
        res['risk_level'] = '高'
        res['trigger_msg'] = "且".join(triggers) + (f" {pending_msg}" if pending_msg else "")
    else:
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
    for b, c in zip(status_list[-3:], clause_list[-3:]):
        if b == 1 and (1 in parse_clause_ids_strict(c)):
            c1_streak += 1

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
        for b, c in zip(status_list[-3:], clause_list[-3:]):
            if b == 1 and (1 in parse_clause_ids_strict(c)):
                c1_streak += 1

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

# ==========================================
# 處置股 90 日明細爬蟲邏輯
# ==========================================
def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def fetch_tpex_jail_90d_requests(s_date, e_date):
    print(f"  [上櫃] 啟動 Requests 爬蟲 (新版官網 API)... {s_date} ~ {e_date}")

    real_end_date = e_date + timedelta(days=30)

    sd = f"{s_date.year - 1911}/{s_date.month:02d}/{s_date.day:02d}"
    ed = f"{real_end_date.year - 1911}/{real_end_date.month:02d}/{real_end_date.day:02d}"

    url = "https://www.tpex.org.tw/www/zh-tw/bulletin/disposal"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "https://www.tpex.org.tw/www/zh-tw/bulletin/disposal"
    }

    payload = {"startDate": sd, "endDate": ed, "response": "json"}

    sess = requests.Session()
    clean_data = []

    try:
        sess.get(url, headers=headers)
        r = sess.post(url, data=payload, headers=headers, timeout=10)

        if r.status_code == 200:
            data = r.json()
            if "tables" in data and len(data["tables"]) > 0:
                rows = data["tables"][0].get("data", [])
                print(f"    偵測到 {len(rows)} 筆資料...")

                for row in rows:
                    if len(row) < 6: continue
                    c_code = str(row[2]).strip()
                    c_name_raw = str(row[3]).strip()
                    c_name = c_name_raw.split("(")[0] if "(" in c_name_raw else c_name_raw
                    c_period = str(row[5]).strip()

                    if c_code.isdigit() and len(c_code) == 4:
                        clean_data.append({
                            "Code": c_code,
                            "Name": c_name,
                            "Period": c_period,
                            "Market": "上櫃"
                        })
    except Exception as e:
        print(f"    TPEx Requests 失敗: {e}")

    if clean_data:
        return pd.DataFrame(clean_data)
    return pd.DataFrame()

def fetch_twse_selenium_90d(s_date, e_date):
    print(f"  [上市] 啟動 Selenium 瀏覽器... {s_date} ~ {e_date}")

    sd_str = s_date.strftime("%Y%m%d")
    ed_str = e_date.strftime("%Y%m%d")

    url = "https://www.twse.com.tw/zh/announcement/punish.html"
    driver = get_driver()
    clean_data = []

    try:
        driver.get(url)
        wait = WebDriverWait(driver, 20)

        driver.execute_script(f"""
            document.querySelector('input[name="startDate"]').value = "{sd_str}";
            document.querySelector('input[name="endDate"]').value = "{ed_str}";
        """)

        search_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.search")))
        search_btn.click()

        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr")))
        time.sleep(3)

        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        print(f"    偵測到 {len(rows)} 筆資料，開始解析...")

        for row in rows:
            try:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 7:
                    c_code = cols[2].text.strip()
                    c_name = cols[3].text.strip()
                    c_period = cols[6].text.strip()

                    if c_code and c_code.isdigit() and len(c_code) == 4:
                          clean_data.append({
                            "Code": c_code,
                            "Name": c_name,
                            "Period": c_period,
                            "Market": "上市"
                        })
            except: continue

    except Exception as e:
        print(f"    TWSE Selenium 操作失敗: {e}")
    finally:
        driver.quit()

    if clean_data:
        print(f"    成功解析 {len(clean_data)} 筆資料")
        return pd.DataFrame(clean_data)

    print("    TWSE 無資料")
    return pd.DataFrame()


def run_jail_crawler_pipeline_sync():
    end_date = TARGET_DATE.date() + timedelta(days=30)
    start_date = TARGET_DATE.date() - timedelta(days=150)

    print(f"啟動全市場處置股抓取 (TWSE: Selenium / TPEx: Requests)")
    print(f"搜尋範圍 (含未來預告): {start_date} ~ {end_date}")

    df_tpex = fetch_tpex_jail_90d_requests(start_date, end_date)
    df_twse = fetch_twse_selenium_90d(start_date, end_date)

    all_dfs = []
    if not df_tpex.empty: all_dfs.append(df_tpex)
    if not df_twse.empty: all_dfs.append(df_twse)

    if all_dfs:
        print("\n合併處置股資料中...")
        final_df = pd.concat(all_dfs, ignore_index=True)

        final_df["Code"] = final_df["Code"].astype(str).str.strip()
        final_df["Name"] = final_df["Name"].astype(str).str.strip()
        final_df["Period"] = final_df["Period"].astype(str).str.strip()

        mask_empty_code = (final_df["Code"] == "")
        if mask_empty_code.any():
            print(f"發現 {mask_empty_code.sum()} 筆代號空白資料，嘗試修復...")
            extracted = final_df.loc[mask_empty_code, "Name"].str.extract(r'^(\d{4})')
            final_df.loc[mask_empty_code, "Code"] = extracted[0].fillna("")
            final_df.loc[mask_empty_code, "Name"] = final_df.loc[mask_empty_code, "Name"].str.replace(r'^\d{4}\s+', '', regex=True)

        final_df["Code"] = final_df["Code"].astype(str).str.replace(r'\D', '', regex=True)
        final_df = final_df[final_df["Code"].str.match(r'^\d{4}$')]

        def parse_sort_date(period_str):
            try:
                start_part = period_str.replace("~", "-").split("-")[0].strip()
                if "/" in start_part:
                    parts = start_part.split("/")
                    if len(parts) == 3:
                        y = int(parts[0]) + 1911
                        m = int(parts[1])
                        d = int(parts[2])
                        return f"{y}{m:02d}{d:02d}"
                return "99999999"
            except:
                return "99999999"

        final_df["SortDate"] = final_df["Period"].apply(parse_sort_date)
        final_df.sort_values(by=["SortDate", "Code"], ascending=[False, True], inplace=True)
        final_df.drop(columns=["SortDate"], inplace=True)

        final_df.rename(columns={
            "Market": "市場",
            "Code": "代號",
            "Name": "名稱",
            "Period": "處置期間"
        }, inplace=True)

        return final_df
    else:
        print("無處置股資料")
        return pd.DataFrame()

# ============================
# Main
# ============================
def main():
    sh, _ = connect_google_sheets()
    if not sh: return

    print("\n" + "="*50)
    print("啟動額外任務：抓取近 90 日處置股清單 (含未來處置)...")
    print("="*50)

    releasing_codes_map = {}

    try:
        df_jail_90 = run_jail_crawler_pipeline_sync()

        sheet_title = "處置股90日明細"
        export_cols = ["市場", "代號", "名稱", "處置期間"]
        ws_jail = get_or_create_ws(sh, sheet_title, headers=export_cols)

        if not df_jail_90.empty:
            df_jail_unique = df_jail_90.drop_duplicates(subset=["代號", "處置期間"])
            print(f"正在寫入 Google Sheet: {sheet_title} (新增模式)...")

            existing_rows = ws_jail.get_all_values()
            existing_keys = set()
            if len(existing_rows) > 1:
                for r in existing_rows[1:]:
                    if len(r) >= 4:
                        k = f"{str(r[1]).strip()}_{str(r[3]).strip()}"
                        existing_keys.add(k)

            rows_to_append = []
            new_count = 0
            for idx, row in df_jail_unique.iterrows():
                code = str(row["代號"]).strip()
                period = str(row["處置期間"]).strip()
                check_key = f"{code}_{period}"

                if check_key not in existing_keys:
                    rows_to_append.append([row["市場"], code, row["名稱"], period])
                    existing_keys.add(check_key)
                    new_count += 1

            if rows_to_append:
                ws_jail.append_rows(rows_to_append, value_input_option='USER_ENTERED')
                print(f"{sheet_title} 更新完成！成功新增 {new_count} 筆新處置資料。")
            else:
                print(f"{sheet_title} 無需新增 (所有資料已存在)。")
        else:
            print("查無新處置股資料，僅讀取現有紀錄。")

        print("重新讀取完整資料庫篩選即將出關股票 (5日內)...")

        all_jail_data = ws_jail.get_all_values()

        releasing_rows = []
        today_date = TARGET_DATE.date()
        stock_latest_end = {}
        release_calendar_start = today_date - timedelta(days=10)
        release_calendar_end = today_date + timedelta(days=90)
        release_cal_dates = get_trading_calendar_between(release_calendar_start, release_calendar_end)

        if len(all_jail_data) > 1:
            for r in all_jail_data[1:]:
                if len(r) < 4: continue

                code = str(r[1]).strip()
                if not code: continue

                period = str(r[3]).strip()
                sd_date, ed_date = parse_jail_period(period)

                if ed_date:
                    final_release_date = next_or_same_trade_date(ed_date, release_cal_dates) or ed_date
                    if code not in stock_latest_end or final_release_date > stock_latest_end[code]['release_date']:
                        stock_latest_end[code] = {
                            'date': ed_date,
                            'release_date': final_release_date,
                            'row_list': r[:4]
                        }

        sorted_stocks = sorted(stock_latest_end.items(), key=lambda x: x[1]['release_date'])

        for code, data in sorted_stocks:
            raw_end_date = data['date']
            final_end_date = data.get('release_date', raw_end_date)
            days_left = trading_days_left_for_release(today_date, final_end_date, release_cal_dates)
            if days_left is None:
                days_left = (final_end_date - today_date).days

            if 0 <= days_left <= 4:
                r_list = data['row_list'][:]
                r_list.append(str(days_left))
                r_list.append(final_end_date.strftime("%Y-%m-%d"))

                releasing_rows.append(r_list)
                releasing_codes_map[code] = days_left

        sheet_title_release = "即將出關監控"
        cols_release = export_cols + ["剩餘天數", "出關日期"]
        ws_release = get_or_create_ws(sh, sheet_title_release, headers=cols_release)
        ws_release.clear()

        if releasing_rows:
            ws_release.append_row(cols_release, value_input_option='USER_ENTERED')
            ws_release.append_rows(releasing_rows, value_input_option='USER_ENTERED')
            print(f"已寫入 {len(releasing_rows)} 檔至「{sheet_title_release}」")
        else:
            ws_release.append_row(["目前無 5 日內即將出關股票"], value_input_option='USER_ENTERED')
            print("目前無符合條件的即將出關股。")

        try:
            print("更新「處置股技術追蹤」工作表...")
            technical_rows = build_jail_technical_tracking_rows(stock_latest_end, releasing_codes_map, today_date)
            upsert_jail_technical_tracking_sheet(sh, technical_rows)
        except Exception as e:
            print(f"處置股技術追蹤更新失敗: {e}")
            traceback.print_exc()

    except Exception as e:
        print(f"處置股爬蟲或處理任務失敗: {e}")
        traceback.print_exc()

    update_market_monitoring_log(sh)

    cal_dates = get_official_trading_calendar(240)

    target_trade_date_obj = cal_dates[-1]
    is_today_trade = (target_trade_date_obj == TARGET_DATE.date())

    if is_today_trade and (not IS_AFTER_SAFE) and len(cal_dates) >= 2:
        print(f"現在時間 {TARGET_DATE.strftime('%H:%M')} 早於 {SAFE_CRAWL_TIME}，且日曆包含今日，切換為 T-1 模式。")
        target_trade_date_obj = cal_dates[-2]

    target_date_str = target_trade_date_obj.strftime("%Y-%m-%d")
    print(f"最終鎖定運算日期: {target_date_str}")

    ws_log = get_or_create_ws(sh, "每日紀錄", headers=['日期','市場','代號','名稱','觸犯條款'])

    backfill_daily_logs(sh, ws_log, cal_dates, target_trade_date_obj)

    print("讀取歷史 Log...")
    log_data = ws_log.get_all_records()
    df_log = pd.DataFrame(log_data)
    if not df_log.empty:
        df_log['代號'] = df_log['代號'].astype(str).str.strip().str.replace("'", "")
        df_log['日期'] = pd.to_datetime(df_log['日期'], errors='coerce').dt.strftime("%Y-%m-%d")
        df_log = df_log[df_log['日期'].notna()]

    clause_map = {}
    for _, r in df_log.iterrows():
        key = (str(r['代號']), str(r['日期']))
        clause_map[key] = merge_clause_text(clause_map.get(key,""), str(r['觸犯條款']))

    jail_map = get_jail_map_from_sheet(sh)

    exclude_map = build_exclude_map(cal_dates, jail_map)

    start_dt_str = cal_dates[-90].strftime("%Y-%m-%d")
    df_recent = df_log[df_log['日期'] >= start_dt_str]
    target_stocks = df_recent['代號'].unique()

    precise_db = load_precise_db_from_sheet(sh)
    rows_stats = []

    safe_cal_dates = [d for d in cal_dates if d <= target_trade_date_obj]

    print(f"掃描 {len(target_stocks)} 檔股票...")
    for idx, code in enumerate(target_stocks):
        code = str(code).strip()
        name = df_log[df_log['代號']==code]['名稱'].iloc[-1] if not df_log[df_log['代號']==code].empty else "未知"

        db_info = precise_db.get(code, {})
        m_type = str(db_info.get('market', '上市')).upper()
        suffix = '.TWO' if any(k in m_type for k in ['上櫃', 'TWO', 'TPEX', 'OTC']) else '.TW'
        ticker_code = f"{code}{suffix}"

        # ===========================================================
        # [V116.29 修正] 熱門統計直接以「每日紀錄」為準
        #
        # 之前即使用 get_last_n_trade_dates_with_attention()，仍可能因為
        # jail_map / 處置期間資料、舊處置區間或 stock_calendar 重建邏輯，
        # 讓「每日紀錄明明有公告」的日期沒有正確反映到近30日注意次數。
        #
        # 近30日熱門統計本質上應該回答：
        #   最近30個交易日內，每日紀錄中這檔股票有效注意股出現幾次？
        #
        # 因此這裡不再讓 jail_map / exclude_map 介入注意次數計算；
        # 只取 safe_cal_dates 最後30個交易日，逐日回查 clause_map。
        # 這樣像 4/29、4/30、5/4 連續三個交易日皆為第1款時，
        # 近30日注意次數會正確顯示為 3。
        # ===========================================================
        stock_calendar = [d for d in safe_cal_dates if d <= target_trade_date_obj][-30:]

        bits = []
        clauses = []
        for d in stock_calendar:
            d_str = d.strftime("%Y-%m-%d")
            c = clause_map.get((code, d_str), "")

            if c:
                bits.append(1)
                clauses.append(c)
            else:
                bits.append(0)
                clauses.append("")

        est_days, reason = simulate_days_to_jail_strict(
            bits, clauses,
            stock_id=code,
            target_date=TARGET_DATE.date(),
            jail_map=jail_map,
            enable_safe_filter=False
        )

        if code in releasing_codes_map:
            d_left = releasing_codes_map[code]
            reason = f"即將出關 (剩{d_left}天)"
            est_days = 3

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
                reason_display += " | 留意人工處置風險"
            if is_clause_13:
                reason_display += " (若進處置將關12天)"

        hist = fetch_history_data(ticker_code)
        if hist.empty:
            alt_s = '.TWO' if suffix=='.TW' else '.TW'
            hist = fetch_history_data(f"{code}{alt_s}")
            if not hist.empty: ticker_code = f"{code}{alt_s}"

        fund = fetch_stock_fundamental(code, ticker_code, precise_db)

        dt_today, dt_avg6 = None, None
        if IS_AFTER_DAYTRADE:
            dt_today, dt_avg6 = get_daytrade_stats_finmind(code, target_date_str)

        risk = calculate_full_risk(code, hist, fund, est_days_int, dt_today, dt_avg6)

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

        last_date_val = ""
        if stock_calendar:
            last_date_val = stock_calendar[-1].strftime("%Y-%m-%d")

        row = [
            f"'{code}", name, safe(streak), safe(sum(valid_bits)), safe(sum(valid_bits[-10:])),
            last_date_val,
            f"'{status_30}", f"'{status_30[-10:]}", est_days_display, safe(reason_display),
            safe(risk['risk_level']), safe(risk['trigger_msg']),
            safe(risk['curr_price']), safe(risk['limit_price']), safe(risk['gap_pct']),
            safe(risk['curr_vol']), safe(risk['limit_vol']), safe(risk['turnover_val']),
            safe(risk['turnover_rate']), safe(risk['pe']), safe(risk['pb']), safe(risk['day_trade_pct'])
        ]
        rows_stats.append(row)
        if (idx+1)%10==0: time.sleep(1)

    if rows_stats:
        print("更新統計表...")
        ws_stats = get_or_create_ws(sh, "近30日熱門統計", headers=STATS_HEADERS)
        ws_stats.clear()
        ws_stats.append_row(STATS_HEADERS, value_input_option='USER_ENTERED')
        ws_stats.append_rows(rows_stats, value_input_option='USER_ENTERED')
        print("完成")

if __name__ == "__main__":
    main()
