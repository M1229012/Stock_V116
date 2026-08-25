# -*- coding: utf-8 -*-
"""
V116.30 台股注意股系統 (同步 115.08.10 注意暨處置新制)

V116.30 中文標題：同步證交所 115.08.10 施行之注意暨處置新制
  修正依據：證交所 115.08.03 公告修正「公布或通知注意交易資訊暨處置作業要點」第6條，
            暨「第四條異常標準之詳細數據及除外情形」第12條，自 115.08.10 施行。
  修正說明：
    - 處置期間：一般處置由 10 個營業日縮短為 5 個營業日 (第一次及第二次含以上皆同)；
      併同「當沖交易占比過高」者由 12 個營業日縮短為 7 個營業日。
      → 新增 DISPOSAL_DAYS_NORMAL / DISPOSAL_DAYS_DAYTRADE 常數，
        並更新第13款的「刑期」提示文字 (原寫死 12 天)。
    - 撮合頻率：處置期間統一為約每 2 分鐘撮合一次 (原每 5 分鐘 / 每 20 分鐘)。
      → 新增 DISPOSAL_MATCH_INTERVAL 常數備查。
    - 第11款「最近6個營業日收盤價起迄價差」標準放寬：
      收盤價須逾 1,000 元才適用，價差門檻 300 元起；逾 2,000 元後每 1,000 元一級距、
      每級距增加 150 元 (舊制為 500 元起算、100 元門檻、每 500 元級距增加 25 元)。
      → 新增 get_clause11_gap_threshold()，並將價差改以「起迄兩個營業日收盤價」計算
        (舊版誤用區間內最高價-最低價的盤中高低點)，另補上「當日收盤價須為該期間
        最高或最低」之條件。
    - 連帶調整：「即將出關監控」預警窗口由 5 個交易日縮為 3 個交易日
      (RELEASE_ALERT_TRADING_DAYS)。因處置期間已縮短為 5 個營業日，
      沿用 5 日窗口會使個股一進處置就落入名單，篩選失去意義。
  未變更部分：
    - 達到處置的注意次數門檻本次未修正，仍為「連續3個營業日第1款」/「連續5個營業日」/
      「最近10個營業日內6次」/「最近30個營業日內12次」，
      故 check_jail_trigger_now() 與 simulate_days_to_jail_strict() 的計次邏輯不動。
    - 處置起訖日仍以官方「處置股90日明細」公告為準，不由天數推算，
      因此出關日與技術追蹤邏輯不受本次天數調整影響。

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

V116.29 中文標題：處置消耗切分點與官方處置同步修正
  修正說明：
    - 已被前一次處置消耗的注意次數，改以前一次「處置開始日前一個交易日」作為切分點，
      不再用「處置結束日」把處置期間內新公告的注意次數整段歸零。
    - 處置期間內若每日紀錄仍有官方注意股公告，會繼續納入新一輪累積。
    - 若「處置股90日明細」已出現官方處置期間，近30日熱門統計會同步覆蓋為官方處置狀態，
      避免其他程式讀取時誤判該股尚未進處置。
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

# ==========================================
# 處置制度參數 (證交所 115.08.03 公告，115.08.10 施行)
# ==========================================
# 修正依據：「公布或通知注意交易資訊暨處置作業要點」第6條，暨
#           「第四條異常標準之詳細數據及除外情形」第12條。
#
# 本次修正重點：
#   1. 一般處置期間：第一次及第二次(含)以上均縮短為 5 個營業日 (原 10 個營業日)。
#   2. 計算期間內同時因「當沖交易占比過高」(第13款) 被公布注意者，
#      處置期間縮短為 7 個營業日 (原 12 個營業日)。
#   3. 處置期間撮合頻率統一為約每 2 分鐘一次 (原為每 5 分鐘 / 每 20 分鐘)。
#   4. 第11款「最近6個營業日收盤價起迄價差」標準放寬 (詳見 CLAUSE11_* 常數)。
#
# ⚠️ 未修正部分：達到處置的注意次數門檻維持原標準，
#    即「連續3個營業日第1款」/「連續5個營業日」/「最近10個營業日內6次」/
#    「最近30個營業日內12次」，故 check_jail_trigger_now() 與
#    simulate_days_to_jail_strict() 的計次邏輯不需調整。
DISPOSAL_DAYS_NORMAL = 5                # 一般處置之營業日數
DISPOSAL_DAYS_DAYTRADE = 7              # 併同「當沖過高」之處置營業日數
DISPOSAL_NEW_RULE_START_DATE = date(2026, 8, 10)   # 新制施行日 (民國115.08.10)
DISPOSAL_OLD_DAYS_NORMAL = 10           # 舊制一般處置營業日數 (供過渡換算反推類型)
DISPOSAL_OLD_DAYS_DAYTRADE = 12         # 舊制併同當沖過高之營業日數
DISPOSAL_MATCH_INTERVAL = "約每2分鐘"   # 處置期間分盤集合競價撮合頻率

# 第11款：最近6個營業日「收盤價起迄價差」標準 (115.08.10 起適用)
#   - 當日收盤價須「逾」1,000 元，本款才適用。
#   - 1,000 元 < 收盤價 <= 2,000 元：價差門檻 300 元。
#   - 收盤價逾 2,000 元：每 1,000 元為一級距，每一級距價差門檻增加 150 元
#     (例：2,000~3,000 元 → 450 元；3,000~4,000 元 → 600 元)。
#   - 舊制為：收盤價 500 元起算、基準價差 100 元，每 500 元一級距增加 25 元。
CLAUSE11_MIN_PRICE = 1000.0    # 適用本款之收盤價下限 (須「逾」此價格)
CLAUSE11_BASE_GAP = 300.0      # 第一級距之價差門檻
CLAUSE11_TIER_SIZE = 1000.0    # 級距大小
CLAUSE11_TIER_STEP = 150.0     # 每增加一級距之價差門檻增額

# 「即將出關監控」預警窗口 (交易日數)。
# 舊制處置 10 個營業日時設為 5 日仍具篩選意義；新制縮短為 5 個營業日後，
# 若沿用 5 日窗口，個股從進處置第一天就會落入名單，篩選將完全失效。
# 故配合新制調整為 3 個交易日，維持「處置後段才預警」的原意。
RELEASE_ALERT_TRADING_DAYS = 3

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
TECH_MA20_GAP_THRESHOLD = 5.0
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

# 證交所注意股公告實際約於 18:00~19:00 才發布完整。
# 早於此時間執行時：
#   - backfill_daily_logs() 會跳過「今天」不去抓 (main.py 回補迴圈)
#   - 主流程切換為 T-1 模式，改以前一交易日為運算基準
# 原本設 17:30 等於宣稱「17:30 資料就齊了」，與實際不符：
# 18:15 那趟會抓到尚未更新的空資料，配合
# fetch_twse_attention_rows() 的「0 筆視為失敗」修正後會直接中止整輪。
# 故調整為 19:00，讓 18:15 那趟安全地算 T-1，19:15 那趟才處理當日。
SAFE_CRAWL_TIME = dt_time(19, 0)
DAYTRADE_PUBLISH_TIME = dt_time(21, 0)
SAFE_MARKET_OPEN_CHECK = dt_time(16, 30)

IS_NIGHT_RUN = TARGET_DATE.hour >= 20
IS_AFTER_SAFE = TARGET_DATE.time() >= SAFE_CRAWL_TIME
IS_AFTER_DAYTRADE = TARGET_DATE.time() >= DAYTRADE_PUBLISH_TIME

MAX_BACKFILL_TRADING_DAYS = 40
VERIFY_RECENT_DAYS = 2

# ==========================================
# ⚠️ 近30日熱門統計資料校正開關
# ==========================================
# 目前近30日熱門統計已固定改為：只讀 Google Sheet「每日紀錄」計算。
# 這個開關保留為舊版校正用途的備註，不再影響熱門統計資料來源。
#
# 正確資料流：每日紀錄 → 30/10/5 日狀態碼與注意次數 → 處置倒數。
# 若未來要校正舊資料，請先修正「每日紀錄」，再重新執行本程式。
FORCE_REFRESH_30D_STATS = False

# ==========================================
# ⚠️ 每日紀錄觸犯條款校正開關
# ==========================================
# 用途：修正過去「每日紀錄」中被整段官方原文污染、或條款解析錯誤的資料。
# 流程：重新抓取最近 N 個交易日官方注意股公告，只校正 / 補齊「每日紀錄」的觸犯條款，
#       之後近30日熱門統計仍只讀 Google Sheet「每日紀錄」來計算。
#
# 目前建議：第一次修正舊資料時設為 60；確認 Google Sheet 正確後，請改回 0，
#           避免每次執行都重新抓取大量歷史公告。
# 注意：若某天 TPEx / TWSE 官方網站暫時 520 或抓取失敗，該日會被跳過，
#       不會用空白資料覆蓋既有正確條款。
REFRESH_DAILY_LOG_CLAUSES_DAYS = 10

# ==========================================
# FinMind 金鑰設定
# ==========================================
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"

token1 = os.getenv('FinMind_1')
token2 = os.getenv('FinMind_2')
FINMIND_TOKENS = [t for t in [token1, token2] if t]

CURRENT_TOKEN_INDEX = 0
_FINMIND_CACHE = {}

# ==========================================
# TWSE 官方網站存取保護與同輪快取
# ==========================================
# TWSE 可能對短時間密集請求或 GitHub Actions 共用出口 IP 回傳
# HTTP 307 /「因為安全性考量」/「請稍候再試」。
# 這裡只調整抓取層：加入節流、官方端點輪替、OpenAPI 與 Selenium 備援；
# 其餘統計、Google Sheet 與處置判斷邏輯不變。
TWSE_REQUEST_INTERVAL_SECONDS = 2.3
TWSE_HTTP_TIMEOUT_SECONDS = 20
TWSE_DAILY_SELENIUM_FALLBACK_LIMIT = 2

_TWSE_LAST_REQUEST_MONOTONIC = 0.0
_TWSE_HTTP_SESSION = None
_TWSE_DAILY_SELENIUM_FALLBACK_USED = 0
_DAILY_NOTICE_CACHE = {}

print(f"啟動 V116.30 台股注意股系統 (同步 115.08.10 注意暨處置新制)")
print(f"系統時間 (Taiwan): {TARGET_DATE.strftime('%Y-%m-%d %H:%M:%S')}")

try: twstock.__update_codes()
except: pass

# ============================
# 工具函式
# ============================
CN_NUM = {
    "十四": "14", "十三": "13", "十二": "12", "十一": "11", "十": "10",
    "九": "9", "八": "8", "七": "7", "六": "6", "五": "5",
    "四": "4", "三": "3", "二": "2", "一": "1",
}

# 僅保留「安全長字串」做款別輔助判斷。
# 不使用「成交量、週轉率、本益比、股價淨值比」這類短詞，避免把一般說明誤判成第1～8款。
KEYWORD_MAP = {
    # 第1款：官方公告若未明確寫「第1款」，但出現第1款常見描述時補判。
    # 只加回明確長字串，不恢復「成交量、週轉率、本益比」等模糊短詞。
    "最近六個營業日累積之收盤價漲跌百分比": 1,
    "最近六個營業日累積之最後成交價漲跌百分比": 1,

    # 第2款：較長期間起迄兩個營業日收盤價漲跌百分比異常
    "最近三十個營業日起迄兩個營業日之收盤價漲跌百分比": 2,
    "最近六十個營業日起迄兩個營業日之收盤價漲跌百分比": 2,
    "最近九十個營業日起迄兩個營業日之收盤價漲跌百分比": 2,
    "起迄兩個營業日之收盤價漲跌百分比": 2,

    # 第9～13款：特殊型態，通常不列入一般處置累積，但仍需正確記錄款別
    "日平均成交量較最近": 9,
    "之日平均成交量較最近": 9,
    "累積週轉率": 10,
    "起迄兩個營業日之最後成交價差": 11,
    "起迄兩個營業日收盤價價差": 11,
    "起迄兩個營業日之收盤價價差": 11,
    "借券賣出成交量占": 12,
    "當日沖銷成交量占": 13,
}

def normalize_clause_text(s: str) -> str:
    if not s: return ""
    s = str(s)
    s = s.replace("第ㄧ款", "第一款")
    s = s.translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    # 先轉換「十一～十四」等長字串，再轉換單字，避免「第十一款」被拆成「第十1款」。
    for cn, dg in CN_NUM.items():
        s = s.replace(f"第{cn}款", f"第{dg}款")
    return s

def parse_clause_ids_strict(clause_text):
    """解析注意股款別。

    優先採用官方文字中明確出現的「第N款」。若沒有明確款別，才使用
    KEYWORD_MAP 中的安全長字串輔助判斷；不得使用過短關鍵字，避免把
    一般交易資訊誤判成可累積處置條款。
    """
    if not isinstance(clause_text, str): return set()
    clause_text = normalize_clause_text(clause_text)
    ids = set()
    matches = re.findall(r'第\s*(\d+)\s*款', clause_text)
    for m in matches:
        try:
            ids.add(int(m))
        except:
            pass

    if not ids:
        for keyword, code in KEYWORD_MAP.items():
            if keyword in clause_text:
                ids.add(code)

    return ids

def merge_clause_text(a, b):
    ids = set()
    ids |= parse_clause_ids_strict(a) if a else set()
    ids |= parse_clause_ids_strict(b) if b else set()
    if ids: return "、".join([f"第{x}款" for x in sorted(ids)])
    # 解析不到明確款別時，不保留原始長文字，避免污染每日紀錄與 clause_map。
    return ""

def is_clean_clause_text(clause_text):
    """判斷觸犯條款是否為乾淨款別格式，例如：第1款 或 第1款、第2款。"""
    if not isinstance(clause_text, str):
        return False
    s = normalize_clause_text(clause_text).strip()
    if not s:
        return False
    pattern = r'^第\s*\d+\s*款(?:[、,，/／\s]+第\s*\d+\s*款)*$'
    return bool(re.fullmatch(pattern, s))

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
    s = s.replace("－", "~").replace("–", "~").replace("—", "~")
    s = s.replace("～", "~").replace("至", "~").replace("到", "~")
    s = re.sub(r"\s+", "", s)

    dates = []
    if "~" in s:
        dates = s.split("~", 1)
    elif "-" in s and "/" in s and s.count("-") == 1:
        dates = s.split("-", 1)

    if len(dates) >= 2:
        sd = parse_roc_date(dates[0].strip())
        ed = parse_roc_date(dates[1].strip())
        if sd and ed:
            return sd, ed
    return None, None


def count_trading_days_inclusive(sd, ed, cal_dates):
    """計算 sd~ed 之間的營業日數 (含頭含尾)。"""
    if not sd or not ed or ed < sd:
        return 0
    return sum(1 for d in cal_dates if sd <= d <= ed)


def apply_disposal_transition_rule(sd, ed, cal_dates):
    """依 115.08.10 新制過渡規定，換算施行前既有處置的實際結束日。

    過渡規定：
      施行日起仍在處置中之有價證券立即適用新制。
      施行日前已執行滿新制所需營業日數者，於施行日解除處置；
      未滿者，繼續執行至滿足新制日數為止。

    如何判斷該檔應適用 5 日或 7 日：
      「處置股90日明細」只有處置期間，沒有「是否併同當沖過高」欄位，
      因此以原公告營業日數反推 —— 舊制僅有兩種長度：
        原 10 個營業日 (一般處置)        -> 新制 5 個營業日
        原 12 個營業日 (併同當沖過高)    -> 新制 7 個營業日

    回傳調整後的結束日；不需調整或資料不足時回傳原結束日。
    """
    if not sd or not ed:
        return ed

    # 新制施行日當天(含)之後才開始的處置，本來就依新制公告，不調整。
    if sd >= DISPOSAL_NEW_RULE_START_DATE:
        return ed

    if not cal_dates:
        return ed

    old_days = count_trading_days_inclusive(sd, ed, cal_dates)
    if old_days <= DISPOSAL_DAYS_NORMAL:
        return ed      # 已等於或短於新制天數，無須調整

    required = (DISPOSAL_DAYS_DAYTRADE
                if old_days > DISPOSAL_OLD_DAYS_NORMAL
                else DISPOSAL_DAYS_NORMAL)

    # 施行日前已執行的營業日
    served_days = [d for d in cal_dates if sd <= d < DISPOSAL_NEW_RULE_START_DATE]

    if len(served_days) >= required:
        # 已關滿 -> 施行日解除，最後處置日為施行日前一營業日
        new_ed = served_days[-1]
    else:
        # 未關滿 -> 從起始日起算，續關至滿足新制日數
        in_period = [d for d in cal_dates if sd <= d <= ed]
        if len(in_period) < required:
            return ed
        new_ed = in_period[required - 1]

    return min(new_ed, ed)

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


def get_consumed_attention_cutoff_date(stock_id, target_trade_date, jail_map, cal_dates):
    """已用處置次數切分點。

    正確規則：
    - 已經用來觸發前一次處置的注意次數不可重複計算。
    - 但處置期間內若交易所又公告注意股，這些屬於新的注意紀錄，不能因為落在處置期間就被排除。
    - 因此切分點應該是「前一次處置開始日前一個交易日」，不是「前一次處置結束日」。
    """
    if not stock_id or not target_trade_date or not jail_map or stock_id not in jail_map:
        return None

    if not isinstance(target_trade_date, date):
        target_trade_date = pd.to_datetime(target_trade_date).date()

    started_periods = []
    for s, e in jail_map.get(stock_id, []):
        if s and s <= target_trade_date:
            started_periods.append((s, e))

    if not started_periods:
        return None

    latest_start = max(s for s, _ in started_periods)
    cutoff_date = prev_trade_date(latest_start, cal_dates)
    if cutoff_date:
        return cutoff_date

    return latest_start - timedelta(days=1)


def format_roc_date_for_display(d):
    if not d:
        return ""
    try:
        if not isinstance(d, date):
            d = pd.to_datetime(d).date()
        return f"{d.year - 1911}/{d.month:02d}/{d.day:02d}"
    except:
        return str(d)


def format_disposal_period_for_display(start_date, end_date, raw_period=""):
    raw_period = str(raw_period or "").strip()
    if raw_period:
        return raw_period
    return f"{format_roc_date_for_display(start_date)}~{format_roc_date_for_display(end_date)}"


def build_official_disposal_status_map_from_rows(all_jail_data, today_date):
    """從「處置股90日明細」原始列建立官方處置狀態表。

    用途：
    近30日熱門統計雖然仍以「每日紀錄」計算注意次數與狀態碼，
    但只要官方處置表已經公告未來處置或目前正在處置，
    顯示狀態就必須以官方公告為準。
    """
    status_map = {}

    if not isinstance(today_date, date):
        today_date = pd.to_datetime(today_date).date()

    if not all_jail_data or len(all_jail_data) <= 1:
        return status_map

    for r in all_jail_data[1:]:
        if len(r) < 4:
            continue

        market = str(r[0]).strip()
        code = str(r[1]).replace("'", "").strip()
        name = str(r[2]).strip()
        period = str(r[3]).strip()

        if not code:
            continue

        sd, ed = parse_jail_period(period)
        if not sd or not ed:
            continue

        period_text = format_disposal_period_for_display(sd, ed, period)

        item = None
        if sd <= today_date <= ed:
            item = {
                "priority": 0,
                "sort_key": -sd.toordinal(),
                "market": market,
                "code": code,
                "name": name,
                "start": sd,
                "end": ed,
                "period": period_text,
                "reason": f"官方處置中：{period_text}",
            }
        elif today_date < sd:
            item = {
                "priority": 1,
                "sort_key": sd.toordinal(),
                "market": market,
                "code": code,
                "name": name,
                "start": sd,
                "end": ed,
                "period": period_text,
                "reason": f"官方已公告處置：{period_text}",
            }

        if item:
            old = status_map.get(code)
            if old is None or (item["priority"], item["sort_key"]) < (old["priority"], old["sort_key"]):
                status_map[code] = item

    return status_map

# ============================
# 每日公告爬蟲區 (TWSE / TPEx)
# ============================
def _twse_clean_text(value):
    if value is None:
        return ""
    s = str(value)
    s = re.sub(r"<[^>]+>", " ", s)
    s = s.replace("&nbsp;", " ")
    s = s.replace("\u3000", " ")
    s = s.replace("\xa0", " ")
    s = s.replace("\r", " ")
    s = s.replace("\n", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _twse_parse_any_date_to_ad_date(value):
    """解析 TWSE 可能回傳的民國或西元日期。"""
    raw = _twse_clean_text(value)
    if not raw:
        return None

    raw = raw.replace("年", "/").replace("月", "/").replace("日", "")
    raw = raw.replace(".", "/").replace("-", "/").strip()

    # 西元 YYYYMMDD
    m = re.fullmatch(r"(\d{4})(\d{2})(\d{2})", raw)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            return None

    # 民國 YYYMMDD
    m = re.fullmatch(r"(\d{3})(\d{2})(\d{2})", raw)
    if m:
        try:
            return date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3)))
        except Exception:
            return None

    m = re.search(r"(\d{2,4})/(\d{1,2})/(\d{1,2})", raw)
    if m:
        try:
            y = int(m.group(1))
            mo = int(m.group(2))
            da = int(m.group(3))
            if y < 1911:
                y += 1911
            return date(y, mo, da)
        except Exception:
            return None

    return None


def _twse_find_field_index(fields, keywords):
    for idx, field in enumerate(fields or []):
        clean_field = re.sub(r"\s+", "", _twse_clean_text(field))
        if any(keyword in clean_field for keyword in keywords):
            return idx
    return None


def _twse_get_session():
    global _TWSE_HTTP_SESSION
    if _TWSE_HTTP_SESSION is None:
        _TWSE_HTTP_SESSION = requests.Session()
    return _TWSE_HTTP_SESSION


def _twse_reset_session():
    global _TWSE_HTTP_SESSION
    try:
        if _TWSE_HTTP_SESSION is not None:
            _TWSE_HTTP_SESSION.close()
    except Exception:
        pass
    _TWSE_HTTP_SESSION = requests.Session()


def _twse_wait_before_request():
    global _TWSE_LAST_REQUEST_MONOTONIC

    now = time.monotonic()
    elapsed = now - _TWSE_LAST_REQUEST_MONOTONIC
    remain = TWSE_REQUEST_INTERVAL_SECONDS - elapsed
    if remain > 0:
        time.sleep(remain + random.uniform(0.10, 0.35))

    _TWSE_LAST_REQUEST_MONOTONIC = time.monotonic()


def _twse_common_headers(referer):
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/149.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": referer,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "close",
    }


def _twse_response_is_blocked(response):
    body = _twse_clean_text(getattr(response, "text", ""))[:1500].lower()
    blocked_words = [
        "for security reasons",
        "因為安全性考量",
        "頁面無法呈現",
        "請稍候再試",
        "access denied",
        "request rejected",
    ]
    return any(word.lower() in body for word in blocked_words)


def _twse_request_json(url, params, referer, label):
    """向 TWSE 官方端點送出一次節流請求；失敗回傳 None。"""
    try:
        _twse_wait_before_request()
        session = _twse_get_session()
        response = session.get(
            url,
            params=params,
            headers=_twse_common_headers(referer),
            timeout=TWSE_HTTP_TIMEOUT_SECONDS,
            allow_redirects=False,
        )

        if response.status_code != 200 or _twse_response_is_blocked(response):
            print(
                f"{label} 失敗：HTTP {response.status_code}，URL={response.url}，"
                f"blocked={_twse_response_is_blocked(response)}"
            )
            if response.text:
                print(f"   回應內容前300字：{response.text[:300]}")
            _twse_reset_session()
            return None

        try:
            return response.json()
        except Exception as e:
            print(f"{label} JSON 解析失敗：{type(e).__name__}: {e}")
            print(f"   URL={response.url}")
            print(f"   回應內容前300字：{response.text[:300]}")
            return None

    except Exception as e:
        print(f"{label} 請求例外：{type(e).__name__}: {e}")
        _twse_reset_session()
        return None


def _twse_dedupe_attention_rows(rows):
    merged = {}
    for row in rows or []:
        date_key = str(row.get("日期", "")).strip()
        code = str(row.get("代號", "")).strip()
        if not date_key or not code:
            continue
        key = (date_key, code)
        if key not in merged:
            merged[key] = dict(row)
        else:
            merged[key]["觸犯條款"] = merge_clause_text(
                merged[key].get("觸犯條款", ""),
                row.get("觸犯條款", ""),
            )
            if not merged[key].get("名稱") and row.get("名稱"):
                merged[key]["名稱"] = row.get("名稱")
    return list(merged.values())


def _twse_parse_notice_payload(payload, query_date_obj, date_str):
    """解析 TWSE 歷史報表 JSON 或 OpenAPI JSON；未知格式回傳 None。"""
    rows = []

    if isinstance(payload, dict) and "data" in payload:
        stat_text = _twse_clean_text(payload.get("stat", ""))
        raw_data = payload.get("data", []) or []
        fields = [_twse_clean_text(x) for x in payload.get("fields", []) or []]

        if not isinstance(raw_data, list):
            return None

        # 少數官方服務可能把 OpenAPI 的 list[dict] 包在 data 欄位內。
        if raw_data and all(isinstance(item, dict) for item in raw_data):
            return _twse_parse_notice_payload(raw_data, query_date_obj, date_str)

        if not raw_data:
            # 官方合法空資料可能回傳 stat=OK 或「沒有符合條件的資料」。
            if (not stat_text) or ("OK" in stat_text.upper()) or ("沒有" in stat_text) or ("查無" in stat_text):
                return []

        code_idx = _twse_find_field_index(fields, ["證券代號", "有價證券代號", "股票代號"])
        name_idx = _twse_find_field_index(fields, ["證券名稱", "有價證券名稱", "股票名稱"])
        clause_idx = _twse_find_field_index(fields, ["注意交易資訊", "注意資訊"])
        date_idx = _twse_find_field_index(fields, ["日期", "公告日期"])

        if code_idx is None:
            code_idx = 1
        if name_idx is None:
            name_idx = 2
        if clause_idx is None:
            clause_idx = 4
        if date_idx is None:
            date_idx = 5

        for item in raw_data:
            if not isinstance(item, list):
                continue

            try:
                code = _twse_clean_text(item[code_idx])
            except Exception:
                continue

            if not (code.isdigit() and len(code) == 4):
                continue

            name = _twse_clean_text(item[name_idx]) if name_idx < len(item) else ""
            raw = " ".join(_twse_clean_text(x) for x in item)
            clause_source = _twse_clean_text(item[clause_idx]) if clause_idx < len(item) else raw

            official_date = None
            if date_idx < len(item):
                official_date = _twse_parse_any_date_to_ad_date(item[date_idx])
            if official_date is not None and official_date != query_date_obj:
                continue

            ids = parse_clause_ids_strict(clause_source or raw)
            clause_text = "、".join(f"第{x}款" for x in sorted(ids))
            rows.append({
                "日期": date_str,
                "市場": "TWSE",
                "代號": code,
                "名稱": name,
                "觸犯條款": clause_text,
            })

        return _twse_dedupe_attention_rows(rows)

    # OpenAPI 通常直接回傳 list[dict]
    if isinstance(payload, list):
        if not payload:
            return []

        recognized = False
        for item in payload:
            if not isinstance(item, dict):
                continue

            recognized = True
            values = [_twse_clean_text(v) for v in item.values()]
            raw = " ".join(values)

            code = ""
            for key in ["證券代號", "有價證券代號", "股票代號", "Code", "code", "SecuritiesCode"]:
                candidate = _twse_clean_text(item.get(key, ""))
                if candidate.isdigit() and len(candidate) == 4:
                    code = candidate
                    break
            if not code:
                for candidate in values:
                    if candidate.isdigit() and len(candidate) == 4:
                        code = candidate
                        break
            if not code:
                continue

            name = ""
            for key in ["證券名稱", "有價證券名稱", "股票名稱", "Name", "name", "SecuritiesName"]:
                candidate = _twse_clean_text(item.get(key, ""))
                if candidate:
                    name = candidate
                    break

            official_date = None
            for key in ["日期", "公告日期", "Date", "date", "TradeDate"]:
                official_date = _twse_parse_any_date_to_ad_date(item.get(key, ""))
                if official_date is not None:
                    break
            if official_date is None:
                for candidate in values:
                    official_date = _twse_parse_any_date_to_ad_date(candidate)
                    if official_date is not None:
                        break

            # OpenAPI 是「當日公布注意股票」。若資料沒有日期欄，只允許用於今日查詢。
            if official_date is not None:
                if official_date != query_date_obj:
                    continue
            elif query_date_obj != TARGET_DATE.date():
                continue

            ids = parse_clause_ids_strict(raw)
            clause_text = "、".join(f"第{x}款" for x in sorted(ids))
            rows.append({
                "日期": date_str,
                "市場": "TWSE",
                "代號": code,
                "名稱": name,
                "觸犯條款": clause_text,
            })

        if recognized:
            return _twse_dedupe_attention_rows(rows)

    return None


def _fetch_twse_attention_openapi(date_obj, date_str):
    if date_obj != TARGET_DATE.date():
        return None

    payload = _twse_request_json(
        "https://openapi.twse.com.tw/v1/announcement/notice",
        params={},
        referer="https://openapi.twse.com.tw/",
        label=f"TWSE OpenAPI {date_str}",
    )
    if payload is None:
        return None
    return _twse_parse_notice_payload(payload, date_obj, date_str)


def _fetch_twse_attention_selenium(date_obj, date_str):
    """最後備援：直接開啟官方單日 HTML 報表，不使用表單按鈕。"""
    global _TWSE_DAILY_SELENIUM_FALLBACK_USED

    if _TWSE_DAILY_SELENIUM_FALLBACK_USED >= TWSE_DAILY_SELENIUM_FALLBACK_LIMIT:
        print(
            f"TWSE {date_str} Selenium 備援已達本輪上限 "
            f"{TWSE_DAILY_SELENIUM_FALLBACK_LIMIT} 次，略過瀏覽器重試。"
        )
        return None

    _TWSE_DAILY_SELENIUM_FALLBACK_USED += 1
    date_nodash = date_obj.strftime("%Y%m%d")
    report_url = (
        "https://www.twse.com.tw/announcement/notice"
        f"?response=html&startDate={date_nodash}&endDate={date_nodash}"
        "&stockNo=&querytype=1&selectType=&sortKind=STKNO"
    )

    driver = None
    try:
        print(f"TWSE {date_str} 啟動 Selenium 單日報表備援...")
        driver = get_driver()
        driver.set_page_load_timeout(40)
        driver.get(report_url)
        time.sleep(2)

        try:
            alert = driver.switch_to.alert
            alert_text = alert.text
            alert.accept()
            print(f"TWSE Selenium 備援遭官方提示阻擋：{alert_text}")
            return None
        except Exception:
            pass

        page_source = driver.page_source or ""
        blocked_texts = ["FOR SECURITY REASONS", "因為安全性考量", "請稍候再試"]
        if any(x in page_source for x in blocked_texts):
            print(f"TWSE {date_str} Selenium 備援仍遭安全機制阻擋。")
            return None

        table_rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        parsed_rows = []

        for tr in table_rows:
            cells = [_twse_clean_text(td.text) for td in tr.find_elements(By.TAG_NAME, "td")]
            if not cells:
                continue

            code_idx = None
            for idx, cell in enumerate(cells):
                if cell.isdigit() and len(cell) == 4:
                    code_idx = idx
                    break
            if code_idx is None:
                continue

            official_dates = [_twse_parse_any_date_to_ad_date(cell) for cell in cells]
            official_dates = [d for d in official_dates if d is not None]
            if official_dates and date_obj not in official_dates:
                continue

            code = cells[code_idx]
            name = cells[code_idx + 1] if code_idx + 1 < len(cells) else ""
            raw = " ".join(cells)
            ids = parse_clause_ids_strict(raw)
            clause_text = "、".join(f"第{x}款" for x in sorted(ids))

            parsed_rows.append({
                "日期": date_str,
                "市場": "TWSE",
                "代號": code,
                "名稱": name,
                "觸犯條款": clause_text,
            })

        if parsed_rows:
            parsed_rows = _twse_dedupe_attention_rows(parsed_rows)
            print(f"TWSE {date_str} Selenium 備援成功：{len(parsed_rows)} 筆")
            return parsed_rows

        # 頁面有正式表格但沒有 4 碼股票，視為合法空資料。
        if driver.find_elements(By.CSS_SELECTOR, "table"):
            print(f"TWSE {date_str} Selenium 報表查無 4 碼上市股票資料。")
            return []

        print(f"TWSE {date_str} Selenium 備援找不到官方報表表格。")
        return None

    except Exception as e:
        print(f"TWSE {date_str} Selenium 備援失敗：{type(e).__name__}: {e}")
        return None
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


def fetch_twse_attention_rows(date_obj, date_str):
    """抓取上市注意股；官方 OpenAPI、歷史報表端點與 Selenium 多層備援。

    ⚠️ 關鍵原則：任何來源回傳「0 筆」都不算抓取成功。

    交易日的上市注意股實務上不可能是 0 筆，出現 0 筆幾乎都代表
    「該來源尚未更新完成」——例如 18:15 執行時，OpenAPI 還停留在
    前一個交易日的資料，經日期過濾後全數被濾掉而回傳空陣列。

    舊版以 `if openapi_rows is not None` 判斷，空陣列 `[]` 也算成功並直接
    return，導致底下的官方 JSON 歷史報表與 Selenium 備援完全不會執行，
    當日上市注意股整批遺失，而 log 還印「抓取成功：0 筆」。
    這就是「當天抓的資料怪怪的、隔天重抓才正常」的原因。

    修正後行為：
      1. 只有「有抓到資料」才立即採用並回傳。
      2. 任何來源回傳 0 筆，一律繼續往下試下一個來源。
      3. 全部來源都回 0 筆時回傳 None（視為抓取失敗），
         讓 get_daily_data() 走「本輪不寫入狀態」的路徑，
         避免把「尚未更新」誤記成「當日無注意股」而污染計次。
    """
    date_str_nodash = date_obj.strftime("%Y%m%d")

    empty_sources = []   # 回傳 0 筆（疑似尚未更新）的來源
    errors = []          # 真正失敗（無回應或格式不明）的來源

    # ---- 第 1 層：官方 OpenAPI（僅適用於查詢當日）----
    openapi_rows = _fetch_twse_attention_openapi(date_obj, date_str)
    if openapi_rows:
        print(f"TWSE {date_str} OpenAPI 抓取成功：{len(openapi_rows)} 筆")
        return openapi_rows
    if openapi_rows is not None:
        print(f"TWSE {date_str} OpenAPI 回傳 0 筆（可能尚未更新），改試官方 JSON 歷史報表。")
        empty_sources.append("OpenAPI")

    # ---- 第 2 層：官方 JSON 歷史報表（多網域備援）----
    params = {
        "response": "json",
        "startDate": date_str_nodash,
        "endDate": date_str_nodash,
        "stockNo": "",
        "querytype": "1",
        "selectType": "",
        "sortKind": "STKNO",
    }

    endpoint_candidates = [
        (
            "https://www.twse.com.tw/rwd/zh/announcement/notice",
            "https://www.twse.com.tw/zh/announcement/notice.html",
        ),
        (
            "https://www.twse.com.tw/announcement/notice",
            "https://www.twse.com.tw/zh/announcement/notice.html",
        ),
        (
            "https://wwwc.twse.com.tw/rwd/zh/announcement/notice",
            "https://wwwc.twse.com.tw/zh/announcement/notice.html",
        ),
        (
            "https://wwwc.twse.com.tw/announcement/notice",
            "https://wwwc.twse.com.tw/zh/announcement/notice.html",
        ),
    ]

    for url, referer in endpoint_candidates:
        payload = _twse_request_json(
            url,
            params=params,
            referer=referer,
            label=f"TWSE {date_str}",
        )
        if payload is None:
            errors.append(url)
            continue

        parsed_rows = _twse_parse_notice_payload(payload, date_obj, date_str)

        if parsed_rows:
            print(f"TWSE {date_str} 官方 JSON 抓取成功：{len(parsed_rows)} 筆，端點={url}")
            return parsed_rows

        if parsed_rows is not None:
            # 0 筆不視為成功，繼續試下一個端點。
            print(f"TWSE {date_str} 端點回傳 0 筆（可能尚未更新）：{url}")
            empty_sources.append(url)
            continue

        print(f"TWSE {date_str} 端點回傳未知 JSON 格式：{url}")
        errors.append(url)

    # ---- 第 3 層：Selenium 直接開官方網頁 ----
    selenium_rows = _fetch_twse_attention_selenium(date_obj, date_str)
    if selenium_rows:
        return selenium_rows
    if selenium_rows is not None:
        print(f"TWSE {date_str} Selenium 備援回傳 0 筆（可能尚未更新）。")
        empty_sources.append("Selenium")

    # ---- 全部來源都拿不到資料 ----
    if empty_sources:
        print(
            f"⚠️ TWSE {date_str} 共 {len(empty_sources)} 個來源皆回傳 0 筆。"
            f"交易日不太可能真的沒有上市注意股，判定為『官方尚未更新』，"
            f"本輪不寫入該日資料，避免污染注意次數計算。"
            f"建議於證交所公告完成後（約 19:00 以後）重新執行。"
        )
        return None

    print(
        f"TWSE {date_str} 所有官方抓取方式均失敗，"
        f"已嘗試 {len(errors)} 個 JSON 端點與 Selenium 備援。"
    )
    return None


def _tpex_clean_text(s):
    if s is None:
        return ""
    s = str(s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = s.replace("&nbsp;", " ")
    s = s.replace("\u3000", " ")
    s = s.replace("\xa0", " ")
    s = s.replace("\r", " ")
    s = s.replace("\n", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _tpex_to_roc_slash(d):
    return f"{d.year - 1911}/{d.month:02d}/{d.day:02d}"


def _tpex_to_yyyymmdd(d):
    return d.strftime("%Y%m%d")


def _tpex_parse_any_date_to_ad_date(s):
    """將 TPEx 可能出現的公告日期格式轉成西元 date。"""
    if s is None:
        return None

    raw = _tpex_clean_text(s)
    if not raw:
        return None

    raw = raw.replace("年", "/").replace("月", "/").replace("日", "")
    raw = raw.replace("-", "/").strip()

    m = re.fullmatch(r"(\d{4})(\d{2})(\d{2})", raw)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            return None

    m = re.search(r"(\d{2,4})/(\d{1,2})/(\d{1,2})", raw)
    if m:
        try:
            y = int(m.group(1))
            mo = int(m.group(2))
            da = int(m.group(3))
            if y < 1911:
                y += 1911
            return date(y, mo, da)
        except Exception:
            return None

    return None


def _tpex_get_field_index(fields, keyword_list):
    if not fields:
        return None

    for idx, f in enumerate(fields):
        f_clean = _tpex_clean_text(f)
        for keyword in keyword_list:
            if keyword in f_clean:
                return idx

    return None


def _tpex_find_stock_code_index(cells):
    """從整列欄位中找出 4 碼股票代號；排除權證、可轉債等非 4 碼標的。"""
    for idx, cell in enumerate(cells):
        s = _tpex_clean_text(cell)
        if re.fullmatch(r"\d{4}", s):
            return idx
    return None


def _tpex_find_date_index(cells):
    for idx, cell in enumerate(cells):
        d = _tpex_parse_any_date_to_ad_date(cell)
        if d is not None:
            return idx, d
    return None, None


def _tpex_safe_get_cell(cells, idx):
    if idx is None:
        return ""
    try:
        idx = int(idx)
        if 0 <= idx < len(cells):
            return _tpex_clean_text(cells[idx])
    except Exception:
        pass
    return ""


def _tpex_extract_raw_items_from_json(data):
    raw_items = []

    if not isinstance(data, dict):
        return raw_items

    if "tables" in data:
        for table_idx, table in enumerate(data.get("tables", []) or []):
            fields = table.get("fields", []) or []
            table_data = table.get("data", []) or []
            for row_idx, row in enumerate(table_data):
                raw_items.append({
                    "table_idx": table_idx,
                    "row_idx": row_idx,
                    "fields": fields,
                    "row": row,
                })
    elif "data" in data:
        fields = data.get("fields", []) or []
        for row_idx, row in enumerate(data.get("data", []) or []):
            raw_items.append({
                "table_idx": 0,
                "row_idx": row_idx,
                "fields": fields,
                "row": row,
            })

    return raw_items


def _tpex_parse_rows_from_json(data, query_date_obj, date_str):
    """解析 TPEx JSON，並強制使用官方公告日期過濾。"""
    raw_items = _tpex_extract_raw_items_from_json(data)
    rows = []

    skipped_other_date = 0
    skipped_no_date = 0
    skipped_no_code = 0
    skipped_non_4_digit = 0

    for obj in raw_items:
        raw_row = obj.get("row", [])
        if not isinstance(raw_row, list):
            continue

        fields = [_tpex_clean_text(x) for x in obj.get("fields", []) or []]
        cells = [_tpex_clean_text(x) for x in raw_row]
        raw = " ".join(cells)

        code_idx = _tpex_get_field_index(fields, ["證券代號", "代號"])
        name_idx = _tpex_get_field_index(fields, ["證券名稱", "名稱"])
        clause_idx = _tpex_get_field_index(fields, ["注意交易資訊", "交易資訊"])
        date_idx = _tpex_get_field_index(fields, ["公告日期", "日期"])

        if code_idx is None:
            code_idx = _tpex_find_stock_code_index(cells)

        if name_idx is None and code_idx is not None:
            name_idx = code_idx + 1

        if date_idx is not None:
            official_date_raw = _tpex_safe_get_cell(cells, date_idx)
            official_date = _tpex_parse_any_date_to_ad_date(official_date_raw)
        else:
            found_date_idx, official_date = _tpex_find_date_index(cells)
            date_idx = found_date_idx

        if official_date is None:
            skipped_no_date += 1
            continue

        if official_date != query_date_obj:
            skipped_other_date += 1
            continue

        if code_idx is None:
            skipped_no_code += 1
            continue

        code = _tpex_safe_get_cell(cells, code_idx)
        name = _tpex_safe_get_cell(cells, name_idx)

        if not (code.isdigit() and len(code) == 4):
            skipped_non_4_digit += 1
            continue

        clause_source_text = _tpex_safe_get_cell(cells, clause_idx) if clause_idx is not None else raw
        ids = parse_clause_ids_strict(clause_source_text)
        c_str = "、".join([f"第{k}款" for k in sorted(ids)]) if ids else ""

        rows.append({
            "日期": date_str,
            "市場": "TPEx",
            "代號": code,
            "名稱": name,
            "觸犯條款": c_str,
        })

    debug = {
        "raw_items": len(raw_items),
        "保留筆數": len(rows),
        "略過_非查詢日期": skipped_other_date,
        "略過_無日期": skipped_no_date,
        "略過_無代號": skipped_no_code,
        "略過_非4碼": skipped_non_4_digit,
    }
    return rows, debug


def _tpex_dedupe_attention_rows(rows):
    seen = set()
    out = []

    for r in rows:
        key = (
            str(r.get("日期", "")).strip(),
            str(r.get("市場", "")).strip(),
            str(r.get("代號", "")).strip(),
            str(r.get("觸犯條款", "")).strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)

    return out


def fetch_tpex_attention_rows(date_obj, date_str):
    """抓取 TPEx 上櫃注意股公告。

    修正重點：
    1. 不再使用舊版 date 參數。
    2. 改用 startDate / endDate 查詢單日資料。
    3. 不再固定假設代號、名稱、日期欄位位置。
    4. 以官方回傳的公告日期過濾，避免同一批資料被套到不同日期。
    """
    roc_date = _tpex_to_roc_slash(date_obj)
    yyyymmdd = _tpex_to_yyyymmdd(date_obj)
    url = "https://www.tpex.org.tw/www/zh-tw/bulletin/attention"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.tpex.org.tw/www/zh-tw/bulletin/attention",
        "Origin": "https://www.tpex.org.tw",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }

    payloads = [
        {"startDate": roc_date, "endDate": roc_date, "response": "json"},
        {"startDate": yyyymmdd, "endDate": yyyymmdd, "response": "json"},
        {"startDate": roc_date, "endDate": roc_date, "type": "all", "response": "json"},
        {"startDate": yyyymmdd, "endDate": yyyymmdd, "type": "all", "response": "json"},
    ]

    s = requests.Session()

    try:
        s.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    except Exception as e:
        print(f"TPEx 初始化 Cookie 失敗：{type(e).__name__}: {e}")

    errors = []
    got_valid_json_response = False

    for payload in payloads:
        for method in ["POST", "GET"]:
            for attempt in range(1, 4):
                try:
                    if method == "POST":
                        r = s.post(url, data=payload, headers=headers, timeout=12)
                    else:
                        r = s.get(url, params=payload, headers=headers, timeout=12)

                    if r.status_code != 200:
                        errors.append(f"{method} HTTP {r.status_code}, payload={payload}")
                        time.sleep(0.8)
                        continue

                    try:
                        res = r.json()
                    except Exception as e:
                        errors.append(f"{method} JSON 解析失敗 {type(e).__name__}: {e}, payload={payload}")
                        time.sleep(0.8)
                        continue

                    got_valid_json_response = True
                    rows, debug = _tpex_parse_rows_from_json(res, date_obj, date_str)

                    if rows:
                        rows = _tpex_dedupe_attention_rows(rows)
                        print(f"TPEx {date_str} 抓取成功：{len(rows)} 筆，debug={debug}")
                        return rows

                    errors.append(f"{method} 無查詢日資料，debug={debug}, payload={payload}")
                    time.sleep(0.5)

                except Exception as e:
                    errors.append(f"{method} 例外 {type(e).__name__}: {e}, payload={payload}")
                    time.sleep(0.8)

    if got_valid_json_response:
        print(f"TPEx {date_str} 查無 4 碼上櫃注意股資料；最後狀態：" + "；".join(errors[-3:]))
        return []

    print("TPEx 三次重試皆失敗，最後錯誤：" + "；".join(errors[-6:]))
    return None
def get_daily_data(date_obj):
    date_str = date_obj.strftime("%Y-%m-%d")

    if date_str in _DAILY_NOTICE_CACHE:
        cached_rows = [dict(row) for row in _DAILY_NOTICE_CACHE[date_str]]
        print(f"使用本輪公告快取 {date_str}：{len(cached_rows)} 筆")
        return cached_rows

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

    # 僅在上市、上櫃都成功時建立同輪快取，避免把半套資料當完整資料。
    _DAILY_NOTICE_CACHE[date_str] = [dict(row) for row in rows]

    if rows:
        print(f"抓到 {len(rows)} 檔")
    else:
        print("無資料")
    return rows


def build_fresh_stats_clause_map(cal_dates, target_trade_date_obj, lookback_days=30):
    """重新抓取近 lookback_days 個交易日官方注意股公告，供近30日熱門統計使用。

    目的：近30日熱門統計的「30日狀態碼 / 10日狀態碼 / 注意次數」
    不再依賴 Google Sheet「每日紀錄」中可能已經存在的舊資料或舊條款，
    而是在每次執行時重新抓取官方公告後即時計算。
    """
    stats_dates = [d for d in cal_dates if d <= target_trade_date_obj][-lookback_days:]
    fresh_clause_map = {}
    fresh_name_map = {}

    print(f"重新抓取近{len(stats_dates)}個交易日公告，重建近30日熱門統計狀態碼...")
    for d in stats_dates:
        d_str = d.strftime("%Y-%m-%d")
        rows = get_daily_data(d)
        if rows is None:
            raise RuntimeError(
                f"關鍵統計日 {d_str} 官方注意股公告重新抓取失敗，停止更新近30日熱門統計，避免沿用舊錯誤資料。"
            )

        for s in rows:
            code = str(s.get('代號', '')).replace("'", "").strip()
            if not code:
                continue
            row_date = str(s.get('日期', d_str)).strip() or d_str
            clause = str(s.get('觸犯條款', '')).strip()
            key = (code, row_date)
            fresh_clause_map[key] = merge_clause_text(fresh_clause_map.get(key, ""), clause)

            name = str(s.get('名稱', '')).strip()
            if name:
                fresh_name_map[code] = name

        time.sleep(0.25)

    print(f"近30日熱門統計官方公告重建完成：{len(fresh_clause_map)} 筆股票日期紀錄")
    return fresh_clause_map, fresh_name_map, stats_dates

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

            # 若「爬取狀態」已有官方筆數，且「每日紀錄」筆數不少於該值，
            # 代表這一天先前已成功完整抓取。官方暫時阻擋時可安全沿用，
            # 不應因最近兩日的強制驗證失敗而讓整個流程中止。
            has_verified_existing_data = (
                st_cnt is not None
                and log_cnt >= int(st_cnt)
            )

            if has_verified_existing_data:
                print(
                    f"{d_str} 官方網站暫時無法存取；"
                    f"既有每日紀錄 {log_cnt} 筆、已驗證官方筆數 {int(st_cnt)} 筆，"
                    "本次沿用既有完整資料，不更新爬取狀態。"
                )
                continue

            # 最近交易日若連既有完整資料都沒有，仍維持原本的安全機制：
            # 停止後續統計，避免用缺漏資料推播。
            if d in recent_dates or d == target_trade_date_obj:
                raise RuntimeError(
                    f"關鍵交易日 {d_str} 公告抓取失敗，且沒有已驗證的完整既有資料，"
                    "已停止後續統計更新，避免錯誤資料被推播。"
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


def refresh_recent_daily_log_clauses(ws_log, cal_dates, target_trade_date_obj):
    """校正最近 N 個交易日「每日紀錄」的觸犯條款，並補回漏寫的官方注意股。

    這個函式只處理 Google Sheet「每日紀錄」：
    1. 重新抓取最近 REFRESH_DAILY_LOG_CLAUSES_DAYS 個交易日官方注意股公告。
    2. 若新版解析得到乾淨款別，則更新既有列的「觸犯條款」。
    3. 若官方有資料但每日紀錄缺少該日期 / 代號，則補新增列。
    4. 若官方網站某天抓取失敗，直接跳過該日，不覆蓋舊資料。
    5. 若新版解析結果為空，絕不覆蓋既有條款，避免把好資料清空。
    """
    try:
        days = int(REFRESH_DAILY_LOG_CLAUSES_DAYS)
    except:
        days = 0

    if days <= 0:
        print("每日紀錄觸犯條款校正：未啟用。")
        return

    refresh_dates = [d for d in cal_dates if d <= target_trade_date_obj][-days:]
    if not refresh_dates:
        print("每日紀錄觸犯條款校正：沒有可校正的交易日。")
        return

    print(
        f"每日紀錄觸犯條款校正啟用：重新檢查最近 {len(refresh_dates)} 個交易日 "
        f"({refresh_dates[0].strftime('%Y-%m-%d')} ~ {refresh_dates[-1].strftime('%Y-%m-%d')})"
    )

    all_values = ws_log.get_all_values()
    key_to_row = {}
    key_to_clause = {}

    for row_num, row in enumerate(all_values[1:], start=2):
        if len(row) < 3:
            continue
        d_str = str(row[0]).strip()
        code = str(row[2]).strip().replace("'", "")
        if not d_str or not code:
            continue
        key = (d_str, code)
        if key not in key_to_row:
            key_to_row[key] = row_num
            key_to_clause[key] = str(row[4]).strip() if len(row) >= 5 else ""

    updates = []
    rows_to_append = []
    skipped_dates = []

    for d in refresh_dates:
        d_str = d.strftime("%Y-%m-%d")
        if d == TARGET_DATE.date() and TARGET_DATE.time() < SAFE_CRAWL_TIME:
            continue

        data = get_daily_data(d)
        if data is None:
            print(f"每日紀錄觸犯條款校正：{d_str} 官方公告抓取失敗，跳過該日，不覆蓋舊資料。")
            skipped_dates.append(d_str)
            continue

        official_map = {}
        info_map = {}

        for s in data:
            code = str(s.get('代號', '')).strip().replace("'", "")
            if not code:
                continue
            row_date = str(s.get('日期', d_str)).strip() or d_str
            market = str(s.get('市場', '')).strip()
            name = str(s.get('名稱', '')).strip()
            new_clause = str(s.get('觸犯條款', '')).strip()

            key = (row_date, code)
            new_ids = parse_clause_ids_strict(new_clause)

            # 關鍵保護：
            # 1. 新解析有乾淨款別 → 才更新或補新增。
            # 2. 新解析失敗 → 不新增；若既有列是舊污染長文字，才清成空字串。
            # 3. 新解析失敗但既有列本來就是乾淨「第N款」格式 → 保留，不覆蓋。
            if not new_clause or not new_ids:
                if key in key_to_row:
                    old_clause = key_to_clause.get(key, "")
                    if old_clause and (not is_clean_clause_text(old_clause)):
                        row_num = key_to_row[key]
                        updates.append({"range": f"E{row_num}:E{row_num}", "values": [[""]]})
                        key_to_clause[key] = ""
                continue

            official_map[key] = merge_clause_text(official_map.get(key, ""), new_clause)
            info_map[key] = (row_date, market, code, name)

        for key, new_clause in official_map.items():
            if not new_clause:
                continue
            if not parse_clause_ids_strict(new_clause):
                continue

            if key in key_to_row:
                row_num = key_to_row[key]
                old_clause = key_to_clause.get(key, "")
                if str(old_clause).strip() != str(new_clause).strip():
                    updates.append({"range": f"E{row_num}:E{row_num}", "values": [[new_clause]]})
            else:
                row_date, market, code, name = info_map.get(key, (key[0], "", key[1], ""))
                rows_to_append.append([row_date, market, f"'{code}", name, new_clause])
                key_to_row[key] = -1
                key_to_clause[key] = new_clause

        time.sleep(0.25)

    if updates:
        print(f"每日紀錄觸犯條款校正：準備更新 {len(updates)} 筆既有條款。")
        for i in range(0, len(updates), 100):
            chunk = updates[i:i+100]
            ws_log.batch_update(chunk, value_input_option="USER_ENTERED")
            if i + 100 < len(updates):
                time.sleep(0.8)
    else:
        print("每日紀錄觸犯條款校正：沒有既有條款需要更新。")

    if rows_to_append:
        print(f"每日紀錄觸犯條款校正：補新增漏寫注意股 {len(rows_to_append)} 筆。")
        ws_log.append_rows(rows_to_append, value_input_option="USER_ENTERED")
    else:
        print("每日紀錄觸犯條款校正：沒有漏寫注意股需要補新增。")

    if skipped_dates:
        print("每日紀錄觸犯條款校正：以下日期因官方抓取失敗而跳過，不影響其他日期：" + ", ".join(skipped_dates))

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
    """抓取技術追蹤用股價資料；以 Yahoo Finance 日線資料計算 MA20。"""
    code = str(code).replace("'", "").strip()
    suffix = get_ticker_suffix(market)
    primary_ticker = f"{code}{suffix}"
    fallback_suffix = ".TWO" if suffix == ".TW" else ".TW"
    fallback_ticker = f"{code}{fallback_suffix}"
    source_label = f"Yahoo:{primary_ticker}"

    def _clean_yahoo_history(raw_df):
        if raw_df is None or raw_df.empty:
            return pd.DataFrame()

        df = raw_df.copy()

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

        if getattr(df.index, 'tz', None) is not None:
            df.index = df.index.tz_localize(None)

        required_cols = ['Open', 'High', 'Low', 'Close']
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            return pd.DataFrame()

        for col in required_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        if 'Volume' in df.columns:
            df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
        else:
            df['Volume'] = 0.0

        df = df.dropna(subset=['Open', 'Close', 'Low']).copy()
        df = df.sort_index()
        return df

    for ticker in [primary_ticker, fallback_ticker]:
        try:
            source_label = f"Yahoo:{ticker}"
            raw_df = yf.Ticker(ticker).history(
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                auto_adjust=False
            )
            df = _clean_yahoo_history(raw_df)
            if not df.empty:
                return df, source_label
            print(f"技術追蹤 Yahoo 無可用股價資料 ({source_label})")
        except Exception as e:
            print(f"技術追蹤 Yahoo 股價抓取失敗 ({source_label}): {e}")

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
    df['LOW_MA20_GAP_PCT'] = ((df['Low'] - df['MA20']) / df['MA20']) * 100

    pre_10_first = pre_df.tail(10).iloc[0]
    pre_10_open = float(pre_10_first['Open'])
    pre_10_close = float(pre_10_first['Close'])
    pre_10_base = min(pre_10_open, pre_10_close)
    pre_last_close = float(pre_df['Close'].iloc[-1])
    pre_10d_pct = ((pre_last_close - pre_10_base) / pre_10_base) * 100 if pre_10_base > 0 else 0.0

    current_price = float(df['Close'].iloc[-1])
    current_low = float(df['Low'].iloc[-1])
    ma20 = float(df['MA20'].iloc[-1]) if not pd.isna(df['MA20'].iloc[-1]) else 0.0
    ma20_gap_pct = ((current_price - ma20) / ma20) * 100 if ma20 > 0 else 0.0
    current_low_ma20_gap_pct = ((current_low - ma20) / ma20) * 100 if ma20 > 0 else 0.0

    pre_rise_ok = pre_10d_pct >= TECH_PRE_10D_RISE_THRESHOLD
    current_close_retest_ok = abs(ma20_gap_pct) <= TECH_MA20_GAP_THRESHOLD
    current_low_retest_ok = abs(current_low_ma20_gap_pct) <= TECH_MA20_GAP_THRESHOLD
    current_retest_ok = pre_rise_ok and (current_low_retest_ok or current_close_retest_ok)

    jail_df = df[(df.index.date >= sd) & (df.index.date <= ed)].copy()
    jail_df = jail_df.dropna(subset=['MA20', 'MA20_GAP_PCT', 'LOW_MA20_GAP_PCT'])
    retest_df = jail_df[
        (jail_df['LOW_MA20_GAP_PCT'].abs() <= TECH_MA20_GAP_THRESHOLD) |
        (jail_df['MA20_GAP_PCT'].abs() <= TECH_MA20_GAP_THRESHOLD)
    ]
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
            f"於 {retest_date_str} 處置期間內盤中低點或收盤價回測 MA20；"
            f"目前收盤距離 MA20 {ma20_gap_pct:+.2f}% (>=+{TECH_BREAKOUT_MA20_GAP_THRESHOLD:.0f}%)"
        )
    elif current_retest_ok:
        signal_status = "目前回測月線"
        reason_text = (
            f"已達成「目前回測月線」｜"
            f"處置前10日漲幅 {pre_10d_pct:+.2f}% (>={TECH_PRE_10D_RISE_THRESHOLD:.0f}%)；"
            f"今日低點距離 MA20 {current_low_ma20_gap_pct:+.2f}%；"
            f"今日收盤距離 MA20 {ma20_gap_pct:+.2f}% (任一在 +/-{TECH_MA20_GAP_THRESHOLD:.0f}% 內)"
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
            if current_low_retest_ok or current_close_retest_ok:
                parts.append(
                    f"O「目前回測月線」成立 (今日低點距離 MA20 {current_low_ma20_gap_pct:+.2f}%，"
                    f"今日收盤距離 MA20 {ma20_gap_pct:+.2f}%)"
                )
            else:
                parts.append(
                    f"X「目前回測月線」不成立 (今日低點距離 MA20 {current_low_ma20_gap_pct:+.2f}%，"
                    f"今日收盤距離 MA20 {ma20_gap_pct:+.2f}%，皆超出 +/-{TECH_MA20_GAP_THRESHOLD:.0f}% 範圍)"
                )
            if not has_retested_ma20:
                parts.append(
                    f"X「回測後轉強」不成立 (處置期間內尚未出現盤中低點或收盤價回測 MA20，"
                    f"今日低點距離 {current_low_ma20_gap_pct:+.2f}%，今日收盤距離 {ma20_gap_pct:+.2f}%)"
                )
            elif ma20_gap_pct < TECH_BREAKOUT_MA20_GAP_THRESHOLD:
                parts.append(
                    f"X「回測後轉強」不成立 ({retest_date_str} 已於處置期間內由盤中低點或收盤價回測 MA20，"
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
    base_row[15] = _safe_round(pre_10_base, 2)
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

def get_clause11_gap_threshold(close_price):
    """第11款：依當日收盤價換算「最近6個營業日收盤價起迄價差」門檻。

    依 115.08.10 施行之新制：
      - 收盤價未逾 1,000 元 → 不適用本款，回傳 None。
      - 1,000 元 < 收盤價 <= 2,000 元 → 300 元。
      - 收盤價逾 2,000 元 → 每 1,000 元為一級距，每級距加 150 元
        (2,000~3,000 → 450；3,000~4,000 → 600，依此類推)。
    """
    if close_price is None:
        return None
    try:
        price = float(close_price)
    except (TypeError, ValueError):
        return None

    if price <= CLAUSE11_MIN_PRICE:
        return None

    first_tier_top = CLAUSE11_MIN_PRICE + CLAUSE11_TIER_SIZE   # 2,000 元
    if price <= first_tier_top:
        return CLAUSE11_BASE_GAP

    # 逾 2,000 元後每滿一個級距 (1,000 元) 增加一階，邊界值 (如 3,000) 仍屬前一階。
    tier = int((price - first_tier_top - 1e-9) // CLAUSE11_TIER_SIZE) + 1
    return CLAUSE11_BASE_GAP + tier * CLAUSE11_TIER_STEP


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

    # 【第十一款】最近6個營業日收盤價起迄價差 (115.08.10 新制)
    # 法規文義為「起迄兩個營業日之收盤價價差」，且當日收盤價須為該期間之最高或最低，
    # 故與第一款共用同一組 6 個營業日起迄收盤價 (ref_6 → curr_close)。
    clause11_threshold = get_clause11_gap_threshold(curr_close)
    if clause11_threshold is not None and len(hist_df) >= 7:
        closes_7 = hist_df['Close'].iloc[-7:].astype(float)
        is_extreme = (curr_close >= closes_7.max()) or (curr_close <= closes_7.min())
        if price_diff_6 >= clause11_threshold and is_extreme:
            triggers.append(
                f"【第十一款】6日收盤起迄價差{price_diff_6:.0f}元(>=門檻{clause11_threshold:.0f})"
            )

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
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36")

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

def _twse_parse_jail_payload(payload):
    """解析 TWSE 處置股歷史報表 JSON 或 OpenAPI JSON。"""
    clean_data = []

    if isinstance(payload, dict) and "data" in payload:
        raw_data = payload.get("data", []) or []
        fields = [_twse_clean_text(x) for x in payload.get("fields", []) or []]
        stat_text = _twse_clean_text(payload.get("stat", ""))

        if not isinstance(raw_data, list):
            return None

        # 少數官方服務可能把 OpenAPI 的 list[dict] 包在 data 欄位內。
        if raw_data and all(isinstance(item, dict) for item in raw_data):
            return _twse_parse_jail_payload(raw_data)

        if not raw_data:
            if (not stat_text) or ("OK" in stat_text.upper()) or ("沒有" in stat_text) or ("查無" in stat_text):
                return []

        code_idx = _twse_find_field_index(fields, ["證券代號", "有價證券代號", "股票代號"])
        name_idx = _twse_find_field_index(fields, ["證券名稱", "有價證券名稱", "股票名稱"])
        period_idx = _twse_find_field_index(fields, ["處置起迄時間", "處置期間", "處置起迄日期"])

        if code_idx is None:
            code_idx = 2
        if name_idx is None:
            name_idx = 3
        if period_idx is None:
            period_idx = 6

        for item in raw_data:
            if not isinstance(item, list):
                continue
            if max(code_idx, name_idx, period_idx) >= len(item):
                continue

            code = _twse_clean_text(item[code_idx])
            name = _twse_clean_text(item[name_idx])
            period = _twse_clean_text(item[period_idx])

            if code.isdigit() and len(code) == 4 and period:
                clean_data.append({
                    "Code": code,
                    "Name": name,
                    "Period": period,
                    "Market": "上市",
                })

        return clean_data

    if isinstance(payload, list):
        if not payload:
            return []

        recognized = False
        for item in payload:
            if not isinstance(item, dict):
                continue
            recognized = True
            values = [_twse_clean_text(v) for v in item.values()]

            code = ""
            for key in ["證券代號", "有價證券代號", "股票代號", "Code", "code", "SecuritiesCode"]:
                candidate = _twse_clean_text(item.get(key, ""))
                if candidate.isdigit() and len(candidate) == 4:
                    code = candidate
                    break
            if not code:
                for candidate in values:
                    if candidate.isdigit() and len(candidate) == 4:
                        code = candidate
                        break
            if not code:
                continue

            name = ""
            for key in ["證券名稱", "有價證券名稱", "股票名稱", "Name", "name", "SecuritiesName"]:
                candidate = _twse_clean_text(item.get(key, ""))
                if candidate:
                    name = candidate
                    break

            period = ""
            for key in ["處置起迄時間", "處置期間", "處置起迄日期", "Period", "period"]:
                candidate = _twse_clean_text(item.get(key, ""))
                if candidate:
                    period = candidate
                    break
            if not period:
                for candidate in values:
                    if ("~" in candidate or "～" in candidate or "至" in candidate) and "/" in candidate:
                        period = candidate
                        break

            if period:
                clean_data.append({
                    "Code": code,
                    "Name": name,
                    "Period": period,
                    "Market": "上市",
                })

        if recognized:
            return clean_data

    return None


def _fetch_twse_jail_chunk_requests(s_date, e_date):
    sd_str = s_date.strftime("%Y%m%d")
    ed_str = e_date.strftime("%Y%m%d")
    params = {
        "response": "json",
        "startDate": sd_str,
        "endDate": ed_str,
        "stockNo": "",
        "selectType": "",
        "proceType": "",
        "remarkType": "",
        "sortKind": "",
        "querytype": "",
    }

    endpoint_candidates = [
        (
            "https://www.twse.com.tw/announcement/punish",
            "https://www.twse.com.tw/zh/announcement/punish.html",
        ),
        (
            "https://www.twse.com.tw/rwd/zh/announcement/punish",
            "https://www.twse.com.tw/zh/announcement/punish.html",
        ),
        (
            "https://wwwc.twse.com.tw/announcement/punish",
            "https://wwwc.twse.com.tw/zh/announcement/punish.html",
        ),
        (
            "https://wwwc.twse.com.tw/rwd/zh/announcement/punish",
            "https://wwwc.twse.com.tw/zh/announcement/punish.html",
        ),
    ]

    for url, referer in endpoint_candidates:
        payload = _twse_request_json(
            url,
            params=params,
            referer=referer,
            label=f"TWSE 處置 {s_date}~{e_date}",
        )
        if payload is None:
            continue

        parsed = _twse_parse_jail_payload(payload)
        if parsed is not None:
            print(
                f"    TWSE 官方 JSON {s_date} ~ {e_date}："
                f"{len(parsed)} 筆，端點={url}"
            )
            return parsed

    return None


def _fetch_twse_jail_openapi():
    payload = _twse_request_json(
        "https://openapi.twse.com.tw/v1/announcement/punish",
        params={},
        referer="https://openapi.twse.com.tw/",
        label="TWSE 處置 OpenAPI",
    )
    if payload is None:
        return []

    parsed = _twse_parse_jail_payload(payload)
    if parsed is None:
        print("    TWSE 處置 OpenAPI 回傳未知格式，略過。")
        return []

    print(f"    TWSE 處置 OpenAPI 備援取得 {len(parsed)} 筆。")
    return parsed


def fetch_twse_selenium_90d(s_date, e_date):
    """TWSE 處置股瀏覽器備援：直接開官方 HTML 報表，避免點擊表單觸發 Alert。"""
    print(f"  [上市] 啟動 Selenium HTML 報表備援... {s_date} ~ {e_date}")

    sd_str = s_date.strftime("%Y%m%d")
    ed_str = e_date.strftime("%Y%m%d")
    report_url = (
        "https://www.twse.com.tw/announcement/punish"
        f"?response=html&startDate={sd_str}&endDate={ed_str}"
        "&stockNo=&selectType=&proceType=&remarkType=&sortKind=&querytype="
    )

    driver = None
    clean_data = []

    try:
        driver = get_driver()
        driver.set_page_load_timeout(45)
        driver.get(report_url)
        time.sleep(3)

        try:
            alert = driver.switch_to.alert
            alert_text = alert.text
            alert.accept()
            print(f"    TWSE Selenium 官方提示：{alert_text}")
            return pd.DataFrame()
        except Exception:
            pass

        page_source = driver.page_source or ""
        if any(x in page_source for x in ["FOR SECURITY REASONS", "因為安全性考量", "請稍候再試"]):
            print("    TWSE Selenium HTML 報表仍遭安全機制阻擋。")
            return pd.DataFrame()

        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        print(f"    Selenium 報表偵測到 {len(rows)} 列，開始解析...")

        for row in rows:
            try:
                cells = [_twse_clean_text(td.text) for td in row.find_elements(By.TAG_NAME, "td")]
                if not cells:
                    continue

                code_idx = None
                for idx, cell in enumerate(cells):
                    if cell.isdigit() and len(cell) == 4:
                        code_idx = idx
                        break
                if code_idx is None:
                    continue

                code = cells[code_idx]
                name = cells[code_idx + 1] if code_idx + 1 < len(cells) else ""

                period = ""
                for cell in cells:
                    sd, ed = parse_jail_period(cell)
                    if sd and ed:
                        period = cell
                        break

                if code and period:
                    clean_data.append({
                        "Code": code,
                        "Name": name,
                        "Period": period,
                        "Market": "上市",
                    })
            except Exception:
                continue

    except Exception as e:
        print(f"    TWSE Selenium HTML 報表失敗: {type(e).__name__}: {e}")
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

    if clean_data:
        df = pd.DataFrame(clean_data).drop_duplicates(subset=["Code", "Period"])
        print(f"    Selenium 備援成功解析 {len(df)} 筆資料")
        return df

    print("    TWSE Selenium 備援無資料")
    return pd.DataFrame()


def fetch_twse_jail_90d_requests(s_date, e_date):
    """TWSE 處置股主流程：官方 JSON 分段查詢，失敗區段再用 Selenium 備援。"""
    print(f"  [上市] 啟動官方 JSON 分段爬蟲... {s_date} ~ {e_date}")

    clean_data = []
    failed_chunks = []
    chunk_start = s_date

    # 官方報表以約一個月為一段，避免一次查半年觸發「請稍候再試」。
    while chunk_start <= e_date:
        chunk_end = min(chunk_start + timedelta(days=29), e_date)
        chunk_rows = _fetch_twse_jail_chunk_requests(chunk_start, chunk_end)

        if chunk_rows is None:
            failed_chunks.append((chunk_start, chunk_end))
        else:
            clean_data.extend(chunk_rows)

        chunk_start = chunk_end + timedelta(days=1)

    # 僅針對 JSON 失敗區段啟動瀏覽器，不再用 Selenium 一次查半年。
    for failed_start, failed_end in failed_chunks:
        df_fallback = fetch_twse_selenium_90d(failed_start, failed_end)
        if not df_fallback.empty:
            clean_data.extend(df_fallback.to_dict("records"))

    # OpenAPI 再補目前官方公布的處置資料，避免近期新公告剛好落在失敗區段。
    if failed_chunks:
        clean_data.extend(_fetch_twse_jail_openapi())

    if clean_data:
        df = pd.DataFrame(clean_data)
        df = df.drop_duplicates(subset=["Code", "Period"]).reset_index(drop=True)
        print(
            f"    TWSE 處置股共取得 {len(df)} 筆；"
            f"JSON 失敗區段 {len(failed_chunks)} 段。"
        )
        return df

    if failed_chunks:
        print(
            f"    TWSE 處置股所有抓取方式皆無法取得資料；"
            f"失敗區段 {len(failed_chunks)} 段。"
        )
    else:
        print("    TWSE 查詢區間內無 4 碼上市處置股票。")

    return pd.DataFrame()


def run_jail_crawler_pipeline_sync():
    end_date = TARGET_DATE.date() + timedelta(days=30)
    start_date = TARGET_DATE.date() - timedelta(days=150)

    print(f"啟動全市場處置股抓取 (TWSE: 官方 JSON + Selenium 備援 / TPEx: Requests)")
    print(f"搜尋範圍 (含未來預告): {start_date} ~ {end_date}")

    df_tpex = fetch_tpex_jail_90d_requests(start_date, end_date)
    df_twse = fetch_twse_jail_90d_requests(start_date, end_date)

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
    all_jail_data = []

    try:
        df_jail_90 = run_jail_crawler_pipeline_sync()

        sheet_title = "處置股90日明細"
        export_cols = ["市場", "代號", "名稱", "處置期間"]
        ws_jail = get_or_create_ws(sh, sheet_title, headers=export_cols)

        if not df_jail_90.empty:
            df_jail_unique = df_jail_90.drop_duplicates(subset=["代號", "處置期間"])
            print(f"正在寫入 Google Sheet: {sheet_title} (新增 + 期間更新模式)...")

            # ===========================================================
            # 為什麼要有「期間更新」：
            #   本表原為只增不改，證交所若提前解除或變更處置期間 (例如
            #   115.08.10 新制上路時，已滿新制天數者當日即解除)，舊的長期間
            #   會留在表裡，而下游 stock_latest_end 與 notify_discord 的
            #   合併邏輯都取「結束日最晚」那筆 -> 選到過期資料，
            #   造成已出關的股票仍顯示為處置中。
            #
            # 判定方式：
            #   提前解除不會更動處置起始日，只會把結束日往前挪，
            #   故以 (代號, 處置起始日) 視為同一次處置，用最新公告覆寫該列。
            #
            # 安全性：
            #   仍然只處理「本次實際爬到」的資料。爬蟲失敗 (df 為空) 或
            #   只成功一半時，未爬到的列完全不動，不會被洗掉。
            # ===========================================================
            existing_rows = ws_jail.get_all_values()
            existing_keys = set()      # "代號_期間"，完全相同者略過
            existing_by_start = {}     # (代號, 起始日) -> (試算表列號, 期間字串)
            if len(existing_rows) > 1:
                for row_idx, r in enumerate(existing_rows[1:], start=2):
                    if len(r) < 4:
                        continue
                    r_code = str(r[1]).strip()
                    r_period = str(r[3]).strip()
                    if not r_code or not r_period:
                        continue
                    existing_keys.add(f"{r_code}_{r_period}")
                    r_sd, _ = parse_jail_period(r_period)
                    if r_sd:
                        existing_by_start[(r_code, r_sd)] = (row_idx, r_period)

            # 過渡換算用的交易日曆：需涵蓋新制施行日之前的處置起始日。
            transition_cal_dates = get_trading_calendar_between(
                TARGET_DATE.date() - timedelta(days=150),
                TARGET_DATE.date() + timedelta(days=90),
            )

            rows_to_append = []
            cells_to_update = []
            queued_row_idx = set()
            new_count = 0
            updated_count = 0
            for idx, row in df_jail_unique.iterrows():
                code = str(row["代號"]).strip()
                period = str(row["處置期間"]).strip()

                # 施行日前開始的處置，證交所歷史報表仍掛原公告期間，
                # 這裡依過渡規定自行換算為實際結束日，寫入表中供下游使用。
                raw_sd, raw_ed = parse_jail_period(period)
                adj_ed = apply_disposal_transition_rule(raw_sd, raw_ed, transition_cal_dates)
                if raw_sd and adj_ed and raw_ed and adj_ed != raw_ed:
                    period = f"{format_roc_date_for_display(raw_sd)}~{format_roc_date_for_display(adj_ed)}"

                check_key = f"{code}_{period}"

                if check_key in existing_keys:
                    continue

                sd, _ = parse_jail_period(period)
                old = existing_by_start.get((code, sd)) if sd else None

                if old and old[0] is not None:
                    # 同一次處置但期間已變更 -> 覆寫原列的「處置期間」欄 (D 欄)
                    old_row_idx, old_period = old
                    cells_to_update.append({"range": f"D{old_row_idx}", "values": [[period]]})
                    queued_row_idx.add(old_row_idx)
                    print(f"    處置期間更新：{code} {old_period} -> {period}")
                    existing_keys.discard(f"{code}_{old_period}")
                    existing_keys.add(check_key)
                    existing_by_start[(code, sd)] = (old_row_idx, period)
                    updated_count += 1
                else:
                    rows_to_append.append([row["市場"], code, row["名稱"], period])
                    existing_keys.add(check_key)
                    new_count += 1

            # 補掃既有列：證交所歷史報表若已不再回傳某筆已結束的處置，
            # 上面的迴圈就碰不到它，過期的舊制期間會一直留著。
            # 這裡直接對表中所有列再套一次過渡換算，確保不漏。
            for row_idx, r in enumerate(existing_rows[1:], start=2):
                if row_idx in queued_row_idx or len(r) < 4:
                    continue
                r_code = str(r[1]).strip()
                r_period = str(r[3]).strip()
                if not r_code or not r_period:
                    continue
                r_sd, r_ed = parse_jail_period(r_period)
                r_adj = apply_disposal_transition_rule(r_sd, r_ed, transition_cal_dates)
                if r_sd and r_ed and r_adj and r_adj != r_ed:
                    fixed = f"{format_roc_date_for_display(r_sd)}~{format_roc_date_for_display(r_adj)}"
                    cells_to_update.append({"range": f"D{row_idx}", "values": [[fixed]]})
                    queued_row_idx.add(row_idx)
                    print(f"    過渡換算修正：{r_code} {r_period} -> {fixed}")
                    updated_count += 1

            if cells_to_update:
                ws_jail.batch_update(cells_to_update, value_input_option='USER_ENTERED')
            if rows_to_append:
                ws_jail.append_rows(rows_to_append, value_input_option='USER_ENTERED')

            if new_count or updated_count:
                print(f"{sheet_title} 更新完成！新增 {new_count} 筆，更新處置期間 {updated_count} 筆。")
            else:
                print(f"{sheet_title} 無需異動 (所有資料已是最新)。")
        else:
            print("查無新處置股資料，僅讀取現有紀錄。")

        print(f"重新讀取完整資料庫篩選即將出關股票 ({RELEASE_ALERT_TRADING_DAYS}日內)...")

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

            if 0 <= days_left <= RELEASE_ALERT_TRADING_DAYS - 1:
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
            ws_release.append_row([f"目前無 {RELEASE_ALERT_TRADING_DAYS} 日內即將出關股票"], value_input_option='USER_ENTERED')
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
    refresh_recent_daily_log_clauses(ws_log, cal_dates, target_trade_date_obj)

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

    safe_cal_dates = [d for d in cal_dates if d <= target_trade_date_obj]

    # ===========================================================
    # 近30日熱門統計資料來源固定為 Google Sheet「每日紀錄」
    # ===========================================================
    # 正確流程：
    #   1. 以「最終鎖定運算日期」target_trade_date_obj 為基準。
    #   2. 從交易日曆 safe_cal_dates 往回取最近 30 / 10 / 5 個交易日。
    #   3. 逐日回查 Google Sheet「每日紀錄」是否有該股票的有效注意股紀錄。
    #   4. 產生 30日狀態碼、10日狀態碼、注意次數與處置倒數。
    #
    # 重要：這裡不要使用處置股90日明細、jail_map、exclude_map 或重新爬取資料
    #       來決定注意次數；注意次數只由「每日紀錄 + 交易日曆」決定。
    recent_30_trade_dates = safe_cal_dates[-30:]
    recent_10_trade_dates = recent_30_trade_dates[-10:]
    recent_5_trade_dates = recent_30_trade_dates[-5:]

    print(
        "近30日熱門統計：使用 Google Sheet「每日紀錄」作為唯一統計來源，"
        f"統計區間 {recent_30_trade_dates[0].strftime('%Y-%m-%d')} ~ {recent_30_trade_dates[-1].strftime('%Y-%m-%d')}。"
    )

    jail_map = get_jail_map_from_sheet(sh)
    official_disposal_status_map = build_official_disposal_status_map_from_rows(
        all_jail_data,
        TARGET_DATE.date()
    )

    stats_date_set = {d.strftime("%Y-%m-%d") for d in recent_30_trade_dates}
    df_recent = df_log[df_log['日期'].isin(stats_date_set)]
    target_stocks = sorted(
        set(df_recent['代號'].unique())
        | set(official_disposal_status_map.keys())
    )

    precise_db = load_precise_db_from_sheet(sh)
    rows_stats = []

    print(f"掃描 {len(target_stocks)} 檔股票...")
    for idx, code in enumerate(target_stocks):
        code = str(code).replace("'", "").strip()
        official_disposal_status = official_disposal_status_map.get(code)

        if not df_log[df_log['代號']==code].empty:
            name = df_log[df_log['代號']==code]['名稱'].iloc[-1]
        elif official_disposal_status:
            name = official_disposal_status.get("name", "未知")
        else:
            name = "未知"

        db_info = precise_db.get(code, {})
        m_type = str(db_info.get('market', '上市')).upper()
        suffix = '.TWO' if any(k in m_type for k in ['上櫃', 'TWO', 'TPEX', 'OTC']) else '.TW'
        ticker_code = f"{code}{suffix}"

        # ===========================================================
        # [核心修正] 近30日熱門統計只依「每日紀錄 + 固定交易日窗」計算
        # ===========================================================
        # stock_calendar 固定為：從最終鎖定運算日期往回 30 個交易日。
        # 每一天都去每日紀錄查：
        #   - 有該股票，且條款屬於可累積處置的第1~8款 → 狀態碼記 1
        #   - 沒有該股票，或條款不是第1~8款 → 狀態碼記 0
        #
        # 但若該股票已有「過去已開始的處置區間」，真正不能重複計算的是
        # 已經被該次處置消耗的那一批注意紀錄。
        # 切分點應為「該次處置開始日前一個交易日」，不是處置結束日。
        # 因此處置期間內若每日紀錄仍有官方注意股公告，仍會納入新一輪累積。
        stock_calendar = recent_30_trade_dates
        used_attention_cutoff_date = get_consumed_attention_cutoff_date(
            code,
            target_trade_date_obj,
            jail_map,
            safe_cal_dates
        )

        bits = []
        clauses = []
        valid_bits = []
        for d in stock_calendar:
            d_str = d.strftime("%Y-%m-%d")
            c = clause_map.get((code, d_str), "")
            ids = parse_clause_ids_strict(c)
            is_valid_attention = bool(c) and is_valid_accumulation_day(ids)
            is_consumed_by_past_jail = bool(used_attention_cutoff_date and d <= used_attention_cutoff_date)

            if is_consumed_by_past_jail:
                bits.append(0)
                valid_bits.append(0)
                clauses.append("")
            else:
                bits.append(1 if is_valid_attention else 0)
                valid_bits.append(1 if is_valid_attention else 0)
                clauses.append(c if c else "")

        # ===========================================================
        # 處置倒數使用「排除已完成處置消耗紀錄後」的最近30個交易日狀態。
        # 這可以避免同一批注意紀錄先觸發前一次處置，出關後又被拿來湊
        # 30日12次、10日6次或連續3次第一款，造成重複處罰。
        # ===========================================================
        est_days, reason = simulate_days_to_jail_strict(
            bits, clauses,
            stock_id=code,
            target_date=target_trade_date_obj,
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
                # 併同當沖過高者處置 7 個營業日，其餘一般處置 5 個營業日 (115.08.10 新制)。
                reason_display += f" (含當沖過高，若進處置將關{DISPOSAL_DAYS_DAYTRADE}天)"

        # [官方處置狀態覆蓋]
        # 若「處置股90日明細」已經公告未來處置或目前正在處置，
        # 近30日熱門統計的顯示狀態必須以官方公告為準；
        # 注意次數與狀態碼仍保留每日紀錄計算結果，方便追蹤觸發來源。
        if official_disposal_status:
            est_days_int = 0
            est_days_display = "0"
            reason_display = official_disposal_status.get("reason", "官方已公告處置")

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

        if official_disposal_status:
            risk['risk_level'] = '高'
            risk['trigger_msg'] = official_disposal_status.get("reason", "官方已公告處置")

        # 連續天數從最新交易日往回計算，最多反映最近5個交易日連續注意狀況。
        streak = 0
        for v in reversed(valid_bits[-5:]):
            if v: streak += 1
            else: break

        status_30 = "".join(map(str, valid_bits)).zfill(30)

        def safe(v):
            if v is None: return ""
            try:
                if np.isnan(v): return ""
            except: pass
            return str(v)

        last_date_val = ""
        attention_dates = [
            d.strftime("%Y-%m-%d")
            for d, v in zip(stock_calendar, valid_bits)
            if v == 1
        ]
        if attention_dates:
            last_date_val = attention_dates[-1]

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
