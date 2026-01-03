# -*- coding: utf-8 -*-
"""
V116.18 台股注意股系統 (GitHub Action 部署優化版)
修正重點：
1. [代號] 修正 get_jail_map 的 Regex 提取邏輯，強制僅抓取 4 碼代號，防止匹配失效。
2. [比對] 每日紀錄比對邏輯維持 .dt.date == d，確保日期匹配精準度。
3. [防呆] update_disposition_database 維持 Upsert 與空表欄位補齊邏輯。
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
import urllib3
import json
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, time as dt_time, date
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

# 關閉 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

# 處置資料庫表頭
JAIL_DB_HEADERS = ['市場', '代號', '名稱', '處置期間', '處置措施', '西元起始', '西元結束', '最後更新']

# ==========================================
# 📆 設定區
# ==========================================
SHEET_NAME = "台股注意股資料庫_V33"
PARAM_SHEET_NAME = "個股參數"
TW_TZ = ZoneInfo("Asia/Taipei")
TARGET_DATE = datetime.now(TW_TZ)

# 時間門檻
SAFE_CRAWL_TIME = dt_time(17, 30)
DAYTRADE_PUBLISH_TIME = dt_time(21, 0)
SAFE_MARKET_OPEN_CHECK = dt_time(16, 30)

IS_NIGHT_RUN = TARGET_DATE.hour >= 20
IS_AFTER_SAFE = TARGET_DATE.time() >= SAFE_CRAWL_TIME
IS_AFTER_DAYTRADE = TARGET_DATE.time() >= DAYTRADE_PUBLISH_TIME

# 回補參數
MAX_BACKFILL_TRADING_DAYS = 40
VERIFY_RECENT_DAYS = 2

# ==========================================
# 🔑 FinMind 金鑰設定
# ==========================================
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"
token1 = os.getenv('FinMind_1')
token2 = os.getenv('FinMind_2')
FINMIND_TOKENS = [t for t in [token1, token2] if t]
CURRENT_TOKEN_INDEX = 0
_FINMIND_CACHE = {}

try: twstock.__update_codes()
except: pass

# ==========================================
# 🛠️ 輔助函數 (解析與格式化)
# ==========================================
def get_today_date():
    return datetime.now(TW_TZ).date()

def extract_dates_any(s: str):
    s = str(s or "").strip()
    p1 = re.findall(r'(\d{2,4})[./-](\d{1,2})[./-](\d{1,2})', s)
    p2 = re.findall(r'(\d{2,4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日?', s)
    p3 = re.findall(r'(\d{3})(\d{2})(\d{2})', s)
    hits = p1 + p2 + p3
    dates = []
    for y, m, d in hits:
        try:
            y = int(y); m = int(m); d = int(d)
            if y < 1911: y += 1911
            dates.append(date(y, m, d))
        except: pass
    return sorted(list(set(dates)))

def format_roc_period(period_str):
    dates = extract_dates_any(period_str)
    if len(dates) >= 2:
        start, end = dates[0], dates[-1]
        s_str = f"{start.year - 1911}/{start.month:02d}/{start.day:02d}"
        e_str = f"{end.year - 1911}/{end.month:02d}/{end.day:02d}"
        return f"{s_str}～{e_str}"
    return period_str

def safe_get(url, headers=None, timeout=10, params=None):
    try:
        res = requests.get(url, headers=headers, timeout=timeout, params=params, verify=False)
        return res
    except: return None

def safe_json(res):
    if res is None: return {}
    try: return res.json()
    except:
        try: return json.loads(res.text.lstrip("\ufeff").strip())
        except: return {}

def clean_text(x):
    return re.sub(r'<[^>]+>', '', str(x)).replace("&nbsp;", " ").strip()

def pick_4digit_code_from_values(obj):
    vals = obj.values() if isinstance(obj, dict) else obj
    for v in vals:
        t = clean_text(v)
        if re.fullmatch(r'\d{4}', t): return t
    return ""

def clean_tpex_name(raw_name):
    return raw_name.split('(')[0] if '(' in raw_name else raw_name

def clean_tpex_measure(content):
    if any(k in content for k in ["第二次", "再次", "每20分鐘", "每25分鐘", "每60分鐘"]): return "20分鐘盤"
    return "5分鐘盤"

CN_NUM = {"一":"1","二":"2","三":"3","四":"4","五":"5","六":"6","七":"7","八":"8","九":"9","十":"10"}
KEYWORD_MAP = {
    "起迄兩個營業日": 11, "當日沖銷": 13, "借券賣出": 12, "累積週轉率": 10, "週轉率": 4,
    "成交量": 9, "本益比": 6, "股價淨值比": 6, "溢折價": 8, "收盤價漲跌百分比": 1,
    "最後成交價漲跌": 1, "最近六個營業日累積": 1
}

def normalize_clause_text(s: str) -> str:
    if not s: return ""
    s = str(s).replace("第ㄧ款", "第一款")
    for cn, dg in CN_NUM.items(): s = s.replace(f"第{cn}款", f"第{dg}款")
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

# ============================
# 🔥 處置資料庫更新模組 (具備 Upsert 歷史保留邏輯)
# ============================
def update_disposition_database(sh):
    print("🔒 正在執行處置(Jail)資料庫 Upsert 更新...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    new_stock_list = []
    now_str = TARGET_DATE.strftime("%Y-%m-%d %H:%M:%S")

    # 1. 抓取最新上市處置
    try:
        url_twse = "https://openapi.twse.com.tw/v1/announcement/punish"
        res = safe_get(url_twse, headers=headers)
        payload = safe_json(res)
        if isinstance(payload, list):
            for item in payload:
                code = item.get('Code', '').strip()
                if not (code.isdigit() and len(code) == 4): continue
                name = item.get('Name', '').strip()
                period_raw = item.get('DispositionPeriod', '').strip()
                raw_measure = item.get('DispositionMeasures', '').strip()
                measure = "20分鐘盤" if any(k in raw_measure for k in ["第二次","再次"]) else "5分鐘盤"
                ds = extract_dates_any(period_raw)
                if len(ds) >= 2:
                    new_stock_list.append(['上市', code, name, format_roc_period(period_raw), measure, ds[0].strftime("%Y-%m-%d"), ds[-1].strftime("%Y-%m-%d"), now_str])
    except Exception as e: print(f"TWSE 抓取異常: {e}")

    # 2. 抓取最新上櫃處置
    try:
        url_tpex = "https://www.tpex.org.tw/openapi/v1/tpex_disposal_information"
        res = safe_get(url_tpex, headers=headers)
        payload = safe_json(res)
        if isinstance(payload, dict) and "data" in payload: payload = payload["data"]
        if isinstance(payload, list):
            for item in payload:
                code = clean_text(item.get("SecuritiesCompanyCode") or item.get("證券代號") or "")
                if not code: code = pick_4digit_code_from_values(item)
                if not (code.isdigit() and len(code) == 4): continue
                name = clean_text(item.get("CompanyName") or item.get("證券名稱") or "")
                period_raw = clean_text(item.get("DispositionPeriod") or item.get("處置期間") or "")
                raw_content = clean_text(item.get("DisposalCondition") or item.get("處置內容") or "")
                ds = extract_dates_any(period_raw)
                if len(ds) >= 2:
                    new_stock_list.append(['上櫃', code, clean_tpex_name(name), format_roc_period(period_raw), clean_tpex_measure(raw_content), ds[0].strftime("%Y-%m-%d"), ds[-1].strftime("%Y-%m-%d"), now_str])
    except Exception as e: print(f"TPEx 抓取異常: {e}")

    # 3. 合併舊有歷史、去重並寫回
    try:
        ws = get_or_create_ws(sh, "處置有價證券紀錄", headers=JAIL_DB_HEADERS)
        existing_data = ws.get_all_records()
        
        df_old = pd.DataFrame(existing_data)
        if df_old.empty:
            df_old = pd.DataFrame(columns=JAIL_DB_HEADERS)
            
        df_new = pd.DataFrame(new_stock_list, columns=JAIL_DB_HEADERS)
        
        # 合併與去重
        df_merged = pd.concat([df_old, df_new], ignore_index=True)
        df_merged = df_merged.drop_duplicates(subset=['市場', '代號', '西元起始', '西元結束'], keep='last')
        
        # 過濾歷史：保留結束日期在 90 個交易日內的資料 (轉 datetime 比較)
        temp_cal = get_official_trading_calendar(90)
        cutoff_date = temp_cal[0].strftime("%Y-%m-%d") if temp_cal else (get_today_date() - timedelta(days=130)).strftime("%Y-%m-%d")
        
        df_merged["西元結束_dt"] = pd.to_datetime(df_merged["西元結束"], errors="coerce")
        cutoff_dt = pd.to_datetime(cutoff_date)
        df_merged = df_merged[df_merged["西元結束_dt"] >= cutoff_dt].drop(columns=["西元結束_dt"])
        
        # 排序
        df_merged = df_merged.sort_values(by=['西元結束', '代號'], ascending=[False, True])
        final_list = df_merged.values.tolist()

        ws.clear()
        ws.append_row(JAIL_DB_HEADERS, value_input_option='USER_ENTERED')
        if final_list:
            ws.append_rows(final_list, value_input_option='USER_ENTERED')
        print(f"✅ 處置庫 Upsert 完成：共保留 {len(final_list)} 筆歷史紀錄")
    except Exception as e:
        print(f"❌ 處置庫更新失敗: {e}")

# ============================
# 🛠️ 核心分析功能
# ============================
def get_or_create_ws(sh, title, headers=None, rows=2000, cols=30):
    try:
        ws = sh.worksheet(title)
    except Exception:
        ws = sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))
    if headers:
        try:
            first_row = ws.row_values(1)
            if first_row != headers:
                ws.clear(); ws.append_row(headers, value_input_option="USER_ENTERED")
        except Exception:
            ws.clear(); ws.append_row(headers, value_input_option="USER_ENTERED")
    return ws

def connect_google_sheets():
    try:
        if not os.path.exists("service_key.json"):
            key_json = os.getenv('GOOGLE_SHEETS_KEY')
            if key_json:
                with open("service_key.json", "w") as f:
                    f.write(key_json)
            else:
                return None, None
        gc = gspread.service_account(filename="service_key.json")
        sh = gc.open(SHEET_NAME)
        return sh, None
    except: return None, None

def finmind_get(dataset, data_id=None, start_date=None, end_date=None):
    global CURRENT_TOKEN_INDEX
    params = {"dataset": dataset}
    if data_id: params["data_id"] = str(data_id)
    if start_date: params["start_date"] = start_date
    if end_date: params["end_date"] = end_date
    if not FINMIND_TOKENS: return pd.DataFrame()
    for _ in range(4):
        headers = {"Authorization": f"Bearer {FINMIND_TOKENS[CURRENT_TOKEN_INDEX]}", "Connection": "close"}
        try:
            r = requests.get(FINMIND_API_URL, params=params, headers=headers, timeout=10)
            if r.status_code == 200: return pd.DataFrame(r.json().get("data", []))
            CURRENT_TOKEN_INDEX = (CURRENT_TOKEN_INDEX + 1) % len(FINMIND_TOKENS)
        except: time.sleep(1)
    return pd.DataFrame()

def get_official_trading_calendar(days=60):
    end = TARGET_DATE.strftime("%Y-%m-%d")
    start = (TARGET_DATE - timedelta(days=days*2)).strftime("%Y-%m-%d")
    df = finmind_get("TaiwanStockTradingDate", start_date=start, end_date=end)
    dates = []
    if not df.empty:
        df['date'] = pd.to_datetime(df['date']).dt.date
        dates = sorted(df['date'].tolist())
    return dates[-days:]

def load_jail_map_from_sheet(sh, sheet_name="處置有價證券紀錄"):
    jail_map = {}
    try:
        ws = sh.worksheet(sheet_name)
        rows = ws.get_all_records()
        if not rows: return jail_map
        for r in rows:
            code = str(r.get("代號", "")).strip().replace("'", "")
            s = str(r.get("西元起始", "")).strip()
            e = str(r.get("西元結束", "")).strip()
            if not (code.isdigit() and len(code) == 4 and s and e): continue
            
            ts_s = pd.to_datetime(s, errors="coerce")
            ts_e = pd.to_datetime(e, errors="coerce")
            if pd.isna(ts_s) or pd.isna(ts_e): continue
            
            sd, ed = ts_s.date(), ts_e.date()
            jail_map.setdefault(code, []).append((sd, ed))
        for k in list(jail_map.keys()):
            jail_map[k] = sorted(jail_map[k], key=lambda x: x[0])
        return jail_map
    except: return jail_map

def get_jail_map(start_date_obj, end_date_obj):
    print("📡 Fallback 爬網建立處置濾網...")
    jail_map = {}
    s_str = start_date_obj.strftime("%Y%m%d")
    e_str = end_date_obj.strftime("%Y%m%d")
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = safe_get("https://www.twse.com.tw/rwd/zh/announcement/punish", params={"startDate": s_str, "endDate": e_str, "response": "json"}, headers=headers)
        j = safe_json(r)
        if j.get("tables"):
            for row in j["tables"][0].get("data", []):
                # ✅ 修正：Regex 強制僅提取 4 碼代號，解決 is_in_jail 對不到的問題
                code_match = re.search(r'(\d{4})', str(row[1]))
                if not code_match: continue
                code = code_match.group(1)
                
                ds = extract_dates_any(str(row[3]))
                if len(ds) >= 2: jail_map.setdefault(code, []).append((ds[0], ds[-1]))
    except: pass
    try:
        r = safe_get("https://www.tpex.org.tw/openapi/v1/tpex_disposal_information", headers=headers)
        payload = safe_json(r)
        if isinstance(payload, dict) and "data" in payload: payload = payload["data"]
        if isinstance(payload, list):
            for item in payload:
                code = str(item.get("SecuritiesCompanyCode", "")).strip()
                if not (code.isdigit() and len(code) == 4): continue
                ds = extract_dates_any(str(item.get("DispositionPeriod", "")))
                if len(ds) >= 2: jail_map.setdefault(code, []).append((ds[0], ds[-1]))
    except: pass
    return jail_map

def is_in_jail(stock_id, target_date, jail_map):
    if not jail_map or stock_id not in jail_map: return False
    for s, e in jail_map[stock_id]:
        if s <= target_date <= e: return True
    return False

def build_exclude_map(cal_dates, jail_map):
    exclude_map = {}
    if not jail_map: return exclude_map
    for code, periods in jail_map.items():
        s = set()
        for start, end in periods:
            idx = -1
            try: idx = cal_dates.index(start)
            except: pass
            if idx > 0: s.add(cal_dates[idx-1]) 
            for d in cal_dates:
                if start <= d <= end: s.add(d)
        exclude_map[code] = s
    return exclude_map

def get_last_n_non_jail_trade_dates(stock_id, cal_dates, jail_map, exclude_map=None, n=30):
    last_jail_end = date(1900, 1, 1)
    if jail_map and stock_id in jail_map:
        last_jail_end = sorted([p[1] for p in jail_map[stock_id]])[-1]
    window = cal_dates[-n:] if len(cal_dates) >= n else cal_dates
    picked = [d for d in window if d > last_jail_end]
    return picked

def fetch_history_data(ticker_code):
    try:
        df = yf.Ticker(ticker_code).history(period="1y", auto_adjust=False)
        if df.empty: return pd.DataFrame()
        df.index = df.index.tz_localize(None)
        return df
    except: return pd.DataFrame()

def simulate_days_to_jail_strict(status_list, clause_list, *, stock_id=None, target_date=None, jail_map=None):
    if stock_id and target_date and jail_map and is_in_jail(stock_id, target_date, jail_map): return 0, "處置中"
    v30 = sum(status_list)
    if v30 >= 12: return 0, "已達標"
    return 99, ""

# ============================
# Main 主程式
# ============================
def main():
    sh, _ = connect_google_sheets()
    if not sh: return

    update_disposition_database(sh)
    cal_dates = get_official_trading_calendar(240)
    if not cal_dates: return

    target_trade_date_obj = cal_dates[-1]
    if (target_trade_date_obj == get_today_date()) and (not IS_AFTER_SAFE) and len(cal_dates) >= 2:
        target_trade_date_obj = cal_dates[-2]
    
    ws_log = get_or_create_ws(sh, "每日紀錄", headers=['日期','市場','代號','名稱','觸犯條款'])
    log_data = ws_log.get_all_records()
    df_log = pd.DataFrame(log_data)
    if not df_log.empty:
        df_log['代號'] = df_log['代號'].astype(str).str.strip().str.replace("'", "")
        df_log['日期'] = pd.to_datetime(df_log['日期'], errors='coerce')
        df_log = df_log.dropna(subset=['日期'])

    # 處置判定優先用 Sheet，沒資料才爬網
    jail_map = load_jail_map_from_sheet(sh)
    if not jail_map:
        start_obj = cal_dates[-90] if len(cal_dates) >= 90 else cal_dates[0]
        jail_map = get_jail_map(start_obj, target_trade_date_obj)
    
    exclude_map = build_exclude_map(cal_dates, jail_map)
    cutoff = pd.Timestamp(cal_dates[-90])
    target_stocks = []
    if not df_log.empty:
        target_stocks = df_log[df_log['日期'] >= cutoff]['代號'].unique()
    
    rows_stats = []
    for code in target_stocks:
        code = str(code).strip()
        name = df_log[df_log['代號']==code]['名稱'].iloc[-1] if not df_log[df_log['代號']==code].empty else "未知"
        stock_calendar = get_last_n_non_jail_trade_dates(code, cal_dates, jail_map, exclude_map, 30)
        
        bits = []; clauses = []
        for d in stock_calendar:
            c = ""
            if not df_log.empty:
                matches = df_log[(df_log['代號']==code) & (df_log['日期'].dt.date==d)]
                if not matches.empty: c = "、".join(matches['觸犯條款'].tolist())
            if (code in exclude_map) and (d in exclude_map[code]): bits.append(0); clauses.append(c)
            elif c: bits.append(1); clauses.append(c)
            else: bits.append(0); clauses.append("")

        v_bits = [1 if b==1 and is_valid_accumulation_day(parse_clause_ids_strict(c)) else 0 for b,c in zip(bits, clauses)]
        v30 = sum(v_bits)
        status_30 = "".join(["1" if b==1 else "0" for b in bits]).zfill(30)
        est_days, reason = simulate_days_to_jail_strict(v_bits, clauses, stock_id=code, target_date=target_trade_date_obj, jail_map=jail_map)
        
        row = [f"'{code}", name, 0, v30, sum(bits[-10:]), stock_calendar[-1].strftime("%Y-%m-%d") if stock_calendar else "", f"'{status_30}", f"'{status_30[-10:]}", str(est_days) if est_days!=99 else "X", reason, "低", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        rows_stats.append(row)

    if rows_stats:
        ws_stats = get_or_create_ws(sh, "近30日熱門統計", headers=STATS_HEADERS)
        ws_stats.clear(); ws_stats.append_row(STATS_HEADERS, value_input_option='USER_ENTERED')
        ws_stats.append_rows(rows_stats, value_input_option='USER_ENTERED')
        print("✅ 統計更新完成")

if __name__ == "__main__":
    main()
