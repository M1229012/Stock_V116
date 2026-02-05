import gspread
import requests
import os
import json
import re
import time
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from io import StringIO

# === 爬蟲相關套件 ===
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ============================
# ⚙️ 設定區
# ============================
DISCORD_WEBHOOK_URL_TEST = os.getenv("DISCORD_WEBHOOK_URL_TEST")
SHEET_NAME = "台股注意股資料庫_V33"
SERVICE_KEY_FILE = "service_key.json"

JAIL_ENTER_THRESHOLD = 3   
JAIL_EXIT_THRESHOLD = 5    

# ⚡ 法人判斷閥值
THRESH_FOREIGN = 0.010  # 外資 1.0%
THRESH_OTHERS  = 0.005  # 投信/自營 0.5%

# ============================
# 🛠️ 爬蟲與工具函式
# ============================
def get_driver():
    """初始化 Selenium Driver"""
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def is_valid_date_row(s): 
    return re.match(r"^\d{2,4}[/-]\d{1,2}[/-]\d{1,2}$", str(s).strip()) is not None

def roc_to_datestr(d_str):
    parts = re.split(r"[/-]", str(d_str).strip())
    if len(parts) < 2: return None
    y = int(parts[0])
    if y < 1911: y += 1911
    return f"{y:04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"

def get_institutional_data(stock_id, start_date, end_date):
    """爬取法人買賣超"""
    driver = get_driver()
    url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcl/zcl.djhtm?a={stock_id}&c={start_date}&d={end_date}"
    try:
        driver.get(url)
        time.sleep(2)
        html = driver.page_source
        tables = pd.read_html(StringIO(html))
        target_df = None
        for df in tables:
            if df.astype(str).apply(lambda x: x.str.contains('外資', na=False)).any().any():
                target_df = df
                break
        if target_df is not None:
            clean_df = target_df.copy()
            clean_df.columns = clean_df.iloc[0]
            clean_df = clean_df[1:].iloc[:, 0:4]
            clean_df.columns = ['日期', '外資買賣超', '投信買賣超', '自營商買賣超']
            clean_df = clean_df[clean_df['日期'].apply(is_valid_date_row)]
            for col in ['外資買賣超', '投信買賣超', '自營商買賣超']:
                clean_df[col] = pd.to_numeric(clean_df[col].astype(str).str.replace(',', '').str.replace('+', ''), errors='coerce').fillna(0)
            clean_df['DateStr'] = clean_df['日期'].apply(roc_to_datestr)
            return clean_df.dropna(subset=['DateStr'])
    except: return None
    finally: driver.quit()

def connect_google_sheets():
    """連線 Google Sheets"""
    try:
        gc = gspread.service_account(filename=SERVICE_KEY_FILE)
        return gc.open(SHEET_NAME)
    except: return None

def send_discord_webhook(embeds):
    """發送訊息至 Discord"""
    if not embeds: return
    data = {"username": "台股處置監控機器人", "avatar_url": "https://cdn-icons-png.flaticon.com/512/2502/2502697.png", "embeds": embeds}
    requests.post(DISCORD_WEBHOOK_URL_TEST, data=json.dumps(data), headers={"Content-Type": "application/json"})

def parse_roc_date(date_str):
    """解析日期格式"""
    s = str(date_str).strip()
    match = re.match(r'^(\d{2,3})[/-](\d{1,2})[/-](\d{1,2})$', s)
    if match:
        y, m, d = map(int, match.groups())
        y_final = y + 1911 if y < 1911 else y
        return datetime(y_final, m, d)
    for fmt in ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]:
        try: return datetime.strptime(s, fmt)
        except: continue
    return None

def get_merged_jail_periods(sh):
    """讀取並合併處置期間"""
    jail_map = {} 
    tw_now = datetime.utcnow() + timedelta(hours=8)
    today = datetime(tw_now.year, tw_now.month, tw_now.day)
    try:
        ws = sh.worksheet("處置股90日明細")
        records = ws.get_all_records()
        for row in records:
            code = str(row.get('代號', '')).replace("'", "").strip()
            period = str(row.get('處置期間', '')).strip()
            if not code or not period: continue
            dates = re.split(r'[~-～]', period)
            if len(dates) >= 2:
                s_date, e_date = parse_roc_date(dates[0]), parse_roc_date(dates[1])
                if s_date and e_date:
                    if e_date < today: continue
                    if code not in jail_map:
                        jail_map[code] = {'start': s_date, 'end': e_date}
                    else:
                        jail_map[code]['start'] = min(jail_map[code]['start'], s_date)
                        jail_map[code]['end'] = max(jail_map[code]['end'], e_date)
    except: return {}
    return {c: f"{d['start'].strftime('%Y/%m/%d')}-{d['end'].strftime('%Y/%m/%d')}" for c, d in jail_map.items()}

# ============================
# 📊 價格與法人計算邏輯 (還原 K 線)
# ============================
def get_price_rank_info(code, period_str, market):
    """核心計算邏輯：計算處置前 vs 處置中的績效對比"""
    try:
        dates = re.split(r'[~-～]', str(period_str))
        start_date = parse_roc_date(dates[0])
        if not start_date: return "❓", "未知", "日期錯", ""
        
        fetch_start = start_date - timedelta(days=60)
        end_date = datetime.now() + timedelta(days=1)
        suffix = ".TWO" if any(x in str(market) for x in ["上櫃", "TPEx"]) else ".TW"
        ticker = f"{code}{suffix}"
        
        # 📌 強制使用還原 K 線 (auto_adjust=True)
        df = yf.Ticker(ticker).history(start=fetch_start.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), auto_adjust=True)
        if not df.empty: df = df.ffill() 
        if df.empty or len(df) < 2: return "❓", "未知", "無股價", ""

        df.index = df.index.tz_localize(None)
        df_in_jail = df[df.index >= pd.Timestamp(start_date)]
        
        # 績效計算 (同天數對比)
        mask_before = df.index < pd.Timestamp(start_date)
        if not mask_before.any(): pre_pct = 0.0
        else:
            jail_base_p = df[mask_before]['Close'].iloc[-1]
            pre_jail_avg_volume = df[mask_before]['Volume'].tail(20).mean()
            lookback = max(1, len(df_in_jail))
            loc_idx = df.index.get_loc(df[mask_before].index[-1])
            pre_entry = df.iloc[max(0, loc_idx - lookback + 1)]['Open']
            pre_pct = ((jail_base_p - pre_entry) / pre_entry) * 100

        in_pct = ((df_in_jail['Close'].iloc[-1] - df_in_jail['Open'].iloc[0]) / df_in_jail['Open'].iloc[0] * 100) if not df_in_jail.empty else 0.0

        # 📌 依照您的要求更新狀態詞彙
        if in_pct > 15:
            status_icon, status_text = "👑", "妖股誕生"
        elif in_pct > 5:
            status_icon, status_text = "🔥", "強勢突圍"
        elif in_pct < -15:
            status_icon, status_text = "💀", "人去樓空"
        elif in_pct < -5:
            status_icon, status_text = "📉", "走勢疲軟"
        else:
            status_icon, status_text = "🧊", "多空膠著"

        price_data = f"處置前{'+' if pre_pct > 0 else ''}{pre_pct:.1f}% / 處置中{'+' if in_pct > 0 else ''}{in_pct:.1f}%"

        # 法人判斷
        inst_msg = ""
        if not df_in_jail.empty and pre_jail_avg_volume > 0:
            inst_df = get_institutional_data(code, start_date.strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d"))
            if inst_df is not None:
                bm = (pre_jail_avg_volume * len(df_in_jail)) / 1000
                r_f, r_t, r_d = inst_df['外資買賣超'].sum()/bm, inst_df['投信買賣超'].sum()/bm, inst_df['自營商買賣超'].sum()/bm
                msgs = []
                if r_t > THRESH_OTHERS: msgs.append("投信買")
                elif r_t < -THRESH_OTHERS: msgs.append("投信賣")
                if r_f > THRESH_FOREIGN: msgs.append("外資買")
                elif r_f < -THRESH_FOREIGN: msgs.append("外資賣")
                if r_d > THRESH_OTHERS: msgs.append("自營買")
                elif r_d < -THRESH_OTHERS: msgs.append("自營賣")
                if msgs:
                    icon = "🔥" if all("買" in m for m in msgs) else ("🧊" if all("賣" in m for m in msgs) else "🔄")
                    inst_msg = f"{icon} **{' '.join(msgs)}**"

        return status_icon, status_text, price_data, inst_msg
    except: return "❓", "未知", "計算中", ""

# ============================
# 🔍 監控邏輯 (排序修正)
# ============================
def check_status_split(sh, rel_codes):
    """檢查並分類股票"""
    ws = sh.worksheet("近30日熱門統計")
    records = ws.get_all_records()
    jail_map = get_merged_jail_periods(sh)
    ent, inj, seen = [], [], set()
    for row in records:
        code = str(row.get('代號', '')).replace("'", "").strip()
        if code in rel_codes or code in seen: continue
        d = int(row.get('最快處置天數', '99')) + 1
        if "處置中" in str(row.get('處置觸發原因', '')):
            inj.append({"code": code, "name": row.get('名稱', ''), "period": jail_map.get(code, "日期未知")})
            seen.add(code)
        elif d <= JAIL_ENTER_THRESHOLD:
            ent.append({"code": code, "name": row.get('名稱', ''), "days": d})
            seen.add(code)
    
    # 📌 排序邏輯：優先比天數（由短至長），天數相同比股號（由小至大）
    ent.sort(key=lambda x: (x['days'], x['code']))
    
    def get_end_date(item):
        try: return datetime.strptime(item['period'].split('-')[1], "%Y/%m/%d")
        except: return datetime.max 
    inj.sort(key=lambda x: (get_end_date(x), x['code']))
    return {'entering': ent, 'in_jail': inj}

def check_releasing_stocks(sh):
    """檢查即將出關股票"""
    ws = sh.worksheet("即將出關監控")
    records = ws.get_all_records()
    res, seen = [], set()
    for row in records:
        code = str(row.get('代號', '')).strip()
        if code in seen: continue
        d = int(row.get('剩餘天數', '99')) + 1
        if d <= JAIL_EXIT_THRESHOLD:
            icon, txt, pr, inst = get_price_rank_info(code, row.get('處置期間', ''), row.get('市場', ''))
            dt = parse_roc_date(row.get('出關日期', ''))
            res.append({"code": code, "name": row.get('名稱', ''), "days": d, "date": dt.strftime("%m/%d") if dt else "??/??", "icon": icon, "txt": txt, "price": pr, "inst": inst})
            seen.add(code)
    
    # 📌 排序邏輯：優先比天數（由短至長），天數相同比股號（由小至大）
    res.sort(key=lambda x: (x['days'], x['code']))
    return res

# ============================
# 🚀 主程式 (### 小標題顯示)
# ============================
def main():
    sh = connect_google_sheets()
    if not sh: return
    rel = check_releasing_stocks(sh)
    rel_codes = {x['code'] for x in rel}
    stats = check_status_split(sh, rel_codes)

    # 1. 處置倒數 (### 標題)
    if stats['entering']:
        total = len(stats['entering'])
        chunk = 10 if total > 15 else 20
        for i in range(0, total, chunk):
            lines = []
            if i == 0: lines.append(f"### 🚨 處置倒數！{total} 檔股票瀕臨處置\n")
            for s in stats['entering'][i:i+chunk]:
                status_msg = '明日開始處置' if s['days'] == 1 else f"處置倒數 {s['days']} 天"
                lines.append(f"{'🔥' if s['days'] == 1 else '⚠️'} **{s['code']} {s['name']}** | `{status_msg}`")
            send_discord_webhook([{"description": "\n".join(lines), "color": 15158332}])
            time.sleep(2)

    # 2. 即將出關 (### 標題 + 法人資訊)
    if rel:
        total = len(rel)
        chunk = 10 if total > 15 else 20
        for i in range(0, total, chunk):
            lines = []
            if i == 0: lines.append(f"### 🔓 越關越大尾？{total} 檔股票即將出關\n")
            for s in rel[i:i+chunk]:
                lines.append(f"**{s['code']} {s['name']}** | 剩 {s['days']} 天 ({s['date']})")
                lines.append(f"▸ {s['icon']} {s['txt']} {s['price']}")
                if s['inst']: lines.append(f"▸ {s['inst']}")
                lines.append("") # 每支股票間空行
            if i + chunk >= total:
                lines.append("---\n*💡 說明：處置前 N 天 vs 處置中 N 天 (同天數對比)*")
            send_discord_webhook([{"description": "\n".join(lines), "color": 3066993}])
            time.sleep(2)

    # 3. 處置中 (### 標題)
    if stats['in_jail']:
        total = len(stats['in_jail'])
        chunk = 10 if total > 15 else 20
        for i in range(0, total, chunk):
            lines = []
            if i == 0: lines.append(f"### ⛓️ 還能噴嗎？{total} 檔股票正在處置\n")
            for s in stats['in_jail'][i:i+chunk]:
                lines.append(f"🔒 **{s['code']} {s['name']}** | `{s['period'].replace('2026/', '')}`")
            send_discord_webhook([{"description": "\n".join(lines), "color": 10181046}])
            time.sleep(2)

if __name__ == "__main__": main()
