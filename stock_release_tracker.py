import gspread
import requests
import os
import json
import re
import time
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound
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
SHEET_NAME = "台股注意股資料庫_V33"
DEST_WORKSHEET = "一年期處置回測數據" 

SERVICE_KEY_FILE = "service_key.json"

# ⚡ 法人判斷閥值
THRESH_FOREIGN = 0.010  # 外資 1.0%
THRESH_OTHERS  = 0.005  # 投信/自營 0.5%

# ============================
# 🛠️ 爬蟲與工具函式
# ============================
def get_driver():
    """初始化 Selenium Driver (無頭模式)"""
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

def connect_google_sheets(sheet_name):
    """連線 Google Sheets"""
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(SERVICE_KEY_FILE, scopes=scope)
        gc = gspread.authorize(creds)
        sh = gc.open(sheet_name)
        return sh
    except Exception as e:
        print(f"❌ Google Sheet 連線失敗 ({sheet_name}): {e}")
        return None

def is_valid_date_row(s): 
    return re.match(r"^\d{2,4}[/-]\d{1,2}[/-]\d{1,2}$", str(s).strip()) is not None

def roc_to_datestr(d_str):
    parts = re.split(r"[/-]", str(d_str).strip())
    if len(parts) < 2: return None
    y = int(parts[0])
    if y < 1911: y += 1911
    return f"{y:04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"

def parse_roc_date(date_str):
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

# ============================
# 📅 歷史名單爬取 (一年份核心邏輯)
# ============================

def fetch_tpex_history_requests(start_date, end_date):
    """
    [上櫃 TPEx] 使用 Requests 抓取歷史資料 (按月迴圈)
    """
    print(f"  [上櫃] 啟動 Requests 爬蟲，範圍: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    
    all_data = []
    curr = start_date
    while curr <= end_date:
        # 計算當月最後一天
        next_month = curr.replace(day=28) + timedelta(days=4)
        last_day_of_month = next_month - timedelta(days=next_month.day)
        batch_end = min(last_day_of_month, end_date)
        
        sd_str = f"{curr.year - 1911}/{curr.month:02d}/{curr.day:02d}"
        ed_str = f"{batch_end.year - 1911}/{batch_end.month:02d}/{batch_end.day:02d}"
        
        url = "https://www.tpex.org.tw/www/zh-tw/bulletin/disposal"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Referer": "https://www.tpex.org.tw/www/zh-tw/bulletin/disposal"
        }
        payload = {"startDate": sd_str, "endDate": ed_str, "response": "json"}
        
        try:
            r = requests.post(url, data=payload, headers=headers, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if "tables" in data and len(data["tables"]) > 0:
                    rows = data["tables"][0].get("data", [])
                    for row in rows:
                        if len(row) < 6: continue
                        c_code = str(row[2]).strip()
                        c_name = str(row[3]).split("(")[0].strip()
                        c_period = str(row[5]).strip()
                        
                        if c_code.isdigit() and len(c_code) == 4:
                            all_data.append({
                                "Code": c_code,
                                "Name": c_name,
                                "Period": c_period,
                                "Market": "上櫃"
                            })
            time.sleep(0.5) 
        except Exception as e:
            print(f"    ❌ TPEx {sd_str} 抓取失敗: {e}")
        
        curr = last_day_of_month + timedelta(days=1)

    if all_data:
        return pd.DataFrame(all_data)
    return pd.DataFrame()

def fetch_twse_history_selenium(start_date, end_date):
    """
    [上市 TWSE] 使用 Selenium 抓取歷史資料 (按月迴圈)
    """
    print(f"  [上市] 啟動 Selenium 瀏覽器，範圍: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    
    driver = get_driver()
    url = "https://www.twse.com.tw/zh/announcement/punish.html"
    all_data = []
    
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 15)
        
        curr = start_date
        while curr <= end_date:
            next_month = curr.replace(day=28) + timedelta(days=4)
            last_day_of_month = next_month - timedelta(days=next_month.day)
            batch_end = min(last_day_of_month, end_date)
            
            sd_str = curr.strftime("%Y%m%d")
            ed_str = batch_end.strftime("%Y%m%d")
            
            try:
                driver.execute_script(f"""
                    document.querySelector('input[name="startDate"]').value = "{sd_str}";
                    document.querySelector('input[name="endDate"]').value = "{ed_str}";
                """)
                
                search_btn = driver.find_element(By.CSS_SELECTOR, "button.search")
                search_btn.click()
                
                time.sleep(1.5) 
                
                rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
                for row in rows:
                    try:
                        cols = row.find_elements(By.TAG_NAME, "td")
                        if len(cols) >= 7:
                            c_code = cols[2].text.strip()
                            c_name = cols[3].text.strip()
                            c_period = cols[6].text.strip()
                            
                            if c_code and c_code.isdigit() and len(c_code) == 4:
                                all_data.append({
                                    "Code": c_code,
                                    "Name": c_name,
                                    "Period": c_period,
                                    "Market": "上市"
                                })
                    except: continue
                    
            except Exception as e:
                print(f"    ❌ TWSE {sd_str} 操作失敗: {e}")
                
            curr = last_day_of_month + timedelta(days=1)
            
    except Exception as e:
        print(f"  ❌ TWSE Driver 錯誤: {e}")
    finally:
        driver.quit()
        
    if all_data:
        return pd.DataFrame(all_data)
    return pd.DataFrame()

# ============================
# 📊 整合與統計函式
# ============================

def determine_status(pre_pct, in_pct):
    if in_pct > 15: return "👑 妖股誕生"
    elif in_pct > 5: return "🔥 強勢突圍"
    elif in_pct < -15: return "💀 人去樓空"
    elif in_pct < -5: return "📉 走勢疲軟"
    else: return "🧊 多空膠著"

def get_ticker_list(code, market=""):
    code = str(code)
    if "上櫃" in market or "TPEx" in market: return [f"{code}.TWO", f"{code}.TW"]
    if "上市" in market: return [f"{code}.TW", f"{code}.TWO"]
    if code and code[0] in ['3', '4', '5', '6', '8']: return [f"{code}.TWO", f"{code}.TW"]
    return [f"{code}.TW", f"{code}.TWO"]

def get_institutional_data(stock_id, start_date, end_date):
    """爬取法人買賣超 (富邦證券)"""
    driver = get_driver()
    if isinstance(start_date, datetime): start_date = start_date.strftime("%Y-%m-%d")
    if isinstance(end_date, datetime): end_date = end_date.strftime("%Y-%m-%d")
    
    url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcl/zcl.djhtm?a={stock_id}&c={start_date}&d={end_date}"
    try:
        driver.get(url)
        time.sleep(1.0)
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
    except Exception as e:
        print(f"⚠️ 爬蟲錯誤 {stock_id}: {e}")
        return None
    finally:
        driver.quit()

# ============================
# 📈 [新增] 月線(MA20)回測統計函式
# ============================
def get_ma_touch_stats(df, start_date, end_date, pre_pct_val):
    """
    計算「上漲進處置 + 處置期間月線(MA20)斜率>1 + 處置期間跌到月線」後的 D+1~D+10 每日漲跌幅。

    條件說明：
    1. pre_pct_val > 0：股票在處置前期間為上漲走勢 (因上漲進入處置)
    2. 處置期間 MA20 斜率 > 1：月線每日平均上升幅度 > 1 點 (月線仍向上)
    3. 處置期間至少有一天收盤價落在 MA20 的 ±15% 範圍內
       即：MA20 * 0.85 <= Close <= MA20 * 1.15

    計算邏輯：
    - 以第一個符合條件的當天收盤價為基準
    - 回傳觸碰日後 D+1~D+10 的每日漲跌幅列表 (float，不足則為 None)
    - 若任一條件不符合則回傳 None
    """
    try:
        # 條件 1: 因上漲進入處置 (處置前漲幅 > 0)
        if pre_pct_val <= 0:
            return None

        df_calc = df.copy()

        # 計算 MA20 (月線，20日移動平均)
        df_calc['MA20'] = df_calc['Close'].rolling(window=20).mean()

        # 取處置期間資料，排除 MA20 為 NaN 的列
        mask_jail = (
            (df_calc.index >= pd.Timestamp(start_date)) &
            (df_calc.index <= pd.Timestamp(end_date))
        )
        df_jail = df_calc[mask_jail].dropna(subset=['MA20'])

        if len(df_jail) < 2:
            return None

        # 條件 2: 月線斜率 > 1 (處置期間 MA20 每日平均上升 > 1 點)
        slope = (df_jail['MA20'].iloc[-1] - df_jail['MA20'].iloc[0]) / len(df_jail)
        if slope <= 1:
            return None

        # 條件 3: 找處置期間第一個收盤價在 MA20 ±15% 範圍內的日子
        # 即：MA20 * 0.85 <= Close <= MA20 * 1.15
        touch_idx = None
        for i in range(len(df_jail)):
            ma_val = df_jail['MA20'].iloc[i]
            close_val = df_jail['Close'].iloc[i]
            if ma_val * 0.85 <= close_val <= ma_val * 1.15:
                touch_idx = i
                break

        if touch_idx is None:
            return None

        touch_date = df_jail.index[touch_idx]
        base_price = float(df_jail['Close'].iloc[touch_idx])

        if base_price == 0:
            return None

        # 取觸碰日之後的 D+1~D+10 (不限於處置期間，可延伸至出關後)
        df_after = df_calc[df_calc.index > touch_date].head(10)

        returns = []
        for i in range(10):
            if i < len(df_after):
                curr = float(df_after['Close'].iloc[i])
                prev = float(df_after['Close'].iloc[i - 1]) if i > 0 else base_price
                if prev != 0:
                    returns.append(((curr - prev) / prev) * 100)
                else:
                    returns.append(None)
            else:
                returns.append(None)

        return returns

    except Exception as e:
        return None

def fetch_stock_data(code, start_date, jail_end_date, market=""):
    """抓取股價與法人資料"""
    try:
        fetch_start = start_date - timedelta(days=365)
        fetch_end = jail_end_date + timedelta(days=65) 
        
        tickers_to_try = get_ticker_list(code, market)
        df = pd.DataFrame()
        
        for ticker in tickers_to_try:
            try:
                temp_df = yf.Ticker(ticker).history(start=fetch_start.strftime("%Y-%m-%d"), 
                                                  end=fetch_end.strftime("%Y-%m-%d"), 
                                                  auto_adjust=True)
                if not temp_df.empty:
                    df = temp_df
                    break
            except Exception:
                continue
        
        if df.empty: return None

        df.index = df.index.tz_localize(None)
        df = df.ffill()

        mask_jail = (df.index >= pd.Timestamp(start_date)) & (df.index <= pd.Timestamp(jail_end_date))
        df_jail = df[mask_jail]
        mask_before = df.index < pd.Timestamp(start_date)
        
        pre_pct = 0.0
        in_pct = 0.0
        pre_jail_avg_volume = 0
        
        if mask_before.any():
            jail_base_p = df[mask_before]['Close'].iloc[-1]
            pre_jail_avg_volume = df[mask_before]['Volume'].tail(60).mean()
            target_idx = max(0, len(df[mask_before]) - len(df_jail))
            pre_entry = df[mask_before]['Open'].iloc[target_idx] if len(df[mask_before]) > target_idx else jail_base_p
            if pre_entry != 0:
                pre_pct = ((jail_base_p - pre_entry) / pre_entry) * 100

        jail_end_price = 0
        if not df_jail.empty:
            jail_start_price = df_jail['Open'].iloc[0]
            jail_end_price = df_jail['Close'].iloc[-1]
            if jail_start_price != 0:
                in_pct = ((jail_end_price - jail_start_price) / jail_start_price) * 100
        
        status = determine_status(pre_pct, in_pct)

        inst_status = "🧊 無明顯動向"
        if not df_jail.empty and pre_jail_avg_volume > 0:
            print(f"  🕷️ 爬取法人資料: {code}...")
            inst_df = get_institutional_data(code, start_date, jail_end_date)
            
            if inst_df is not None:
                bm_shares = pre_jail_avg_volume * len(df_jail) 
                if bm_shares == 0: bm_shares = 1

                r_f = (inst_df['外資買賣超'].sum() * 1000) / bm_shares
                r_t = (inst_df['投信買賣超'].sum() * 1000) / bm_shares
                
                is_foreign_buy = r_f > THRESH_FOREIGN
                is_foreign_sell = r_f < -THRESH_FOREIGN
                is_trust_buy = r_t > THRESH_OTHERS
                is_trust_sell = r_t < -THRESH_OTHERS
                
                if is_foreign_buy and is_trust_buy: inst_status = "🔴 土洋合購"
                elif is_foreign_sell and is_trust_sell: inst_status = "🟢 土洋合賣"
                elif is_foreign_buy and is_trust_sell: inst_status = "🔴 外資買/投信賣"
                elif is_foreign_sell and is_trust_buy: inst_status = "🔴 投信買/外資賣"
                elif is_foreign_buy: inst_status = "🔴 外資大買"
                elif is_trust_buy: inst_status = "🔴 投信大買"
                elif is_foreign_sell: inst_status = "🟢 外資大賣"
                elif is_trust_sell: inst_status = "🟢 投信大賣"

        df_after = df[df.index > pd.Timestamp(jail_end_date)]
        
        if not df_after.empty:
            release_date_str = df_after.index[0].strftime("%Y/%m/%d")
        else:
            release_date_str = (jail_end_date + timedelta(days=1)).strftime("%Y/%m/%d")

        post_data = []
        accumulated_pct = 0.0
        base_price = jail_end_price if jail_end_price != 0 else (df_after['Open'].iloc[0] if not df_after.empty else 0)

        track_days = 20
        for i in range(track_days):
            if i < len(df_after):
                curr_close = df_after['Close'].iloc[i]
                prev_close = df_after['Close'].iloc[i-1] if i > 0 else base_price
                if prev_close != 0:
                    daily_chg = ((curr_close - prev_close) / prev_close) * 100
                    post_data.append(f"{daily_chg:+.1f}%")
                else:
                    post_data.append("0.0%")
                
                if i == len(df_after) - 1 or i == track_days - 1:
                    if base_price != 0:
                        accumulated_pct = ((curr_close - base_price) / base_price) * 100
            else:
                post_data.append("")

        while len(post_data) < track_days:
            post_data.append("")

        # ============================
        # [新增] 計算月線回測數據
        # ============================
        ma_touch_returns = get_ma_touch_stats(df, start_date, jail_end_date, pre_pct)

        return {
            "status": status,
            "inst_status": inst_status,
            "pre_pct": f"{pre_pct:+.1f}%",
            "in_pct": f"{in_pct:+.1f}%",
            "acc_pct": f"{accumulated_pct:+.1f}%",
            "daily_trends": post_data,
            "release_date": release_date_str,
            "ma_touch_returns": ma_touch_returns  # [新增] D+1~D+10，或 None
        }

    except Exception as e:
        print(f"⚠️ 數據計算錯誤 {code}: {e}")
        return None

# ============================
# 🚀 主程式
# ============================
def main():
    print("🚀 啟動一年期全量處置股回測 (TWSE-Selenium / TPEx-Requests)...")
    
    sh = connect_google_sheets(SHEET_NAME)
    if not sh: return

    today = datetime.now()
    one_year_ago = today - timedelta(days=365)
    end_fetch = today + timedelta(days=30) 

    # 1. 抓取歷史名單
    print(f"🔎 抓取歷史名單區間: {one_year_ago.strftime('%Y-%m-%d')} ~ {end_fetch.strftime('%Y-%m-%d')}")
    
    df_tpex = fetch_tpex_history_requests(one_year_ago, end_fetch)
    print(f"  --> 上櫃抓到: {len(df_tpex)} 筆")
    
    df_twse = fetch_twse_history_selenium(one_year_ago, end_fetch)
    print(f"  --> 上市抓到: {len(df_twse)} 筆")

    all_dfs = []
    if not df_tpex.empty: all_dfs.append(df_tpex)
    if not df_twse.empty: all_dfs.append(df_twse)

    if not all_dfs:
        print("❌ 抓取失敗，無資料可供回測。")
        return

    print("\n🔄 合併並整理資料...")
    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df.drop_duplicates(subset=['Code', 'Period'], inplace=True)
    source_data = final_df.to_dict('records')
    
    print(f"✅ 共取得 {len(source_data)} 筆不重複處置公告。開始進行 D+20 回測...")

    # 2. 準備寫入 Header
    header_base = ["出關日期", "股號", "股名", "狀態", "法人動向", "處置前%", "處置中%", "累積漲跌幅"]
    header_days = [f"D+{i+1}" for i in range(20)]
    header = header_base + header_days
    
    try:
        ws_dest = sh.worksheet(DEST_WORKSHEET)
    except WorksheetNotFound:
        print(f"💡 工作表 '{DEST_WORKSHEET}' 不存在，正在建立...")
        ws_dest = sh.add_worksheet(title=DEST_WORKSHEET, rows=5000, cols=60)
        ws_dest.append_row(header)

    raw_rows = ws_dest.get_all_values()
    existing_map = {} 
    if len(raw_rows) > 1:
        for row in raw_rows[1:]:
            if len(row) < 8: continue 
            rdate = str(row[0])
            rid = str(row[1])
            d_last_idx = 7 + 20 
            d_last = ""
            if len(row) > d_last_idx: d_last = str(row[d_last_idx]).strip()
            if rid:
                key = f"{rid}_{rdate}"
                row_dict = {}
                for idx, h in enumerate(header):
                    if idx < len(row): row_dict[h] = row[idx]
                    else: row_dict[h] = ""
                existing_map[key] = {'data': row_dict, 'done': bool(d_last)}

    processed_list = []
    
    status_order = ["👑 妖股誕生", "🔥 強勢突圍", "🧊 多空膠著", "📉 走勢疲軟", "💀 人去樓空"]
    inst_order = ["🔴 土洋合購", "🔴 外資大買", "🔴 投信大買", "🔴 外資買/投信賣", "🔴 投信買/外資賣", 
                  "🟢 土洋合賣", "🟢 外資大賣", "🟢 投信大賣", "🧊 無明顯動向"]
    
    track_days = 20
    interval_checkpoints = [5, 10, 15, 20]
    
    daily_stats = {s: [{'sum': 0.0, 'wins': 0, 'count': 0} for _ in range(track_days)] for s in status_order}
    summary_stats = {s: {'count': 0, 'wins': 0, 'total_pct': 0.0} for s in status_order}
    interval_data = {s: {cp: [] for cp in interval_checkpoints} for s in status_order}
    inst_stats_data = {i: {'count': 0, 'wins': 0, 'total_pct': 0.0} for i in inst_order}
    
    # 📌 新增：組合的區間統計 (狀態+法人)
    combo_interval_data = {} # Key: (status, inst), Value: {5: [], 10: [], 15: [], 20: []}

    # ============================
    # [新增] 月線回測追蹤變數
    # 條件：上漲進處置 + 月線斜率>1 + 處置期間跌到月線
    # 追蹤出關日後 D+1~D+10 的每日統計
    # ============================
    MA_TOUCH_TRACK_DAYS = 10
    ma_touch_daily = [{'sum': 0.0, 'wins': 0, 'count': 0} for _ in range(MA_TOUCH_TRACK_DAYS)]
    ma_touch_total = {'count': 0, 'wins': 0, 'total_pct': 0.0}

    total_count = 0
    update_count = 0

    for row in source_data:
        code = str(row.get('Code', '')).strip()
        name = str(row.get('Name', '')).strip()
        period = str(row.get('Period', '')).strip()
        market = str(row.get('Market', ''))
        
        if not code or not period: continue
        
        dates = re.split(r'[~-～]', period)
        if len(dates) < 2: continue
        
        s_date = parse_roc_date(dates[0])
        e_date = parse_roc_date(dates[1])
        
        if not s_date or not e_date: continue
        
        if e_date < one_year_ago: continue 
        if e_date > today: continue 

        result = fetch_stock_data(code, s_date, e_date, market)
        
        if not result: continue
            
        release_date_str = result['release_date']
        key = f"{code}_{release_date_str}"
        
        row_vals = []
        need_rerun = True
        if key in existing_map and existing_map[key]['done']:
            old_row = existing_map[key]['data']
            if old_row.get('法人動向', '') != "":
                row_vals = [old_row.get(h, "") for h in header]
                need_rerun = False
        
        if need_rerun:
            row_vals = [
                release_date_str, code, name, result['status'], result['inst_status'],
                result['pre_pct'], result['in_pct'], result['acc_pct']
            ] + result['daily_trends']
            update_count += 1
            print(f"  ✨ ({update_count}) 更新: {result['release_date']} {code} {name} | {result['status']} | {result['inst_status']}")
        
        processed_list.append(row_vals)

        stat_status = row_vals[3] 
        inst_tag = row_vals[4]    
        acc_pct_str = row_vals[7] 
        
        # 初始化組合鍵
        combo_key = (stat_status, inst_tag)
        if combo_key not in combo_interval_data:
            combo_interval_data[combo_key] = {cp: [] for cp in interval_checkpoints}
            combo_interval_data[combo_key]['total_pct_sum'] = 0.0
            combo_interval_data[combo_key]['count'] = 0

        try:
            acc_val = float(acc_pct_str.replace('%', '').replace('+', ''))
            
            if stat_status in summary_stats:
                summary_stats[stat_status]['count'] += 1
                summary_stats[stat_status]['total_pct'] += acc_val
                if acc_val > 0: summary_stats[stat_status]['wins'] += 1
            
            if inst_tag in inst_stats_data:
                inst_stats_data[inst_tag]['count'] += 1
                inst_stats_data[inst_tag]['total_pct'] += acc_val
                if acc_val > 0: inst_stats_data[inst_tag]['wins'] += 1
            
            # 更新組合總計
            combo_interval_data[combo_key]['count'] += 1
            combo_interval_data[combo_key]['total_pct_sum'] += acc_val

        except: pass
            
        if stat_status in daily_stats:
            current_compound = 1.0 
            for day_idx in range(track_days):
                col_idx = 8 + day_idx 
                if col_idx < len(row_vals):
                    val_str = row_vals[col_idx]
                    if val_str:
                        try:
                            daily_val = float(val_str.replace('%', '').replace('+', ''))
                            daily_stats[stat_status][day_idx]['count'] += 1
                            daily_stats[stat_status][day_idx]['sum'] += daily_val
                            if daily_val > 0: daily_stats[stat_status][day_idx]['wins'] += 1
                            
                            current_compound *= (1 + daily_val / 100)
                            current_day = day_idx + 1
                            if current_day in interval_checkpoints:
                                ret = (current_compound - 1) * 100
                                # 狀態區間
                                interval_data[stat_status][current_day].append(ret)
                                # 組合區間 (新增)
                                combo_interval_data[combo_key][current_day].append(ret)
                        except: pass

        # ============================
        # [新增] 月線回測統計追蹤
        # ============================
        ma_touch_returns = result.get('ma_touch_returns')
        if ma_touch_returns is not None:
            compound_ma = 1.0
            last_valid_acc_ma = 0.0
            has_any_ma = False
            for day_idx, ret in enumerate(ma_touch_returns):
                if ret is not None:
                    ma_touch_daily[day_idx]['count'] += 1
                    ma_touch_daily[day_idx]['sum'] += ret
                    if ret > 0:
                        ma_touch_daily[day_idx]['wins'] += 1
                    compound_ma *= (1 + ret / 100)
                    last_valid_acc_ma = (compound_ma - 1) * 100
                    has_any_ma = True
            if has_any_ma:
                ma_touch_total['count'] += 1
                ma_touch_total['total_pct'] += last_valid_acc_ma
                if last_valid_acc_ma > 0:
                    ma_touch_total['wins'] += 1
        
        total_count += 1

    processed_list.sort(key=lambda x: x[0], reverse=True)
    
    print("📊 正在計算彙整統計數據...")
    right_side_rows = []
    
    right_side_rows.append(["", "📊 狀態總覽 (一年期回測)", "個股數", "D+20勝率", "D+20平均", "", "", "", ""])
    for s in status_order:
        t = summary_stats[s]['count']
        w = summary_stats[s]['wins']
        avg = summary_stats[s]['total_pct'] / t if t > 0 else 0
        wr = (w / t * 100) if t > 0 else 0
        right_side_rows.append(["", s, t, f"{wr:.1f}%", f"{avg:+.1f}%", "", "", "", ""])

    right_side_rows.append([""] * 9) 
    days_header = [f"D+{i+1}" for i in range(track_days)]

    right_side_rows.append(["", "📈 平均漲跌幅 (每日)"] + days_header)
    for s in status_order:
        row_vals = ["", s]
        for d in range(track_days):
            data = daily_stats[s][d]
            if data['count'] > 0:
                avg = data['sum'] / data['count']
                row_vals.append(f"{avg:+.1f}%")
            else:
                row_vals.append("-")
        right_side_rows.append(row_vals)

    right_side_rows.append([""] * (2 + track_days)) 

    right_side_rows.append(["", "🏆 每日勝率 (每日)"] + days_header)
    for s in status_order:
        row_vals = ["", s]
        for d in range(track_days):
            data = daily_stats[s][d]
            if data['count'] > 0:
                wr = (data['wins'] / data['count']) * 100
                row_vals.append(f"{wr:.1f}%")
            else:
                row_vals.append("-")
        right_side_rows.append(row_vals)
        
    right_side_rows.append([""] * (2 + track_days)) 

    interval_header = ["D+5", "D+10", "D+15", "D+20"]
    right_side_rows.append(["", "🏆 每5日累計勝率"] + interval_header)
    for s in status_order:
        row_vals = ["", s]
        for cp in interval_checkpoints:
            data_list = interval_data[s][cp]
            if data_list:
                wins = sum(1 for x in data_list if x > 0)
                total = len(data_list)
                wr = (wins / total * 100)
                row_vals.append(f"{wr:.1f}%")
            else:
                row_vals.append("-")
        right_side_rows.append(row_vals)

    right_side_rows.append([""] * (2 + 4))

    right_side_rows.append(["", "📈 每5日累計漲跌"] + interval_header)
    for s in status_order:
        row_vals = ["", s]
        for cp in interval_checkpoints:
            data_list = interval_data[s][cp]
            if data_list:
                avg = sum(data_list) / len(data_list)
                row_vals.append(f"{avg:+.1f}%")
            else:
                row_vals.append("-")
        right_side_rows.append(row_vals)

    right_side_rows.append([""] * (2 + 4))

    right_side_rows.append(["", "📊 法人籌碼統計 (D+20)", "個股數", "勝率", "平均漲幅"])
    for i in inst_order:
        d = inst_stats_data[i]
        t = d['count']
        wr = (d['wins'] / t * 100) if t > 0 else 0.0
        avg = d['total_pct'] / t if t > 0 else 0.0
        right_side_rows.append(["", i, t, f"{wr:.1f}%", f"{avg:+.1f}%"])

    right_side_rows.append([""] * 5)

    # 📌 [修正] 狀態+法人 組合統計 (擴充顯示每5日勝率)
    combo_header = ["D+5勝率", "D+10勝率", "D+15勝率", "D+20勝率"]
    right_side_rows.append(["", "📊 狀態+法人 組合統計", "個股數"] + combo_header + ["D+20平均漲跌"])
    
    for s in status_order:
        for i in inst_order:
            combo_key = (s, i)
            if combo_key in combo_interval_data:
                d = combo_interval_data[combo_key]
                t = d['count']
                if t > 0:
                    display_name = f"{s} + {i}"
                    row_vals = ["", display_name, t]
                    
                    # 填入區間勝率
                    for cp in interval_checkpoints:
                        data_list = d[cp]
                        if data_list:
                            wins = sum(1 for x in data_list if x > 0)
                            wr = (wins / len(data_list) * 100)
                            row_vals.append(f"{wr:.1f}%")
                        else:
                            row_vals.append("-")
                    
                    # 填入總平均
                    avg = d['total_pct_sum'] / t
                    row_vals.append(f"{avg:+.1f}%")
                    
                    right_side_rows.append(row_vals)

    # ============================
    # [新增] 月線回測統計輸出區塊
    # 條件篩選：上漲進處置 + 處置期間月線(MA20)斜率>1 + 處置期間跌到月線
    # 統計：觸碰月線當天收盤後的 D+1~D+10 每日勝率與平均漲跌幅
    # ============================
    right_side_rows.append([""] * (2 + MA_TOUCH_TRACK_DAYS))

    t_ma = ma_touch_total['count']
    wr_ma_overall = (ma_touch_total['wins'] / t_ma * 100) if t_ma > 0 else 0.0
    avg_ma_overall = ma_touch_total['total_pct'] / t_ma if t_ma > 0 else 0.0

    ma_days_header = [f"D+{i+1}" for i in range(MA_TOUCH_TRACK_DAYS)]

    right_side_rows.append([
        "",
        f"📈 上漲進處置 + 月線(MA20)斜率>1 + 處置期間接近月線±15% (共{t_ma}筆 | 整體勝率{wr_ma_overall:.1f}% | D+10累積平均{avg_ma_overall:+.1f}%)"
    ])
    right_side_rows.append(["", "篩選條件：處置前漲幅>0% 且 處置期間MA20每日上升>1點 且 處置期間存在 MA20×0.85 ≤ 收盤價 ≤ MA20×1.15 的交易日"])
    right_side_rows.append(["", "計算基準：第一個符合條件當天的收盤價，往後追蹤 D+1~D+10"])
    right_side_rows.append(["", ""] + ma_days_header)


    # 每日平均漲跌幅
    avg_row_ma = ["", "平均漲跌幅"]
    for d in range(MA_TOUCH_TRACK_DAYS):
        data = ma_touch_daily[d]
        if data['count'] > 0:
            avg_row_ma.append(f"{data['sum'] / data['count']:+.1f}%")
        else:
            avg_row_ma.append("-")
    right_side_rows.append(avg_row_ma)

    # 每日勝率
    wr_row_ma = ["", "勝率"]
    for d in range(MA_TOUCH_TRACK_DAYS):
        data = ma_touch_daily[d]
        if data['count'] > 0:
            wr_row_ma.append(f"{data['wins'] / data['count'] * 100:.1f}%")
        else:
            wr_row_ma.append("-")
    right_side_rows.append(wr_row_ma)

    # 各日樣本數
    cnt_row_ma = ["", "樣本數"]
    for d in range(MA_TOUCH_TRACK_DAYS):
        cnt_row_ma.append(str(ma_touch_daily[d]['count']))
    right_side_rows.append(cnt_row_ma)

    final_header = header + [""] * (3 + track_days) 
    final_output = [final_header]
    max_rows = max(len(processed_list), len(right_side_rows))
    
    for i in range(max_rows):
        if i < len(processed_list): left_part = processed_list[i]
        else: left_part = [""] * 28 
        if i < len(right_side_rows): right_part = right_side_rows[i]
        else: right_part = [""] * (3 + track_days)
        final_output.append(left_part + [""] + right_part)

    ws_dest.clear()
    ws_dest.update(final_output)

    print("🎨 更新條件格式化...")
    ranges = [
        {"sheetId": ws_dest.id, "startRowIndex": 1, "startColumnIndex": 5, "endColumnIndex": 28},
        {"sheetId": ws_dest.id, "startRowIndex": 1, "startColumnIndex": 29, "endColumnIndex": 60}
    ]

    header_rule = {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": ranges,
                "booleanRule": {
                    "condition": {"type": "TEXT_STARTS_WITH", "values": [{"userEnteredValue": "D+"}]},
                    "format": {
                        "backgroundColor": {"red": 1.0, "green": 0.9, "blue": 0.7}, 
                        "textFormat": {"bold": True}
                    }
                }
            },
            "index": 0 
        }
    }

    positive_rule = {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": ranges,
                "booleanRule": {
                    "condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "+"}]},
                    "format": {"backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8}}
                }
            },
            "index": 1
        }
    }

    negative_rule = {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": ranges,
                "booleanRule": {
                    "condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "-"}]},
                    "format": {"backgroundColor": {"red": 0.8, "green": 1.0, "blue": 0.8}}
                }
            },
            "index": 2
        }
    }

    requests = [header_rule, positive_rule, negative_rule]

    win_rate_start_row = -1
    for idx, row in enumerate(final_output):
        if len(row) > 29 and "🏆 每日勝率 (每日)" in str(row[30]):
            win_rate_start_row = idx
            break
    
    if win_rate_start_row != -1:
        start_col = 31 
        end_col = 31 + track_days
        for col_idx in range(start_col, end_col): 
            col_values = []
            valid_rows = []
            for r in range(1, 6): 
                row_idx = win_rate_start_row + r
                if row_idx < len(final_output):
                    val_str = final_output[row_idx][col_idx]
                    try:
                        val = float(val_str.replace('%', ''))
                        col_values.append(val)
                        valid_rows.append(row_idx)
                    except:
                        col_values.append(-1.0) 
                        valid_rows.append(row_idx)
            
            valid_vals = [v for v in col_values if v != -1.0]
            if valid_vals:
                max_val = max(valid_vals)
                min_val = min(valid_vals)
                for i, val in enumerate(col_values):
                    if val == -1.0: continue
                    bg_color = None
                    if val == max_val: bg_color = {"red": 1.0, "green": 0.8, "blue": 0.8} 
                    elif val == min_val: bg_color = {"red": 0.8, "green": 1.0, "blue": 0.8} 
                    if bg_color:
                        req = {
                            "repeatCell": {
                                "range": {
                                    "sheetId": ws_dest.id,
                                    "startRowIndex": valid_rows[i],
                                    "endRowIndex": valid_rows[i] + 1,
                                    "startColumnIndex": col_idx,
                                    "endColumnIndex": col_idx + 1
                                },
                                "cell": {"userEnteredFormat": {"backgroundColor": bg_color}},
                                "fields": "userEnteredFormat.backgroundColor"
                            }
                        }
                        requests.append(req)

    try:
        sh.batch_update({"requests": requests})
    except Exception as e:
        print(f"⚠️ 格式化設定失敗 (可能是權限或版本問題): {e}")

    print(f"🎉 完成！共掃描 {total_count} 筆，本次更新 {update_count} 筆。")
    print(f"📈 月線回測命中統計：共 {ma_touch_total['count']} 筆符合條件，整體勝率 {wr_ma_overall:.1f}%，D+10累積平均 {avg_ma_overall:+.1f}%")

if __name__ == "__main__":
    main()
