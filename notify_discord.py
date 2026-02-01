import gspread
import requests
import os
import json
import re
import time
import yfinance as yf
import pandas as pd
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
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_TEST")
SHEET_NAME = "台股注意股資料庫_V33"
SERVICE_KEY_FILE = "service_key.json"

# 設定閥值
JAIL_ENTER_THRESHOLD = 3   # 剩餘 X 天內進處置就要通知
JAIL_EXIT_THRESHOLD = 5    # 剩餘 X 天內出關就要通知 

# ⚡ 法人判斷閥值 (還原常態量能佔比)
# 維持：投信/自營商門檻 0.5%, 外資 1.0%
THRESH_FOREIGN = 0.010  # 外資 1.0%
THRESH_OTHERS  = 0.005  # 投信/自營 0.5%

# ============================
# 🛠️ 爬蟲工具函式
# ============================
def get_driver():
    """初始化 Selenium Driver (強化偽裝模式)"""
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # 防止被偵測為自動化程式
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--disable-blink-features=AutomationControlled')
    
    options.page_load_strategy = 'eager'
    prefs = {"profile.managed_default_content_settings.images": 2} 
    options.add_experimental_option("prefs", prefs)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def is_valid_date_row(s: str) -> bool:
    return re.match(r"^\d{2,4}[/-]\d{1,2}[/-]\d{1,2}$", str(s).strip()) is not None

def roc_to_datestr(d_str: str) -> str | None:
    parts = re.split(r"[/-]", str(d_str).strip())
    if len(parts) < 2: return None
    y = int(parts[0])
    if y < 1911: y += 1911
    m = int(parts[1])
    d = int(parts[2]) if len(parts) > 2 else 1
    return f"{y:04d}-{m:02d}-{d:02d}"

def get_institutional_data(stock_id, start_date, end_date):
    """爬取富邦證券的個股法人買賣超"""
    driver = get_driver()
    url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcl/zcl.djhtm?a={stock_id}&c={start_date}&d={end_date}"
    
    try:
        driver.get(url)
        time.sleep(2) 
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        
        html = driver.page_source
        tables = pd.read_html(StringIO(html))
        
        target_df = None
        for df in tables:
            if len(df.columns) >= 4 and len(df) > 2:
                if df.astype(str).apply(lambda x: x.str.contains('外資', na=False)).any().any():
                    target_df = df
                    break
        
        if target_df is not None:
            clean_df = target_df.copy()
            if '外資' not in str(clean_df.columns):
                clean_df.columns = clean_df.iloc[0]
                clean_df = clean_df[1:]
            
            clean_df = clean_df.iloc[:, 0:4]
            clean_df.columns = ['日期', '外資買賣超', '投信買賣超', '自營商買賣超']
            clean_df = clean_df[clean_df['日期'].apply(is_valid_date_row)]
            
            for col in ['外資買賣超', '投信買賣超', '自營商買賣超']:
                clean_df[col] = clean_df[col].astype(str).str.replace(',', '').str.replace('+', '').str.replace('nan', '0')
                clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce').fillna(0)

            clean_df['DateStr'] = clean_df['日期'].apply(roc_to_datestr)
            return clean_df.dropna(subset=['DateStr'])
            
    except Exception as e:
        print(f"⚠️ 爬蟲失敗 ({stock_id}): {e}")
        return None
    finally:
        try: driver.quit()
        except: pass

# ============================
# 🛠️ 原有工具函式
# ============================
def connect_google_sheets():
    try:
        if not os.path.exists(SERVICE_KEY_FILE): return None
        gc = gspread.service_account(filename=SERVICE_KEY_FILE)
        return gc.open(SHEET_NAME)
    except: return None

def send_discord_webhook(embeds):
    if not embeds: return
    data = {"username": "台股處置監控機器人", "avatar_url": "https://cdn-icons-png.flaticon.com/512/2502/2502697.png", "embeds": embeds}
    try: requests.post(DISCORD_WEBHOOK_URL, data=json.dumps(data), headers={"Content-Type": "application/json"})
    except: pass

def parse_roc_date(date_str):
    s = str(date_str).strip()
    match = re.match(r'^(\d{2,3})[/-](\d{1,2})[/-](\d{1,2})$', s)
    if match:
        y, m, d = map(int, match.groups())
        if y < 1911: return datetime(y + 1911, m, d)
        return datetime(y, m, d)
    formats = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]
    for fmt in formats:
        try: return datetime.strptime(s, fmt)
        except: continue
    return None

def get_merged_jail_periods(sh):
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
                s_date = parse_roc_date(dates[0])
                e_date = parse_roc_date(dates[1])
                if s_date and e_date:
                    if e_date < today: continue
                    if code not in jail_map: jail_map[code] = {'start': s_date, 'end': e_date}
                    else:
                        if s_date < jail_map[code]['start']: jail_map[code]['start'] = s_date
                        if e_date > jail_map[code]['end']: jail_map[code]['end'] = e_date
    except: return {}
    
    final_map = {}
    for code, dates in jail_map.items():
        final_map[code] = f"{dates['start'].strftime('%Y/%m/%d')}-{dates['end'].strftime('%Y/%m/%d')}"
    return final_map

# ============================
# 📌 核心邏輯
# ============================
def get_price_rank_info(code, period_str, market):
    """
    回傳 Tuple: (狀態ICON, 狀態文字, 價格數據字串, 法人資訊字串)
    """
    try:
        dates = re.split(r'[~-～]', str(period_str))
        if len(dates) < 1: return "❓", "未知", "無日期", ""
        start_date = parse_roc_date(dates[0])
        if not start_date: return "❓", "未知", "日期錯", ""
        
        fetch_start = start_date - timedelta(days=60)
        end_date = datetime.now() + timedelta(days=1)
        suffix = ".TWO" if "上櫃" in str(market) or "TPEx" in str(market) else ".TW"
        ticker = f"{code}{suffix}"
        
        df = yf.Ticker(ticker).history(start=fetch_start.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), auto_adjust=False)
        if df.empty:
            alt_suffix = ".TW" if suffix == ".TWO" else ".TWO"
            df = yf.Ticker(f"{code}{alt_suffix}").history(start=fetch_start.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), auto_adjust=False)
            if df.empty: return "❓", "未知", "無股價", ""

        df.index = df.index.tz_localize(None)
        
        # 切分處置期間
        df_in_jail = df[df.index >= pd.Timestamp(start_date)]
        
        # 切分處置前
        mask_before_jail = df.index < pd.Timestamp(start_date)
        df_before_jail = df[mask_before_jail]
        
        # 1. 計算處置天數
        if df_in_jail.empty:
            jail_days_count = 0
            total_volume_in_jail = 0 
        else:
            jail_days_count = len(df_in_jail)
            total_volume_in_jail = df_in_jail['Volume'].sum()

        # 2. 計算處置前的漲跌幅 與 常態均量
        if df_before_jail.empty: 
            pre_jail_pct = 0.0
            pre_jail_avg_volume = 0
        else:
            # 使用 20 日均量 (月均量) 作為常態基準
            days_to_avg = min(20, len(df_before_jail))
            pre_jail_avg_volume = df_before_jail['Volume'].tail(days_to_avg).mean()

            jail_base_date = df_before_jail.index[-1]
            jail_base_price = df.loc[jail_base_date]['Close']
            lookback_days = max(1, jail_days_count)
            loc_idx = df.index.get_loc(jail_base_date)
            target_idx = loc_idx - lookback_days + 1
            if target_idx >= 0:
                pre_n_entry = df.iloc[target_idx]['Open']
                pre_jail_pct = ((jail_base_price - pre_n_entry) / pre_n_entry) * 100
            else:
                pre_jail_pct = 0.0

        # 3. 計算處置中的漲跌幅
        if df_in_jail.empty: in_jail_pct = 0.0
        else:
            jail_start_entry = df_in_jail.iloc[0]['Open']
            curr_p = df_in_jail['Close'].iloc[-1]
            in_jail_pct = ((curr_p - jail_start_entry) / jail_start_entry) * 100

        sign_pre = "+" if pre_jail_pct > 0 else ""
        sign_in = "+" if in_jail_pct > 0 else ""
        
        # 回傳「圖示」與「文字」
        if abs(in_jail_pct) <= 5: 
            status_icon = "🧊"
            status_text = "盤整"
        elif in_jail_pct > 5: 
            status_icon = "🔥"
            status_text = "創高"
        else: 
            status_icon = "📉"
            status_text = "破底"
        
        # 價格字串 (維持雙膠囊格式，主程式會再處理)
        price_data = f"`處置前{sign_pre}{pre_jail_pct:.0f}%` `處置中{sign_in}{in_jail_pct:.0f}%`"

        # ==========================================
        # 🔥 法人判斷
        # ==========================================
        inst_msg = ""
        if total_volume_in_jail > 0 and pre_jail_avg_volume > 0:
            crawl_start = start_date.strftime("%Y-%m-%d")
            crawl_end = datetime.now().strftime("%Y-%m-%d")
            inst_df = get_institutional_data(code, crawl_start, crawl_end)
            
            if inst_df is not None and not inst_df.empty:
                sum_foreign = inst_df['外資買賣超'].sum()
                sum_trust = inst_df['投信買賣超'].sum()
                sum_dealer = inst_df['自營商買賣超'].sum()
                
                benchmark_lots = (pre_jail_avg_volume * jail_days_count) / 1000
                if benchmark_lots == 0: benchmark_lots = 1 

                ratio_foreign = sum_foreign / benchmark_lots
                ratio_trust = sum_trust / benchmark_lots
                ratio_dealer = sum_dealer / benchmark_lots
                
                is_foreign_buy = ratio_foreign > THRESH_FOREIGN
                is_foreign_sell = ratio_foreign < -THRESH_FOREIGN
                
                is_trust_buy = ratio_trust > THRESH_OTHERS
                is_trust_sell = ratio_trust < -THRESH_OTHERS
                
                is_dealer_buy = ratio_dealer > THRESH_OTHERS
                is_dealer_sell = ratio_dealer < -THRESH_OTHERS

                # 共識與個別表態判斷
                if is_foreign_buy and is_trust_buy and is_dealer_buy:
                    inst_msg = "🔥 三大法人累計買超"
                elif is_foreign_sell and is_trust_sell and is_dealer_sell:
                    inst_msg = "🧊 三大法人累計賣超"
                else:
                    msgs = []
                    if is_trust_buy: msgs.append("投信買")
                    elif is_trust_sell: msgs.append("投信賣")
                    
                    if is_foreign_buy: msgs.append("外資買")
                    elif is_foreign_sell: msgs.append("外資賣")
                    
                    if is_dealer_buy: msgs.append("自營買")
                    elif is_dealer_sell: msgs.append("自營賣")
                    
                    if msgs:
                        if all("賣" in m for m in msgs):
                            inst_msg = "🧊 **" + " ".join(msgs) + "**"
                        elif all("買" in m for m in msgs):
                            inst_msg = "🔥 **" + " ".join(msgs) + "**"
                        else:
                            inst_msg = "🔄 **" + " ".join(msgs) + "**"

        return status_icon, status_text, price_data, inst_msg
        
    except Exception as e:
        print(f"⚠️ 失敗: {e}")
        return "❓", "未知", "Error", ""

# ============================
# 🔍 核心邏輯
# ============================
def check_status_split(sh, releasing_codes):
    try:
        ws = sh.worksheet("近30日熱門統計")
        records = ws.get_all_records()
    except: return {'entering': [], 'in_jail': []}

    jail_period_map = get_merged_jail_periods(sh)
    entering_list = []; in_jail_list = []; seen_codes = set()
    
    for row in records:
        code = str(row.get('代號', '')).replace("'", "").strip()
        if code in releasing_codes or code in seen_codes: continue
        name = row.get('名稱', '')
        days_str = str(row.get('最快處置天數', '99'))
        reason = str(row.get('處置觸發原因', ''))
        if not days_str.isdigit(): continue
        days = int(days_str) + 1  
        
        is_in_jail = "處置中" in reason
        is_approaching = days <= JAIL_ENTER_THRESHOLD

        if is_in_jail:
            period = jail_period_map.get(code, "日期未知")
            in_jail_list.append({"code": code, "name": name, "period": period})
            seen_codes.add(code)
        elif is_approaching:
            entering_list.append({"code": code, "name": name, "days": days})
            seen_codes.add(code)
    
    entering_list.sort(key=lambda x: x['days'])
    def get_end_date(item):
        try: return datetime.strptime(item['period'].split('-')[1], "%Y/%m/%d")
        except: return datetime.max 
    in_jail_list.sort(key=get_end_date)
    return {'entering': entering_list, 'in_jail': in_jail_list}

def check_releasing_stocks(sh):
    try:
        ws = sh.worksheet("即將出關監控")
        if len(ws.get_all_values()) < 2: return [] 
        records = ws.get_all_records()
    except: return []

    releasing_list = []; seen_codes = set()
    for row in records:
        code = str(row.get('代號', '')).strip()
        if code in seen_codes: continue
        name = row.get('名稱', '')
        days_left_str = str(row.get('剩餘天數', '99'))
        
        # 日期格式化 (維持年份)
        release_date_raw = row.get('出關日期', '')
        dt = parse_roc_date(release_date_raw)
        if dt:
            release_date = dt.strftime("%Y/%m/%d") 
        else:
            release_date = str(release_date_raw)

        period_str = str(row.get('處置期間', ''))
        market = str(row.get('市場', '上市'))
        
        if not days_left_str.isdigit(): continue
        days = int(days_left_str) + 1
        
        if days <= JAIL_EXIT_THRESHOLD:
            # 取得分離後的數據
            status_icon, status_text, price_info, inst_info = get_price_rank_info(code, period_str, market)
            
            releasing_list.append({
                "code": code, "name": name, "days": days,
                "date": release_date,
                "status_icon": status_icon,
                "status_text": status_text, 
                "price_info": price_info, 
                "inst_info": inst_info    
            })
            seen_codes.add(code)
    releasing_list.sort(key=lambda x: x['days'])
    return releasing_list

def main():
    if not DISCORD_WEBHOOK_URL or "你的_DISCORD_WEBHOOK" in DISCORD_WEBHOOK_URL:
        print("❌ 請先設定 DISCORD_WEBHOOK_URL")
        return

    sh = connect_google_sheets()
    if not sh: return

    releasing_stocks = check_releasing_stocks(sh)
    releasing_codes = {item['code'] for item in releasing_stocks}
    status_data = check_status_split(sh, releasing_codes)
    entering_stocks = status_data['entering']
    in_jail_stocks = status_data['in_jail']

    # 1. 瀕臨處置
    if entering_stocks:
        total = len(entering_stocks)
        chunk_size = 10 if total > 15 else 20
        print(f"📤 發送瀕臨處置 ({total} 檔)...")
        for i in range(0, total, chunk_size):
            chunk = entering_stocks[i : i + chunk_size]
            desc_lines = []
            for s in chunk:
                icon = "🔥" if s['days'] == 1 else "⚠️"
                msg = "明日開始處置" if s['days'] == 1 else f"最快 {s['days']} 天進處置"
                desc_lines.append(f"{icon} **{s['code']} {s['name']}** | `{msg}`")
            embed = {"description": "\n".join(desc_lines), "color": 15158332}
            if i == 0: embed["title"] = f"🚨 注意！{total} 檔股票瀕臨處置"
            send_discord_webhook([embed])
            time.sleep(2) 

    # 2. 即將出關 (🔥 修正：第一行加粗，箭頭樣式，斜線分隔數據，並加入空行)
    if releasing_stocks:
        total = len(releasing_stocks)
        chunk_size = 10 if total > 15 else 20
        print(f"📤 發送即將出關 ({total} 檔)...")
        for i in range(0, total, chunk_size):
            chunk = releasing_stocks[i : i + chunk_size]
            desc_lines = []
            
            for s in chunk:
                day_msg = "剩 " + str(s['days']) + " 天"
                
                # Line 1: **2312 金寶  剩 4 天  2026/02/02** (加粗)
                desc_lines.append(f"**{s['code']} {s['name']} | {day_msg}   {s['date']}**")
                
                # Line 2: ▸ 📉 破底  處置前+51% / 處置中-24%
                # 清除反引號，將空格替換為 /
                clean_price = s['price_info'].replace('`', '').replace(' ', ' / ')
                desc_lines.append(f"▸ {s['status_icon']} {s['status_text']}  {clean_price}")
                
                # Line 3: ▸ 🔄 投信賣 外資買
                if s['inst_info']:
                    desc_lines.append(f"▸ {s['inst_info']}")
                
                # 🔥 依照您的最新要求，加入空行 spacer
                desc_lines.append("")

            embed = {
                "description": "\n".join(desc_lines),
                "color": 3066993,
                "title": f"🔓 關注！{total} 檔股票即將出關"
            }
            if i == 0: 
                embed["footer"] = {"text": "💡 說明：處置前 N 天 vs 處置中 N 天 (同天數對比)"}

            send_discord_webhook([embed])
            time.sleep(2)

    # 3. 處置中
    if in_jail_stocks:
        total = len(in_jail_stocks)
        chunk_size = 10 if total > 15 else 20
        print(f"📤 發送處置中 ({total} 檔)...")
        for i in range(0, total, chunk_size):
            chunk = in_jail_stocks[i : i + chunk_size]
            desc_lines = [f"🔒 **{s['code']} {s['name']}** | `{s['period']}`" for s in chunk]
            embed = {"description": "\n".join(desc_lines), "color": 10181046}
            if i == 0: embed["title"] = f"⛓️ 監控中！{total} 檔股票正在處置"
            send_discord_webhook([embed])
            time.sleep(2)

    if not entering_stocks and not releasing_stocks and not in_jail_stocks:
        print("😴 無資料，不發送。")

if __name__ == "__main__":
    main()
