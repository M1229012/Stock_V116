import gspread
import requests
import os
import json
import re
import time
import yfinance as yf
import pandas as pd
import shutil
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
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
SHEET_NAME = "台股注意股資料庫_V33"
SERVICE_KEY_FILE = "service_key.json"

# 設定閥值
JAIL_ENTER_THRESHOLD = 3   # 剩餘 X 天內進處置就要通知
JAIL_EXIT_THRESHOLD = 5    # 剩餘 X 天內出關就要通知

# ⚡ 法人判斷閥值 (成交量佔比)
INST_RATIO_THRESHOLD = 0.03

# ============================
# 🛠️ 爬蟲工具函式 (100% 還原籌碼K線邏輯 + Debug 輸出)
# ============================

def get_driver_path():
    return ChromeDriverManager().install()

def get_driver():
    """初始化 Selenium Driver (完全還原籌碼K線 APP 設定)"""
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    # 1. 開啟 Eager 模式 (不等待資源載入完畢)
    options.page_load_strategy = 'eager'

    # 2. 禁止圖片、CSS、通知等資源載入
    prefs = {
        "profile.managed_default_content_settings.images": 2,          # 禁止圖片
        "profile.default_content_setting_values.notifications": 2,     # 禁止通知
        "profile.managed_default_content_settings.stylesheets": 2,     # 禁止 CSS
        "profile.managed_default_content_settings.cookies": 2,         # 禁止 Cookies
        "profile.managed_default_content_settings.javascript": 1,      # JS 建議開啟
        "profile.managed_default_content_settings.plugins": 1,
        "profile.managed_default_content_settings.popups": 2,
        "profile.managed_default_content_settings.geolocation": 2,
        "profile.managed_default_content_settings.media_stream": 2,
    }
    options.add_experimental_option("prefs", prefs)
    
    # 額外參數減少渲染負擔
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    
    # 自動偵測 binary 位置 (這段是關鍵，確保在不同環境都能找到瀏覽器)
    if shutil.which("chromium"):
        options.binary_location = shutil.which("chromium")
    elif shutil.which("chromium-browser"):
        options.binary_location = shutil.which("chromium-browser")
        
    if shutil.which("chromedriver"):
        service = Service(shutil.which("chromedriver"))
    else:
        service = Service(get_driver_path())

    driver = webdriver.Chrome(service=service, options=options)
    return driver

def is_roc_date(s: str) -> bool:
    return re.match(r"\d{2,3}/\d{1,2}/\d{1,2}", str(s).strip()) is not None

def roc_to_datestr(d_str: str) -> str | None:
    parts = re.split(r"[/-]", str(d_str).strip())
    if len(parts) < 2:
        return None
    y = int(parts[0])
    y = y + 1911 if y < 1911 else y
    m = int(parts[1])
    d = int(parts[2]) if len(parts) > 2 else 1
    return f"{y:04d}-{m:02d}-{d:02d}"

def get_institutional_data(stock_id, start_date, end_date):
    """
    爬取富邦證券 (完全還原籌碼K線 APP 邏輯 + Debug)
    """
    driver = get_driver()
    url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcl/zcl.djhtm?a={stock_id}&c={start_date}&d={end_date}"
    
    print(f"\n[DEBUG] 正在爬取: {stock_id}, URL: {url}") # DEBUG

    try:
        driver.get(url)
        # ⚠️ 這裡使用原本程式碼中特定的 XPath，確保抓取目標一致
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/table/tbody/tr[2]/td[2]/table/tbody/tr/td/form/table/tbody/tr/td/table/tbody/tr[8]/td[1]")))
        
        html = driver.page_source
        tables = pd.read_html(StringIO(html))
        print(f"[DEBUG] 頁面讀取成功，找到表格數: {len(tables)}") # DEBUG
        
        target_df = None
        for df in tables:
            # 透過關鍵字尋找正確的表格
            if df.astype(str).apply(lambda x: x.str.contains('外資買賣超', na=False)).any().any():
                target_df = df
                break
        
        if target_df is not None:
            print(f"[DEBUG] 找到目標表格，原始形狀: {target_df.shape}") # DEBUG
            if len(target_df.columns) >= 4:
                clean_df = target_df.iloc[:, [0, 1, 2, 3]].copy()
                clean_df.columns = ['日期', '外資買賣超', '投信買賣超', '自營商買賣超']
                
                clean_df = clean_df[clean_df['日期'].apply(is_roc_date)]
                
                # 資料清洗 (移除逗號、加號、處理 nan)
                for col in ['外資買賣超', '投信買賣超', '自營商買賣超']:
                    clean_df[col] = clean_df[col].astype(str).str.replace(',', '').str.replace('+', '').str.replace('nan', '0')
                    clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce').fillna(0)

                clean_df['DateStr'] = clean_df['日期'].apply(roc_to_datestr)
                final_df = clean_df.dropna(subset=['DateStr'])
                
                print(f"[DEBUG] 清洗後資料筆數: {len(final_df)}") # DEBUG
                if not final_df.empty:
                    print(f"[DEBUG] 最新一筆資料範例:\n{final_df.head(1)}") # DEBUG
                
                return final_df
        else:
            print("[DEBUG] ❌ 未找到包含 '外資買賣超' 的表格") # DEBUG

    except Exception as e:
        print(f"⚠️ [DEBUG] 爬蟲發生錯誤 ({stock_id}): {e}")
        pass
    finally:
        driver.quit()
    return None

# ============================
# 🛠️ 其他工具函式 (保持不變)
# ============================
def connect_google_sheets():
    try:
        if not os.path.exists(SERVICE_KEY_FILE):
            print("❌ 找不到 service_key.json")
            return None
        gc = gspread.service_account(filename=SERVICE_KEY_FILE)
        sh = gc.open(SHEET_NAME)
        return sh
    except Exception as e:
        print(f"❌ Google Sheet 連線失敗: {e}")
        return None

def send_discord_webhook(embeds):
    if not embeds:
        return
    data = {
        "username": "台股處置監控機器人",
        "avatar_url": "https://cdn-icons-png.flaticon.com/512/2502/2502697.png", 
        "embeds": embeds
    }
    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL, 
            data=json.dumps(data), 
            headers={"Content-Type": "application/json"}
        )
        if response.status_code == 204:
            print("✅ Discord 部分推播成功！")
        else:
            print(f"❌ Discord 推播失敗: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"❌ 發送請求錯誤: {e}")

def parse_roc_date(date_str):
    s = str(date_str).strip()
    match = re.match(r'^(\d{2,3})[/-](\d{1,2})[/-](\d{1,2})$', s)
    if match:
        y, m, d = map(int, match.groups())
        if y < 1911:
            return datetime(y + 1911, m, d)
        return datetime(y, m, d)
    formats = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
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
                    if code not in jail_map:
                        jail_map[code] = {'start': s_date, 'end': e_date}
                    else:
                        if s_date < jail_map[code]['start']: jail_map[code]['start'] = s_date
                        if e_date > jail_map[code]['end']: jail_map[code]['end'] = e_date
    except Exception as e:
        print(f"⚠️ 讀取處置明細失敗: {e}")
        return {}

    final_map = {}
    for code, dates in jail_map.items():
        fmt_str = f"{dates['start'].strftime('%Y/%m/%d')}-{dates['end'].strftime('%Y/%m/%d')}"
        final_map[code] = fmt_str
    return final_map

# ============================
# 📌 核心邏輯
# ============================
def get_price_rank_info(code, period_str, market):
    try:
        dates = re.split(r'[~-～]', str(period_str))
        if len(dates) < 1: return "無日期"
        
        start_date = parse_roc_date(dates[0])
        if not start_date: return "日期錯"
        
        fetch_start = start_date - timedelta(days=60)
        end_date = datetime.now() + timedelta(days=1)
        
        suffix = ".TWO" if "上櫃" in str(market) or "TPEx" in str(market) else ".TW"
        ticker = f"{code}{suffix}"
        
        df = yf.Ticker(ticker).history(start=fetch_start.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), auto_adjust=False)
        
        if df.empty:
            alt_suffix = ".TW" if suffix == ".TWO" else ".TWO"
            df = yf.Ticker(f"{code}{alt_suffix}").history(start=fetch_start.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), auto_adjust=False)
            if df.empty: return "無股價"

        df.index = df.index.tz_localize(None)
        df_in_jail = df[df.index >= pd.Timestamp(start_date)]
        
        if df_in_jail.empty:
            jail_days_count = 0
            total_volume_in_jail = 0
        else:
            jail_days_count = len(df_in_jail)
            total_volume_in_jail = df_in_jail['Volume'].sum()

        mask_before_jail = df.index < pd.Timestamp(start_date)
        if not mask_before_jail.any(): 
            pre_jail_pct = 0.0
        else:
            jail_base_date = df[mask_before_jail].index[-1]
            jail_base_price = df.loc[jail_base_date]['Close']
            lookback_days = max(1, jail_days_count)
            loc_idx = df.index.get_loc(jail_base_date)
            target_idx = loc_idx - lookback_days + 1
            
            if target_idx >= 0:
                pre_n_entry = df.iloc[target_idx]['Open']
                pre_jail_pct = ((jail_base_price - pre_n_entry) / pre_n_entry) * 100
            else:
                pre_jail_pct = 0.0

        if df_in_jail.empty: 
            in_jail_pct = 0.0
        else:
            jail_start_entry = df_in_jail.iloc[0]['Open']
            curr_p = df_in_jail['Close'].iloc[-1]
            in_jail_pct = ((curr_p - jail_start_entry) / jail_start_entry) * 100

        sign_pre = "+" if pre_jail_pct > 0 else ""
        sign_in = "+" if in_jail_pct > 0 else ""
        
        if abs(in_jail_pct) <= 5: status = "🧊盤整"
        elif in_jail_pct > 5: status = "🔥創高"
        else: status = "📉破底"
        
        base_info = f"{status}｜`處置前{sign_pre}{pre_jail_pct:.1f}% 處置中{sign_in}{in_jail_pct:.1f}%`"

        # 🔥 法人買賣超判斷
        inst_msg = ""
        
        # DEBUG: 檢查成交量是否足夠
        print(f"[DEBUG] {code} 處置期間成交量(volume_in_jail): {total_volume_in_jail}")

        # 當處置期間有量才爬蟲
        if total_volume_in_jail > 0:
            crawl_start = start_date.strftime("%Y-%m-%d")
            crawl_end = datetime.now().strftime("%Y-%m-%d")
            
            print(f"[DEBUG] 準備抓取 {code} 法人資料, 區間: {crawl_start} ~ {crawl_end}") # DEBUG

            # 使用修正後的函式抓取資料
            inst_df = get_institutional_data(code, crawl_start, crawl_end)
            
            if inst_df is not None and not inst_df.empty:
                sum_foreign = inst_df['外資買賣超'].sum()
                sum_trust = inst_df['投信買賣超'].sum()
                sum_dealer = inst_df['自營商買賣超'].sum()
                
                # yfinance Volume (股) / 1000 = 張
                volume_in_lots = total_volume_in_jail / 1000
                if volume_in_lots == 0: volume_in_lots = 1

                ratio_foreign = sum_foreign / volume_in_lots
                ratio_trust = sum_trust / volume_in_lots
                ratio_dealer = sum_dealer / volume_in_lots
                
                threshold = INST_RATIO_THRESHOLD 
                
                # DEBUG: 印出計算結果
                print(f"[DEBUG] {code} 佔比分析 (總量張數: {volume_in_lots:.0f})")
                print(f"      外資: {sum_foreign} (佔比 {ratio_foreign:.4f})")
                print(f"      投信: {sum_trust} (佔比 {ratio_trust:.4f})")
                print(f"      自營: {sum_dealer} (佔比 {ratio_dealer:.4f})")
                print(f"      門檻: {threshold}")

                if ratio_foreign > threshold and ratio_trust > threshold and ratio_dealer > threshold:
                    inst_msg = "🔥 三大法人累計買超"
                elif ratio_foreign < -threshold and ratio_trust < -threshold and ratio_dealer < -threshold:
                    inst_msg = "🟢 三大法人累計賣超"
                else:
                    msgs = []
                    if ratio_trust > threshold: msgs.append("投信累計買超")
                    elif ratio_trust < -threshold: msgs.append("投信累計賣超")
                    
                    if ratio_foreign > threshold: msgs.append("外資累計買超")
                    elif ratio_foreign < -threshold: msgs.append("外資累計賣超")
                    
                    if ratio_dealer > threshold: msgs.append("自營商累計買超")
                    elif ratio_dealer < -threshold: msgs.append("自營商累計賣超")
                    
                    if msgs:
                        if all("賣超" in m for m in msgs):
                            inst_msg = "🟢 " + " ".join(msgs)
                        else:
                            inst_msg = "🔥 " + " ".join(msgs)
            else:
                 print(f"[DEBUG] {code} 法人資料為 None 或空值") # DEBUG
        else:
             print(f"[DEBUG] {code} 成交量為 0，跳過爬蟲") # DEBUG

        if inst_msg:
            return f"{base_info}\n╰ `{inst_msg}`"
        else:
            return base_info
        
    except Exception as e:
        print(f"⚠️ 失敗 ({code}): {e}")
        return "計算失敗"

def check_status_split(sh, releasing_codes):
    print("🔍 檢查「即將進處置/處置中」名單...")
    try:
        ws = sh.worksheet("近30日熱門統計")
        records = ws.get_all_records()
    except Exception as e:
        print(f"⚠️ 讀取「近30日熱門統計」失敗: {e}")
        return {'entering': [], 'in_jail': []}

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
            period_str = jail_period_map.get(code, "日期未知")
            in_jail_list.append({"code": code, "name": name, "period": period_str})
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
    print("🔍 檢查「即將出關」名單...")
    try:
        ws = sh.worksheet("即將出關監控")
        if len(ws.get_all_values()) < 2: return [] 
        records = ws.get_all_records()
    except Exception as e:
        print(f"⚠️ 讀取「即將出關監控」失敗: {e}")
        return []

    releasing_list = []; seen_codes = set()
    
    for row in records:
        code = str(row.get('代號', '')).strip()
        if code in seen_codes: continue

        name = row.get('名稱', '')
        days_left_str = str(row.get('剩餘天數', '99'))
        release_date = row.get('出關日期', '')
        period_str = str(row.get('處置期間', ''))
        market = str(row.get('市場', '上市'))
        
        if not days_left_str.isdigit(): continue
        days = int(days_left_str) + 1
        
        if days <= JAIL_EXIT_THRESHOLD:
            rank_info = get_price_rank_info(code, period_str, market)
            releasing_list.append({
                "code": code, "name": name, "days": days,
                "date": release_date, "rank_info": rank_info
            })
            seen_codes.add(code)
            
    releasing_list.sort(key=lambda x: x['days'])
    return releasing_list

def main():
    if not DISCORD_WEBHOOK_URL or "你的_DISCORD_WEBHOOK" in DISCORD_WEBHOOK_URL:
        print("❌ 請先設定 DISCORD_WEBHOOK_URL")
        return

    utc_now = datetime.utcnow()
    current_weekday = (utc_now + timedelta(hours=8)).weekday()
    print(f"🕒 目前台灣時間: 星期{current_weekday+1}")

    sh = connect_google_sheets()
    if not sh: return

    releasing_stocks = check_releasing_stocks(sh)
    releasing_codes = {item['code'] for item in releasing_stocks}
    status_data = check_status_split(sh, releasing_codes)
    entering_stocks = status_data['entering']
    in_jail_stocks = status_data['in_jail']

    if entering_stocks:
        total = len(entering_stocks)
        chunk_size = 10 if total > 15 else 20
        print(f"📤 發送瀕臨處置 ({total} 檔)...")
        for i in range(0, total, chunk_size):
            chunk = entering_stocks[i : i + chunk_size]
            desc_lines = []
            for s in chunk:
                if s['days'] == 1: icon = "🔥"; msg = "明日開始處置"
                else: icon = "⚠️"; msg = f"最快 {s['days']} 天進處置"
                desc_lines.append(f"{icon} **{s['code']} {s['name']}** | `{msg}`")
            embed = {"description": "\n".join(desc_lines), "color": 15158332}
            if i == 0: embed["title"] = f"🚨 注意！{total} 檔股票瀕臨處置"
            send_discord_webhook([embed])
            time.sleep(2) 

    if releasing_stocks:
        total = len(releasing_stocks)
        chunk_size = 10 if total > 15 else 20
        print(f"📤 發送即將出關 ({total} 檔)...")
        for i in range(0, total, chunk_size):
            chunk = releasing_stocks[i : i + chunk_size]
            desc_lines = []
            if i == 0: desc_lines.append("`💡 說明：處置前 N 天 vs 處置中 N 天 (同天數對比)`\n" + "─" * 15)
            for s in chunk:
                day_msg = "明天出關" if s['days'] <= 1 else f"剩 {s['days']} 天出關"
                desc_lines.append(f"🕊️ **{s['code']} {s['name']}** | `{day_msg}` ({s['date']})\n╰ {s['rank_info']}")
            embed = {"description": "\n".join(desc_lines), "color": 3066993}
            if i == 0: embed["title"] = f"🔓 關注！{total} 檔股票即將出關"
            send_discord_webhook([embed])
            time.sleep(2)

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
