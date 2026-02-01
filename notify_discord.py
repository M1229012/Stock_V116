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

# ============================
# ⚙️ 設定區
# ============================
DISCORD_WEBHOOK_URL_TEST = os.getenv("DISCORD_WEBHOOK_URL_TEST")
SHEET_NAME = "台股注意股資料庫_V33"
SERVICE_KEY_FILE = "service_key.json"

# 設定閥值
JAIL_ENTER_THRESHOLD = 3   # 剩餘 X 天內進處置就要通知
JAIL_EXIT_THRESHOLD = 5    # 剩餘 X 天內出關就要通知

# ============================
# 🛠️ 工具函式
# ============================
def connect_google_sheets():
    """連線 Google Sheets"""
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
    """發送訊息到 Discord"""
    if not embeds:
        return

    data = {
        "username": "台股處置監控機器人",
        "avatar_url": "https://cdn-icons-png.flaticon.com/512/2502/2502697.png", 
        "embeds": embeds
    }

    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL_TEST, 
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
    """專門解析民國年格式"""
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
    """從「處置股90日明細」讀取並合併處置期間"""
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
# 📌 視覺優化：動態週期 + 盤整判定(5%)
# ============================
def get_price_rank_info(code, period_str, market):
    """
    計算處置期間數據，並回傳單行字串
    """
    try:
        dates = re.split(r'[~-～]', str(period_str))
        if len(dates) < 1: return "無日期"
        
        start_date = parse_roc_date(dates[0])
        if not start_date: return "日期錯"
        
        # 📌 加大抓取範圍 (60天)，確保如果處置很久，還能抓到足夠的「前 N 天」
        fetch_start = start_date - timedelta(days=60)
        end_date = datetime.now() + timedelta(days=1)
        
        suffix = ".TWO" if "上櫃" in str(market) or "TPEx" in str(market) else ".TW"
        ticker = f"{code}{suffix}"
        
        df = yf.Ticker(ticker).history(start=fetch_start.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), auto_adjust=False)
        
        if df.empty:
            alt_suffix = ".TW" if suffix == ".TWO" else ".TWO"
            df = yf.Ticker(f"{code}{alt_suffix}").history(start=fetch_start.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), auto_adjust=False)
            if df.empty: return "無股價"

        # 🔧 關鍵修正：移除 yfinance 的時區資訊
        df.index = df.index.tz_localize(None)

        # 準備資料：處置期間 DataFrame
        df_in_jail = df[df.index >= pd.Timestamp(start_date)]
        
        # 1. 取得處置天數 N (Trading Days)
        if df_in_jail.empty:
            jail_days_count = 0
        else:
            jail_days_count = len(df_in_jail)

        # =========================================================
        # 2. 計算【處置前熱度】(入獄前 N 日開盤 ~ 入獄前 1 日收盤)
        # =========================================================
        mask_before_jail = df.index < pd.Timestamp(start_date)
        if not mask_before_jail.any(): 
            pre_jail_pct = 0.0
            jail_base_price = 0 
        else:
            jail_base_date = df[mask_before_jail].index[-1]
            jail_base_price = df.loc[jail_base_date]['Close'] # 入獄前1日收盤 (賣點)

            # 找出入獄前第 N 個交易日 (動態調整)
            # 如果 jail_days_count 為 0 (例如剛開盤), 預設抓前 1 天
            lookback_days = max(1, jail_days_count)
            
            loc_idx = df.index.get_loc(jail_base_date)
            # 要往前推 N 天 (包含 base date 本身是第 0 天的話... 不，是往前數)
            target_idx = loc_idx - lookback_days + 1
            
            if target_idx >= 0:
                start_row_pre = df.iloc[target_idx]
                pre_n_entry = start_row_pre['Open'] # 📌 統一抓開盤價
                
                pre_jail_pct = ((jail_base_price - pre_n_entry) / pre_n_entry) * 100
            else:
                pre_jail_pct = 0.0

        # =========================================================
        # 3. 計算【處置期間績效】(處置第 1 日開盤 ~ 目前最新收盤)
        # =========================================================
        if df_in_jail.empty: 
            in_jail_pct = 0.0
        else:
            start_row_in = df_in_jail.iloc[0] # 處置第1天 K 棒
            jail_start_entry = start_row_in['Open'] # 📌 統一抓開盤價
            
            curr_p = df_in_jail['Close'].iloc[-1] # 目前最新收盤
            
            in_jail_pct = ((curr_p - jail_start_entry) / jail_start_entry) * 100

        # ----------------------------------------------------
        # 💡 圖示修正：依照漲跌幅 ±5% 判定
        # ----------------------------------------------------
        sign_pre = "+" if pre_jail_pct > 0 else ""
        sign_in = "+" if in_jail_pct > 0 else ""
        
        # 📌 新邏輯：5% 區間內算盤整，超過算創高/破底
        if abs(in_jail_pct) <= 5:
            status = "🧊盤整"
        elif in_jail_pct > 5:
            status = "🔥創高"
        else:
            status = "📉破底"
        
        # 格式：🔥創高｜`處置前+25.3% 期間+10.5%`
        return f"{status}｜`處置前{sign_pre}{pre_jail_pct:.1f}% 處置中{sign_in}{in_jail_pct:.1f}%`"
        
    except Exception as e:
        print(f"⚠️ 失敗: {e}")
        return "計算失敗"

# ============================
# 🔍 核心邏輯
# ============================
def check_status_split(sh, releasing_codes):
    """檢查並分類股票"""
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
    """檢查即將出關的股票"""
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

# ============================
# 🚀 主程式 (修正：註解加上反引號縮小)
# ============================
def main():
    if not DISCORD_WEBHOOK_URL_TEST or "你的_DISCORD_WEBHOOK" in DISCORD_WEBHOOK_URL_TEST:
        print("❌ 請先設定 DISCORD_WEBHOOK_URL_TEST")
        return

    utc_now = datetime.utcnow()
    current_weekday = (utc_now + timedelta(hours=8)).weekday()
    print(f"🕒 目前台灣時間: 星期{current_weekday+1}")

    sh = connect_google_sheets()
    if not sh: return

    # 1. 取得資料
    releasing_stocks = check_releasing_stocks(sh)
    releasing_codes = {item['code'] for item in releasing_stocks}
    status_data = check_status_split(sh, releasing_codes)
    entering_stocks = status_data['entering']
    in_jail_stocks = status_data['in_jail']

    # --- 第一段: 🚨 瀕臨處置 (統一分段) ---
    if entering_stocks:
        total = len(entering_stocks)
        chunk_size = 10 if total > 15 else 20
        print(f"📤 發送瀕臨處置 ({total} 檔)...")
        
        for i in range(0, total, chunk_size):
            chunk = entering_stocks[i : i + chunk_size]
            desc_lines = []
            for s in chunk:
                if s['days'] == 1:
                    icon = "🔥"; msg = "明日開始處置"
                else:
                    icon = "⚠️"; msg = f"最快 {s['days']} 天進處置"
                desc_lines.append(f"{icon} **{s['code']} {s['name']}** | `{msg}`")
            
            embed = {
                "description": "\n".join(desc_lines),
                "color": 15158332,
            }
            if i == 0: embed["title"] = f"🚨 注意！{total} 檔股票瀕臨處置"
            
            send_discord_webhook([embed])
            time.sleep(2) 

    # --- 第二段: 🔓 即將出關 (統一分段 + 註解更新) ---
    if releasing_stocks:
        total = len(releasing_stocks)
        chunk_size = 10 if total > 15 else 20
        print(f"📤 發送即將出關 ({total} 檔)...")

        for i in range(0, total, chunk_size):
            chunk = releasing_stocks[i : i + chunk_size]
            desc_lines = []
            
            # 📌 修正：加上反引號 (`) 讓字體變小並與下方風格統一
            if i == 0:
                desc_lines.append("`💡 說明：處置前 N 天 vs 處置中 N 天 (同天數對比)`\n" + "─" * 15)

            for s in chunk:
                day_msg = "明天出關" if s['days'] <= 1 else f"剩 {s['days']} 天出關"
                desc_lines.append(f"🕊️ **{s['code']} {s['name']}** | `{day_msg}` ({s['date']})\n╰ {s['rank_info']}")
            
            embed = {
                "description": "\n".join(desc_lines),
                "color": 3066993,
            }
            if i == 0: embed["title"] = f"🔓 關注！{total} 檔股票即將出關"

            send_discord_webhook([embed])
            time.sleep(2)

    # --- 第三段: ⛓️ 處置中 (維持統一分段) ---
    if in_jail_stocks:
        total = len(in_jail_stocks)
        chunk_size = 10 if total > 15 else 20
        print(f"📤 發送處置中 ({total} 檔)...")
        
        for i in range(0, total, chunk_size):
            chunk = in_jail_stocks[i : i + chunk_size]
            desc_lines = [f"🔒 **{s['code']} {s['name']}** | `{s['period']}`" for s in chunk]
            
            embed = {
                "description": "\n".join(desc_lines),
                "color": 10181046
            }
            if i == 0: embed["title"] = f"⛓️ 監控中！{total} 檔股票正在處置"
            
            send_discord_webhook([embed])
            time.sleep(2)

    if not entering_stocks and not releasing_stocks and not in_jail_stocks:
        print("😴 無資料，不發送。")

if __name__ == "__main__":
    main()
