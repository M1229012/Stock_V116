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
# 📊 價格數據處理邏輯 (還原 K 線修正)
# ============================
def get_price_rank_info(code, period_str, market):
    """計算處置期間數據，並回傳格式化資料"""
    try:
        dates = re.split(r'[~-～]', str(period_str))
        if len(dates) < 1: return "❓ 未知", "無日期"
        
        start_date = parse_roc_date(dates[0])
        if not start_date: return "❓ 未知", "日期錯"
        
        fetch_start = start_date - timedelta(days=60)
        end_date = datetime.now() + timedelta(days=1)
        
        suffix = ".TWO" if "上櫃" in str(market) or "TPEx" in str(market) else ".TW"
        ticker = f"{code}{suffix}"
        
        # 📌 auto_adjust=True 使用還原 K 線
        df = yf.Ticker(ticker).history(start=fetch_start.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), auto_adjust=True)
        
        if df.empty:
            alt_suffix = ".TW" if suffix == ".TWO" else ".TWO"
            df = yf.Ticker(f"{code}{alt_suffix}").history(start=fetch_start.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), auto_adjust=True)
            if df.empty: return "❓ 未知", "無股價"

        df.index = df.index.tz_localize(None)
        df_in_jail = df[df.index >= pd.Timestamp(start_date)]
        jail_days_count = len(df_in_jail) if not df_in_jail.empty else 0

        # 處置前績效
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

        # 處置中績效
        if df_in_jail.empty: 
            in_jail_pct = 0.0
        else:
            jail_start_entry = df_in_jail.iloc[0]['Open']
            curr_p = df_in_jail['Close'].iloc[-1]
            in_jail_pct = ((curr_p - jail_start_entry) / jail_start_entry) * 100

        sign_pre = "+" if pre_jail_pct > 0 else ""
        sign_in = "+" if in_jail_pct > 0 else ""
        
        if abs(in_jail_pct) <= 5:
            status = "🧊 盤整"
        elif in_jail_pct > 5:
            status = "🔥 創高"
        else:
            status = "📉 破底"
        
        # 📌 保留「處置前」與「處置中」完整字樣
        price_data = f"處置前{sign_pre}{pre_jail_pct:.1f}% / 處置中{sign_in}{in_jail_pct:.1f}%"
        return status, price_data
        
    except Exception as e:
        print(f"⚠️ 失敗 ({code}): {e}")
        return "❓ 未知", "計算失敗"

# ============================
# 🔍 分類與監控邏輯 (依日期後股號排序)
# ============================
def check_status_split(sh, releasing_codes):
    """檢查並分類股票"""
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
    
    # 📌 排序：先依日期，再依股號
    entering_list.sort(key=lambda x: (x['days'], x['code']))
    
    def get_end_date(item):
        try: return datetime.strptime(item['period'].split('-')[1], "%Y/%m/%d")
        except: return datetime.max 
    
    in_jail_list.sort(key=lambda x: (get_end_date(x), x['code']))
    return {'entering': entering_list, 'in_jail': in_jail_list}

def check_releasing_stocks(sh):
    """檢查即將出關的股票"""
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
        
        release_date_raw = row.get('出關日期', '')
        dt = parse_roc_date(release_date_raw)
        release_date = dt.strftime("%Y/%m/%d") if dt else str(release_date_raw)

        period_str = str(row.get('處置期間', ''))
        market = str(row.get('市場', '上市'))
        
        if not days_left_str.isdigit(): continue
        days = int(days_left_str) + 1
        
        if days <= JAIL_EXIT_THRESHOLD:
            status, price_info = get_price_rank_info(code, period_str, market)
            releasing_list.append({
                "code": code, "name": name, "days": days,
                "date": release_date, "status": status, "price_info": price_info
            })
            seen_codes.add(code)
            
    releasing_list.sort(key=lambda x: (x['days'], x['code']))
    return releasing_list

# ============================
# 🚀 主程式
# ============================
def main():
    if not DISCORD_WEBHOOK_URL_TEST or "你的_DISCORD_WEBHOOK" in DISCORD_WEBHOOK_URL_TEST:
        print("❌ 請先設定 DISCORD_WEBHOOK_URL_TEST")
        return

    sh = connect_google_sheets()
    if not sh: return

    releasing_stocks = check_releasing_stocks(sh)
    releasing_codes = {item['code'] for item in releasing_stocks}
    status_data = check_status_split(sh, releasing_codes)
    entering_stocks = status_data['entering']
    in_jail_stocks = status_data['in_jail']

    # 1. 瀕臨處置 (統一純淨流排版)
    if entering_stocks:
        total = len(entering_stocks)
        chunk_size = 10 if total > 15 else 20
        for i in range(0, total, chunk_size):
            chunk = entering_stocks[i : i + chunk_size]
            desc_lines = []
            for s in chunk:
                icon = "🔥" if s['days'] == 1 else "⚠️"
                msg = "明日開始處置" if s['days'] == 1 else f"最快 {s['days']} 天進處置"
                desc_lines.append(f"{icon} **{s['code']} {s['name']}** ─ {msg}")
                desc_lines.append("") # 增加適度距離感
            embed = {"description": "\n".join(desc_lines), "color": 15158332}
            if i == 0: embed["title"] = f"🚨 注意！{total} 檔股票瀕臨處置"
            send_discord_webhook([embed])
            time.sleep(2) 

    # 2. 即將出關 (統一純淨流排版，不省略字樣)
    if releasing_stocks:
        total = len(releasing_stocks)
        chunk_size = 10 if total > 15 else 20
        for i in range(0, total, chunk_size):
            chunk = releasing_stocks[i : i + chunk_size]
            desc_lines = []
            
            if i == 0:
                desc_lines.append("`💡 說明：處置前 N 天 vs 處置中 N 天`\n`(同天數對比)`\n" + "─" * 15)
            
            for s in chunk:
                day_msg = f"剩 {s['days']} 天"
                display_date = s['date'].replace("2026/", "")
                # 📌 標題行：股號 名稱 ─ 剩餘資訊
                desc_lines.append(f"**{s['code']} {s['name']}** ─ {day_msg} ({display_date})")
                # 📌 數據行：圖示 處置前 / 處置中
                desc_lines.append(f"{s['status']} 處置前{s['price_info'].split('處置前')[1]}")
                desc_lines.append("")

            embed = {"description": "\n".join(desc_lines), "color": 3066993}
            if i == 0: embed["title"] = f"🔓 關注！{total} 檔股票即將出關"
            send_discord_webhook([embed])
            time.sleep(2)

    # 3. 處置中 (統一純淨流排版)
    if in_jail_stocks:
        total = len(in_jail_stocks)
        chunk_size = 10 if total > 15 else 20
        for i in range(0, total, chunk_size):
            chunk = in_jail_stocks[i : i + chunk_size]
            desc_lines = []
            for s in chunk:
                period_display = s['period'].replace('2026/', '').replace('-', ' ➟ ')
                desc_lines.append(f"🔒 **{s['code']} {s['name']}** ─ {period_display}")
                desc_lines.append("")
            embed = {"description": "\n".join(desc_lines), "color": 10181046}
            if i == 0: embed["title"] = f"⛓️ 監控中！{total} 檔股票正在處置"
            send_discord_webhook([embed])
            time.sleep(2)

    if not entering_stocks and not releasing_stocks and not in_jail_stocks:
        print("😴 無資料，不發送。")

if __name__ == "__main__":
    main()
