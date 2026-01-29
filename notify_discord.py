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
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
SHEET_NAME = "台股注意股資料庫_V33"
SERVICE_KEY_FILE = "service_key.json"

# 設定閥值
JAIL_ENTER_THRESHOLD = 2   # 剩餘 X 天內進處置就要通知
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
# 📌 視覺優化：計算邏輯修正 + 小數點一位
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
        
        # 往前多抓一點確保有前 5 日數據
        fetch_start = start_date - timedelta(days=30)
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

        # =========================================================
        # 1. 計算【處置前熱度】(入獄前5日開盤 ~ 入獄前1日收盤)
        # =========================================================
        mask_before_jail = df.index < pd.Timestamp(start_date)
        if not mask_before_jail.any(): 
            pre_jail_pct = 0.0
            jail_base_price = 0 # 避免未定義變數
        else:
            jail_base_date = df[mask_before_jail].index[-1]
            jail_base_price = df.loc[jail_base_date]['Close'] # 入獄前1日收盤

            # 找出入獄前第 5 個交易日 (包含 base_date 往前數第 5 根)
            loc_idx = df.index.get_loc(jail_base_date)
            if loc_idx >= 4:
                # loc_idx 是前1日，loc_idx-4 是前5日
                pre_5d_open = df['Open'].iloc[loc_idx - 4] 
                pre_jail_pct = ((jail_base_price - pre_5d_open) / pre_5d_open) * 100
            else:
                pre_jail_pct = 0.0

        # =========================================================
        # 2. 計算【處置期間績效】(處置第1日開盤 ~ 目前最新收盤)
        # =========================================================
        df_in_jail = df[df.index >= pd.Timestamp(start_date)]
        
        if df_in_jail.empty: 
            # 如果還沒有處置期間的 K 棒 (例如剛開盤尚未抓到)，用目前的 close 暫代
            in_jail_pct = 0.0
            curr_p = df['Close'].iloc[-1]
            high_p = curr_p
            low_p = curr_p
        else:
            jail_start_open = df_in_jail['Open'].iloc[0] # 處置第1天開盤
            curr_p = df_in_jail['Close'].iloc[-1]        # 目前最新收盤
            
            in_jail_pct = ((curr_p - jail_start_open) / jail_start_open) * 100
            
            high_p = df_in_jail['High'].max()
            low_p = df_in_jail['Low'].min()
        
        # 3. 計算位階
        if high_p == low_p: ratio = 0.5
        else: ratio = (curr_p - low_p) / (high_p - low_p)
        rank_pct = int(ratio * 100)

        # ----------------------------------------------------
        # 💡 格式修正：小數點一位 (.1f)
        # ----------------------------------------------------
        sign_pre = "+" if pre_jail_pct > 0 else ""
        sign_in = "+" if in_jail_pct > 0 else ""
        
        if rank_pct >= 85: status = "🔥創高"
        elif rank_pct <= 20: status = "🟢破底"
        else: status = "🟡盤整"
        
        # 格式：🔥創高｜`處置前+25.3% 期間+10.5%`
        return f"{status}｜`處置前{sign_pre}{pre_jail_pct:.1f}% 期間{sign_in}{in_jail_pct:.1f}%`"
        
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
# 🚀 主程式
# ============================
def main():
    if not DISCORD_WEBHOOK_URL or "你的_DISCORD_WEBHOOK" in DISCORD_WEBHOOK_URL:
        print("❌ 請先設定 DISCORD_WEBHOOK_URL")
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

    # --- 第一段: 🚨 瀕臨處置 ---
    if entering_stocks:
        print(f"📤 發送瀕臨處置 ({len(entering_stocks)} 檔)...")
        desc_lines = []
        for s in entering_stocks:
            if s['days'] == 1:
                icon = "🔥"; msg = "明日開始處置"
            else:
                icon = "⚠️"; msg = f"最快 {s['days']} 天進處置"
            desc_lines.append(f"{icon} **{s['code']} {s['name']}** | `{msg}`")
        
        send_discord_webhook([{
            "title": f"🚨 注意！{len(entering_stocks)} 檔股票瀕臨處置",
            "description": "\n".join(desc_lines),
            "color": 15158332,
        }])
        time.sleep(2) 

    # --- 第二段: 🔓 即將出關 (簡潔版) ---
    if releasing_stocks:
        print(f"📤 發送即將出關 ({len(releasing_stocks)} 檔)...")
        desc_lines = []
        for s in releasing_stocks:
            day_msg = "明天出關" if s['days'] <= 1 else f"剩 {s['days']} 天出關"
            # 📌 格式：🕊️ 2330 台積電 | `明天出關` (2024-02-01)
            #           ╰ 🔥創高｜`處置前+25.3% 期間+10.5%`
            desc_lines.append(f"🕊️ **{s['code']} {s['name']}** | `{day_msg}` ({s['date']})\n╰ {s['rank_info']}")

        send_discord_webhook([{
            "title": f"🔓 關注！{len(releasing_stocks)} 檔股票即將出關",
            "description": "\n".join(desc_lines),
            "color": 3066993,
        }])
        time.sleep(2)

    # --- 第三段: ⛓️ 處置中 ---
    if in_jail_stocks:
        total = len(in_jail_stocks)
        chunk_size = 10 if total > 15 else 20
        print(f"📤 發送處置中 ({total} 檔)...")
        
        for i in range(0, total, chunk_size):
            chunk = in_jail_stocks[i : i + chunk_size]
            desc_lines = [f"🔒 **{s['code']} {s['name']}** | `{s['period']}`" for s in chunk]
            jail_embed = {"description": "\n".join(desc_lines), "color": 10181046}
            if i == 0: jail_embed["title"] = f"⛓️ 監控中！{total} 檔股票正在處置"
            send_discord_webhook([jail_embed])
            time.sleep(2)

    if not entering_stocks and not releasing_stocks and not in_jail_stocks:
        print("😴 無資料，不發送。")

if __name__ == "__main__":
    main()
