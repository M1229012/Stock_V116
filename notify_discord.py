import gspread
import requests
import os
import json
import re
import time  # 📌 新增：用於控制發送間隔
import yfinance as yf # 📌 新增：用於抓取股價計算位階
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
    """
    專門解析民國年格式 (例如 115/01/09 -> 2026-01-09)
    同時兼容西元格式
    """
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
    """
    從「處置股90日明細」讀取資料，並合併同一檔股票的處置期間。
    """
    jail_map = {} 
    
    tw_now = datetime.utcnow() + timedelta(hours=8)
    today = datetime(tw_now.year, tw_now.month, tw_now.day)

    try:
        ws = sh.worksheet("處置股90日明細")
        records = ws.get_all_records()
        
        for row in records:
            code = str(row.get('代號', '')).replace("'", "").strip()
            period = str(row.get('處置期間', '')).strip()
            
            if not code or not period:
                continue
            
            dates = re.split(r'[~-～]', period)
            
            if len(dates) >= 2:
                s_date = parse_roc_date(dates[0])
                e_date = parse_roc_date(dates[1])
                
                if s_date and e_date:
                    if e_date < today:
                        continue

                    if code not in jail_map:
                        jail_map[code] = {'start': s_date, 'end': e_date}
                    else:
                        if s_date < jail_map[code]['start']:
                            jail_map[code]['start'] = s_date
                        if e_date > jail_map[code]['end']:
                            jail_map[code]['end'] = e_date

    except Exception as e:
        print(f"⚠️ 讀取處置明細失敗: {e}")
        return {}

    final_map = {}
    for code, dates in jail_map.items():
        fmt_str = f"{dates['start'].strftime('%Y/%m/%d')}-{dates['end'].strftime('%Y/%m/%d')}"
        final_map[code] = fmt_str
        
    return final_map

# ============================
# 📌 新增：股價位階計算函式
# ============================
def get_price_rank_info(code, period_str, market):
    """
    計算處置期間的價格位階
    Return: 格式化後的狀態字串 (e.g., "🔥 強勢創高 (位階 95%)")
    """
    try:
        # 1. 解析日期範圍 (從處置開始 到 今天)
        dates = re.split(r'[~-～]', str(period_str))
        if len(dates) < 1: return "無日期資料"
        
        start_date = parse_roc_date(dates[0])
        if not start_date: return "日期解析錯誤"
        
        # 結束日期設為今天 (才能包含最新的價格)
        end_date = datetime.now() + timedelta(days=1) 
        
        # 2. 判斷後綴 (TWSE: .TW, TPEx: .TWO)
        suffix = ".TWO" if "上櫃" in str(market) or "TPEx" in str(market) else ".TW"
        ticker = f"{code}{suffix}"
        
        # 3. 抓取歷史資料
        df = yf.Ticker(ticker).history(start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), auto_adjust=False)
        
        if df.empty:
            # 嘗試另一種後綴 (防呆)
            alt_suffix = ".TW" if suffix == ".TWO" else ".TWO"
            df = yf.Ticker(f"{code}{alt_suffix}").history(start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), auto_adjust=False)
            if df.empty: return "暫無股價資料"

        # 4. 計算位階
        high_p = df['High'].max()
        low_p = df['Low'].min()
        curr_p = df['Close'].iloc[-1]
        
        if high_p == low_p:
            ratio = 0.5
        else:
            ratio = (curr_p - low_p) / (high_p - low_p)
            
        pct = int(ratio * 100)
        
        # 5. 判斷狀態
        if pct >= 85:
            status = "🔥 **強勢創高**"
        elif pct <= 20:
            status = "📉 **弱勢破底**"
        else:
            status = "🧊 **區間整理**"
            
        return f"{status} (位階 {pct}%)"
        
    except Exception as e:
        print(f"⚠️ 計算位階失敗 ({code}): {e}")
        return "位階計算失敗"

# ============================
# 🔍 核心邏輯
# ============================
def check_status_split(sh, releasing_codes):
    """
    檢查並分類股票，並進行排序
    """
    print("🔍 檢查「即將進處置/處置中」名單並分類...")
    try:
        ws = sh.worksheet("近30日熱門統計")
        records = ws.get_all_records()
    except Exception as e:
        print(f"⚠️ 讀取「近30日熱門統計」失敗: {e}")
        return {'entering': [], 'in_jail': []}

    jail_period_map = get_merged_jail_periods(sh)

    entering_list = []
    in_jail_list = []
    seen_codes = set()
    
    for row in records:
        code = str(row.get('代號', '')).replace("'", "").strip()
        
        if code in releasing_codes:
            continue

        if code in seen_codes:
            continue

        name = row.get('名稱', '')
        days_str = str(row.get('最快處置天數', '99'))
        reason = str(row.get('處置觸發原因', ''))

        if not days_str.isdigit():
            continue

        # ---------------------------------------------------
        # 修正：依照指示將天數 +1，改以當下時間計算
        # ---------------------------------------------------
        days = int(days_str) + 1  
        
        is_in_jail = "處置中" in reason
        is_approaching = days <= JAIL_ENTER_THRESHOLD

        if is_in_jail:
            period_str = jail_period_map.get(code, "日期未知")
            in_jail_list.append({
                "code": code,
                "name": name,
                "period": period_str
            })
            seen_codes.add(code)
            
        elif is_approaching:
            entering_list.append({
                "code": code,
                "name": name,
                "days": days
            })
            seen_codes.add(code)
    
    entering_list.sort(key=lambda x: x['days'])
    
    def get_end_date(item):
        try:
            end_date_str = item['period'].split('-')[1]
            return datetime.strptime(end_date_str, "%Y/%m/%d")
        except:
            return datetime.max 
            
    in_jail_list.sort(key=get_end_date)

    return {'entering': entering_list, 'in_jail': in_jail_list}

def check_releasing_stocks(sh):
    """檢查即將出關的股票，並進行排序 + 計算位階"""
    print("🔍 檢查「即將出關」名單...")
    try:
        ws = sh.worksheet("即將出關監控")
        if len(ws.get_all_values()) < 2: return [] 
        records = ws.get_all_records()
    except Exception as e:
        print(f"⚠️ 讀取「即將出關監控」失敗: {e}")
        return []

    releasing_list = []
    seen_codes = set()
    
    for row in records:
        code = str(row.get('代號', '')).strip()
        
        if code in seen_codes:
            continue

        name = row.get('名稱', '')
        days_left_str = str(row.get('剩餘天數', '99'))
        release_date = row.get('出關日期', '')
        period_str = str(row.get('處置期間', ''))
        market = str(row.get('市場', '上市'))
        
        if not days_left_str.isdigit():
            continue
            
        # ---------------------------------------------------
        # 修正：依照指示將天數 +1，改以當下時間計算
        # ---------------------------------------------------
        days = int(days_left_str) + 1
        
        if days <= JAIL_EXIT_THRESHOLD:
            # 📌 計算位階資訊
            rank_info = get_price_rank_info(code, period_str, market)
            
            releasing_list.append({
                "code": code,
                "name": name,
                "days": days,
                "date": release_date,
                "rank_info": rank_info # 儲存位階資訊
            })
            seen_codes.add(code)
            
    releasing_list.sort(key=lambda x: x['days'])

    return releasing_list

# ============================
# 🚀 主程式 (修正第四次發送邏輯 + 新增延遲)
# ============================
def main():
    if not DISCORD_WEBHOOK_URL or "你的_DISCORD_WEBHOOK" in DISCORD_WEBHOOK_URL:
        print("❌ 請先設定 DISCORD_WEBHOOK_URL")
        return

    utc_now = datetime.utcnow()
    tw_now = utc_now + timedelta(hours=8)
    current_hour = tw_now.hour
    current_weekday = tw_now.weekday()

    print(f"🕒 目前台灣時間: 星期{current_weekday+1}, {current_hour} 點")

    sh = connect_google_sheets()
    if not sh: return

    # 1. 取得資料
    releasing_stocks = check_releasing_stocks(sh)
    releasing_codes = {item['code'] for item in releasing_stocks}
    status_data = check_status_split(sh, releasing_codes)
    entering_stocks = status_data['entering']
    in_jail_stocks = status_data['in_jail']

    # --- 第一段發送: 🚨 瀕臨處置股票 ---
    if entering_stocks:
        print(f"📤 正在發送瀕臨處置名單 ({len(entering_stocks)} 檔)...")
        desc_lines = []
        for s in entering_stocks:
            # 📌 修正：days=1 代表 DB值為0 ，給予明確的處置訊息
            if s['days'] == 1:
                icon = "🔥"
                msg = "明日開始處置"
            else:
                icon = "⚠️"
                msg = f"最快 {s['days']} 天進處置"
            
            desc_lines.append(f"{icon} **{s['code']} {s['name']}** | `{msg}`")
        
        entering_embed = [{
            "title": f"🚨 注意！{len(entering_stocks)} 檔股票瀕臨處置",
            "description": "\n".join(desc_lines),
            "color": 15158332,
        }]
        send_discord_webhook(entering_embed)
        # 🛑 修改：暫停 2 秒，確保 Discord 有足夠時間處理順序
        time.sleep(2) 

    # --- 第二段發送: 🔓 即將出關股票 (含位階) ---
    if releasing_stocks:
        print(f"📤 正在發送即將出關名單 ({len(releasing_stocks)} 檔)...")
        desc_lines = []
        for s in releasing_stocks:
            day_msg = "明天出關" if s['days'] <= 1 else f"剩 {s['days']} 天出關"
            # 📌 修正：格式化輸出，增加位階資訊
            desc_lines.append(f"🕊️ **{s['code']} {s['name']}** | `{day_msg}` ({s['date']})\n╰ {s['rank_info']}")

        releasing_embed = [{
            "title": f"🔓 關注！{len(releasing_stocks)} 檔股票即將出關",
            "description": "\n".join(desc_lines),
            "color": 3066993,
        }]
        send_discord_webhook(releasing_embed)
        # 🛑 修改：暫停 2 秒，確保 Discord 有足夠時間處理順序
        time.sleep(2)

    # --- 第三段(及之後)發送: ⛓️ 處置中名單 (動態判定) ---
    if in_jail_stocks:
        total_count = len(in_jail_stocks)
        
        # 💡 邏輯：超過 15 檔才分段(每10個一段)，否則維持每20個一段
        chunk_size = 10 if total_count > 15 else 20
        print(f"📤 正在發送處置中名單 (共 {total_count} 檔，分段大小: {chunk_size})...")
        
        for i in range(0, total_count, chunk_size):
            chunk = in_jail_stocks[i : i + chunk_size]
            desc_lines = [f"🔒 **{s['code']} {s['name']}** | `{s['period']}`" for s in chunk]
            
            # 判斷是否為第一段 (i=0 為第一段，其餘為接續段)
            is_first_part = (i == 0)
            
            jail_embed = {
                "description": "\n".join(desc_lines),
                "color": 10181046,
            }
            
            # 💡 只有第一段才放標題
            if is_first_part:
                jail_embed["title"] = f"⛓️ 監控中！{total_count} 檔股票正在處置"

            send_discord_webhook([jail_embed])
            # 🛑 修改：分段之間也休息 2 秒，避免最後幾段順序亂掉
            time.sleep(2)

    if not entering_stocks and not releasing_stocks and not in_jail_stocks:
        print("😴 今日無符合條件的股票，不發送通知。")

if __name__ == "__main__":
    main()
