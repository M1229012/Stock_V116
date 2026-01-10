# -*- coding: utf-8 -*-
import gspread
import requests
import os
import json
import re
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# ============================
# ⚙️ 設定區
# ============================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
SHEET_NAME = "台股注意股資料庫_V33"
SERVICE_KEY_FILE = "service_key.json"

# 設定閥值
JAIL_ENTER_THRESHOLD = 2  # 剩餘 X 天內進處置就要通知
JAIL_EXIT_THRESHOLD = 5   # 剩餘 X 天內出關就要通知

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
            print("✅ Discord 推播成功！")
        else:
            print(f"❌ Discord 推播失敗: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"❌ 發送請求錯誤: {e}")

def parse_date_str(date_str):
    """解析各種格式的日期字串為 datetime object"""
    date_str = str(date_str).strip()
    formats = ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def get_merged_jail_periods(sh):
    """
    從「處置股90日明細」讀取資料，並合併同一檔股票的處置期間
    回傳字典: { 'StockCode': 'YYYY/MM/DD-YYYY/MM/DD' }
    """
    jail_map = {} # 暫存 {code: {'start': min_date, 'end': max_date}}
    
    try:
        ws = sh.worksheet("處置股90日明細")
        records = ws.get_all_records()
        
        for row in records:
            code = str(row.get('代號', '')).strip()
            period = str(row.get('處置期間', '')).strip()
            
            if not code or not period:
                continue
                
            dates = re.split(r'[~-]', period)
            if len(dates) >= 2:
                s_date = parse_date_str(dates[0])
                e_date = parse_date_str(dates[1])
                
                if s_date and e_date:
                    if code not in jail_map:
                        jail_map[code] = {'start': s_date, 'end': e_date}
                    else:
                        if s_date < jail_map[code]['start']:
                            jail_map[code]['start'] = s_date
                        if e_date > jail_map[code]['end']:
                            jail_map[code]['end'] = e_date

    except Exception as e:
        print(f"⚠️ 讀取處置明細失敗 (可能該工作表不存在): {e}")
        return {}

    final_map = {}
    for code, dates in jail_map.items():
        fmt_str = f"{dates['start'].strftime('%Y/%m/%d')}-{dates['end'].strftime('%Y/%m/%d')}"
        final_map[code] = fmt_str
        
    return final_map

# ============================
# 🔍 核心邏輯
# ============================
def check_status_split(sh, releasing_codes):
    """
    檢查並分類股票：
    1. 即將進處置 (entering)
    2. 正在處置中 (in_jail)
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
        
        # 排除已在出關名單的股票
        if code in releasing_codes:
            continue

        if code in seen_codes:
            continue

        name = row.get('名稱', '')
        days_str = str(row.get('最快處置天數', '99'))
        reason = str(row.get('處置觸發原因', ''))

        if not days_str.isdigit():
            continue

        days = int(days_str)
        is_in_jail = "處置中" in reason
        is_approaching = days <= JAIL_ENTER_THRESHOLD

        # 分類邏輯
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

    releasing_list = []
    seen_codes = set()
    
    for row in records:
        code = str(row.get('代號', '')).strip()
        
        if code in seen_codes:
            continue

        name = row.get('名稱', '')
        days_left_str = str(row.get('剩餘天數', '99'))
        release_date = row.get('出關日期', '')
        
        if not days_left_str.isdigit():
            continue
            
        days = int(days_left_str)
        
        if days <= JAIL_EXIT_THRESHOLD:
            releasing_list.append({
                "code": code,
                "name": name,
                "days": days,
                "date": release_date
            })
            seen_codes.add(code)
            
    return releasing_list

# ============================
# 🚀 主程式
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

    # 🔥 [測試模式] 如需測試請保持註解；正式上線請取消註解 🔥
    # if current_weekday > 4:
    #     print("🔕 今天是假日，暫停推播。")
    #     return

    # if current_hour != 18:
    #     print(f"🔕 非推播時間 (18點)，跳過通知。")
    #     return

    sh = connect_google_sheets()
    if not sh: return

    embeds_to_send = []

    # 1. 取得即將出關名單
    releasing_stocks = check_releasing_stocks(sh)
    releasing_codes = {item['code'] for item in releasing_stocks}

    # 2. 取得並分類 進處置/處置中 名單
    status_data = check_status_split(sh, releasing_codes)
    entering_stocks = status_data['entering']
    in_jail_stocks = status_data['in_jail']

    # --- Part 1: 即將進處置 (Entering) [最上面] ---
    if entering_stocks:
        desc_lines = []
        for s in entering_stocks:
            if s['days'] == 0:
                icon = "🔥"
                msg = "明天進處置"
            else:
                icon = "⚠️"
                msg = f"再 {s['days']} 天進處置"
            
            desc_lines.append(f"{icon} **{s['code']} {s['name']}** | `{msg}`")

        embed_entering = {
            "title": f"🚨 注意！{len(entering_stocks)} 檔股票瀕臨處置",
            "description": "\n".join(desc_lines),
            "color": 15158332, # 紅色
        }
        embeds_to_send.append(embed_entering)

    # --- Part 2: 即將出關 (Releasing) [中間] ---
    if releasing_stocks:
        desc_lines = []
        for s in releasing_stocks:
            day_msg = "明天出關" if s['days'] <= 1 else f"剩 {s['days']} 天出關"
            desc_lines.append(f"🕊️ **{s['code']} {s['name']}** | `{day_msg}` ({s['date']})")

        embed_releasing = {
            "title": f"🔓 關注！{len(releasing_stocks)} 檔股票即將出關",
            "description": "\n".join(desc_lines),
            "color": 3066993, # 綠色
        }
        embeds_to_send.append(embed_releasing)

    # --- Part 3: 正在處置中 (In Jail) [最下面] ---
    if in_jail_stocks:
        desc_lines = []
        for s in in_jail_stocks:
            desc_lines.append(f"🔒 **{s['code']} {s['name']}** | `{s['period']}`")

        embed_in_jail = {
            "title": f"⛓️ 監控中！{len(in_jail_stocks)} 檔股票正在處置",
            "description": "\n".join(desc_lines),
            "color": 10181046, # 紫色/深灰色
        }
        embeds_to_send.append(embed_in_jail)

    # --- Footer 處理 (確保時間戳記附在最後一個區塊) ---
    if embeds_to_send:
        # 取得列表最後一個 embed，加入 footer
        embeds_to_send[-1]["footer"] = {
            "text": f"資料時間: {tw_now.strftime('%Y-%m-%d %H:%M')}"
        }
        send_discord_webhook(embeds_to_send)
    else:
        print("😴 今日無符合條件的股票，不發送通知。")
        # send_discord_webhook([{"title": "測試", "description": "系統運作正常，但無股票符合條件。"}])

if __name__ == "__main__":
    main()
