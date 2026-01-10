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
                
            # 解析期間字串，例如 "2025/01/01-2025/01/12" 或 "2025/01/01~2025/01/12"
            dates = re.split(r'[~-]', period)
            if len(dates) >= 2:
                s_date = parse_date_str(dates[0])
                e_date = parse_date_str(dates[1])
                
                if s_date and e_date:
                    if code not in jail_map:
                        jail_map[code] = {'start': s_date, 'end': e_date}
                    else:
                        # 合併邏輯：取最早開始，最晚結束
                        if s_date < jail_map[code]['start']:
                            jail_map[code]['start'] = s_date
                        if e_date > jail_map[code]['end']:
                            jail_map[code]['end'] = e_date

    except Exception as e:
        print(f"⚠️ 讀取處置明細失敗 (可能該工作表不存在): {e}")
        return {}

    # 轉回字串格式
    final_map = {}
    for code, dates in jail_map.items():
        fmt_str = f"{dates['start'].strftime('%Y/%m/%d')}-{dates['end'].strftime('%Y/%m/%d')}"
        final_map[code] = fmt_str
        
    return final_map

# ============================
# 🔍 核心邏輯
# ============================
def check_danger_stocks(sh, releasing_codes):
    """
    檢查即將進入處置 + 正在處置中的股票
    releasing_codes: 已經在「即將出關」名單的股票代號集合 (用來排除)
    """
    print("🔍 檢查「即將進處置/處置中」名單...")
    try:
        ws = sh.worksheet("近30日熱門統計")
        records = ws.get_all_records()
    except Exception as e:
        print(f"⚠️ 讀取「近30日熱門統計」失敗: {e}")
        return None

    # 取得處置期間對應表
    jail_period_map = get_merged_jail_periods(sh)

    danger_list = []
    seen_codes = set() # 用來防止同一支股票被推播兩次
    
    for row in records:
        code = str(row.get('代號', '')).replace("'", "").strip()
        
        # 1. 如果這支股票已經在「即將出關」名單，這裡就不要顯示 (優先權給出關名單)
        if code in releasing_codes:
            continue

        # 2. 防止重複添加
        if code in seen_codes:
            continue

        name = row.get('名稱', '')
        days_str = str(row.get('最快處置天數', '99'))
        reason = str(row.get('處置觸發原因', ''))
        risk = row.get('風險等級', '')

        if not days_str.isdigit():
            continue

        days = int(days_str)
        
        is_in_jail = "處置中" in reason
        is_approaching = days <= JAIL_ENTER_THRESHOLD

        if is_in_jail or is_approaching:
            
            display_reason = reason
            # 如果是處置中，嘗試附加日期區間
            if is_in_jail and code in jail_period_map:
                period_str = jail_period_map[code]
                display_reason = f"{reason} ({period_str})"

            danger_list.append({
                "code": code,
                "name": name,
                "days": days,
                "reason": display_reason, 
                "risk": risk
            })
            seen_codes.add(code) # 標記已處理
    
    return danger_list

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
        
        # 防止重複
        if code in seen_codes:
            continue

        name = row.get('名稱', '')
        days_left_str = str(row.get('剩餘天數', '99'))
        release_date = row.get('出關日期', '')
        
        if not days_left_str.isdigit():
            continue
            
        days = int(days_left_str)
        
        # 假如處置股當天出關 (days < 0 或是邏輯上已過)，清單通常不會有，但若有則過濾
        # 此處保留 <= 閥值的邏輯
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

    # 時間與假日判斷 (保留平日 18:00 推播邏輯)
    utc_now = datetime.utcnow()
    tw_now = utc_now + timedelta(hours=8)
    current_hour = tw_now.hour
    current_weekday = tw_now.weekday()

    print(f"🕒 目前台灣時間: 星期{current_weekday+1}, {current_hour} 點")

    # 🔥 [測試模式] 已註解假日與時間鎖，以便立即測試 🔥
    # 假日鎖
    # if current_weekday > 4:
    #     print("🔕 今天是假日，暫停推播。")
    #     return

    # 時間鎖
    # if current_hour != 18:
    #     print(f"🔕 非推播時間 (18點)，跳過通知。")
    #     return

    sh = connect_google_sheets()
    if not sh: return

    embeds_to_send = []

    # 1. 先處理 即將出關 (取得名單以便後續排除)
    releasing_stocks = check_releasing_stocks(sh)
    # 建立一個集合，包含所有即將出關的股票代號
    releasing_codes = {item['code'] for item in releasing_stocks}

    # 2. 處理 危險股 + 處置中 (傳入排除名單)
    danger_stocks = check_danger_stocks(sh, releasing_codes)
    
    if danger_stocks:
        desc_lines = []
        for s in danger_stocks:
            # ✅ 根據狀態顯示不同文字與圖示
            if "處置中" in s['reason']:
                icon = "🔒"
                msg = "正在處置中"
            elif s['days'] == 0:
                icon = "🔥"
                msg = "明天處置"
            else:
                icon = "⚠️"
                msg = f"再 {s['days']} 天"
            
            # ✅ [修改] 加上 Markdown Code Block (`) 讓文字串打包顯示
            desc_lines.append(
                f"{icon} **{s['code']} {s['name']}** | `{msg}`\n   └ `{s['reason']}`"
            )
        
        embed_danger = {
            "title": f"🚨 注意！{len(danger_stocks)} 檔股票 處置監控報告",
            "description": "\n".join(desc_lines),
            "color": 15158332, # 紅色
            "footer": {"text": f"資料時間: {tw_now.strftime('%Y-%m-%d %H:%M')}"}
        }
        embeds_to_send.append(embed_danger)

    # 3. 放入即將出關的 Embed
    if releasing_stocks:
        desc_lines = []
        for s in releasing_stocks:
            day_msg = "明天出關" if s['days'] <= 1 else f"剩 {s['days']} 天"
            # ✅ [修改] 加上 Markdown Code Block
            desc_lines.append(
                f"🔓 **{s['code']} {s['name']}** | `{day_msg}` ({s['date']})"
            )
        
        embed_release = {
            "title": f"🕊️ 關注！{len(releasing_stocks)} 檔股票即將出關",
            "description": "\n".join(desc_lines),
            "color": 3066993, # 綠色
            "footer": {"text": "處置結束後通常會有行情波動，請留意風險。"}
        }
        embeds_to_send.append(embed_release)

    # 4. 發送
    if embeds_to_send:
        send_discord_webhook(embeds_to_send)
    else:
        print("😴 今日無符合條件的股票，不發送通知。")
        # 如果你想確認機器人是活的，可以取消下面這行的註解
        # send_discord_webhook([{"title": "測試", "description": "系統運作正常，但無股票符合條件。"}])

if __name__ == "__main__":
    main()
