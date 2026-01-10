# -*- coding: utf-8 -*-
import gspread
import requests
import os
import json
from datetime import datetime
from google.oauth2.service_account import Credentials

# ============================
# ⚙️ 設定區
# ============================
# 環境變數讀取 (GitHub Secrets)
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
SHEET_NAME = "台股注意股資料庫_V33"
SERVICE_KEY_FILE = "service_key.json"

# 設定閥值
JAIL_ENTER_THRESHOLD = 2  # 剩餘 X 天內進處置就要通知 (會排除 0)
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
        print("⚠️ 沒有內容需要推播")
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

# ============================
# 🔍 核心邏輯
# ============================
def check_danger_stocks(sh):
    """檢查即將進入處置的股票 (讀取：近30日熱門統計)"""
    print("🔍 檢查「即將進處置」名單...")
    try:
        ws = sh.worksheet("近30日熱門統計")
        records = ws.get_all_records()
    except Exception as e:
        print(f"⚠️ 讀取「近30日熱門統計」失敗: {e}")
        return None

    danger_list = []
    
    for row in records:
        code = str(row.get('代號', '')).replace("'", "").strip()
        name = row.get('名稱', '')
        days_str = str(row.get('最快處置天數', '99'))
        reason = str(row.get('處置觸發原因', '')) # 雖然不推播，但邏輯判斷可能還是會用到
        risk = row.get('風險等級', '')

        # 排除掉 "X" 或空值
        if not days_str.isdigit():
            continue

        days = int(days_str)
        
        # 🛑 過濾規則 1：如果天數是 0，直接跳過 (解決處置中股票誤報問題)
        if days == 0:
            continue

        # 🛑 過濾規則 2：原因包含「處置中」也跳過 (雙重保險)
        if "處置中" in reason:
            continue
        
        # 條件：天數 <= 2 (現在只會抓到 1 和 2)
        if days <= JAIL_ENTER_THRESHOLD:
            danger_list.append({
                "code": code,
                "name": name,
                "days": days,
                "risk": risk
                # reason 已不需要存入
            })
    
    return danger_list

def check_releasing_stocks(sh):
    """檢查即將出關的股票 (讀取：即將出關監控)"""
    print("🔍 檢查「即將出關」名單...")
    try:
        ws = sh.worksheet("即將出關監控")
        all_values = ws.get_all_values()
        if len(all_values) < 2: return [] 
        
        headers = all_values[0]
        if "剩餘天數" not in headers: return []
        
        records = ws.get_all_records()
    except Exception as e:
        print(f"⚠️ 讀取「即將出關監控」失敗: {e}")
        return []

    releasing_list = []
    
    for row in records:
        code = str(row.get('代號', '')).strip()
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
            
    return releasing_list

# ============================
# 🚀 主程式
# ============================
def main():
    if not DISCORD_WEBHOOK_URL or "你的_DISCORD_WEBHOOK" in DISCORD_WEBHOOK_URL:
        print("❌ 請先設定 DISCORD_WEBHOOK_URL")
        return

    sh = connect_google_sheets()
    if not sh: return

    embeds_to_send = []

    # 1. 處理危險股 (紅色警報)
    danger_stocks = check_danger_stocks(sh)
    if danger_stocks:
        desc_lines = []
        for s in danger_stocks:
            # 因為排除 0 了，所以只會有 "再 X 天"
            icon = "⚠️"
            day_msg = f"再 {s['days']} 天"
            
            # 🛑 修改：不顯示原因，只顯示代號、名稱、天數
            desc_lines.append(
                f"{icon} **{s['code']} {s['name']}** | {day_msg}"
            )
        
        embed_danger = {
            "title": f"🚨 注意！{len(danger_stocks)} 檔股票瀕臨處置邊緣",
            "description": "\n".join(desc_lines),
            "color": 15158332, # 紅色
            "footer": {"text": f"資料時間: {datetime.now().strftime('%Y-%m-%d %H:%M')}"}
        }
        embeds_to_send.append(embed_danger)

    # 2. 處理即將出關股 (綠色機會)
    releasing_stocks = check_releasing_stocks(sh)
    if releasing_stocks:
        desc_lines = []
        for s in releasing_stocks:
            day_msg = "明天出關" if s['days'] <= 1 else f"剩 {s['days']} 天"
            desc_lines.append(
                f"🔓 **{s['code']} {s['name']}** | {day_msg} ({s['date']})"
            )
        
        embed_release = {
            "title": f"🕊️ 關注！{len(releasing_stocks)} 檔股票即將出關",
            "description": "\n".join(desc_lines),
            "color": 3066993, # 綠色
            "footer": {"text": "處置結束後通常會有行情波動，請留意風險。"}
        }
        embeds_to_send.append(embed_release)

    # 3. 發送
    if embeds_to_send:
        send_discord_webhook(embeds_to_send)
    else:
        print("😴 今日無符合條件的股票，不發送通知。")

if __name__ == "__main__":
    main()
