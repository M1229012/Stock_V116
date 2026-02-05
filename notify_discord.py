import gspread
import requests
import os
import json
import re
import time
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# ============================
# ⚙️ 設定區
# ============================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
SHEET_NAME = "台股注意股資料庫_V33"
SERVICE_KEY_FILE = "service_key.json"

JAIL_ENTER_THRESHOLD = 3   
JAIL_EXIT_THRESHOLD = 5    

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
        if response.status_code != 204:
            print(f"❌ Discord 推播失敗: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"❌ 發送請求錯誤: {e}")

def parse_roc_date(date_str):
    """解析日期格式"""
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

def get_merged_jail_periods(sh):
    """讀取並合併處置期間"""
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
                s_date, e_date = parse_roc_date(dates[0]), parse_roc_date(dates[1])
                if s_date and e_date:
                    if e_date < today: continue
                    if code not in jail_map:
                        jail_map[code] = {'start': s_date, 'end': e_date}
                    else:
                        jail_map[code]['start'] = min(jail_map[code]['start'], s_date)
                        jail_map[code]['end'] = max(jail_map[code]['end'], e_date)
    except: return {}
    return {c: f"{d['start'].strftime('%Y/%m/%d')}-{d['end'].strftime('%Y/%m/%d')}" for c, d in jail_map.items()}

# ============================
# 📊 價格數據處理邏輯 (還原 K 線 & 百分比計算)
# ============================
def get_price_rank_info(code, period_str, market):
    """計算處置前 vs 處置中的績效對比"""
    try:
        dates = re.split(r'[~-～]', str(period_str))
        start_date = parse_roc_date(dates[0])
        if not start_date: return "❓ 未知", "日期錯"
        
        fetch_start = start_date - timedelta(days=60)
        end_date = datetime.now() + timedelta(days=1)
        suffix = ".TWO" if any(x in str(market) for x in ["上櫃", "TPEx"]) else ".TW"
        ticker = f"{code}{suffix}"
        
        # 📌 抓取還原 K 線 (auto_adjust=True)
        df = yf.Ticker(ticker).history(start=fetch_start.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), auto_adjust=True)
        
        # 📌 補齊分割股導致的 NaN (ffill)
        if not df.empty:
            df = df.ffill() 
        
        if df.empty or len(df) < 2: return "❓ 未知", "無股價"

        df.index = df.index.tz_localize(None)
        df_in_jail = df[df.index >= pd.Timestamp(start_date)]
        
        # 處置前績效 (同天數對比)
        mask_before = df.index < pd.Timestamp(start_date)
        if not mask_before.any(): 
            pre_pct = 0.0
        else:
            jail_base_p = df[mask_before]['Close'].iloc[-1]
            jail_days_count = len(df_in_jail) if not df_in_jail.empty else 1
            loc_idx = df.index.get_loc(df[mask_before].index[-1])
            target_idx = max(0, loc_idx - jail_days_count + 1)
            pre_entry = df.iloc[target_idx]['Open']
            pre_pct = ((jail_base_p - pre_entry) / pre_entry) * 100

        # 處置中績效
        if df_in_jail.empty: 
            in_pct = 0.0
        else:
            jail_start_entry = df_in_jail['Open'].iloc[0]
            curr_p = df_in_jail['Close'].iloc[-1]
            in_pct = ((curr_p - jail_start_entry) / jail_start_entry) * 100

        # 判斷狀態圖示與文字 (修改後邏輯)
        if in_pct > 15:
            status_icon, status_text = "👑", "妖股誕生"
        elif in_pct > 5:
            status_icon, status_text = "🔥", "強勢突圍"
        elif in_pct < -15:
            status_icon, status_text = "💀", "人去樓空"
        elif in_pct < -5:
            status_icon, status_text = "📉", "走勢疲軟"
        else:
            status_icon, status_text = "🧊", "多空膠著"
        
        status = f"{status_icon} {status_text}"
        price_result = f"處置前{'+' if pre_pct > 0 else ''}{pre_pct:.1f}% / 處置中{'+' if in_pct > 0 else ''}{in_pct:.1f}%"
        return status, price_result
    except Exception as e:
        print(f"⚠️ 失敗 ({code}): {e}")
        return "❓ 未知", "數據計算中"

# ============================
# 🔍 監控邏輯 (排序與分類)
# ============================
def check_status_split(sh, releasing_codes):
    try:
        ws = sh.worksheet("近30日熱門統計")
        records = ws.get_all_records()
    except: return {'entering': [], 'in_jail': []}
    jail_map = get_merged_jail_periods(sh)
    ent, inj, seen = [], [], set()
    for row in records:
        code = str(row.get('代號', '')).replace("'", "").strip()
        if code in releasing_codes or code in seen: continue
        name, days_str, reason = row.get('名稱', ''), str(row.get('最快處置天數', '99')), str(row.get('處置觸發原因', ''))
        if not days_str.isdigit(): continue
        d = int(days_str) + 1  
        if "處置中" in reason:
            inj.append({"code": code, "name": name, "period": jail_map.get(code, "日期未知")})
            seen.add(code)
        elif d <= JAIL_ENTER_THRESHOLD:
            ent.append({"code": code, "name": name, "days": d})
            seen.add(code)
            
    # 瀕臨處置排序
    ent.sort(key=lambda x: (x['days'], x['code']))
    
    # 【新增：正在處置排序】先按時間（越快出關越上面），再按股號
    def get_inj_sort_key(item):
        p = item.get('period', '')
        # 取得結束日期字串 (YYYY/MM/DD) 作為第一排序基準
        end_date = p.split('-')[1] if '-' in p else "9999/12/31"
        return (end_date, item['code'])
    
    inj.sort(key=get_inj_sort_key)
    
    return {'entering': ent, 'in_jail': inj}

def check_releasing_stocks(sh):
    try:
        ws = sh.worksheet("即將出關監控")
        records = ws.get_all_records()
    except: return []
    res, seen = [], set()
    for row in records:
        code = str(row.get('代號', '')).strip()
        if code in seen: continue
        days_str = str(row.get('剩餘天數', '99'))
        if not days_str.isdigit(): continue
        d = int(days_str) + 1
        if d <= JAIL_EXIT_THRESHOLD:
            st, pr = get_price_rank_info(code, row.get('處置期間', ''), row.get('市場', '上市'))
            dt = parse_roc_date(row.get('出關日期', ''))
            res.append({"code": code, "name": row.get('名稱', ''), "days": d, "date": dt.strftime("%m/%d") if dt else "??/??", "status": st, "price": pr})
            seen.add(code)
    res.sort(key=lambda x: (x['days'], x['code']))
    return res

# ============================
# 🚀 主程式 (分段邏輯 & ## 標題)
# ============================
def main():
    sh = connect_google_sheets()
    if not sh: return
    rel = check_releasing_stocks(sh)
    rel_codes = {x['code'] for x in rel}
    stats = check_status_split(sh, rel_codes)

    # 1. 瀕臨處置 (10 支分段 + ## 標題)
    if stats['entering']:
        total = len(stats['entering'])
        chunk_size = 10 if total > 15 else 20
        for i in range(0, total, chunk_size):
            chunk = stats['entering'][i : i + chunk_size]
            desc_lines = []
            if i == 0:
                desc_lines.append(f"### 🚨 處置倒數！{total} 檔股票瀕臨處置\n")
            for s in chunk:
                icon = "🔥" if s['days'] == 1 else "⚠️"
                # 修改此處文字：明日強制入獄 -> 明日開始處置
                msg = "明日開始處置" if s['days'] == 1 else f"處置倒數 {s['days']} 天"
                desc_lines.append(f"{icon} **{s['code']} {s['name']}** |  `{msg}`")
            send_discord_webhook([{"description": "\n".join(desc_lines), "color": 15158332}])
            time.sleep(2)

    # 2. 即將出關 (10 支分段 + ## 標題 + 說明文字)
    if rel:
        total = len(rel)
        chunk_size = 10 if total > 15 else 20
        for i in range(0, total, chunk_size):
            chunk = rel[i : i + chunk_size]
            desc_lines = []
            if i == 0:
                desc_lines.append(f"### 🔓 越關越大尾？{total} 檔股票即將出關\n")
            for s in chunk:
                # 第一行：名稱與日期
                desc_lines.append(f"**{s['code']} {s['name']}** | 剩 {s['days']} 天 ({s['date']})")
                # 第二行：依照圖片格式 ▸ 資訊
                desc_lines.append(f"▸ {s['status']} {s['price']}")
                # 間隔空行
                desc_lines.append("")
            
            # 說明文字僅在最後一段訊息結尾，且上方僅留空一行
            if i + chunk_size >= total:
                if desc_lines and desc_lines[-1] == "": desc_lines.pop() # 移除最後一個空行
                desc_lines.append("\n---\n*💡 說明：處置前 N 天 vs 處置中 N 天 (同天數對比)*")
            
            send_discord_webhook([{"description": "\n".join(desc_lines), "color": 3066993}])
            time.sleep(2)

    # 3. 處置中 (10 支分段 + ## 標題)
    if stats['in_jail']:
        total = len(stats['in_jail'])
        chunk_size = 10 if total > 15 else 20
        for i in range(0, total, chunk_size):
            chunk = stats['in_jail'][i : i + chunk_size]
            desc_lines = []
            if i == 0:
                desc_lines.append(f"### ⛓️ 還能噴嗎？{total} 檔股票正在處置\n")
            for s in chunk:
                pd_display = s['period'].replace('2026/', '').replace('-', '-')
                desc_lines.append(f"🔒 **{s['code']} {s['name']}** |  `{pd_display}`")
            send_discord_webhook([{"description": "\n".join(desc_lines), "color": 10181046}])
            time.sleep(2)

if __name__ == "__main__":
    main()
