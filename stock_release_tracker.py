import gspread
import yfinance as yf
import pandas as pd
import numpy as np
import re
import time
import os
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# ============================
# ⚙️ 設定區
# ============================
SOURCE_SHEET_NAME = "台股注意股資料庫_V33"
SOURCE_WORKSHEET = "處置股90日明細"

DEST_SHEET_NAME = "處置股出關記錄"
DEST_WORKSHEET = "出關記錄"

SERVICE_KEY_FILE = "service_key.json"

# ============================
# 🛠️ 工具函式
# ============================
def connect_google_sheets(sheet_name):
    """連線 Google Sheets"""
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(SERVICE_KEY_FILE, scopes=scope)
        gc = gspread.authorize(creds)
        sh = gc.open(sheet_name)
        return sh
    except Exception as e:
        print(f"❌ Google Sheet 連線失敗 ({sheet_name}): {e}")
        return None

def parse_roc_date(date_str):
    """解析民國或西元日期"""
    s = str(date_str).strip()
    # 處理民國年 113/01/01 或 113-01-01
    match = re.match(r'^(\d{2,3})[/-](\d{1,2})[/-](\d{1,2})$', s)
    if match:
        y, m, d = map(int, match.groups())
        y_final = y + 1911 if y < 1911 else y
        return datetime(y_final, m, d)
    # 處理西元年
    for fmt in ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]:
        try: return datetime.strptime(s, fmt)
        except: continue
    return None

def determine_status(pre_pct, in_pct):
    """判斷處置狀態 (共用邏輯)"""
    if in_pct > 15: return "👑 妖股誕生"
    elif in_pct > 5: return "🔥 強勢突圍"
    elif in_pct < -15: return "💀 人去樓空"
    elif in_pct < -5: return "📉 走勢疲軟"
    else: return "🧊 多空膠著"

def fetch_stock_data(code, start_date, jail_end_date):
    """
    抓取歷史股價並計算：
    1. 狀態 (處置前/處置中 %)
    2. 出關後 10 日走勢 (D+1 ~ D+10)
    """
    try:
        # 設定抓取範圍：處置前 60 天 ~ 出關後 30 天 (確保有足夠數據)
        fetch_start = start_date - timedelta(days=60)
        fetch_end = jail_end_date + timedelta(days=40) 
        
        suffix = ".TWO" if len(code) < 4 else ".TW" # 簡易判斷，若不準確建議從 Sheet 讀取市場別
        # 嘗試上市或上櫃後綴
        ticker = f"{code}.TW"
        df = yf.Ticker(ticker).history(start=fetch_start, end=fetch_end, auto_adjust=True)
        if df.empty:
            ticker = f"{code}.TWO"
            df = yf.Ticker(ticker).history(start=fetch_start, end=fetch_end, auto_adjust=True)
        
        if df.empty: return None

        df.index = df.index.tz_localize(None)
        df = df.ffill() # 補假日空值

        # === 1. 計算處置狀態 ===
        # 處置中區間
        mask_jail = (df.index >= pd.Timestamp(start_date)) & (df.index <= pd.Timestamp(jail_end_date))
        df_jail = df[mask_jail]
        
        # 處置前區間
        mask_before = df.index < pd.Timestamp(start_date)
        
        pre_pct = 0.0
        in_pct = 0.0
        
        if not mask_before.any():
            pre_pct = 0.0
        else:
            jail_base_p = df[mask_before]['Close'].iloc[-1]
            # 簡單計算：處置前最後收盤 vs 處置第一天開盤 (或依你原本邏輯調整)
            # 這裡沿用你之前的邏輯概念
            target_idx = max(0, len(df[mask_before]) - len(df_jail))
            pre_entry = df[mask_before]['Open'].iloc[target_idx] if len(df[mask_before]) > target_idx else jail_base_p
            if pre_entry != 0:
                pre_pct = ((jail_base_p - pre_entry) / pre_entry) * 100

        jail_end_price = 0
        if not df_jail.empty:
            jail_start_price = df_jail['Open'].iloc[0]
            jail_end_price = df_jail['Close'].iloc[-1]
            if jail_start_price != 0:
                in_pct = ((jail_end_price - jail_start_price) / jail_start_price) * 100
        
        status = determine_status(pre_pct, in_pct)

        # === 2. 計算出關後 D+1 ~ D+10 ===
        # 找出大於處置結束日期的交易日
        df_after = df[df.index > pd.Timestamp(jail_end_date)]
        
        post_data = []
        accumulated_pct = 0.0
        
        # 出關基準價 (通常是處置最後一天的收盤價)
        base_price = jail_end_price if jail_end_price != 0 else (df_after['Open'].iloc[0] if not df_after.empty else 0)

        for i in range(10):
            if i < len(df_after):
                curr_close = df_after['Close'].iloc[i]
                # 計算當日漲跌幅 (vs 前一日收盤)
                prev_close = df_after['Close'].iloc[i-1] if i > 0 else base_price
                daily_chg = ((curr_close - prev_close) / prev_close) * 100
                post_data.append(f"{daily_chg:+.1f}%")
                
                # 更新累積漲跌幅 (D+10 vs 處置結束價)
                if i == len(df_after) - 1 or i == 9:
                    if base_price != 0:
                        accumulated_pct = ((curr_close - base_price) / base_price) * 100
            else:
                post_data.append("") # 未來日期留空

        # 補滿 10 格
        while len(post_data) < 10:
            post_data.append("")

        return {
            "status": status,
            "pre_pct": f"{pre_pct:+.1f}%",
            "in_pct": f"{in_pct:+.1f}%",
            "acc_pct": f"{accumulated_pct:+.1f}%",
            "daily_trends": post_data,
            "release_date": df_after.index[0].strftime("%Y/%m/%d") if not df_after.empty else "未知"
        }

    except Exception as e:
        print(f"⚠️ 數據抓取錯誤 {code}: {e}")
        return None

# ============================
# 🚀 主程式
# ============================
def main():
    print("🚀 開始執行處置股出關記錄更新...")
    
    # 1. 連線資料庫
    sh_source = connect_google_sheets(SOURCE_SHEET_NAME)
    sh_dest = connect_google_sheets(DEST_SHEET_NAME)
    
    if not sh_source or not sh_dest: return

    try:
        ws_source = sh_source.worksheet(SOURCE_WORKSHEET)
        ws_dest = sh_dest.worksheet(DEST_WORKSHEET)
    except Exception as e:
        print(f"❌ 找不到工作表: {e}")
        return

    # 2. 讀取現有記錄 (避免重複抓取已完成的)
    existing_records = ws_dest.get_all_records()
    existing_map = {} # Key: "Code_ReleaseDate"
    
    # 用來判斷是否需要更新
    # 格式: {'2330_2024-01-01': {'row_index': 2, 'd10_filled': True/False}}
    for i, row in enumerate(existing_records):
        rid = str(row.get('股號', ''))
        rdate = str(row.get('出關日期', ''))
        d10 = str(row.get('D+10', '')).strip()
        if rid and rdate:
            key = f"{rid}_{rdate}"
            existing_map[key] = {
                'data': row,
                'done': bool(d10) # 如果 D+10 有值，視為已結案
            }

    # 3. 讀取處置名單
    source_data = ws_source.get_all_records()
    processed_list = []
    
    today = datetime.now()

    print(f"🔍 掃描 {len(source_data)} 筆處置紀錄...")

    for row in source_data:
        code = str(row.get('代號', '')).replace("'", "").strip()
        name = row.get('名稱', '')
        period = str(row.get('處置期間', '')).strip()
        
        if not code or not period: continue
        
        dates = re.split(r'[~-～]', period)
        if len(dates) < 2: continue
        
        s_date = parse_roc_date(dates[0])
        e_date = parse_roc_date(dates[1])
        
        if not s_date or not e_date: continue
        
        # 只處理「已經結束」或「今天結束」的處置 (未來的不處理)
        if e_date > today: continue

        # 預估出關日 (處置結束日 + 1 天，但準確日期需看 yfinance 第一筆交易日)
        # 先用 key 檢查是否存在
        # 由於 yfinance 才能確定準確的出關交易日，這裡我們先做初步過濾
        # 如果這筆資料已經在 Sheet 裡且 D+10 滿了，就直接用舊資料
        
        # 為了比對，我們需要先知道「大概」的出關日，或是掃描 existing_map 裡有沒有該代號且日期接近的
        # 這裡採取策略：只要是已結束的處置，都丟進去處理，但在 fetch 內部做快取判斷
        
        # 檢查是否已存在且完成
        # 注意：因為出關日可能因假日變動，我們這裡無法精確組出 Key，
        # 所以策略改為：一律重新計算數據，但如果資料庫已存在該代號且日期相近的完整紀錄，則可以用舊的。
        # 簡單起見：我們對每一筆都去抓 yfinance (因為 yfinance 有快取，且執行頻率不高)
        # 或是：只對「最近 30 天內出關」或「D+10 未填滿」的做 fetch
        
        is_fully_done = False
        # 簡易檢查：如果結束日期距今超過 20 天，且我們在現有資料庫找不到它，可能需要補抓
        # 但如果找到了且 D+10 有值，就 skip
        
        # 這裡直接執行 fetch，邏輯比較乾淨，雖然花點時間但確保資料正確
        # 為了避免 API 限制，建議加一點 delay
        
        print(f"處理: {code} {name} (處置結束: {e_date.strftime('%Y-%m-%d')})...")
        
        result = fetch_stock_data(code, s_date, e_date)
        if not result:
            print(f"  ⚠️ 無法抓取數據，跳過")
            continue
            
        release_date_str = result['release_date'] # 格式 YYYY/MM/DD
        
        # 檢查是否已存在且 D+10 已填滿
        key = f"{code}_{release_date_str}"
        if key in existing_map and existing_map[key]['done']:
            # 使用舊資料 (保留原本的記錄，避免覆蓋)
            old_row = existing_map[key]['data']
            processed_list.append([
                old_row.get('出關日期'), old_row.get('股號'), old_row.get('股名'),
                old_row.get('狀態'), old_row.get('處置前%'), old_row.get('處置中%'),
                old_row.get('累積漲跌幅'),
                old_row.get('D+1'), old_row.get('D+2'), old_row.get('D+3'), old_row.get('D+4'), old_row.get('D+5'),
                old_row.get('D+6'), old_row.get('D+7'), old_row.get('D+8'), old_row.get('D+9'), old_row.get('D+10')
            ])
            # print(f"  ✅ 已存在且完整，跳過更新")
        else:
            # 新資料 或 需要更新的資料
            row_data = [
                release_date_str,
                code,
                name,
                result['status'],
                result['pre_pct'],
                result['in_pct'],
                result['acc_pct']
            ] + result['daily_trends']
            
            processed_list.append(row_data)
            print(f"  ✨ 更新數據: {result['status']}")
            time.sleep(0.5) # 避免太快

    # 4. 排序與寫入
    # 依日期 (index 0) 排序，由新到舊
    processed_list.sort(key=lambda x: x[0], reverse=True)
    
    # 準備寫入 Header
    header = ["出關日期", "股號", "股名", "狀態", "處置前%", "處置中%", "累積漲跌幅", 
              "D+1", "D+2", "D+3", "D+4", "D+5", "D+6", "D+7", "D+8", "D+9", "D+10"]
    
    final_output = [header] + processed_list
    
    # 清空並寫入
    ws_dest.clear()
    ws_dest.update(final_output)
    print(f"🎉 完成！共寫入 {len(processed_list)} 筆資料到「出關記錄」。")

if __name__ == "__main__":
    main()
