import gspread
import yfinance as yf
import pandas as pd
import numpy as np
import re
import time
import os
import sys
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

# ============================
# ⚙️ 設定區
# ============================
SHEET_NAME = "台股注意股資料庫_V33"
SOURCE_WORKSHEET = "處置股90日明細"
DEST_WORKSHEET = "處置股出關記錄"

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
    match = re.match(r'^(\d{2,3})[/-](\d{1,2})[/-](\d{1,2})$', s)
    if match:
        y, m, d = map(int, match.groups())
        y_final = y + 1911 if y < 1911 else y
        return datetime(y_final, m, d)
    for fmt in ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]:
        try: return datetime.strptime(s, fmt)
        except: continue
    return None

def determine_status(pre_pct, in_pct):
    """判斷處置狀態"""
    if in_pct > 15: return "👑 妖股誕生"
    elif in_pct > 5: return "🔥 強勢突圍"
    elif in_pct < -15: return "💀 人去樓空"
    elif in_pct < -5: return "📉 走勢疲軟"
    else: return "🧊 多空膠著"

def get_ticker_list(code, market=""):
    """
    根據市場別與股號決定嘗試的順序，減少 404 錯誤
    """
    code = str(code)
    # 1. 明確指定市場
    if "上櫃" in market or "TPEx" in market:
        return [f"{code}.TWO", f"{code}.TW"]
    if "上市" in market:
        return [f"{code}.TW", f"{code}.TWO"]
    
    # 2. 若市場未知，根據股號開頭簡單猜測 (常見上櫃開頭)
    # 3, 4, 5, 6, 8 開頭很多是上櫃，先試 .TWO 可以減少紅字錯誤
    if code and code[0] in ['3', '4', '5', '6', '8']:
        return [f"{code}.TWO", f"{code}.TW"]
    
    # 3. 預設先試上市
    return [f"{code}.TW", f"{code}.TWO"]

def fetch_stock_data(code, start_date, jail_end_date, market=""):
    """抓取歷史股價並計算狀態與出關後走勢"""
    try:
        fetch_start = start_date - timedelta(days=60)
        fetch_end = jail_end_date + timedelta(days=40) 
        
        # 取得建議的後綴清單
        tickers_to_try = get_ticker_list(code, market)
        
        df = pd.DataFrame()
        used_ticker = ""

        # 嘗試抓取 (靜默模式，盡量減少錯誤輸出)
        for ticker in tickers_to_try:
            try:
                # 使用 yfinance 的 shared 錯誤處理機制或是直接捕捉
                temp_df = yf.Ticker(ticker).history(start=fetch_start, end=fetch_end, auto_adjust=True)
                if not temp_df.empty:
                    df = temp_df
                    used_ticker = ticker
                    break
            except Exception:
                continue
        
        if df.empty:
            # print(f"  ⚠️ 找不到股價資料: {code}")
            return None

        df.index = df.index.tz_localize(None)
        df = df.ffill()

        # === 1. 計算處置狀態 ===
        mask_jail = (df.index >= pd.Timestamp(start_date)) & (df.index <= pd.Timestamp(jail_end_date))
        df_jail = df[mask_jail]
        mask_before = df.index < pd.Timestamp(start_date)
        
        pre_pct = 0.0
        in_pct = 0.0
        
        if mask_before.any():
            jail_base_p = df[mask_before]['Close'].iloc[-1]
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
        df_after = df[df.index > pd.Timestamp(jail_end_date)]
        
        post_data = []
        accumulated_pct = 0.0
        base_price = jail_end_price if jail_end_price != 0 else (df_after['Open'].iloc[0] if not df_after.empty else 0)

        for i in range(10):
            if i < len(df_after):
                curr_close = df_after['Close'].iloc[i]
                prev_close = df_after['Close'].iloc[i-1] if i > 0 else base_price
                if prev_close != 0:
                    daily_chg = ((curr_close - prev_close) / prev_close) * 100
                    post_data.append(f"{daily_chg:+.1f}%")
                else:
                    post_data.append("0.0%")
                
                if i == len(df_after) - 1 or i == 9:
                    if base_price != 0:
                        accumulated_pct = ((curr_close - base_price) / base_price) * 100
            else:
                post_data.append("")

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
        print(f"⚠️ 數據計算錯誤 {code}: {e}")
        return None

# ============================
# 🚀 主程式
# ============================
def main():
    print("🚀 開始執行處置股出關記錄更新...")
    
    # 1. 連線資料庫
    sh = connect_google_sheets(SHEET_NAME)
    if not sh: return

    try:
        ws_source = sh.worksheet(SOURCE_WORKSHEET)
    except WorksheetNotFound:
        print(f"❌ 找不到來源工作表 '{SOURCE_WORKSHEET}'")
        return

    header = ["出關日期", "股號", "股名", "狀態", "處置前%", "處置中%", "累積漲跌幅", 
              "D+1", "D+2", "D+3", "D+4", "D+5", "D+6", "D+7", "D+8", "D+9", "D+10"]

    try:
        ws_dest = sh.worksheet(DEST_WORKSHEET)
    except WorksheetNotFound:
        print(f"💡 工作表 '{DEST_WORKSHEET}' 不存在，正在建立...")
        ws_dest = sh.add_worksheet(title=DEST_WORKSHEET, rows=1000, cols=20)
        ws_dest.append_row(header)

    # 2. 讀取現有記錄
    existing_records = ws_dest.get_all_records()
    existing_map = {} 
    
    for i, row in enumerate(existing_records):
        rid = str(row.get('股號', ''))
        rdate = str(row.get('出關日期', ''))
        d10 = str(row.get('D+10', '')).strip()
        if rid:
            key = f"{rid}_{rdate}"
            existing_map[key] = {
                'data': row,
                'done': bool(d10)
            }

    # 3. 讀取處置名單
    source_data = ws_source.get_all_records()
    processed_list = []
    today = datetime.now()

    print(f"🔍 掃描 {len(source_data)} 筆處置紀錄...")

    # 為了避免 Log 太多，計算一下進度
    total_count = 0
    update_count = 0

    for row in source_data:
        code = str(row.get('代號', '')).replace("'", "").strip()
        name = row.get('名稱', '')
        period = str(row.get('處置期間', '')).strip()
        market = str(row.get('市場', '')) # 嘗試讀取市場欄位
        
        if not code or not period: continue
        
        dates = re.split(r'[~-～]', period)
        if len(dates) < 2: continue
        
        s_date = parse_roc_date(dates[0])
        e_date = parse_roc_date(dates[1])
        
        if not s_date or not e_date: continue
        if e_date > today: continue 

        # 這裡不印出每一筆，只印出真正要處理的
        
        # 執行抓取
        # 傳入 market 參數以優化抓取
        result = fetch_stock_data(code, s_date, e_date, market)
        
        if not result:
            # 只有抓不到資料才印出來，且不中斷
            # print(f"⚠️ 跳過 {code} {name} (無資料)") 
            continue
            
        release_date_str = result['release_date']
        key = f"{code}_{release_date_str}"
        
        if key in existing_map and existing_map[key]['done']:
            old_row = existing_map[key]['data']
            row_vals = [old_row.get(h, "") for h in header]
            processed_list.append(row_vals)
        else:
            row_data = [
                release_date_str, code, name, result['status'],
                result['pre_pct'], result['in_pct'], result['acc_pct']
            ] + result['daily_trends']
            processed_list.append(row_data)
            update_count += 1
            print(f"  ✨ 更新: {code} {name} | {result['status']}")
            time.sleep(0.5) # 稍微加速

        total_count += 1

    # 4. 寫入
    processed_list.sort(key=lambda x: x[0], reverse=True)
    final_output = [header] + processed_list
    
    ws_dest.clear()
    ws_dest.update(final_output)
    print(f"🎉 完成！共掃描 {total_count} 筆，本次更新 {update_count} 筆。")

if __name__ == "__main__":
    main()
