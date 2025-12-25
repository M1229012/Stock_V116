# -*- coding: utf-8 -*-
import os
import time
import pandas as pd
import requests
import gspread
import yfinance as yf
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from zoneinfo import ZoneInfo

# ==========================================
# 1. 設定與常數
# ==========================================
# 設定台灣時區 (GitHub 主機在 UTC，必須轉時區)
try: TW_TZ = ZoneInfo("Asia/Taipei")
except: TW_TZ = ZoneInfo("UTC")

CURRENT_TIME = datetime.now(TW_TZ)
TODAY_STR = CURRENT_TIME.strftime("%Y-%m-%d")

# 判斷執行模式
# 如果是晚上 8 點以後執行，代表要跑 FinMind 當沖 + Yahoo 修正
IS_NIGHT_RUN = CURRENT_TIME.hour >= 20 

print(f"🕒 系統時間: {CURRENT_TIME} | 模式: {'🌙 晚上補單與修正 (FinMind+Yahoo)' if IS_NIGHT_RUN else '☀️ 下午盤後更新 (Yahoo only)'}")

# API 設定
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"
FINMIND_TOKEN = os.getenv('FinMind_1')

# Google Sheet 設定
SHEET_NAME = "台股注意股資料庫_V33"
STATS_HEADERS = [
    '代號', '名稱', '連續天數', '近30日注意次數', '近10日注意次數', '最近一次日期',
    '30日狀態碼', '10日狀態碼', '最快處置天數', '處置觸發原因', '風險等級', '觸發條件',
    '目前價', '警戒價', '差幅(%)', '目前量', '警戒量', '成交值(億)',
    '週轉率(%)', 'PE', 'PB', '當沖佔比(%)'
]

# ============================
# 2. 抓取函式 (Yahoo & FinMind)
# ============================

def fetch_yahoo_data(stock_id):
    """抓取 Yahoo 數據 (價格、成交量、基本面、歷史K線)"""
    # 優先試上市，失敗試上櫃
    tickers = [f"{stock_id}.TW", f"{stock_id}.TWO"]
    data = {'price': 0, 'vol': 0, 'pe': 0, 'pb': 0, 'history': pd.DataFrame()}

    for t_code in tickers:
        try:
            ticker = yf.Ticker(t_code)
            hist = ticker.history(period="5d")
            
            if not hist.empty:
                last = hist.iloc[-1]
                data['price'] = float(last['Close'])
                data['vol'] = int(last['Volume'])
                data['history'] = hist
                
                # 嘗試抓 PE/PB
                try:
                    info = ticker.info
                    data['pe'] = info.get('trailingPE', 0) or 0
                    data['pb'] = info.get('priceToBook', 0) or 0
                except: pass
                
                return data # 成功就回傳
        except: continue
        
    return data

def fetch_finmind_daytrade(stock_id):
    """抓取 FinMind 當沖數據 (僅在晚上執行)"""
    if not IS_NIGHT_RUN: return 0.0
    
    # 往前抓幾天以防今天資料還沒出來
    start_date = (datetime.strptime(TODAY_STR, "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d")
    
    headers = {}
    if FINMIND_TOKEN: headers["Authorization"] = f"Bearer {FINMIND_TOKEN}"
    
    try:
        # 1. 抓當沖量
        params_dt = {"dataset": "TaiwanStockDayTrading", "data_id": stock_id, "start_date": start_date}
        r_dt = requests.get(FINMIND_API_URL, params=params_dt, headers=headers, timeout=5)
        df_dt = pd.DataFrame(r_dt.json().get("data", []))

        # 2. 抓總成交量 (為了計算占比)
        params_p = {"dataset": "TaiwanStockPrice", "data_id": stock_id, "start_date": start_date}
        r_p = requests.get(FINMIND_API_URL, params=params_p, headers=headers, timeout=5)
        df_p = pd.DataFrame(r_p.json().get("data", []))

        if not df_dt.empty and not df_p.empty:
            # 合併
            merged = pd.merge(df_p[['date', 'Trading_Volume']], df_dt[['date', 'Volume']], on='date')
            if not merged.empty:
                # 取最後一天 (也就是今天)
                last = merged.iloc[-1]
                if last['Trading_Volume'] > 0:
                    return round((last['Volume'] / last['Trading_Volume']) * 100, 2)
    except: pass
    
    return 0.0

# ============================
# 3. 風險計算邏輯
# ============================
def calculate_risk(row, y_data, dt_pct):
    res = row.copy()
    
    # 更新 Yahoo 數據 (無論下午或晚上，只要 Yahoo 有資料就更新，確保修正)
    if y_data['price'] > 0:
        res['目前價'] = y_data['price']
        res['目前量'] = int(y_data['vol'] / 1000) # 轉張數
        res['PE'] = round(y_data['pe'], 2)
        res['PB'] = round(y_data['pb'], 2)
        res['成交值(億)'] = round((y_data['price'] * y_data['vol']) / 100000000, 2)
        
        # 計算警戒值
        hist = y_data['history']
        if len(hist) >= 7:
            ref_price = hist.iloc[-7]['Close']
            limit_price = round(ref_price * 1.32, 2)
            res['警戒價'] = limit_price
            if y_data['price'] > 0:
                res['差幅(%)'] = round(((limit_price - y_data['price']) / y_data['price']) * 100, 1)
        
        if len(hist) >= 5:
            avg_vol = hist['Volume'].mean()
            res['警戒量'] = int((avg_vol * 5) / 1000)

    # 更新 FinMind 當沖數據 (只有晚上有值)
    if dt_pct > 0:
        res['當沖佔比(%)'] = dt_pct
        
    return res

# ============================
# 4. 主程式
# ============================
def main():
    # 1. 連線 Google Sheet
    key_path = "service_key.json"
    if not os.path.exists(key_path):
        print("❌ 錯誤: 找不到 service_key.json")
        return

    gc = gspread.service_account(filename=key_path)
    sh = gc.open(SHEET_NAME)
    ws = sh.worksheet("近30日熱門統計")
    records = ws.get_all_records()
    
    updates = []
    
    print(f"📋 開始掃描 {len(records)} 檔股票...")

    for i, row in enumerate(records):
        code = str(row['代號'])
        
        # 1. 抓 Yahoo (下午、晚上都抓，確保數據修正)
        y_data = fetch_yahoo_data(code)
        
        # 簡單防呆：Yahoo 有時候會擋，如果連續失敗建議 sleep 久一點
        # 但因為我們一天只跑兩次，量不大，通常沒事
        time.sleep(0.5) 

        # 2. 抓 FinMind (只在晚上抓)
        dt_val = 0.0
        if IS_NIGHT_RUN:
            dt_val = fetch_finmind_daytrade(code)
            # 這裡不特別 sleep，因為 FinMind 有額度但我們一天只跑一次晚上，應該夠用
        
        # 3. 整合與計算
        if y_data['price'] > 0:
            # 有抓到 Yahoo 資料才更新，避免把原本有的資料覆蓋成 0
            new_row = calculate_risk(row, y_data, dt_val)
            new_row['最近一次日期'] = TODAY_STR
            updates.append(new_row)
            print(f"[{i+1}] {code} 更新成功 (Price: {y_data['price']}, DT: {dt_val}%)")
        else:
            print(f"[{i+1}] {code} Yahoo 抓取失敗，跳過更新")

    # 4. 寫回 Google Sheet
    if updates:
        print(f"💾 正在寫入 {len(updates)} 筆資料...")
        # 建立 Map 加速寫入
        update_map = {str(r['代號']): r for r in updates}
        
        final_rows = []
        for row in records:
            code = str(row['代號'])
            if code in update_map:
                target = update_map[code]
                final_rows.append([target.get(h, '') for h in STATS_HEADERS])
            else:
                final_rows.append([row.get(h, '') for h in STATS_HEADERS])
        
        ws.clear()
        ws.append_row(STATS_HEADERS)
        ws.append_rows(final_rows)
        print("✅ 作業完成！")
    else:
        print("⚠️ 本次沒有任何資料被更新。")

if __name__ == "__main__":
    main()
