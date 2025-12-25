# -*- coding: utf-8 -*-
import os
import time
import requests
import pandas as pd
import gspread
import yfinance as yf
from datetime import datetime, timedelta
from config import FINMIND_API_URL, FINMIND_TOKEN, SHEET_NAME, WORKSHEET_NAME, TODAY_STR, IS_NIGHT_RUN

def connect_google_sheets():
    """連線 Google Sheet"""
    key_path = "service_key.json"
    if not os.path.exists(key_path):
        print("❌ 錯誤: 找不到 service_key.json")
        return None

    try:
        gc = gspread.service_account(filename=key_path)
        sh = gc.open(SHEET_NAME)
        ws = sh.worksheet(WORKSHEET_NAME)
        return ws
    except Exception as e:
        print(f"❌ Google Sheet 連線失敗: {e}")
        return None

def fetch_yahoo_data(stock_id):
    """抓取 Yahoo 數據 (價格、成交量、基本面、歷史K線)"""
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
                
                return data
        except: continue
        
    return data

def fetch_finmind_daytrade(stock_id):
    """抓取 FinMind 當沖數據 (僅在晚上執行)"""
    # 如果不是晚上模式，直接回傳 0，節省資源
    if not IS_NIGHT_RUN: return 0.0
    
    start_date = (datetime.strptime(TODAY_STR, "%Y-%m-%d") - timedelta(days=5)).strftime("%Y-%m-%d")
    headers = {}
    if FINMIND_TOKEN: headers["Authorization"] = f"Bearer {FINMIND_TOKEN}"
    
    try:
        # 1. 抓當沖量
        params_dt = {"dataset": "TaiwanStockDayTrading", "data_id": stock_id, "start_date": start_date}
        r_dt = requests.get(FINMIND_API_URL, params=params_dt, headers=headers, timeout=5)
        df_dt = pd.DataFrame(r_dt.json().get("data", []))

        # 2. 抓總成交量
        params_p = {"dataset": "TaiwanStockPrice", "data_id": stock_id, "start_date": start_date}
        r_p = requests.get(FINMIND_API_URL, params=params_p, headers=headers, timeout=5)
        df_p = pd.DataFrame(r_p.json().get("data", []))

        if not df_dt.empty and not df_p.empty:
            merged = pd.merge(df_p[['date', 'Trading_Volume']], df_dt[['date', 'Volume']], on='date')
            if not merged.empty:
                last = merged.iloc[-1]
                if last['Trading_Volume'] > 0:
                    return round((last['Volume'] / last['Trading_Volume']) * 100, 2)
    except: pass
    
    return 0.0
