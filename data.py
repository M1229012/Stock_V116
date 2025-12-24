# -*- coding: utf-8 -*-
import requests
import pandas as pd
import time
import yfinance as yf
import gspread
import logging
import os
from datetime import datetime, timedelta, date
from google.oauth2.service_account import Credentials

# 嘗試匯入 config，如果失敗則使用預設值 (避免單獨測試報錯)
try:
    from config import FINMIND_API_URL, PARAM_SHEET_NAME, SAFE_MARKET_OPEN_CHECK, SHEET_NAME
    from utils import parse_clause_ids_strict, parse_jail_period, get_or_create_ws
except ImportError:
    pass

# ==========================================
# 設定 yfinance 靜音模式
# ==========================================
logger = logging.getLogger('yfinance')
logger.setLevel(logging.CRITICAL)
logger.disabled = True

# ==========================================
# Token 管理 (雙模組：支援 Colab Secrets 與 環境變數)
# ==========================================
FINMIND_TOKENS = []

# 1. 嘗試從環境變數讀取 (Zeabur 模式)
env_token = os.getenv('FinMind_1')
if env_token:
    FINMIND_TOKENS.append(env_token)

# 2. 嘗試從 Colab userdata 讀取 (Colab 模式)
try:
    from google.colab import userdata
    colab_token = userdata.get('FinMind_1')
    if colab_token and colab_token not in FINMIND_TOKENS:
        FINMIND_TOKENS.append(colab_token)
except ImportError:
    pass # 不在 Colab 環境，忽略錯誤

CURRENT_TOKEN_INDEX = 0
_FINMIND_CACHE = {}

def connect_google_sheets():
    print("正在進行 Google 驗證...")
    try:
        # A. 優先嘗試讀取 Zeabur 注入的設定檔 /service_key.json
        # (Zeabur 的 Config File 通常掛載在根目錄或當前目錄)
        key_path = "/service_key.json" # Zeabur 絕對路徑
        if not os.path.exists(key_path):
            key_path = "service_key.json" # 本地相對路徑
            
        if os.path.exists(key_path):
            gc = gspread.service_account(filename=key_path)
        else:
            # B. Fallback 到 Colab 的自動驗證
            from google.colab import auth
            from google.auth import default
            auth.authenticate_user()
            creds, _ = default()
            gc = gspread.authorize(creds)
            
        try: sh = gc.open(SHEET_NAME)
        except: sh = gc.create(SHEET_NAME)
        return sh, None
    except Exception as e:
        print(f"❌ Google Sheet 連線失敗: {e}")
        return None, None

def finmind_get(dataset, data_id=None, start_date=None, end_date=None):
    global CURRENT_TOKEN_INDEX
    cache_key = (dataset, data_id, start_date, end_date)
    if cache_key in _FINMIND_CACHE:
        return _FINMIND_CACHE[cache_key].copy()

    params = {"dataset": dataset}
    if data_id: params["data_id"] = str(data_id)
    if start_date: params["start_date"] = start_date
    if end_date: params["end_date"] = end_date
    
    # 如果有 Token 就用 Token，沒有就用免費版 (限制較多)
    tokens_to_try = FINMIND_TOKENS if FINMIND_TOKENS else [None]

    for _ in range(4):
        headers = {"User-Agent": "Mozilla/5.0", "Connection": "close"}
        token = tokens_to_try[CURRENT_TOKEN_INDEX % len(tokens_to_try)]
        if token:
            headers["Authorization"] = f"Bearer {token}"
            
        try:
            r = requests.get(FINMIND_API_URL, params=params, headers=headers, timeout=10)
            if r.status_code == 200:
                j = r.json()
                df = pd.DataFrame(j["data"]) if "data" in j else pd.DataFrame()
                if len(_FINMIND_CACHE) >= 2000: _FINMIND_CACHE.clear()
                _FINMIND_CACHE[cache_key] = df
                return df.copy()
            elif r.status_code != 200 and token:
                print(f"   ⚠️ Token {CURRENT_TOKEN_INDEX} 異常, 切換...")
                time.sleep(2)
                CURRENT_TOKEN_INDEX += 1
                continue
        except:
            time.sleep(1)
    return pd.DataFrame()

# ... (以下函式保持不變，fetch_history_data, load_precise_db_from_sheet 等等...)
# 為了節省篇幅，請保留您原本 data.py 剩下的函式 (fetch_history_data 到 update_market_monitoring_log)
# 只要把上面的 connect_google_sheets 和 finmind_get 替換掉即可
# 務必確保 import config 和 utils 都在最上面
# 若您不確定，您可以直接把原本 data.py 的後半段貼在這些程式碼後面
