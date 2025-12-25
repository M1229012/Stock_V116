# -*- coding: utf-8 -*-
import os
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo

# 時區設定
try: TW_TZ = ZoneInfo("Asia/Taipei")
except: TW_TZ = ZoneInfo("UTC")

# 核心時間變數
TARGET_DATE = datetime.now(TW_TZ)
CURRENT_TIME = TARGET_DATE
IS_NIGHT_RUN = TARGET_DATE.hour >= 20

# Google Sheet 設定
SHEET_NAME = "台股注意股資料庫_V33"
PARAM_SHEET_NAME = "個股參數"

# 時間門檻
SAFE_CRAWL_TIME = dt_time(19, 0)
SAFE_MARKET_OPEN_CHECK = dt_time(16, 30)

# 統計表頭 (完全一致)
STATS_HEADERS = [
    '代號', '名稱', '連續天數', '近30日注意次數', '近10日注意次數', '最近一次日期',
    '30日狀態碼', '10日狀態碼', '最快處置天數', '處置觸發原因', '風險等級', '觸發條件',
    '目前價', '警戒價', '差幅(%)', '目前量', '警戒量', '成交值(億)',
    '週轉率(%)', 'PE', 'PB', '當沖佔比(%)'
]

# FinMind API
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"
# 讀取 GitHub Secrets
TOKENS = [os.getenv('FinMind_1'), os.getenv('FinMind_2')]
FINMIND_TOKENS = [t for t in TOKENS if t]
