# -*- coding: utf-8 -*-
import os
import logging
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo
import twstock

logger = logging.getLogger('yfinance')
logger.setLevel(logging.CRITICAL)
logger.disabled = True

UNIT_LOT = 1000

# ✅ 改回自動時間 (這樣才能判斷「今天」與「昨天」)
TW_TZ = ZoneInfo("Asia/Taipei")
TARGET_DATE = datetime.now(TW_TZ)
CURRENT_TIME = TARGET_DATE
IS_NIGHT_RUN = TARGET_DATE.hour >= 20

SAFE_CRAWL_TIME = dt_time(19, 0)
SAFE_MARKET_OPEN_CHECK = dt_time(16, 30)

SHEET_NAME = "台股注意股資料庫_V33"
PARAM_SHEET_NAME = "個股參數"

STATS_HEADERS = [
    '代號','名稱','連續天數','近30日注意次數','近10日注意次數','最近一次日期',
    '30日狀態碼','10日狀態碼','最快處置天數','處置觸發原因','風險等級','觸發條件',
    '目前價','警戒價','差幅(%)','目前量','警戒量','成交值(億)',
    '週轉率(%)','PE','PB','當沖佔比(%)'
]

FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"
TOKENS = [os.getenv('FinMind_1'), os.getenv('FinMind_2')]
FINMIND_TOKENS = [t for t in TOKENS if t]

CURRENT_TOKEN_INDEX = 0
_FINMIND_CACHE = {}

print("🚀 啟動 V116.18 台股注意股系統 (Fix: Trigger=0 Days)")
print(f"🕒 系統時間 (Taiwan): {TARGET_DATE.strftime('%Y-%m-%d %H:%M:%S')}")

try: twstock.__update_codes()
except: pass
