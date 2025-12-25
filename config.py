# -*- coding: utf-8 -*-
import os
from datetime import datetime
from zoneinfo import ZoneInfo

# 設定台灣時區
try: TW_TZ = ZoneInfo("Asia/Taipei")
except: TW_TZ = ZoneInfo("UTC")

# 取得現在時間
CURRENT_TIME = datetime.now(TW_TZ)
TODAY_STR = CURRENT_TIME.strftime("%Y-%m-%d")

# 判斷是否為晚上 (20:00 後視為晚上補單模式)
IS_NIGHT_RUN = CURRENT_TIME.hour >= 20

# Google Sheet 設定
SHEET_NAME = "台股注意股資料庫_V33"
WORKSHEET_NAME = "近30日熱門統計"

# 輸出欄位順序
STATS_HEADERS = [
    '代號', '名稱', '連續天數', '近30日注意次數', '近10日注意次數', '最近一次日期',
    '30日狀態碼', '10日狀態碼', '最快處置天數', '處置觸發原因', '風險等級', '觸發條件',
    '目前價', '警戒價', '差幅(%)', '目前量', '警戒量', '成交值(億)',
    '週轉率(%)', 'PE', 'PB', '當沖佔比(%)'
]

# API 設定
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"
FINMIND_TOKEN = os.getenv('FinMind_1')
