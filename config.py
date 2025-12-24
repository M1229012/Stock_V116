# -*- coding: utf-8 -*-
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo

# 系統設定
SHEET_NAME = "台股注意股資料庫_V33"
PARAM_SHEET_NAME = "個股參數"
TW_TZ = ZoneInfo("Asia/Taipei")
UNIT_LOT = 1000

# 時間設定 (動態獲取當前時間)
def get_target_date():
    return datetime.now(TW_TZ)

SAFE_CRAWL_TIME = dt_time(19, 0)
SAFE_MARKET_OPEN_CHECK = dt_time(16, 30)

# API 設定
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"

# 統計表頭
STATS_HEADERS = [
    '代號', '名稱', '連續天數', '近30日注意次數', '近10日注意次數', '最近一次日期',
    '30日狀態碼', '10日狀態碼', '最快處置天數', '處置觸發原因', '風險等級', '觸發條件',
    '目前價', '警戒價', '差幅(%)', '目前量', '警戒量', '成交值(億)',
    '週轉率(%)', 'PE', 'PB', '當沖佔比(%)'
]

# 中文數字對照
CN_NUM = {"一":"1","二":"2","三":"3","四":"4","五":"5","六":"6","七":"7","八":"8","九":"9","十":"10"}

# 🔥 [關鍵字對照表] 必須保留，用於補救官方公告格式不統一
KEYWORD_MAP = {
    "起迄兩個營業日": 11,
    "當日沖銷": 13,
    "借券賣出": 12,
    "累積週轉率": 10,
    "週轉率": 4,
    "成交量": 9,
    "本益比": 6,
    "股價淨值比": 6,
    "溢折價": 8,
    "收盤價漲跌百分比": 1,
    "最後成交價漲跌": 1,
    "最近六個營業日累積": 1
}
