# -*- coding: utf-8 -*-
"""
每週大股東籌碼強勢榜 Top20｜PSCNet / MoneyDJ 正式部署版
=====================================================

部署重點：
1. 資料邏輯改用 PSCNet / MoneyDJ Stock-Chip0007 JSON。
2. 使用 ThreadPoolExecutor 多執行緒加速：
   - 補 Stock-Chip0007 API URL 快取
   - requests 抓 60 週股權分散歷史
   - yfinance 抓 Top20 股價
3. Google Sheet 使用原本程式設定：
   - SHEET_NAME 預設：台股注意股資料庫_V33
   - HOLDER_HISTORY_SHEET_NAME 預設：每週大戶排行紀錄
4. 另外在同一份 Google Sheet 寫入：
   - 上市400張比例歷史
   - 上櫃400張比例歷史
   - PSCNet_API快取
5. 圖片排版沿用原本程式的白底雙欄格式。
6. 連2 / 連3 / 連4 判斷方式：
   - 只看「每週大戶排行紀錄」裡的每週 Top20
   - 本週 Top20 + 上週 Top20 + 上上週 Top20... 連續出現才標記。

GitHub Actions 建議 secrets：
- DISCORD_WEBHOOK_URL_TEST
- GOOGLE_SERVICE_ACCOUNT_JSON
  或 GOOGLE_SERVICE_ACCOUNT_JSON_BASE64

必要套件：
pip install requests pandas yfinance selenium webdriver-manager gspread matplotlib wcwidth beautifulsoup4
"""

import os
import re
import json
import time
import base64
import unicodedata
from io import StringIO, BytesIO
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup

try:
    import gspread
except Exception:
    gspread = None

from wcwidth import wcwidth

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.patheffects as pe
from matplotlib import font_manager

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


# ================= 設定區 =================

DISCORD_WEBHOOK_URL = (
    os.getenv("DISCORD_WEBHOOK_URL_TEST")
    or os.getenv("DISCORD_WEBHOOK_URL")
    or ""
)

SHEET_NAME = os.getenv("SHEET_NAME", "台股注意股資料庫_V33")
SERVICE_KEY_FILE = os.getenv("SERVICE_KEY_FILE", "service_key.json")

HOLDER_HISTORY_SHEET_NAME = os.getenv("HOLDER_HISTORY_SHEET_NAME", "每週大戶排行紀錄")
LISTED_RATIO_SHEET_NAME = os.getenv("LISTED_RATIO_SHEET_NAME", "上市400張比例歷史")
OTC_RATIO_SHEET_NAME = os.getenv("OTC_RATIO_SHEET_NAME", "上櫃400張比例歷史")
API_CACHE_SHEET_NAME = os.getenv("API_CACHE_SHEET_NAME", "PSCNet_API快取")

TOP_N = int(os.getenv("TOP_N", "20"))
HISTORY_INITIAL_WEEKS = int(os.getenv("HISTORY_INITIAL_WEEKS", "5"))
HISTORY_EXTEND_WEEKS = int(os.getenv("HISTORY_EXTEND_WEEKS", "12"))

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "36"))
PRICE_WORKERS = int(os.getenv("PRICE_WORKERS", "18"))
# Selenium 在 GitHub runner 上太多執行緒容易只有部分 worker 成功。
# 預設改成 2 個 worker，穩定性優先；要加速可在 GitHub env 調高。
MAX_DISCOVER_WORKERS = int(os.getenv("MAX_DISCOVER_WORKERS", "2"))
DISCOVER_TIMEOUT_SEC = int(os.getenv("DISCOVER_TIMEOUT_SEC", "22"))

# 快取補齊最多重跑幾輪。第一輪若只補到一半，第二輪會自動補剩下的。
DISCOVER_MAX_ROUNDS = int(os.getenv("DISCOVER_MAX_ROUNDS", "3"))

# API 快取允許缺漏檔數。超過就直接停止，不推播不完整圖片。
MAX_ALLOWED_MISSING_API = int(os.getenv("MAX_ALLOWED_MISSING_API", "30"))

# requests 抓資料允許錯誤檔數。超過就停止，避免漏太多股票仍推播。
MAX_ALLOWED_REQUEST_ERRORS = int(os.getenv("MAX_ALLOWED_REQUEST_ERRORS", "80"))

# 若 Google Sheet 尚無 API 快取，是否用 Selenium headless 補快取。
DISCOVER_MISSING_API = os.getenv("DISCOVER_MISSING_API", "1") != "0"

# GitHub Actions 偶發 DNS 解析失敗時使用，避免 isin.twse.com.tw 短暫失敗直接中斷。
STOCK_LIST_RETRY_TIMES = int(os.getenv("STOCK_LIST_RETRY_TIMES", "5"))
STOCK_LIST_RETRY_SLEEP = float(os.getenv("STOCK_LIST_RETRY_SLEEP", "8"))

# 本機備援快取；GitHub 上主要會以 Google Sheet 的 PSCNet_API快取為準。
LOCAL_API_CACHE_FILE = Path(os.getenv("LOCAL_API_CACHE_FILE", "pscnet_chip0007_api_cache.json"))

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

HDR = {
    "User-Agent": USER_AGENT,
    "Accept": "*/*",
    "Referer": "https://pscnetsecrwd.moneydj.com/",
}

ISIN_URLS = {
    "上市": "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2",
    "上櫃": "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4",
}

PSCNET_PAGE_URL = (
    "https://pscnetsecrwd.moneydj.com/b2brwd/page/1000/chip/0007"
    "?sym=AS{code}&symidxq={code}.{suffix}&symidbsr={code}&_ts={ts}"
)


# ================= 圖片樣式設定：社群圖卡優化版 =================

WATERMARK_TEXT = "股市艾斯\n台股DC討論群"
TOPRIGHT_WATERMARK_TEXT = "By 股市艾斯出品-轉傳請註明"
DISCLAIMER_TEXT = "資訊分享非投資建議 投資請自行評估風險"
WATERMARK_ALPHA = 0.065
WATERMARK_FONT_SIZE = 124
WATERMARK_ROTATION = 18
TOPRIGHT_WATERMARK_ALPHA = 0.72
TOPRIGHT_WATERMARK_FONT_SIZE = 11
TOPRIGHT_DISCLAIMER_FONT_SIZE = 10
STREAK_NOTE_TEXT = "標記：連2／連3／連4 代表連續 2／3／4 週進入該榜單"

IMG_BG = "#F6F8FB"
CARD_BG = "#FFFFFF"
CARD_BORDER = "#C9D5E3"
HEADER_BG = "#F3F7FC"
TEXT_MAIN = "#111827"
TEXT_DARK = "#061D3D"
TEXT_NAVY = "#0B2E5B"
TEXT_MUTED = "#475569"
TEXT_RED = "#D92323"
TEXT_GREEN = "#16803C"
ACCENT_LISTED = "#2563EB"
ACCENT_OTC = "#16803C"
ACCENT_NAVY = "#061D3D"
ROW_ALT = "#FAFCFF"
TOP1_BG = "#FFF4D9"
TOP2_BG = "#EEF4FF"
TOP3_BG = "#FFF0E6"
TOP1_BORDER = "#E7B84B"
TOP2_BORDER = "#AFC4EA"
TOP3_BORDER = "#E3A678"
TOP1_BADGE = "#F4C95D"
TOP2_BADGE = "#C9D2E3"
TOP3_BADGE = "#E6BA8A"

CJK_FONT_PATH = None
CJK_BOLD_FONT_PATH = None


# ================= 基本工具 =================

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


_ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u202a-\u202e\ufeff]")


def clean_cell(s) -> str:
    s = "" if s is None else str(s)
    s = s.replace("\xa0", " ")
    s = _ZERO_WIDTH_RE.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def clean_text(x):
    return clean_cell(x)


def to_float(x, default=0.0):
    try:
        return float(str(x).replace(",", "").replace("%", "").strip())
    except Exception:
        return default


def to_int(x, default=0):
    try:
        return int(float(str(x).replace(",", "").strip()))
    except Exception:
        return default


def normalize_date_str(raw_date):
    s = "" if raw_date is None else str(raw_date).strip()
    digits = re.sub(r"\D", "", s)

    if len(digits) == 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:]}"
    if len(digits) == 4:
        return f"{datetime.now().year}-{digits[:2]}-{digits[2:]}"

    dt = pd.to_datetime(s, errors="coerce")
    if not pd.isna(dt):
        return dt.strftime("%Y-%m-%d")

    return s if s else "未知日期"


def date_sort_key(x):
    dt = pd.to_datetime(str(x), errors="coerce")
    return pd.Timestamp.min if pd.isna(dt) else dt


def parse_latest_trade_date(raw_date):
    s = "" if raw_date is None else str(raw_date).strip()
    digits = re.sub(r"\D", "", s)
    year_now = datetime.now().year

    try:
        if len(digits) == 4:
            return datetime(year_now, int(digits[:2]), int(digits[2:]))
        if len(digits) == 8:
            return datetime(int(digits[:4]), int(digits[4:6]), int(digits[6:]))
    except Exception:
        pass

    dt = pd.to_datetime(s, errors="coerce")
    if not pd.isna(dt):
        return dt.to_pydatetime()
    return datetime.now()


def fmt_change(x):
    s = str(x)
    s = s.replace("%", "").replace(",", "")
    s = re.sub(r"\s+", "", s)
    v = pd.to_numeric(s, errors="coerce")
    return "-" if pd.isna(v) else f"{v:.2f}"


def split_code_name(raw):
    raw_str = clean_cell(raw)
    match = re.match(r"(\d{4})\s*(.*)", raw_str)
    if match:
        code = clean_cell(match.group(1))
        name = clean_cell(match.group(2).strip())
    else:
        code = clean_cell(raw_str[:4])
        name = clean_cell(raw_str[4:].strip())
    name = name.replace("卅卅", "碁")
    return code, name


def visual_len(s) -> int:
    s = clean_cell(s)
    w = 0
    for ch in s:
        cw = wcwidth(ch)
        if cw > 0:
            w += cw
    return w


def truncate_to_width(s, max_w: int) -> str:
    s = clean_cell(s)
    w = 0
    out = []
    for ch in s:
        cw = wcwidth(ch)
        if cw < 0:
            continue
        if w + cw > max_w:
            break
        out.append(ch)
        w += cw
    return "".join(out)


def pad_visual(s, target_w: int, align="left") -> str:
    s = truncate_to_width(s, target_w)
    diff = max(0, target_w - visual_len(s))
    full_spaces = diff // 2
    half_spaces = diff % 2
    padding = "\u3000" * full_spaces + " " * half_spaces
    return padding + s if align == "right" else s + padding


def to_fullwidth(s):
    res = []
    for char in str(s):
        code = ord(char)
        if 0x21 <= code <= 0x7E:
            res.append(chr(code + 0xFEE0))
        elif code == 0x20:
            res.append(chr(0x3000))
        else:
            res.append(char)
    return "".join(res)


# ================= Google Sheet 連線 =================

HOLDER_HISTORY_HEADERS = [
    "資料日期", "榜單類型", "市場", "排名", "代號", "名稱", "類別",
    "現價", "週漲跌", "總增減%", "寫入時間"
]

API_CACHE_HEADERS = ["代號", "suffix", "api_url", "更新時間"]


def prepare_service_key_file():
    """
    GitHub Actions 可使用：
    - GOOGLE_SERVICE_ACCOUNT_JSON：完整 JSON 內容
    - GOOGLE_SERVICE_ACCOUNT_JSON_BASE64：base64 後的 JSON
    """
    raw_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    raw_b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64", "").strip()

    if raw_json:
        Path(SERVICE_KEY_FILE).write_text(raw_json, encoding="utf-8")
        return

    if raw_b64:
        decoded = base64.b64decode(raw_b64).decode("utf-8")
        Path(SERVICE_KEY_FILE).write_text(decoded, encoding="utf-8")


def connect_google_sheet():
    prepare_service_key_file()

    log(f"準備連線 Google Sheet：{SHEET_NAME}")
    if gspread is None:
        raise RuntimeError("gspread 未安裝，無法連線 Google Sheet。")
    if not os.path.exists(SERVICE_KEY_FILE):
        raise FileNotFoundError(
            f"找不到 {SERVICE_KEY_FILE}。請在 GitHub Secrets 設定 GOOGLE_SERVICE_ACCOUNT_JSON "
            "或將 service_key.json 放在專案根目錄。"
        )

    gc = gspread.service_account(filename=SERVICE_KEY_FILE)
    sh = gc.open(SHEET_NAME)
    log(f"Google Sheet 連線成功：{SHEET_NAME}")
    return sh


def get_or_create_ws(sh, title, headers, rows=2000, cols=None):
    cols = cols or max(20, len(headers) + 10)

    try:
        ws = sh.worksheet(title)
        log(f"已找到工作表：{title}")
    except Exception:
        ws = sh.add_worksheet(title=title, rows=rows, cols=cols)
        log(f"已建立新工作表：{title}")

    values = ws.get_all_values()
    if not values:
        ws.update(values=[headers], range_name="A1")
        log(f"已初始化欄位：{title}")
    elif headers and values[0][:len(headers)] != headers:
        # 只強制修正固定欄位的工作表；比例歷史表會自己整張覆蓋。
        if title in [HOLDER_HISTORY_SHEET_NAME, API_CACHE_SHEET_NAME]:
            ws.update(values=[headers], range_name="A1")
            log(f"已修正欄位：{title}")

    return ws


def overwrite_ws(ws, headers, rows):
    values = [headers] + rows
    ws.clear()
    if values:
        ws.update(values=values, range_name="A1", value_input_option="USER_ENTERED")


def read_records(ws):
    try:
        return ws.get_all_records()
    except Exception:
        return []



def requests_get_with_retry(url, headers=None, timeout=30, retries=5, sleep_sec=5, label="request"):
    last_err = None

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers or {}, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            log(f"⚠️ {label} 失敗 {attempt}/{retries}：{repr(e)}")
            if attempt < retries:
                time.sleep(sleep_sec * attempt)

    raise last_err


def stock_list_from_ratio_sheet(ws, market):
    """
    當 ISIN 網站 DNS 暫時失敗時，優先從 Google Sheet 既有的
    上市/上櫃400張比例歷史表還原股票清單。
    """
    try:
        values = ws.get_all_values()
    except Exception:
        return pd.DataFrame()

    if not values or len(values) < 2:
        return pd.DataFrame()

    headers = [clean_text(x) for x in values[0]]
    need = ["代號", "股名"]
    if not all(c in headers for c in need):
        return pd.DataFrame()

    idx_code = headers.index("代號")
    idx_name = headers.index("股名")
    idx_cat = headers.index("類別") if "類別" in headers else None

    rows = []
    for row in values[1:]:
        code = clean_text(row[idx_code] if idx_code < len(row) else "").replace("'", "")
        name = clean_text(row[idx_name] if idx_name < len(row) else "")
        category = clean_text(row[idx_cat] if idx_cat is not None and idx_cat < len(row) else "-")

        if not (len(code) == 4 and code.isdigit()):
            continue
        if not name:
            name = code

        rows.append({
            "代號": code,
            "股名": name.replace("卅卅", "碁"),
            "市場": market,
            "suffix": "TW" if market == "上市" else "TWO",
            "類別": category if category and category != "nan" else "-",
        })

    out = pd.DataFrame(rows).drop_duplicates(subset=["代號", "市場"]) if rows else pd.DataFrame()
    if not out.empty:
        log(f"✅ 從 {market}400張比例歷史 還原股票清單：{len(out)} 檔")
    return out


def stock_list_from_api_cache(cache, market):
    """
    第二層 fallback：從 PSCNet_API快取還原代號清單。
    這種方式沒有股名/類別，只能先讓程式不中斷；後續 ISIN 恢復後會補回名稱。
    """
    suffix = "TW" if market == "上市" else "TWO"
    rows = []

    for key in cache.keys():
        if "." not in key:
            continue
        code, key_suffix = key.split(".", 1)
        code = clean_text(code)
        key_suffix = clean_text(key_suffix)

        if key_suffix != suffix:
            continue
        if not (len(code) == 4 and code.isdigit()):
            continue

        rows.append({
            "代號": code,
            "股名": code,
            "市場": market,
            "suffix": suffix,
            "類別": "-",
        })

    out = pd.DataFrame(rows).drop_duplicates(subset=["代號", "市場"]) if rows else pd.DataFrame()
    if not out.empty:
        log(f"✅ 從 PSCNet_API快取 還原 {market} 股票清單：{len(out)} 檔")
    return out


def fetch_stock_list_one_market_with_fallback(market, ratio_ws, api_cache):
    try:
        return fetch_isin_stock_list(market)
    except Exception as e:
        log(f"⚠️ {market} ISIN 股票清單抓取失敗，啟用 Google Sheet fallback：{repr(e)}")

        fallback = stock_list_from_ratio_sheet(ratio_ws, market)
        if fallback is not None and not fallback.empty:
            return fallback

        fallback = stock_list_from_api_cache(api_cache, market)
        if fallback is not None and not fallback.empty:
            return fallback

        raise RuntimeError(
            f"{market} 股票清單抓取失敗，且 Google Sheet 沒有可用 fallback。"
            f" 原始錯誤：{repr(e)}"
        )


# ================= 股票清單 =================

def fetch_isin_stock_list(market):
    url = ISIN_URLS[market]
    log(f"抓取 {market} 股票清單...")

    resp = requests_get_with_retry(
        url,
        headers={"User-Agent": USER_AGENT},
        timeout=30,
        retries=STOCK_LIST_RETRY_TIMES,
        sleep_sec=STOCK_LIST_RETRY_SLEEP,
        label=f"{market} ISIN股票清單"
    )
    resp.encoding = "cp950"

    tables = pd.read_html(StringIO(resp.text))
    df = tables[0]

    rows = []
    for _, r in df.iterrows():
        cell = clean_text(r.iloc[0] if len(r) > 0 else "")
        if not cell:
            continue

        if "\u3000" in cell:
            code, name = cell.split("\u3000", 1)
            code, name = clean_text(code), clean_text(name)
        else:
            m = re.match(r"^(\d{4})\s+(.+)$", cell)
            if not m:
                continue
            code, name = m.group(1), clean_text(m.group(2))

        if not (len(code) == 4 and code.isdigit()):
            continue

        category = "-"
        try:
            category = clean_text(r.iloc[4])
        except Exception:
            pass

        if any(k in name for k in ["ETF", "ETN", "指數", "受益", "債", "期貨"]):
            continue

        rows.append({
            "代號": code,
            "股名": name.replace("卅卅", "碁"),
            "市場": market,
            "suffix": "TW" if market == "上市" else "TWO",
            "類別": category if category and category != "nan" else "-",
        })

    out = pd.DataFrame(rows).drop_duplicates(subset=["代號", "市場"])
    log(f"{market} 股票清單筆數：{len(out)}")
    return out


def fetch_all_stock_list(listed_ratio_ws=None, otc_ratio_ws=None, api_cache=None):
    api_cache = api_cache or {}

    if listed_ratio_ws is None or otc_ratio_ws is None:
        listed = fetch_isin_stock_list("上市")
        otc = fetch_isin_stock_list("上櫃")
    else:
        listed = fetch_stock_list_one_market_with_fallback("上市", listed_ratio_ws, api_cache)
        otc = fetch_stock_list_one_market_with_fallback("上櫃", otc_ratio_ws, api_cache)

    out = pd.concat([listed, otc], ignore_index=True)
    out = out.drop_duplicates(subset=["代號", "市場"]).reset_index(drop=True)
    log(f"股票清單合計：{len(out)} 檔")
    return out


# ================= PSCNet / MoneyDJ API 快取 =================

def local_load_api_cache():
    if not LOCAL_API_CACHE_FILE.exists():
        return {}
    try:
        return json.loads(LOCAL_API_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def local_save_api_cache(cache):
    try:
        LOCAL_API_CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        log(f"⚠️ 本機 API 快取寫入失敗：{e}")


def load_api_cache_from_sheet(ws):
    cache = {}

    records = read_records(ws)
    for r in records:
        code = str(r.get("代號", "")).strip().replace("'", "")
        suffix = str(r.get("suffix", "")).strip()
        api_url = str(r.get("api_url", "")).strip()
        if code and suffix and api_url:
            cache[f"{code}.{suffix}"] = api_url

    # 本機 cache 作為補充
    local_cache = local_load_api_cache()
    for k, v in local_cache.items():
        if k not in cache and v:
            cache[k] = v

    log(f"讀取 API 快取：{len(cache)} 筆")
    return cache


def save_api_cache_to_sheet(ws, cache):
    rows = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for key, api_url in sorted(cache.items()):
        if "." in key:
            code, suffix = key.split(".", 1)
        else:
            code, suffix = key, ""
        rows.append([code, suffix, api_url, now])

    overwrite_ws(ws, API_CACHE_HEADERS, rows)
    local_save_api_cache(cache)
    log(f"API 快取已寫入 Google Sheet：{len(rows)} 筆")


def make_pscnet_page_url(code, suffix):
    return PSCNET_PAGE_URL.format(code=code, suffix=suffix, ts=int(time.time() * 1000))


def is_correct_stock_chip0007_url(url, code):
    u = str(url).lower()
    return (
        "twstockdata.xdjjson" in u
        and "x=stock-chip0007" in u
        and f"a=as{code}".lower() in u
    )


def make_discovery_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1400,1000")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-application-cache")
    options.add_argument("--disk-cache-size=0")
    options.add_argument("--media-cache-size=0")
    options.add_argument(f"user-agent={USER_AGENT}")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL", "browser": "ALL"})

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
    driver.set_page_load_timeout(45)

    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    except Exception:
        pass

    return driver


def drain_discovery_logs(driver):
    try:
        driver.get_log("performance")
    except Exception:
        pass


def collect_discovery_urls(driver):
    urls = []
    try:
        logs = driver.get_log("performance")
    except Exception:
        return urls

    for item in logs:
        try:
            msg = json.loads(item.get("message", "{}")).get("message", {})
            method = msg.get("method", "")
            params = msg.get("params", {})

            if method == "Network.requestWillBeSent":
                url = params.get("request", {}).get("url", "")
            elif method == "Network.responseReceived":
                url = params.get("response", {}).get("url", "")
            else:
                url = ""

            if url:
                urls.append(url)
        except Exception:
            pass

    return list(dict.fromkeys(urls))


def chunk_list(items, n_chunks):
    if not items:
        return []
    n_chunks = max(1, min(n_chunks, len(items)))
    chunks = [[] for _ in range(n_chunks)]
    for i, item in enumerate(items):
        chunks[i % n_chunks].append(item)
    return chunks


def discover_cache_worker(worker_id, metas):
    found = {}
    errors = []
    driver = None

    try:
        driver = make_discovery_driver()

        for idx, meta in enumerate(metas, start=1):
            code = str(meta["代號"])
            suffix = str(meta["suffix"])
            key = f"{code}.{suffix}"
            page_url = make_pscnet_page_url(code, suffix)

            try:
                if idx == 1 or idx % 25 == 0:
                    log(f"[API快取Worker {worker_id}] 進度 {idx}/{len(metas)}：{code} {meta.get('股名','')}")

                driver.get("about:blank")
                time.sleep(0.08)
                drain_discovery_logs(driver)
                driver.get(page_url)

                deadline = time.time() + DISCOVER_TIMEOUT_SEC
                hit_url = ""

                while time.time() < deadline:
                    urls = collect_discovery_urls(driver)
                    for u in urls:
                        if is_correct_stock_chip0007_url(u, code):
                            hit_url = u
                            break
                    if hit_url:
                        break
                    time.sleep(0.35)

                if hit_url:
                    found[key] = hit_url
                else:
                    errors.append({
                        "代號": code,
                        "股名": meta.get("股名", ""),
                        "市場": meta.get("市場", ""),
                        "錯誤": "找不到 Stock-Chip0007 API URL",
                    })

            except Exception as e:
                errors.append({
                    "代號": code,
                    "股名": meta.get("股名", ""),
                    "市場": meta.get("市場", ""),
                    "錯誤": repr(e),
                })

    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

    return found, errors


def get_missing_api_metas(stock_df, cache):
    metas = [r.to_dict() for _, r in stock_df.iterrows()]
    missing = []

    for meta in metas:
        code = str(meta["代號"])
        suffix = str(meta["suffix"])
        key = f"{code}.{suffix}"
        url = cache.get(key, "")
        if not url or not is_correct_stock_chip0007_url(url, code):
            missing.append(meta)

    return missing


def ensure_api_cache_threaded(stock_df, cache, api_ws):
    """
    補 Stock-Chip0007 API 快取。

    修正版重點：
    1. 不再只跑一輪。
    2. 每一輪都重新計算缺少哪些股票。
    3. 若 GitHub runner 某些 Chrome worker 不穩，只要下一輪還能補，就會繼續補。
    4. 若最後仍缺太多，直接 raise，避免漏抓太多股票還推播。
    """
    all_errors = []

    if not DISCOVER_MISSING_API:
        missing = get_missing_api_metas(stock_df, cache)
        if missing:
            log(f"API 快取缺 {len(missing)} 檔，但 DISCOVER_MISSING_API=0，略過補快取。")
            if len(missing) > MAX_ALLOWED_MISSING_API:
                raise RuntimeError(
                    f"API 快取缺 {len(missing)} 檔，超過允許上限 {MAX_ALLOWED_MISSING_API}，停止推播。"
                )
            return cache, [{"錯誤": f"API快取缺 {len(missing)} 檔"}]
        log(f"API 快取完整可用：{len(cache)} 筆")
        return cache, []

    total_stocks = len(stock_df)

    for round_idx in range(1, DISCOVER_MAX_ROUNDS + 1):
        missing = get_missing_api_metas(stock_df, cache)
        missing_count = len(missing)

        if missing_count == 0:
            log(f"API 快取完整可用：{len(cache)} / {total_stocks} 筆")
            return cache, all_errors

        if missing_count <= MAX_ALLOWED_MISSING_API:
            log(
                f"API 快取仍缺 {missing_count} 檔，但低於允許上限 "
                f"{MAX_ALLOWED_MISSING_API}，繼續後續抓取。"
            )
            return cache, all_errors

        workers = max(1, min(MAX_DISCOVER_WORKERS, missing_count))
        log(
            f"API 快取不足：缺 {missing_count}/{total_stocks} 檔，"
            f"第 {round_idx}/{DISCOVER_MAX_ROUNDS} 輪用 {workers} 個執行緒補快取"
        )

        chunks = chunk_list(missing, workers)
        round_found = 0
        round_errors = []

        with ThreadPoolExecutor(max_workers=len(chunks)) as ex:
            futures = {
                ex.submit(discover_cache_worker, i + 1, chunk): i + 1
                for i, chunk in enumerate(chunks)
                if chunk
            }

            for fut in as_completed(futures):
                worker_id = futures[fut]
                try:
                    found, errors = fut.result()
                    cache.update(found)
                    round_found += len(found)
                    round_errors.extend(errors)
                    all_errors.extend(errors)
                    save_api_cache_to_sheet(api_ws, cache)
                    log(f"[API快取Worker {worker_id}] 完成：新增 {len(found)}，錯誤 {len(errors)}")
                except Exception as e:
                    err = {"錯誤": f"API快取Worker {worker_id} 失敗：{repr(e)}"}
                    round_errors.append(err)
                    all_errors.append(err)
                    log(f"⚠️ [API快取Worker {worker_id}] 失敗：{repr(e)}")

        save_api_cache_to_sheet(api_ws, cache)

        after_missing = len(get_missing_api_metas(stock_df, cache))
        coverage = (total_stocks - after_missing) / total_stocks * 100 if total_stocks else 0

        log(
            f"第 {round_idx} 輪 API 快取補完：新增 {round_found}，"
            f"目前快取 {len(cache)}，仍缺 {after_missing}，覆蓋率 {coverage:.2f}%"
        )

        # 這一輪完全沒有新增，表示繼續重試也可能卡住，直接跳出檢查門檻。
        if round_found == 0:
            log("⚠️ 本輪沒有新增任何 API 快取，停止重試並進行完整度檢查。")
            break

    final_missing = get_missing_api_metas(stock_df, cache)
    final_missing_count = len(final_missing)

    if final_missing_count > MAX_ALLOWED_MISSING_API:
        sample = ", ".join(
            f"{m.get('代號','')} {m.get('股名','')}" for m in final_missing[:20]
        )
        raise RuntimeError(
            f"API 快取仍缺 {final_missing_count} 檔，超過允許上限 {MAX_ALLOWED_MISSING_API}，"
            f"為避免漏抓太多股票，停止推播。缺漏範例：{sample}"
        )

    log(f"API 快取補齊完成，目前快取 {len(cache)}，仍缺 {final_missing_count} 檔。")
    return cache, all_errors


# ================= PSCNet JSON 解析 =================

def requests_get_json(url, timeout=25):
    r = requests.get(url, headers=HDR, timeout=timeout)
    r.raise_for_status()

    try:
        text = r.content.decode("utf-8-sig")
    except Exception:
        r.encoding = r.apparent_encoding
        text = r.text

    return json.loads(text)


def get_result_rows(data):
    if isinstance(data, list):
        if not data:
            return []
        data = data[0]

    if not isinstance(data, dict):
        return []

    rs = data.get("ResultSet", {})
    if not isinstance(rs, dict):
        return []

    result = rs.get("Result", [])
    return result if isinstance(result, list) else []


def normalize_level_text(level):
    s = clean_text(level)
    s = s.replace(",", "")
    s = s.replace("股", "")
    s = re.sub(r"\s+", "", s)
    return s


def is_normal_level(level):
    s = normalize_level_text(level)
    if not s:
        return False
    if "差異" in s or "調整" in s or "合計" in s:
        return False
    return bool(re.search(r"\d", s))


def is_total_holder_level(level):
    s = normalize_level_text(level)
    return bool(s) and "合計" not in s


def is_400_up_level(level):
    s = normalize_level_text(level)
    if not s:
        return False
    if "差異" in s or "調整" in s or "合計" in s:
        return False

    m = re.search(r"(\d+)以上", s)
    if m:
        return int(m.group(1)) >= 400001

    m = re.search(r"(\d+)-(\d+)", s)
    if m:
        return int(m.group(1)) >= 400001

    return False


def parse_pscnet_json_one_stock(meta, data):
    result = get_result_rows(data)
    grouped = {}

    for row in result:
        date = normalize_date_str(row.get("V1", ""))
        level = clean_text(row.get("V2", ""))

        if not date or not level:
            continue

        people = to_int(row.get("V3", 0))
        shares = to_float(row.get("V4", 0))

        if date not in grouped:
            grouped[date] = {
                "代號": meta["代號"],
                "股名": meta["股名"],
                "市場": meta["市場"],
                "suffix": meta["suffix"],
                "類別": meta.get("類別", "-"),
                "資料日期": date,
                "總股東人數": 0,
                "正常分級總股數": 0.0,
                "400張以上股數": 0.0,
            }

        if is_total_holder_level(level):
            grouped[date]["總股東人數"] += people

        if is_normal_level(level):
            grouped[date]["正常分級總股數"] += shares

        if is_400_up_level(level):
            grouped[date]["400張以上股數"] += shares

    rows = []
    for rec in grouped.values():
        total_shares = rec["正常分級總股數"]
        over_shares = rec["400張以上股數"]

        if total_shares <= 0:
            continue

        over_pct = round(over_shares / total_shares * 100, 2)
        under_pct = round(100 - over_pct, 2)

        rows.append({
            "代號": rec["代號"],
            "股名": rec["股名"],
            "市場": rec["市場"],
            "suffix": rec["suffix"],
            "類別": rec["類別"],
            "資料日期": rec["資料日期"],
            "400張以上": over_pct,
            "400張未滿": under_pct,
            "總股東人數": rec["總股東人數"],
        })

    return rows


def fetch_pscnet_history_all(stock_df, cache):
    metas = [r.to_dict() for _, r in stock_df.iterrows()]
    out_rows = []
    errors = []

    def one(meta):
        code = str(meta["代號"])
        key = f"{code}.{meta['suffix']}"
        url = cache.get(key, "")

        if not url:
            return [], {"代號": code, "股名": meta["股名"], "市場": meta["市場"], "錯誤": "沒有 PSCNet API 快取"}

        try:
            data = requests_get_json(url)
            rows = parse_pscnet_json_one_stock(meta, data)
            return rows, None
        except Exception as e:
            return [], {"代號": code, "股名": meta["股名"], "市場": meta["市場"], "錯誤": repr(e), "api_url": url}

    log(f"PSCNet requests 抓 60週資料：{len(metas)} 檔，MAX_WORKERS={MAX_WORKERS}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(one, meta): meta for meta in metas}

        done = 0
        for fut in as_completed(futures):
            done += 1
            rows, err = fut.result()
            out_rows.extend(rows)

            if err:
                errors.append(err)

            if done % 100 == 0 or done == len(metas):
                log(f"PSCNet 歷史進度 {done}/{len(metas)}，資料列 {len(out_rows)}，錯誤 {len(errors)}")

    df = pd.DataFrame(out_rows)
    if df.empty:
        raise RuntimeError("PSCNet 沒有抓到任何 60 週歷史資料，請檢查 API 快取或網站連線。")

    if len(errors) > MAX_ALLOWED_REQUEST_ERRORS:
        missing_api_errors = [e for e in errors if e.get("錯誤") == "沒有 PSCNet API 快取"]
        sample = ", ".join(
            f"{e.get('代號','')} {e.get('股名','')}" for e in errors[:20]
        )
        raise RuntimeError(
            f"PSCNet requests 錯誤 {len(errors)} 檔，超過允許上限 {MAX_ALLOWED_REQUEST_ERRORS}。"
            f"其中沒有API快取 {len(missing_api_errors)} 檔。"
            f"為避免漏抓太多股票，停止推播。錯誤範例：{sample}"
        )

    return df, pd.DataFrame(errors)


# ================= 歷史比例表與排名 =================

def identify_date_columns(df):
    base_cols = {"代號", "股名", "類別", "與上週相比增減%", "最新400張未滿", "總股東人數"}
    cols = []
    for c in df.columns:
        if c in base_cols:
            continue
        d = pd.to_datetime(str(c), errors="coerce")
        if not pd.isna(d):
            cols.append(c)
    return cols


def build_ratio_history_from_long(history_long_df, market):
    if history_long_df is None or history_long_df.empty:
        return pd.DataFrame()

    df = history_long_df[history_long_df["市場"] == market].copy()
    if df.empty:
        return pd.DataFrame()

    df["資料日期"] = df["資料日期"].map(normalize_date_str)
    df["_date"] = pd.to_datetime(df["資料日期"], errors="coerce")
    df = df.dropna(subset=["_date"])

    date_cols = sorted(df["資料日期"].dropna().unique(), key=lambda x: date_sort_key(x), reverse=True)
    if not date_cols:
        return pd.DataFrame()

    latest_date = date_cols[0]

    pivot = df.pivot_table(
        index=["代號", "股名", "類別"],
        columns="資料日期",
        values="400張以上",
        aggfunc="first"
    ).reset_index()

    latest_info = df[df["資料日期"] == latest_date].copy()
    under_map = dict(zip(latest_info["代號"].astype(str), latest_info["400張未滿"]))
    holder_map = dict(zip(latest_info["代號"].astype(str), latest_info["總股東人數"]))

    pivot["代號"] = pivot["代號"].astype(str)
    pivot["最新400張未滿"] = pivot["代號"].map(under_map)
    pivot["總股東人數"] = pivot["代號"].map(holder_map)

    if len(date_cols) >= 2:
        latest_col = date_cols[0]
        prev_col = date_cols[1]
        pivot["與上週相比增減%"] = (
            pd.to_numeric(pivot[latest_col], errors="coerce")
            - pd.to_numeric(pivot[prev_col], errors="coerce")
        ).round(2)
    else:
        pivot["與上週相比增減%"] = pd.NA

    keep_cols = ["代號", "股名", "類別", "與上週相比增減%", "最新400張未滿", "總股東人數"] + date_cols
    out = pivot[[c for c in keep_cols if c in pivot.columns]].copy()
    out = out.sort_values("與上週相比增減%", ascending=False, na_position="last").reset_index(drop=True)

    return out


def build_rank_from_history(hist, market, rank_type="增加"):
    if hist is None or hist.empty:
        return pd.DataFrame()

    df = hist.copy()
    df["總增減"] = pd.to_numeric(df["與上週相比增減%"], errors="coerce")
    df = df.dropna(subset=["總增減"])

    if df.empty:
        return pd.DataFrame()

    date_cols = identify_date_columns(df)
    date_cols = sorted(date_cols, key=lambda x: date_sort_key(x), reverse=True)
    latest_date = date_cols[0] if date_cols else ""

    ascending = True if rank_type == "減少" else False
    out = df.sort_values("總增減", ascending=ascending).head(TOP_N).copy().reset_index(drop=True)
    out["市場"] = market
    out["suffix"] = ".TW" if market == "上市" else ".TWO"
    out["最新日期"] = latest_date
    out["股票代號/名稱"] = out["代號"].astype(str) + " " + out["股名"].astype(str)
    out["榜單類型"] = rank_type

    return out


def build_top_from_history(hist, market):
    return build_rank_from_history(hist, market, "增加")


def build_bottom_from_history(hist, market):
    return build_rank_from_history(hist, market, "減少")


def get_latest_data_date_from_hist(hist_list):
    dates = []
    for hist in hist_list:
        if hist is None or hist.empty:
            continue
        dates.extend(identify_date_columns(hist))
    if not dates:
        return "未知日期"
    dates = sorted(set(dates), key=lambda x: date_sort_key(x), reverse=True)
    return dates[0]


def sheet_values_from_df(df):
    if df is None or df.empty:
        return [], []
    df = df.copy()
    df = df.where(pd.notna(df), "")
    headers = list(df.columns)
    rows = df.astype(str).values.tolist()
    return headers, rows


def write_ratio_history_sheet(ws, hist):
    headers, rows = sheet_values_from_df(hist)
    overwrite_ws(ws, headers, rows)


# ================= 股價資訊 =================

def get_week_price_info(code, market_suffix, latest_date_str):
    try:
        ref_date = parse_latest_trade_date(latest_date_str)
        week_start = ref_date - timedelta(days=ref_date.weekday())
        fetch_start = week_start - timedelta(days=15)
        fetch_end = week_start + timedelta(days=7)

        ticker = f"{code}{market_suffix}"
        df = yf.Ticker(ticker).history(
            start=fetch_start.strftime("%Y-%m-%d"),
            end=fetch_end.strftime("%Y-%m-%d"),
            auto_adjust=True
        )

        if df.empty or "Close" not in df.columns:
            return "-", "-"

        df = df.dropna(subset=["Close"])

        try:
            df.index = df.index.tz_localize(None)
        except Exception:
            pass

        past_df = df[df.index < week_start]
        current_week_df = df[df.index >= week_start]

        if past_df.empty or current_week_df.empty:
            return "-", "-"

        prev_close = float(past_df["Close"].iloc[-1])
        current_close = float(current_week_df["Close"].iloc[-1])

        if prev_close <= 0:
            return f"{current_close:.1f}", "-"

        week_pct = ((current_close - prev_close) / prev_close) * 100
        arrow = "▲" if week_pct > 0 else "▼" if week_pct < 0 else "—"

        return f"{current_close:.1f}", f"{arrow}{abs(week_pct):.1f}%"

    except Exception as e:
        print(f"⚠️ 股價資料取得失敗 ({code}{market_suffix}): {e}")
        return "-", "-"


def add_price_info(df):
    if df is None or df.empty:
        return df

    out = df.copy()
    metas = [r.to_dict() for _, r in out.iterrows()]
    price_map = {}

    def one(meta):
        code = str(meta["代號"])
        suffix = str(meta["suffix"])
        return code, get_week_price_info(code, suffix, str(meta["最新日期"]))

    with ThreadPoolExecutor(max_workers=PRICE_WORKERS) as ex:
        futures = {ex.submit(one, meta): meta for meta in metas}
        for fut in as_completed(futures):
            try:
                code, result = fut.result()
                price_map[str(code)] = result
            except Exception:
                pass

    out["現價"] = out["代號"].astype(str).map(lambda c: price_map.get(c, ("-", "-"))[0])
    out["週漲跌"] = out["代號"].astype(str).map(lambda c: price_map.get(c, ("-", "-"))[1])

    return out


# ================= 大戶排行歷史紀錄與連續上榜 =================

def normalize_history_date(raw_date):
    return normalize_date_str(raw_date)


def parse_history_pct(x, invalid_value=None):
    try:
        s = str(x).replace("%", "").replace(",", "")
        s = re.sub(r"\s+", "", s)
        return float(s)
    except Exception:
        return invalid_value


def rows_to_append_values(rows):
    return [[r.get(h, "") for h in HOLDER_HISTORY_HEADERS] for r in rows]


def append_history_rows(ws, rows):
    if ws is None:
        raise RuntimeError("工作表物件為空，無法寫入每週大戶排行紀錄。")
    if not rows:
        return 0

    existing_records = ws.get_all_records()
    existing_keys = set()

    for r in existing_records:
        existing_keys.add((
            str(r.get("資料日期", "")).strip(),
            str(r.get("榜單類型", "")).strip(),
            str(r.get("市場", "")).strip(),
            str(r.get("代號", "")).replace("'", "").strip(),
        ))

    new_rows = []
    for r in rows:
        key = (
            str(r.get("資料日期", "")).strip(),
            str(r.get("榜單類型", "")).strip(),
            str(r.get("市場", "")).strip(),
            str(r.get("代號", "")).replace("'", "").strip(),
        )
        if key not in existing_keys:
            new_rows.append(r)
            existing_keys.add(key)

    if new_rows:
        ws.append_rows(rows_to_append_values(new_rows), value_input_option="USER_ENTERED")
    return len(new_rows)


def build_rank_rows_for_date(hist, market, date_idx, rank_type="增加", top_n=20):
    if hist is None or hist.empty:
        return []

    date_cols = identify_date_columns(hist)
    date_cols = sorted(date_cols, key=lambda x: date_sort_key(x), reverse=True)

    if len(date_cols) < date_idx + 2:
        return []

    cur_col = date_cols[date_idx]
    prev_col = date_cols[date_idx + 1]
    rank_date = normalize_history_date(cur_col)

    df = hist.copy()
    df["_diff"] = (
        pd.to_numeric(df[cur_col], errors="coerce")
        - pd.to_numeric(df[prev_col], errors="coerce")
    ).round(2)
    df = df.dropna(subset=["_diff"])

    ascending = rank_type == "減少"
    top_df = df.sort_values("_diff", ascending=ascending).head(top_n).copy()

    write_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for rank, (_, row) in enumerate(top_df.iterrows(), start=1):
        rows.append({
            "資料日期": rank_date,
            "榜單類型": rank_type,
            "市場": market,
            "排名": rank,
            "代號": str(row.get("代號", "")),
            "名稱": str(row.get("股名", "")),
            "類別": str(row.get("類別", "-")),
            "現價": "-",
            "週漲跌": "-",
            "總增減%": f"{float(row['_diff']):+.2f}%",
            "寫入時間": write_time,
        })

    return rows


def backfill_holder_history_from_ratio(ws, listed_hist, otc_hist, weeks):
    """
    用我們自己的比例歷史表回補最近幾週 Top20，
    不再回去爬 Norway。
    """
    log(f"正在用 PSCNet 歷史資料回補最近 {weeks} 週 Top20 紀錄...")

    all_rows = []
    for market, hist in [("上市", listed_hist), ("上櫃", otc_hist)]:
        for i in range(1, weeks):  # 從上一週開始回補；本週會用 current rows 寫入
            all_rows.extend(build_rank_rows_for_date(hist, market, i, "增加", TOP_N))

    added = append_history_rows(ws, all_rows)
    log(f"歷史 Top20 回補完成，新增 {added} 筆。")


def build_current_history_rows(df, display_date, rank_type, market):
    if df is None or df.empty:
        return []
    history_date = normalize_history_date(display_date)
    write_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for idx, row in df.reset_index(drop=True).iterrows():
        code, name = split_code_name(row.get("股票代號/名稱", ""))
        if not code:
            code = str(row.get("代號", ""))
            name = str(row.get("股名", ""))

        rows.append({
            "資料日期": history_date,
            "榜單類型": rank_type,
            "市場": market,
            "排名": idx + 1,
            "代號": code,
            "名稱": name,
            "類別": clean_cell(row.get("類別", "-")),
            "現價": clean_cell(row.get("現價", "-")),
            "週漲跌": clean_cell(row.get("週漲跌", "-")),
            "總增減%": f"{float(row.get('總增減', 0)):+.2f}%",
            "寫入時間": write_time,
        })
    return rows


def append_current_rank_history(ws, listed_df, otc_df, display_date, rank_type):
    rows = []
    rows.extend(build_current_history_rows(listed_df, display_date, rank_type, "上市"))
    rows.extend(build_current_history_rows(otc_df, display_date, rank_type, "上櫃"))
    added = append_history_rows(ws, rows)
    log(f"每週大戶{rank_type}排行本週紀錄新增 {added} 筆。")


def compute_streak_map(ws):
    streak_map = {}
    if ws is None:
        return streak_map

    records = ws.get_all_records()
    if not records:
        return streak_map

    df = pd.DataFrame(records)
    required_cols = {"資料日期", "榜單類型", "市場", "代號"}
    if not required_cols.issubset(df.columns):
        return streak_map

    df["資料日期"] = df["資料日期"].astype(str).str.strip()
    df["榜單類型"] = df["榜單類型"].astype(str).str.strip()
    df["市場"] = df["市場"].astype(str).str.strip()
    df["代號"] = df["代號"].astype(str).str.replace("'", "", regex=False).str.strip()
    df = df[(df["資料日期"] != "") & (df["代號"] != "")]

    for (rank_type, market), group in df.groupby(["榜單類型", "市場"]):
        dates = sorted(group["資料日期"].unique().tolist(), reverse=True)
        date_code_map = {
            d: set(group[group["資料日期"] == d]["代號"].astype(str).tolist())
            for d in dates
        }
        all_codes = set(group["代號"].astype(str).tolist())
        for code in all_codes:
            streak = 0
            for d in dates:
                if code in date_code_map.get(d, set()):
                    streak += 1
                else:
                    break
            if streak >= 2:
                streak_map[(rank_type, market, code)] = streak
    return streak_map


def maybe_extend_history_for_long_streak(ws, streak_map, listed_hist, otc_hist):
    if ws is None or not streak_map:
        return streak_map

    max_streak = max(streak_map.values()) if streak_map else 0
    if max_streak >= HISTORY_INITIAL_WEEKS:
        log(f"偵測到連{max_streak}上榜股票，擴充回補最近 {HISTORY_EXTEND_WEEKS} 週歷史資料...")
        backfill_holder_history_from_ratio(ws, listed_hist, otc_hist, HISTORY_EXTEND_WEEKS)
        return compute_streak_map(ws)
    return streak_map


def apply_streak_labels(df, market, rank_type, streak_map):
    if df is None or df.empty:
        return df

    df = df.copy()
    new_names = []

    for _, row in df.iterrows():
        code, name = split_code_name(row.get("股票代號/名稱", ""))
        if not code:
            code = str(row.get("代號", ""))
            name = str(row.get("股名", ""))

        streak = streak_map.get((rank_type, market, code), 1)
        if streak >= 2:
            new_names.append(f"{code} {name}  連{streak}")
        else:
            new_names.append(f"{code} {name}")

    df["股票代號/名稱"] = new_names
    return df


# ================= 圖片排版：沿用原始程式 =================

def load_cjk_font(bold=False):
    global CJK_FONT_PATH, CJK_BOLD_FONT_PATH

    regular_paths = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJKtc-Regular.otf",
        "/usr/share/fonts/noto-cjk/NotoSansCJKtc-Regular.otf",
        "/usr/local/share/fonts/NotoSansCJKtc-Regular.otf",
        "C:/Windows/Fonts/msjh.ttc",
        "/System/Library/Fonts/PingFang.ttc",
    ]
    bold_paths = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Black.ttc",
        "/usr/share/fonts/noto-cjk/NotoSansCJKtc-Bold.otf",
        "/usr/local/share/fonts/NotoSansCJKtc-Bold.otf",
        "C:/Windows/Fonts/msjhbd.ttc",
        "/System/Library/Fonts/PingFang.ttc",
    ]

    paths = bold_paths if bold else regular_paths
    for path in paths:
        if os.path.exists(path):
            font_manager.fontManager.addfont(path)
            if bold:
                CJK_BOLD_FONT_PATH = path
            else:
                CJK_FONT_PATH = path
            return font_manager.FontProperties(fname=path)

    return font_manager.FontProperties(family="DejaVu Sans")


FONT_PROP = load_cjk_font(True)
FONT_BOLD = load_cjk_font(True)

try:
    sans_list = []
    for font_path in [CJK_FONT_PATH, CJK_BOLD_FONT_PATH]:
        if font_path:
            try:
                sans_list.append(font_manager.FontProperties(fname=font_path).get_name())
            except Exception:
                pass
    sans_list.extend([
        "Noto Sans CJK TC",
        "Noto Sans CJK JP",
        "Microsoft JhengHei",
        "PingFang TC",
        "DejaVu Sans",
    ])
    plt.rcParams["font.family"] = "sans-serif"
    plt.rcParams["font.sans-serif"] = list(dict.fromkeys(sans_list))
    plt.rcParams["axes.unicode_minus"] = False
except Exception as e:
    print(f"⚠️ matplotlib 字型設定失敗: {e}")


def draw_text(ax, x, y, text, size=14, color=TEXT_MAIN, weight="normal",
              ha="left", va="center", bold=False, alpha=1.0):
    txt = ax.text(
        x, y, clean_cell(text),
        transform=ax.transAxes,
        ha=ha, va=va,
        fontsize=size,
        fontweight=weight,
        fontproperties=FONT_BOLD if bold else FONT_PROP,
        color=color,
        alpha=alpha,
        zorder=5
    )

    # Discord 預覽圖會先縮圖，較細的中文字容易糊在一起。
    # 這裡統一加上很淡的描邊，讓未點開圖片時也比較清楚。
    stroke_fg = "#0B1F3A" if str(color).upper() in {"#FFFFFF", "WHITE"} else "#FFFFFF"
    stroke_lw = 1.10 if size >= 16 else 0.90
    txt.set_path_effects([
        pe.withStroke(linewidth=stroke_lw, foreground=stroke_fg, alpha=0.82)
    ])
    return txt


def _shorten_text(text, max_chars):
    text = clean_cell(text)
    if len(text) <= max_chars:
        return text
    return text[:max_chars - 1] + "…"


def _split_streak_badge(text):
    text = clean_cell(text)
    match = re.search(r"\s*(連\d+)$", text)
    if match:
        badge = match.group(1)
        base_text = clean_cell(text[:match.start()].strip())
        return base_text, badge
    return text, ""


def _rank_summary(df):
    if df is None or df.empty:
        return "無資料", "-", TEXT_MUTED

    row = df.reset_index(drop=True).iloc[0]
    code, name = split_code_name(row.get("股票代號/名稱", ""))
    name, _ = _split_streak_badge(name)
    change_str = fmt_change(row.get("總增減", 0))

    try:
        change_val = float(change_str)
        change_text = f"{change_val:+.2f}%"
        change_color = TEXT_RED if change_val > 0 else TEXT_GREEN if change_val < 0 else TEXT_MUTED
    except Exception:
        change_text = "-"
        change_color = TEXT_MUTED

    label = f"{code} {name}".strip() if code or name else "無資料"
    return label, change_text, change_color


def _draw_kpi_card(ax, x, y, w, h, title, main_value, sub_value, accent, sub_color=None):
    sub_color = sub_color or accent

    ax.add_patch(patches.FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.006,rounding_size=0.018",
        linewidth=1.25, edgecolor=accent, facecolor=CARD_BG,
        transform=ax.transAxes, zorder=2
    ))
    ax.add_patch(patches.FancyBboxPatch(
        (x + 0.012, y + h - 0.028), 0.060, 0.010,
        boxstyle="round,pad=0.001,rounding_size=0.006",
        linewidth=0, facecolor=accent,
        transform=ax.transAxes, zorder=3
    ))
    draw_text(ax, x + 0.022, y + h - 0.045, title,
              size=13.5, color=TEXT_MUTED, weight="bold", bold=True)
    draw_text(ax, x + 0.022, y + h * 0.48, _shorten_text(main_value, 16),
              size=18.5, color=TEXT_DARK, weight="bold", bold=True)
    draw_text(ax, x + 0.022, y + 0.026, sub_value,
              size=15.0, color=sub_color, weight="bold", bold=True)


def draw_rank_table(ax, df, title, accent, x_left, y_top, card_w, card_h, top_n=20):
    title_h = 0.072
    header_gap = 0.012
    header_h = 0.054
    inner_pad_x = 0.014
    inner_w = card_w - inner_pad_x * 2
    row_h = (card_h - title_h - header_gap - header_h - 0.024) / max(top_n, 1)

    ax.add_patch(patches.FancyBboxPatch(
        (x_left, y_top - card_h), card_w, card_h,
        boxstyle="round,pad=0.006,rounding_size=0.018",
        linewidth=1.25, edgecolor=CARD_BORDER, facecolor=CARD_BG,
        transform=ax.transAxes, zorder=1
    ))

    # 標題列重新整理：保留上方圓角，但下緣改成乾淨直角，避免看起來和表頭區塊重疊。
    ax.add_patch(patches.FancyBboxPatch(
        (x_left, y_top - title_h), card_w, title_h,
        boxstyle="round,pad=0,rounding_size=0.016",
        linewidth=0, facecolor=ACCENT_NAVY,
        transform=ax.transAxes, zorder=2
    ))
    ax.add_patch(patches.Rectangle(
        (x_left, y_top - title_h), card_w, title_h * 0.58,
        linewidth=0, facecolor=ACCENT_NAVY,
        transform=ax.transAxes, zorder=2
    ))
    ax.add_patch(patches.Rectangle(
        (x_left, y_top - title_h), 0.009, title_h,
        linewidth=0, facecolor=accent,
        transform=ax.transAxes, zorder=3
    ))

    draw_text(ax, x_left + 0.026, y_top - title_h / 2, title,
              size=20, color="#FFFFFF", weight="bold", bold=True)
    draw_text(ax, x_left + card_w - 0.020, y_top - title_h / 2, f"TOP {top_n}",
              size=15.0, color="#FFFFFF", weight="bold", bold=True, ha="right")

    # 欄位分配：新增一個很窄的「連續上榜標記欄」放在股名與類別之間。
    # 這樣連2／連3不會被畫在類別文字裡面；右側四個欄位也縮小一點，距離更緊湊。
    col_rel = [0.058, 0.083, 0.277, 0.042, 0.135, 0.115, 0.140, 0.150]
    labels = ["排名", "代號", "股名", "", "類別", "現價", "週漲跌", "總增減%"]
    aligns = ["center", "center", "left", "center", "left", "center", "center", "right"]
    streak_col_idx = 3

    x0 = x_left + inner_pad_x
    col_x = [x0]
    acc = 0
    for w in col_rel[:-1]:
        acc += w
        col_x.append(x0 + inner_w * acc)

    # 表頭與深藍標題列之間留白，避免視覺上貼住「上市排行 / 上櫃排行」。
    header_top = y_top - title_h - header_gap
    ax.add_patch(patches.Rectangle(
        (x_left, header_top - header_h), card_w, header_h,
        linewidth=0, facecolor=HEADER_BG,
        transform=ax.transAxes, zorder=2
    ))
    ax.plot([x_left, x_left + card_w], [header_top, header_top],
            transform=ax.transAxes, color="#E6EDF5", linewidth=0.9, zorder=3)
    ax.plot([x_left, x_left + card_w], [header_top - header_h, header_top - header_h],
            transform=ax.transAxes, color=CARD_BORDER, linewidth=0.8, zorder=3)

    for i, label in enumerate(labels):
        if label == "":
            continue

        cell_x = col_x[i]
        cell_w = inner_w * col_rel[i]
        if aligns[i] == "center":
            tx, ha = cell_x + cell_w / 2, "center"
        elif aligns[i] == "right":
            tx, ha = cell_x + cell_w - 0.010, "right"
        else:
            tx, ha = cell_x + 0.010, "left"

        draw_text(ax, tx, header_top - header_h / 2, label, size=14.2,
                  color=TEXT_NAVY, weight="bold", ha=ha, bold=True)

    if df is None or df.empty:
        draw_text(ax, x_left + card_w / 2, header_top - header_h - row_h / 2,
                  "無資料", size=15.2, color=TEXT_MUTED, ha="center", bold=True)
        return

    df = df.head(top_n).reset_index(drop=True)
    for i in range(top_n):
        y = header_top - header_h - i * row_h
        if i < len(df):
            row = df.iloc[i]
            code, name = split_code_name(row["股票代號/名稱"])
            name, streak_badge = _split_streak_badge(name)
            category = clean_cell(row.get("類別", "-"))
            price = clean_cell(row.get("現價", "-"))
            week_chg = clean_cell(row.get("週漲跌", "-"))
            change_str = fmt_change(row["總增減"])
            try:
                change_val = float(change_str)
            except Exception:
                change_val = 0.0
        else:
            code, name, category, price, week_chg, change_val = "", "", "", "", "", 0.0
            streak_badge = ""
            change_str = "-"

        if i == 0:
            bg, edge, lw = TOP1_BG, TOP1_BORDER, 1.20
        elif i == 1:
            bg, edge, lw = TOP2_BG, TOP2_BORDER, 1.05
        elif i == 2:
            bg, edge, lw = TOP3_BG, TOP3_BORDER, 1.05
        else:
            bg, edge, lw = ("#FFFFFF" if i % 2 == 0 else ROW_ALT), "#E8EDF3", 0.45

        ax.add_patch(patches.Rectangle(
            (x_left, y - row_h), card_w, row_h,
            linewidth=lw, edgecolor=edge if edge else "none", facecolor=bg,
            transform=ax.transAxes, zorder=2
        ))
        ax.plot([x_left + 0.012, x_left + card_w - 0.012], [y - row_h, y - row_h],
                transform=ax.transAxes, color="#E8EDF3", linewidth=0.55, zorder=3)

        if "▲" in week_chg:
            week_color = TEXT_RED
        elif "▼" in week_chg:
            week_color = TEXT_GREEN
        else:
            week_color = TEXT_MUTED

        chg_color = TEXT_RED if change_val > 0 else TEXT_GREEN if change_val < 0 else TEXT_MUTED
        chg_display = f"{change_val:+.2f}%" if change_str != "-" else "-"

        values = [
            f"{i+1:02d}",
            code,
            _shorten_text(name, 10),
            clean_cell(streak_badge),
            _shorten_text(category, 6),
            price,
            week_chg,
            chg_display,
        ]

        name_weight = "bold" if i < 3 else "normal"
        colors = [TEXT_MUTED, TEXT_DARK, TEXT_MAIN, "#A06A00", TEXT_MUTED, TEXT_MAIN, week_color, chg_color]
        weights = ["bold", "bold", name_weight, "bold", "normal", "bold", "bold", "bold"]
        sizes = [11.6, 14.8, 16.8 if i < 3 else 14.9, 10.3, 12.4, 13.3, 14.5, 15.4]

        rank_cell_x = col_x[0]
        rank_cell_w = inner_w * col_rel[0]
        rank_center_x = rank_cell_x + rank_cell_w / 2
        rank_center_y = y - row_h / 2
        if i < 3:
            badge_color = [TOP1_BADGE, TOP2_BADGE, TOP3_BADGE][i]
            ax.add_patch(patches.Circle(
                (rank_center_x, rank_center_y), row_h * 0.305,
                transform=ax.transAxes, facecolor=badge_color,
                edgecolor="white", linewidth=1.2, zorder=4
            ))
            draw_text(ax, rank_center_x, rank_center_y, values[0], size=12.6,
                      color="#6B4A12" if i == 0 else TEXT_DARK, weight="bold", ha="center", bold=True)
            start_j = 1
        else:
            start_j = 0

        for j in range(start_j, len(values)):
            value = values[j]
            if j == streak_col_idx:
                if not value:
                    continue
                cell_x = col_x[j]
                cell_w = inner_w * col_rel[j]
                badge_w = min(cell_w * 0.92, 0.034)
                badge_h = row_h * 0.48
                badge_x = cell_x + (cell_w - badge_w) / 2
                badge_y = y - row_h / 2 - badge_h / 2
                ax.add_patch(patches.FancyBboxPatch(
                    (badge_x, badge_y), badge_w, badge_h,
                    boxstyle="round,pad=0.001,rounding_size=0.0045",
                    linewidth=0.8, edgecolor="#D8B83F", facecolor="#FFF3C4",
                    transform=ax.transAxes, zorder=7
                ))
                ax.text(
                    badge_x + badge_w / 2, y - row_h / 2, value,
                    transform=ax.transAxes,
                    ha="center", va="center",
                    fontsize=sizes[j],
                    fontweight="bold",
                    fontproperties=FONT_BOLD,
                    color=colors[j],
                    zorder=8
                )
                continue

            cell_x = col_x[j]
            cell_w = inner_w * col_rel[j]
            if aligns[j] == "center":
                tx, ha = cell_x + cell_w / 2, "center"
            elif aligns[j] == "right":
                tx, ha = cell_x + cell_w - 0.010, "right"
            else:
                tx, ha = cell_x + 0.010, "left"

            draw_text(ax, tx, y - row_h / 2, value, size=sizes[j],
                      color=colors[j], weight=weights[j], ha=ha,
                      bold=(weights[j] == "bold"))

def build_rank_image(listed_df, otc_df, display_date, main_title="每週大股東籌碼強勢榜  Top 20"):
    top_n = 20
    fig_w = 19.4
    fig_h = 12.8

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), facecolor=IMG_BG)
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.set_position([0, 0, 1, 1])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_axis_off()

    is_decrease_rank = "減少" in str(main_title)
    theme_color = TEXT_GREEN if is_decrease_rank else TEXT_RED
    theme_bg = "#EFFAF2" if is_decrease_rank else "#FFF2F2"

    # Header 背景卡片
    ax.add_patch(patches.FancyBboxPatch(
        (0.020, 0.875), 0.960, 0.104,
        boxstyle="round,pad=0.006,rounding_size=0.020",
        linewidth=1.0, edgecolor="#D9E2EE", facecolor="#FFFFFF",
        transform=ax.transAxes, zorder=1
    ))
    ax.add_patch(patches.FancyBboxPatch(
        (0.040, 0.952), 0.090, 0.011,
        boxstyle="round,pad=0.001,rounding_size=0.007",
        linewidth=0, facecolor=theme_color,
        transform=ax.transAxes, zorder=2
    ))
    draw_text(ax, 0.040, 0.915, main_title,
              size=34, color=TEXT_DARK, weight="bold", ha="left", bold=True)
    draw_text(ax, 0.042, 0.880, f"資料統計日期：{display_date}　｜　上市 / 上櫃各 TOP {top_n}",
              size=16.0, color=TEXT_NAVY, weight="bold", ha="left", bold=True)

    ax.add_patch(patches.FancyBboxPatch(
        (0.792, 0.899), 0.160, 0.045,
        boxstyle="round,pad=0.004,rounding_size=0.018",
        linewidth=0, facecolor=theme_bg,
        transform=ax.transAxes, zorder=2
    ))
    draw_text(ax, 0.872, 0.921, "股權分散｜400張以上比例", size=16.2,
              color=theme_color, weight="bold", ha="center", bold=True)

    # 不顯示額外摘要卡，讓畫面回到單純排名表格。
    card_y_top = 0.842
    card_h = 0.790
    gap = 0.020
    card_w = (0.960 - gap) / 2
    left_x = 0.020
    right_x = left_x + card_w + gap

    draw_rank_table(
        ax,
        listed_df.reset_index(drop=True) if listed_df is not None else None,
        "上市排行",
        ACCENT_LISTED,
        left_x,
        card_y_top,
        card_w,
        card_h,
        top_n=top_n,
    )
    draw_rank_table(
        ax,
        otc_df.reset_index(drop=True) if otc_df is not None else None,
        "上櫃排行",
        ACCENT_OTC,
        right_x,
        card_y_top,
        card_w,
        card_h,
        top_n=top_n,
    )

    ax.text(
        0.5, 0.45, WATERMARK_TEXT,
        transform=ax.transAxes,
        ha="center", va="center",
        fontsize=WATERMARK_FONT_SIZE,
        fontweight="bold",
        fontproperties=FONT_BOLD,
        color="#2C3440",
        alpha=WATERMARK_ALPHA,
        rotation=WATERMARK_ROTATION,
        linespacing=1.18,
        zorder=4
    )


    ax.add_patch(patches.FancyBboxPatch(
        (0.020, 0.012), 0.960, 0.028,
        boxstyle="round,pad=0.004,rounding_size=0.010",
        linewidth=0.8, edgecolor="#D9E2EE", facecolor="#FFFFFF",
        transform=ax.transAxes, zorder=4
    ))
    draw_text(ax, 0.034, 0.026, STREAK_NOTE_TEXT,
              size=12.2, color=TEXT_MUTED, ha="left")
    draw_text(ax, 0.965, 0.026, clean_cell(TOPRIGHT_WATERMARK_TEXT) + "｜" + clean_cell(DISCLAIMER_TEXT),
              size=12.2, color=TEXT_MUTED, weight="bold", ha="right", va="center", bold=True)

    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=180, facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf


# ================= Discord 文字格式 =================

def format_rank_block(df, title):
    if df is None or df.empty:
        return f"{title} ❌ **無資料**\n\n"

    msg = f"{title}\n"
    msg += "```text\n"

    W_RANK = 4
    W_CODE = 6
    W_NAME = 12
    W_CAT = 10
    W_PRICE = 8
    W_WEEK = 9
    W_CHANGE = 10
    GAP = " "

    h_rank = pad_visual("排名", W_RANK)
    h_code = pad_visual("代號", W_CODE)
    h_name = pad_visual("股名", W_NAME)
    h_cat = pad_visual("類別", W_CAT)
    h_price = pad_visual("現價", W_PRICE, align="left")
    h_week = pad_visual("週漲跌", W_WEEK, align="left")
    h_chg = pad_visual("總增減%", W_CHANGE, align="left")

    msg += f"{h_rank}{GAP}{h_code}{GAP}{h_name}{GAP}{h_cat}{GAP}{h_price}{GAP}{h_week}{GAP}{h_chg}\n"
    total_width = W_RANK + W_CODE + W_NAME + W_CAT + W_PRICE + W_WEEK + W_CHANGE + (len(GAP) * 6)
    msg += "=" * total_width + "\n"

    for i, row in df.reset_index(drop=True).iterrows():
        raw_str = clean_cell(row["股票代號/名稱"])
        code, name = split_code_name(raw_str)

        name = name.replace("卅卅", "碁")
        category = clean_cell(row.get("類別", "-"))
        price = clean_cell(row.get("現價", "-"))
        week_chg = clean_cell(row.get("週漲跌", "-"))

        change_str = fmt_change(row["總增減"])
        if change_str != "-":
            try:
                change_str = f"{float(change_str):+.2f}%"
            except Exception:
                pass

        full_name = to_fullwidth(name)

        s_rank = pad_visual(f"{i+1:02d}", W_RANK)
        s_code = pad_visual(code, W_CODE)
        s_name = pad_visual(full_name, W_NAME, align="left")
        s_cat = pad_visual(category, W_CAT, align="left")
        s_price = pad_visual(price, W_PRICE, align="left")
        s_week = pad_visual(week_chg, W_WEEK, align="left")
        s_chg = pad_visual(change_str, W_CHANGE, align="left")

        msg += f"{s_rank}{GAP}{s_code}{GAP}{s_name}{GAP}{s_cat}{GAP}{s_price}{GAP}{s_week}{GAP}{s_chg}\n"

    msg += "```\n"
    return msg


def send_discord_image(listed_df, otc_df, display_date, title_text, image_filename):
    if not DISCORD_WEBHOOK_URL:
        log("⚠️ 找不到 DISCORD_WEBHOOK_URL_TEST / DISCORD_WEBHOOK_URL，略過 Discord 推播。")
        return

    image_buf = build_rank_image(
        listed_df.reset_index(drop=True) if listed_df is not None else None,
        otc_df.reset_index(drop=True) if otc_df is not None else None,
        display_date,
        main_title=title_text,
    )

    files = {"file": (image_filename, image_buf, "image/png")}
    data = {
        "content": "📊 **" + title_text.replace("  Top 20", "") + "**\n> 📅 **資料統計日期：" + str(display_date) + "**"
    }

    response = requests.post(DISCORD_WEBHOOK_URL, data=data, files=files, timeout=30)
    if response.status_code in (200, 204):
        log(f"✅ Discord 圖片推播完成：{image_filename}")
        return

    log(f"❌ Discord 圖片推播失敗: {response.status_code}，改用文字推播")
    content = "📊 **" + title_text.replace("  Top 20", "") + "**\n"
    content += "> 📅 **資料統計日期：" + str(display_date) + "**\n\n"
    content += format_rank_block(listed_df.reset_index(drop=True), "🟦 **【上市排行】**")
    content += format_rank_block(otc_df.reset_index(drop=True), "🟩 **【上櫃排行】**")
    fallback = requests.post(DISCORD_WEBHOOK_URL, json={"content": content}, timeout=30)
    if fallback.status_code in (200, 204):
        log("✅ Discord 文字備援推播完成！")
    else:
        log(f"❌ Discord 文字備援推播失敗: {fallback.status_code}")


# ================= 主流程 =================

def push_rank_to_dc():
    start = time.time()
    log("=" * 100)
    log("啟動：每週大股東籌碼強勢榜 Top20｜PSCNet / MoneyDJ 正式部署版")
    log("=" * 100)

    sh = connect_google_sheet()

    history_ws = get_or_create_ws(sh, HOLDER_HISTORY_SHEET_NAME, HOLDER_HISTORY_HEADERS, rows=3000)
    listed_ratio_ws = get_or_create_ws(sh, LISTED_RATIO_SHEET_NAME, [], rows=2500, cols=80)
    otc_ratio_ws = get_or_create_ws(sh, OTC_RATIO_SHEET_NAME, [], rows=2500, cols=80)
    api_ws = get_or_create_ws(sh, API_CACHE_SHEET_NAME, API_CACHE_HEADERS, rows=2500, cols=4)

    # 先讀 API 快取，再抓股票清單。
    # 若 ISIN 網站在 GitHub Actions 偶發 DNS 失敗，可用 Google Sheet 既有資料 fallback。
    cache = load_api_cache_from_sheet(api_ws)
    stock_df = fetch_all_stock_list(listed_ratio_ws, otc_ratio_ws, cache)

    cache, cache_errors = ensure_api_cache_threaded(stock_df, cache, api_ws)

    history_long, pscnet_errors = fetch_pscnet_history_all(stock_df, cache)

    listed_hist = build_ratio_history_from_long(history_long, "上市")
    otc_hist = build_ratio_history_from_long(history_long, "上櫃")
    display_date = get_latest_data_date_from_hist([listed_hist, otc_hist])

    log("寫入上市 / 上櫃 400張比例歷史到 Google Sheet...")
    write_ratio_history_sheet(listed_ratio_ws, listed_hist)
    write_ratio_history_sheet(otc_ratio_ws, otc_hist)

    # 增加榜
    listed_df = build_top_from_history(listed_hist, "上市")
    otc_df = build_top_from_history(otc_hist, "上櫃")

    # 減少榜
    listed_dec_df = build_bottom_from_history(listed_hist, "上市")
    otc_dec_df = build_bottom_from_history(otc_hist, "上櫃")

    # 股價資訊
    listed_df = add_price_info(listed_df)
    otc_df = add_price_info(otc_df)
    listed_dec_df = add_price_info(listed_dec_df)
    otc_dec_df = add_price_info(otc_dec_df)

    # 若歷史不足，先用 PSCNet ratio history 回補最近幾週前20。
    records = history_ws.get_all_records()
    if not records:
        backfill_holder_history_from_ratio(history_ws, listed_hist, otc_hist, HISTORY_INITIAL_WEEKS)

    # 寫入本週歷史：增加榜 / 減少榜
    append_current_rank_history(history_ws, listed_df, otc_df, display_date, "增加")
    append_current_rank_history(history_ws, listed_dec_df, otc_dec_df, display_date, "減少")

    streak_map = compute_streak_map(history_ws)
    streak_map = maybe_extend_history_for_long_streak(history_ws, streak_map, listed_hist, otc_hist)

    listed_df = apply_streak_labels(listed_df, "上市", "增加", streak_map)
    otc_df = apply_streak_labels(otc_df, "上櫃", "增加", streak_map)
    listed_dec_df = apply_streak_labels(listed_dec_df, "上市", "減少", streak_map)
    otc_dec_df = apply_streak_labels(otc_dec_df, "上櫃", "減少", streak_map)

    send_discord_image(
        listed_df, otc_df, display_date,
        "每週大股東籌碼強勢榜  Top 20",
        "weekly_holder_rank_increase.png"
    )
    send_discord_image(
        listed_dec_df, otc_dec_df, display_date,
        "每週大股東籌碼減少榜  Top 20",
        "weekly_holder_rank_decrease.png"
    )

    elapsed = time.time() - start
    log("=" * 100)
    log("完成：每週大股東籌碼強勢榜 / 減少榜 Top20")
    log(f"資料日期：{display_date}")
    log(f"API快取錯誤數：{len(cache_errors)}")
    log(f"PSCNet requests錯誤數：{len(pscnet_errors)}")
    log(f"耗時：{elapsed:.2f} 秒")
    log("=" * 100)


if __name__ == "__main__":
    push_rank_to_dc()
