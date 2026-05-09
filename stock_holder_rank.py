import requests
import pandas as pd
import yfinance as yf
from io import StringIO, BytesIO
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import re
import time
import os
try:
    import gspread
except Exception:
    gspread = None
from datetime import datetime, timedelta
from wcwidth import wcwidth
import unicodedata

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib import font_manager

# ================= 設定區 =================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_TEST")
SHEET_NAME = "台股注意股資料庫_V33"
SERVICE_KEY_FILE = "service_key.json"
HOLDER_HISTORY_SHEET_NAME = "每週大戶排行紀錄"
HISTORY_INITIAL_WEEKS = 5
HISTORY_EXTEND_WEEKS = 12


# ================= 圖片樣式設定 =================
WATERMARK_TEXT = "股市艾斯\n台股DC討論群"
TOPRIGHT_WATERMARK_TEXT = "By 股市艾斯出品-轉傳請註明"
DISCLAIMER_TEXT = "資訊分享非投資建議 投資請自行評估風險"
WATERMARK_ALPHA = 0.12
WATERMARK_FONT_SIZE = 104
WATERMARK_ROTATION = 18
TOPRIGHT_WATERMARK_ALPHA = 0.80
TOPRIGHT_WATERMARK_FONT_SIZE = 10
TOPRIGHT_DISCLAIMER_FONT_SIZE = 9
STREAK_NOTE_TEXT = "標記：連2／連3／連4 代表連續 2／3／4 週進入該榜單"

IMG_BG = "#F5F7FA"
CARD_BG = "#FFFFFF"
CARD_BORDER = "#DDE5EF"
HEADER_BG = "#F1F5F9"
TEXT_MAIN = "#243044"
TEXT_MUTED = "#718096"
TEXT_RED = "#E53E3E"
TEXT_GREEN = "#16A34A"
ACCENT_LISTED = "#3182CE"
ACCENT_OTC = "#22A06B"
TOP1_BG = "#FFF4D9"
TOP2_BG = "#EEF4FF"
TOP3_BG = "#FDF0E6"
TOP1_BORDER = "#F2C56B"
TOP2_BORDER = "#BFD0F3"
TOP3_BORDER = "#E6B88A"
TOP1_BADGE = "#F4C95D"
TOP2_BADGE = "#C9D2E3"
TOP3_BADGE = "#E6BA8A"

CJK_FONT_PATH = None
CJK_BOLD_FONT_PATH = None


def load_cjk_font(bold=False):
    """載入中文字型，讓白底圖片可正常顯示中文。"""
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


FONT_PROP = load_cjk_font(False)
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


def parse_latest_trade_date(raw_date):
    """將 Norway 表頭日期轉成 datetime，用於計算該週週一開盤到週五收盤。"""
    s = "" if raw_date is None else str(raw_date).strip()
    digits = re.sub(r"\D", "", s)
    year_now = datetime.now().year

    try:
        if len(digits) == 4:
            return datetime(year_now, int(digits[:2]), int(digits[2:]))
        if len(digits) == 8:
            return datetime(int(digits[:4]), int(digits[4:6]), int(digits[6:]))
    except:
        pass

    return datetime.now()


def get_week_price_info(code, market_suffix, latest_date_str):
    """
    計算股價與週漲跌 (標準跨週算法)：
    抓取這週的最後收盤價，以及「上週」的最後收盤價來計算漲跌幅，
    這樣計算出的數值才會與一般看盤軟體的周K漲跌幅完全一致。
    """
    try:
        ref_date = parse_latest_trade_date(latest_date_str)
        # 本週一的日期
        week_start = ref_date - timedelta(days=ref_date.weekday())
        
        # 往前多抓一點時間 (抓15天)，確保一定能抓到上週與本週的日K資料
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
        
        # 移除 index 的時區資訊，方便與 week_start (datetime) 做比對
        df.index = df.index.tz_localize(None)

        # 1. 篩選出「本週一之前」的所有交易日，取最後一筆作為「上週收盤價」
        past_df = df[df.index < week_start]
        # 2. 篩選出「本週一(含)之後」的所有交易日，取最後一筆作為「本週最新收盤價」
        current_week_df = df[df.index >= week_start]

        if past_df.empty or current_week_df.empty:
            return "-", "-"

        prev_close = float(past_df["Close"].iloc[-1])
        current_close = float(current_week_df["Close"].iloc[-1])

        if prev_close <= 0:
            return f"{current_close:.1f}", "-"

        # 用上週收盤價來計算標準週漲跌幅
        week_pct = ((current_close - prev_close) / prev_close) * 100
        arrow = "▲" if week_pct > 0 else "▼" if week_pct < 0 else "—"
        
        return f"{current_close:.1f}", f"{arrow}{abs(week_pct):.1f}%"
        
    except Exception as e:
        print(f"⚠️ 股價資料取得失敗 ({code}{market_suffix}): {e}")
        return "-", "-"


def get_norway_rank_logic(url):
    """
    依照APP邏輯爬取，並加入「依最新週漲幅排序」功能
    修正: 使用 iloc 避免 FutureWarning 及索引錯誤
    """
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    try:
        driver.get(url)
        
        # 1. 依照原程式碼邏輯：等待特定 XPath 出現
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//table[contains(., '大股東持有張數增減')]"))
        )
        
        html = driver.page_source
        dfs = pd.read_html(StringIO(html), header=None)
        
        target_df = None
        # 2. 依照原程式碼邏輯：尋找包含關鍵字的表格
        for df in dfs:
            if len(df.columns) > 10 and len(df) > 20:
                if df.astype(str).apply(lambda x: x.str.contains('大股東持有').any()).any():
                    target_df = df
                    break
        
        if target_df is None and len(dfs) > 0:
             target_df = max(dfs, key=len)

        if target_df is None:
            return None, None

        # 3. 依照原程式碼邏輯：定位 Header 與 Data Start Index
        header_idx = -1
        data_start_idx = -1
        
        for idx, row in target_df.iterrows():
            # 找股票代號 (4碼數字)
            if re.search(r'\d{4}', str(row.iloc[3])):
                data_start_idx = idx
                break
        
        if data_start_idx == -1: 
            return None, None
        
        # 往回找日期 Header
        for idx in range(max(0, data_start_idx - 5), data_start_idx):
            row = target_df.iloc[idx]
            if re.match(r'^\d{4,}$', str(row.iloc[5])): # 判斷日期格式
                header_idx = idx
                break
        
        # 4. 抓取所有資料並依照「最新週」排序
        
        # 4.1 找出「最新日期」對應的欄位索引
        max_col_index = target_df.shape[1] - 1
        start_search = min(10, max_col_index)
        
        latest_date_col_idx = 5 # 預設值
        latest_date_str = "未知日期"
        
        if header_idx != -1:
            # 倒序檢查，確保抓到最右邊(最新)的日期
            for col_i in range(start_search, 4, -1): 
                try:
                    val = str(target_df.iloc[header_idx, col_i]).strip()
                    if re.search(r'\d+', val):
                        latest_date_col_idx = col_i
                        latest_date_str = val
                        break
                except:
                    continue
        
        # 4.2 抓取所有資料列
        raw_data = target_df.iloc[data_start_idx:].copy()
        
        # 4.3 定義排序用的數值轉換函數
        def parse_pct(x):
            try:
                # 移除 % 和逗號，轉為 float
                return float(str(x).replace('%', '').replace(',', ''))
            except:
                return -999999.0 # 無法解析的排到最後
        
        # 4.4 建立排序依據欄位
        raw_data['_sort_val'] = raw_data.iloc[:, latest_date_col_idx].apply(parse_pct)
        
        # 4.5 依照最新週漲幅由大到小排序，並取出前 20 名
        top20_data = raw_data.sort_values(by='_sort_val', ascending=False).head(20)
        
        # 4.6 構建回傳 DataFrame
        result_df = pd.DataFrame()
        result_df['股票代號/名稱'] = top20_data.iloc[:, 3]

        # 類別欄位參考網頁表格第 5 欄，也就是 XPath 的 td[5]/a
        if target_df.shape[1] > 4:
            result_df['類別'] = top20_data.iloc[:, 4]
        else:
            result_df['類別'] = "-"

        market_suffix = ".TWO" if "CID=100" in url else ".TW"
        price_list, week_chg_list = [], []
        for raw_name in result_df['股票代號/名稱']:
            match = re.match(r'(\d{4})', clean_cell(raw_name))
            code = match.group(1) if match else ""
            price, week_chg = get_week_price_info(code, market_suffix, latest_date_str)
            price_list.append(price)
            week_chg_list.append(week_chg)

        result_df['現價'] = price_list
        result_df['週漲跌'] = week_chg_list
        result_df['總增減'] = top20_data.iloc[:, latest_date_col_idx] 

        return result_df, latest_date_str

    except Exception as e:
        print(f"爬取錯誤: {e}")
        return None, None
    finally:
        driver.quit()


def get_norway_decrease_rank_logic(url):
    """
    依照APP邏輯爬取，並加入「依最新週大戶持股減少排序」功能
    修正: 使用 iloc 避免 FutureWarning 及索引錯誤
    """
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    try:
        driver.get(url)
        
        # 1. 依照原程式碼邏輯：等待特定 XPath 出現
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//table[contains(., '大股東持有張數增減')]"))
        )
        
        html = driver.page_source
        dfs = pd.read_html(StringIO(html), header=None)
        
        target_df = None
        # 2. 依照原程式碼邏輯：尋找包含關鍵字的表格
        for df in dfs:
            if len(df.columns) > 10 and len(df) > 20:
                if df.astype(str).apply(lambda x: x.str.contains('大股東持有').any()).any():
                    target_df = df
                    break
        
        if target_df is None and len(dfs) > 0:
             target_df = max(dfs, key=len)

        if target_df is None:
            return None, None

        # 3. 依照原程式碼邏輯：定位 Header 與 Data Start Index
        header_idx = -1
        data_start_idx = -1
        
        for idx, row in target_df.iterrows():
            # 找股票代號 (4碼數字)
            if re.search(r'\d{4}', str(row.iloc[3])):
                data_start_idx = idx
                break
        
        if data_start_idx == -1: 
            return None, None
        
        # 往回找日期 Header
        for idx in range(max(0, data_start_idx - 5), data_start_idx):
            row = target_df.iloc[idx]
            if re.match(r'^\d{4,}$', str(row.iloc[5])): # 判斷日期格式
                header_idx = idx
                break
        
        # 4. 抓取所有資料並依照「最新週大戶持股減少」排序
        
        # 4.1 找出「最新日期」對應的欄位索引
        max_col_index = target_df.shape[1] - 1
        start_search = min(10, max_col_index)
        
        latest_date_col_idx = 5 # 預設值
        latest_date_str = "未知日期"
        
        if header_idx != -1:
            # 倒序檢查，確保抓到最右邊(最新)的日期
            for col_i in range(start_search, 4, -1): 
                try:
                    val = str(target_df.iloc[header_idx, col_i]).strip()
                    if re.search(r'\d+', val):
                        latest_date_col_idx = col_i
                        latest_date_str = val
                        break
                except:
                    continue
        
        # 4.2 抓取所有資料列
        raw_data = target_df.iloc[data_start_idx:].copy()
        
        # 4.3 定義排序用的數值轉換函數
        def parse_pct(x):
            try:
                # 移除 % 和逗號，轉為 float
                return float(str(x).replace('%', '').replace(',', ''))
            except:
                return 999999.0 # 無法解析的排到最後
        
        # 4.4 建立排序依據欄位
        raw_data['_sort_val'] = raw_data.iloc[:, latest_date_col_idx].apply(parse_pct)
        
        # 4.5 依照最新週總增減由小到大排序，並取出前 15 名
        top20_data = raw_data.sort_values(by='_sort_val', ascending=True).head(15)
        
        # 4.6 構建回傳 DataFrame
        result_df = pd.DataFrame()
        result_df['股票代號/名稱'] = top20_data.iloc[:, 3]

        # 類別欄位參考網頁表格第 5 欄，也就是 XPath 的 td[5]/a
        if target_df.shape[1] > 4:
            result_df['類別'] = top20_data.iloc[:, 4]
        else:
            result_df['類別'] = "-"

        market_suffix = ".TWO" if "CID=100" in url else ".TW"
        price_list, week_chg_list = [], []
        for raw_name in result_df['股票代號/名稱']:
            match = re.match(r'(\d{4})', clean_cell(raw_name))
            code = match.group(1) if match else ""
            price, week_chg = get_week_price_info(code, market_suffix, latest_date_str)
            price_list.append(price)
            week_chg_list.append(week_chg)

        result_df['現價'] = price_list
        result_df['週漲跌'] = week_chg_list
        result_df['總增減'] = top20_data.iloc[:, latest_date_col_idx] 

        return result_df, latest_date_str

    except Exception as e:
        print(f"爬取錯誤: {e}")
        return None, None
    finally:
        driver.quit()


# ================= 大戶排行歷史紀錄與連續上榜工具 =================
HOLDER_HISTORY_HEADERS = [
    "資料日期", "榜單類型", "市場", "排名", "代號", "名稱", "類別",
    "現價", "週漲跌", "總增減%", "寫入時間"
]


def connect_holder_history_sheet():
    print(f"準備連線 Google Sheet：{SHEET_NAME}")
    if gspread is None:
        raise RuntimeError("gspread 未安裝，無法建立或寫入『每週大戶排行紀錄』工作表。")
    if not os.path.exists(SERVICE_KEY_FILE):
        raise FileNotFoundError(f"找不到 {SERVICE_KEY_FILE}，無法建立或寫入『每週大戶排行紀錄』工作表。")
    try:
        gc = gspread.service_account(filename=SERVICE_KEY_FILE)
        sh = gc.open(SHEET_NAME)
        print(f"Google Sheet 連線成功：{SHEET_NAME}")
        return sh
    except Exception as e:
        raise RuntimeError(f"連線 Google Sheet 失敗，無法建立或寫入『每週大戶排行紀錄』工作表：{e}")


def get_or_create_holder_history_ws(sh):
    if sh is None:
        raise RuntimeError("Google Sheet 連線物件為空，無法建立或讀取『每週大戶排行紀錄』工作表。")
    created = False
    try:
        ws = sh.worksheet(HOLDER_HISTORY_SHEET_NAME)
        print(f"已找到工作表：{HOLDER_HISTORY_SHEET_NAME}")
    except Exception:
        ws = sh.add_worksheet(title=HOLDER_HISTORY_SHEET_NAME, rows=2000, cols=len(HOLDER_HISTORY_HEADERS))
        created = True
        print(f"已建立新工作表：{HOLDER_HISTORY_SHEET_NAME}")

    try:
        values = ws.get_all_values()
        if not values:
            ws.update("A1", [HOLDER_HISTORY_HEADERS])
            print(f"已初始化『{HOLDER_HISTORY_SHEET_NAME}』欄位。")
        elif values[0] != HOLDER_HISTORY_HEADERS:
            ws.update("A1", [HOLDER_HISTORY_HEADERS])
            print(f"已修正『{HOLDER_HISTORY_SHEET_NAME}』欄位標題。")
    except Exception as e:
        raise RuntimeError(f"初始化『{HOLDER_HISTORY_SHEET_NAME}』欄位失敗：{e}")

    return ws, created


def normalize_history_date(raw_date):
    try:
        return parse_latest_trade_date(raw_date).strftime("%Y-%m-%d")
    except Exception:
        return str(raw_date).strip() if raw_date else "未知日期"


def parse_history_pct(x, invalid_value=None):
    try:
        s = str(x).replace('%', '').replace(',', '')
        s = re.sub(r'\s+', '', s)
        return float(s)
    except Exception:
        return invalid_value


def _fetch_norway_table_for_history(url):
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    try:
        driver.get(url)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//table[contains(., '大股東持有張數增減')]"))
        )
        html = driver.page_source
        dfs = pd.read_html(StringIO(html), header=None)

        target_df = None
        for df in dfs:
            if len(df.columns) > 10 and len(df) > 20:
                if df.astype(str).apply(lambda x: x.str.contains('大股東持有').any()).any():
                    target_df = df
                    break
        if target_df is None and len(dfs) > 0:
            target_df = max(dfs, key=len)
        if target_df is None:
            return None, -1, -1, []

        header_idx = -1
        data_start_idx = -1
        for idx, row in target_df.iterrows():
            if re.search(r'\d{4}', str(row.iloc[3])):
                data_start_idx = idx
                break
        if data_start_idx == -1:
            return None, -1, -1, []

        for idx in range(max(0, data_start_idx - 5), data_start_idx):
            row = target_df.iloc[idx]
            if re.match(r'^\d{4,}$', str(row.iloc[5])):
                header_idx = idx
                break
        if header_idx == -1:
            return None, -1, -1, []

        date_cols = []
        for col_i in range(target_df.shape[1] - 1, 4, -1):
            try:
                val = str(target_df.iloc[header_idx, col_i]).strip()
                if re.search(r'\d+', val):
                    date_cols.append((col_i, val))
            except Exception:
                continue

        return target_df, header_idx, data_start_idx, date_cols
    except Exception as e:
        print(f"⚠️ 歷史排行資料抓取失敗：{e}")
        return None, -1, -1, []
    finally:
        driver.quit()


def get_norway_history_rank_rows(url, market, rank_type, top_n, weeks):
    target_df, header_idx, data_start_idx, date_cols = _fetch_norway_table_for_history(url)
    if target_df is None or data_start_idx == -1 or not date_cols:
        return []

    raw_data_base = target_df.iloc[data_start_idx:].copy()
    rows = []
    write_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ascending = True if rank_type == "減少" else False
    invalid_value = 999999.0 if ascending else -999999.0

    for col_i, raw_date in date_cols[:weeks]:
        rank_date = normalize_history_date(raw_date)
        raw_data = raw_data_base.copy()
        raw_data['_sort_val'] = raw_data.iloc[:, col_i].apply(lambda x: parse_history_pct(x, invalid_value))
        top_data = raw_data.sort_values(by='_sort_val', ascending=ascending).head(top_n)

        for rank, (_, row) in enumerate(top_data.iterrows(), start=1):
            code, name = split_code_name(row.iloc[3])
            if not code:
                continue
            category = clean_cell(row.iloc[4]) if target_df.shape[1] > 4 else "-"
            total_change = fmt_change(row.iloc[col_i])
            rows.append({
                "資料日期": rank_date,
                "榜單類型": rank_type,
                "市場": market,
                "排名": rank,
                "代號": code,
                "名稱": name,
                "類別": category,
                "現價": "-",
                "週漲跌": "-",
                "總增減%": total_change,
                "寫入時間": write_time,
            })
    return rows


def rows_to_append_values(rows):
    return [[r.get(h, "") for h in HOLDER_HISTORY_HEADERS] for r in rows]


def append_history_rows(ws, rows):
    if ws is None:
        raise RuntimeError("工作表物件為空，無法寫入每週大戶排行紀錄。")
    if not rows:
        return 0
    try:
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
    except Exception as e:
        raise RuntimeError(f"寫入『{HOLDER_HISTORY_SHEET_NAME}』失敗：{e}")


def backfill_holder_history(ws, weeks):
    if ws is None:
        return
    print(f"正在建立 / 回補每週大戶排行紀錄：最近 {weeks} 週...")
    jobs = [
        ("https://norway.twsthr.info/StockHoldersTopWeek.aspx", "上市", "增加", 20),
        ("https://norway.twsthr.info/StockHoldersTopWeek.aspx?CID=100&Show=1", "上櫃", "增加", 20),
        ("https://norway.twsthr.info/StockHoldersTopWeek.aspx", "上市", "減少", 15),
        ("https://norway.twsthr.info/StockHoldersTopWeek.aspx?CID=100&Show=1", "上櫃", "減少", 15),
    ]
    all_rows = []
    for url, market, rank_type, top_n in jobs:
        print(f"  回補 {market} 大戶{rank_type}排行 Top {top_n}...")
        all_rows.extend(get_norway_history_rank_rows(url, market, rank_type, top_n, weeks))
    added = append_history_rows(ws, all_rows)
    print(f"每週大戶排行紀錄回補完成，新增 {added} 筆。")


def initialize_holder_history(ws, created=False):
    if ws is None:
        raise RuntimeError("工作表物件為空，無法初始化每週大戶排行紀錄。")
    try:
        records = ws.get_all_records()
        if created or not records:
            backfill_holder_history(ws, HISTORY_INITIAL_WEEKS)
        else:
            print(f"『{HOLDER_HISTORY_SHEET_NAME}』已有 {len(records)} 筆紀錄，略過首次 5 週回補。")
    except Exception as e:
        raise RuntimeError(f"檢查或初始化『{HOLDER_HISTORY_SHEET_NAME}』失敗：{e}")


def build_current_history_rows(df, display_date, rank_type, market):
    if df is None or df.empty:
        return []
    history_date = normalize_history_date(display_date)
    write_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for idx, row in df.reset_index(drop=True).iterrows():
        code, name = split_code_name(row.get('股票代號/名稱', ''))
        if not code:
            continue
        rows.append({
            "資料日期": history_date,
            "榜單類型": rank_type,
            "市場": market,
            "排名": idx + 1,
            "代號": code,
            "名稱": name,
            "類別": clean_cell(row.get('類別', '-')),
            "現價": clean_cell(row.get('現價', '-')),
            "週漲跌": clean_cell(row.get('週漲跌', '-')),
            "總增減%": fmt_change(row.get('總增減', '-')),
            "寫入時間": write_time,
        })
    return rows


def append_current_rank_history(ws, listed_df, otc_df, display_date, rank_type):
    if ws is None:
        return
    rows = []
    rows.extend(build_current_history_rows(listed_df, display_date, rank_type, "上市"))
    rows.extend(build_current_history_rows(otc_df, display_date, rank_type, "上櫃"))
    added = append_history_rows(ws, rows)
    print(f"每週大戶{rank_type}排行本週紀錄新增 {added} 筆。")


def compute_streak_map(ws):
    streak_map = {}
    if ws is None:
        return streak_map
    try:
        records = ws.get_all_records()
    except Exception as e:
        print(f"⚠️ 讀取每週大戶排行紀錄失敗：{e}")
        return streak_map
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


def maybe_extend_history_for_long_streak(ws, streak_map):
    if ws is None or not streak_map:
        return streak_map
    max_streak = max(streak_map.values()) if streak_map else 0
    if max_streak >= HISTORY_INITIAL_WEEKS:
        print(f"偵測到連{max_streak}上榜股票，擴充回補最近 {HISTORY_EXTEND_WEEKS} 週歷史資料...")
        backfill_holder_history(ws, HISTORY_EXTEND_WEEKS)
        return compute_streak_map(ws)
    return streak_map


def apply_streak_labels(df, market, rank_type, streak_map):
    if df is None or df.empty:
        return df
    df = df.copy()
    new_names = []
    for _, row in df.iterrows():
        code, name = split_code_name(row.get('股票代號/名稱', ''))
        streak = streak_map.get((rank_type, market, code), 1)
        if streak >= 2:
            new_names.append(f"{code} {name}  連{streak}")
        else:
            new_names.append(f"{code} {name}")
    df['股票代號/名稱'] = new_names
    return df


# ================= 排版工具區 (終極修正版) =================

_ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u202a-\u202e\ufeff]")

# 將半形英數字轉為全形 (解決 KY 對齊問題)
def to_fullwidth(s):
    res = []
    for char in str(s):
        code = ord(char)
        # ASCII 範圍 (33-126) -> 全形範圍 (65281-65374)
        if 0x21 <= code <= 0x7E:
            res.append(chr(code + 0xFEE0))
        # 空白 (32) -> 全形空白 (12288)
        elif code == 0x20:
            res.append(chr(0x3000))
        else:
            res.append(char)
    return "".join(res)

def clean_cell(s) -> str:
    s = "" if s is None else str(s)
    # [關鍵修正] 移除 NFKC 正規化，避免全形字又被轉回半形 (導致 KY 歪掉)
    # s = unicodedata.normalize("NFKC", s) 
    
    s = s.replace("\xa0", " ")               # NBSP
    s = _ZERO_WIDTH_RE.sub("", s)            # zero-width
    s = re.sub(r"\s+", " ", s).strip()       # 多空白統一
    return s

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
    vis_len = visual_len(s)
    
    diff = max(0, target_w - vis_len)
    
    full_spaces = diff // 2
    half_spaces = diff % 2
    
    padding = "\u3000" * full_spaces + " " * half_spaces
    
    if align == "right":
        return padding + s
    return s + padding

# 數值標準化格式
def fmt_change(x):
    s = str(x)
    s = s.replace('%', '').replace(',', '')
    s = re.sub(r'\s+', '', s)  # 清掉各種奇怪空白
    v = pd.to_numeric(s, errors='coerce')
    return "-" if pd.isna(v) else f"{v:.2f}"


def split_code_name(raw):
    raw_str = clean_cell(raw)
    match = re.match(r'(\d{4})\s*(.*)', raw_str)
    if match:
        code = clean_cell(match.group(1))
        name = clean_cell(match.group(2).strip())
    else:
        code = clean_cell(raw_str[:4])
        name = clean_cell(raw_str[4:].strip())
    name = name.replace("卅卅", "碁")
    return code, name


def draw_text(ax, x, y, text, size=13, color=TEXT_MAIN, weight='normal',
              ha='left', va='center', bold=False, alpha=1.0):
    ax.text(
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


def draw_rank_table(ax, df, title, accent, x_left, y_top, card_w, card_h, top_n=20):
    """白色版並列表格：上市 / 上櫃各一張卡片，每張保留 7 欄資訊。"""
    title_h = 0.062
    header_h = 0.046
    inner_pad_x = 0.014
    inner_w = card_w - inner_pad_x * 2
    row_h = (card_h - title_h - header_h - 0.024) / max(top_n, 1)

    # 外框卡片
    ax.add_patch(patches.FancyBboxPatch(
        (x_left, y_top - card_h), card_w, card_h,
        boxstyle="round,pad=0.006,rounding_size=0.012",
        linewidth=1.1, edgecolor=CARD_BORDER, facecolor=CARD_BG,
        transform=ax.transAxes, zorder=1
    ))

    # 標題條：保留原版左右分區感，但改為白底風格
    ax.add_patch(patches.Rectangle(
        (x_left, y_top - title_h), card_w, title_h,
        linewidth=0, facecolor=accent,
        transform=ax.transAxes, zorder=2
    ))
    draw_text(ax, x_left + 0.018, y_top - title_h / 2, title,
              size=16, color="#FFFFFF", weight='bold', bold=True)
    draw_text(ax, x_left + card_w - 0.018, y_top - title_h / 2, f"TOP {top_n}",
              size=12, color="#FFFFFF", weight='bold', bold=True, ha='right')

    # 欄位設定：排名｜代號｜股名｜類別｜現價｜週漲跌｜總增減%
    # 【關鍵修正】：重新分配欄寬比例，並將靠左對齊的欄位統一 padding 間距
    col_rel = [0.060, 0.080, 0.210, 0.150, 0.130, 0.150, 0.220]
    labels = ["排名", "代號", "股名", "類別", "現價", "週漲跌", "總增減%"]
    aligns = ["center", "center", "left", "left", "left", "left", "right"]
    shift_cols = {3, 4, 5}
    col_shift = 0.038

    x0 = x_left + inner_pad_x
    col_x = [x0]
    acc = 0
    for w in col_rel[:-1]:
        acc += w
        col_x.append(x0 + inner_w * acc)

    header_top = y_top - title_h
    ax.add_patch(patches.Rectangle(
        (x_left, header_top - header_h), card_w, header_h,
        linewidth=0, facecolor=HEADER_BG,
        transform=ax.transAxes, zorder=2
    ))
    ax.plot([x_left, x_left + card_w], [header_top - header_h, header_top - header_h],
            transform=ax.transAxes, color=CARD_BORDER, linewidth=0.8, zorder=3)

    for i, label in enumerate(labels):
        cell_x = col_x[i]
        cell_w = inner_w * col_rel[i]
        if aligns[i] == "center":
            tx, ha = cell_x + cell_w / 2, "center"
        elif aligns[i] == "right":
            pad = 0.012
            tx, ha = cell_x + cell_w - pad, "right"
        else:
            # 統一所有靠左對齊欄位的間距，確保整齊度
            tx, ha = cell_x + 0.010, "left"

        if i in shift_cols:
            tx += col_shift
            
        draw_text(ax, tx, header_top - header_h / 2, label, size=12,
                  color=TEXT_MUTED, weight='bold', ha=ha, bold=True)

    if df is None or df.empty:
        draw_text(ax, x_left + card_w / 2, header_top - header_h - row_h / 2,
                  "無資料", size=11, color=TEXT_MUTED, ha='center')
        return

    df = df.head(top_n).reset_index(drop=True)
    for i in range(top_n):
        y = header_top - header_h - i * row_h
        if i < len(df):
            row = df.iloc[i]
            code, name = split_code_name(row['股票代號/名稱'])
            name, streak_badge = _split_streak_badge(name)
            category = clean_cell(row.get('類別', '-'))
            price = clean_cell(row.get('現價', '-'))
            week_chg = clean_cell(row.get('週漲跌', '-'))
            change_str = fmt_change(row['總增減'])
            try:
                change_val = float(change_str)
            except:
                change_val = 0.0
        else:
            code, name, category, price, week_chg, change_val = "", "", "", "", "", 0.0
            streak_badge = ""

        if i == 0:
            bg, edge, lw = TOP1_BG, TOP1_BORDER, 1.1
        elif i == 1:
            bg, edge, lw = TOP2_BG, TOP2_BORDER, 1.0
        elif i == 2:
            bg, edge, lw = TOP3_BG, TOP3_BORDER, 1.0
        else:
            bg, edge, lw = ("#FFFFFF" if i % 2 == 0 else "#F6F8FB"), None, 0.0

        ax.add_patch(patches.Rectangle(
            (x_left, y - row_h), card_w, row_h,
            linewidth=lw, edgecolor=edge if edge else 'none', facecolor=bg,
            transform=ax.transAxes, zorder=2
        ))
        ax.plot([x_left + 0.010, x_left + card_w - 0.010], [y - row_h, y - row_h],
                transform=ax.transAxes, color="#E8EDF3", linewidth=0.55, zorder=3)

        if "▲" in week_chg:
            week_color = TEXT_RED
        elif "▼" in week_chg:
            week_color = TEXT_GREEN
        else:
            week_color = TEXT_MUTED

        chg_color = TEXT_RED if change_val > 0 else TEXT_GREEN if change_val < 0 else TEXT_MUTED
        chg_display = "-" if fmt_change(change_val) == "-" else f"{change_val:+.2f}%"

        values = [
            f"{i+1:02d}",
            code,
            name,
            _shorten_text(category, 7),
            price,
            week_chg,
            chg_display,
        ]
        name_weight = 'bold' if i < 3 else 'normal'
        colors = [TEXT_MUTED, TEXT_MAIN, TEXT_MAIN, TEXT_MUTED, TEXT_MAIN, week_color, chg_color]
        weights = ['bold', 'bold', name_weight, 'normal', 'bold', 'bold', 'bold']
        sizes = [9.2, 12, 14 if i < 3 else 12, 10, 10, 12, 12]

        # 前三名排名徽章
        rank_cell_x = col_x[0]
        rank_cell_w = inner_w * col_rel[0]
        rank_center_x = rank_cell_x + rank_cell_w / 2
        rank_center_y = y - row_h / 2
        if i < 3:
            badge_color = [TOP1_BADGE, TOP2_BADGE, TOP3_BADGE][i]
            ax.add_patch(patches.Circle(
                (rank_center_x, rank_center_y), row_h * 0.24,
                transform=ax.transAxes, facecolor=badge_color,
                edgecolor='white', linewidth=1.0, zorder=4
            ))
            draw_text(ax, rank_center_x, rank_center_y, values[0], size=10.5,
                      color="#6B4A12" if i == 0 else TEXT_MAIN, weight='bold', ha='center', bold=True)
            start_j = 1
        else:
            start_j = 0

        for j in range(start_j, len(values)):
            value = values[j]
            cell_x = col_x[j]
            cell_w = inner_w * col_rel[j]
            if aligns[j] == "center":
                tx, ha = cell_x + cell_w / 2, "center"
            elif aligns[j] == "right":
                pad = 0.012
                tx, ha = cell_x + cell_w - pad, "right"
            else:
                # 統一靠左對齊間距
                tx, ha = cell_x + 0.010, "left"

            if j in shift_cols:
                tx += col_shift

            if j == 2 and streak_badge:
                draw_text(ax, tx, y - row_h / 2, value, size=sizes[j],
                          color=colors[j], weight=weights[j], ha=ha,
                          bold=(weights[j] == 'bold'))
                badge_font_size = 9.0
                badge_w = 0.030
                badge_h = row_h * 0.56
                badge_x = col_x[3] + 0.010
                badge_y = y - row_h / 2 - badge_h / 2
                ax.add_patch(patches.FancyBboxPatch(
                    (badge_x, badge_y), badge_w, badge_h,
                    boxstyle="round,pad=0.001,rounding_size=0.003",
                    linewidth=0.9, edgecolor="#D8B83F", facecolor="#FFF3C4",
                    transform=ax.transAxes, zorder=7
                ))
                ax.text(
                    badge_x + badge_w / 2, y - row_h / 2, clean_cell(streak_badge),
                    transform=ax.transAxes,
                    ha='center', va='center',
                    fontsize=badge_font_size,
                    fontweight='bold',
                    fontproperties=FONT_BOLD,
                    color="#A06A00",
                    zorder=8
                )
                continue
                
            draw_text(ax, tx, y - row_h / 2, value, size=sizes[j],
                      color=colors[j], weight=weights[j], ha=ha,
                      bold=(weights[j] == 'bold'))


def build_rank_image(listed_df, otc_df, display_date):
    """白色風格 + 原版雙欄樣式：上市、上櫃並排，各 20 名。"""
    top_n = 20
    fig_w = 18.0
    fig_h = 10.6

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), facecolor=IMG_BG)
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.set_position([0, 0, 1, 1])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_axis_off()

    # 標題區
    ax.add_patch(patches.Rectangle(
        (0.015, 0.905), 0.970, 0.072,
        linewidth=0, facecolor="#FFFFFF",
        transform=ax.transAxes, zorder=1
    ))
    draw_text(ax, 0.5, 0.945, "每週大股東籌碼強勢榜  Top 20",
              size=22, color=TEXT_MAIN, weight='bold', ha='center', bold=True)
    draw_text(ax, 0.5, 0.915, f"資料統計日期：{display_date}",
              size=11, color=TEXT_MUTED, ha='center')

    # 雙欄卡片
    card_y_top = 0.852
    card_h = 0.825
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

    # 中央大文字浮水印：置中、超大、半透明，但仍不影響表格文字辨識
    ax.text(
        0.5, 0.50, WATERMARK_TEXT,
        transform=ax.transAxes,
        ha='center', va='center',
        fontsize=WATERMARK_FONT_SIZE,
        fontweight='bold',
        fontproperties=FONT_BOLD,
        color="#2C3440",
        alpha=WATERMARK_ALPHA,
        rotation=WATERMARK_ROTATION,
        linespacing=1.18,
        zorder=4
    )

    fig.text(0.985, 0.988, clean_cell(TOPRIGHT_WATERMARK_TEXT),
             ha='right', va='top',
             fontsize=TOPRIGHT_WATERMARK_FONT_SIZE,
             fontproperties=FONT_PROP,
             color="#2C3440",
             alpha=TOPRIGHT_WATERMARK_ALPHA,
             zorder=10)

    fig.text(0.985, 0.968, clean_cell(DISCLAIMER_TEXT),
             ha='right', va='top',
             fontsize=TOPRIGHT_DISCLAIMER_FONT_SIZE,
             fontproperties=FONT_PROP,
             color="#2C3440",
             alpha=TOPRIGHT_WATERMARK_ALPHA,
             zorder=10)

    fig.text(0.020, 0.018, clean_cell(STREAK_NOTE_TEXT),
             ha='left', va='bottom',
             fontsize=10,
             fontproperties=FONT_PROP,
             color=TEXT_MUTED,
             alpha=0.92,
             zorder=10)

    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=150, facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf


def build_decrease_rank_image(listed_df, otc_df, display_date):
    """白色風格 + 原版雙欄樣式：上市、上櫃並排，各 15 名。"""
    top_n = 15
    fig_w = 18.0
    fig_h = 10.6

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), facecolor=IMG_BG)
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
    ax.set_position([0, 0, 1, 1])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_axis_off()

    # 標題區
    ax.add_patch(patches.Rectangle(
        (0.015, 0.905), 0.970, 0.072,
        linewidth=0, facecolor="#FFFFFF",
        transform=ax.transAxes, zorder=1
    ))
    draw_text(ax, 0.5, 0.945, "本週大戶持股減少觀察名單  Top 15",
              size=22, color=TEXT_MAIN, weight='bold', ha='center', bold=True)
    draw_text(ax, 0.5, 0.915, f"資料統計日期：{display_date}",
              size=11, color=TEXT_MUTED, ha='center')

    # 雙欄卡片
    card_y_top = 0.852
    card_h = 0.825
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

    # 中央大文字浮水印：置中、超大、半透明，但仍不影響表格文字辨識
    ax.text(
        0.5, 0.50, WATERMARK_TEXT,
        transform=ax.transAxes,
        ha='center', va='center',
        fontsize=WATERMARK_FONT_SIZE,
        fontweight='bold',
        fontproperties=FONT_BOLD,
        color="#2C3440",
        alpha=WATERMARK_ALPHA,
        rotation=WATERMARK_ROTATION,
        linespacing=1.18,
        zorder=4
    )

    fig.text(0.985, 0.988, clean_cell(TOPRIGHT_WATERMARK_TEXT),
             ha='right', va='top',
             fontsize=TOPRIGHT_WATERMARK_FONT_SIZE,
             fontproperties=FONT_PROP,
             color="#2C3440",
             alpha=TOPRIGHT_WATERMARK_ALPHA,
             zorder=10)

    fig.text(0.985, 0.968, clean_cell(DISCLAIMER_TEXT),
             ha='right', va='top',
             fontsize=TOPRIGHT_DISCLAIMER_FONT_SIZE,
             fontproperties=FONT_PROP,
             color="#2C3440",
             alpha=TOPRIGHT_WATERMARK_ALPHA,
             zorder=10)

    fig.text(0.020, 0.018, clean_cell(STREAK_NOTE_TEXT),
             ha='left', va='bottom',
             fontsize=10,
             fontproperties=FONT_PROP,
             color=TEXT_MUTED,
             alpha=0.92,
             zorder=10)

    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=150, facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf


def push_rank_to_dc():
    print("啟動 stock_holder_rank.py：每週大戶增加 / 減少排行與歷史紀錄")
    can_send_discord = bool(DISCORD_WEBHOOK_URL)
    if not can_send_discord:
        print("⚠️ 找不到 DISCORD_WEBHOOK_URL_TEST，將只建立 / 更新 Google Sheet 歷史紀錄，不進行 Discord 推播。")

    print("正在處理上市排行...")
    listed_df, listed_date = get_norway_rank_logic("https://norway.twsthr.info/StockHoldersTopWeek.aspx")
    
    print("正在處理上櫃排行...")
    otc_df, otc_date = get_norway_rank_logic("https://norway.twsthr.info/StockHoldersTopWeek.aspx?CID=100&Show=1")

    if listed_df is None and otc_df is None:
        print("抓取失敗，無資料")
        return

    # 顯示日期優先順序
    raw_date = listed_date if listed_date != "未知日期" else otc_date
    
    # 日期格式化
    display_date = raw_date
    if raw_date and raw_date.isdigit():
        if len(raw_date) == 4:
            display_date = f"2026-{raw_date[:2]}-{raw_date[2:]}"
        elif len(raw_date) == 8:
            display_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"

    holder_history_sh = connect_holder_history_sheet()
    holder_history_ws, holder_history_created = get_or_create_holder_history_ws(holder_history_sh)
    initialize_holder_history(holder_history_ws, created=holder_history_created)
    append_current_rank_history(holder_history_ws, listed_df, otc_df, display_date, "增加")
    streak_map = compute_streak_map(holder_history_ws)
    streak_map = maybe_extend_history_for_long_streak(holder_history_ws, streak_map)
    listed_df = apply_streak_labels(listed_df, "上市", "增加", streak_map)
    otc_df = apply_streak_labels(otc_df, "上櫃", "增加", streak_map)

    content = "📊 **每週大股東籌碼強勢榜 Top 20**\n"
    content += f"> 📅 **資料統計日期：{display_date}**\n\n"

    def format_rank_block(df, title):
        if df is None or df.empty:
            return f"{title} ❌ **無資料**\n\n"
        
        msg = f"{title}\n"
        msg += "```text\n"
        
        # 定義視覺寬度
        W_RANK   = 4
        W_CODE   = 6
        W_NAME   = 12
        W_CAT    = 10
        W_PRICE  = 8
        W_WEEK   = 9
        W_CHANGE = 10

        # 定義 Gap (單一半形空白，拉近距離)
        GAP = " "

        # 標題列
        h_rank = pad_visual("排名", W_RANK)
        h_code = pad_visual("代號", W_CODE)
        h_name = pad_visual("股名", W_NAME)
        h_cat  = pad_visual("類別", W_CAT)
        h_price = pad_visual("現價", W_PRICE, align='left')
        h_week = pad_visual("週漲跌", W_WEEK, align='left')
        h_chg  = pad_visual("總增減%", W_CHANGE, align='left')

        msg += f"{h_rank}{GAP}{h_code}{GAP}{h_name}{GAP}{h_cat}{GAP}{h_price}{GAP}{h_week}{GAP}{h_chg}\n"

        total_width = W_RANK + W_CODE + W_NAME + W_CAT + W_PRICE + W_WEEK + W_CHANGE + (len(GAP) * 6)
        msg += "=" * total_width + "\n"

        for i, row in df.iterrows():
            # 清洗
            raw_str = clean_cell(row['股票代號/名稱'])
            
            match = re.match(r'(\d{4})\s*(.*)', raw_str)
            if match:
                code = match.group(1)
                name = match.group(2).strip()
            else:
                code = raw_str[:4]
                name = raw_str[4:].strip()
            
            code = clean_cell(code)
            name = clean_cell(name)
            
            # [新增] 修正亂碼：將 "卅卅" 替換為 "碁" (要在轉全形之前做)
            name = name.replace("卅卅", "碁")
            
            category = clean_cell(row.get('類別', '-'))
            price = clean_cell(row.get('現價', '-'))
            week_chg = clean_cell(row.get('週漲跌', '-'))
            change_str = fmt_change(row['總增減'])
            if change_str != "-":
                try:
                    change_str = f"{float(change_str):+.2f}%"
                except:
                    pass

            # 轉為全形字元 (解決 KY 混排問題)
            full_name = to_fullwidth(name)

            # 截斷與填充
            s_name = pad_visual(full_name, W_NAME, align='left')

            # 其他欄位
            s_rank = pad_visual(f"{i+1:02d}", W_RANK) 
            s_code = pad_visual(code, W_CODE)
            s_cat = pad_visual(category, W_CAT, align='left')
            s_price = pad_visual(price, W_PRICE, align='left')
            s_week = pad_visual(week_chg, W_WEEK, align='left')
            s_chg  = pad_visual(change_str, W_CHANGE, align='left')

            msg += f"{s_rank}{GAP}{s_code}{GAP}{s_name}{GAP}{s_cat}{GAP}{s_price}{GAP}{s_week}{GAP}{s_chg}\n"
            
        msg += "```\n"
        return msg

    content += format_rank_block(listed_df.reset_index(drop=True), "🟦 **【上市排行】**")
    content += format_rank_block(otc_df.reset_index(drop=True), "🟩 **【上櫃排行】**")

    # 發送
    if can_send_discord:
        try:
            image_buf = build_rank_image(
                listed_df.reset_index(drop=True) if listed_df is not None else None,
                otc_df.reset_index(drop=True) if otc_df is not None else None,
                display_date
            )
            files = {"file": ("weekly_holder_rank.png", image_buf, "image/png")}
            data = {
                "content": f"📊 **每週大股東籌碼強勢榜 Top 20**\n> 📅 **資料統計日期：{display_date}**"
            }
            response = requests.post(DISCORD_WEBHOOK_URL, data=data, files=files)
            if response.status_code in (200, 204):
                print("✅ 推播完成！")
            else:
                print(f"❌ 圖片推播失敗: {response.status_code}，改用文字推播")
                fallback = requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
                if fallback.status_code == 204:
                    print("✅ 文字備援推播完成！")
                else:
                    print(f"❌ 文字備援推播失敗: {fallback.status_code}")
        except Exception as e:
            print(f"❌ 圖片發送錯誤: {e}，改用文字推播")
            try:
                response = requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
                if response.status_code == 204:
                    print("✅ 文字備援推播完成！")
                else:
                    print(f"❌ 文字備援推播失敗: {response.status_code}")
            except Exception as inner_e:
                print(f"❌ 發送錯誤: {inner_e}")
    else:
        print("已略過每週大股東籌碼強勢榜 Discord 推播。")

    print("正在處理上市大戶減少排行...")
    decrease_listed_df, decrease_listed_date = get_norway_decrease_rank_logic("https://norway.twsthr.info/StockHoldersTopWeek.aspx")
    
    print("正在處理上櫃大戶減少排行...")
    decrease_otc_df, decrease_otc_date = get_norway_decrease_rank_logic("https://norway.twsthr.info/StockHoldersTopWeek.aspx?CID=100&Show=1")

    if decrease_listed_df is None and decrease_otc_df is None:
        print("大戶減少觀察名單抓取失敗，無資料")
        return

    # 顯示日期優先順序
    decrease_raw_date = decrease_listed_date if decrease_listed_date != "未知日期" else decrease_otc_date
    
    # 日期格式化
    decrease_display_date = decrease_raw_date
    if decrease_raw_date and decrease_raw_date.isdigit():
        if len(decrease_raw_date) == 4:
            decrease_display_date = f"2026-{decrease_raw_date[:2]}-{decrease_raw_date[2:]}"
        elif len(decrease_raw_date) == 8:
            decrease_display_date = f"{decrease_raw_date[:4]}-{decrease_raw_date[4:6]}-{decrease_raw_date[6:]}"

    if holder_history_ws is not None:
        append_current_rank_history(holder_history_ws, decrease_listed_df, decrease_otc_df, decrease_display_date, "減少")
        streak_map = compute_streak_map(holder_history_ws)
        streak_map = maybe_extend_history_for_long_streak(holder_history_ws, streak_map)
        decrease_listed_df = apply_streak_labels(decrease_listed_df, "上市", "減少", streak_map)
        decrease_otc_df = apply_streak_labels(decrease_otc_df, "上櫃", "減少", streak_map)

    decrease_content = "📉 **本週大戶持股減少觀察名單 Top 15**\n"
    decrease_content += f"> 📅 **資料統計日期：{decrease_display_date}**\n\n"

    decrease_content += format_rank_block(decrease_listed_df.reset_index(drop=True), "🟦 **【上市排行】**")
    decrease_content += format_rank_block(decrease_otc_df.reset_index(drop=True), "🟩 **【上櫃排行】**")

    # 發送
    if can_send_discord:
        try:
            decrease_image_buf = build_decrease_rank_image(
                decrease_listed_df.reset_index(drop=True) if decrease_listed_df is not None else None,
                decrease_otc_df.reset_index(drop=True) if decrease_otc_df is not None else None,
                decrease_display_date
            )
            decrease_files = {"file": ("weekly_holder_decrease_watchlist.png", decrease_image_buf, "image/png")}
            decrease_data = {
                "content": f"📉 **本週大戶持股減少觀察名單 Top 15**\n> 📅 **資料統計日期：{decrease_display_date}**"
            }
            decrease_response = requests.post(DISCORD_WEBHOOK_URL, data=decrease_data, files=decrease_files)
            if decrease_response.status_code in (200, 204):
                print("✅ 大戶減少觀察名單推播完成！")
            else:
                print(f"❌ 大戶減少觀察名單圖片推播失敗: {decrease_response.status_code}，改用文字推播")
                decrease_fallback = requests.post(DISCORD_WEBHOOK_URL, json={"content": decrease_content})
                if decrease_fallback.status_code == 204:
                    print("✅ 大戶減少觀察名單文字備援推播完成！")
                else:
                    print(f"❌ 大戶減少觀察名單文字備援推播失敗: {decrease_fallback.status_code}")
        except Exception as e:
            print(f"❌ 大戶減少觀察名單圖片發送錯誤: {e}，改用文字推播")
            try:
                decrease_response = requests.post(DISCORD_WEBHOOK_URL, json={"content": decrease_content})
                if decrease_response.status_code == 204:
                    print("✅ 大戶減少觀察名單文字備援推播完成！")
                else:
                    print(f"❌ 大戶減少觀察名單文字備援推播失敗: {decrease_response.status_code}")
            except Exception as inner_e:
                print(f"❌ 大戶減少觀察名單發送錯誤: {inner_e}")
    else:
        print("已略過本週大戶持股減少觀察名單 Discord 推播。")

if __name__ == "__main__":
    push_rank_to_dc()
