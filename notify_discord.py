import gspread
import requests
import os
import json
import re
import time
import random
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from io import BytesIO

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib import font_manager
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from PIL import Image

# ============================
# ⚙️ 設定區
# ============================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_TEST", "").strip()
SHEET_NAME = "台股注意股資料庫_V33"
SERVICE_KEY_FILE = "service_key.json"

# 啟動時診斷:檢查環境變數狀態 (不會印出 webhook 內容,只印長度)
if not DISCORD_WEBHOOK_URL:
    print("=" * 60)
    print("❌ 嚴重錯誤: 環境變數 DISCORD_WEBHOOK_URL_TEST 未設定或為空")
    print("   請至 GitHub repo → Settings → Secrets and variables")
    print("   → Actions,確認有名為 DISCORD_WEBHOOK_URL_TEST 的 Secret,")
    print("   且內容不為空")
    print("=" * 60)
else:
    # 只印長度,不印內容,確認 Secret 真的有值
    print(f"✅ DISCORD_WEBHOOK_URL_TEST 已載入 (長度: {len(DISCORD_WEBHOOK_URL)} 字元)")

JAIL_ENTER_THRESHOLD = 3   
JAIL_EXIT_THRESHOLD = 5    

# 處置股技術追蹤工作表名稱（用來查訊號狀態，幫股號股名變色）
TECH_TRACK_SHEET_NAME = "處置股技術追蹤"


def format_display_price(value):
    """將目前價整理成適合圖片顯示的字串。"""
    try:
        if value is None:
            return "--"
        s = str(value).replace(",", "").strip()
        if s == "" or s.lower() in {"nan", "none"}:
            return "--"
        num = float(s)
        if abs(num - round(num)) < 1e-9:
            return str(int(round(num)))
        return f"{num:.1f}"
    except Exception:
        s = str(value).strip()
        return s if s else "--"

# ============================
# 🎨 圖片風格設定
# ============================
CJK_FONT_PATH = None
CJK_BOLD_FONT_PATH = None
EMOJI_FONT_PATH = None
EMOJI_IMAGE_CACHE = {}
FONT_DOWNLOAD_DIR = os.path.join(os.getenv("RUNNER_TEMP", "/tmp"), "stock_monitor_fonts")


def _download_font_if_needed(url, filename):
    """在 GitHub Actions 沒有安裝中文字型時，自動下載 Noto CJK 字型到暫存目錄。"""
    try:
        os.makedirs(FONT_DOWNLOAD_DIR, exist_ok=True)
        font_path = os.path.join(FONT_DOWNLOAD_DIR, filename)
        if os.path.exists(font_path) and os.path.getsize(font_path) > 1024 * 1024:
            return font_path

        print(f"⚠️ 系統找不到中文字型，嘗試下載備援字型: {filename}")
        response = requests.get(url, timeout=20)
        if response.status_code == 200 and response.content:
            with open(font_path, "wb") as f:
                f.write(response.content)
            if os.path.getsize(font_path) > 1024 * 1024:
                print(f"✅ 備援中文字型下載成功: {font_path}")
                return font_path
        print(f"⚠️ 備援中文字型下載失敗: HTTP {response.status_code}")
    except Exception as e:
        print(f"⚠️ 備援中文字型下載錯誤: {e}")
    return None


def load_chinese_font():
    """載入中文字型 (含詳細診斷)"""
    global CJK_FONT_PATH
    search_paths = [
        # GitHub Actions / Ubuntu 常見路徑
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJKtc-Regular.otf",
        "/usr/share/fonts/noto-cjk/NotoSansCJKtc-Regular.otf",
        "/usr/local/share/fonts/NotoSansCJKtc-Regular.otf",
        "/usr/local/share/fonts/NotoSansTC-Regular.otf",
        # Windows / macOS 常見路徑
        "C:/Windows/Fonts/msjh.ttc",
        "C:/Windows/Fonts/mingliu.ttc",
        "/System/Library/Fonts/PingFang.ttc",
        "/System/Library/Fonts/STHeiti Light.ttc",
    ]
    for path in search_paths:
        if os.path.exists(path):
            font_manager.fontManager.addfont(path)
            CJK_FONT_PATH = path
            print(f"✅ 中文字型載入成功: {path}")
            return font_manager.FontProperties(fname=path)
    fallback_path = _download_font_if_needed(
        "https://raw.githubusercontent.com/googlefonts/noto-cjk/main/Sans/OTF/TraditionalChinese/NotoSansCJKtc-Regular.otf",
        "NotoSansCJKtc-Regular.otf"
    )
    if fallback_path and os.path.exists(fallback_path):
        font_manager.fontManager.addfont(fallback_path)
        CJK_FONT_PATH = fallback_path
        return font_manager.FontProperties(fname=fallback_path)

    print("❌ 嚴重錯誤: 找不到任何中文字型! 請確認 GitHub Actions 已安裝 fonts-noto-cjk")
    return font_manager.FontProperties(family="DejaVu Sans")


def load_chinese_bold_font():
    """載入中文粗體字型"""
    global CJK_BOLD_FONT_PATH
    search_paths = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Black.ttc",
        "/usr/share/fonts/noto-cjk/NotoSansCJKtc-Bold.otf",
        "/usr/local/share/fonts/NotoSansCJKtc-Bold.otf",
        "/usr/local/share/fonts/NotoSansTC-Bold.otf",
        "C:/Windows/Fonts/msjhbd.ttc",
        "/System/Library/Fonts/PingFang.ttc",
    ]
    for path in search_paths:
        if os.path.exists(path):
            font_manager.fontManager.addfont(path)
            CJK_BOLD_FONT_PATH = path
            print(f"✅ 中文粗體載入成功: {path}")
            return font_manager.FontProperties(fname=path)
    fallback_path = _download_font_if_needed(
        "https://raw.githubusercontent.com/googlefonts/noto-cjk/main/Sans/OTF/TraditionalChinese/NotoSansCJKtc-Bold.otf",
        "NotoSansCJKtc-Bold.otf"
    )
    if fallback_path and os.path.exists(fallback_path):
        font_manager.fontManager.addfont(fallback_path)
        CJK_BOLD_FONT_PATH = fallback_path
        return font_manager.FontProperties(fname=fallback_path)

    print("⚠️ 找不到中文粗體字型,使用一般中文字型代替")
    CJK_BOLD_FONT_PATH = CJK_FONT_PATH
    return load_chinese_font()


def load_emoji_font():
    """載入 emoji 字型。注意: Matplotlib 對彩色 emoji 字型支援不完整,圖片內 emoji 會改用 Twemoji PNG 疊圖。"""
    global EMOJI_FONT_PATH
    search_paths = [
        "/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf",
        "/usr/share/fonts/noto/NotoColorEmoji.ttf",
        "/usr/share/fonts/google-noto-emoji/NotoColorEmoji.ttf",
        "C:/Windows/Fonts/seguiemj.ttf",
        "/System/Library/Fonts/Apple Color Emoji.ttc",
    ]
    for path in search_paths:
        if os.path.exists(path):
            # 只記錄路徑,不要交給 Matplotlib 直接載入。
            # 原因: Noto Color Emoji / Apple Color Emoji 屬於彩色 emoji 字型,
            # Matplotlib 在部分 Linux Actions 環境會顯示方塊、缺字,甚至載入失敗。
            EMOJI_FONT_PATH = path
            print(f"✅ Emoji 字型偵測成功: {path}")
            return None
    print("⚠️ 找不到 Emoji 字型。圖片內 emoji 將優先使用 Twemoji PNG,失敗時改用安全符號替代")
    return None


# 重建 matplotlib 字型快取 (Actions 環境每次都是全新的,但保險起見)
try:
    cache_dir = matplotlib.get_cachedir()
    print(f"📂 matplotlib 快取目錄: {cache_dir}")
except Exception as e:
    print(f"⚠️ 取得快取路徑失敗: {e}")

FONT_PROP = load_chinese_font()
FONT_BOLD = load_chinese_bold_font()
FONT_EMOJI = load_emoji_font()

# 設定 matplotlib 全域預設字型 fallback 鏈
# 順序: 已載入中文字型 → 常見中文字型名稱 → DejaVu Sans
try:
    sans_list = []
    for font_path in [CJK_FONT_PATH, CJK_BOLD_FONT_PATH]:
        if font_path:
            try:
                sans_list.append(font_manager.FontProperties(fname=font_path).get_name())
            except Exception:
                pass
    sans_list.extend([
        'Noto Sans CJK TC',
        'Noto Sans CJK JP',
        'Noto Sans CJK SC',
        'Microsoft JhengHei',
        'PingFang TC',
        'Arial Unicode MS',
        'DejaVu Sans'
    ])
    # 去除重複,保留順序
    sans_list = list(dict.fromkeys(sans_list))
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = sans_list
    plt.rcParams['axes.unicode_minus'] = False
    print(f"✅ matplotlib 字型優先順序: {sans_list}")
except Exception as e:
    print(f"⚠️ matplotlib 字型設定失敗: {e}")


# ============================
# 🧹 文字清洗工具
# ============================
_ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u202a-\u202e\ufeff]")

# 將半形英數字轉為全形，用於股票名稱中英混排時避免顯示歪掉
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


def clean_cell(s) -> str:
    s = "" if s is None else str(s)
    # 保留全形字，不做 NFKC，避免中文與全形英數又被轉回半形
    s = s.replace("\xa0", " ")
    s = _ZERO_WIDTH_RE.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def fix_common_cjk_mojibake(s) -> str:
    """修正常見中文字資料來源亂碼。"""
    s = clean_cell(s)
    replacements = {
        "卅卅": "碁",
    }
    for bad, good in replacements.items():
        s = s.replace(bad, good)
    return s


def clean_display_text(s, fullwidth_ascii=False) -> str:
    s = fix_common_cjk_mojibake(s)
    if fullwidth_ascii:
        s = to_fullwidth(s)
    return s


EMOJI_FALLBACK_SYMBOLS = {
    "🚨": "!",
    "🔓": "OPEN",
    "⛓️": "LOCK",
    "👑": "★",
    "🔥": "▲",
    "💀": "▼",
    "📉": "↓",
    "🧊": "◆",
    "❓": "?",
}


def _twemoji_codepoints(emoji_text):
    """將 emoji 轉為 Twemoji 檔名用 codepoint。"""
    return "-".join(f"{ord(ch):x}" for ch in emoji_text if ord(ch) != 0xfe0f)


def _twemoji_codepoints_keep_vs16(emoji_text):
    """保留 VS16 的 Twemoji codepoint,部分符號 emoji 需要這個版本。"""
    return "-".join(f"{ord(ch):x}" for ch in emoji_text)


def get_twemoji_image(emoji_text):
    """下載並快取 Twemoji PNG,用來避開 Matplotlib 無法穩定顯示彩色 emoji 的問題。"""
    if emoji_text in EMOJI_IMAGE_CACHE:
        return EMOJI_IMAGE_CACHE[emoji_text]

    candidates = []
    keep_vs16 = _twemoji_codepoints_keep_vs16(emoji_text)
    no_vs16 = _twemoji_codepoints(emoji_text)
    if keep_vs16:
        candidates.append(keep_vs16)
    if no_vs16 and no_vs16 not in candidates:
        candidates.append(no_vs16)

    base_urls = [
        "https://raw.githubusercontent.com/jdecked/twemoji/main/assets/72x72",
        "https://raw.githubusercontent.com/twitter/twemoji/master/assets/72x72",
    ]

    for code in candidates:
        for base_url in base_urls:
            url = f"{base_url}/{code}.png"
            try:
                response = requests.get(url, timeout=2.5)
                if response.status_code == 200 and response.content:
                    img = Image.open(BytesIO(response.content)).convert("RGBA")
                    EMOJI_IMAGE_CACHE[emoji_text] = img
                    return img
            except Exception:
                continue

    EMOJI_IMAGE_CACHE[emoji_text] = None
    return None


def draw_emoji_image(ax, emoji_text, x, y, fontsize=18, transform=None, zorder=5, fallback_color="#4A5565"):
    """在 Matplotlib 圖上以 PNG 疊上 emoji；下載失敗時使用安全符號，避免方塊或缺字。"""
    transform = transform or ax.transAxes
    img = get_twemoji_image(emoji_text)

    if img is not None:
        imagebox = OffsetImage(img, zoom=max(0.18, fontsize / 42.0), resample=True)
        ab = AnnotationBbox(
            imagebox,
            (x, y),
            xycoords=transform,
            frameon=False,
            pad=0,
            box_alignment=(0.5, 0.5),
            zorder=zorder
        )
        ab.set_clip_on(False)
        ax.add_artist(ab)
        return True

    fallback = EMOJI_FALLBACK_SYMBOLS.get(emoji_text, "")
    if fallback:
        ax.text(x, y, fallback,
                transform=transform, ha='center', va='center',
                fontsize=fontsize, fontweight='bold',
                fontproperties=FONT_BOLD, color=fallback_color,
                zorder=zorder)
    return False


def draw_emoji_on_fig(fig, emoji_text, x, y, fontsize=34, zorder=5):
    """用 figure 座標疊上 emoji。"""
    if not fig.axes:
        ax = fig.add_axes([0, 0, 1, 1], frameon=False)
        ax.set_axis_off()
    else:
        ax = fig.axes[0]
    return draw_emoji_image(ax, emoji_text, x, y, fontsize=fontsize, transform=fig.transFigure, zorder=zorder)


# ---- 共用顏色 ----
BG_MAIN     = '#F5F7FA'
BG_TOPBAR   = '#EEF2F6'
BG_TABLE    = '#FFFFFF'
BG_ROW_ODD  = '#FFFFFF'
BG_ROW_EVEN = '#F7F9FC'
BG_RANK     = '#FBFCFE'

TEXT_HEADER = '#2F3A4A'
TEXT_MAIN   = '#2C3440'
TEXT_MUTED  = '#8A94A6'
TEXT_POS    = '#FF6B6B'
TEXT_NEG    = '#15803D'
TEXT_FLAT   = '#8A94A6'
TEXT_PRICE  = '#1E3A8A'

GOLD        = '#FFD060'
SILVER      = '#C0C8D4'
BRONZE      = '#E8A070'
BORDER_DARK = '#E6EBF2'
BORDER_MID  = '#D8E0EA'

DAYS_URGENT_BG = '#FF4757'
DAYS_URGENT_FG = '#FFFFFF'
DAYS_WARN_BG   = '#FFA502'
DAYS_WARN_FG   = '#1A1A1A'
DAYS_NORMAL_BG = '#E9EEF5'
DAYS_NORMAL_FG = '#2C3440'

# ---- 訊號狀態色（從處置股技術追蹤工作表的「訊號狀態」欄位讀取後套用） ----
# 背景是白色 / 淺灰，所以兩個顏色都選偏深的版本，不會反白也不會刺眼。
SIGNAL_COLOR_RETEST   = '#C2410C'  # 深橘：訊號狀態 = 目前回測月線
SIGNAL_COLOR_BREAKOUT = '#1D4ED8'  # 深藍：訊號狀態 = 回測後轉強

THEME_ENTERING  = {'accent': '#E85D6A', 'header': '#FCECEF', 'title': '處置倒數  瀕臨處置監控', 'title_icon': '🚨', 'subtitle_text': '瀕臨處置 (3日內)', 'title_fontsize': 30}
THEME_RELEASING = {'accent': '#16B27A', 'header': '#EAF7F1', 'title': '越關越大尾  即將出關監控', 'title_icon': '🔓', 'subtitle_text': '即將出關 (5日內)', 'title_fontsize': 34}
THEME_INJAIL    = {'accent': '#B06FD3', 'header': '#F5ECFB', 'title': '還能噴嗎  正在處置監控', 'title_icon': '⛓️', 'subtitle_text': '處置中股票名單', 'title_fontsize': 30}


# ============================
# 🛠️ 工具函式 (原本邏輯,加上重試)
# ============================
def connect_google_sheets():
    """連線 Google Sheets (含指數退避重試,解決 Google API 偶發 5xx/429 錯誤)"""
    if not os.path.exists(SERVICE_KEY_FILE):
        print("❌ 找不到 service_key.json")
        return None

    max_retries = 5
    for attempt in range(max_retries):
        try:
            gc = gspread.service_account(filename=SERVICE_KEY_FILE)
            sh = gc.open(SHEET_NAME)
            if attempt > 0:
                print(f"✅ 第 {attempt + 1} 次重試成功")
            return sh
        except gspread.exceptions.APIError as e:
            msg = str(e)
            is_retryable = any(code in msg for code in ['429', '500', '502', '503', '504'])
            if is_retryable and attempt < max_retries - 1:
                wait = (2 ** attempt) + random.uniform(0, 1)
                print(f"⚠️ Google API 暫時性錯誤,{wait:.1f}秒後重試 ({attempt + 1}/{max_retries}): {msg[:80]}")
                time.sleep(wait)
                continue
            print(f"❌ Google Sheet 連線失敗 (不可重試): {e}")
            return None
        except Exception as e:
            print(f"❌ 未預期錯誤: {e}")
            return None

    print(f"❌ 重試 {max_retries} 次後仍失敗")
    return None


def send_discord_image(image_buf, content_text=""):
    """發送圖片到 Discord"""
    if not DISCORD_WEBHOOK_URL:
        print("❌ 找不到 DISCORD_WEBHOOK_URL")
        return
    try:
        files = {"file": ("chart.png", image_buf, "image/png")}
        data = {
            "username": "台股處置監控機器人",
            "avatar_url": "https://cdn-icons-png.flaticon.com/512/2502/2502697.png",
            "content": content_text
        }
        response = requests.post(DISCORD_WEBHOOK_URL, data=data, files=files)
        if response.status_code not in (200, 204):
            print(f"❌ Discord 推播失敗: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"❌ 發送請求錯誤: {e}")


def parse_roc_date(date_str):
    """解析日期格式"""
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


def get_merged_jail_periods(sh):
    """讀取並合併處置期間 (原本邏輯)"""
    jail_map = {} 
    tw_now = datetime.utcnow() + timedelta(hours=8)
    today = datetime(tw_now.year, tw_now.month, tw_now.day)
    try:
        ws = sh.worksheet("處置股90日明細")
        records = ws.get_all_records()
        for row in records:
            code = str(row.get('代號', '')).replace("'", "").strip()
            period = str(row.get('處置期間', '')).strip()
            if not code or not period: continue
            dates = re.split(r'[~-～]', period)
            if len(dates) >= 2:
                s_date, e_date = parse_roc_date(dates[0]), parse_roc_date(dates[1])
                if s_date and e_date:
                    if e_date < today: continue
                    if code not in jail_map:
                        jail_map[code] = {'start': s_date, 'end': e_date}
                    else:
                        jail_map[code]['start'] = min(jail_map[code]['start'], s_date)
                        jail_map[code]['end'] = max(jail_map[code]['end'], e_date)
    except: return {}
    return {c: f"{d['start'].strftime('%m/%d')}-{d['end'].strftime('%m/%d')}" for c, d in jail_map.items()}


# ============================
# 🎯 訊號狀態 → 顏色對照（讀取「處置股技術追蹤」工作表）
# ============================
def load_tech_tracking_latest_map(sh):
    """從「處置股技術追蹤」讀取每檔最新一筆資料。

    回傳：{代號: {'status': 訊號狀態, 'price': 目前價, 'date': 計算日期}}
    """
    tech_map = {}
    try:
        ws = sh.worksheet(TECH_TRACK_SHEET_NAME)
        records = ws.get_all_records()
        latest_by_code = {}
        for row in records:
            code = str(row.get('代號', '')).replace("'", "").strip()
            if not code:
                continue
            status = str(row.get('訊號狀態', '')).strip()
            calc_date = str(row.get('計算日期', '')).strip()
            price = format_display_price(row.get('目前價', ''))
            if code not in latest_by_code or calc_date >= latest_by_code[code]['date']:
                latest_by_code[code] = {'status': status, 'price': price, 'date': calc_date}
        tech_map = latest_by_code
    except Exception as e:
        print(f"⚠️ 讀取「{TECH_TRACK_SHEET_NAME}」失敗（可能工作表不存在或欄位有變動）: {e}")
    return tech_map


def load_signal_status_map(sh):
    """從「處置股技術追蹤」工作表讀取每檔最新訊號狀態。"""
    signal_map = {}
    tech_map = load_tech_tracking_latest_map(sh)
    if tech_map:
        signal_map = {c: v.get('status', '') for c, v in tech_map.items()}
        retest_n = sum(1 for s in signal_map.values() if s == "目前回測月線")
        breakout_n = sum(1 for s in signal_map.values() if s == "回測後轉強")
        print(f"✅ 已載入處置股技術追蹤訊號 {len(signal_map)} 檔 (目前回測月線:{retest_n} / 回測後轉強:{breakout_n})")
    return signal_map


def load_current_price_map(sh):
    """從「處置股技術追蹤」工作表讀取每檔最新目前價。"""
    tech_map = load_tech_tracking_latest_map(sh)
    price_map = {c: v.get('price', '--') for c, v in tech_map.items()}
    valid_n = sum(1 for p in price_map.values() if p != '--')
    print(f"✅ 已載入處置股技術追蹤目前價 {len(price_map)} 檔 (有效價格:{valid_n})")
    return price_map


def get_signal_color(code, signal_map, default_color=None):
    """依訊號狀態決定股號股名顏色。

    規則：
        訊號狀態 = "回測後轉強"   → 深藍 (#1D4ED8)
        訊號狀態 = "目前回測月線" → 深橘 (#C2410C)
        其他狀態 / 沒有資料        → 預設色 (TEXT_MAIN)
    """
    if default_color is None:
        default_color = TEXT_MAIN
    if not signal_map:
        return default_color
    status = signal_map.get(code, '')
    if status == "回測後轉強":
        return SIGNAL_COLOR_BREAKOUT
    if status == "目前回測月線":
        return SIGNAL_COLOR_RETEST
    return default_color


# ============================
# 📊 價格數據處理邏輯 (原本邏輯)
# ============================
def get_price_rank_info(code, period_str, market):
    """計算處置前 vs 處置中的績效對比"""
    try:
        dates = re.split(r'[~-～]', str(period_str))
        start_date = parse_roc_date(dates[0])
        if not start_date: return "❓ 未知", "日期錯", "+0.0", "+0.0"
        
        fetch_start = start_date - timedelta(days=60)
        end_date = datetime.now() + timedelta(days=1)
        suffix = ".TWO" if any(x in str(market) for x in ["上櫃", "TPEx"]) else ".TW"
        ticker = f"{code}{suffix}"
        
        df = yf.Ticker(ticker).history(start=fetch_start.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), auto_adjust=True)
        
        if not df.empty:
            df = df.ffill() 
        
        if df.empty or len(df) < 2: return "❓ 未知", "無股價", "+0.0", "+0.0"

        df.index = df.index.tz_localize(None)
        df_in_jail = df[df.index >= pd.Timestamp(start_date)]
        
        mask_before = df.index < pd.Timestamp(start_date)
        if not mask_before.any(): 
            pre_pct = 0.0
        else:
            jail_base_p = df[mask_before]['Close'].iloc[-1]
            jail_days_count = len(df_in_jail) if not df_in_jail.empty else 1
            loc_idx = df.index.get_loc(df[mask_before].index[-1])
            target_idx = max(0, loc_idx - jail_days_count + 1)
            pre_entry = df.iloc[target_idx]['Open']
            pre_pct = ((jail_base_p - pre_entry) / pre_entry) * 100

        if df_in_jail.empty: 
            in_pct = 0.0
        else:
            jail_start_entry = df_in_jail['Open'].iloc[0]
            curr_p = df_in_jail['Close'].iloc[-1]
            in_pct = ((curr_p - jail_start_entry) / jail_start_entry) * 100

        if in_pct > 15:    icon, status_text = "👑", "妖股誕生"
        elif in_pct > 5:   icon, status_text = "🔥", "強勢突圍"
        elif in_pct < -15: icon, status_text = "💀", "人去樓空"
        elif in_pct < -5:  icon, status_text = "📉", "走勢疲軟"
        else:              icon, status_text = "🧊", "多空膠著"
        
        status_text = clean_display_text(status_text)
        pre_str = f"{'+' if pre_pct > 0 else ''}{pre_pct:.1f}"
        in_str  = f"{'+' if in_pct > 0 else ''}{in_pct:.1f}"
        return icon, status_text, pre_str, in_str
    except Exception as e:
        print(f"⚠️ 失敗 ({code}): {e}")
        return "❓ 未知", "數據計算中", "+0.0", "+0.0"


# ============================
# 🔍 監控邏輯 (原本邏輯)
# ============================
def check_status_split(sh, releasing_codes, price_map=None):
    try:
        ws = sh.worksheet("近30日熱門統計")
        records = ws.get_all_records()
    except: return {'entering': [], 'in_jail': []}
    jail_map = get_merged_jail_periods(sh)
    ent, inj, seen = [], [], set()
    for row in records:
        code = str(row.get('代號', '')).replace("'", "").strip()
        if code in releasing_codes or code in seen: continue
        name, days_str, reason = clean_display_text(row.get('名稱', '')), str(row.get('最快處置天數', '99')), clean_display_text(row.get('處置觸發原因', ''))
        if not days_str.isdigit(): continue
        d = int(days_str) + 1  
        if "處置中" in reason:
            inj.append({"code": code, "name": name, "price": format_display_price((price_map or {}).get(code, "--")), "period": jail_map.get(code, "日期未知")})
            seen.add(code)
        elif d <= JAIL_ENTER_THRESHOLD:
            ent.append({"code": code, "name": name, "days": d})
            seen.add(code)
            
    ent.sort(key=lambda x: (x['days'], x['code']))
    
    def get_inj_sort_key(item):
        p = item.get('period', '')
        end_date = p.split('-')[1] if '-' in p else "9999/12/31"
        return (end_date, item['code'])
    
    inj.sort(key=get_inj_sort_key)
    
    return {'entering': ent, 'in_jail': inj}


def check_releasing_stocks(sh, price_map=None):
    try:
        ws = sh.worksheet("即將出關監控")
        records = ws.get_all_records()
    except: return []
    res, seen = [], set()
    for row in records:
        code = str(row.get('代號', '')).strip()
        if code in seen: continue
        days_str = str(row.get('剩餘天數', '99'))
        if not days_str.isdigit(): continue
        d = int(days_str) + 1
        if d <= JAIL_EXIT_THRESHOLD:
            icon, status_text, pre_str, in_str = get_price_rank_info(code, row.get('處置期間', ''), row.get('市場', '上市'))
            dt = parse_roc_date(row.get('出關日期', ''))
            res.append({
                "code": code,
                "name": clean_display_text(row.get('名稱', '')),
                "days": d,
                "price": format_display_price((price_map or {}).get(code, "--")),
                "date": dt.strftime("%m/%d") if dt else "??/??",
                "icon": icon,
                "status_text": status_text,
                "pre_pct": pre_str,
                "in_pct": in_str
            })
            seen.add(code)
    res.sort(key=lambda x: (x['days'], x['code']))
    return res


# ============================
# 🎨 圖片生成函式
# ============================
def parse_pct(s):
    try:
        return float(str(s).replace('%', '').replace('+', ''))
    except:
        return 0


def get_pct_color(pct_str):
    pct = parse_pct(pct_str)
    if pct > 0: return TEXT_POS
    if pct < 0: return TEXT_NEG
    return TEXT_FLAT


def get_days_style(days):
    if days <= 1:  return DAYS_URGENT_BG, DAYS_URGENT_FG
    if days <= 3:  return DAYS_WARN_BG, DAYS_WARN_FG
    return DAYS_NORMAL_BG, DAYS_NORMAL_FG


TOPBAR_TITLE_FONT_SIZE = 38
TOPBAR_SUBTITLE_FONT_SIZE = 17
TOPBAR_ICON_FONT_SIZE = 20
TOPBAR_ICON_GAP = 0.022
TOPBAR_ICON_WIDTH_INCH = 0.28
TOPBAR_TITLE_X = 0.5
TOPBAR_SUBTITLE_X = 0.5

# [調整重點 1] topbar 區域往上收緊：
#   - 上方背景區塊高度從 0.085 縮成 0.055（縮 35%）
#   - 標題 y 位置從 0.958 上移到 0.972（更靠近頂端）
#   - 副標題 y 位置從 0.927 上移到 0.948（與標題距離拉近）
#   - 整個 topbar 從佔 8.5% 縮成 4.5%，騰出 4% 空間給表格
TOPBAR_BG_TOP = 1.0
TOPBAR_BG_HEIGHT = 0.055
TOPBAR_BG_BOTTOM = TOPBAR_BG_TOP - TOPBAR_BG_HEIGHT  # = 0.945
TOPBAR_TITLE_Y = 0.978
TOPBAR_SUBTITLE_Y = 0.954


def draw_topbar(fig, theme, total, page_info=""):
    fig.add_artist(patches.Rectangle(
        (0, TOPBAR_BG_BOTTOM), 1, TOPBAR_BG_HEIGHT,
        linewidth=0, facecolor=BG_MAIN,
        transform=fig.transFigure, clip_on=False, zorder=0
    ))
    fig.add_artist(patches.Rectangle(
        (0, 0.99), 1, 0.01,
        linewidth=0, facecolor=theme['accent'],
        transform=fig.transFigure, clip_on=False, zorder=1
    ))

    title_fontsize = theme.get('title_fontsize', TOPBAR_TITLE_FONT_SIZE)
    title_y = TOPBAR_TITLE_Y
    icon_gap = theme.get('title_icon_gap', TOPBAR_ICON_GAP)
    icon_fontsize = theme.get('title_icon_fontsize', TOPBAR_ICON_FONT_SIZE)
    icon_width = TOPBAR_ICON_WIDTH_INCH / max(fig.get_size_inches()[0], 1)

    title_obj = fig.text(TOPBAR_TITLE_X, title_y, clean_display_text(theme['title']),
                         ha='center', va='center',
                         fontsize=title_fontsize, fontweight='bold',
                         fontproperties=FONT_BOLD,
                         color='#2C3440', zorder=2)

    if theme.get('title_icon'):
        # 以「emoji + 標題文字」整組置中，而不是只讓文字置中。
        # 這樣不同圖片寬度下，標題區視覺中心會一致，不會因 emoji 在左側而看起來偏掉。
        try:
            fig.canvas.draw()
            title_obj.set_position((TOPBAR_TITLE_X + (icon_width + icon_gap) / 2, title_y))
            fig.canvas.draw()
            renderer = fig.canvas.get_renderer()
            bbox = title_obj.get_window_extent(renderer=renderer)
            bbox_fig = bbox.transformed(fig.transFigure.inverted())
            icon_x = max(0.05, bbox_fig.x0 - icon_gap - icon_width / 2)
            draw_emoji_on_fig(fig, theme['title_icon'], icon_x, title_y, fontsize=icon_fontsize, zorder=4)
        except Exception:
            # 若 bbox 計算失敗，退回較保守的位置
            draw_emoji_on_fig(fig, theme['title_icon'], 0.24, title_y, fontsize=icon_fontsize, zorder=4)

    today_str = datetime.now().strftime("%Y-%m-%d")
    sub = f"資料日期: {today_str}  |  共 {total} 檔"
    if page_info: sub += f"  |  {clean_display_text(page_info)}"
    fig.text(TOPBAR_SUBTITLE_X, TOPBAR_SUBTITLE_Y, clean_display_text(sub),
             ha='center', va='center',
             fontsize=TOPBAR_SUBTITLE_FONT_SIZE,
             fontproperties=FONT_PROP,
             color='#7F8C9B', zorder=2)


def draw_table_frame(ax, theme, subtitle, top_y, total_h):
    ax.add_patch(patches.Rectangle(
        (0.005, top_y - total_h - 0.01), 0.99, total_h + 0.015,
        linewidth=1.2, edgecolor=BORDER_MID, facecolor=BG_TABLE,
        transform=ax.transAxes, clip_on=False, zorder=0
    ))
    ax.add_patch(patches.Rectangle(
        (0.005, top_y - total_h - 0.01), 0.009, total_h + 0.015,
        linewidth=0, facecolor=theme['accent'],
        transform=ax.transAxes, clip_on=False, zorder=1
    ))
    ax.text(0.018, top_y + 0.015, f"▌ {clean_display_text(subtitle)}",
            transform=ax.transAxes, ha='left', va='bottom',
            fontsize=19, fontweight='bold',
            fontproperties=FONT_BOLD, color=theme['accent'])


COMMON_FIG_WIDTH = 15
WATERMARK_TEXT = "By 股市艾斯出品-轉傳請註明"
DISCLAIMER_TEXT = "資訊分享非投資建議 投資請自行評估風險"
WATERMARK_ALPHA = 0.80

# 底部圖例（只顯示在「即將出關」與「處置中」兩張圖）
LEGEND_BG = '#F8FAFC'
LEGEND_BORDER = '#D6DEE8'
LEGEND_TEXT = '#5B6678'
LEGEND_BOX_ORANGE = SIGNAL_COLOR_RETEST
LEGEND_BOX_BLUE = SIGNAL_COLOR_BREAKOUT


def draw_watermark(fig):
    watermark_text = clean_display_text(WATERMARK_TEXT) + "\n" + clean_display_text(DISCLAIMER_TEXT)
    fig.text(0.985, 0.010, watermark_text,
             ha='right', va='bottom',
             fontsize=10.5,
             linespacing=1.04,
             fontproperties=FONT_PROP,
             color='#2C3440', alpha=WATERMARK_ALPHA, zorder=10)


def draw_signal_legend(fig):
    """在圖片底部加入訊號顏色圖例（即將出關 / 處置中使用）。"""
    box_x = 0.028
    box_y = 0.010
    box_w = 0.385
    box_h = 0.028

    fig.add_artist(patches.FancyBboxPatch(
        (box_x, box_y), box_w, box_h,
        boxstyle="round,pad=0.0025,rounding_size=0.008",
        linewidth=0.85, edgecolor=LEGEND_BORDER, facecolor=LEGEND_BG,
        transform=fig.transFigure, clip_on=False, zorder=8
    ))

    text_y = box_y + box_h / 2
    x = box_x + 0.010
    main_fs = 12.6
    item_fs = 12.4
    sep_fs = 10.8
    swatch_w = 0.0085
    swatch_h = 0.010

    fig.text(x, text_y, clean_display_text("顏色說明"),
             ha='left', va='center', fontsize=main_fs,
             fontproperties=FONT_BOLD, color=LEGEND_TEXT, zorder=9)
    x += 0.062
    fig.text(x, text_y, "｜",
             ha='left', va='center', fontsize=sep_fs,
             fontproperties=FONT_PROP, color='#A0AAB8', zorder=9)
    x += 0.010

    fig.add_artist(patches.Rectangle(
        (x, text_y - swatch_h / 2), swatch_w, swatch_h,
        linewidth=0, facecolor=LEGEND_BOX_ORANGE,
        transform=fig.transFigure, clip_on=False, zorder=9
    ))
    x += 0.013
    fig.text(x, text_y, clean_display_text("接近20MA"),
             ha='left', va='center', fontsize=item_fs,
             fontproperties=FONT_PROP, color=LEGEND_TEXT, zorder=9)
    x += 0.068
    fig.text(x, text_y, "｜",
             ha='left', va='center', fontsize=sep_fs,
             fontproperties=FONT_PROP, color='#A0AAB8', zorder=9)
    x += 0.010

    fig.add_artist(patches.Rectangle(
        (x, text_y - swatch_h / 2), swatch_w, swatch_h,
        linewidth=0, facecolor=LEGEND_BOX_BLUE,
        transform=fig.transFigure, clip_on=False, zorder=9
    ))
    x += 0.013
    fig.text(x, text_y, clean_display_text("回測20MA後再轉強"),
             ha='left', va='center', fontsize=item_fs,
             fontproperties=FONT_PROP, color=LEGEND_TEXT, zorder=9)


# [調整重點 2] 表格區整體往上拉：
#   - UNIFIED_SUBPLOT_TOP 從 0.908 拉到 0.940（+3.2%）
#   - TABLE_TOP_Y 從 0.988 拉到 0.998（盡量貼近 axes 頂端）
#   - TABLE_TOTAL_H 從 0.936 加大到 0.965（多吃 2.9%）
UNIFIED_SUBPLOT_LEFT = 0.025
UNIFIED_SUBPLOT_RIGHT = 0.975
UNIFIED_SUBPLOT_TOP = 0.940
UNIFIED_SUBPLOT_BOTTOM = 0.009
TABLE_TOP_Y = 0.998
TABLE_TOTAL_H = 0.965
TABLE_HEADER_H_INCH = 0.50
TABLE_HEADER_H_MIN = 0.020
TABLE_HEADER_H_MAX = 0.070


def calc_dynamic_fig_h(n, *, base_h, per_row_h, min_h, max_h):
    return max(min_h, min(max_h, base_h + n * per_row_h))


def calc_header_h(fig_h):
    """依圖片實際高度反推表頭比例，讓三張圖的表頭視覺高度一致。"""
    axes_h_inch = fig_h * (UNIFIED_SUBPLOT_TOP - UNIFIED_SUBPLOT_BOTTOM)
    if axes_h_inch <= 0:
        return 0.05
    return max(TABLE_HEADER_H_MIN, min(TABLE_HEADER_H_MAX, TABLE_HEADER_H_INCH / axes_h_inch))


def get_injail_n_cols(n):
    if n <= 12:
        return 1
    elif n <= 40:
        return 2
    else:
        return 3


def save_figure_to_buffer(fig):
    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=130, facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf


def draw_entering_image(data, signal_map=None):
    """瀕臨處置 - 單欄詳細圖

    signal_map: {代號: 訊號狀態} dict（從處置股技術追蹤讀取），
                用來決定股號股名的顏色：
                - 回測後轉強 → 深藍
                - 目前回測月線 → 深橘
                - 其他/沒資料 → 預設深灰
    """
    theme = THEME_ENTERING
    n = len(data)
    fig_h = calc_dynamic_fig_h(n, base_h=5.9, per_row_h=0.36, min_h=7.0, max_h=15.6)

    fig, ax = plt.subplots(figsize=(COMMON_FIG_WIDTH, fig_h), facecolor=BG_MAIN)
    fig.subplots_adjust(left=UNIFIED_SUBPLOT_LEFT, right=UNIFIED_SUBPLOT_RIGHT,
                        top=UNIFIED_SUBPLOT_TOP, bottom=UNIFIED_SUBPLOT_BOTTOM)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.set_axis_off()

    draw_topbar(fig, theme, n)

    header_h = calc_header_h(fig_h)
    total_h = TABLE_TOTAL_H
    row_h = (total_h - header_h) / max(n, 1)
    top_y = TABLE_TOP_Y

    draw_table_frame(ax, theme, theme['subtitle_text'], top_y, total_h)

    col_widths = [0.10, 0.20, 0.40, 0.30]
    col_labels = ["#", "代號", "股票名稱", "倒數天數"]
    col_aligns = ['center', 'center', 'left', 'center']

    table_left = 0.005
    table_right = 0.995
    table_w = table_right - table_left

    x_starts = []
    x_widths = []
    acc = table_left
    for w in col_widths:
        scaled_w = w * table_w
        x_starts.append(acc)
        x_widths.append(scaled_w)
        acc += scaled_w

    header_top = top_y

    ax.add_patch(patches.Rectangle(
        (0.005, header_top - header_h), 0.99, header_h,
        linewidth=0, facecolor=theme['header'],
        transform=ax.transAxes, clip_on=False, zorder=1
    ))
    ax.plot([0.005, 0.995], [header_top, header_top],
            color=theme['accent'], linewidth=2.5,
            transform=ax.transAxes, clip_on=False, zorder=2)

    for col_i, (xst, w, label, align) in enumerate(zip(x_starts, x_widths, col_labels, col_aligns)):
        text_x = xst + w/2 if align == 'center' else xst + 0.015
        ax.text(text_x, header_top - header_h/2, clean_display_text(label),
                transform=ax.transAxes, ha=align, va='center',
                fontsize=20, fontweight='bold',
                fontproperties=FONT_BOLD, color=TEXT_HEADER, zorder=3)

    for row_i, row in enumerate(data):
        code, name, days = clean_display_text(row['code']), clean_display_text(row['name'], fullwidth_ascii=True), row['days']
        rank_num = row_i + 1
        y_top = header_top - header_h - row_i * row_h
        bg_color = BG_ROW_ODD if row_i % 2 == 0 else BG_ROW_EVEN

        # 依訊號狀態決定股號股名的顏色
        name_color = get_signal_color(code, signal_map)

        ax.add_patch(patches.Rectangle(
            (0.005, y_top - row_h), 0.99, row_h,
            linewidth=0, facecolor=bg_color,
            transform=ax.transAxes, clip_on=False, zorder=1
        ))
        ax.plot([0.014, 0.995], [y_top - row_h, y_top - row_h],
                color=BORDER_DARK, linewidth=0.6,
                transform=ax.transAxes, clip_on=False, zorder=2)
        ax.add_patch(patches.Rectangle(
            (x_starts[0], y_top - row_h), col_widths[0], row_h,
            linewidth=0, facecolor=BG_RANK,
            transform=ax.transAxes, clip_on=False, zorder=1
        ))

        if rank_num == 1:   rank_color, rank_fw = GOLD, 'bold'
        elif rank_num == 2: rank_color, rank_fw = SILVER, 'bold'
        elif rank_num == 3: rank_color, rank_fw = BRONZE, 'bold'
        else:               rank_color, rank_fw = TEXT_MUTED, 'normal'

        ax.text(x_starts[0] + x_widths[0]/2, y_top - row_h/2, f"{rank_num:02d}",
                transform=ax.transAxes, ha='center', va='center',
                fontsize=18, fontweight=rank_fw,
                fontproperties=FONT_BOLD, color=rank_color, zorder=3)
        ax.text(x_starts[1] + x_widths[1]/2, y_top - row_h/2, code,
                transform=ax.transAxes, ha='center', va='center',
                fontsize=20, fontweight='bold',
                fontproperties=FONT_BOLD, color=name_color, zorder=3)
        ax.text(x_starts[2] + 0.015, y_top - row_h/2, name,
                transform=ax.transAxes, ha='left', va='center',
                fontsize=19, fontproperties=FONT_PROP,
                color=name_color, zorder=3)

        bg_clr, fg_clr = get_days_style(days)
        capsule_w = col_widths[3] * 0.6
        capsule_h = row_h * 0.62
        capsule_x = x_starts[3] + (col_widths[3] - capsule_w) / 2
        capsule_y = y_top - row_h/2 - capsule_h/2

        ax.add_patch(patches.FancyBboxPatch(
            (capsule_x, capsule_y), capsule_w, capsule_h,
            boxstyle="round,pad=0.002,rounding_size=0.014",
            linewidth=0, facecolor=bg_clr,
            transform=ax.transAxes, clip_on=False, zorder=2
        ))

        label_text = clean_display_text("明日處置" if days == 1 else f"剩 {days} 天")
        ax.text(x_starts[3] + col_widths[3]/2, y_top - row_h/2, label_text,
                transform=ax.transAxes, ha='center', va='center',
                fontsize=18, fontweight='bold',
                fontproperties=FONT_BOLD, color=fg_clr, zorder=3)

    draw_watermark(fig)
    return save_figure_to_buffer(fig)


def draw_releasing_image(data, signal_map=None):
    """即將出關 - 單欄詳細含績效

    signal_map: {代號: 訊號狀態} dict，用來決定股號股名的顏色（同 draw_entering_image）。
    """
    theme = THEME_RELEASING
    n = len(data)
    fig_w = 16.2
    fig_h = calc_dynamic_fig_h(n, base_h=7.4, per_row_h=0.46, min_h=10.2, max_h=23.5)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), facecolor=BG_MAIN)
    fig.subplots_adjust(left=UNIFIED_SUBPLOT_LEFT, right=UNIFIED_SUBPLOT_RIGHT,
                        top=UNIFIED_SUBPLOT_TOP, bottom=UNIFIED_SUBPLOT_BOTTOM)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.set_axis_off()

    draw_topbar(fig, theme, n)

    header_h = calc_header_h(fig_h)
    total_h = TABLE_TOTAL_H
    row_h = (total_h - header_h) / max(n, 1)
    top_y = TABLE_TOP_Y

    draw_table_frame(ax, theme, theme['subtitle_text'], top_y, total_h)

    # [調整重點 3] 後 5 欄（倒數天數/狀態/處置前/處置中/出關日）整體往右移 +5%：
    #   做法：擴大前面 3 欄（代號/名稱/現價）寬度，把後面所有欄位推到右邊。
    #   後 5 欄『欄寬本身保持不變』，內容大小（剩X天膠囊、狀態、%、日期）不會被擠壓。
    #
    #   舊欄寬: [0.045, 0.090, 0.165, 0.085, 0.120, 0.175, 0.100, 0.100, 0.120]
    #   新欄寬: [0.045, 0.105, 0.180, 0.100, 0.120, 0.175, 0.100, 0.100, 0.075]
    #
    #   變化說明：
    #     代號:   0.090 → 0.105 (+0.015)  ← 加寬
    #     名稱:   0.165 → 0.180 (+0.015)  ← 加寬
    #     現價:   0.085 → 0.100 (+0.015)  ← 加寬
    #     倒數天數: 0.120 → 0.120 (不變)   起始 +4.5%
    #     狀態:   0.175 → 0.175 (不變)   起始 +4.5%
    #     處置前: 0.100 → 0.100 (不變)   起始 +4.5%
    #     處置中: 0.100 → 0.100 (不變)   起始 +4.5%
    #     出關日: 0.120 → 0.075 (-0.045) ← 縮窄補回總寬
    #             (理由：標題改成右對齊後，'出關日' 3字 + 'mm/dd' 5字符
    #              在 fontsize 16~18 下約需 0.65 inch，0.075×16.2=1.215 inch 綽綽有餘)
    col_widths = [0.045, 0.105, 0.180, 0.100, 0.120, 0.175, 0.100, 0.100, 0.075]
    col_labels = ["#", "代號", "名稱", "現價", "倒數天數", "狀態", "處置前", "處置中", "出關日"]
    # [調整重點 4] 出關日 header 從 'center' 改成 'right'，跟資料的右對齊一致
    col_aligns = ['center', 'center', 'left', 'right', 'center', 'center', 'center', 'center', 'right']

    table_left = 0.005
    table_right = 0.995
    table_w = table_right - table_left

    x_starts = []
    x_widths = []
    acc = table_left
    for w in col_widths:
        scaled_w = w * table_w
        x_starts.append(acc)
        x_widths.append(scaled_w)
        acc += scaled_w

    header_top = top_y

    ax.add_patch(patches.Rectangle(
        (0.005, header_top - header_h), 0.99, header_h,
        linewidth=0, facecolor=theme['header'],
        transform=ax.transAxes, clip_on=False, zorder=1
    ))
    ax.plot([0.005, 0.995], [header_top, header_top],
            color=theme['accent'], linewidth=2.5,
            transform=ax.transAxes, clip_on=False, zorder=2)

    for col_i, (xst, w, label, align) in enumerate(zip(x_starts, x_widths, col_labels, col_aligns)):
        if align == 'center':
            text_x = xst + w/2
        elif align == 'right':
            text_x = xst + w - 0.010
        else:
            text_x = xst + 0.012
        ax.text(text_x, header_top - header_h/2, clean_display_text(label),
                transform=ax.transAxes, ha=align, va='center',
                fontsize=16, fontweight='bold',
                fontproperties=FONT_BOLD, color=TEXT_HEADER, zorder=3)

    for row_i, row in enumerate(data):
        code, name, price, days, date = clean_display_text(row['code']), clean_display_text(row['name'], fullwidth_ascii=True), clean_display_text(str(row.get('price', '--'))), row['days'], clean_display_text(row['date'])
        icon, status_text = row['icon'], clean_display_text(row['status_text'])
        pre_pct, in_pct = row['pre_pct'], row['in_pct']
        rank_num = row_i + 1
        y_top = header_top - header_h - row_i * row_h
        bg_color = BG_ROW_ODD if row_i % 2 == 0 else BG_ROW_EVEN

        # 依訊號狀態決定股號股名的顏色
        name_color = get_signal_color(code, signal_map)

        ax.add_patch(patches.Rectangle(
            (0.005, y_top - row_h), 0.99, row_h,
            linewidth=0, facecolor=bg_color,
            transform=ax.transAxes, clip_on=False, zorder=1
        ))
        ax.plot([0.014, 0.995], [y_top - row_h, y_top - row_h],
                color=BORDER_DARK, linewidth=0.6,
                transform=ax.transAxes, clip_on=False, zorder=2)
        ax.add_patch(patches.Rectangle(
            (x_starts[0], y_top - row_h), x_widths[0], row_h,
            linewidth=0, facecolor=BG_RANK,
            transform=ax.transAxes, clip_on=False, zorder=1
        ))

        if rank_num == 1:   rank_color, rank_fw = GOLD, 'bold'
        elif rank_num == 2: rank_color, rank_fw = SILVER, 'bold'
        elif rank_num == 3: rank_color, rank_fw = BRONZE, 'bold'
        else:               rank_color, rank_fw = TEXT_MUTED, 'normal'

        ax.text(x_starts[0] + x_widths[0]/2, y_top - row_h/2, f"{rank_num:02d}",
                transform=ax.transAxes, ha='center', va='center',
                fontsize=16, fontweight=rank_fw,
                fontproperties=FONT_BOLD, color=rank_color, zorder=3)
        ax.text(x_starts[1] + x_widths[1]/2, y_top - row_h/2, code,
                transform=ax.transAxes, ha='center', va='center',
                fontsize=20, fontweight='bold',
                fontproperties=FONT_BOLD, color=name_color, zorder=3)
        ax.text(x_starts[2] + 0.012, y_top - row_h/2, name,
                transform=ax.transAxes, ha='left', va='center',
                fontsize=19, fontproperties=FONT_PROP,
                color=name_color, zorder=3)
        ax.text(x_starts[3] + x_widths[3] - 0.008, y_top - row_h/2, price,
                transform=ax.transAxes, ha='right', va='center',
                fontsize=16, fontweight='bold',
                fontproperties=FONT_BOLD, color=TEXT_PRICE, zorder=3)

        bg_clr, fg_clr = get_days_style(days)
        capsule_w = x_widths[4] * 0.80
        capsule_h = row_h * 0.62
        capsule_x = x_starts[4] + (x_widths[4] - capsule_w) / 2
        capsule_y = y_top - row_h/2 - capsule_h/2

        ax.add_patch(patches.FancyBboxPatch(
            (capsule_x, capsule_y), capsule_w, capsule_h,
            boxstyle="round,pad=0.002,rounding_size=0.012",
            linewidth=0, facecolor=bg_clr,
            transform=ax.transAxes, clip_on=False, zorder=2
        ))
        ax.text(x_starts[4] + x_widths[4]/2, y_top - row_h/2, clean_display_text(f"剩 {days} 天"),
                transform=ax.transAxes, ha='center', va='center',
                fontsize=18, fontweight='bold',
                fontproperties=FONT_BOLD, color=fg_clr, zorder=3)

        if "妖股" in status_text:    st_color = '#D69E2E'
        elif "強勢" in status_text:  st_color = '#E35D6A'
        elif "人去樓空" in status_text: st_color = '#9B59B6'
        elif "走勢疲軟" in status_text: st_color = '#2F9E72'
        else:                         st_color = TEXT_MUTED

        status_center_x = x_starts[5] + x_widths[5]/2
        status_y = y_top - row_h/2
        status_icon_x = x_starts[5] + x_widths[5] * 0.30
        status_text_x = x_starts[5] + x_widths[5] * 0.55
        emoji_ok = draw_emoji_image(ax, icon, status_icon_x, status_y,
                                    fontsize=15, transform=ax.transAxes,
                                    zorder=4, fallback_color=st_color)
        if emoji_ok:
            ax.text(status_text_x, status_y, status_text,
                    transform=ax.transAxes, ha='center', va='center',
                    fontsize=18, fontweight='bold',
                    fontproperties=FONT_BOLD, color=st_color, zorder=3)
        else:
            icon_fallback = EMOJI_FALLBACK_SYMBOLS.get(icon, icon)
            ax.text(status_center_x, status_y,
                    f"{icon_fallback} {status_text}",
                    transform=ax.transAxes, ha='center', va='center',
                    fontsize=18, fontweight='bold',
                    fontproperties=FONT_BOLD, color=st_color, zorder=3)

        ax.text(x_starts[6] + x_widths[6]/2, y_top - row_h/2, f"{pre_pct}%",
                transform=ax.transAxes, ha='center', va='center',
                fontsize=18, fontweight='bold',
                fontproperties=FONT_BOLD, color=get_pct_color(pre_pct), zorder=3)
        ax.text(x_starts[7] + x_widths[7]/2, y_top - row_h/2, f"{in_pct}%",
                transform=ax.transAxes, ha='center', va='center',
                fontsize=18, fontweight='bold',
                fontproperties=FONT_BOLD, color=get_pct_color(in_pct), zorder=3)
        ax.text(x_starts[8] + x_widths[8] - 0.010, y_top - row_h/2, date,
                transform=ax.transAxes, ha='right', va='center',
                fontsize=18, fontproperties=FONT_PROP,
                color=TEXT_MAIN, zorder=3)

    draw_signal_legend(fig)
    draw_watermark(fig)
    return save_figure_to_buffer(fig)


def draw_injail_image(data, signal_map=None):
    """處置中 - 動態欄數版

    signal_map: {代號: 訊號狀態} dict，用來決定股號股名的顏色（同 draw_entering_image）。
    """
    theme = THEME_INJAIL
    n = len(data)
    n_cols = get_injail_n_cols(n)
    rows_per_col = max(1, (n + n_cols - 1) // n_cols)
    fig_h = calc_dynamic_fig_h(rows_per_col, base_h=6.1, per_row_h=0.39, min_h=7.8, max_h=16.8)

    fig, ax = plt.subplots(figsize=(COMMON_FIG_WIDTH, fig_h), facecolor=BG_MAIN)
    fig.subplots_adjust(left=UNIFIED_SUBPLOT_LEFT, right=UNIFIED_SUBPLOT_RIGHT,
                        top=UNIFIED_SUBPLOT_TOP, bottom=UNIFIED_SUBPLOT_BOTTOM)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.set_axis_off()

    draw_topbar(fig, theme, n)

    header_h = calc_header_h(fig_h)
    total_h = TABLE_TOTAL_H
    row_h = (total_h - header_h) / rows_per_col
    top_y = TABLE_TOP_Y

    draw_table_frame(ax, theme, theme['subtitle_text'], top_y, total_h)

    header_top = top_y

    ax.add_patch(patches.Rectangle(
        (0.005, header_top - header_h), 0.99, header_h,
        linewidth=0, facecolor=theme['header'],
        transform=ax.transAxes, clip_on=False, zorder=1
    ))
    ax.plot([0.005, 0.995], [header_top, header_top],
            color=theme['accent'], linewidth=2.5,
            transform=ax.transAxes, clip_on=False, zorder=2)

    col_total_w = 0.99
    col_unit_w = col_total_w / n_cols
    col_xs = [0.005 + i * col_unit_w for i in range(n_cols)]
    sub_col_widths_ratio = [0.08, 0.16, 0.24, 0.14, 0.38]
    sub_labels = ["#", "代號", "名稱", "現價", "處置期間"]
    sub_aligns = ['center', 'center', 'left', 'right', 'center']

    for col_idx in range(n_cols):
        col_x_start = col_xs[col_idx]
        sub_x_starts = []
        acc = 0
        for r in sub_col_widths_ratio:
            sub_x_starts.append(col_x_start + acc * col_unit_w); acc += r
        sub_x_widths = [r * col_unit_w for r in sub_col_widths_ratio]

        for sub_i, (xst, w, label, align) in enumerate(zip(sub_x_starts, sub_x_widths, sub_labels, sub_aligns)):
            if align == 'center':
                text_x = xst + w/2
            elif align == 'right':
                text_x = xst + w - 0.008
            else:
                text_x = xst + 0.008
            ax.text(text_x, header_top - header_h/2, clean_display_text(label),
                    transform=ax.transAxes, ha=align, va='center',
                    fontsize=15, fontweight='bold',
                    fontproperties=FONT_BOLD, color=TEXT_HEADER, zorder=3)

        if col_idx < n_cols - 1:
            divider_x = col_x_start + col_unit_w
            ax.plot([divider_x, divider_x],
                    [top_y - total_h - 0.01, header_top - header_h],
                    color=BORDER_MID, linewidth=0.8,
                    transform=ax.transAxes, clip_on=False, zorder=2)

    for idx, row in enumerate(data):
        code, name, price, period = clean_display_text(row['code']), clean_display_text(row['name'], fullwidth_ascii=True), clean_display_text(str(row.get('price', '--'))), clean_display_text(row['period'])
        col_idx = idx // rows_per_col
        row_idx = idx % rows_per_col
        if col_idx >= n_cols:
            break

        # 依訊號狀態決定股號股名的顏色
        name_color = get_signal_color(code, signal_map)

        col_x_start = col_xs[col_idx]
        sub_x_starts = []
        acc = 0
        for r in sub_col_widths_ratio:
            sub_x_starts.append(col_x_start + acc * col_unit_w); acc += r
        sub_x_widths = [r * col_unit_w for r in sub_col_widths_ratio]

        global_idx = idx + 1
        y_top = header_top - header_h - row_idx * row_h
        bg_color = BG_ROW_ODD if row_idx % 2 == 0 else BG_ROW_EVEN

        ax.add_patch(patches.Rectangle(
            (col_x_start, y_top - row_h), col_unit_w, row_h,
            linewidth=0, facecolor=bg_color,
            transform=ax.transAxes, clip_on=False, zorder=1
        ))
        ax.plot([col_x_start, col_x_start + col_unit_w], [y_top - row_h, y_top - row_h],
                color=BORDER_DARK, linewidth=0.6,
                transform=ax.transAxes, clip_on=False, zorder=2)
        ax.add_patch(patches.Rectangle(
            (sub_x_starts[0], y_top - row_h), sub_x_widths[0], row_h,
            linewidth=0, facecolor=BG_RANK,
            transform=ax.transAxes, clip_on=False, zorder=1
        ))

        if global_idx == 1:   rank_color, rank_fw = GOLD, 'bold'
        elif global_idx == 2: rank_color, rank_fw = SILVER, 'bold'
        elif global_idx == 3: rank_color, rank_fw = BRONZE, 'bold'
        else:                 rank_color, rank_fw = TEXT_MUTED, 'normal'

        ax.text(sub_x_starts[0] + sub_x_widths[0]/2, y_top - row_h/2, f"{global_idx:02d}",
                transform=ax.transAxes, ha='center', va='center',
                fontsize=14, fontweight=rank_fw,
                fontproperties=FONT_BOLD, color=rank_color, zorder=3)
        ax.text(sub_x_starts[1] + sub_x_widths[1]/2, y_top - row_h/2, code,
                transform=ax.transAxes, ha='center', va='center',
                fontsize=15, fontweight='bold',
                fontproperties=FONT_BOLD, color=name_color, zorder=3)
        ax.text(sub_x_starts[2] + 0.008, y_top - row_h/2, name,
                transform=ax.transAxes, ha='left', va='center',
                fontsize=14, fontproperties=FONT_PROP,
                color=name_color, zorder=3)
        ax.text(sub_x_starts[3] + sub_x_widths[3] - 0.008, y_top - row_h/2, price,
                transform=ax.transAxes, ha='right', va='center',
                fontsize=12.5, fontweight='bold',
                fontproperties=FONT_BOLD, color=TEXT_PRICE, zorder=3)
        ax.text(sub_x_starts[4] + sub_x_widths[4]/2, y_top - row_h/2, period,
                transform=ax.transAxes, ha='center', va='center',
                fontsize=13, fontproperties=FONT_PROP,
                color=TEXT_MAIN, zorder=3)

    draw_signal_legend(fig)
    draw_watermark(fig)
    return save_figure_to_buffer(fig)


# ============================
# 🚀 主程式
# ============================
def main():
    sh = connect_google_sheets()
    if not sh: return

    # 預先載入「處置股技術追蹤」的訊號狀態與目前價，三張圖都會用到
    signal_map = load_signal_status_map(sh)
    price_map = load_current_price_map(sh)

    rel = check_releasing_stocks(sh, price_map=price_map)
    rel_codes = {x['code'] for x in rel}
    stats = check_status_split(sh, rel_codes, price_map=price_map)

    # 1. 瀕臨處置
    if stats['entering']:
        print(f"📊 產生瀕臨處置圖片 ({len(stats['entering'])} 檔)...")
        try:
            buf = draw_entering_image(stats['entering'], signal_map=signal_map)
            send_discord_image(buf)
            time.sleep(2)
        except Exception as e:
            print(f"❌ 瀕臨處置圖片產生失敗: {e}")

    # 2. 即將出關
    if rel:
        print(f"📊 產生即將出關圖片 ({len(rel)} 檔)...")
        try:
            buf = draw_releasing_image(rel, signal_map=signal_map)
            send_discord_image(buf)
            time.sleep(2)
        except Exception as e:
            print(f"❌ 即將出關圖片產生失敗: {e}")

    # 3. 處置中
    if stats['in_jail']:
        print(f"📊 產生處置中圖片 ({len(stats['in_jail'])} 檔)...")
        try:
            buf = draw_injail_image(stats['in_jail'], signal_map=signal_map)
            send_discord_image(buf)
            time.sleep(2)
        except Exception as e:
            print(f"❌ 處置中圖片產生失敗: {e}")

    print("✅ 完成")


if __name__ == "__main__":
    main()
