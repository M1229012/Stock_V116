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

if not DISCORD_WEBHOOK_URL:
    print("=" * 60)
    print("❌ 嚴重錯誤: 環境變數 DISCORD_WEBHOOK_URL_TEST 未設定或為空")
    print("=" * 60)
else:
    print(f"✅ DISCORD_WEBHOOK_URL_TEST 已載入 (長度: {len(DISCORD_WEBHOOK_URL)} 字元)")

JAIL_ENTER_THRESHOLD = 3   
JAIL_EXIT_THRESHOLD = 5    
TECH_TRACK_SHEET_NAME = "處置股技術追蹤"

def format_display_price(value):
    try:
        if value is None: return "--"
        s = str(value).replace(",", "").strip()
        if s == "" or s.lower() in {"nan", "none"}: return "--"
        num = float(s)
        if abs(num - round(num)) < 1e-9: return f"{int(round(num))}.0"
        return f"{num:.1f}"
    except Exception:
        s = str(value).strip()
        return s if s else "--"

# ============================
# 🎨 圖片風格與字型設定
# ============================
CJK_FONT_PATH = None
CJK_BOLD_FONT_PATH = None
EMOJI_IMAGE_CACHE = {}
FONT_DOWNLOAD_DIR = os.path.join(os.getenv("RUNNER_TEMP", "/tmp"), "stock_monitor_fonts")

def _download_font_if_needed(url, filename):
    try:
        os.makedirs(FONT_DOWNLOAD_DIR, exist_ok=True)
        font_path = os.path.join(FONT_DOWNLOAD_DIR, filename)
        if os.path.exists(font_path) and os.path.getsize(font_path) > 1024 * 1024:
            return font_path
        response = requests.get(url, timeout=20)
        if response.status_code == 200 and response.content:
            with open(font_path, "wb") as f: f.write(response.content)
            if os.path.getsize(font_path) > 1024 * 1024: return font_path
    except Exception: pass
    return None

def load_chinese_font():
    global CJK_FONT_PATH
    search_paths = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJKtc-Regular.otf",
        "/usr/share/fonts/noto-cjk/NotoSansCJKtc-Regular.otf",
        "/usr/local/share/fonts/NotoSansCJKtc-Regular.otf",
        "/usr/local/share/fonts/NotoSansTC-Regular.otf",
        "C:/Windows/Fonts/msjh.ttc", "C:/Windows/Fonts/mingliu.ttc",
        "/System/Library/Fonts/PingFang.ttc", "/System/Library/Fonts/STHeiti Light.ttc",
    ]
    for path in search_paths:
        if os.path.exists(path):
            font_manager.fontManager.addfont(path)
            CJK_FONT_PATH = path
            return font_manager.FontProperties(fname=path)
    fallback_path = _download_font_if_needed("https://raw.githubusercontent.com/googlefonts/noto-cjk/main/Sans/OTF/TraditionalChinese/NotoSansCJKtc-Regular.otf", "NotoSansCJKtc-Regular.otf")
    if fallback_path and os.path.exists(fallback_path):
        font_manager.fontManager.addfont(fallback_path)
        CJK_FONT_PATH = fallback_path
        return font_manager.FontProperties(fname=fallback_path)
    return font_manager.FontProperties(family="DejaVu Sans")

def load_chinese_bold_font():
    global CJK_BOLD_FONT_PATH
    search_paths = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Black.ttc",
        "/usr/share/fonts/noto-cjk/NotoSansCJKtc-Bold.otf",
        "/usr/local/share/fonts/NotoSansCJKtc-Bold.otf",
        "/usr/local/share/fonts/NotoSansTC-Bold.otf",
        "C:/Windows/Fonts/msjhbd.ttc", "/System/Library/Fonts/PingFang.ttc",
    ]
    for path in search_paths:
        if os.path.exists(path):
            font_manager.fontManager.addfont(path)
            CJK_BOLD_FONT_PATH = path
            return font_manager.FontProperties(fname=path)
    fallback_path = _download_font_if_needed("https://raw.githubusercontent.com/googlefonts/noto-cjk/main/Sans/OTF/TraditionalChinese/NotoSansCJKtc-Bold.otf", "NotoSansCJKtc-Bold.otf")
    if fallback_path and os.path.exists(fallback_path):
        font_manager.fontManager.addfont(fallback_path)
        CJK_BOLD_FONT_PATH = fallback_path
        return font_manager.FontProperties(fname=fallback_path)
    CJK_BOLD_FONT_PATH = CJK_FONT_PATH
    return load_chinese_font()

try: cache_dir = matplotlib.get_cachedir()
except: pass

FONT_PROP = load_chinese_font()
FONT_BOLD = load_chinese_bold_font()

try:
    sans_list = []
    for font_path in [CJK_FONT_PATH, CJK_BOLD_FONT_PATH]:
        if font_path:
            try: sans_list.append(font_manager.FontProperties(fname=font_path).get_name())
            except: pass
    sans_list.extend(['Noto Sans CJK TC', 'Noto Sans CJK JP', 'Noto Sans CJK SC', 'Microsoft JhengHei', 'PingFang TC', 'Arial Unicode MS', 'DejaVu Sans'])
    sans_list = list(dict.fromkeys(sans_list))
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = sans_list
    plt.rcParams['axes.unicode_minus'] = False
except Exception: pass

# ============================
# 🧹 文字清洗工具與 Emoji
# ============================
_ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u202a-\u202e\ufeff]")

def to_fullwidth(s):
    res = []
    for char in str(s):
        code = ord(char)
        if 0x21 <= code <= 0x7E: res.append(chr(code + 0xFEE0))
        elif code == 0x20: res.append(chr(0x3000))
        else: res.append(char)
    return "".join(res)

def clean_cell(s) -> str:
    s = "" if s is None else str(s)
    s = s.replace("\xa0", " ")
    s = _ZERO_WIDTH_RE.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def fix_common_cjk_mojibake(s) -> str:
    s = clean_cell(s)
    replacements = {"卅卅": "碁"}
    for bad, good in replacements.items(): s = s.replace(bad, good)
    return s

def clean_display_text(s, fullwidth_ascii=False) -> str:
    s = fix_common_cjk_mojibake(s)
    if fullwidth_ascii: s = to_fullwidth(s)
    return s

EMOJI_FALLBACK_SYMBOLS = {
    "🚨": "!", "🔓": "OPEN", "⛓️": "LOCK", "👑": "★", 
    "🔥": "▲", "💀": "▼", "📉": "↓", "🧊": "◆", "❓": "?",
}

def _twemoji_codepoints_keep_vs16(emoji_text):
    return "-".join(f"{ord(ch):x}" for ch in emoji_text)
def _twemoji_codepoints(emoji_text):
    return "-".join(f"{ord(ch):x}" for ch in emoji_text if ord(ch) != 0xfe0f)

def get_twemoji_image(emoji_text):
    if emoji_text in EMOJI_IMAGE_CACHE: return EMOJI_IMAGE_CACHE[emoji_text]
    candidates = []
    keep_vs16 = _twemoji_codepoints_keep_vs16(emoji_text)
    no_vs16 = _twemoji_codepoints(emoji_text)
    if keep_vs16: candidates.append(keep_vs16)
    if no_vs16 and no_vs16 not in candidates: candidates.append(no_vs16)
    base_urls = ["https://raw.githubusercontent.com/jdecked/twemoji/main/assets/72x72", "https://raw.githubusercontent.com/twitter/twemoji/master/assets/72x72"]
    for code in candidates:
        for base_url in base_urls:
            try:
                response = requests.get(f"{base_url}/{code}.png", timeout=2.5)
                if response.status_code == 200 and response.content:
                    img = Image.open(BytesIO(response.content)).convert("RGBA")
                    EMOJI_IMAGE_CACHE[emoji_text] = img
                    return img
            except: continue
    EMOJI_IMAGE_CACHE[emoji_text] = None
    return None

def draw_emoji_image(ax, emoji_text, x, y, fontsize=18, transform=None, zorder=5, fallback_color="#4A5565"):
    transform = transform or ax.transData
    img = get_twemoji_image(emoji_text)
    if img is not None:
        imagebox = OffsetImage(img, zoom=max(0.18, fontsize / 42.0), resample=True)
        ab = AnnotationBbox(imagebox, (x, y), xycoords=transform, frameon=False, pad=0, box_alignment=(0.5, 0.5), zorder=zorder)
        ab.set_clip_on(False)
        ax.add_artist(ab)
        return True
    fallback = EMOJI_FALLBACK_SYMBOLS.get(emoji_text, "")
    if fallback:
        ax.text(x, y, fallback, transform=transform, ha='center', va='center', fontsize=fontsize, fontweight='bold', fontproperties=FONT_BOLD, color=fallback_color, zorder=zorder)
    return False

# ---- 共用顏色 ----
BG_MAIN     = '#F5F7FA'
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

SIGNAL_COLOR_RETEST   = '#C2410C'
SIGNAL_COLOR_BREAKOUT = '#1D4ED8'

THEME_ENTERING  = {'accent': '#E85D6A', 'header': '#FCECEF', 'title': '處置倒數 瀕臨處置監控', 'title_icon': '🚨', 'subtitle_text': '瀕臨處置 (3日內)'}
THEME_RELEASING = {'accent': '#16B27A', 'header': '#EAF7F1', 'title': '越關越大尾 即將出關監控', 'title_icon': '🔓', 'subtitle_text': '即將出關 (5日內)'}
THEME_INJAIL    = {'accent': '#B06FD3', 'header': '#F5ECFB', 'title': '還能噴嗎 正在處置監控', 'title_icon': '⛓️', 'subtitle_text': '處置中股票名單'}


# ============================
# 🛠️ 工具與 API 抓取函式
# ============================
def connect_google_sheets():
    if not os.path.exists(SERVICE_KEY_FILE): return None
    max_retries = 5
    for attempt in range(max_retries):
        try:
            gc = gspread.service_account(filename=SERVICE_KEY_FILE)
            sh = gc.open(SHEET_NAME)
            return sh
        except gspread.exceptions.APIError as e:
            msg = str(e)
            if any(code in msg for code in ['429', '500', '502', '503', '504']) and attempt < max_retries - 1:
                time.sleep((2 ** attempt) + random.uniform(0, 1))
                continue
            return None
        except Exception: return None
    return None

def send_discord_image(image_buf, content_text=""):
    if not DISCORD_WEBHOOK_URL: return
    try:
        files = {"file": ("chart.png", image_buf, "image/png")}
        data = {"username": "台股處置監控機器人", "avatar_url": "https://cdn-icons-png.flaticon.com/512/2502/2502697.png", "content": content_text}
        requests.post(DISCORD_WEBHOOK_URL, data=data, files=files)
    except Exception: pass

def parse_roc_date(date_str):
    s = str(date_str).strip()
    match = re.match(r'^(\d{2,3})[/-](\d{1,2})[/-](\d{1,2})$', s)
    if match:
        y, m, d = map(int, match.groups())
        return datetime(y + 1911 if y < 1911 else y, m, d)
    for fmt in ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]:
        try: return datetime.strptime(s, fmt)
        except: continue
    return None

def code_sort_key(code):
    s = str(code).replace("'", "").strip()
    return int(s) if s.isdigit() else 999999

def build_period_detail(period_str):
    period = str(period_str).strip()
    dates = re.split(r'[~-～]', period)
    if len(dates) >= 2:
        s_date, e_date = parse_roc_date(dates[0]), parse_roc_date(dates[1])
        if s_date and e_date:
            return {
                'period': f"{s_date.strftime('%m/%d')}-{e_date.strftime('%m/%d')}",
                'sort_start': s_date,
                'sort_end': e_date,
            }
    return {'period': period if period else '日期未知', 'sort_start': None, 'sort_end': None}

def injail_sort_key(item):
    sort_end = item.get('sort_end')
    if sort_end is None:
        detail = build_period_detail(item.get('period', ''))
        sort_end = detail.get('sort_end')
    if sort_end is None:
        sort_end = datetime.max
    return (sort_end, code_sort_key(item.get('code', '')))

def get_merged_jail_period_details(sh):
    jail_map = {}
    tw_now = datetime.utcnow() + timedelta(hours=8)
    today = datetime(tw_now.year, tw_now.month, tw_now.day)
    try:
        ws = sh.worksheet("處置股90日明細")
        for row in ws.get_all_records():
            code = str(row.get('代號', '')).replace("'", "").strip()
            period = str(row.get('處置期間', '')).strip()
            if not code or not period: continue
            detail = build_period_detail(period)
            s_date, e_date = detail.get('sort_start'), detail.get('sort_end')
            if s_date and e_date and e_date >= today:
                if code not in jail_map:
                    jail_map[code] = {'start': s_date, 'end': e_date}
                else:
                    jail_map[code]['start'] = min(jail_map[code]['start'], s_date)
                    jail_map[code]['end'] = max(jail_map[code]['end'], e_date)
    except: return {}
    return {
        c: {
            'period': f"{d['start'].strftime('%m/%d')}-{d['end'].strftime('%m/%d')}",
            'sort_start': d['start'],
            'sort_end': d['end'],
        }
        for c, d in jail_map.items()
    }

def load_signal_status_map(sh):
    tech_map = {}
    try:
        ws = sh.worksheet(TECH_TRACK_SHEET_NAME)
        for row in ws.get_all_records():
            code = str(row.get('代號', '')).replace("'", "").strip()
            if not code: continue
            calc_date = str(row.get('計算日期', '')).strip()
            if code not in tech_map or calc_date >= tech_map[code]['date']:
                tech_map[code] = {'status': str(row.get('訊號狀態', '')).strip(), 'price': format_display_price(row.get('目前價', '')), 'date': calc_date}
    except: pass
    return {c: v.get('status', '') for c, v in tech_map.items()}

def load_current_price_map(sh):
    tech_map = {}
    try:
        ws = sh.worksheet(TECH_TRACK_SHEET_NAME)
        for row in ws.get_all_records():
            code = str(row.get('代號', '')).replace("'", "").strip()
            if not code: continue
            calc_date = str(row.get('計算日期', '')).strip()
            if code not in tech_map or calc_date >= tech_map[code]['date']:
                tech_map[code] = {'status': str(row.get('訊號狀態', '')).strip(), 'price': format_display_price(row.get('目前價', '')), 'date': calc_date}
    except: pass
    return {c: v.get('price', '--') for c, v in tech_map.items()}

def get_signal_color(code, signal_map):
    status = signal_map.get(code, '') if signal_map else ''
    if status == "回測後轉強": return SIGNAL_COLOR_BREAKOUT
    if status == "目前回測月線": return SIGNAL_COLOR_RETEST
    return TEXT_MAIN

def get_price_rank_info(code, period_str, market):
    try:
        dates = re.split(r'[~-～]', str(period_str))
        start_date = parse_roc_date(dates[0])
        if not start_date: return "❓ 未知", "日期錯", "+0.0", "+0.0"
        fetch_start = start_date - timedelta(days=60)
        end_date = datetime.now() + timedelta(days=1)
        suffix = ".TWO" if any(x in str(market) for x in ["上櫃", "TPEx"]) else ".TW"
        df = yf.Ticker(f"{code}{suffix}").history(start=fetch_start.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), auto_adjust=True)
        if not df.empty: df = df.ffill() 
        if df.empty or len(df) < 2: return "❓ 未知", "無股價", "+0.0", "+0.0"
        df.index = df.index.tz_localize(None)
        df_in_jail = df[df.index >= pd.Timestamp(start_date)]
        mask_before = df.index < pd.Timestamp(start_date)
        if not mask_before.any(): pre_pct = 0.0
        else:
            jail_base_p = df[mask_before]['Close'].iloc[-1]
            jail_days_count = len(df_in_jail) if not df_in_jail.empty else 1
            loc_idx = df.index.get_loc(df[mask_before].index[-1])
            target_idx = max(0, loc_idx - jail_days_count + 1)
            pre_entry = df.iloc[target_idx]['Open']
            pre_pct = ((jail_base_p - pre_entry) / pre_entry) * 100
        if df_in_jail.empty: in_pct = 0.0
        else:
            jail_start_entry = df_in_jail['Open'].iloc[0]
            curr_p = df_in_jail['Close'].iloc[-1]
            in_pct = ((curr_p - jail_start_entry) / jail_start_entry) * 100
        if in_pct > 15:    icon, status_text = "👑", "妖股誕生"
        elif in_pct > 5:   icon, status_text = "🔥", "強勢突圍"
        elif in_pct < -15: icon, status_text = "💀", "人去樓空"
        elif in_pct < -5:  icon, status_text = "📉", "走勢疲軟"
        else:              icon, status_text = "🧊", "多空膠著"
        # 格式化百分比確保寬度一致
        return icon, clean_display_text(status_text), f"{'+' if pre_pct >= 0 else ''}{pre_pct:.1f}%", f"{'+' if in_pct >= 0 else ''}{in_pct:.1f}%"
    except: return "❓ 未知", "數據計算中", "+0.0%", "+0.0%"

def check_status_split(sh, releasing_codes, price_map=None):
    try: records = sh.worksheet("近30日熱門統計").get_all_records()
    except: return {'entering': [], 'in_jail': []}
    jail_detail_map = get_merged_jail_period_details(sh)
    ent, inj, seen = [], [], set()
    for row in records:
        code = str(row.get('代號', '')).replace("'", "").strip()
        if code in releasing_codes or code in seen: continue
        name, days_str, reason = clean_display_text(row.get('名稱', '')), str(row.get('最快處置天數', '99')), clean_display_text(row.get('處置觸發原因', ''))
        if "處置中" in reason:
            detail = jail_detail_map.get(code, {'period': '日期未知', 'sort_end': None})
            inj.append({
                "code": code, "name": name, "price": format_display_price((price_map or {}).get(code, "--")),
                "period": detail.get('period', '日期未知'), "sort_end": detail.get('sort_end'),
            })
            seen.add(code)
        elif days_str.isdigit():
            d = int(days_str) + 1  
            if d <= JAIL_ENTER_THRESHOLD:
                ent.append({"code": code, "name": name, "days": d})
                seen.add(code)
    ent.sort(key=lambda x: (x['days'], code_sort_key(x['code'])))
    inj.sort(key=injail_sort_key)
    return {'entering': ent, 'in_jail': inj}

def check_releasing_stocks(sh, price_map=None, overflow_injail=None):
    try: records = sh.worksheet("即將出關監控").get_all_records()
    except: return []
    res, seen = [], set()
    for row in records:
        code = str(row.get('代號', '')).strip()
        if code in seen: continue
        days_str = str(row.get('剩餘天數', '99'))
        if not days_str.isdigit(): continue
        d = int(days_str) + 1
        last_day_dt = parse_roc_date(row.get('出關日期', ''))
        actual_release_dt = None
        if last_day_dt:
            actual_release_dt = last_day_dt + timedelta(days=1)
            if actual_release_dt.weekday() == 5: actual_release_dt += timedelta(days=2)
            elif actual_release_dt.weekday() == 6: actual_release_dt += timedelta(days=1)
        tw_now = datetime.utcnow() + timedelta(hours=8)
        display_days = d + 1 if (tw_now.weekday() >= 4 and tw_now.weekday() <= 6) else d
        if display_days <= 5:
            icon, status_text, pre_pct, in_pct = get_price_rank_info(code, row.get('處置期間', ''), row.get('市場', '上市'))
            res.append({"code": code, "name": clean_display_text(row.get('名稱', '')), "days": display_days, "price": format_display_price((price_map or {}).get(code, "--")), "date": actual_release_dt.strftime("%m/%d") if actual_release_dt else "??/??", "icon": icon, "status_text": status_text, "pre_pct": pre_pct, "in_pct": in_pct})
            seen.add(code)
        elif overflow_injail is not None:
            detail = build_period_detail(row.get('處置期間', ''))
            overflow_injail.append({
                "code": code, "name": clean_display_text(row.get('名稱', '')), "price": format_display_price((price_map or {}).get(code, "--")),
                "period": detail.get('period', '日期未知'), "sort_end": detail.get('sort_end'),
            })
            seen.add(code)
    res.sort(key=lambda x: (x['days'], code_sort_key(x['code'])))
    return res


# ============================
# 🎨 【核心版面引擎】
# ============================
COMMON_FIG_WIDTH = 13.0  
MARGIN_X = 0.4  

WATERMARK_TEXT = "By 股市艾斯出品-轉傳請註明\n資訊分享非投資建議 投資請自行評估風險"
WATERMARK_ALPHA = 0.80

def parse_pct(s):
    try: return float(str(s).replace('%', '').replace('+', ''))
    except: return 0

def get_pct_color(pct_str):
    pct = parse_pct(pct_str)
    if pct > 0: return TEXT_POS
    if pct < 0: return TEXT_NEG
    return TEXT_FLAT

def get_days_style(days):
    if days <= 1:  return DAYS_URGENT_BG, DAYS_URGENT_FG
    if days <= 3:  return DAYS_WARN_BG, DAYS_WARN_FG
    return DAYS_NORMAL_BG, DAYS_NORMAL_FG

def get_base_layout(n_rows, has_legend=False, custom_margin=MARGIN_X):
    top_offset = 1.35     
    header_h = 0.60       
    row_h = 0.45          
    bottom_offset = 0.90 if has_legend else 0.75  
    fig_h = top_offset + header_h + max(1, n_rows) * row_h + bottom_offset
    return fig_h, row_h, header_h, top_offset

def setup_canvas(fig_w, fig_h):
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), facecolor=BG_MAIN)
    ax.set_xlim(0, fig_w)
    ax.set_ylim(0, fig_h)
    ax.set_axis_off()
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0) 
    return fig, ax

def draw_topbar_and_frame(ax, theme, total_count, fig_w, fig_h, n_rows, row_h, header_h, top_offset, custom_margin=MARGIN_X):
    bar_h = 0.15
    ax.add_patch(patches.Rectangle((0, fig_h - bar_h), fig_w, bar_h, facecolor=theme['accent'], linewidth=0))
    title_y = fig_h - 0.55
    ax.text(fig_w/2, title_y, clean_display_text(theme['title']), ha='center', va='center', fontsize=26, fontproperties=FONT_BOLD, color='#2C3440')
    if theme.get('title_icon'):
        try:
            fig = ax.figure
            fig.canvas.draw()
            bbox = ax.texts[-1].get_window_extent(renderer=fig.canvas.get_renderer())
            bbox_data = ax.transData.inverted().transform(bbox)
            icon_x = bbox_data[0][0] - 0.40 
            draw_emoji_image(ax, theme['title_icon'], icon_x, title_y, fontsize=22, transform=ax.transData)
        except: draw_emoji_image(ax, theme['title_icon'], fig_w/2 - 2.5, title_y, fontsize=22, transform=ax.transData)
    
    sub_y = fig_h - 0.95
    today_str = datetime.now().strftime("%Y-%m-%d")
    ax.text(fig_w/2, sub_y, clean_display_text(f"資料日期: {today_str} | 共 {total_count} 檔"), ha='center', va='center', fontsize=15, fontproperties=FONT_PROP, color='#8A97A8')
    
    y_table_top = fig_h - top_offset
    table_total_h = header_h + max(1, n_rows) * row_h
    y_table_bottom = y_table_top - table_total_h
    y_header_bottom = y_table_top - header_h
    ax.text(custom_margin + 0.05, y_table_top + 0.15, f"▌ {clean_display_text(theme['subtitle_text'])}", ha='left', va='bottom', fontsize=17, fontproperties=FONT_BOLD, color=theme['accent'])
    
    table_w = fig_w - 2 * custom_margin
    ax.add_patch(patches.Rectangle((custom_margin, y_table_bottom), table_w, table_total_h, linewidth=1.2, edgecolor=BORDER_MID, facecolor=BG_TABLE))
    ax.add_patch(patches.Rectangle((custom_margin, y_header_bottom), table_w, header_h, linewidth=0, facecolor=theme['header']))
    ax.plot([custom_margin, fig_w - custom_margin], [y_table_top, y_table_top], color=theme['accent'], linewidth=2.5)
    return y_header_bottom

def draw_col_text(ax, xst, w, y, text, align, fs, fp, color):
    if align == 'center':
        ax.text(xst + w/2, y, text, ha='center', va='center', fontsize=fs, fontproperties=fp, color=color, zorder=3)
    elif align == 'right':
        ax.text(xst + w - 0.25, y, text, ha='right', va='center', fontsize=fs, fontproperties=fp, color=color, zorder=3)
    elif align == 'left':
        ax.text(xst + 0.15, y, text, ha='left', va='center', fontsize=fs, fontproperties=fp, color=color, zorder=3)

def draw_bottom_info(ax, fig_w, has_legend=False, custom_margin=MARGIN_X):
    ax.text(fig_w - custom_margin - 0.05, 0.20, WATERMARK_TEXT, ha='right', va='bottom', fontsize=13, linespacing=1.3, fontproperties=FONT_PROP, color='#2C3440', alpha=WATERMARK_ALPHA, zorder=10)
    if has_legend:
        y_pos_leg = 0.45 
        x_inch = custom_margin + 0.05
        ax.text(x_inch, y_pos_leg, "顏色說明", ha='left', va='center', fontsize=14, fontproperties=FONT_BOLD, color='#5B6678', zorder=9)
        ax.text(x_inch + 1.15, y_pos_leg, "■ 接近20MA  ｜  ■ 回測20MA後再轉強", ha='left', va='center', fontsize=14, fontproperties=FONT_PROP, color='#5B6678', zorder=9)
        ax.text(x_inch + 1.15, y_pos_leg, "■", ha='left', va='center', fontsize=14, fontproperties=FONT_PROP, color=SIGNAL_COLOR_RETEST, zorder=10)
        ax.text(x_inch + 2.45, y_pos_leg, "■", ha='left', va='center', fontsize=14, fontproperties=FONT_PROP, color=SIGNAL_COLOR_BREAKOUT, zorder=10)

def save_figure_to_buffer(fig):
    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=130, facecolor=fig.get_facecolor(), pad_inches=0)
    plt.close(fig)
    buf.seek(0)
    return buf

# ============================
# 📊 圖表繪製函式
# ============================

def draw_entering_image(data, signal_map=None):
    n = len(data)
    fig_w = COMMON_FIG_WIDTH
    fig_h, row_h, header_h, top_offset = get_base_layout(n)
    fig, ax = setup_canvas(fig_w, fig_h)
    y_header_bottom = draw_topbar_and_frame(ax, THEME_ENTERING, n, fig_w, fig_h, n, row_h, header_h, top_offset)
    col_widths_ratio = [0.10, 0.22, 0.36, 0.32]
    col_labels, col_aligns = ["#", "代號", "股票名稱", "倒數天數"], ['center', 'center', 'left', 'center']
    table_w = fig_w - 2 * MARGIN_X
    x_widths = [r * table_w for r in col_widths_ratio]
    x_starts = [MARGIN_X + sum(x_widths[:i]) for i in range(len(x_widths))]
    
    y_hc = y_header_bottom + header_h/2
    for xst, w, label, al in zip(x_starts, x_widths, col_labels, col_aligns):
        draw_col_text(ax, xst, w, y_hc, label, al, 16, FONT_BOLD, TEXT_HEADER)

    for i, row in enumerate(data):
        y_top = y_header_bottom - i * row_h
        bg_clr = BG_ROW_ODD if i % 2 == 0 else BG_ROW_EVEN
        code, name, days = row['code'], row['name'], row['days']
        ax.add_patch(patches.Rectangle((MARGIN_X, y_top - row_h), table_w, row_h, facecolor=bg_clr, zorder=1))
        draw_col_text(ax, x_starts[0], x_widths[0], y_top - row_h/2, f"{i+1:02d}", 'center', 16, FONT_BOLD, GOLD if i<3 else TEXT_MUTED)
        draw_col_text(ax, x_starts[1], x_widths[1], y_top - row_h/2, code, 'center', 18, FONT_BOLD, get_signal_color(code, signal_map))
        draw_col_text(ax, x_starts[2], x_widths[2], y_top - row_h/2, name, 'left', 17, FONT_PROP, get_signal_color(code, signal_map))
        
        # 標籤修正
        b_clr, f_clr = get_days_style(days)
        cap_w, cap_h = 1.7, 0.28
        ax.add_patch(patches.FancyBboxPatch((x_starts[3]+x_widths[3]/2-cap_w/2, y_top-row_h/2-cap_h/2), cap_w, cap_h, boxstyle="round,pad=0,rounding_size=0.14", facecolor=b_clr, linewidth=0, zorder=2))
        draw_col_text(ax, x_starts[3], x_widths[3], y_top-row_h/2, "明日處置" if days==1 else f"最快 {days} 天", 'center', 14, FONT_BOLD, f_clr)
    
    draw_bottom_info(ax, fig_w)
    return save_figure_to_buffer(fig)

def draw_releasing_image(data, signal_map=None):
    n = len(data)
    fig_w = COMMON_FIG_WIDTH
    fig_h, row_h, header_h, top_offset = get_base_layout(n, True)
    fig, ax = setup_canvas(fig_w, fig_h)
    y_header_bottom = draw_topbar_and_frame(ax, THEME_RELEASING, n, fig_w, fig_h, n, row_h, header_h, top_offset)

    # 重分配比例，後方 6 欄均等 (各 0.11)，名稱欄放寬
    col_widths_ratio = [0.05, 0.08, 0.21, 0.11, 0.11, 0.11, 0.11, 0.11, 0.11]
    col_labels = ["#", "代號", "股票名稱", "現價", "倒數交易日", "狀態", "處置前", "處置中", "出關日"]
    # 數值類採格式化後 center，狀態 center
    col_aligns = ['center', 'center', 'left', 'center', 'center', 'center', 'center', 'center', 'center']

    table_w = fig_w - 2 * MARGIN_X
    x_widths = [r * table_w for r in col_widths_ratio]
    x_starts = [MARGIN_X + sum(x_widths[:i]) for i in range(len(x_widths))]

    y_hc = y_header_bottom + header_h / 2
    for xst, w, label, al in zip(x_starts, x_widths, col_labels, col_aligns):
        draw_col_text(ax, xst, w, y_hc, label, al, 15, FONT_BOLD, TEXT_HEADER)

    for i, row in enumerate(data):
        y_top = y_header_bottom - i * row_h
        y_mid = y_top - row_h/2
        bg_clr = BG_ROW_ODD if i % 2 == 0 else BG_ROW_EVEN
        code, name, price, days, date = row['code'], row['name'], row['price'], row['days'], row['date']
        icon, status, pre, inj = row['icon'], row['status_text'], row['pre_pct'], row['in_pct']
        
        ax.add_patch(patches.Rectangle((MARGIN_X, y_top - row_h), table_w, row_h, facecolor=bg_clr, zorder=1))
        draw_col_text(ax, x_starts[0], x_widths[0], y_mid, f"{i+1:02d}", 'center', 15, FONT_BOLD, GOLD if i<3 else TEXT_MUTED)
        draw_col_text(ax, x_starts[1], x_widths[1], y_mid, code, 'center', 17, FONT_BOLD, get_signal_color(code, signal_map))
        draw_col_text(ax, x_starts[2], x_widths[2], y_mid, name, 'left', 16, FONT_PROP, get_signal_color(code, signal_map))
        
        # 數值置中對齊 (已格式化)
        draw_col_text(ax, x_starts[3], x_widths[3], y_mid, price, 'center', 15, FONT_BOLD, TEXT_PRICE)
        
        b_clr, f_clr = get_days_style(days)
        cap_w = 1.45
        ax.add_patch(patches.FancyBboxPatch((x_starts[4]+x_widths[4]/2-cap_w/2, y_mid-0.14), cap_w, 0.28, boxstyle="round,pad=0,rounding_size=0.14", facecolor=b_clr, linewidth=0, zorder=2))
        draw_col_text(ax, x_starts[4], x_widths[4], y_mid, "明日出關" if days==1 else f"剩 {days} 交易日", 'center', 13, FONT_BOLD, f_clr)
        
        # 狀態
        st_color = '#D69E2E' if "妖股" in status else '#E35D6A' if "強勢" in status else TEXT_MUTED
        draw_emoji_image(ax, icon, x_starts[5]+x_widths[5]/2-0.45, y_mid, 14)
        ax.text(x_starts[5]+x_widths[5]/2+0.1, y_mid, status, ha='center', va='center', fontsize=15, fontproperties=FONT_BOLD, color=st_color, zorder=3)
        
        draw_col_text(ax, x_starts[6], x_widths[6], y_mid, pre, 'center', 15, FONT_BOLD, get_pct_color(pre))
        draw_col_text(ax, x_starts[7], x_widths[7], y_mid, inj, 'center', 15, FONT_BOLD, get_pct_color(inj))
        draw_col_text(ax, x_starts[8], x_widths[8], y_mid, date, 'center', 15, FONT_PROP, TEXT_MAIN)

    draw_bottom_info(ax, fig_w, True)
    return save_figure_to_buffer(fig)

def draw_injail_image(data, signal_map=None):
    n = len(data)
    fig_w = COMMON_FIG_WIDTH
    # 增加邊距解決空洞感 (0.4 -> 1.2)
    custom_margin = 1.2
    fig_h, row_h, header_h, top_offset = get_base_layout(n, True, custom_margin)
    fig, ax = setup_canvas(fig_w, fig_h)
    y_header_bottom = draw_topbar_and_frame(ax, THEME_INJAIL, n, fig_w, fig_h, n, row_h, header_h, top_offset, custom_margin)
    
    col_widths_ratio = [0.08, 0.15, 0.32, 0.20, 0.25]
    col_labels = ["#", "代號", "股票名稱", "現價", "處置期間"]
    col_aligns = ['center', 'center', 'left', 'center', 'center']
    
    table_w = fig_w - 2 * custom_margin
    x_widths = [r * table_w for r in col_widths_ratio]
    x_starts = [custom_margin + sum(x_widths[:i]) for i in range(len(x_widths))]

    y_hc = y_header_bottom + header_h / 2
    for xst, w, label, al in zip(x_starts, x_widths, col_labels, col_aligns):
        draw_col_text(ax, xst, w, y_hc, label, al, 16, FONT_BOLD, TEXT_HEADER)

    for i, row in enumerate(data):
        y_mid = y_header_bottom - i * row_h - row_h/2
        bg_clr = BG_ROW_ODD if i % 2 == 0 else BG_ROW_EVEN
        code, name, price, period = row['code'], row['name'], row['price'], row['period']
        ax.add_patch(patches.Rectangle((custom_margin, y_header_bottom - (i+1)*row_h), table_w, row_h, facecolor=bg_clr, zorder=1))
        draw_col_text(ax, x_starts[0], x_widths[0], y_mid, f"{i+1:02d}", 'center', 16, FONT_BOLD, GOLD if i<3 else TEXT_MUTED)
        draw_col_text(ax, x_starts[1], x_widths[1], y_mid, code, 'center', 18, FONT_BOLD, get_signal_color(code, signal_map))
        draw_col_text(ax, x_starts[2], x_widths[2], y_mid, name, 'left', 17, FONT_PROP, get_signal_color(code, signal_map))
        draw_col_text(ax, x_starts[3], x_widths[3], y_mid, price, 'center', 16, FONT_BOLD, TEXT_PRICE)
        draw_col_text(ax, x_starts[4], x_widths[4], y_mid, period, 'center', 16, FONT_PROP, TEXT_MAIN)

    draw_bottom_info(ax, fig_w, True, custom_margin)
    return save_figure_to_buffer(fig)

# ============================
# 🚀 主程式
# ============================
def main():
    sh = connect_google_sheets()
    if not sh: return
    sig_map = load_signal_status_map(sh)
    pri_map = load_current_price_map(sh)
    
    overflow = []
    rel = check_releasing_stocks(sh, pri_map, overflow)
    rel_codes = {x['code'] for x in rel}
    
    stats = check_status_split(sh, rel_codes, pri_map)
    if overflow:
        seen = {x.get('code') for x in stats['in_jail']}
        for item in overflow:
            if item.get('code') not in seen:
                stats['in_jail'].append(item)
                seen.add(item.get('code'))
        stats['in_jail'].sort(key=injail_sort_key)
    
    tasks = [
        (stats['entering'], draw_entering_image, "瀕臨處置"),
        (rel, draw_releasing_image, "即將出關"),
        (stats['in_jail'], draw_injail_image, "處置中")
    ]
    
    for data, func, name in tasks:
        if data:
            print(f"📊 產生{name}圖片 ({len(data)} 檔)...")
            try:
                buf = func(data, sig_map)
                send_discord_image(buf)
                time.sleep(2)
            except Exception as e: print(f"❌ {name}圖片失敗: {e}")
    print("✅ 完成")

if __name__ == "__main__":
    main()
