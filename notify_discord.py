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
        if abs(num - round(num)) < 1e-9: return str(int(round(num)))
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


def get_merged_jail_periods(sh):
    return {c: d['period'] for c, d in get_merged_jail_period_details(sh).items()}


def load_tech_tracking_latest_map(sh):
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
    return tech_map


def load_signal_status_map(sh):
    return {c: v.get('status', '') for c, v in load_tech_tracking_latest_map(sh).items()}


def load_current_price_map(sh):
    return {c: v.get('price', '--') for c, v in load_tech_tracking_latest_map(sh).items()}


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
        return icon, clean_display_text(status_text), f"{'+' if pre_pct > 0 else ''}{pre_pct:.1f}", f"{'+' if in_pct > 0 else ''}{in_pct:.1f}"
    except: return "❓ 未知", "數據計算中", "+0.0", "+0.0"


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
                "code": code,
                "name": name,
                "price": format_display_price((price_map or {}).get(code, "--")),
                "period": detail.get('period', '日期未知'),
                "sort_end": detail.get('sort_end'),
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
        if tw_now.weekday() >= 4 and tw_now.weekday() <= 6:
            display_days = d + 1
        else:
            display_days = d
        if display_days <= 5:
            icon, status_text, pre_pct, in_pct = get_price_rank_info(code, row.get('處置期間', ''), row.get('市場', '上市'))
            res.append({"code": code, "name": clean_display_text(row.get('名稱', '')), "days": display_days, "price": format_display_price((price_map or {}).get(code, "--")), "date": actual_release_dt.strftime("%m/%d") if actual_release_dt else "??/??", "icon": icon, "status_text": status_text, "pre_pct": pre_pct, "in_pct": in_pct})
            seen.add(code)
        elif overflow_injail is not None:
            detail = build_period_detail(row.get('處置期間', ''))
            overflow_injail.append({
                "code": code,
                "name": clean_display_text(row.get('名稱', '')),
                "price": format_display_price((price_map or {}).get(code, "--")),
                "period": detail.get('period', '日期未知'),
                "sort_end": detail.get('sort_end'),
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


def get_base_layout(n_rows, has_legend=False):
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


def draw_topbar_and_frame(ax, theme, total_count, fig_w, fig_h, n_rows, row_h, header_h, top_offset):
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
        except:
            draw_emoji_image(ax, theme['title_icon'], fig_w/2 - 2.5, title_y, fontsize=22, transform=ax.transData)
    sub_y = fig_h - 0.95
    today_str = datetime.now().strftime("%Y-%m-%d")
    ax.text(fig_w/2, sub_y, clean_display_text(f"資料日期: {today_str} | 共 {total_count} 檔"), ha='center', va='center', fontsize=15, fontproperties=FONT_PROP, color='#8A97A8')
    y_table_top = fig_h - top_offset
    table_total_h = header_h + max(1, n_rows) * row_h
    y_table_bottom = y_table_top - table_total_h
    y_header_bottom = y_table_top - header_h
    ax.text(MARGIN_X + 0.05, y_table_top + 0.15, f"▌ {clean_display_text(theme['subtitle_text'])}", ha='left', va='bottom', fontsize=17, fontproperties=FONT_BOLD, color=theme['accent'])
    table_w = fig_w - 2 * MARGIN_X
    ax.add_patch(patches.Rectangle((MARGIN_X, y_table_bottom), table_w, table_total_h, linewidth=1.2, edgecolor=BORDER_MID, facecolor=BG_TABLE))
    ax.add_patch(patches.Rectangle((MARGIN_X, y_header_bottom), table_w, header_h, linewidth=0, facecolor=theme['header']))
    ax.plot([MARGIN_X, fig_w - MARGIN_X], [y_table_top, y_table_top], color=theme['accent'], linewidth=2.5)
    return y_header_bottom


def draw_col_text(ax, xst, w, y, text, align, fs, fp, color):
    if align == 'center':
        ax.text(xst + w/2, y, text, ha='center', va='center', fontsize=fs, fontproperties=fp, color=color, zorder=3)
    elif align == 'right':
        # 加大右間距，確保靠右對齊時不會貼邊且整齊
        ax.text(xst + w - 0.35, y, text, ha='right', va='center', fontsize=fs, fontproperties=fp, color=color, zorder=3)
    elif align == 'left':
        ax.text(xst + 0.15, y, text, ha='left', va='center', fontsize=fs, fontproperties=fp, color=color, zorder=3)


def draw_bottom_info(ax, fig_w, has_legend=False):
    y_pos_wm = 0.20
    ax.text(fig_w - MARGIN_X - 0.05, y_pos_wm, WATERMARK_TEXT, ha='right', va='bottom',
            fontsize=13, linespacing=1.3, fontproperties=FONT_PROP, color='#2C3440', alpha=WATERMARK_ALPHA, zorder=10)
    if has_legend:
        y_pos_leg = 0.45
        x_inch = MARGIN_X + 0.05
        def add_text(inch_x, text, fs, fp, color):
            ax.text(inch_x, y_pos_leg, text, ha='left', va='center', fontsize=fs, fontproperties=fp, color=color, zorder=9)
        add_text(x_inch, "顏色說明", 14, FONT_BOLD, '#5B6678')
        x_inch += 0.90
        add_text(x_inch, "｜", 13, FONT_PROP, '#A0AAB8')
        x_inch += 0.25
        add_text(x_inch, "■", 14, FONT_PROP, SIGNAL_COLOR_RETEST)
        x_inch += 0.25
        add_text(x_inch, "接近20MA", 14, FONT_PROP, '#5B6678')
        x_inch += 1.05
        add_text(x_inch, "｜", 13, FONT_PROP, '#A0AAB8')
        x_inch += 0.25
        add_text(x_inch, "■", 14, FONT_PROP, SIGNAL_COLOR_BREAKOUT)
        x_inch += 0.25
        add_text(x_inch, "回測20MA後再轉強", 14, FONT_PROP, '#5B6678')


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
    fig_h, row_h, header_h, top_offset = get_base_layout(n, has_legend=False)
    fig, ax = setup_canvas(fig_w, fig_h)
    y_header_bottom = draw_topbar_and_frame(ax, THEME_ENTERING, n, fig_w, fig_h, n, row_h, header_h, top_offset)
    col_widths_ratio = [0.10, 0.22, 0.36, 0.32]
    col_labels = ["#", "代號", "股票名稱", "倒數天數"]
    col_aligns = ['center', 'center', 'left', 'center']
    table_w = fig_w - 2 * MARGIN_X
    x_widths = [r * table_w for r in col_widths_ratio]
    x_starts = []
    acc = MARGIN_X
    for w in x_widths:
        x_starts.append(acc)
        acc += w
    y_header_center = y_header_bottom + header_h / 2
    for xst, w, label, align in zip(x_starts, x_widths, col_labels, col_aligns):
        draw_col_text(ax, xst, w, y_header_center, clean_display_text(label), align, 16, FONT_BOLD, TEXT_HEADER)
    for i, row in enumerate(data):
        y_top = y_header_bottom - i * row_h
        y_center = y_top - row_h / 2
        bg_color = BG_ROW_ODD if i % 2 == 0 else BG_ROW_EVEN
        code, name, days = clean_display_text(row['code']), clean_display_text(row['name'], True), row['days']
        name_color = get_signal_color(code, signal_map)
        ax.add_patch(patches.Rectangle((MARGIN_X, y_top - row_h), table_w, row_h, linewidth=0, facecolor=bg_color, zorder=1))
        ax.add_patch(patches.Rectangle((x_starts[0], y_top - row_h), x_widths[0], row_h, linewidth=0, facecolor=BG_RANK, zorder=1))
        ax.plot([MARGIN_X + 0.1, fig_w - MARGIN_X - 0.1], [y_top - row_h, y_top - row_h], color=BORDER_DARK, linewidth=0.6, zorder=2)
        rank_num = i + 1
        if rank_num == 1:   rank_color, rank_fw = GOLD, FONT_BOLD
        elif rank_num == 2: rank_color, rank_fw = SILVER, FONT_BOLD
        elif rank_num == 3: rank_color, rank_fw = BRONZE, FONT_BOLD
        else:               rank_color, rank_fw = TEXT_MUTED, FONT_PROP
        draw_col_text(ax, x_starts[0], x_widths[0], y_center, f"{rank_num:02d}", 'center', 16, rank_fw, rank_color)
        draw_col_text(ax, x_starts[1], x_widths[1], y_center, code, col_aligns[1], 18, FONT_BOLD, name_color)
        draw_col_text(ax, x_starts[2], x_widths[2], y_center, name, col_aligns[2], 17, FONT_PROP, name_color)
        bg_clr, fg_clr = get_days_style(days)
        capsule_w, capsule_h = 1.8, 0.28
        capsule_x = x_starts[3] + x_widths[3]/2 - capsule_w/2
        capsule_y = y_center - capsule_h/2
        ax.add_patch(patches.FancyBboxPatch((capsule_x, capsule_y), capsule_w, capsule_h, boxstyle="round,pad=0,rounding_size=0.14", facecolor=bg_clr, linewidth=0, zorder=2))
        label_text = "明日處置" if days == 1 else f"最快 {days} 天"
        ax.text(x_starts[3] + x_widths[3]/2, y_center, clean_display_text(label_text), ha='center', va='center', fontsize=14, fontproperties=FONT_BOLD, color=fg_clr, zorder=3)
    draw_bottom_info(ax, fig_w, has_legend=False)
    return save_figure_to_buffer(fig)


# ===========================================================================
# 即將出關 - 完全重寫的對齊邏輯
# ---------------------------------------------------------------------------
# 核心設計：「重心等距網格」(Visual Anchor Grid)
#
# 問題根源：
#   後 6 欄混合了「靠右」和「置中」對齊：
#     - 現價 / 處置前 / 處置中：靠右（位數變動，靠右最整齊）
#     - 倒數交易日 / 狀態 / 出關日：置中（膠囊、emoji+文字、固定日期）
#   如果只是用「等寬欄位」，靠右的內容會貼欄位右緣、置中內容會在欄位中央
#   → 兩種內容的「視覺重心」不在同一條等距線上 → 看起來歪歪的
#
# 解法：
#   1. 把後 6 欄的總寬度切成 6 個等距「重心點」(anchor)
#   2. 置中欄 → 內容中心放在 anchor
#   3. 靠右欄 → 內容右緣放在「anchor + half_anchor_pitch * 0.5」（也就是兩 anchor 中點偏右）
#      這樣靠右欄的「視覺重心」也會回落到 anchor 上
#   4. Header 也用相同 anchor，標題跟資料對齊一致
# ===========================================================================
def draw_releasing_image(data, signal_map=None):
    """即將出關 - 後 6 欄等距重心對齊。"""
    n = len(data)
    fig_w = COMMON_FIG_WIDTH
    fig_h, row_h, header_h, top_offset = get_base_layout(n, has_legend=True)
    fig, ax = setup_canvas(fig_w, fig_h)
    y_header_bottom = draw_topbar_and_frame(ax, THEME_RELEASING, n, fig_w, fig_h, n, row_h, header_h, top_offset)

    table_w = fig_w - 2 * MARGIN_X

    # ---------- 第 1 步：切版面 ----------
    # 前 3 欄（#、代號、股票名稱）佔表格左半邊 35%
    # 後 6 欄（現價、倒數交易日、狀態、處置前、處置中、出關日）佔右半邊 65%
    LEFT_BLOCK_RATIO = 0.35
    RIGHT_BLOCK_RATIO = 0.65

    left_block_w = table_w * LEFT_BLOCK_RATIO
    right_block_w = table_w * RIGHT_BLOCK_RATIO
    right_block_start_x = MARGIN_X + left_block_w
    right_block_end_x = MARGIN_X + table_w

    # ---------- 第 2 步：前 3 欄欄寬 ----------
    left_col_ratios = [0.13, 0.22, 0.65]   # # / 代號 / 股票名稱
    left_x_widths = [r * left_block_w for r in left_col_ratios]
    left_x_starts = []
    acc = MARGIN_X
    for w in left_x_widths:
        left_x_starts.append(acc)
        acc += w

    # ---------- 第 3 步：後 6 欄重心網格 ----------
    # 在 right_block 中放 6 個等距重心點。
    # anchor_pitch = 兩個重心點之間的距離
    # 第 1 個重心點與右邊框各留 anchor_pitch/2 的邊距，視覺最舒服
    anchor_pitch = right_block_w / 6.0
    anchors_x = [right_block_start_x + anchor_pitch * (i + 0.5) for i in range(6)]

    # 後 6 欄定義
    right_col_labels = ["現價", "倒數交易日", "狀態", "處置前", "處置中", "出關日"]
    right_col_aligns = ['right', 'center', 'center', 'right', 'center', 'center']
    #                   現價    倒數      狀態     處置前   處置中   出關日

    # 靠右欄需要的「半寬」: 把資料右緣放在 anchor + half_pitch_offset 位置
    # 這個值越大，靠右的數字會越往該欄右側貼；
    # 為了讓「視覺重心」依然落在 anchor 上，half_pitch_offset 需略小於 anchor_pitch/2
    # 經驗值：anchor_pitch * 0.30
    right_text_offset = anchor_pitch * 0.30

    def get_anchor_x(col_idx, align):
        """依該欄對齊方式回傳要放文字的 X 座標。"""
        anchor = anchors_x[col_idx]
        if align == 'right':
            return anchor + right_text_offset
        return anchor   # center

    # ---------- 第 4 步：畫表頭 ----------
    y_header_center = y_header_bottom + header_h / 2

    # 前 3 欄表頭
    front_labels = ["#", "代號", "股票名稱"]
    front_aligns = ['center', 'center', 'left']
    for xst, w, label, align in zip(left_x_starts, left_x_widths, front_labels, front_aligns):
        draw_col_text(ax, xst, w, y_header_center, clean_display_text(label), align, 15, FONT_BOLD, TEXT_HEADER)

    # 後 6 欄表頭：用重心網格對齊
    for col_idx, (label, align) in enumerate(zip(right_col_labels, right_col_aligns)):
        text_x = get_anchor_x(col_idx, align)
        ha_style = 'right' if align == 'right' else 'center'
        ax.text(text_x, y_header_center, clean_display_text(label),
                ha=ha_style, va='center',
                fontsize=15, fontproperties=FONT_BOLD, color=TEXT_HEADER, zorder=3)

    # ---------- 第 5 步：畫每一列 ----------
    for i, row in enumerate(data):
        y_top = y_header_bottom - i * row_h
        y_center = y_top - row_h / 2
        bg_color = BG_ROW_ODD if i % 2 == 0 else BG_ROW_EVEN
        code = clean_display_text(row['code'])
        name = clean_display_text(row['name'], True)
        price = clean_display_text(str(row.get('price', '--')))
        days = row['days']
        date = clean_display_text(row['date'])
        icon = row['icon']
        status_text = clean_display_text(row['status_text'])
        pre_pct = row['pre_pct']
        in_pct = row['in_pct']
        name_color = get_signal_color(code, signal_map)

        # 列底色 + 排名底色 + 分隔線
        ax.add_patch(patches.Rectangle((MARGIN_X, y_top - row_h), table_w, row_h, linewidth=0, facecolor=bg_color, zorder=1))
        ax.add_patch(patches.Rectangle((left_x_starts[0], y_top - row_h), left_x_widths[0], row_h, linewidth=0, facecolor=BG_RANK, zorder=1))
        ax.plot([MARGIN_X + 0.1, fig_w - MARGIN_X - 0.1], [y_top - row_h, y_top - row_h], color=BORDER_DARK, linewidth=0.6, zorder=2)

        # 排名顏色
        rank_num = i + 1
        if rank_num == 1:   rank_color, rank_fw = GOLD, FONT_BOLD
        elif rank_num == 2: rank_color, rank_fw = SILVER, FONT_BOLD
        elif rank_num == 3: rank_color, rank_fw = BRONZE, FONT_BOLD
        else:               rank_color, rank_fw = TEXT_MUTED, FONT_PROP

        # 前 3 欄：# / 代號 / 股票名稱
        draw_col_text(ax, left_x_starts[0], left_x_widths[0], y_center, f"{rank_num:02d}", 'center', 15, rank_fw, rank_color)
        draw_col_text(ax, left_x_starts[1], left_x_widths[1], y_center, code, 'center', 17, FONT_BOLD, name_color)
        draw_col_text(ax, left_x_starts[2], left_x_widths[2], y_center, name, 'left', 16, FONT_PROP, name_color)

        # 後 6 欄：用重心網格
        # [0] 現價 - 靠右
        ax.text(get_anchor_x(0, 'right'), y_center, price,
                ha='right', va='center',
                fontsize=15, fontproperties=FONT_BOLD, color=TEXT_PRICE, zorder=3)

        # [1] 倒數交易日 - 置中（膠囊）
        bg_clr, fg_clr = get_days_style(days)
        capsule_w, capsule_h = 1.55, 0.28
        capsule_x = anchors_x[1] - capsule_w / 2
        capsule_y = y_center - capsule_h / 2
        ax.add_patch(patches.FancyBboxPatch(
            (capsule_x, capsule_y), capsule_w, capsule_h,
            boxstyle="round,pad=0,rounding_size=0.14",
            facecolor=bg_clr, linewidth=0, zorder=2
        ))
        label_text = clean_display_text("明日出關" if days == 1 else f"剩 {days} 交易日")
        ax.text(anchors_x[1], y_center, label_text,
                ha='center', va='center',
                fontsize=13, fontproperties=FONT_BOLD, color=fg_clr, zorder=3)

        # [2] 狀態 - 置中（emoji + 文字當整組置中）
        if "妖股" in status_text:    st_color = '#D69E2E'
        elif "強勢" in status_text:  st_color = '#E35D6A'
        elif "人去樓空" in status_text: st_color = '#9B59B6'
        elif "走勢疲軟" in status_text: st_color = '#2F9E72'
        else:                         st_color = TEXT_MUTED

        # 估算 emoji + 文字整組的寬度，再放到 anchor 中心
        # emoji 視覺寬度 ≈ 0.30 inch，文字（4 中文字 fontsize 15）≈ 0.95 inch，間隔 0.10
        emoji_w = 0.30
        text_w_est = 0.95
        gap = 0.10
        total_w = emoji_w + gap + text_w_est
        group_left = anchors_x[2] - total_w / 2
        emoji_center_x = group_left + emoji_w / 2
        text_left_x = group_left + emoji_w + gap

        emoji_ok = draw_emoji_image(ax, icon, emoji_center_x, y_center, fontsize=14,
                                    transform=ax.transData, zorder=4, fallback_color=st_color)
        if emoji_ok:
            ax.text(text_left_x, y_center, status_text,
                    ha='left', va='center',
                    fontsize=15, fontproperties=FONT_BOLD, color=st_color, zorder=3)
        else:
            # emoji 載入失敗時用 ASCII fallback 整組重新置中
            icon_fallback = EMOJI_FALLBACK_SYMBOLS.get(icon, icon)
            ax.text(anchors_x[2], y_center, f"{icon_fallback} {status_text}",
                    ha='center', va='center',
                    fontsize=15, fontproperties=FONT_BOLD, color=st_color, zorder=3)

        # [3] 處置前 - 靠右
        ax.text(get_anchor_x(3, 'right'), y_center, f"{pre_pct}%",
                ha='right', va='center',
                fontsize=15, fontproperties=FONT_BOLD, color=get_pct_color(pre_pct), zorder=3)

        # [4] 處置中 - 靠右
        ax.text(get_anchor_x(4, 'right'), y_center, f"{in_pct}%",
                ha='right', va='center',
                fontsize=15, fontproperties=FONT_BOLD, color=get_pct_color(in_pct), zorder=3)

        # [5] 出關日 - 置中
        ax.text(anchors_x[5], y_center, date,
                ha='center', va='center',
                fontsize=15, fontproperties=FONT_PROP, color=TEXT_MAIN, zorder=3)

    draw_bottom_info(ax, fig_w, has_legend=True)
    return save_figure_to_buffer(fig)


# ===========================================================================
# 處置中 - 雙欄佈局
# ---------------------------------------------------------------------------
# 規則：
#   ≤ 18 檔  → 單欄
#   19-44 檔 → 雙欄
#   > 44 檔  → 雙欄分頁，每頁 44 檔
# ===========================================================================
def _draw_injail_single_column(data, total_count, signal_map=None, page_no=None, total_pages=None, rank_offset=0):
    """處置中 - 單欄版（≤18 檔用）"""
    n = len(data)
    fig_w = COMMON_FIG_WIDTH
    fig_h, row_h, header_h, top_offset = get_base_layout(n, has_legend=True)
    fig, ax = setup_canvas(fig_w, fig_h)
    theme = dict(THEME_INJAIL)
    if total_pages and total_pages > 1:
        theme['title'] = f"{theme['title']} ({page_no}/{total_pages})"
    y_header_bottom = draw_topbar_and_frame(ax, theme, total_count, fig_w, fig_h, n, row_h, header_h, top_offset)

    col_widths_ratio = [0.10, 0.18, 0.24, 0.16, 0.32]
    col_labels = ["#", "代號", "股票名稱", "現價", "處置期間"]
    col_aligns = ['center', 'center', 'left', 'right', 'center']

    table_w = fig_w - 2 * MARGIN_X
    x_widths = [r * table_w for r in col_widths_ratio]
    x_starts = []
    acc = MARGIN_X
    for w in x_widths:
        x_starts.append(acc)
        acc += w

    y_header_center = y_header_bottom + header_h / 2
    for xst, w, label, align in zip(x_starts, x_widths, col_labels, col_aligns):
        draw_col_text(ax, xst, w, y_header_center, clean_display_text(label), align, 16, FONT_BOLD, TEXT_HEADER)

    for i, row in enumerate(data):
        y_top = y_header_bottom - i * row_h
        y_center = y_top - row_h / 2
        bg_color = BG_ROW_ODD if i % 2 == 0 else BG_ROW_EVEN
        code = clean_display_text(row['code'])
        name = clean_display_text(row['name'], True)
        price = clean_display_text(str(row.get('price', '--')))
        period = clean_display_text(row['period'])
        name_color = get_signal_color(code, signal_map)

        ax.add_patch(patches.Rectangle((MARGIN_X, y_top - row_h), table_w, row_h, linewidth=0, facecolor=bg_color, zorder=1))
        ax.add_patch(patches.Rectangle((x_starts[0], y_top - row_h), x_widths[0], row_h, linewidth=0, facecolor=BG_RANK, zorder=1))
        ax.plot([MARGIN_X + 0.1, fig_w - MARGIN_X - 0.1], [y_top - row_h, y_top - row_h], color=BORDER_DARK, linewidth=0.6, zorder=2)

        rank_num = rank_offset + i + 1
        if rank_num == 1:   rank_color, rank_fw = GOLD, FONT_BOLD
        elif rank_num == 2: rank_color, rank_fw = SILVER, FONT_BOLD
        elif rank_num == 3: rank_color, rank_fw = BRONZE, FONT_BOLD
        else:               rank_color, rank_fw = TEXT_MUTED, FONT_PROP

        draw_col_text(ax, x_starts[0], x_widths[0], y_center, f"{rank_num:02d}", 'center', 16, rank_fw, rank_color)
        draw_col_text(ax, x_starts[1], x_widths[1], y_center, code, col_aligns[1], 18, FONT_BOLD, name_color)
        draw_col_text(ax, x_starts[2], x_widths[2], y_center, name, col_aligns[2], 17, FONT_PROP, name_color)
        draw_col_text(ax, x_starts[3], x_widths[3], y_center, price, col_aligns[3], 16, FONT_BOLD, TEXT_PRICE)
        draw_col_text(ax, x_starts[4], x_widths[4], y_center, period, col_aligns[4], 16, FONT_PROP, TEXT_MAIN)

    draw_bottom_info(ax, fig_w, has_legend=True)
    return save_figure_to_buffer(fig)


def _draw_injail_two_column(page_data, total_count, signal_map=None, page_no=None, total_pages=None, rank_offset=0):
    """處置中 - 雙欄版（19~44 檔用，每欄最多 22 列）"""
    rows_per_col = max(1, int(np.ceil(len(page_data) / 2)))
    fig_w = COMMON_FIG_WIDTH
    fig_h, row_h, header_h, top_offset = get_base_layout(rows_per_col, has_legend=True)
    fig, ax = setup_canvas(fig_w, fig_h)
    theme = dict(THEME_INJAIL)
    if total_pages and total_pages > 1:
        theme['title'] = f"{theme['title']} ({page_no}/{total_pages})"
    y_header_bottom = draw_topbar_and_frame(ax, theme, total_count, fig_w, fig_h, rows_per_col, row_h, header_h, top_offset)

    table_w = fig_w - 2 * MARGIN_X
    half_w = table_w / 2.0
    divider_x = MARGIN_X + half_w
    y_table_top = y_header_bottom + header_h
    y_table_bottom = y_header_bottom - rows_per_col * row_h
    ax.plot([divider_x, divider_x], [y_table_bottom, y_table_top], color=BORDER_MID, linewidth=1.0, zorder=2)

    col_widths_ratio = [0.09, 0.16, 0.31, 0.16, 0.28]
    col_labels = ["#", "代號", "股票名稱", "現價", "處置期間"]
    col_aligns = ['center', 'center', 'left', 'right', 'center']

    def draw_half(half_idx, rows):
        x_base = MARGIN_X + half_idx * half_w
        widths = [r * half_w for r in col_widths_ratio]
        starts = []
        acc = x_base
        for w in widths:
            starts.append(acc)
            acc += w

        y_header_center = y_header_bottom + header_h / 2
        for xst, w, label, align in zip(starts, widths, col_labels, col_aligns):
            draw_col_text(ax, xst, w, y_header_center, clean_display_text(label), align, 15, FONT_BOLD, TEXT_HEADER)

        for i, row in enumerate(rows):
            y_top = y_header_bottom - i * row_h
            y_center = y_top - row_h / 2
            bg_color = BG_ROW_ODD if i % 2 == 0 else BG_ROW_EVEN
            code = clean_display_text(row['code'])
            name = clean_display_text(row['name'], True)
            price = clean_display_text(str(row.get('price', '--')))
            period = clean_display_text(row['period'])
            name_color = get_signal_color(code, signal_map)

            ax.add_patch(patches.Rectangle((x_base, y_top - row_h), half_w, row_h, linewidth=0, facecolor=bg_color, zorder=1))
            ax.add_patch(patches.Rectangle((starts[0], y_top - row_h), widths[0], row_h, linewidth=0, facecolor=BG_RANK, zorder=1))
            ax.plot([x_base + 0.05, x_base + half_w - 0.05], [y_top - row_h, y_top - row_h], color=BORDER_DARK, linewidth=0.6, zorder=2)

            rank_num = rank_offset + half_idx * rows_per_col + i + 1
            if rank_num == 1:   rank_color, rank_fw = GOLD, FONT_BOLD
            elif rank_num == 2: rank_color, rank_fw = SILVER, FONT_BOLD
            elif rank_num == 3: rank_color, rank_fw = BRONZE, FONT_BOLD
            else:               rank_color, rank_fw = TEXT_MUTED, FONT_PROP

            draw_col_text(ax, starts[0], widths[0], y_center, f"{rank_num:02d}", 'center', 15, rank_fw, rank_color)
            draw_col_text(ax, starts[1], widths[1], y_center, code, col_aligns[1], 17, FONT_BOLD, name_color)
            draw_col_text(ax, starts[2], widths[2], y_center, name, col_aligns[2], 16, FONT_PROP, name_color)
            draw_col_text(ax, starts[3], widths[3], y_center, price, col_aligns[3], 15, FONT_BOLD, TEXT_PRICE)
            draw_col_text(ax, starts[4], widths[4], y_center, period, col_aligns[4], 15, FONT_PROP, TEXT_MAIN)

    left_rows = page_data[:rows_per_col]
    right_rows = page_data[rows_per_col:rows_per_col * 2]
    draw_half(0, left_rows)
    draw_half(1, right_rows)

    draw_bottom_info(ax, fig_w, has_legend=True)
    return save_figure_to_buffer(fig)


def draw_injail_image(data, signal_map=None):
    """處置中圖片產生器：依檔數自動決定單欄/雙欄/分頁。

    回傳值：
        - 單一 buffer (≤18 檔，或 19~44 檔單頁雙欄)
        - list of buffer (>44 檔，多頁雙欄)
    """
    n = len(data)
    if n <= 18:
        return _draw_injail_single_column(data, n, signal_map=signal_map)

    page_size = 44
    pages = [data[i:i + page_size] for i in range(0, n, page_size)]
    if len(pages) == 1:
        return _draw_injail_two_column(pages[0], n, signal_map=signal_map)

    buffers = []
    total_pages = len(pages)
    for page_no, page_data in enumerate(pages, start=1):
        buffers.append(_draw_injail_two_column(
            page_data,
            n,
            signal_map=signal_map,
            page_no=page_no,
            total_pages=total_pages,
            rank_offset=(page_no - 1) * page_size
        ))
    return buffers


# ============================
# 🚀 主程式
# ============================
def main():
    sh = connect_google_sheets()
    if not sh: return
    signal_map = load_signal_status_map(sh)
    price_map = load_current_price_map(sh)

    releasing_over5_injail = []
    rel = check_releasing_stocks(sh, price_map=price_map, overflow_injail=releasing_over5_injail)
    rel_codes = {x['code'] for x in rel}

    stats = check_status_split(sh, rel_codes, price_map=price_map)
    if releasing_over5_injail:
        existing_codes = {x.get('code') for x in stats['in_jail']}
        for item in releasing_over5_injail:
            if item.get('code') not in existing_codes:
                stats['in_jail'].append(item)
                existing_codes.add(item.get('code'))
        stats['in_jail'].sort(key=injail_sort_key)

    if stats['entering']:
        print(f"📊 產生瀕臨處置圖片 ({len(stats['entering'])} 檔)...")
        try:
            buf = draw_entering_image(stats['entering'], signal_map=signal_map)
            send_discord_image(buf)
            time.sleep(2)
        except Exception as e: print(f"❌ 瀕臨處置圖片產生失敗: {e}")
    if rel:
        print(f"📊 產生即將出關圖片 ({len(rel)} 檔)...")
        try:
            buf = draw_releasing_image(rel, signal_map=signal_map)
            send_discord_image(buf)
            time.sleep(2)
        except Exception as e: print(f"❌ 即將出關圖片產生失敗: {e}")
    if stats['in_jail']:
        print(f"📊 產生處置中圖片 ({len(stats['in_jail'])} 檔)...")
        try:
            injail_result = draw_injail_image(stats['in_jail'], signal_map=signal_map)
            if isinstance(injail_result, list):
                for idx, buf in enumerate(injail_result, start=1):
                    print(f"   └─ 發送處置中第 {idx}/{len(injail_result)} 張圖片")
                    send_discord_image(buf)
                    time.sleep(2)
            else:
                send_discord_image(injail_result)
                time.sleep(2)
        except Exception as e: print(f"❌ 處置中圖片產生失敗: {e}")
    print("✅ 完成")


if __name__ == "__main__":
    main()
