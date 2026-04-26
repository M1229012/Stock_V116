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

# ============================
# ⚙️ 設定區
# ============================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_TEST")
SHEET_NAME = "台股注意股資料庫_V33"
SERVICE_KEY_FILE = "service_key.json"

JAIL_ENTER_THRESHOLD = 3   
JAIL_EXIT_THRESHOLD = 5    

# ============================
# 🎨 圖片風格設定
# ============================
def load_chinese_font():
    """載入中文字型 (沿用週報的搜尋邏輯)"""
    search_paths = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/noto-cjk/NotoSansCJKtc-Regular.otf",
        "/usr/share/fonts/opentype/noto/NotoSansCJKtc-Regular.otf",
        "/usr/local/share/fonts/NotoSansCJKtc-Regular.otf",
        "C:/Windows/Fonts/msjh.ttc",
        "/System/Library/Fonts/PingFang.ttc",
    ]
    for path in search_paths:
        if os.path.exists(path):
            font_manager.fontManager.addfont(path)
            return font_manager.FontProperties(fname=path)
    return font_manager.FontProperties()

def load_chinese_bold_font():
    """載入中文粗體字型"""
    search_paths = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Black.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
        "/usr/share/fonts/noto-cjk/NotoSansCJKtc-Bold.otf",
    ]
    for path in search_paths:
        if os.path.exists(path):
            font_manager.fontManager.addfont(path)
            return font_manager.FontProperties(fname=path)
    return load_chinese_font()

FONT_PROP = load_chinese_font()
FONT_BOLD = load_chinese_bold_font()

# ---- 共用顏色 ----
BG_MAIN     = '#0D1B2A'
BG_TOPBAR   = '#0A1520'
BG_TABLE    = '#1C2B3A'
BG_ROW_ODD  = '#1C2B3A'
BG_ROW_EVEN = '#243447'
BG_RANK     = '#162030'

TEXT_HEADER = '#FFFFFF'
TEXT_MAIN   = '#E8EFF7'
TEXT_MUTED  = '#8FA8C0'
TEXT_POS    = '#FF6B6B'
TEXT_NEG    = '#4CD964'
TEXT_FLAT   = '#8FA8C0'

GOLD        = '#FFD060'
SILVER      = '#C0C8D4'
BRONZE      = '#E8A070'
BORDER_DARK = '#0D1B2A'
BORDER_MID  = '#2E4560'

DAYS_URGENT_BG = '#FF4757'
DAYS_URGENT_FG = '#FFFFFF'
DAYS_WARN_BG   = '#FFA502'
DAYS_WARN_FG   = '#1A1A1A'
DAYS_NORMAL_BG = '#2E4560'
DAYS_NORMAL_FG = '#E8EFF7'

THEME_ENTERING  = {'accent': '#FF4757', 'header': '#3A0A0F', 'title': '🚨 處置倒數  瀕臨處置監控', 'subtitle_text': '瀕臨處置 (3日內)'}
THEME_RELEASING = {'accent': '#10B981', 'header': '#002A33', 'title': '🔓 越關越大尾  即將出關監控', 'subtitle_text': '即將出關 (5日內)'}
THEME_INJAIL    = {'accent': '#9B59B6', 'header': '#1F0A2E', 'title': '⛓️ 還能噴嗎  正在處置監控', 'subtitle_text': '處置中股票名單'}


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
    return {c: f"{d['start'].strftime('%Y/%m/%d')}-{d['end'].strftime('%Y/%m/%d')}" for c, d in jail_map.items()}


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
        
        pre_str = f"{'+' if pre_pct > 0 else ''}{pre_pct:.1f}"
        in_str  = f"{'+' if in_pct > 0 else ''}{in_pct:.1f}"
        return icon, status_text, pre_str, in_str
    except Exception as e:
        print(f"⚠️ 失敗 ({code}): {e}")
        return "❓ 未知", "數據計算中", "+0.0", "+0.0"


# ============================
# 🔍 監控邏輯 (原本邏輯)
# ============================
def check_status_split(sh, releasing_codes):
    try:
        ws = sh.worksheet("近30日熱門統計")
        records = ws.get_all_records()
    except: return {'entering': [], 'in_jail': []}
    jail_map = get_merged_jail_periods(sh)
    ent, inj, seen = [], [], set()
    for row in records:
        code = str(row.get('代號', '')).replace("'", "").strip()
        if code in releasing_codes or code in seen: continue
        name, days_str, reason = row.get('名稱', ''), str(row.get('最快處置天數', '99')), str(row.get('處置觸發原因', ''))
        if not days_str.isdigit(): continue
        d = int(days_str) + 1  
        if "處置中" in reason:
            inj.append({"code": code, "name": name, "period": jail_map.get(code, "日期未知")})
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


def check_releasing_stocks(sh):
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
                "name": row.get('名稱', ''),
                "days": d,
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


def draw_topbar(fig, theme, total, page_info=""):
    fig.add_artist(patches.Rectangle(
        (0, 0.91), 1, 0.09,
        linewidth=0, facecolor=BG_TOPBAR,
        transform=fig.transFigure, clip_on=False, zorder=0
    ))
    fig.add_artist(patches.Rectangle(
        (0, 0.99), 1, 0.01,
        linewidth=0, facecolor=theme['accent'],
        transform=fig.transFigure, clip_on=False, zorder=1
    ))
    fig.text(0.5, 0.955, theme['title'],
             ha='center', va='center',
             fontsize=38, fontweight='bold',
             fontproperties=FONT_BOLD,
             color='#FFFFFF', zorder=2)
    today_str = datetime.now().strftime("%Y-%m-%d")
    sub = f"資料日期: {today_str}  |  共 {total} 檔"
    if page_info: sub += f"  |  {page_info}"
    fig.text(0.5, 0.92, sub,
             ha='center', va='center',
             fontsize=19,
             fontproperties=FONT_PROP,
             color='#7BA8C8', zorder=2)


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
    ax.text(0.018, top_y + 0.015, f"▌ {subtitle}",
            transform=ax.transAxes, ha='left', va='bottom',
            fontsize=19, fontweight='bold',
            fontproperties=FONT_BOLD, color=theme['accent'])


def draw_entering_image(data):
    """瀕臨處置 - 單欄詳細圖"""
    theme = THEME_ENTERING
    n = len(data)
    fig_h = max(8, n * 0.7 + 4)
    
    fig, ax = plt.subplots(figsize=(13, fig_h), facecolor=BG_MAIN)
    fig.subplots_adjust(left=0.025, right=0.975, top=0.88, bottom=0.04)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.set_axis_off()
    
    draw_topbar(fig, theme, n)
    
    header_h = 0.075
    total_h = 0.86
    row_h = (total_h - header_h) / n
    top_y = 0.96
    
    draw_table_frame(ax, theme, theme['subtitle_text'], top_y, total_h)
    
    col_widths = [0.10, 0.20, 0.40, 0.30]
    col_labels = ["#", "代號", "股票名稱", "倒數天數"]
    col_aligns = ['center', 'center', 'left', 'center']
    
    x_starts = []
    acc = 0
    for w in col_widths:
        x_starts.append(acc); acc += w
    
    header_top = top_y
    
    ax.add_patch(patches.Rectangle(
        (0.005, header_top - header_h), 0.99, header_h,
        linewidth=0, facecolor=theme['header'],
        transform=ax.transAxes, clip_on=False, zorder=1
    ))
    ax.plot([0.005, 0.995], [header_top, header_top],
            color=theme['accent'], linewidth=2.5,
            transform=ax.transAxes, clip_on=False, zorder=2)
    
    for col_i, (xst, w, label, align) in enumerate(zip(x_starts, col_widths, col_labels, col_aligns)):
        text_x = xst + w/2 if align == 'center' else xst + 0.015
        ax.text(text_x, header_top - header_h/2, label,
                transform=ax.transAxes, ha=align, va='center',
                fontsize=20, fontweight='bold',
                fontproperties=FONT_BOLD, color=TEXT_HEADER, zorder=3)
    
    for row_i, row in enumerate(data):
        code, name, days = row['code'], row['name'], row['days']
        rank_num = row_i + 1
        y_top = header_top - header_h - row_i * row_h
        bg_color = BG_ROW_ODD if row_i % 2 == 0 else BG_ROW_EVEN
        
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
        
        ax.text(x_starts[0] + col_widths[0]/2, y_top - row_h/2, f"{rank_num:02d}",
                transform=ax.transAxes, ha='center', va='center',
                fontsize=18, fontweight=rank_fw,
                fontproperties=FONT_BOLD, color=rank_color, zorder=3)
        ax.text(x_starts[1] + col_widths[1]/2, y_top - row_h/2, code,
                transform=ax.transAxes, ha='center', va='center',
                fontsize=20, fontweight='bold',
                fontproperties=FONT_BOLD, color=TEXT_MAIN, zorder=3)
        ax.text(x_starts[2] + 0.015, y_top - row_h/2, name,
                transform=ax.transAxes, ha='left', va='center',
                fontsize=19, fontproperties=FONT_PROP,
                color=TEXT_MAIN, zorder=3)
        
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
        
        label_text = "明日處置" if days == 1 else f"剩 {days} 天"
        ax.text(x_starts[3] + col_widths[3]/2, y_top - row_h/2, label_text,
                transform=ax.transAxes, ha='center', va='center',
                fontsize=18, fontweight='bold',
                fontproperties=FONT_BOLD, color=fg_clr, zorder=3)
    
    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=130, bbox_inches='tight', facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf


def draw_releasing_image(data):
    """即將出關 - 單欄詳細含績效"""
    theme = THEME_RELEASING
    n = len(data)
    fig_h = max(10, n * 0.85 + 4)
    
    fig, ax = plt.subplots(figsize=(15, fig_h), facecolor=BG_MAIN)
    fig.subplots_adjust(left=0.025, right=0.975, top=0.88, bottom=0.04)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.set_axis_off()
    
    draw_topbar(fig, theme, n)
    
    header_h = 0.062
    total_h = 0.86
    row_h = (total_h - header_h) / n
    top_y = 0.96
    
    draw_table_frame(ax, theme, theme['subtitle_text'], top_y, total_h)
    
    col_widths = [0.05, 0.10, 0.16, 0.14, 0.18, 0.12, 0.12, 0.13]
    col_labels = ["#", "代號", "名稱", "倒數天數", "狀態", "處置前", "處置中", "出關日"]
    col_aligns = ['center', 'center', 'left', 'center', 'center', 'center', 'center', 'center']
    
    x_starts = []
    acc = 0
    for w in col_widths:
        x_starts.append(acc); acc += w
    
    header_top = top_y
    
    ax.add_patch(patches.Rectangle(
        (0.005, header_top - header_h), 0.99, header_h,
        linewidth=0, facecolor=theme['header'],
        transform=ax.transAxes, clip_on=False, zorder=1
    ))
    ax.plot([0.005, 0.995], [header_top, header_top],
            color=theme['accent'], linewidth=2.5,
            transform=ax.transAxes, clip_on=False, zorder=2)
    
    for col_i, (xst, w, label, align) in enumerate(zip(x_starts, col_widths, col_labels, col_aligns)):
        text_x = xst + w/2 if align == 'center' else xst + 0.012
        ax.text(text_x, header_top - header_h/2, label,
                transform=ax.transAxes, ha=align, va='center',
                fontsize=18, fontweight='bold',
                fontproperties=FONT_BOLD, color=TEXT_HEADER, zorder=3)
    
    for row_i, row in enumerate(data):
        code, name, days, date = row['code'], row['name'], row['days'], row['date']
        icon, status_text = row['icon'], row['status_text']
        pre_pct, in_pct = row['pre_pct'], row['in_pct']
        rank_num = row_i + 1
        y_top = header_top - header_h - row_i * row_h
        bg_color = BG_ROW_ODD if row_i % 2 == 0 else BG_ROW_EVEN
        
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
        
        ax.text(x_starts[0] + col_widths[0]/2, y_top - row_h/2, f"{rank_num:02d}",
                transform=ax.transAxes, ha='center', va='center',
                fontsize=16, fontweight=rank_fw,
                fontproperties=FONT_BOLD, color=rank_color, zorder=3)
        ax.text(x_starts[1] + col_widths[1]/2, y_top - row_h/2, code,
                transform=ax.transAxes, ha='center', va='center',
                fontsize=18, fontweight='bold',
                fontproperties=FONT_BOLD, color=TEXT_MAIN, zorder=3)
        ax.text(x_starts[2] + 0.012, y_top - row_h/2, name,
                transform=ax.transAxes, ha='left', va='center',
                fontsize=17, fontproperties=FONT_PROP,
                color=TEXT_MAIN, zorder=3)
        
        bg_clr, fg_clr = get_days_style(days)
        capsule_w = col_widths[3] * 0.78
        capsule_h = row_h * 0.62
        capsule_x = x_starts[3] + (col_widths[3] - capsule_w) / 2
        capsule_y = y_top - row_h/2 - capsule_h/2
        
        ax.add_patch(patches.FancyBboxPatch(
            (capsule_x, capsule_y), capsule_w, capsule_h,
            boxstyle="round,pad=0.002,rounding_size=0.012",
            linewidth=0, facecolor=bg_clr,
            transform=ax.transAxes, clip_on=False, zorder=2
        ))
        ax.text(x_starts[3] + col_widths[3]/2, y_top - row_h/2, f"剩 {days} 天",
                transform=ax.transAxes, ha='center', va='center',
                fontsize=16, fontweight='bold',
                fontproperties=FONT_BOLD, color=fg_clr, zorder=3)
        
        if "妖股" in status_text:    st_color = '#FFD060'
        elif "強勢" in status_text:  st_color = '#FF6B6B'
        elif "人去樓空" in status_text: st_color = '#9B59B6'
        elif "走勢疲軟" in status_text: st_color = '#4CD964'
        else:                         st_color = TEXT_MUTED
        
        ax.text(x_starts[4] + col_widths[4]/2, y_top - row_h/2,
                f"{icon} {status_text}",
                transform=ax.transAxes, ha='center', va='center',
                fontsize=16, fontweight='bold',
                fontproperties=FONT_BOLD, color=st_color, zorder=3)
        
        ax.text(x_starts[5] + col_widths[5]/2, y_top - row_h/2, f"{pre_pct}%",
                transform=ax.transAxes, ha='center', va='center',
                fontsize=17, fontweight='bold',
                fontproperties=FONT_BOLD, color=get_pct_color(pre_pct), zorder=3)
        ax.text(x_starts[6] + col_widths[6]/2, y_top - row_h/2, f"{in_pct}%",
                transform=ax.transAxes, ha='center', va='center',
                fontsize=17, fontweight='bold',
                fontproperties=FONT_BOLD, color=get_pct_color(in_pct), zorder=3)
        ax.text(x_starts[7] + col_widths[7]/2, y_top - row_h/2, date,
                transform=ax.transAxes, ha='center', va='center',
                fontsize=16, fontproperties=FONT_PROP,
                color=TEXT_MAIN, zorder=3)
    
    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=130, bbox_inches='tight', facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf


def draw_injail_image(data):
    """處置中 - 三欄並列"""
    theme = THEME_INJAIL
    n = len(data)
    n_cols = 3
    rows_per_col = (n + n_cols - 1) // n_cols
    fig_h = max(12, rows_per_col * 0.43 + 4)
    
    fig, ax = plt.subplots(figsize=(20, fig_h), facecolor=BG_MAIN)
    fig.subplots_adjust(left=0.02, right=0.98, top=0.88, bottom=0.04)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.set_axis_off()
    
    draw_topbar(fig, theme, n)
    
    header_h = 0.05
    total_h = 0.86
    row_h = (total_h - header_h) / rows_per_col
    top_y = 0.96
    
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
    sub_col_widths_ratio = [0.10, 0.18, 0.30, 0.42]
    sub_labels = ["#", "代號", "名稱", "處置期間"]
    sub_aligns = ['center', 'center', 'left', 'center']
    
    for col_idx in range(n_cols):
        col_x_start = col_xs[col_idx]
        sub_x_starts = []
        acc = 0
        for r in sub_col_widths_ratio:
            sub_x_starts.append(col_x_start + acc * col_unit_w); acc += r
        sub_x_widths = [r * col_unit_w for r in sub_col_widths_ratio]
        
        for sub_i, (xst, w, label, align) in enumerate(zip(sub_x_starts, sub_x_widths, sub_labels, sub_aligns)):
            text_x = xst + w/2 if align == 'center' else xst + 0.008
            ax.text(text_x, header_top - header_h/2, label,
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
        code, name, period = row['code'], row['name'], row['period']
        col_idx = idx // rows_per_col
        row_idx = idx % rows_per_col
        if col_idx >= n_cols: break
        
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
        ax.add_patch(patches.Rectangle(
            (sub_x_starts[0], y_top - row_h), sub_x_widths[0], row_h,
            linewidth=0, facecolor=BG_RANK,
            transform=ax.transAxes, clip_on=False, zorder=1
        ))
        
        if global_idx == 1:   rank_color = GOLD
        elif global_idx == 2: rank_color = SILVER
        elif global_idx == 3: rank_color = BRONZE
        else:                 rank_color = TEXT_MUTED
        
        ax.text(sub_x_starts[0] + sub_x_widths[0]/2, y_top - row_h/2, f"{global_idx:02d}",
                transform=ax.transAxes, ha='center', va='center',
                fontsize=14, fontweight='bold',
                fontproperties=FONT_BOLD, color=rank_color, zorder=3)
        ax.text(sub_x_starts[1] + sub_x_widths[1]/2, y_top - row_h/2, code,
                transform=ax.transAxes, ha='center', va='center',
                fontsize=16, fontweight='bold',
                fontproperties=FONT_BOLD, color=TEXT_MAIN, zorder=3)
        ax.text(sub_x_starts[2] + 0.008, y_top - row_h/2, name,
                transform=ax.transAxes, ha='left', va='center',
                fontsize=15, fontproperties=FONT_PROP,
                color=TEXT_MAIN, zorder=3)
        
        # 處置中只顯示日期 (像範例 04/24-04/29)
        try:
            if "/" in period and "-" in period:
                parts = period.split("-")
                start_p = parts[0].strip().replace("2026/", "")
                end_p   = parts[1].strip().replace("2026/", "")
                period_display = f"{start_p}-{end_p}"
            else:
                period_display = period
        except:
            period_display = period
        
        ax.text(sub_x_starts[3] + sub_x_widths[3]/2, y_top - row_h/2, period_display,
                transform=ax.transAxes, ha='center', va='center',
                fontsize=14, fontweight='bold',
                fontproperties=FONT_BOLD, color='#A8C8E0', zorder=3)
    
    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=130, bbox_inches='tight', facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf


# ============================
# 🚀 主程式
# ============================
def main():
    sh = connect_google_sheets()
    if not sh: return
    
    rel = check_releasing_stocks(sh)
    rel_codes = {x['code'] for x in rel}
    stats = check_status_split(sh, rel_codes)

    # 1. 瀕臨處置
    if stats['entering']:
        print(f"📊 產生瀕臨處置圖片 ({len(stats['entering'])} 檔)...")
        try:
            buf = draw_entering_image(stats['entering'])
            send_discord_image(buf, content_text=f"### 🚨 處置倒數!{len(stats['entering'])} 檔股票瀕臨處置")
            time.sleep(2)
        except Exception as e:
            print(f"❌ 瀕臨處置圖片產生失敗: {e}")

    # 2. 即將出關
    if rel:
        print(f"📊 產生即將出關圖片 ({len(rel)} 檔)...")
        try:
            buf = draw_releasing_image(rel)
            send_discord_image(buf, content_text=f"### 🔓 越關越大尾?{len(rel)} 檔股票即將出關\n*💡 處置前 N 天 vs 處置中 N 天 (同天數對比)*")
            time.sleep(2)
        except Exception as e:
            print(f"❌ 即將出關圖片產生失敗: {e}")

    # 3. 處置中
    if stats['in_jail']:
        print(f"📊 產生處置中圖片 ({len(stats['in_jail'])} 檔)...")
        try:
            buf = draw_injail_image(stats['in_jail'])
            send_discord_image(buf, content_text=f"### ⛓️ 還能噴嗎?{len(stats['in_jail'])} 檔股票正在處置")
            time.sleep(2)
        except Exception as e:
            print(f"❌ 處置中圖片產生失敗: {e}")

    print("✅ 完成")


if __name__ == "__main__":
    main()
