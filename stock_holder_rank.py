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

# ================= 圖片樣式設定 =================
WATERMARK_TEXT = "By 股市艾斯出品-轉傳請註明"
WATERMARK_ALPHA = 0.80

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
    計算股價與週漲跌：
    以 latest_date_str 所在週為基準，抓該週第一個有效交易日的 Open，
    以及該週最後一個有效交易日的 Close。
    若週一或週五休市，會自動改用週二開盤或週四收盤等可取得的交易日。
    """
    try:
        ref_date = parse_latest_trade_date(latest_date_str)
        week_start = ref_date - timedelta(days=ref_date.weekday())
        week_end = week_start + timedelta(days=7)

        ticker = f"{code}{market_suffix}"
        df = yf.Ticker(ticker).history(
            start=week_start.strftime("%Y-%m-%d"),
            end=week_end.strftime("%Y-%m-%d"),
            auto_adjust=True
        )

        # 若資料來源當週尚未更新，往前補抓一段時間，仍取最新可用週資料。
        if df.empty:
            fallback_start = ref_date - timedelta(days=14)
            fallback_end = ref_date + timedelta(days=2)
            df = yf.Ticker(ticker).history(
                start=fallback_start.strftime("%Y-%m-%d"),
                end=fallback_end.strftime("%Y-%m-%d"),
                auto_adjust=True
            )

        if df.empty or "Open" not in df.columns or "Close" not in df.columns:
            return "-", "-"

        df = df.dropna(subset=["Open", "Close"])
        if df.empty:
            return "-", "-"

        first_open = float(df["Open"].iloc[0])
        last_close = float(df["Close"].iloc[-1])

        if first_open <= 0:
            return f"{last_close:.1f}", "-"

        week_pct = ((last_close - first_open) / first_open) * 100
        arrow = "▲" if week_pct > 0 else "▼" if week_pct < 0 else "—"
        return f"{last_close:.1f}", f"{arrow}{abs(week_pct):.1f}%"
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


def draw_rank_table(ax, df, title, accent, y_top, card_h):
    left = 0.045
    width = 0.91
    row_n = 0 if df is None else len(df)
    header_h = 0.048
    title_h = 0.058
    row_h = (card_h - title_h - header_h - 0.028) / max(row_n, 1)

    ax.add_patch(patches.FancyBboxPatch(
        (left, y_top - card_h), width, card_h,
        boxstyle="round,pad=0.006,rounding_size=0.010",
        linewidth=1.2, edgecolor=CARD_BORDER, facecolor=CARD_BG,
        transform=ax.transAxes, zorder=1
    ))
    ax.add_patch(patches.Rectangle(
        (left, y_top - title_h), width, title_h,
        linewidth=0, facecolor=HEADER_BG,
        transform=ax.transAxes, zorder=2
    ))
    ax.add_patch(patches.Rectangle(
        (left, y_top - title_h), 0.010, title_h,
        linewidth=0, facecolor=accent,
        transform=ax.transAxes, zorder=3
    ))

    draw_text(ax, left + 0.025, y_top - title_h / 2, title, size=14,
              color=accent, weight='bold', bold=True)

    col_rel = [0.075, 0.105, 0.205, 0.165, 0.130, 0.150, 0.170]
    labels = ["排名", "代號", "股名", "類別", "現價", "週漲跌", "總增減%"]
    aligns = ["center", "center", "left", "left", "right", "right", "right"]
    col_x = [left]
    acc = 0
    for w in col_rel[:-1]:
        acc += w
        col_x.append(left + width * acc)

    header_top = y_top - title_h
    ax.add_patch(patches.Rectangle(
        (left, header_top - header_h), width, header_h,
        linewidth=0, facecolor="#F8FAFC",
        transform=ax.transAxes, zorder=2
    ))
    ax.plot([left, left + width], [header_top, header_top],
            transform=ax.transAxes, color=accent, linewidth=1.8, zorder=3)
    ax.plot([left, left + width], [header_top - header_h, header_top - header_h],
            transform=ax.transAxes, color=CARD_BORDER, linewidth=0.8, zorder=3)

    for i, label in enumerate(labels):
        x0 = col_x[i]
        cw = width * col_rel[i]
        if aligns[i] == "center":
            tx = x0 + cw / 2
            ha = "center"
        elif aligns[i] == "right":
            tx = x0 + cw - 0.018
            ha = "right"
        else:
            tx = x0 + 0.018
            ha = "left"
        draw_text(ax, tx, header_top - header_h / 2, label, size=10,
                  color=TEXT_MUTED, weight='bold', ha=ha, bold=True)

    if df is None or df.empty:
        draw_text(ax, left + width / 2, header_top - header_h - row_h / 2,
                  "無資料", size=13, color=TEXT_MUTED, ha='center')
        return

    df = df.reset_index(drop=True)
    for i, row in df.iterrows():
        y = header_top - header_h - i * row_h
        bg = "#FFFFFF" if i % 2 == 0 else "#F8FAFC"
        ax.add_patch(patches.Rectangle(
            (left, y - row_h), width, row_h,
            linewidth=0, facecolor=bg,
            transform=ax.transAxes, zorder=2
        ))
        ax.plot([left, left + width], [y - row_h, y - row_h],
                transform=ax.transAxes, color="#EDF2F7", linewidth=0.6, zorder=3)

        code, name = split_code_name(row['股票代號/名稱'])
        category = clean_cell(row.get('類別', '-'))
        price = clean_cell(row.get('現價', '-'))
        week_chg = clean_cell(row.get('週漲跌', '-'))
        change_str = fmt_change(row['總增減'])
        try:
            change_val = float(change_str)
        except:
            change_val = 0.0

        if "▲" in week_chg:
            week_color = TEXT_RED
        elif "▼" in week_chg:
            week_color = TEXT_GREEN
        else:
            week_color = TEXT_MUTED

        chg_color = TEXT_RED if change_val > 0 else TEXT_GREEN if change_val < 0 else TEXT_MUTED
        chg_display = "-" if change_str == "-" else f"{change_val:+.2f}%"

        values = [f"{i+1:02d}", code, name, category, price, week_chg, chg_display]
        colors = [TEXT_MUTED, TEXT_MAIN, TEXT_MAIN, TEXT_MUTED, TEXT_MAIN, week_color, chg_color]
        weights = ['bold', 'bold', 'normal', 'normal', 'bold', 'bold', 'bold']

        for j, value in enumerate(values):
            x0 = col_x[j]
            cw = width * col_rel[j]
            if aligns[j] == "center":
                tx = x0 + cw / 2
                ha = "center"
            elif aligns[j] == "right":
                tx = x0 + cw - 0.018
                ha = "right"
            else:
                tx = x0 + 0.018
                ha = "left"
            draw_text(ax, tx, y - row_h / 2, value, size=11,
                      color=colors[j], weight=weights[j], ha=ha,
                      bold=(weights[j] == 'bold'))


def build_rank_image(listed_df, otc_df, display_date):
    listed_n = 0 if listed_df is None else len(listed_df)
    otc_n = 0 if otc_df is None else len(otc_df)

    row_unit = 0.030
    listed_card_h = 0.130 + max(listed_n, 1) * row_unit
    otc_card_h = 0.130 + max(otc_n, 1) * row_unit
    top_area_h = 0.145
    gap_h = 0.030
    bottom_h = 0.060
    total_units = top_area_h + listed_card_h + gap_h + otc_card_h + bottom_h
    fig_h = max(8.0, min(16.0, total_units * 12.0))
    fig_w = 13.5

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), facecolor=IMG_BG)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_axis_off()

    fig.add_artist(patches.Rectangle(
        (0, 0.94), 1, 0.06,
        linewidth=0, facecolor="#FFFFFF",
        transform=fig.transFigure, clip_on=False, zorder=0
    ))

    draw_text(ax, 0.5, 0.965, "每週大股東籌碼強勢榜 Top 20",
              size=22, color=TEXT_MAIN, weight='bold', ha='center', bold=True)
    draw_text(ax, 0.5, 0.925, f"資料統計日期：{display_date}",
              size=12, color=TEXT_MUTED, ha='center')

    y_top = 0.875
    draw_rank_table(ax, listed_df.reset_index(drop=True) if listed_df is not None else None,
                    "上市排行", ACCENT_LISTED, y_top, listed_card_h / total_units)
    y_top -= listed_card_h / total_units + gap_h / total_units
    draw_rank_table(ax, otc_df.reset_index(drop=True) if otc_df is not None else None,
                    "上櫃排行", ACCENT_OTC, y_top, otc_card_h / total_units)

    fig.text(0.985, 0.018, clean_cell(WATERMARK_TEXT),
             ha='right', va='bottom',
             fontsize=10,
             fontproperties=FONT_PROP,
             color="#2C3440",
             alpha=WATERMARK_ALPHA,
             zorder=10)

    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=150, facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf


def push_rank_to_dc():
    if not DISCORD_WEBHOOK_URL:
        print("錯誤：找不到 DISCORD_WEBHOOK_URL_TEST 環境變數")
        return

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
        h_price = pad_visual("現價", W_PRICE, align='right')
        h_week = pad_visual("週漲跌", W_WEEK, align='right')
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
            s_price = pad_visual(price, W_PRICE, align='right')
            s_week = pad_visual(week_chg, W_WEEK, align='right')
            s_chg  = pad_visual(change_str, W_CHANGE, align='left')

            msg += f"{s_rank}{GAP}{s_code}{GAP}{s_name}{GAP}{s_cat}{GAP}{s_price}{GAP}{s_week}{GAP}{s_chg}\n"
            
        msg += "```\n"
        return msg

    content += format_rank_block(listed_df.reset_index(drop=True), "🟦 **【上市排行】**")
    content += format_rank_block(otc_df.reset_index(drop=True), "🟩 **【上櫃排行】**")

    # 發送
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

if __name__ == "__main__":
    push_rank_to_dc()
