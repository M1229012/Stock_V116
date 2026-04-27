import requests
import pandas as pd
from io import StringIO
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import re
import os
from datetime import datetime
from wcwidth import wcwidth
import unicodedata
from PIL import Image, ImageDraw, ImageFont

# ================= 設定區 =================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_TEST")
OUTPUT_IMAGE = "norway_parallel_rank.png"
TOP_N = 10
WATERMARK_TEXT = "By 股市艾斯出品-轉傳請註明"

# ================= 資料爬取區 =================
def get_norway_rank_logic(url):
    """
    依照 APP 邏輯爬取，並加入「依最新週漲幅排序」功能
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

        # 1. 等待特定表格出現
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//table[contains(., '大股東持有張數增減')]"))
        )

        html = driver.page_source
        dfs = pd.read_html(StringIO(html), header=None)

        target_df = None
        # 2. 尋找包含關鍵字的表格
        for df in dfs:
            if len(df.columns) > 10 and len(df) > 20:
                if df.astype(str).apply(lambda x: x.str.contains('大股東持有').any()).any():
                    target_df = df
                    break

        if target_df is None and len(dfs) > 0:
            target_df = max(dfs, key=len)

        if target_df is None:
            return None, None

        # 3. 定位 Header 與 Data Start Index
        header_idx = -1
        data_start_idx = -1

        for idx, row in target_df.iterrows():
            if re.search(r'\d{4}', str(row.iloc[3])):
                data_start_idx = idx
                break

        if data_start_idx == -1:
            return None, None

        for idx in range(max(0, data_start_idx - 5), data_start_idx):
            row = target_df.iloc[idx]
            if re.match(r'^\d{4,}$', str(row.iloc[5])):
                header_idx = idx
                break

        # 4. 找出最新日期欄位
        max_col_index = target_df.shape[1] - 1
        start_search = min(10, max_col_index)

        latest_date_col_idx = 5
        latest_date_str = "未知日期"

        if header_idx != -1:
            for col_i in range(start_search, 4, -1):
                try:
                    val = str(target_df.iloc[header_idx, col_i]).strip()
                    if re.search(r'\d+', val):
                        latest_date_col_idx = col_i
                        latest_date_str = val
                        break
                except Exception:
                    continue

        # 5. 抓取所有資料列
        raw_data = target_df.iloc[data_start_idx:].copy()

        def parse_pct(x):
            try:
                return float(str(x).replace('%', '').replace(',', ''))
            except Exception:
                return -999999.0

        raw_data['_sort_val'] = raw_data.iloc[:, latest_date_col_idx].apply(parse_pct)
        top_data = raw_data.sort_values(by='_sort_val', ascending=False).head(20)

        result_df = pd.DataFrame()
        result_df['股票代號/名稱'] = top_data.iloc[:, 3]
        result_df['總增減'] = top_data.iloc[:, latest_date_col_idx]

        return result_df, latest_date_str

    except Exception as e:
        print(f"爬取錯誤: {e}")
        return None, None
    finally:
        driver.quit()

# ================= 排版工具區（保留原本） =================
_ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u202a-\u202e\ufeff]")

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
    s = s.replace("\xa0", " ")
    s = _ZERO_WIDTH_RE.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
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
    s = re.sub(r'\s+', '', s)
    v = pd.to_numeric(s, errors='coerce')
    return "-" if pd.isna(v) else f"{v:.2f}"

# ================= 圖片版工具 =================
def load_font(size, bold=False):
    font_candidates = [
        ('/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc' if bold else '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc'),
        ('/usr/share/fonts/opentype/noto/NotoSerifCJK-Bold.ttc' if bold else '/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc'),
    ]
    for path in font_candidates:
        if os.path.exists(path):
            return ImageFont.truetype(path, size=size)
    return ImageFont.load_default()

FONT_TITLE = load_font(34, bold=True)
FONT_SUBTITLE = load_font(18, bold=False)
FONT_PANEL_TITLE = load_font(28, bold=True)
FONT_PANEL_TOP = load_font(20, bold=True)
FONT_HEADER = load_font(18, bold=True)
FONT_ROW = load_font(18, bold=False)
FONT_ROW_BOLD = load_font(18, bold=True)
FONT_CHANGE = load_font(26, bold=True)
FONT_RANK_BIG = load_font(24, bold=True)
FONT_RANK = load_font(18, bold=False)
FONT_WATERMARK = load_font(16, bold=False)

COLOR_BG = '#F3F4F6'
COLOR_CARD = '#FFFFFF'
COLOR_PANEL_BAR = '#2F66DB'
COLOR_HEADER_BG = '#D8DCE3'
COLOR_BORDER = '#E5E7EB'
COLOR_TEXT = '#1F2937'
COLOR_MUTED = '#7A8699'
COLOR_ROW_ALT = '#F0F2F5'
COLOR_TOP1_BG = '#EEEADD'
COLOR_UP = '#E32727'
COLOR_DOWN = '#16A34A'
COLOR_GOLD = '#F4D07A'
COLOR_SILVER = '#D8DCE3'
COLOR_BRONZE = '#E9C08E'
COLOR_WATERMARK = '#A0A7B3'


def parse_stock_identity(raw_str):
    raw_str = clean_cell(raw_str)
    match = re.match(r'(\d{4})\s*(.*)', raw_str)
    if match:
        code = clean_cell(match.group(1))
        name = clean_cell(match.group(2).strip())
    else:
        code = clean_cell(raw_str[:4])
        name = clean_cell(raw_str[4:].strip())
    name = name.replace('卅卅', '碁')
    return code, name


def parse_change_value(x):
    try:
        return float(str(x).replace('%', '').replace(',', '').strip())
    except Exception:
        return 0.0


def format_change_for_image(v):
    arrow = '▲' if v >= 0 else '▼'
    sign = '+' if v >= 0 else ''
    return f"{arrow} {sign}{v:.2f}%"


def ellipsize(draw, text, font, max_width):
    text = clean_cell(text)
    if draw.textbbox((0, 0), text, font=font)[2] <= max_width:
        return text
    ellipsis = '…'
    out = ''
    for ch in text:
        test = out + ch + ellipsis
        if draw.textbbox((0, 0), test, font=font)[2] > max_width:
            break
        out += ch
    return out + ellipsis


def draw_centered_text(draw, xy, text, font, fill):
    bbox = draw.textbbox((0, 0), text, font=font)
    w = bbox[2] - bbox[0]
    h = bbox[3] - bbox[1]
    draw.text((xy[0] - w / 2, xy[1] - h / 2), text, font=font, fill=fill)


def draw_panel(draw, x, y, w, title, df, top_n=10):
    panel_bar_h = 58
    header_h = 50
    row_h = 66

    # 卡片背景
    h = panel_bar_h + header_h + row_h * top_n + 18
    draw.rounded_rectangle([x, y, x + w, y + h], radius=18, fill=COLOR_CARD, outline=COLOR_BORDER, width=1)

    # 藍色標題條
    draw.rounded_rectangle([x, y, x + w, y + panel_bar_h], radius=18, fill=COLOR_PANEL_BAR)
    draw.rectangle([x, y + panel_bar_h - 18, x + w, y + panel_bar_h], fill=COLOR_PANEL_BAR)
    draw.text((x + 24, y + 12), title, font=FONT_PANEL_TITLE, fill='white')

    top_n_text = f"TOP {top_n}"
    tb = draw.textbbox((0, 0), top_n_text, font=FONT_PANEL_TOP)
    draw.text((x + w - (tb[2] - tb[0]) - 24, y + 16), top_n_text, font=FONT_PANEL_TOP, fill='#E8EEFF')

    # 欄位寬度
    col_rank = 88
    col_code = 120
    col_name = w - col_rank - col_code - 220
    col_change = 220

    y0 = y + panel_bar_h
    draw.rectangle([x, y0, x + w, y0 + header_h], fill=COLOR_HEADER_BG)

    draw_centered_text(draw, (x + col_rank / 2, y0 + header_h / 2), '排名', FONT_HEADER, COLOR_TEXT)
    draw_centered_text(draw, (x + col_rank + col_code / 2, y0 + header_h / 2), '代號', FONT_HEADER, COLOR_TEXT)
    draw_centered_text(draw, (x + col_rank + col_code + col_name / 2, y0 + header_h / 2), '股名', FONT_HEADER, COLOR_TEXT)
    draw_centered_text(draw, (x + col_rank + col_code + col_name + col_change / 2, y0 + header_h / 2), '增減幅度', FONT_HEADER, COLOR_TEXT)

    display_df = df.head(top_n).reset_index(drop=True) if df is not None else pd.DataFrame(columns=['股票代號/名稱', '總增減'])

    for idx in range(top_n):
        row_y1 = y0 + header_h + idx * row_h
        row_y2 = row_y1 + row_h

        if idx < len(display_df):
            row = display_df.iloc[idx]
            code, name = parse_stock_identity(row['股票代號/名稱'])
            change_val = parse_change_value(row['總增減'])
        else:
            code, name, change_val = '', '', 0.0

        bg_fill = COLOR_TOP1_BG if idx == 0 else (COLOR_ROW_ALT if idx % 2 == 1 else COLOR_CARD)
        draw.rectangle([x, row_y1, x + w, row_y2], fill=bg_fill)
        draw.line([x + 16, row_y2, x + w - 16, row_y2], fill=COLOR_BORDER, width=1)

        cy = (row_y1 + row_y2) / 2

        # 排名
        rank_x = x + col_rank / 2
        if idx < 3:
            medal_color = [COLOR_GOLD, COLOR_SILVER, COLOR_BRONZE][idx]
            r = 22
            draw.ellipse([rank_x - r, cy - r, rank_x + r, cy + r], fill=medal_color)
            draw_centered_text(draw, (rank_x, cy), f'{idx + 1}', FONT_RANK_BIG, '#9A4B00' if idx != 1 else '#445066')
        else:
            draw_centered_text(draw, (rank_x, cy), f'{idx + 1:02d}', FONT_RANK, COLOR_MUTED)

        # 代號 / 股名
        draw_centered_text(draw, (x + col_rank + col_code / 2, cy), code, FONT_ROW_BOLD, COLOR_TEXT)
        name_text = ellipsize(draw, name, FONT_ROW, max(10, col_name - 20))
        draw.text((x + col_rank + col_code + 12, cy - 14), name_text, font=FONT_ROW, fill=COLOR_TEXT)

        # 漲跌幅
        change_text = format_change_for_image(change_val)
        change_color = COLOR_UP if change_val >= 0 else COLOR_DOWN
        change_bbox = draw.textbbox((0, 0), change_text, font=FONT_CHANGE)
        change_w = change_bbox[2] - change_bbox[0]
        draw.text((x + w - change_w - 20, cy - 18), change_text, font=FONT_CHANGE, fill=change_color)

    return h


def generate_parallel_rank_image(listed_df, otc_df, display_date, output_path=OUTPUT_IMAGE, top_n=TOP_N):
    margin_x = 28
    top_title_h = 110
    gap = 26
    panel_w = 760
    panel_bar_h = 58
    header_h = 50
    row_h = 66
    panel_h = panel_bar_h + header_h + row_h * top_n + 18

    img_w = margin_x * 2 + panel_w * 2 + gap
    img_h = top_title_h + panel_h + 36

    img = Image.new('RGB', (img_w, img_h), COLOR_BG)
    draw = ImageDraw.Draw(img)

    # 主標題
    title = '每週大股東籌碼強勢榜'
    subtitle = f'資料統計日期：{display_date}  ｜  上市 / 上櫃並列 Top {top_n}'
    draw_centered_text(draw, (img_w / 2, 34), title, FONT_TITLE, COLOR_TEXT)
    draw_centered_text(draw, (img_w / 2, 72), subtitle, FONT_SUBTITLE, COLOR_MUTED)

    # 左右兩個 panel
    left_x = margin_x
    right_x = margin_x + panel_w + gap
    panel_y = top_title_h

    draw_panel(draw, left_x, panel_y, panel_w, '上市排行', listed_df if listed_df is not None else pd.DataFrame(), top_n=top_n)
    draw_panel(draw, right_x, panel_y, panel_w, '上櫃排行', otc_df if otc_df is not None else pd.DataFrame(), top_n=top_n)

    # 浮水印
    wbbox = draw.textbbox((0, 0), WATERMARK_TEXT, font=FONT_WATERMARK)
    ww = wbbox[2] - wbbox[0]
    wh = wbbox[3] - wbbox[1]
    draw.text((img_w - ww - 18, img_h - wh - 14), WATERMARK_TEXT, font=FONT_WATERMARK, fill=COLOR_WATERMARK)

    img.save(output_path)
    return output_path

# ================= 推播主流程 =================
def build_text_fallback(listed_df, otc_df, display_date):
    content = '📊 **每週大股東籌碼強勢榜**\n'
    content += f'> 📅 **資料統計日期：{display_date}**\n\n'

    def format_rank_block(df, title):
        if df is None or df.empty:
            return f'{title} ❌ **無資料**\n\n'

        msg = f'{title}\n'
        msg += '```text\n'

        W_RANK = 4
        W_CODE = 6
        W_NAME = 12
        W_CHANGE = 10
        GAP = ' '

        h_rank = pad_visual('排名', W_RANK)
        h_code = pad_visual('代號', W_CODE)
        h_name = pad_visual('股名', W_NAME)
        h_chg = pad_visual('總增減%', W_CHANGE, align='left')
        msg += f'{h_rank}{GAP}{h_code}{GAP}{h_name}{GAP}{h_chg}\n'

        total_width = W_RANK + W_CODE + W_NAME + W_CHANGE + (len(GAP) * 3)
        msg += '=' * (total_width - 4) + '\n'

        for i, row in df.head(TOP_N).reset_index(drop=True).iterrows():
            raw_str = clean_cell(row['股票代號/名稱'])
            match = re.match(r'(\d{4})\s*(.*)', raw_str)
            if match:
                code = match.group(1)
                name = match.group(2).strip()
            else:
                code = raw_str[:4]
                name = raw_str[4:].strip()

            code = clean_cell(code)
            name = clean_cell(name).replace('卅卅', '碁')
            change_str = fmt_change(row['總增減'])
            full_name = to_fullwidth(name)
            s_name = pad_visual(full_name, W_NAME, align='left')
            s_rank = pad_visual(f'{i + 1:02d}', W_RANK)
            s_code = pad_visual(code, W_CODE)
            s_chg = pad_visual(change_str, W_CHANGE, align='left')
            msg += f'{s_rank}{GAP}{s_code}{GAP}{s_name}{GAP}{s_chg}\n'

        msg += '```\n'
        return msg

    content += format_rank_block(listed_df, '🟦 **【上市排行】**')
    content += format_rank_block(otc_df, '🟩 **【上櫃排行】**')
    return content


def push_rank_to_dc():
    if not DISCORD_WEBHOOK_URL:
        print('錯誤：找不到 DISCORD_WEBHOOK_URL_TEST 環境變數')
        return

    print('正在處理上市排行...')
    listed_df, listed_date = get_norway_rank_logic('https://norway.twsthr.info/StockHoldersTopWeek.aspx')

    print('正在處理上櫃排行...')
    otc_df, otc_date = get_norway_rank_logic('https://norway.twsthr.info/StockHoldersTopWeek.aspx?CID=100&Show=1')

    if listed_df is None and otc_df is None:
        print('抓取失敗，無資料')
        return

    raw_date = listed_date if listed_date != '未知日期' else otc_date
    display_date = raw_date
    if raw_date and str(raw_date).isdigit():
        raw_date = str(raw_date)
        if len(raw_date) == 4:
            display_date = f'2026-{raw_date[:2]}-{raw_date[2:]}'
        elif len(raw_date) == 8:
            display_date = f'{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}'

    fallback_text = build_text_fallback(listed_df, otc_df, display_date)

    try:
        image_path = generate_parallel_rank_image(listed_df, otc_df, display_date, OUTPUT_IMAGE, top_n=TOP_N)
        with open(image_path, 'rb') as f:
            response = requests.post(
                DISCORD_WEBHOOK_URL,
                data={'content': f'📊 每週大股東籌碼強勢榜\n> 資料統計日期：{display_date}'},
                files={'file': (os.path.basename(image_path), f, 'image/png')}
            )

        if response.status_code in (200, 204):
            print('✅ 圖片推播完成！')
            return
        else:
            print(f'⚠️ 圖片推播失敗，改用文字備援：{response.status_code}')

    except Exception as e:
        print(f'⚠️ 圖片推播失敗，改用文字備援：{e}')

    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json={'content': fallback_text})
        if response.status_code == 204:
            print('✅ 文字備援推播完成！')
        else:
            print(f'❌ 文字推播失敗: {response.status_code}')
    except Exception as e:
        print(f'❌ 發送錯誤: {e}')


if __name__ == '__main__':
    push_rank_to_dc()
