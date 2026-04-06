import requests
import pandas as pd
from io import StringIO, BytesIO
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib import font_manager
import re
import os

# ================= 設定區 =================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_TEST")

# ---- 字型設定 ----
def load_chinese_font():
    search_paths = [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/noto-cjk/NotoSansCJKtc-Regular.otf",
        "/usr/share/fonts/opentype/noto/NotoSansCJKtc-Regular.otf",
        "/usr/local/share/fonts/NotoSansCJKtc-Regular.otf",
        "C:/Windows/Fonts/msjh.ttc",
        "C:/Windows/Fonts/mingliu.ttc",
        "/System/Library/Fonts/PingFang.ttc",
        "/Library/Fonts/Arial Unicode MS.ttf",
    ]
    for path in search_paths:
        if os.path.exists(path):
            print(f"✅ 找到字型：{path}")
            font_manager.fontManager.addfont(path)
            return font_manager.FontProperties(fname=path)
    print("⚠️ 找不到中文字型，中文可能顯示為方塊")
    return font_manager.FontProperties()

FONT_PROP = load_chinese_font()

# ================= 爬蟲區 =================

def get_norway_rank_logic(url):
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
            return None, None

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
                except:
                    continue

        raw_data = target_df.iloc[data_start_idx:].copy()

        def parse_pct(x):
            try:
                return float(str(x).replace('%', '').replace(',', ''))
            except:
                return -999999.0

        raw_data['_sort_val'] = raw_data.iloc[:, latest_date_col_idx].apply(parse_pct)
        top20_data = raw_data.sort_values(by='_sort_val', ascending=False).head(20)

        result_df = pd.DataFrame()
        result_df['股票代號/名稱'] = top20_data.iloc[:, 3]
        result_df['總增減'] = top20_data.iloc[:, latest_date_col_idx]
        return result_df, latest_date_str

    except Exception as e:
        print(f"爬取錯誤: {e}")
        return None, None
    finally:
        driver.quit()

# ================= 圖片生成區 =================

def fmt_change(x):
    s = str(x).replace('%', '').replace(',', '')
    v = pd.to_numeric(s, errors='coerce')
    return "-" if pd.isna(v) else f"{v:.2f}%"


def parse_code_name(raw_str):
    raw_str = str(raw_str).strip().replace("卅卅", "碁")
    match = re.match(r'(\d{4})\s*(.*)', raw_str)
    if match:
        return match.group(1), match.group(2).strip()
    return raw_str[:4], raw_str[4:].strip()


def draw_clean_table(ax, df, title, accent_color):
    """
    仿照圖片風格：白底、藍色/綠色 Header、斑馬紋淺灰列
    """
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_axis_off()

    # ---- 顏色定義 ----
    COLOR_HEADER_BG   = accent_color       # Header 底色
    COLOR_HEADER_TEXT = '#FFFFFF'          # Header 文字
    COLOR_ROW_ODD     = '#FFFFFF'          # 奇數列底色
    COLOR_ROW_EVEN    = '#F0F4FF'          # 偶數列底色（極淺藍）
    COLOR_TEXT        = '#1A1A1A'          # 一般文字
    COLOR_POSITIVE    = '#CC0000'          # 正漲（台股紅）
    COLOR_NEGATIVE    = '#006600'          # 負跌（台股綠）
    COLOR_GOLD        = '#B8860B'          # 前三名
    COLOR_BORDER      = '#D0D0D0'          # 格線

    # ---- 版面參數 ----
    n_rows   = len(df)
    header_h = 0.052       # Header 列高
    row_h    = (0.92 - header_h) / n_rows  # 每資料列高度
    top_y    = 0.96        # 從頂端開始畫

    # ---- 欄位寬度（比例）& X 起點 ----
    col_widths = [0.10, 0.16, 0.44, 0.30]
    col_labels = ["排名", "代號", "股票名稱", "大戶增減%"]
    col_aligns = ['center', 'center', 'left', 'center']
    x_starts = []
    acc = 0
    for w in col_widths:
        x_starts.append(acc)
        acc += w

    # ---- 標題列（區塊標題）----
    title_y = top_y + 0.005
    ax.text(
        0.0, title_y, title,
        transform=ax.transAxes,
        ha='left', va='bottom',
        fontsize=11, fontweight='bold',
        fontproperties=FONT_PROP,
        color=accent_color
    )

    header_top = top_y - 0.01

    # ---- 畫 Header ----
    for col_i, (xst, w, label, align) in enumerate(zip(x_starts, col_widths, col_labels, col_aligns)):
        rect = patches.FancyBboxPatch(
            (xst, header_top - header_h), w, header_h,
            boxstyle="square,pad=0",
            linewidth=0.5,
            edgecolor=COLOR_BORDER,
            facecolor=COLOR_HEADER_BG,
            transform=ax.transAxes,
            clip_on=False
        )
        ax.add_patch(rect)

        text_x = xst + w / 2 if align == 'center' else xst + 0.01
        ax.text(
            text_x, header_top - header_h / 2,
            label,
            transform=ax.transAxes,
            ha=align, va='center',
            fontsize=9.5, fontweight='bold',
            fontproperties=FONT_PROP,
            color=COLOR_HEADER_TEXT
        )

    # ---- 畫資料列 ----
    for row_i, (_, row) in enumerate(df.iterrows()):
        code, name = parse_code_name(row['股票代號/名稱'])
        chg_str = fmt_change(row['總增減'])
        try:
            chg_val = float(chg_str.replace('%', ''))
        except:
            chg_val = 0

        rank_num = row_i + 1
        row_data = [f"{rank_num:02d}", code, name, chg_str]

        y_top = header_top - header_h - row_i * row_h
        bg_color = COLOR_ROW_ODD if row_i % 2 == 0 else COLOR_ROW_EVEN

        for col_i, (xst, w, val, align) in enumerate(zip(x_starts, col_widths, row_data, col_aligns)):
            # 底色
            rect = patches.Rectangle(
                (xst, y_top - row_h), w, row_h,
                linewidth=0.5,
                edgecolor=COLOR_BORDER,
                facecolor=bg_color,
                transform=ax.transAxes,
                clip_on=False
            )
            ax.add_patch(rect)

            # 文字顏色邏輯
            if col_i == 0:  # 排名欄
                if rank_num <= 3:
                    txt_color = COLOR_GOLD
                    fw = 'bold'
                else:
                    txt_color = COLOR_TEXT
                    fw = 'normal'
            elif col_i == 3:  # 增減欄
                if chg_val > 0:
                    txt_color = COLOR_POSITIVE
                    fw = 'bold'
                elif chg_val < 0:
                    txt_color = COLOR_NEGATIVE
                    fw = 'bold'
                else:
                    txt_color = COLOR_TEXT
                    fw = 'normal'
            else:
                txt_color = COLOR_TEXT
                fw = 'normal'

            text_x = xst + w / 2 if align == 'center' else xst + 0.015
            ax.text(
                text_x, y_top - row_h / 2,
                val,
                transform=ax.transAxes,
                ha=align, va='center',
                fontsize=9, fontweight=fw,
                fontproperties=FONT_PROP,
                color=txt_color
            )


def generate_rank_image(listed_df, otc_df, date_str) -> BytesIO:
    """
    產生包含上市 / 上櫃兩張表格的圖片，回傳 BytesIO
    白底乾淨版
    """
    fig, (ax_listed, ax_otc) = plt.subplots(
        1, 2,
        figsize=(16, 13),
        facecolor='#FFFFFF'
    )
    fig.subplots_adjust(left=0.02, right=0.98, top=0.93, bottom=0.01, wspace=0.08)

    # ---- 大標題 ----
    fig.text(
        0.5, 0.97,
        f"每週大股東籌碼強勢榜  Top 20",
        ha='center', va='center',
        fontsize=17, fontweight='bold',
        fontproperties=FONT_PROP,
        color='#1A1A2E'
    )
    fig.text(
        0.5, 0.945,
        f"資料統計日期：{date_str}",
        ha='center', va='center',
        fontsize=10,
        fontproperties=FONT_PROP,
        color='#555555'
    )

    # ---- 上市 ----
    if listed_df is not None and not listed_df.empty:
        draw_clean_table(
            ax_listed,
            listed_df.reset_index(drop=True),
            title="▌ 上市排行",
            accent_color='#2563EB'   # 藍色系
        )
    else:
        ax_listed.text(0.5, 0.5, '無資料', ha='center', va='center',
                       fontsize=14, fontproperties=FONT_PROP, color='#888888')
        ax_listed.set_facecolor('#FFFFFF')
        ax_listed.set_axis_off()

    # ---- 上櫃 ----
    if otc_df is not None and not otc_df.empty:
        draw_clean_table(
            ax_otc,
            otc_df.reset_index(drop=True),
            title="▌ 上櫃排行",
            accent_color='#16A34A'   # 綠色系
        )
    else:
        ax_otc.text(0.5, 0.5, '無資料', ha='center', va='center',
                    fontsize=14, fontproperties=FONT_PROP, color='#888888')
        ax_otc.set_facecolor('#FFFFFF')
        ax_otc.set_axis_off()

    # ---- 底部註記 ----
    fig.text(
        0.5, 0.005,
        "資料來源：norway.twsthr.info　　紅字＝大戶增加　綠字＝大戶減少　金字＝前三名",
        ha='center', va='bottom',
        fontsize=8, fontproperties=FONT_PROP, color='#999999'
    )

    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf


# ================= 主流程 =================

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

    raw_date = listed_date if listed_date != "未知日期" else otc_date
    display_date = raw_date
    if raw_date and raw_date.isdigit():
        if len(raw_date) == 4:
            display_date = f"2026-{raw_date[:2]}-{raw_date[2:]}"
        elif len(raw_date) == 8:
            display_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"

    print("正在生成圖片...")
    img_buf = generate_rank_image(listed_df, otc_df, display_date)

    # ---- 發送圖片到 Discord ----
    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL,
            data={"content": "📊 每週大股東籌碼強勢榜 Top 20"},
            files={"file": ("rank_chart.png", img_buf, "image/png")}
        )
        if response.status_code in (200, 204):
            print("✅ 圖片推播完成！")
        else:
            print(f"❌ 推播失敗: {response.status_code} {response.text}")
    except Exception as e:
        print(f"❌ 發送錯誤: {e}")


if __name__ == "__main__":
    push_rank_to_dc()
