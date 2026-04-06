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


def draw_clean_table(ax, df, title, accent_color, header_dark):
    """
    金融終端機風格：深色底、大字體、清晰間距
    """
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_axis_off()

    # ---- 顏色 ----
    BG_TABLE    = '#1C2B3A'        # 表格底色（深藍灰）
    BG_ROW_ODD  = '#1C2B3A'        # 奇數列
    BG_ROW_EVEN = '#243447'        # 偶數列（稍淺）
    BG_HEADER   = header_dark      # Header 深色底
    BG_RANK     = '#162030'        # 排名欄更深底色
    ACCENT      = accent_color     # 強調色（標題列左邊線 / Header 上緣線）

    TEXT_HEADER = '#FFFFFF'
    TEXT_MAIN   = '#E8EFF7'        # 主要文字（接近白）
    TEXT_MUTED  = '#8FA8C0'        # 次要文字（代號、排名）
    TEXT_POS    = '#FF6B6B'        # 正漲（亮紅）
    TEXT_NEG    = '#4CD964'        # 負跌（亮綠）
    GOLD        = '#FFD060'        # 第一名
    SILVER      = '#C0C8D4'        # 第二名
    BRONZE      = '#E8A070'        # 第三名
    BORDER_DARK = '#0D1B2A'        # 格線（深）
    BORDER_MID  = '#2E4560'        # 格線（中）

    # ---- 版面 ----
    n_rows   = len(df)
    header_h = 0.064
    total_h  = 0.91
    row_h    = (total_h - header_h) / n_rows
    top_y    = 0.975

    # ---- 欄位 ----
    col_widths = [0.09, 0.16, 0.43, 0.32]
    col_labels = ["排名", "代號", "股票名稱", "大戶增減%"]
    col_aligns = ['center', 'center', 'left', 'center']
    x_starts = []
    acc = 0
    for w in col_widths:
        x_starts.append(acc)
        acc += w

    # ---- 整體表格背景 ----
    bg_rect = patches.FancyBboxPatch(
        (0, top_y - total_h - 0.005), 1, total_h + 0.01,
        boxstyle="round,pad=0.005",
        linewidth=1.5,
        edgecolor=BORDER_MID,
        facecolor=BG_TABLE,
        transform=ax.transAxes,
        clip_on=False,
        zorder=0
    )
    ax.add_patch(bg_rect)

    # ---- 左側強調色縱條 ----
    accent_bar = patches.Rectangle(
        (0, top_y - total_h - 0.005), 0.008, total_h + 0.01,
        linewidth=0,
        facecolor=ACCENT,
        transform=ax.transAxes,
        clip_on=False,
        zorder=1
    )
    ax.add_patch(accent_bar)

    # ---- 區塊標題（表格上方） ----
    ax.text(
        0.012, top_y + 0.01,
        title,
        transform=ax.transAxes,
        ha='left', va='bottom',
        fontsize=16, fontweight='bold',
        fontproperties=FONT_PROP,
        color=ACCENT
    )

    header_top = top_y - 0.005

    # ---- Header 列 ----
    header_rect = patches.Rectangle(
        (0, header_top - header_h), 1, header_h,
        linewidth=0,
        facecolor=BG_HEADER,
        transform=ax.transAxes,
        clip_on=False,
        zorder=1
    )
    ax.add_patch(header_rect)

    # Header 上緣強調線
    ax.plot([0, 1], [header_top, header_top],
            color=ACCENT, linewidth=2,
            transform=ax.transAxes, clip_on=False, zorder=2)

    for col_i, (xst, w, label, align) in enumerate(zip(x_starts, col_widths, col_labels, col_aligns)):
        text_x = xst + w / 2 if align == 'center' else xst + 0.02
        ax.text(
            text_x, header_top - header_h / 2,
            label,
            transform=ax.transAxes,
            ha=align, va='center',
            fontsize=20, fontweight='bold',
            fontproperties=FONT_PROP,
            color=TEXT_HEADER,
            zorder=3
        )

    # ---- 資料列 ----
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
        bg_color = BG_ROW_ODD if row_i % 2 == 0 else BG_ROW_EVEN

        # 列底色
        row_rect = patches.Rectangle(
            (0, y_top - row_h), 1, row_h,
            linewidth=0,
            facecolor=bg_color,
            transform=ax.transAxes,
            clip_on=False,
            zorder=1
        )
        ax.add_patch(row_rect)

        # 列底部分隔線
        ax.plot([0.008, 1], [y_top - row_h, y_top - row_h],
                color=BORDER_DARK, linewidth=0.6,
                transform=ax.transAxes, clip_on=False, zorder=2)

        for col_i, (xst, w, val, align) in enumerate(zip(x_starts, col_widths, row_data, col_aligns)):

            # 排名欄獨立底色
            if col_i == 0:
                rank_bg = patches.Rectangle(
                    (xst, y_top - row_h), w, row_h,
                    linewidth=0,
                    facecolor=BG_RANK,
                    transform=ax.transAxes,
                    clip_on=False,
                    zorder=1
                )
                ax.add_patch(rank_bg)

            # 文字顏色
            if col_i == 0:
                if rank_num == 1:
                    txt_color, fw = GOLD, 'bold'
                elif rank_num == 2:
                    txt_color, fw = SILVER, 'bold'
                elif rank_num == 3:
                    txt_color, fw = BRONZE, 'bold'
                else:
                    txt_color, fw = TEXT_MUTED, 'normal'
                fs = 20
            elif col_i == 1:
                txt_color, fw, fs = TEXT_MUTED, 'normal', 20
            elif col_i == 2:
                txt_color, fw, fs = TEXT_MAIN, 'normal', 20
            else:  # 增減欄
                if chg_val > 0:
                    txt_color, fw = TEXT_POS, 'bold'
                elif chg_val < 0:
                    txt_color, fw = TEXT_NEG, 'bold'
                else:
                    txt_color, fw = TEXT_MUTED, 'normal'
                fs = 20  # 增減數字最大

            text_x = xst + w / 2 if align == 'center' else xst + 0.02
            ax.text(
                text_x, y_top - row_h / 2,
                val,
                transform=ax.transAxes,
                ha=align, va='center',
                fontsize=fs, fontweight=fw,
                fontproperties=FONT_PROP,
                color=txt_color,
                zorder=3
            )


def generate_rank_image(listed_df, otc_df, date_str) -> BytesIO:
    # ---- 畫布：深藍色背景 ----
    BG_MAIN    = '#0D1B2A'   # 整體深藍背景
    BG_TOPBAR  = '#0A1520'   # 頂部更深

    fig, (ax_listed, ax_otc) = plt.subplots(
        1, 2,
        figsize=(22, 16),
        facecolor=BG_MAIN
    )
    fig.subplots_adjust(left=0.025, right=0.975, top=0.88, bottom=0.025, wspace=0.06)

    # ---- 頂部橫條 ----
    top_bar = patches.Rectangle(
        (0, 0.89), 1, 0.11,
        linewidth=0,
        facecolor=BG_TOPBAR,
        transform=fig.transFigure,
        clip_on=False,
        zorder=0
    )
    fig.add_artist(top_bar)

    # 頂部色線（上市藍 | 上櫃綠 漸層分割）
    fig.add_artist(patches.Rectangle(
        (0, 0.99), 0.5, 0.01,
        linewidth=0, facecolor='#3B82F6',
        transform=fig.transFigure, clip_on=False, zorder=1
    ))
    fig.add_artist(patches.Rectangle(
        (0.5, 0.99), 0.5, 0.01,
        linewidth=0, facecolor='#22C55E',
        transform=fig.transFigure, clip_on=False, zorder=1
    ))

    # ---- 大標題 ----
    fig.text(
        0.5, 0.945,
        "每週大股東籌碼強勢榜  Top 20",
        ha='center', va='center',
        fontsize=35, fontweight='bold',
        fontproperties=FONT_PROP,
        color='#FFFFFF',
        zorder=2
    )
    fig.text(
        0.5, 0.905,
        f"資料統計日期：{date_str}",
        ha='center', va='center',
        fontsize=20,
        fontproperties=FONT_PROP,
        color='#7BA8C8',
        zorder=2
    )

    # ---- 上市 ----
    if listed_df is not None and not listed_df.empty:
        draw_clean_table(
            ax_listed,
            listed_df.reset_index(drop=True),
            title="▌ 上市排行",
            accent_color='#3B82F6',    # 藍
            header_dark='#162340'
        )
    else:
        ax_listed.text(0.5, 0.5, '無資料', ha='center', va='center',
                       fontsize=20, fontproperties=FONT_PROP, color='#7BA8C8')
        ax_listed.set_facecolor(BG_MAIN)
        ax_listed.set_axis_off()

    # ---- 上櫃 ----
    if otc_df is not None and not otc_df.empty:
        draw_clean_table(
            ax_otc,
            otc_df.reset_index(drop=True),
            title="▌ 上櫃排行",
            accent_color='#22C55E',    # 綠
            header_dark='#112318'
        )
    else:
        ax_otc.text(0.5, 0.5, '無資料', ha='center', va='center',
                    fontsize=20, fontproperties=FONT_PROP, color='#7BA8C8')
        ax_otc.set_facecolor(BG_MAIN)
        ax_otc.set_axis_off()

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
