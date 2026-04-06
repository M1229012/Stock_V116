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
matplotlib.use('Agg')  # 無視窗模式，CI/CD 環境必加
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib import font_manager
import re
import os

# ================= 設定區 =================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_TEST")

# ---- 字型設定 ----
# 優先嘗試系統中文字型，找不到就用預設
def get_chinese_font():
    candidates = [
        "Noto Sans CJK TC",
        "Microsoft JhengHei",
        "PingFang TC",
        "WenQuanYi Micro Hei",
    ]
    available = {f.name for f in font_manager.fontManager.ttflist}
    for name in candidates:
        if name in available:
            return name
    return None  # fallback，中文可能變方塊，建議在環境安裝 fonts-noto-cjk

FONT_NAME = get_chinese_font()

# ================= 爬蟲區（與原版相同，不動）=================

def get_norway_rank_logic(url):
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

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


def draw_table(ax, df, title, header_color, row_colors):
    """在指定 Axes 上畫一張排行榜表格"""
    ax.set_axis_off()

    # ---- 準備資料 ----
    rows = []
    change_vals = []
    for i, row in df.iterrows():
        code, name = parse_code_name(row['股票代號/名稱'])
        chg_str = fmt_change(row['總增減'])
        try:
            chg_val = float(chg_str.replace('%', ''))
        except:
            chg_val = 0
        rows.append([f"{i+1:02d}", code, name, chg_str])
        change_vals.append(chg_val)

    col_labels = ["排名", "代號", "股票名稱", "大戶增減%"]
    col_widths = [0.10, 0.15, 0.42, 0.33]

    # ---- 標題 ----
    ax.text(0.5, 1.02, title, transform=ax.transAxes,
            ha='center', va='bottom', fontsize=13, fontweight='bold',
            fontfamily=FONT_NAME, color='white')

    # ---- 畫表格 ----
    table = ax.table(
        cellText=rows,
        colLabels=col_labels,
        colWidths=col_widths,
        loc='center',
        cellLoc='center',
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.55)  # 列高倍率

    # ---- 樣式：Header ----
    for col_idx in range(len(col_labels)):
        cell = table[0, col_idx]
        cell.set_facecolor(header_color)
        cell.set_text_props(color='white', fontweight='bold', fontfamily=FONT_NAME)
        cell.set_edgecolor('#333333')

    # ---- 樣式：資料列 ----
    max_val = max(change_vals) if change_vals else 1

    for row_idx, (data_row, chg_val) in enumerate(zip(rows, change_vals), start=1):
        # 底色：斑馬紋
        bg = row_colors[row_idx % 2]

        for col_idx in range(len(col_labels)):
            cell = table[row_idx, col_idx]
            cell.set_facecolor(bg)
            cell.set_edgecolor('#444444')

            # 增減欄：正值紅色、負值綠色（台股習慣）
            if col_idx == 3:
                if chg_val > 0:
                    cell.set_text_props(color='#FF4444', fontweight='bold', fontfamily=FONT_NAME)
                elif chg_val < 0:
                    cell.set_text_props(color='#33CC66', fontweight='bold', fontfamily=FONT_NAME)
                else:
                    cell.set_text_props(color='#CCCCCC', fontfamily=FONT_NAME)
            else:
                cell.set_text_props(color='#EEEEEE', fontfamily=FONT_NAME)

            # 排名欄：前三名金色加粗
            if col_idx == 0 and row_idx <= 3:
                cell.set_text_props(color='#FFD700', fontweight='bold', fontfamily=FONT_NAME)


def generate_rank_image(listed_df, otc_df, date_str) -> BytesIO:
    """
    產生包含上市 / 上櫃兩張表格的圖片，回傳 BytesIO
    """
    # ---- 版面：左右兩欄 ----
    fig, (ax_listed, ax_otc) = plt.subplots(
        1, 2,
        figsize=(16, 12),
        facecolor='#1A1A2E'   # 深藍底色
    )
    fig.subplots_adjust(left=0.02, right=0.98, top=0.88, bottom=0.02, wspace=0.06)

    # ---- 大標題 ----
    fig.text(
        0.5, 0.95,
        f"📊 每週大股東籌碼強勢榜 Top 20　　📅 {date_str}",
        ha='center', va='center',
        fontsize=16, fontweight='bold',
        color='white', fontfamily=FONT_NAME
    )

    # ---- 上市 ----
    if listed_df is not None and not listed_df.empty:
        draw_table(
            ax_listed,
            listed_df.reset_index(drop=True),
            title="🟦 上市排行",
            header_color='#1565C0',          # 藍
            row_colors=['#16213E', '#0F3460'] # 斑馬紋
        )
    else:
        ax_listed.text(0.5, 0.5, '無資料', ha='center', va='center',
                       color='white', fontsize=14, fontfamily=FONT_NAME)
        ax_listed.set_facecolor('#1A1A2E')
        ax_listed.set_axis_off()

    # ---- 上櫃 ----
    if otc_df is not None and not otc_df.empty:
        draw_table(
            ax_otc,
            otc_df.reset_index(drop=True),
            title="🟩 上櫃排行",
            header_color='#1B5E20',          # 綠
            row_colors=['#1A2E1A', '#0F3A0F']
        )
    else:
        ax_otc.text(0.5, 0.5, '無資料', ha='center', va='center',
                    color='white', fontsize=14, fontfamily=FONT_NAME)
        ax_otc.set_facecolor('#1A1A2E')
        ax_otc.set_axis_off()

    # ---- 輸出為 BytesIO ----
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
