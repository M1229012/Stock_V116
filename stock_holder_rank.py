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
import time
import os
from datetime import datetime
from wcwidth import wcwidth
import unicodedata

# ================= 設定區 =================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_TEST")

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
        result_df['總增減'] = top20_data.iloc[:, latest_date_col_idx] 
        
        return result_df, latest_date_str

    except Exception as e:
        print(f"爬取錯誤: {e}")
        return None, None
    finally:
        driver.quit()

# ================= 排版工具區 (終極對齊修正版) =================

_ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u202a-\u202e\ufeff]")

def clean_cell(s) -> str:
    s = "" if s is None else str(s)
    s = unicodedata.normalize("NFKC", s)     # 統一全/半形
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

# [修正功能] 填充字串 (使用全形空白 \u3000 修正對齊)
def pad_visual(s, target_w: int, align="left") -> str:
    s = truncate_to_width(s, target_w)
    vis_len = visual_len(s)
    
    # 計算還差多少寬度
    diff = max(0, target_w - vis_len)
    
    # [魔法修正] 
    # 因為 1 個中文字(寬度2) 通常比 2 個半形空白寬
    # 所以每差 2 個單位，我們直接補 1 個「全形空白(\u3000)」
    # 這樣才能跟中文字完美對齊，防止數字欄位飄移
    full_spaces = diff // 2
    half_spaces = diff % 2
    
    padding = "\u3000" * full_spaces + " " * half_spaces
    
    if align == "right":
        return padding + s
    return s + padding

# [保留] 數值標準化格式
def fmt_change(x):
    s = str(x)
    s = s.replace('%', '').replace(',', '')
    s = re.sub(r'\s+', '', s)  # 清掉各種奇怪空白（含不可見空白）
    v = pd.to_numeric(s, errors='coerce')
    return "-" if pd.isna(v) else f"{v:.2f}"

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

    # [移除] 移除所有品牌字眼
    content = "📊 **每週大股東籌碼強勢榜 Top 20**\n"
    content += f"> 📅 **資料統計日期：{display_date}**\n\n"

    def format_rank_block(df, title):
        if df is None or df.empty:
            return f"{title} ❌ **無資料**\n\n"
        
        msg = f"{title}\n"
        msg += "```text\n"
        
        # [嚴格排版] 定義視覺寬度
        # W_NAME 設為 16 (約8個字)
        W_RANK   = 4 
        W_CODE   = 6 
        W_NAME   = 16 
        W_CHANGE = 10 
        
        # 定義 Gap (使用全形空白 \u3000 做間隔，對齊最穩)
        GAP = "\u3000"
        
        # 標題列
        h_rank = pad_visual("排名", W_RANK)
        h_code = pad_visual("代號", W_CODE)
        h_name = pad_visual("股名", W_NAME)
        # [重點] 總增減標題強制靠左
        h_chg  = pad_visual("總增減", W_CHANGE, align='left') 
        
        msg += f"{h_rank}{GAP}{h_code}{GAP}{h_name}{GAP}{h_chg}\n"
        
        # 分隔線
        msg += "=" * 42 + "\n"
        
        for i, row in df.iterrows():
            # 先清洗隱藏字元
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
            change_str = fmt_change(row['總增減'])
            
            # 截斷股名
            name = truncate_to_width(name, W_NAME)
            
            # [組裝] 
            s_rank = pad_visual(f"{i+1:02d}", W_RANK) 
            s_code = pad_visual(code, W_CODE)
            # 股名靠左 (右側會補上全形空白)
            s_name = pad_visual(name, W_NAME, align='left')
            
            # [重點] 數字強制靠左對齊
            # 由於前方 s_name 寬度已被全形空白完美鎖定，這裡的數字會筆直對齊
            s_chg  = pad_visual(change_str, W_CHANGE, align='left')
            
            msg += f"{s_rank}{GAP}{s_code}{GAP}{s_name}{GAP}{s_chg}\n"
            
        msg += "```\n"
        return msg

    # [移除] 移除 Listed/OTC 字樣
    content += format_rank_block(listed_df.reset_index(drop=True), "🟦 **【上市排行】**")
    content += format_rank_block(otc_df.reset_index(drop=True), "🟩 **【上櫃排行】**")

    # [移除] 底部資料來源已刪除

    # 發送
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
        if response.status_code == 204:
            print("✅ 推播完成！")
        else:
            print(f"❌ 推播失敗: {response.status_code}")
    except Exception as e:
        print(f"❌ 發送錯誤: {e}")

if __name__ == "__main__":
    push_rank_to_dc()
