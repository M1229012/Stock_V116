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

# ================= 設定區 =================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_TEST")

def get_norway_rank_logic(url):
    """
    依照「籌碼K線」APP 邏輯爬取，並加入「依最新週漲幅排序」功能
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
        
        # 4. [修改部分]：抓取所有資料並依照「最新週」排序
        
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

# ================= 排版工具區 =================

# [核心功能] 計算字串的視覺寬度 (Visual Width)
# 中文字(全形) = 2, 英數字(半形) = 1
def get_visual_len(text):
    length = 0
    for char in str(text):
        if ord(char) > 127: 
            length += 2
        else:
            length += 1
    return length

# [新增功能] 智慧截斷字串
# 確保字串在視覺寬度限制內，避免切斷中文字或超出表格
def truncate_to_width(text, max_visual_width):
    text = str(text)
    current_width = 0
    new_text = ""
    for char in text:
        char_w = 2 if ord(char) > 127 else 1
        if current_width + char_w > max_visual_width:
            break
        current_width += char_w
        new_text += char
    return new_text

# [核心功能] 填充字串以達到目標視覺寬度
def pad_visual(text, target_width, align='left'):
    text = str(text)
    vis_len = get_visual_len(text)
    pad_len = max(0, target_width - vis_len)
    padding = " " * pad_len
    
    if align == 'right':
        return padding + text
    else:
        return text + padding

def push_rank_to_dc():
    if not DISCORD_WEBHOOK_URL:
        print("錯誤：找不到 DISCORD_WEBHOOK_URL_TEST 環境變數")
        return

    print("正在處理上市排行 (使用籌碼K線邏輯)...")
    listed_df, listed_date = get_norway_rank_logic("https://norway.twsthr.info/StockHoldersTopWeek.aspx")
    
    print("正在處理上櫃排行 (使用籌碼K線邏輯)...")
    otc_df, otc_date = get_norway_rank_logic("https://norway.twsthr.info/StockHoldersTopWeek.aspx?CID=100&Show=1")

    if listed_df is None and otc_df is None:
        print("抓取失敗，無資料")
        return

    # 顯示日期優先順序
    raw_date = listed_date if listed_date != "未知日期" else otc_date
    
    # [修改] 日期強制格式化: 0130 -> 2026-01-30
    display_date = raw_date
    if raw_date and raw_date.isdigit():
        if len(raw_date) == 4:
            display_date = f"2026-{raw_date[:2]}-{raw_date[2:]}"
        elif len(raw_date) == 8:
            display_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"

    # [美化] 標題區塊
    content = "📊 **籌碼K線｜每週大股東籌碼強勢榜 Top 20**\n"
    content += f"> 📅 **資料統計日期：{display_date}**\n\n"

    def format_rank_block(df, title):
        if df is None or df.empty:
            return f"{title} ❌ **無資料**\n\n"
        
        msg = f"{title}\n"
        msg += "```text\n"
        
        # [嚴格排版] 定義各欄位的「視覺寬度」
        # 調整欄位寬度以達到更佳視覺平衡
        W_RANK   = 4   # 排名
        W_CODE   = 6   # 代號
        W_NAME   = 14  # 股名 (約7個全形字)
        W_CHANGE = 11  # 總增減 (預留符號空間)
        
        # 定義 Gap (欄位間距)
        GAP = "  " 
        
        # 標題列
        h_rank = pad_visual("排名", W_RANK)
        h_code = pad_visual("代號", W_CODE)
        h_name = pad_visual("股名", W_NAME) # 靠左
        # [修改] 總增減標題改為靠左對齊
        h_chg  = pad_visual("總增減", W_CHANGE, align='left')
        
        msg += f"{h_rank}{GAP}{h_code}{GAP}{h_name}{GAP}{h_chg}\n"
        
        # 分隔線 (動態計算長度)
        total_width = W_RANK + W_CODE + W_NAME + W_CHANGE + (len(GAP) * 3)
        msg += "=" * total_width + "\n"
        
        for i, row in df.iterrows():
            raw_str = str(row['股票代號/名稱']).strip()
            
            # 分離代號與名稱
            match = re.match(r'(\d{4})\s*(.*)', raw_str)
            if match:
                code = match.group(1)
                name = match.group(2).strip()
            else:
                code = raw_str[:4]
                name = raw_str[4:].strip()
                
            change = str(row['總增減']).replace(',', '').strip()
            
            # [優化] 智慧截斷股名
            name = truncate_to_width(name, W_NAME)
            
            # [組裝] 嚴格依照指定順序與間距
            s_rank = pad_visual(f"{i+1:02d}", W_RANK) # 補零變成 01, 02
            s_code = pad_visual(code, W_CODE)
            s_name = pad_visual(name, W_NAME, align='left')
            # [修改] 數字強制靠左對齊，與標題對齊
            s_chg  = pad_visual(change, W_CHANGE, align='left')
            
            msg += f"{s_rank}{GAP}{s_code}{GAP}{s_name}{GAP}{s_chg}\n"
            
        msg += "```\n"
        return msg

    # 上市 [移除 Listed 字樣]
    content += format_rank_block(listed_df.reset_index(drop=True), "🟦 **【上市排行】**")
    
    # 上櫃 [移除 OTC 字樣]
    content += format_rank_block(otc_df.reset_index(drop=True), "🟩 **【上櫃排行】**")

    # [移除] 這裡已經刪除資料來源的 footer 程式碼

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
