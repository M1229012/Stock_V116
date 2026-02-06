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

# ================= 設定區 =================
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_TEST")

def get_norway_rank_logic(url):
    """
    完全依照「籌碼K線」APP 中的 get_norway_rank_data 邏輯進行爬取
    """
    options = Options()
    # 為了在 GitHub Actions 運行，這些設定必須保留，但邏輯層面完全不動
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    # 加入 User-Agent 防止被阻擋 (這是為了讓爬蟲能跑起來的必要手段)
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
                if df.apply(lambda x: x.astype(str).str.contains('大股東持有').any()).any():
                    target_df = df
                    break
        
        # 若沒找到，取最大的 (原程式碼邏輯)
        if target_df is None and len(dfs) > 0:
             target_df = max(dfs, key=len)

        if target_df is None:
            return None, None

        # 3. 依照原程式碼邏輯：定位 Header 與 Data Start Index
        header_idx = -1
        data_start_idx = -1
        
        for idx, row in target_df.iterrows():
            row_str = row.astype(str).values
            # 找股票代號 (4碼數字)
            if re.search(r'\d{4}', str(row[3])):
                data_start_idx = idx
                break
        
        if data_start_idx == -1: 
            return None, None
        
        # 往回找日期 Header
        for idx in range(max(0, data_start_idx - 5), data_start_idx):
            row = target_df.iloc[idx]
            if re.match(r'^\d{4,}$', str(row[5])): # 判斷日期格式
                header_idx = idx
                break
        
        # 4. 依照原程式碼邏輯：選取特定欄位
        # 取前 15 名 (原程式取 100，這裡為了 DC 推播取前 15)
        raw_data = target_df.iloc[data_start_idx : data_start_idx + 15].copy()
        
        col_indices = [3, 5, 6, 7, 8, 9, 10, 13, 15]
        
        # 處理日期標題 (用來顯示在 Discord)
        latest_date_str = "未知日期"
        final_cols = ["股票代號/名稱"]
        if header_idx != -1:
            date_headers = target_df.iloc[header_idx, 5:11].tolist()
            final_cols.extend([str(d) for d in date_headers])
            # 抓取最新的日期 (通常是第一個)
            if len(date_headers) > 0:
                latest_date_str = str(date_headers[0])
        else:
            final_cols.extend([f"Date_{i}" for i in range(1, 7)])
            
        final_cols.extend(["總增減", "上週持有%"])
        
        # 重組 DataFrame
        result_df = raw_data.iloc[:, col_indices]
        result_df.columns = final_cols
        
        return result_df, latest_date_str

    except Exception as e:
        print(f"爬取錯誤: {e}")
        return None, None
    finally:
        driver.quit()

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
    display_date = listed_date if listed_date != "未知日期" else otc_date

    content = "🚀 **每週大股東籌碼強勢榜 (Top 15)**\n"
    content += f"📅 **資料統計日期：{display_date}**\n"
    content += f"⏰ 抓取時間：{time.strftime('%Y-%m-%d %H:%M')}\n\n"

    def format_rank_block(df, title):
        if df is None or df.empty:
            return f"{title} ❌ 無資料\n\n"
        
        msg = f"{title}\n"
        msg += "```"
        # 這裡使用籌碼K線邏輯抓到的「總增減」欄位
        msg += f"{'排名':<2} {'股票代號/名稱':<12} {'總增減':>8}\n"
        msg += "-" * 30 + "\n"
        
        for i, row in df.iterrows():
            name = str(row['股票代號/名稱']).strip()
            # 確保內容是字串並去除多餘空格
            change = str(row['總增減']).replace(',', '').strip()
            
            # 嘗試格式化讓排版好看一點 (如果太長截斷)
            if len(name) > 12: name = name[:12]
            
            msg += f"{i+1:<4} {name:<14} {change:>8}\n"
        msg += "```\n"
        return msg

    # 上市
    content += format_rank_block(listed_df.reset_index(drop=True), "🟦 **【上市排行】**")
    
    content += "─" * 20 + "\n\n"

    # 上櫃
    content += format_rank_block(otc_df.reset_index(drop=True), "🟩 **【上櫃排行】**")

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
