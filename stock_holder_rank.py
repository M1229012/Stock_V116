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
# 從 GitHub Actions 的 Secrets 讀取
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_TEST")

def get_weekly_rank(url):
    """ 爬取並解析排行榜前 15 名 """
    options = Options()
    options.add_argument('--headless=new') # 使用新版 headless 模式
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu') 
    options.add_argument('--window-size=1920,1080') # 關鍵：設定視窗大小，避免 RWD 隱藏表格
    
    # 建立 WebDriver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    try:
        driver.get(url)
        # 等待表格出現
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        
        html = driver.page_source
        dfs = pd.read_html(StringIO(html))
        
        # 找出最大的資料表
        target_df = max(dfs, key=len)
        
        # 1. 尋找資料起始行 (透過正規表達式確認第 4 欄是否為股票代號)
        data_start_idx = -1
        for idx, row in target_df.iterrows():
            if re.search(r'\d{4}', str(row.iloc[3])):
                data_start_idx = idx
                break
        
        if data_start_idx == -1:
            return None

        # 2. 擷取前 15 名
        # 欄位索引參考：[3]=代號名稱, [5]=最新一週的「增減比例」
        raw_rows = target_df.iloc[data_start_idx : data_start_idx + 15]
        
        rank_list = []
        for _, row in raw_rows.iterrows():
            stock_info = str(row.iloc[3]).strip()
            # 欄位 5 是「大股東持有張數增減(%)」，代表當週變化
            weekly_change = str(row.iloc[5]).replace('%', '').strip()
            
            rank_list.append({
                "name": stock_info,
                "change": weekly_change
            })
        return rank_list

    except Exception as e:
        print(f"抓取失敗: {e}")
        return None
    finally:
        driver.quit()

def push_rank_to_dc():
    """ 整合上市上櫃排行並推播 """
    if not DISCORD_WEBHOOK_URL:
        print("錯誤：找不到 DISCORD_WEBHOOK_URL_TEST 環境變數，請檢查 GitHub Secrets")
        return

    print("正在處理上市排行...")
    listed = get_weekly_rank("https://norway.twsthr.info/StockHoldersTopWeek.aspx")
    
    print("正在處理上櫃排行...")
    otc = get_weekly_rank("https://norway.twsthr.info/StockHoldersTopWeek.aspx?CID=100&Show=1")

    # 建立 Discord 訊息內容
    content = "🚀 **每週大股東籌碼強勢榜 (Top 15)**\n"
    content += f"📅 統計時間：{time.strftime('%Y-%m-%d %H:%M')}\n\n"

    # 上市部分
    if listed:
        content += "🟦 **【上市排行榜 - 當週增加%】**\n"
        content += "```"
        content += f"{'排名':<2} {'股票代號/名稱':<12} {'當週增減':>8}\n"
        content += "-" * 30 + "\n"
        for i, item in enumerate(listed, 1):
            content += f"{i:<4} {item['name']:<14} {item['change']:>8}%\n"
        content += "```\n"
    
    content += "─" * 20 + "\n\n"

    # 上櫃部分
    if otc:
        content += "🟩 **【上櫃排行榜 - 當週增加%】**\n"
        content += "```"
        content += f"{'排名':<2} {'股票代號/名稱':<12} {'當週增減':>8}\n"
        content += "-" * 30 + "\n"
        for i, item in enumerate(otc, 1):
            content += f"{i:<4} {item['name']:<14} {item['change']:>8}%\n"
        content += "```"

    # 發送 Webhook
    response = requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
    if response.status_code == 204:
        print("類股排行推播完成！")
    else:
        print(f"推播失敗：{response.status_code}")

# ================= 執行區 =================
if __name__ == "__main__":
    push_rank_to_dc()
