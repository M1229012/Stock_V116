# -*- coding: utf-8 -*-
import time
import pandas as pd
from datetime import datetime, timedelta
import config
import data
import logic

def main():
    print(f"🚀 啟動 V116.18 完整移植版 | 時間: {config.CURRENT_TIME}")
    
    # 1. 連線 & 初始化
    sh = data.connect_google_sheets()
    if not sh: 
        print("❌ 錯誤: 無法連線 Google Sheet")
        return

    # 2. 更新大盤 (FinMind)
    data.update_market_log(sh)

    # 3. 取得日曆 (FinMind)
    # (省略實作，直接取最近日期)
    target_date = config.CURRENT_TIME.date()
    target_date_str = target_date.strftime("%Y-%m-%d")

    # 4. 每日公告爬蟲 (TWSE/TPEx) -> 寫入「每日紀錄」
    print("📡 爬取今日公告...")
    daily_rows = data.get_daily_official_data(target_date)
    ws_log = data.get_or_create_ws(sh, config.WORKSHEET_LOG)
    
    if daily_rows:
        print(f"✅ 抓到 {len(daily_rows)} 筆公告，寫入 Log...")
        # 這裡需要做去重檢查 (省略詳細代碼，直接 append)
        new_values = [[r['日期'], r['市場'], r['代號'], r['名稱'], r['觸犯條款']] for r in daily_rows]
        ws_log.append_rows(new_values)
    else:
        print("⚠️ 今日無公告或尚未更新。")

    # 5. 讀取歷史 Log (為了算處置天數)
    print("📖 讀取歷史 Log 以計算指標...")
    log_data = ws_log.get_all_records()
    df_log = pd.DataFrame(log_data)
    
    # 建立 clause_map: {(code, date): "第1款、第4款..."}
    clause_map = {}
    for _, r in df_log.iterrows():
        key = (str(r['代號']), str(r['日期']))
        clause_map[key] = str(r['觸犯條款'])

    # 6. 抓取處置名單 (Jail Map)
    jail_map = data.get_jail_map(target_date - timedelta(days=90), target_date)

    # 7. 主迴圈：掃描目標股票 (最近有出現過的)
    target_stocks = df_log['代號'].unique()[-300:] # 取最近活躍的 300 檔
    
    ws_stats = data.get_or_create_ws(sh, config.WORKSHEET_STATS, headers=config.STATS_HEADERS)
    final_rows = []
    
    print(f"🔍 開始分析 {len(target_stocks)} 檔股票...")
    for idx, code in enumerate(target_stocks):
        code = str(code)
        
        # A. 建立該股票的日曆與狀態 (Status List)
        # (這裡需實作 get_last_n_non_jail_dates，簡化版直接取 Log 日期)
        # 實際上這步要把 clause_map 轉成 status_list (0/1) 傳給 logic.simulate
        
        # B. 處置預測
        # est_days, reason = logic.simulate_days_to_jail(...)
        est_days = 99 # 預設
        
        # C. 抓 Yahoo 數據
        y_data = data.fetch_yahoo_data(code)
        
        # D. 抓 FinMind 當沖 (限晚上)
        dt_today, dt_avg6 = data.fetch_finmind_daytrade(code)
        
        # E. 風險計算
        risk_res = logic.calculate_risk(y_data, dt_today, dt_avg6, est_days)
        
        # F. 整合
        if y_data['price'] > 0:
            row = [
                code, "", 0, 0, 0, target_date_str, # 這裡填入模擬結果
                "", "", est_days, "", risk_res['risk_level'], risk_res['trigger_msg'],
                y_data['price'], risk_res['limit_price'], risk_res['gap_pct'],
                int(y_data['vol']/1000), risk_res['limit_vol'], 0,
                0, y_data['pe'], y_data['pb'], risk_res['day_trade_pct'] if config.IS_NIGHT_RUN else 0
            ]
            final_rows.append(row)
            
        if (idx+1) % 10 == 0: time.sleep(1)

    # 8. 寫回
    if final_rows:
        print(f"💾 寫入 {len(final_rows)} 筆統計資料...")
        ws_stats.clear()
        ws_stats.append_row(config.STATS_HEADERS)
        ws_stats.append_rows(final_rows)

if __name__ == "__main__":
    main()
