# -*- coding: utf-8 -*-
import time
import pandas as pd
from datetime import timedelta
import config
import data
import logic

def main():
    print(f"🚀 啟動 V116.18 模組化復刻版 (純文字輸出) | {config.CURRENT_TIME}")
    sh = data.connect_google_sheets()
    if not sh: return

    # 1. 更新大盤
    data.update_market_monitoring_log(sh)

    # 2. 處理日曆與爬蟲 (回朔機制)
    cal_dates = data.get_official_trading_calendar(240)
    target_date_obj = cal_dates[-1]
    
    # 爬取今日公告
    official_stocks = data.get_daily_data(target_date_obj)
    
    # 判斷是否需要回朔
    is_today = (target_date_obj == config.TARGET_DATE.date())
    is_early = (config.TARGET_DATE.time() < config.SAFE_CRAWL_TIME)
    
    if (not official_stocks) and is_today and is_early:
        print("🔄 啟動回朔 (T-1)...")
        if len(cal_dates) >= 2:
            target_date_obj = cal_dates[-2]
            official_stocks = data.get_daily_data(target_date_obj)
            cal_dates = cal_dates[:-1]

    target_date_str = target_date_obj.strftime("%Y-%m-%d")
    print(f"📅 鎖定日期: {target_date_str}")

    # 3. 寫入 Log
    ws_log = data.get_or_create_ws(sh, "每日紀錄", headers=['日期','市場','代號','名稱','觸犯條款'])
    if official_stocks:
        print("💾 寫入每日紀錄...")
        # 🔥 [修正] 代號加上 ' 強制為文字，其他欄位轉 str
        rows = [[str(s['日期']), str(s['市場']), f"'{s['代號']}", str(s['名稱']), str(s['觸犯條款'])] for s in official_stocks]
        ws_log.append_rows(rows, value_input_option='USER_ENTERED')

    # 4. 準備掃描
    print("📊 讀取歷史 Log...")
    log_data = ws_log.get_all_records()
    df_log = pd.DataFrame(log_data)
    
    if not df_log.empty:
        df_log['代號'] = df_log['代號'].astype(str).str.strip().str.replace("'", "") # 讀取時去掉單引號以便比對
        df_log['日期'] = df_log['日期'].astype(str).str.strip()

    clause_map = {}
    for _, r in df_log.iterrows():
        key = (str(r['代號']), str(r['日期']))
        clause_map[key] = logic.merge_clause_text(clause_map.get(key,""), str(r['觸犯條款']))

    # 5. 處置名單
    jail_map = data.get_jail_map(target_date_obj - timedelta(days=90), target_date_obj)
    
    # 6. 掃描目標
    start_dt_str = cal_dates[-90].strftime("%Y-%m-%d")
    df_recent = df_log[df_log['日期'] >= start_dt_str]
    target_stocks = df_recent['代號'].unique()
    
    precise_db = data.load_precise_db_from_sheet(sh)
    rows_stats = []
    
    print(f"🔍 掃描 {len(target_stocks)} 檔股票...")
    for idx, code in enumerate(target_stocks):
        code = str(code).strip()
        
        name_series = df_log[df_log['代號'] == code]['名稱']
        name = name_series.iloc[-1] if not name_series.empty else "未知"
        
        # A. 建立日曆
        valid_dates = data.get_last_n_non_jail_trade_dates(code, cal_dates, jail_map)
        
        bits = []; clauses = []
        for d in valid_dates:
            d_str = d.strftime("%Y-%m-%d")
            c = clause_map.get((code, d_str), "")
            bits.append(1 if c else 0)
            clauses.append(c)
            
        # B. 處置預測
        est_days, reason = logic.simulate_days_to_jail_strict(
            bits, clauses, stock_id=code, target_date=target_date_obj, jail_map=jail_map
        )
        
        # C. 抓 Yahoo
        suffix = '.TWO' if '上櫃' in precise_db.get(code,{}).get('market','') else '.TW'
        hist = data.fetch_history_data(f"{code}{suffix}")
        fund = data.fetch_stock_fundamental(code, f"{code}{suffix}", precise_db)
        
        # D. 抓 FinMind
        dt_today, dt_avg6 = 0.0, 0.0
        if config.IS_NIGHT_RUN:
            dt_today, dt_avg6 = data.get_daytrade_stats_finmind(code, target_date_str)
            
        # E. 風險計算
        risk = logic.calculate_full_risk(code, hist, fund, est_days, dt_today, dt_avg6)
        
        # F. 整合 (🔥 強制轉文字區塊)
        status_30_str = "".join([str(1 if logic.is_valid_accumulation_day(logic.parse_clause_ids_strict(c)) else 0) for c in clauses])
        status_30_full = status_30_str.zfill(30)
        status_10_sub = status_30_full[-10:]
        last_date = valid_dates[-1].strftime("%Y-%m-%d") if valid_dates else "無"
        
        row = [
            f"'{code}",           # [文字] 代號 (加單引號)
            str(name),            # [文字] 名稱
            "0",                  # [文字] 連續天數 (TODO: 若需計算需補上 streak 邏輯)
            str(sum(bits)),       # [文字] 30日次數
            str(sum(bits[-10:])), # [文字] 10日次數
            str(last_date),       # [文字] 日期
            f"'{status_30_full}", # [文字] 30日狀態碼 (加單引號，防止 leading zero 消失)
            f"'{status_10_sub}",  # [文字] 10日狀態碼 (加單引號)
            str(est_days),        # [文字] 最快天數
            str(reason),          # [文字] 原因
            str(risk['risk_level']),
            str(risk['trigger_msg']),
            str(risk['curr_price']),
            str(risk['limit_price']),
            str(risk['gap_pct']),
            str(risk['curr_vol']),
            str(risk['limit_vol']),
            str(risk['turnover_val']),
            str(risk['turnover_rate']),
            str(risk['pe']),
            str(risk['pb']),
            str(risk['day_trade_pct'])
        ]
        rows_stats.append(row)
        
        if (idx+1)%10 == 0: time.sleep(1)

    # 7. 寫回
    if rows_stats:
        print("💾 更新統計表...")
        ws_stats = data.get_or_create_ws(sh, "近30日熱門統計", headers=config.STATS_HEADERS)
        ws_stats.clear()
        ws_stats.append_row(config.STATS_HEADERS, value_input_option='USER_ENTERED')
        # USER_ENTERED 會識別我們加的單引號 '，將其視為強制文字格式
        ws_stats.append_rows(rows_stats, value_input_option='USER_ENTERED')
        print("✅ 完成")

if __name__ == "__main__":
    main()
