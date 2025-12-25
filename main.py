# -*- coding: utf-8 -*-
import time
import pandas as pd
from datetime import timedelta
import config
import data
import logic

def main():
    print(f"🚀 啟動 V116.18 模組化復刻版 | {config.CURRENT_TIME}")
    sh = data.connect_google_sheets()
    if not sh: return

    # 1. 更新大盤
    data.update_market_monitoring_log(sh)

    # 2. 處理日曆與爬蟲 (回朔機制)
    cal_dates = data.get_official_trading_calendar(240)
    target_date_obj = cal_dates[-1]
    
    # 爬取今日公告
    official_stocks = data.get_daily_data(target_date_obj)
    
    # 判斷是否需要回朔 (若今日沒資料且時間尚早)
    is_today = (target_date_obj == config.TARGET_DATE.date())
    is_early = (config.TARGET_DATE.time() < config.SAFE_CRAWL_TIME)
    
    if (not official_stocks) and is_today and is_early:
        print("🔄 啟動回朔 (T-1)...")
        if len(cal_dates) >= 2:
            target_date_obj = cal_dates[-2]
            official_stocks = data.get_daily_data(target_date_obj)
            cal_dates = cal_dates[:-1] # 調整日曆

    target_date_str = target_date_obj.strftime("%Y-%m-%d")
    print(f"📅 鎖定日期: {target_date_str}")

    # 3. 寫入 Log
    ws_log = data.get_or_create_ws(sh, "每日紀錄", headers=['日期','市場','代號','名稱','觸犯條款'])
    if official_stocks:
        print("💾 寫入每日紀錄...")
        # 這裡簡化去重檢查，直接寫入 (V116.18 原版有複雜的檢查，這裡為確保不重複可略過或直接 append)
        rows = [[s['日期'], s['市場'], s['代號'], s['名稱'], s['觸犯條款']] for s in official_stocks]
        ws_log.append_rows(rows, value_input_option='USER_ENTERED')

    # 4. 準備掃描 (讀取歷史 Log)
    print("📊 讀取歷史 Log...")
    log_data = ws_log.get_all_records()
    df_log = pd.DataFrame(log_data)
    
    clause_map = {}
    for _, r in df_log.iterrows():
        key = (str(r['代號']), str(r['日期']))
        clause_map[key] = logic.merge_clause_text(clause_map.get(key,""), str(r['觸犯條款']))

    # 5. 處置名單
    jail_map = data.get_jail_map(target_date_obj - timedelta(days=90), target_date_obj)
    
    # 6. 掃描目標 (最近 90 天出現過的)
    df_recent = df_log[pd.to_datetime(df_log['日期']) >= pd.Timestamp(cal_dates[-90])]
    target_stocks = df_recent['代號'].unique()
    
    precise_db = data.load_precise_db_from_sheet(sh)
    rows_stats = []
    
    print(f"🔍 掃描 {len(target_stocks)} 檔股票...")
    for idx, code in enumerate(target_stocks):
        code = str(code).strip()
        name = df_log[df_log['代號']==code]['名稱'].iloc[-1]
        
        # A. 建立日曆 (排除處置日)
        valid_dates = data.get_last_n_non_jail_trade_dates(code, cal_dates, jail_map)
        
        bits = []; clauses = []
        for d in valid_dates:
            d_str = d.strftime("%Y-%m-%d")
            c = clause_map.get((code, d_str), "")
            bits.append(1 if c else 0)
            clauses.append(c)
            
        # B. 處置預測 (Logic)
        est_days, reason = logic.simulate_days_to_jail_strict(
            bits, clauses, stock_id=code, target_date=target_date_obj, jail_map=jail_map
        )
        
        # C. 抓 Yahoo (全時段)
        suffix = '.TWO' if '上櫃' in precise_db.get(code,{}).get('market','') else '.TW'
        hist = data.fetch_history_data(f"{code}{suffix}")
        fund = data.fetch_stock_fundamental(code, f"{code}{suffix}", precise_db)
        
        # D. 抓 FinMind (限晚上)
        dt_today, dt_avg6 = 0.0, 0.0
        if config.IS_NIGHT_RUN:
            dt_today, dt_avg6 = data.get_daytrade_stats_finmind(code, target_date_str)
            
        # E. 風險計算 (Logic)
        risk = logic.calculate_full_risk(code, hist, fund, est_days, dt_today, dt_avg6)
        
        # F. 整合
        status_30 = "".join([str(1 if logic.is_valid_accumulation_day(logic.parse_clause_ids_strict(c)) else 0) for c in clauses])
        last_date = valid_dates[-1].strftime("%Y-%m-%d") if valid_dates else "無"
        
        row = [
            code, name, 0, sum(bits), sum(bits[-10:]), last_date,
            status_30.zfill(30), status_30[-10:], str(est_days), reason,
            risk['risk_level'], risk['trigger_msg'],
            risk['curr_price'], risk['limit_price'], risk['gap_pct'],
            risk['curr_vol'], risk['limit_vol'], risk['turnover_val'],
            risk['turnover_rate'], risk['pe'], risk['pb'], risk['day_trade_pct']
        ]
        rows_stats.append(row)
        
        if (idx+1)%10 == 0: time.sleep(1)

    # 7. 寫回
    if rows_stats:
        print("💾 更新統計表...")
        ws_stats = data.get_or_create_ws(sh, "近30日熱門統計", headers=config.STATS_HEADERS)
        ws_stats.clear()
        ws_stats.append_row(config.STATS_HEADERS, value_input_option='USER_ENTERED')
        ws_stats.append_rows(rows_stats, value_input_option='USER_ENTERED')
        print("✅ 完成")

if __name__ == "__main__":
    main()
