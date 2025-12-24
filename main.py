# -*- coding: utf-8 -*-
import sys
import os
import time
import pandas as pd
from datetime import timedelta

# 確保引用路徑
sys.path.append(os.getcwd())

import config
import utils
import data
import logic

def main():
    print(f"🚀 啟動 V116.18 模組化版本 (Zeabur Fix)")
    
    # 1. 連線
    sh, _ = data.connect_google_sheets()
    if not sh: return

    # 2. 更新大盤
    target_date_obj = config.get_target_date()
    data.update_market_monitoring_log(sh, target_date_obj)

    # 3. 取得日曆
    cal_dates = data.get_official_trading_calendar(240, target_date_obj)
    target_trade_date_obj = cal_dates[-1]
    
    # 4. 抓公告
    official_stocks = data.get_daily_data(target_trade_date_obj)
    
    # 回朔機制
    is_today = (target_trade_date_obj == target_date_obj.date())
    is_early = (target_date_obj.time() < config.SAFE_CRAWL_TIME)
    if (official_stocks == []) and is_today and is_early:
        if len(cal_dates) >= 2:
            print("🔄 啟動回朔 (T-1)...")
            cal_dates = cal_dates[:-1]
            target_trade_date_obj = cal_dates[-1]
            official_stocks = data.get_daily_data(target_trade_date_obj)
    
    target_date_str = target_trade_date_obj.strftime("%Y-%m-%d")
    print(f"📅 鎖定日期: {target_date_str}")

    # 5. 寫入 Log
    ws_log = utils.get_or_create_ws(sh, "每日紀錄", headers=['日期','市場','代號','名稱','觸犯條款'])
    total_log_rows = len(ws_log.col_values(1))
    
    if official_stocks:
        print("💾 寫入資料庫...")
        existing_keys = set()
        if total_log_rows > 1:
            try:
                start = max(1, total_log_rows - 3000)
                for r in ws_log.get(f'A{start}:E{total_log_rows}'):
                    if len(r) >= 3 and r[0] != '日期': existing_keys.add(f"{r[0].strip()}_{r[2].strip()}")
            except: pass
        
        new_rows = []
        for s in official_stocks:
            if f"{s['日期']}_{s['代號']}" not in existing_keys:
                new_rows.append([s['日期'], s['市場'], s['代號'], s['名稱'], s['觸犯條款']])
        if new_rows:
            ws_log.append_rows(new_rows, value_input_option='USER_ENTERED')
            total_log_rows += len(new_rows)

    # 6. 載入資料
    precise_db = data.load_precise_db_from_sheet(sh)
    print("📊 讀取歷史 Log...")
    start_idx = max(1, total_log_rows - 8000)
    raw_vals = ws_log.get(f'A{start_idx}:E{total_log_rows}')
    if start_idx > 1: raw_vals = ws_log.get('A1:E1') + raw_vals
    
    df = pd.DataFrame(raw_vals[1:], columns=raw_vals[0])
    df['日期'] = pd.to_datetime(df['日期'], errors='coerce').dt.date
    df = df[pd.notna(df['日期'])]
    
    clause_map = {}
    for _, r in df.iterrows():
        try:
            k = (str(r['代號']).strip(), r['日期'])
            clause_map[k] = utils.merge_clause_text(clause_map.get(k, ""), str(r.get('觸犯條款','')))
        except: pass

    # 7. 準備掃描
    start_date_90 = cal_dates[-90] if len(cal_dates)>=90 else cal_dates[0]
    df_recent = df[df['日期'] >= start_date_90]
    target_stocks = df_recent['代號'].unique()
    
    jail_lookback = target_trade_date_obj - timedelta(days=90)
    jail_map = data.get_jail_map(jail_lookback, target_trade_date_obj)
    exclude_map = logic.build_exclude_map(cal_dates, jail_map)

    print(f"🔍 掃描 {len(target_stocks)} 檔股票...")
    rows_stats = []

    for idx, code in enumerate(target_stocks):
        code = str(code).strip()
        name = df[df['代號']==code]['名稱'].iloc[-1]
        
        db_info = precise_db.get(code, {})
        suffix = utils.get_ticker_suffix(db_info.get('market', '上市'))
        ticker_code = f"{code}{suffix}"

        # Logic: Calendar
        cal_30 = logic.get_last_n_non_jail_trade_dates(code, cal_dates, jail_map, exclude_map, 30)
        
        bits = []; clauses = []
        for d in cal_30:
            c_str = clause_map.get((code, d), "")
            if logic.is_excluded(code, d, exclude_map):
                bits.append(0); clauses.append(c_str)
            elif c_str:
                bits.append(1); clauses.append(c_str)
            else:
                bits.append(0); clauses.append("")
        
        valid_bits = [1 if (b==1 and logic.is_valid_accumulation_day(utils.parse_clause_ids_strict(c))) else 0 for b, c in zip(bits, clauses)]
        status_30 = "".join(map(str, valid_bits)).zfill(30)

        # Logic: Prediction
        est_days, reason = logic.simulate_days_to_jail_strict(
            bits, clauses, stock_id=code, target_date=target_trade_date_obj, jail_map=jail_map, enable_safe_filter=False
        )
        
        # Logic: Flags
        latest_ids = utils.parse_clause_ids_strict(clauses[-1] if clauses else "")
        is_risk = logic.is_special_risk_day(latest_ids)
        is_c13 = any(13 in utils.parse_clause_ids_strict(c) for c in clauses)

        est_disp = "X" if reason == "X" else ("0" if est_days==0 else str(int(est_days)))
        reason_disp = reason
        if reason == "X" and is_risk: reason_disp = "籌碼異常(人工審核風險)" + (" + 刑期可能延長" if is_c13 else "")
        elif est_days != 0:
            if is_risk: reason_disp += " | ⚠️留意人工處置風險"
            if is_c13: reason_disp += " (若進處置將關12天)"

        # Data: History & Fund
        hist = data.fetch_history_data(ticker_code)
        if hist.empty:
            alt = f"{code}{'.TWO' if suffix=='.TW' else '.TW'}"
            hist = data.fetch_history_data(alt)
            if not hist.empty: ticker_code = alt
        
        fund = data.fetch_stock_fundamental(code, ticker_code, precise_db)
        
        if (idx+1)%10 == 0: time.sleep(1.5)
        dt_today, dt_avg6 = data.get_daytrade_stats_finmind(code, target_date_str)
        
        risk_res = logic.calculate_full_risk(code, hist, fund, 99 if est_disp=="X" else int(est_days), dt_today, dt_avg6)
        
        print(f"   [{idx+1}/{len(target_stocks)}] {code} {name}: 最快{est_disp}天 {reason_disp}")

        streak = 0
        for b in valid_bits[::-1]:
            if b == 1: streak += 1
            else: break
        
        last_date_str = "無"
        for i in range(len(valid_bits)-1, -1, -1):
            if valid_bits[i] == 1:
                last_date_str = cal_30[i].strftime("%Y-%m-%d")
                break

        rows_stats.append([
            code, name, streak, sum(valid_bits), sum(valid_bits[-10:]), last_date_str,
            status_30, status_30[-10:], est_disp, reason_disp,
            risk_res['risk_level'], risk_res['trigger_msg'],
            risk_res['curr_price'], risk_res['limit_price'], risk_res['gap_pct'],
            risk_res['curr_vol'], risk_res['limit_vol'], risk_res['turnover_val'],
            risk_res['turnover_rate'], risk_res['pe'], risk_res['pb'],
            risk_res['day_trade_pct']
        ])

    try:
        ws_stats = utils.get_or_create_ws(sh, "近30日熱門統計", headers=config.STATS_HEADERS)
        print("💾 更新統計表...")
        ws_stats.clear()
        ws_stats.append_row(config.STATS_HEADERS, value_input_option='USER_ENTERED')
        if rows_stats: ws_stats.append_rows(rows_stats, value_input_option='USER_ENTERED')
        print("\n✅ 執行完成！")
    except Exception as e: print(f"❌ 寫入失敗: {e}")

if __name__ == "__main__":
    main()
