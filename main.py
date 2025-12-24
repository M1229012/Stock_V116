# -*- coding: utf-8 -*-
import sys
import os
import time
import pandas as pd
from datetime import timedelta

# 確保能匯入模組
sys.path.append(os.getcwd())

import config
import utils
import data
import logic

def main():
    # 連線 Google Sheets
    sh, _ = data.connect_google_sheets()
    if not sh: 
        print("❌ 無法連線至 Google Sheets，程式終止。")
        return

    # 取得系統時間
    target_date_obj = config.get_target_date()
    print(f"🚀 啟動 V116.18 Zeabur 版本 | 時間: {target_date_obj}")

    # 更新大盤監控
    data.update_market_monitoring_log(sh, target_date_obj)

    # 取得交易日曆 (回朔 240 天)
    cal_dates = data.get_official_trading_calendar(240, target_date_obj)
    target_trade_date_obj = cal_dates[-1]
    
    # 爬取注意股公告
    official_stocks = data.get_daily_data(target_trade_date_obj)
    
    # 若當日無資料且非等待時段，嘗試回朔 (T-1)
    is_today = (target_trade_date_obj == target_date_obj.date())
    is_early = (target_date_obj.time() < config.SAFE_CRAWL_TIME)
    is_pending = (official_stocks == [] and is_today and is_early)

    if official_stocks is None or is_pending:
        if len(cal_dates) >= 2:
            print("🔄 啟動「時光回朔機制」，退回上一個交易日 (T-1)...")
            cal_dates = cal_dates[:-1]
            target_trade_date_obj = cal_dates[-1]
            official_stocks = data.get_daily_data(target_trade_date_obj)
        else:
            print("❌ 交易日曆不足，無法回朔。")

    target_date_str = target_trade_date_obj.strftime("%Y-%m-%d")
    finmind_trade_date_str = target_date_str
    print(f"📅 最終鎖定運算日期: {target_date_str}")

    # 寫入歷史紀錄
    ws_log = utils.get_or_create_ws(sh, "每日紀錄", headers=['日期','市場','代號','名稱','觸犯條款'])
    total_log_rows = len(ws_log.col_values(1))
    
    if official_stocks:
        print(f"💾 寫入資料庫...")
        existing_keys = set()
        if total_log_rows > 1:
            start_row = max(1, total_log_rows - 3000)
            raw_keys = ws_log.get(f'A{start_row}:E{total_log_rows}')
            for r in raw_keys:
                if len(r) >= 3 and r[0] != '日期':
                    existing_keys.add(f"{r[0]}_{r[2]}")
        
        new_rows = []
        for stock in official_stocks:
            key = f"{stock['日期']}_{stock['代號']}"
            if key not in existing_keys:
                new_rows.append([stock['日期'], stock['市場'], stock['代號'], stock['名稱'], stock['觸犯條款']])
        
        if new_rows:
            ws_log.append_rows(new_rows, value_input_option='USER_ENTERED')
            total_log_rows += len(new_rows)

    # 載入參數表
    precise_db_cache = data.load_precise_db_from_sheet(sh)

    # 讀取歷史資料 (Log)
    print("📊 讀取歷史 Log...")
    start_idx = max(1, total_log_rows - 8000)
    raw_vals = ws_log.get(f'A{start_idx}:E{total_log_rows}')
    if start_idx > 1:
        headers = ws_log.get('A1:E1')
        raw_vals = headers + raw_vals
    df = pd.DataFrame(raw_vals[1:], columns=raw_vals[0])
    df['日期'] = pd.to_datetime(df['日期'], errors='coerce').dt.date
    df = df[pd.notna(df['日期'])]

    # 建立 Clause Map
    clause_map = {}
    for _, r in df.iterrows():
        try:
            k = (str(r['代號']), r['日期'])
            new_text = str(r.get('觸犯條款', '') or '')
            old_text = clause_map.get(k, "")
            clause_map[k] = utils.merge_clause_text(old_text, new_text)
        except: pass

    # 篩選最近出現的股票
    start_date_90 = cal_dates[-90] if len(cal_dates) >= 90 else cal_dates[0]
    df_recent = df[df['日期'] >= start_date_90]
    target_stocks = df_recent['代號'].unique()
    total_scan = len(target_stocks)

    # 建立處置濾網
    jail_lookback = target_trade_date_obj - timedelta(days=90)
    jail_map = data.get_jail_map(jail_lookback, target_trade_date_obj)
    exclude_map = logic.build_exclude_map(cal_dates, jail_map)

    print(f"🔍 開始掃描 {total_scan} 檔股票...")
    rows_stats = []

    for idx, code in enumerate(target_stocks):
        code = str(code).strip()
        name_series = df[df['代號']==code]['名稱']
        name = name_series.iloc[-1] if not name_series.empty else "未知"

        db_info = precise_db_cache.get(code, {})
        suffix = utils.get_ticker_suffix(db_info.get('market', '上市'))
        ticker_code = f"{code}{suffix}"

        # 取得非處置交易日
        stock_calendar = logic.get_last_n_non_jail_trade_dates(
            code, cal_dates, jail_map, exclude_map=exclude_map, n=30
        )

        bits = []
        clauses = []
        for d in stock_calendar:
            c_str = clause_map.get((code, d), "")
            if logic.is_excluded(code, d, exclude_map):
                bits.append(0); clauses.append(c_str)
            elif c_str:
                bits.append(1); clauses.append(c_str)
            else:
                bits.append(0); clauses.append("")

        valid_bits = []
        for i in range(len(bits)):
            if bits[i] == 1:
                ids = utils.parse_clause_ids_strict(clauses[i])
                valid_bits.append(1 if logic.is_valid_accumulation_day(ids) else 0)
            else: valid_bits.append(0)

        status_30 = "".join(map(str, valid_bits))
        if len(status_30) < 30: status_30 = status_30.zfill(30)

        # 處置預測
        est_days, reason_msg = logic.simulate_days_to_jail_strict(
            bits, clauses,
            stock_id=code,
            target_date=target_trade_date_obj,
            jail_map=jail_map,
            enable_safe_filter=False 
        )

        # 特殊風險
        latest_ids = utils.parse_clause_ids_strict(clauses[-1] if clauses else "")
        is_special_risk = logic.is_special_risk_day(latest_ids)
        is_clause_13 = False
        for c in clauses:
            if 13 in utils.parse_clause_ids_strict(c): is_clause_13 = True; break

        if reason_msg == "X":
            est_days_display = "X"
            reason_display = "籌碼異常(人工審核風險)" if is_special_risk else ""
            if is_special_risk and is_clause_13: reason_display += " + 刑期可能延長"
        elif est_days == 0:
            est_days_display = "0"
            reason_display = reason_msg
        else:
            est_days_display = str(int(est_days))
            reason_display = reason_msg
            if is_special_risk: reason_display += " | ⚠️留意人工處置風險"
            if is_clause_13: reason_display += " (若進處置將關12天)"

        # 抓取技術資料
        hist = data.fetch_history_data(ticker_code)
        if hist.empty:
            alt_suffix = '.TWO' if suffix == '.TW' else '.TW'
            hist = data.fetch_history_data(f"{code}{alt_suffix}")
            if not hist.empty: ticker_code = f"{code}{alt_suffix}"
        
        fund = data.fetch_stock_fundamental(code, ticker_code, precise_db_cache)
        
        if (idx + 1) % 10 == 0: time.sleep(1.5)
        dt_today, dt_avg6 = data.get_daytrade_stats_finmind(code, finmind_trade_date_str)

        risk_res = logic.calculate_full_risk(code, hist, fund, 99 if est_days_display=="X" else int(est_days), dt_today, dt_avg6)

        print(f"   [{idx+1}/{total_scan}] {code} {name}: 最快{est_days_display}天 {reason_display}")

        streak = 0
        for b in valid_bits[::-1]:
            if b == 1: streak += 1
            else: break
        
        last_trigger_date_str = "無"
        if len(valid_bits) > 0:
            for i in range(len(valid_bits)-1, -1, -1):
                if valid_bits[i] == 1:
                    last_trigger_date_str = stock_calendar[i].strftime("%Y-%m-%d")
                    break

        rows_stats.append([
            code, name, streak, 
            sum(valid_bits), sum(valid_bits[-10:]),
            last_trigger_date_str,
            status_30, status_30[-10:], est_days_display, reason_display, 
            risk_res['risk_level'], risk_res['trigger_msg'],
            risk_res['curr_price'], risk_res['limit_price'], risk_res['gap_pct'],
            risk_res['curr_vol'], risk_res['limit_vol'], risk_res['turnover_val'],
            risk_res['turnover_rate'], risk_res['pe'], risk_res['pb'],
            risk_res['day_trade_pct']
        ])

    try:
        ws_stats = utils.get_or_create_ws(sh, "近30日熱門統計", headers=config.STATS_HEADERS)
        print("💾 更新 [近30日熱門統計]...")
        ws_stats.clear()
        ws_stats.append_row(config.STATS_HEADERS, value_input_option='USER_ENTERED')
        if rows_stats:
            ws_stats.append_rows(rows_stats, value_input_option='USER_ENTERED')
        print("\n✅ V116.18 執行完成！")
    except Exception as e:
        print(f"❌ 寫入失敗: {e}")

if __name__ == "__main__":
    main()
