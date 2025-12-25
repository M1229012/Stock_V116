# -*- coding: utf-8 -*-
import time
import pandas as pd
from datetime import timedelta
import config
import data
import logic

def main():
    print(f"🚀 啟動 V116.18 模組化復刻版 (100% 邏輯還原) | {config.CURRENT_TIME}")
    sh = data.connect_google_sheets()
    if not sh: return

    # 1. 更新大盤
    data.update_market_monitoring_log(sh)

    # 2. 處理日曆與爬蟲 (回朔機制)
    cal_dates = data.get_official_trading_calendar(240)
    target_trade_date_obj = cal_dates[-1]
    
    # 爬取今日公告
    official_stocks = data.get_daily_data(target_trade_date_obj)
    
    # 判斷是否需要回朔
    is_today = (target_trade_date_obj == config.TARGET_DATE.date())
    is_early = (config.TARGET_DATE.time() < config.SAFE_CRAWL_TIME)
    
    if (not official_stocks) and is_today and is_early:
        print("🔄 啟動回朔 (T-1)...")
        if len(cal_dates) >= 2:
            target_trade_date_obj = cal_dates[-2]
            official_stocks = data.get_daily_data(target_trade_date_obj)
            cal_dates = cal_dates[:-1]

    target_date_str = target_trade_date_obj.strftime("%Y-%m-%d")
    print(f"📅 鎖定日期: {target_date_str}")

    # 3. 寫入 Log (防止重複 + 強制文字)
    ws_log = data.get_or_create_ws(sh, "每日紀錄", headers=['日期','市場','代號','名稱','觸犯條款'])
    if official_stocks:
        print("💾 檢查重複並寫入每日紀錄...")
        existing_data = ws_log.get_all_values()
        existing_keys = set()
        if len(existing_data) > 1:
            for row in existing_data[1:]:
                if len(row) >= 3 and row[0]:
                    d_txt = str(row[0]).strip()
                    c_txt = str(row[2]).strip().replace("'", "")
                    existing_keys.add(f"{d_txt}_{c_txt}")

        rows_to_append = []
        for s in official_stocks:
            key = f"{s['日期']}_{s['代號']}"
            if key not in existing_keys:
                rows_to_append.append([str(s['日期']), str(s['市場']), f"'{s['代號']}", str(s['名稱']), str(s['觸犯條款'])])
        
        if rows_to_append:
            ws_log.append_rows(rows_to_append, value_input_option='USER_ENTERED')
            print(f"✅ 已寫入 {len(rows_to_append)} 筆新資料。")

    # 4. 準備掃描
    print("📊 讀取歷史 Log...")
    log_data = ws_log.get_all_records()
    df_log = pd.DataFrame(log_data)
    
    if not df_log.empty:
        df_log['代號'] = df_log['代號'].astype(str).str.strip().str.replace("'", "")
        df_log['日期'] = df_log['日期'].astype(str).str.strip()

    clause_map = {}
    for _, r in df_log.iterrows():
        key = (str(r['代號']), str(r['日期']))
        clause_map[key] = logic.merge_clause_text(clause_map.get(key,""), str(r['觸犯條款']))

    # 5. 處置名單與排除地圖 (🔥 關鍵還原)
    jail_lookback = target_trade_date_obj - timedelta(days=90)
    jail_map = data.get_jail_map(jail_lookback, target_trade_date_obj)
    exclude_map = logic.build_exclude_map(cal_dates, jail_map) # 這裡用 logic.build_exclude_map

    # 6. 掃描目標
    start_dt_str = cal_dates[-90].strftime("%Y-%m-%d")
    df_recent = df_log[df_log['日期'] >= start_dt_str]
    target_stocks = df_recent['代號'].unique()
    
    precise_db_cache = data.load_precise_db_from_sheet(sh)
    rows_stats = []
    
    print(f"🔍 掃描 {len(target_stocks)} 檔股票 (完全還原版)...")
    for idx, code in enumerate(target_stocks):
        code = str(code).strip()
        name_series = df_log[df_log['代號'] == code]['名稱']
        name = name_series.iloc[-1] if not name_series.empty else "未知"

        db_info = precise_db_cache.get(code, {})
        suffix = data.get_ticker_suffix(db_info.get('market', '上市')) # 這裡用 data.get_ticker_suffix (如果之前沒在data.py定義，記得補上，或者這裡直接硬寫)
        # 修正：data.py 沒有 get_ticker_suffix，我直接補在這裡確保不缺漏
        m_type = str(db_info.get('market', '上市')).upper()
        if any(k in m_type for k in ['上櫃', 'TWO', 'TPEX', 'OTC']): suffix = '.TWO'
        else: suffix = '.TW'
        
        ticker_code = f"{code}{suffix}"

        # 🔥 [關鍵修正] 使用「最近 N 個非處置交易日」作為日曆 (使用 exclude_map)
        stock_calendar_30_asc = logic.get_last_n_non_jail_trade_dates(
            code, cal_dates, jail_map, exclude_map=exclude_map, n=30
        )

        bits = []
        clauses = []
        for d in stock_calendar_30_asc:
            c_str = clause_map.get((code, d.strftime("%Y-%m-%d")), "")
            if logic.is_excluded(code, d, exclude_map):
                bits.append(0)
                clauses.append(c_str)
                continue
            if c_str:
                bits.append(1)
                clauses.append(c_str)
            else:
                bits.append(0)
                clauses.append("")

        valid_bits = []
        for i in range(len(bits)):
            if bits[i] == 1:
                ids = logic.parse_clause_ids_strict(clauses[i])
                valid_bits.append(1 if logic.is_valid_accumulation_day(ids) else 0)
            else:
                valid_bits.append(0)

        status_30 = "".join(map(str, valid_bits))
        if len(status_30) < 30: status_30 = status_30.zfill(30)

        est_days, reason_msg = logic.simulate_days_to_jail_strict(
            bits, clauses, stock_id=code, target_date=target_trade_date_obj, jail_map=jail_map, enable_safe_filter=False
        )

        latest_ids = logic.parse_clause_ids_strict(clauses[-1] if clauses else "")
        is_special_risk = logic.is_special_risk_day(latest_ids)
        is_clause_13 = False
        for c in clauses:
            if 13 in logic.parse_clause_ids_strict(c):
                is_clause_13 = True
                break

        est_days_display = "X"
        reason_display = ""
        if reason_msg == "X":
            est_days_display = "X"
            if is_special_risk:
                reason_display = "籌碼異常(人工審核風險)"
                if is_clause_13: reason_display += " + 刑期可能延長"
        elif est_days == 0:
            est_days_display = "0"
            reason_display = reason_msg
        else:
            est_days_display = str(int(est_days))
            reason_display = reason_msg
            if is_special_risk: reason_display += " | ⚠️留意人工處置風險"
            if is_clause_13: reason_display += " (若進處置將關12天)"

        hist = data.fetch_history_data(ticker_code)
        if hist.empty:
            alt_suffix = '.TWO' if suffix == '.TW' else '.TW'
            hist = data.fetch_history_data(f"{code}{alt_suffix}")
            if not hist.empty: ticker_code = f"{code}{alt_suffix}"

        fund = data.fetch_stock_fundamental(code, ticker_code, precise_db_cache)

        dt_today, dt_avg6 = 0.0, 0.0
        if config.IS_NIGHT_RUN:
            dt_today, dt_avg6 = data.get_daytrade_stats_finmind(code, config.TARGET_DATE.strftime("%Y-%m-%d"))

        risk_res = logic.calculate_full_risk(code, hist, fund, 99 if est_days_display=="X" else int(est_days_display), dt_today, dt_avg6)

        # streak 計算
        streak = 0
        for b in valid_bits[::-1]:
            if b == 1: streak += 1
            else: break

        last_date = "無"
        if len(valid_bits) > 0:
            for i in range(len(valid_bits)-1, -1, -1):
                if valid_bits[i] == 1:
                    last_date = stock_calendar_30_asc[i].strftime("%Y-%m-%d")
                    break

        cnt_30 = sum(valid_bits)
        cnt_10 = sum(valid_bits[-10:])

        row = [
            f"'{code}",
            str(name),
            str(streak),
            str(cnt_30),
            str(cnt_10),
            str(last_date),
            f"'{status_30}",
            f"'{status_30[-10:]}",
            str(est_days_display),
            str(reason_display),
            str(risk_res['risk_level']),
            str(risk_res['trigger_msg']),
            str(risk_res['curr_price']),
            str(risk_res['limit_price']),
            str(risk_res['gap_pct']),
            str(risk_res['curr_vol']),
            str(risk_res['limit_vol']),
            str(risk_res['turnover_val']),
            str(risk_res['turnover_rate']),
            str(risk_res['pe']),
            str(risk_res['pb']),
            str(risk_res['day_trade_pct'])
        ]
        rows_stats.append(row)
        
        if (idx+1)%10 == 0: time.sleep(1.5)

    if rows_stats:
        print("💾 更新統計表...")
        ws_stats = data.get_or_create_ws(sh, "近30日熱門統計", headers=config.STATS_HEADERS)
        ws_stats.clear()
        ws_stats.append_row(config.STATS_HEADERS, value_input_option='USER_ENTERED')
        ws_stats.append_rows(rows_stats, value_input_option='USER_ENTERED')
        print("✅ 完成")

if __name__ == "__main__":
    main()
