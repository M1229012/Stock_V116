# -*- coding: utf-8 -*-
import time
import pandas as pd
import math
from datetime import timedelta
import config
import data
import logic

# --- Sheet 寫入安全過濾 (保留 0/-1/999) ---
def sheet_safe(v):
    if v is None: return ""
    try:
        if isinstance(v, float) and math.isnan(v): return ""
    except: pass
    return str(v)

def main():
    print(f"🚀 啟動 V116.18 模組化復刻版 (自動回朔增強版) | {config.TARGET_DATE}")
    sh = data.connect_google_sheets()
    if not sh: return

    data.update_market_monitoring_log(sh)
    cal_dates = data.get_official_trading_calendar(240)
    target_trade_date_obj = cal_dates[-1]
    
    # 第一次嘗試爬取
    official_stocks = data.get_daily_data(target_trade_date_obj)
    target_date_str = target_trade_date_obj.strftime("%Y-%m-%d")

    # 🔥 [修改重點]：只要沒資料，無條件回朔 T-1
    if not official_stocks:
        print(f"⚠️ {target_date_str} 查無資料 (可能是假日、未開盤或網站延遲)。")
        print("🔄 啟動自動回朔，嘗試抓取上一個交易日 (T-1)...")
        
        if len(cal_dates) >= 2:
            # 往回推一天
            target_trade_date_obj = cal_dates[-2]
            target_date_str = target_trade_date_obj.strftime("%Y-%m-%d")
            print(f"📅 改鎖定日期: {target_date_str}")
            
            # 重抓
            official_stocks = data.get_daily_data(target_trade_date_obj)
            cal_dates = cal_dates[:-1] # 調整日曆列表以符合新日期
        else:
            print("❌ 無法回朔 (交易日曆不足)。")

    print(f"📅 最終確認日期: {target_date_str}")

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
    else:
        print("💤 本次執行無任何資料可寫入。")

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

    jail_lookback = target_trade_date_obj - timedelta(days=90)
    jail_map = data.get_jail_map(jail_lookback, target_trade_date_obj)
    exclude_map = logic.build_exclude_map(cal_dates, jail_map)

    start_dt_str = cal_dates[-90].strftime("%Y-%m-%d")
    df_recent = df_log[df_log['日期'] >= start_dt_str]
    target_stocks = df_recent['代號'].unique()
    
    precise_db_cache = data.load_precise_db_from_sheet(sh)
    rows_stats = []
    
    print(f"🔍 掃描 {len(target_stocks)} 檔股票...")
    for idx, code in enumerate(target_stocks):
        code = str(code).strip()
        name_series = df_log[df_log['代號'] == code]['名稱']
        name = name_series.iloc[-1] if not name_series.empty else "未知"

        db_info = precise_db_cache.get(code, {})
        m_type = str(db_info.get('market', '上市')).upper()
        suffix = '.TWO' if any(k in m_type for k in ['上櫃', 'TWO', 'TPEX', 'OTC']) else '.TW'
        ticker_code = f"{code}{suffix}"

        stock_calendar_30_asc = logic.get_last_n_non_jail_trade_dates(
            code, cal_dates, jail_map, exclude_map=exclude_map, n=30
        )

        bits = []; clauses = []
        for d in stock_calendar_30_asc:
            c_str = clause_map.get((code, d.strftime("%Y-%m-%d")), "")
            if logic.is_excluded(code, d, exclude_map):
                bits.append(0); clauses.append(c_str); continue
            if c_str: bits.append(1); clauses.append(c_str)
            else: bits.append(0); clauses.append("")

        est_days, reason_msg = logic.simulate_days_to_jail_strict(
            bits, clauses, stock_id=code, target_date=target_trade_date_obj, jail_map=jail_map, enable_safe_filter=False
        )

        latest_ids = logic.parse_clause_ids_strict(clauses[-1] if clauses else "")
        is_special_risk = logic.is_special_risk_day(latest_ids)
        is_clause_13 = any(13 in logic.parse_clause_ids_strict(c) for c in clauses)

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
        # 這裡依照 config 的 TARGET_DATE 判斷，若無 config.TARGET_DATE 則需修正 config
        if config.TARGET_DATE.hour >= 20:
            dt_today, dt_avg6 = data.get_daytrade_stats_finmind(code, target_trade_date_obj.strftime("%Y-%m-%d"))

        risk_res = logic.calculate_full_risk(code, hist, fund, 99 if est_days_display=="X" else int(est_days_display), dt_today, dt_avg6)

        valid_bits = []
        for i in range(len(bits)):
            if bits[i] == 1:
                ids = logic.parse_clause_ids_strict(clauses[i])
                valid_bits.append(1 if logic.is_valid_accumulation_day(ids) else 0)
            else: valid_bits.append(0)

        status_30 = "".join(map(str, valid_bits)).zfill(30)
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

        row = [
            f"'{code}",
            sheet_safe(name),
            sheet_safe(streak),
            sheet_safe(sum(valid_bits)),
            sheet_safe(sum(valid_bits[-10:])),
            sheet_safe(last_date),
            f"'{status_30}",
            f"'{status_30[-10:]}",
            sheet_safe(est_days_display),
            sheet_safe(reason_display),
            sheet_safe(risk_res['risk_level']),
            sheet_safe(risk_res['trigger_msg']),
            sheet_safe(risk_res['curr_price']),
            sheet_safe(risk_res['limit_price']),
            sheet_safe(risk_res['gap_pct']),
            sheet_safe(risk_res['curr_vol']),
            sheet_safe(risk_res['limit_vol']),
            sheet_safe(risk_res['turnover_val']),
            sheet_safe(risk_res['turnover_rate']),
            sheet_safe(risk_res['pe']),
            sheet_safe(risk_res['pb']),
            sheet_safe(risk_res['day_trade_pct'])
        ]
        rows_stats.append(row)
        
        if (idx+1)%10 == 0: time.sleep(1.2)

    if rows_stats:
        print("💾 更新統計表...")
        ws_stats = data.get_or_create_ws(sh, "近30日熱門統計", headers=config.STATS_HEADERS)
        ws_stats.clear()
        ws_stats.append_row(config.STATS_HEADERS, value_input_option='USER_ENTERED')
        ws_stats.append_rows(rows_stats, value_input_option='USER_ENTERED')
        print("✅ 完成")

if __name__ == "__main__":
    main()
