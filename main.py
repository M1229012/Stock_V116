# -*- coding: utf-8 -*-
import time
from config import CURRENT_TIME, IS_NIGHT_RUN, TODAY_STR, STATS_HEADERS
import data
import logic

def main():
    mode_str = "🌙 晚上補單與修正 (FinMind+Yahoo)" if IS_NIGHT_RUN else "☀️ 下午盤後更新 (Yahoo only)"
    print(f"🚀 啟動模組化後端 | 時間: {CURRENT_TIME} | 模式: {mode_str}")

    # 1. 連線 Google Sheet
    ws = data.connect_google_sheets()
    if not ws: return

    records = ws.get_all_records()
    updates = []
    
    print(f"📋 開始掃描 {len(records)} 檔股票...")

    for i, row in enumerate(records):
        code = str(row['代號'])
        
        # 2. 抓 Yahoo (下午、晚上都抓)
        y_data = data.fetch_yahoo_data(code)
        time.sleep(0.5) # 避免太快

        # 3. 抓 FinMind (只在晚上抓)
        dt_val = 0.0
        if IS_NIGHT_RUN:
            dt_val = data.fetch_finmind_daytrade(code)
        
        # 4. 整合與計算
        if y_data['price'] > 0:
            new_row = logic.calculate_risk(row, y_data, dt_val)
            new_row['最近一次日期'] = TODAY_STR
            updates.append(new_row)
            print(f"[{i+1}] {code} OK (P:{y_data['price']}, DT:{dt_val}%)")
        else:
            print(f"[{i+1}] {code} Yahoo 失敗")

    # 5. 寫回 Google Sheet
    if updates:
        print(f"💾 正在寫入 {len(updates)} 筆資料...")
        final_rows = logic.prepare_batch_update(records, updates)
        
        ws.clear()
        ws.append_row(STATS_HEADERS)
        ws.append_rows(final_rows)
        print("✅ 作業完成！")
    else:
        print("⚠️ 無資料更新。")

if __name__ == "__main__":
    main()
