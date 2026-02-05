import gspread
import yfinance as yf
import pandas as pd
import numpy as np
import re
import time
import os
import sys
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

# ============================
# ⚙️ 設定區
# ============================
SHEET_NAME = "台股注意股資料庫_V33"
SOURCE_WORKSHEET = "處置股90日明細"
DEST_WORKSHEET = "處置股出關記錄"

SERVICE_KEY_FILE = "service_key.json"

# ============================
# 🛠️ 工具函式
# ============================
def connect_google_sheets(sheet_name):
    """連線 Google Sheets"""
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(SERVICE_KEY_FILE, scopes=scope)
        gc = gspread.authorize(creds)
        sh = gc.open(sheet_name)
        return sh
    except Exception as e:
        print(f"❌ Google Sheet 連線失敗 ({sheet_name}): {e}")
        return None

def parse_roc_date(date_str):
    """解析民國或西元日期"""
    s = str(date_str).strip()
    match = re.match(r'^(\d{2,3})[/-](\d{1,2})[/-](\d{1,2})$', s)
    if match:
        y, m, d = map(int, match.groups())
        y_final = y + 1911 if y < 1911 else y
        return datetime(y_final, m, d)
    for fmt in ["%Y/%m/%d", "%Y-%m-%d", "%Y%m%d"]:
        try: return datetime.strptime(s, fmt)
        except: continue
    return None

def determine_status_legacy(pre_pct, in_pct):
    """
    [舊標準] 用於顯示在左側表格，保持原始紀錄不變
    門檻：5% / 15%
    """
    if in_pct > 15: return "👑 妖股誕生"
    elif in_pct > 5: return "🔥 強勢突圍"
    elif in_pct < -15: return "💀 人去樓空"
    elif in_pct < -5: return "📉 走勢疲軟"
    else: return "🧊 多空膠著"

def determine_status_new(pre_pct, in_pct):
    """
    [新標準] 用於右側統計，測試過濾雜訊後的勝率
    門檻：10% / 20%
    """
    if in_pct > 20: return "👑 妖股誕生"
    elif in_pct > 10: return "🔥 強勢突圍"
    elif in_pct < -20: return "💀 人去樓空"
    elif in_pct < -10: return "📉 走勢疲軟"
    else: return "🧊 多空膠著"

def get_ticker_list(code, market=""):
    """根據市場別與股號決定嘗試的順序，減少 404 錯誤"""
    code = str(code)
    if "上櫃" in market or "TPEx" in market:
        return [f"{code}.TWO", f"{code}.TW"]
    if "上市" in market:
        return [f"{code}.TW", f"{code}.TWO"]
    if code and code[0] in ['3', '4', '5', '6', '8']:
        return [f"{code}.TWO", f"{code}.TW"]
    return [f"{code}.TW", f"{code}.TWO"]

def fetch_stock_data(code, start_date, jail_end_date, market=""):
    """抓取歷史股價並計算狀態與出關後走勢"""
    try:
        fetch_start = start_date - timedelta(days=60)
        fetch_end = jail_end_date + timedelta(days=40) 
        
        tickers_to_try = get_ticker_list(code, market)
        df = pd.DataFrame()
        
        for ticker in tickers_to_try:
            try:
                temp_df = yf.Ticker(ticker).history(start=fetch_start, end=fetch_end, auto_adjust=True)
                if not temp_df.empty:
                    df = temp_df
                    break
            except Exception:
                continue
        
        if df.empty:
            return None

        df.index = df.index.tz_localize(None)
        df = df.ffill()

        # === 1. 計算處置狀態 ===
        mask_jail = (df.index >= pd.Timestamp(start_date)) & (df.index <= pd.Timestamp(jail_end_date))
        df_jail = df[mask_jail]
        mask_before = df.index < pd.Timestamp(start_date)
        
        pre_pct = 0.0
        in_pct = 0.0
        
        if mask_before.any():
            jail_base_p = df[mask_before]['Close'].iloc[-1]
            target_idx = max(0, len(df[mask_before]) - len(df_jail))
            pre_entry = df[mask_before]['Open'].iloc[target_idx] if len(df[mask_before]) > target_idx else jail_base_p
            if pre_entry != 0:
                pre_pct = ((jail_base_p - pre_entry) / pre_entry) * 100

        jail_end_price = 0
        if not df_jail.empty:
            jail_start_price = df_jail['Open'].iloc[0]
            jail_end_price = df_jail['Close'].iloc[-1]
            if jail_start_price != 0:
                in_pct = ((jail_end_price - jail_start_price) / jail_start_price) * 100
        
        # 這裡會同時回傳兩種狀態，分別用於顯示和統計
        status_legacy = determine_status_legacy(pre_pct, in_pct)
        status_new = determine_status_new(pre_pct, in_pct)

        # === 2. 計算出關後 D+1 ~ D+10 ===
        df_after = df[df.index > pd.Timestamp(jail_end_date)]
        
        if not df_after.empty:
            release_date_str = df_after.index[0].strftime("%Y/%m/%d")
        else:
            release_date_str = (jail_end_date + timedelta(days=1)).strftime("%Y/%m/%d")

        post_data = []
        accumulated_pct = 0.0
        base_price = jail_end_price if jail_end_price != 0 else (df_after['Open'].iloc[0] if not df_after.empty else 0)

        for i in range(10):
            if i < len(df_after):
                curr_close = df_after['Close'].iloc[i]
                prev_close = df_after['Close'].iloc[i-1] if i > 0 else base_price
                if prev_close != 0:
                    daily_chg = ((curr_close - prev_close) / prev_close) * 100
                    post_data.append(f"{daily_chg:+.1f}%")
                else:
                    post_data.append("0.0%")
                
                if i == len(df_after) - 1 or i == 9:
                    if base_price != 0:
                        accumulated_pct = ((curr_close - base_price) / base_price) * 100
            else:
                post_data.append("")

        while len(post_data) < 10:
            post_data.append("")

        return {
            "status_legacy": status_legacy, # 舊標準 (顯示用)
            "status_new": status_new,       # 新標準 (統計用)
            "pre_pct": f"{pre_pct:+.1f}%",
            "in_pct": f"{in_pct:+.1f}%",
            "acc_pct": f"{accumulated_pct:+.1f}%",
            "daily_trends": post_data,
            "release_date": release_date_str
        }

    except Exception as e:
        print(f"⚠️ 數據計算錯誤 {code}: {e}")
        return None

# ============================
# 🚀 主程式
# ============================
def main():
    print("🚀 開始執行處置股出關記錄更新...")
    
    sh = connect_google_sheets(SHEET_NAME)
    if not sh: return

    try:
        ws_source = sh.worksheet(SOURCE_WORKSHEET)
    except WorksheetNotFound:
        print(f"❌ 找不到來源工作表 '{SOURCE_WORKSHEET}'")
        return

    header = ["出關日期", "股號", "股名", "狀態", "處置前%", "處置中%", "累積漲跌幅", 
              "D+1", "D+2", "D+3", "D+4", "D+5", "D+6", "D+7", "D+8", "D+9", "D+10"]

    try:
        ws_dest = sh.worksheet(DEST_WORKSHEET)
    except WorksheetNotFound:
        print(f"💡 工作表 '{DEST_WORKSHEET}' 不存在，正在建立...")
        ws_dest = sh.add_worksheet(title=DEST_WORKSHEET, rows=1000, cols=30) 
        ws_dest.append_row(header)

    # 讀取現有記錄
    raw_rows = ws_dest.get_all_values()
    existing_map = {} 
    
    if len(raw_rows) > 1:
        for row in raw_rows[1:]:
            if len(row) < 17: continue
            rdate = str(row[0])
            rid = str(row[1])
            d10 = str(row[16]).strip()
            
            if rid:
                key = f"{rid}_{rdate}"
                row_dict = {}
                for idx, h in enumerate(header):
                    if idx < len(row):
                        row_dict[h] = row[idx]
                    else:
                        row_dict[h] = ""
                
                existing_map[key] = {
                    'data': row_dict,
                    'done': bool(d10)
                }

    source_data = ws_source.get_all_records()
    processed_list = []
    
    # 準備統計容器 (使用新標準)
    status_order = ["👑 妖股誕生", "🔥 強勢突圍", "🧊 多空膠著", "📉 走勢疲軟", "💀 人去樓空"]
    daily_stats = {s: [{'sum': 0.0, 'wins': 0, 'count': 0} for _ in range(10)] for s in status_order}
    summary_stats = {s: {'count': 0, 'wins': 0, 'total_pct': 0.0} for s in status_order}

    today = datetime.now()
    print(f"🔍 掃描 {len(source_data)} 筆處置紀錄...")
    total_count = 0
    update_count = 0

    for row in source_data:
        code = str(row.get('代號', '')).replace("'", "").strip()
        name = row.get('名稱', '')
        period = str(row.get('處置期間', '')).strip()
        market = str(row.get('市場', ''))
        
        if not code or not period: continue
        
        dates = re.split(r'[~-～]', period)
        if len(dates) < 2: continue
        
        s_date = parse_roc_date(dates[0])
        e_date = parse_roc_date(dates[1])
        
        if not s_date or not e_date: continue
        if e_date > today: continue 

        result = fetch_stock_data(code, s_date, e_date, market)
        
        if not result:
            continue
            
        release_date_str = result['release_date']
        key = f"{code}_{release_date_str}"
        
        # 無論是新資料還是舊資料，我們都需要它的數值來跑統計
        # 但只有新資料(或未完成的資料)才需要更新到 processed_list 寫回表格
        
        # --- 處理寫回表格的資料 (左側) ---
        # 這裡使用 result['status_legacy'] (舊標準) 以保持原始紀錄
        final_row_data = []
        if key in existing_map and existing_map[key]['done']:
            old_row = existing_map[key]['data']
            final_row_data = [old_row.get(h, "") for h in header]
            # 如果舊資料裡面的狀態是空的或是舊標準，我們就保持原樣
            # 但為了統計，我們需要當下的數值(可能要重新抓，但為了效能，我們假設 result 抓到的是最新的)
        else:
            final_row_data = [
                release_date_str, code, name, result['status_legacy'], # 顯示舊標準
                result['pre_pct'], result['in_pct'], result['acc_pct']
            ] + result['daily_trends']
            update_count += 1
            print(f"  ✨ 更新: {code} {name} | {result['status_legacy']}")
            time.sleep(0.5)
        
        processed_list.append(final_row_data)

        # --- 處理統計資料 (右側) ---
        # 這裡使用 result['status_new'] (新標準) 來計算統計
        
        # 為了統計，我們必須解析 final_row_data 裡的數值
        # 即使是舊資料，我們也重新用新標準歸類一次
        
        # 1. 取得這筆資料的數值 (不管它是剛抓的還是舊的，數值應該是一樣的)
        # 處置中% 在 index 5
        in_pct_str = final_row_data[5] 
        # 我們直接用 result['status_new']，因為它是根據當下抓到的數值判斷的
        # 注意：如果現有資料的數值跟重新抓的不一樣(極少見)，這裡會以重新抓的為準進行統計
        stat_status = result['status_new']
        
        # D+10 累積
        acc_pct_str = final_row_data[6]
        if stat_status in summary_stats:
            summary_stats[stat_status]['count'] += 1
            try:
                acc_val = float(acc_pct_str.replace('%', '').replace('+', ''))
                summary_stats[stat_status]['total_pct'] += acc_val
                if acc_val > 0: summary_stats[stat_status]['wins'] += 1
            except: pass
            
        # 每日詳細
        if stat_status in daily_stats:
            for day_idx in range(10):
                # D+1 在 index 7
                val_str = final_row_data[7 + day_idx]
                if val_str:
                    try:
                        val = float(val_str.replace('%', '').replace('+', ''))
                        daily_stats[stat_status][day_idx]['count'] += 1
                        daily_stats[stat_status][day_idx]['sum'] += val
                        if val > 0:
                            daily_stats[stat_status][day_idx]['wins'] += 1
                    except: pass
        
        total_count += 1

    # 4. 排序
    processed_list.sort(key=lambda x: x[0], reverse=True)
    
    # 5. === 建構右側統計區 (新標準) ===
    print("📊 計算新標準統計數據 (右側)...")
    
    right_side_rows = []
    
    # 1. 總覽表格 (標題加註新標準)
    right_side_rows.append(["", "📊 狀態總覽 (新標準10%/20%)", "個股數", "D+10勝率", "D+10平均", "", "", "", "", "", "", ""])
    for s in status_order:
        t = summary_stats[s]['count']
        w = summary_stats[s]['wins']
        avg = summary_stats[s]['total_pct'] / t if t > 0 else 0
        wr = (w / t * 100) if t > 0 else 0
        right_side_rows.append(["", s, t, f"{wr:.1f}%", f"{avg:+.1f}%", "", "", "", "", "", "", ""])

    right_side_rows.append([""] * 12) 

    # 2. 每日平均漲跌幅走勢
    days_header = [f"D+{i+1}" for i in range(10)]
    right_side_rows.append(["", "📈 平均漲跌幅 (新標準)"] + days_header)
    
    for s in status_order:
        row_vals = ["", s]
        for d in range(10):
            data = daily_stats[s][d]
            if data['count'] > 0:
                avg = data['sum'] / data['count']
                row_vals.append(f"{avg:+.1f}%")
            else:
                row_vals.append("-")
        right_side_rows.append(row_vals)

    right_side_rows.append([""] * 12) 

    # 3. 每日勝率走勢
    right_side_rows.append(["", "🏆 每日勝率 (新標準)"] + days_header)
    
    for s in status_order:
        row_vals = ["", s]
        for d in range(10):
            data = daily_stats[s][d]
            if data['count'] > 0:
                wr = (data['wins'] / data['count']) * 100
                row_vals.append(f"{wr:.1f}%")
            else:
                row_vals.append("-")
        right_side_rows.append(row_vals)

    # 6. === 合併 ===
    final_header = header + [""] * 12 
    final_output = [final_header]
    
    max_rows = max(len(processed_list), len(right_side_rows))
    
    for i in range(max_rows):
        if i < len(processed_list):
            left_part = processed_list[i]
        else:
            left_part = [""] * 17 
            
        if i < len(right_side_rows):
            right_part = right_side_rows[i]
        else:
            right_part = [""] * 12
            
        final_output.append(left_part + right_part)

    # 寫入 Sheet
    ws_dest.clear()
    ws_dest.update(final_output)

    # 7. === 設定條件格式 ===
    print("🎨 更新條件格式化與勝率高低標記...")

    ranges = [
        {"sheetId": ws_dest.id, "startRowIndex": 1, "startColumnIndex": 4, "endColumnIndex": 17},
        {"sheetId": ws_dest.id, "startRowIndex": 1, "startColumnIndex": 19, "endColumnIndex": 30}
    ]

    header_rule = {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": ranges,
                "booleanRule": {
                    "condition": {"type": "TEXT_STARTS_WITH", "values": [{"userEnteredValue": "D+"}]},
                    "format": {
                        "backgroundColor": {"red": 1.0, "green": 0.9, "blue": 0.7}, 
                        "textFormat": {"bold": True}
                    }
                }
            },
            "index": 0 
        }
    }

    positive_rule = {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": ranges,
                "booleanRule": {
                    "condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "+"}]},
                    "format": {"backgroundColor": {"red": 1.0, "green": 0.8, "blue": 0.8}}
                }
            },
            "index": 1
        }
    }

    negative_rule = {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": ranges,
                "booleanRule": {
                    "condition": {"type": "TEXT_CONTAINS", "values": [{"userEnteredValue": "-"}]},
                    "format": {"backgroundColor": {"red": 0.8, "green": 1.0, "blue": 0.8}}
                }
            },
            "index": 2
        }
    }

    requests = [header_rule, positive_rule, negative_rule]

    # --- 標記最高/最低 ---
    win_rate_start_row = -1
    for idx, row in enumerate(final_output):
        if len(row) > 18 and "🏆 每日勝率" in str(row[18]):
            win_rate_start_row = idx
            break
    
    if win_rate_start_row != -1:
        for col_idx in range(19, 29): 
            col_values = []
            valid_rows = []
            for r in range(1, 6):
                row_idx = win_rate_start_row + r
                if row_idx < len(final_output):
                    val_str = final_output[row_idx][col_idx]
                    try:
                        val = float(val_str.replace('%', ''))
                        col_values.append(val)
                        valid_rows.append(row_idx)
                    except:
                        col_values.append(-1.0) 
                        valid_rows.append(row_idx)
            
            valid_vals = [v for v in col_values if v != -1.0]
            if valid_vals:
                max_val = max(valid_vals)
                min_val = min(valid_vals)
                
                for i, val in enumerate(col_values):
                    if val == -1.0: continue
                    bg_color = None
                    if val == max_val:
                        bg_color = {"red": 1.0, "green": 0.8, "blue": 0.8} 
                    elif val == min_val:
                        bg_color = {"red": 0.8, "green": 1.0, "blue": 0.8} 
                    
                    if bg_color:
                        req = {
                            "repeatCell": {
                                "range": {
                                    "sheetId": ws_dest.id,
                                    "startRowIndex": valid_rows[i],
                                    "endRowIndex": valid_rows[i] + 1,
                                    "startColumnIndex": col_idx,
                                    "endColumnIndex": col_idx + 1
                                },
                                "cell": {"userEnteredFormat": {"backgroundColor": bg_color}},
                                "fields": "userEnteredFormat.backgroundColor"
                            }
                        }
                        requests.append(req)

    try:
        sh.batch_update({"requests": requests})
    except Exception as e:
        print(f"⚠️ 格式化設定失敗 (可能是權限或版本問題): {e}")

    print(f"🎉 完成！共掃描 {total_count} 筆，本次更新 {update_count} 筆。")

if __name__ == "__main__":
    main()
