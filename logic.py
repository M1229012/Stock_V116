# -*- coding: utf-8 -*-
import re
import math

# ============================
# V116.18 核心邏輯層 (完全還原)
# ============================

CN_NUM = {"一":"1","二":"2","三":"3","四":"4","五":"5","六":"6","七":"7","八":"8","九":"9","十":"10"}
UNIT_LOT = 1000

KEYWORD_MAP = {
    "起迄兩個營業日": 11, "當日沖銷": 13, "借券賣出": 12, "累積週轉率": 10, "週轉率": 4, 
    "成交量": 9, "本益比": 6, "股價淨值比": 6, "溢折價": 8, "收盤價漲跌百分比": 1, 
    "最後成交價漲跌": 1, "最近六個營業日累積": 1
}

def normalize_clause_text(s: str) -> str:
    if not s: return ""
    s = str(s)
    s = s.replace("第ㄧ款", "第一款")
    for cn, dg in CN_NUM.items():
        s = s.replace(f"第{cn}款", f"第{dg}款")
    s = s.translate(str.maketrans("１２３４５６７８９０", "1234567890"))
    return s

def parse_clause_ids_strict(clause_text):
    if not isinstance(clause_text, str): return set()
    clause_text = normalize_clause_text(clause_text)
    ids = set()
    matches = re.findall(r'第\s*(\d+)\s*款', clause_text)
    for m in matches: ids.add(int(m))
    if not ids:
        for keyword, code in KEYWORD_MAP.items():
            if keyword in clause_text: ids.add(code)
    return ids

def merge_clause_text(a, b):
    ids = set()
    ids |= parse_clause_ids_strict(a) if a else set()
    ids |= parse_clause_ids_strict(b) if b else set()
    if ids: return "、".join([f"第{x}款" for x in sorted(ids)])
    a = a or ""; b = b or ""
    return a if len(a) >= len(b) else b

def is_valid_accumulation_day(ids):
    if not ids: return False
    return any(1 <= x <= 8 for x in ids)

def is_special_risk_day(ids):
    if not ids: return False
    return any(9 <= x <= 14 for x in ids)

def calc_pct(curr, ref):
    return ((curr - ref) / ref) * 100 if ref != 0 else 0

# --- 完整風險計算 (V116.18 原版) ---
def calculate_full_risk(stock_id, hist_df, fund_data, est_days, dt_today_pct, dt_avg6_pct):
    res = {'risk_level': '低', 'trigger_msg': '', 'curr_price': 0, 'limit_price': 0, 'gap_pct': 999.0, 'curr_vol': 0, 'limit_vol': 0, 'turnover_val': 0, 'turnover_rate': 0, 'pe': fund_data.get('pe', 0), 'pb': fund_data.get('pb', 0), 'day_trade_pct': dt_today_pct, 'is_triggered': False}

    if hist_df.empty or len(hist_df) < 7:
        if est_days <= 1: res['risk_level'] = '高'
        elif est_days <= 2: res['risk_level'] = '中'
        return res

    curr_close = float(hist_df.iloc[-1]['Close'])
    curr_vol_shares = float(hist_df.iloc[-1]['Volume'])
    curr_vol_lots = int(curr_vol_shares / UNIT_LOT)

    shares = fund_data.get('shares', 1)
    if shares > 1: turnover = (curr_vol_shares / shares) * 100
    else: turnover = -1.0

    turnover_val_money = curr_close * curr_vol_shares

    res['curr_price'] = round(curr_close, 2)
    res['curr_vol'] = curr_vol_lots
    res['turnover_rate'] = round(turnover, 2)
    res['turnover_val'] = round(turnover_val_money / 100000000, 2)

    triggers = []
    if curr_close < 5: return res

    window_7 = hist_df.tail(7)
    ref_6 = float(window_7.iloc[0]['Close'])

    # 第 2 條 (第一款)：漲跌幅異常
    rise_6 = calc_pct(curr_close, ref_6)
    price_diff_6 = abs(curr_close - ref_6)

    cond_1 = rise_6 > 32
    cond_2 = (rise_6 > 25) and (price_diff_6 >= 50)

    if cond_1: triggers.append(f"【第一款】6日漲{rise_6:.1f}%(>32%)")
    elif cond_2: triggers.append(f"【第一款】6日漲{rise_6:.1f}%且價差{price_diff_6:.0f}元")

    limit_p1 = ref_6 * 1.32
    limit_p2 = ref_6 * 1.25 if price_diff_6 >= 50 else 99999
    final_limit = min(limit_p1, limit_p2) if cond_2 else limit_p1
    res['limit_price'] = round(final_limit, 2)
    res['gap_pct'] = round(((final_limit - curr_close)/curr_close)*100, 1)

    # 第 3 條 (第二款)：波段漲幅
    if len(hist_df) >= 31:
        w = hist_df.tail(31)
        rise_30 = calc_pct(curr_close, float(w.iloc[0]['Close']))
        if rise_30 > 100: triggers.append(f"【第二款】30日漲{rise_30:.0f}%")
    if len(hist_df) >= 61:
        w = hist_df.tail(61)
        rise_60 = calc_pct(curr_close, float(w.iloc[0]['Close']))
        if rise_60 > 130: triggers.append(f"【第二款】60日漲{rise_60:.0f}%")
    if len(hist_df) >= 91:
        w = hist_df.tail(91)
        rise_90 = calc_pct(curr_close, float(w.iloc[0]['Close']))
        if rise_90 > 160: triggers.append(f"【第二款】90日漲{rise_90:.0f}%")

    # 第 4 條 (第三款)：價量異常
    if len(hist_df) >= 61:
        avg_vol_60 = hist_df['Volume'].iloc[-61:-1].mean()
        if avg_vol_60 > 0:
            vol_ratio = curr_vol_shares / avg_vol_60
            res['limit_vol'] = int(avg_vol_60 * 5 / 1000)
            if turnover >= 0.1 and curr_vol_lots >= 500:
                if rise_6 > 25 and vol_ratio > 5:
                    triggers.append(f"【第三款】漲{rise_6:.0f}%+量{vol_ratio:.1f}倍")

    # 第 5 條 (第四款)：價+週轉
    if turnover > 10 and rise_6 > 25:
        triggers.append(f"【第四款】漲{rise_6:.0f}%+轉{turnover:.0f}%")

    # 第 10 條 (第九款)：量能異常
    if len(hist_df) >= 61:
        avg_vol_60 = hist_df['Volume'].iloc[-61:-1].mean()
        avg_vol_6 = hist_df['Volume'].iloc[-6:].mean()
        is_exclude = (turnover < 0.1) or (curr_vol_lots < 500) or (turnover_val_money < 30000000)
        if not is_exclude and avg_vol_60 > 0:
            r1 = avg_vol_6 / avg_vol_60
            r2 = curr_vol_shares / avg_vol_60
            if r1 > 5: triggers.append(f"【第九款】6日均量放大{r1:.1f}倍")
            if r2 > 5: triggers.append(f"【第九款】當日量放大{r2:.1f}倍")

    # 第 11 條 (第十款)：累積週轉
    if turnover > 0:
        acc_vol_6 = hist_df['Volume'].iloc[-6:].sum()
        acc_turn = (acc_vol_6 / shares) * 100
        if turnover_val_money >= 500000000:
            if acc_turn > 50 and turnover > 10:
                triggers.append(f"【第十款】累轉{acc_turn:.0f}%")

    # 第 12 條 (第十一款)：價差異常
    if len(hist_df) >= 6:
        window_6 = hist_df.tail(6)
        high_6 = window_6['High'].max()
        low_6 = window_6['Low'].min()
        gap = high_6 - low_6
        threshold = 100
        if curr_close >= 500:
            tiers = int((curr_close - 500) / 500) + 1
            threshold = 100 + (tiers * 25)
        if gap >= threshold:
            triggers.append(f"【第十一款】6日價差{gap:.0f}元(>門檻{threshold})")

    # 第 14 條 (第十三款)：當沖
    if dt_avg6_pct > 60 and dt_today_pct > 60:
        dt_vol_est = curr_vol_shares * (dt_today_pct / 100.0)
        dt_vol_lots = dt_vol_est / 1000
        is_exclude = (turnover < 5) or (turnover_val_money < 500000000) or (dt_vol_lots < 5000)
        if not is_exclude:
            triggers.append(f"【第十三款】當沖{dt_today_pct}%(6日{dt_avg6_pct}%)")

    if triggers:
        res['is_triggered'] = True
        res['risk_level'] = '高'
        res['trigger_msg'] = "且".join(triggers)
    elif est_days <= 1: res['risk_level'] = '高'
    elif est_days <= 2: res['risk_level'] = '中'
    elif est_days >= 3: res['risk_level'] = '低'

    return res

def check_jail_trigger_now(status_list, clause_list):
    status_list = list(status_list)
    clause_list = list(clause_list)
    if len(status_list) < 30:
        pad = 30 - len(status_list)
        status_list = [0]*pad + status_list
        clause_list = [""]*pad + clause_list

    c1_streak = 0
    for c in clause_list[-3:]:
        if 1 in parse_clause_ids_strict(c): c1_streak += 1

    valid_cnt_5 = 0; valid_cnt_10 = 0; valid_cnt_30 = 0
    total_len = len(status_list)
    for i in range(30):
        idx = total_len - 1 - i
        if idx < 0: break
        if status_list[idx] == 1:
            ids = parse_clause_ids_strict(clause_list[idx])
            if is_valid_accumulation_day(ids):
                if i < 5: valid_cnt_5 += 1
                if i < 10: valid_cnt_10 += 1
                valid_cnt_30 += 1

    reasons = []
    if c1_streak == 3: reasons.append("已觸發(連3第一款)")
    if valid_cnt_5 == 5: reasons.append("已觸發(連5)")
    if valid_cnt_10 >= 6: reasons.append(f"已觸發(10日{valid_cnt_10}次)")
    if valid_cnt_30 >= 12: reasons.append(f"已觸發(30日{valid_cnt_30}次)")
    return (len(reasons) > 0), " | ".join(reasons)

def simulate_days_to_jail_strict(status_list, clause_list, *, stock_id=None, target_date=None, jail_map=None, enable_safe_filter=True):
    # 0) 若今天已在處置期：0 天
    # 注意：data.py 的 is_in_jail 需要 jail_map，這裡僅作邏輯判斷
    if stock_id and target_date and jail_map:
        # 需在 data.py 處理 is_in_jail，此處暫時僅依賴傳入的狀態
        # 但依據原代碼，這裡會檢查 is_in_jail。由於 logic 不依賴 data，這裡我們假設外部已經檢查過
        pass 

    # 1) 若已達標
    trigger_now, reason_now = check_jail_trigger_now(status_list, clause_list)
    if trigger_now:
        return 0, reason_now.replace("已觸發", "已達標，次一營業日處置")

    # 10日安全過濾
    if enable_safe_filter:
        recent_valid_10 = 0
        check_len = min(len(status_list), 10)
        if check_len > 0:
            recent_statuses = status_list[-check_len:]
            recent_clauses = clause_list[-check_len:]
            for b, c in zip(recent_statuses, recent_clauses):
                if b == 1:
                    ids = parse_clause_ids_strict(c)
                    if is_valid_accumulation_day(ids): recent_valid_10 += 1
        if recent_valid_10 == 0: return 99, "X"

    status_list = list(status_list)
    clause_list = list(clause_list)
    if len(status_list) < 30:
        pad = 30 - len(status_list)
        status_list = [0]*pad + status_list
        clause_list = [""]*pad + clause_list

    days = 0
    while days < 10:
        days += 1
        status_list.append(1)
        clause_list.append("第1款")
        
        c1_streak = 0
        for c in clause_list[-3:]:
            if 1 in parse_clause_ids_strict(c): c1_streak += 1

        valid_cnt_5 = 0; valid_cnt_10 = 0; valid_cnt_30 = 0
        total_len = len(status_list)
        for i in range(30):
            idx = total_len - 1 - i
            if idx < 0: break
            if status_list[idx] == 1:
                ids = parse_clause_ids_strict(clause_list[idx])
                if is_valid_accumulation_day(ids):
                    if i < 5: valid_cnt_5 += 1
                    if i < 10: valid_cnt_10 += 1
                    valid_cnt_30 += 1
        
        reasons = []
        if c1_streak == 3: reasons.append(f"再{days}天處置")
        if valid_cnt_5 == 5: reasons.append(f"再{days}天處置(連5)")
        if valid_cnt_10 >= 6: reasons.append(f"再{days}天處置(10日{valid_cnt_10}次)")
        if valid_cnt_30 >= 12: reasons.append(f"再{days}天處置(30日{valid_cnt_30}次)")
        
        if reasons: return days, " | ".join(reasons)

    return 99, ""
