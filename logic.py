# -*- coding: utf-8 -*-
from datetime import date
from utils import parse_clause_ids_strict
from config import UNIT_LOT

def is_valid_accumulation_day(ids):
    if not ids: return False
    return any(1 <= x <= 8 for x in ids)

def is_special_risk_day(ids):
    if not ids: return False
    return any(9 <= x <= 14 for x in ids)

def is_in_jail(stock_id, target_date, jail_map):
    if not jail_map or stock_id not in jail_map: return False
    periods = jail_map[stock_id]
    for start, end in periods:
        if start <= target_date <= end: return True
    return False

def prev_trade_date(d, cal_dates):
    if not cal_dates: return None
    try:
        idx = cal_dates.index(d)
    except ValueError:
        idx = None
        for i in range(len(cal_dates)-1, -1, -1):
            if cal_dates[i] < d:
                idx = i
                break
        if idx is None: return None
    if idx - 1 >= 0: return cal_dates[idx - 1]
    return None

def build_exclude_map(cal_dates, jail_map):
    exclude_map = {}
    if not jail_map: return exclude_map
    for code, periods in jail_map.items():
        s = set()
        for start, end in periods:
            # 處置前一日
            pd = prev_trade_date(start, cal_dates)
            if pd: s.add(pd)
            # 處置期間
            for d in cal_dates:
                if start <= d <= end: s.add(d)
        exclude_map[code] = s
    return exclude_map

def is_excluded(code, d, exclude_map):
    return bool(exclude_map) and (code in exclude_map) and (d in exclude_map[code])

def get_last_n_non_jail_trade_dates(stock_id, cal_dates, jail_map, exclude_map=None, n=30):
    # 🔥 [Jail Reset Fix] 處置歸零邏輯
    last_jail_end = date(1900, 1, 1)
    if jail_map and stock_id in jail_map:
        last_jail_end = jail_map[stock_id][-1][1]

    picked = []
    for d in reversed(cal_dates):
        # 遇到處置結束日，停止回朔 (切斷案底)
        if d <= last_jail_end:
            break
        if is_excluded(stock_id, d, exclude_map):
            continue
        if jail_map and is_in_jail(stock_id, d, jail_map):
            continue
        picked.append(d)
        if len(picked) >= n: break
    return list(reversed(picked))

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

    valid_cnt_5 = 0
    valid_cnt_10 = 0
    valid_cnt_30 = 0
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
    # 0) 處置中
    if stock_id and target_date and jail_map and is_in_jail(stock_id, target_date, jail_map):
        return 0, "處置中"

    # 1) 當日已達標 (Trigger Zero Fix)
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
        clause_list.append("第1款") # 模擬累積條款

        c1_streak = 0
        for c in clause_list[-3:]:
            if 1 in parse_clause_ids_strict(c): c1_streak += 1

        valid_cnt_5 = 0
        valid_cnt_10 = 0
        valid_cnt_30 = 0
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

        if reasons:
            # [Day Count Fix] 直接回傳需要觸發的天數
            return days, " | ".join(reasons)

    return 99, ""

def calc_pct(curr, ref):
    return ((curr - ref) / ref) * 100 if ref != 0 else 0

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
    turnover = (curr_vol_shares / shares) * 100 if shares > 1 else -1.0
    turnover_val_money = curr_close * curr_vol_shares

    res['curr_price'] = round(curr_close, 2)
    res['curr_vol'] = curr_vol_lots
    res['turnover_rate'] = round(turnover, 2)
    res['turnover_val'] = round(turnover_val_money / 100000000, 2)

    triggers = []
    if curr_close < 5: return res

    window_7 = hist_df.tail(7)
    ref_6 = float(window_7.iloc[0]['Close'])
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

    if len(hist_df) >= 31:
        w = hist_df.tail(31)
        rise_30 = calc_pct(curr_close, float(w.iloc[0]['Close']))
        if rise_30 > 100: triggers.append(f"【第二款】30日漲{rise_30:.0f}%")
    if len(hist_df) >= 61:
        avg_vol_60 = hist_df['Volume'].iloc[-61:-1].mean()
        if avg_vol_60 > 0:
            vol_ratio = curr_vol_shares / avg_vol_60
            res['limit_vol'] = int(avg_vol_60 * 5 / 1000)
            if turnover >= 0.1 and curr_vol_lots >= 500:
                if rise_6 > 25 and vol_ratio > 5: triggers.append(f"【第三款】漲{rise_6:.0f}%+量{vol_ratio:.1f}倍")
    
    if turnover > 10 and rise_6 > 25: triggers.append(f"【第四款】漲{rise_6:.0f}%+轉{turnover:.0f}%")

    if len(hist_df) >= 61:
        avg_vol_60 = hist_df['Volume'].iloc[-61:-1].mean()
        avg_vol_6 = hist_df['Volume'].iloc[-6:].mean()
        is_exclude = (turnover < 0.1) or (curr_vol_lots < 500) or (turnover_val_money < 30000000)
        if not is_exclude and avg_vol_60 > 0:
            r1 = avg_vol_6 / avg_vol_60
            r2 = curr_vol_shares / avg_vol_60
            if r1 > 5: triggers.append(f"【第九款】6日均量放大{r1:.1f}倍")
            if r2 > 5: triggers.append(f"【第九款】當日量放大{r2:.1f}倍")

    if turnover > 0:
        acc_vol_6 = hist_df['Volume'].iloc[-6:].sum()
        acc_turn = (acc_vol_6 / shares) * 100
        if turnover_val_money >= 500000000:
            if acc_turn > 50 and turnover > 10: triggers.append(f"【第十款】累轉{acc_turn:.0f}%")

    if len(hist_df) >= 6:
        window_6 = hist_df.tail(6)
        gap = window_6['High'].max() - window_6['Low'].min()
        threshold = 100
        if curr_close >= 500: threshold = 100 + (int((curr_close - 500) / 500) + 1) * 25
        if gap >= threshold: triggers.append(f"【第十一款】6日價差{gap:.0f}元(>門檻{threshold})")

    if dt_avg6_pct > 60 and dt_today_pct > 60:
        dt_vol_lots = (curr_vol_shares * (dt_today_pct / 100.0)) / 1000
        is_exclude = (turnover < 5) or (turnover_val_money < 500000000) or (dt_vol_lots < 5000)
        if not is_exclude: triggers.append(f"【第十三款】當沖{dt_today_pct}%(6日{dt_avg6_pct}%)")

    if triggers:
        res['is_triggered'] = True
        res['risk_level'] = '高'
        res['trigger_msg'] = "且".join(triggers)
    elif est_days <= 1: res['risk_level'] = '高'
    elif est_days <= 2: res['risk_level'] = '中'
    elif est_days >= 3: res['risk_level'] = '低'
    return res
