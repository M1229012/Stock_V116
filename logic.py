# -*- coding: utf-8 -*-
import utils
from datetime import date

UNIT_LOT = 1000

# ==========================================
# 邏輯判斷
# ==========================================
def is_valid_accumulation_day(ids):
    if not ids: return False
    return any(1 <= x <= 8 for x in ids)

def is_special_risk_day(ids):
    if not ids: return False
    return any(9 <= x <= 14 for x in ids)

def calc_pct(curr, ref):
    return ((curr - ref) / ref) * 100 if ref != 0 else 0

def is_in_jail(stock_id, target_date, jail_map):
    if not jail_map or stock_id not in jail_map: return False
    for start, end in jail_map[stock_id]:
        if start <= target_date <= end: return True
    return False

def prev_trade_date(d, cal_dates):
    if not cal_dates: return None
    try: idx = cal_dates.index(d)
    except:
        for i in range(len(cal_dates)-1, -1, -1):
            if cal_dates[i] < d: return cal_dates[i]
        return None
    if idx - 1 >= 0: return cal_dates[idx - 1]
    return None

def build_exclude_map(cal_dates, jail_map):
    exclude_map = {}
    if not jail_map: return exclude_map
    for code, periods in jail_map.items():
        s = set()
        for start, end in periods:
            pd = prev_trade_date(start, cal_dates)
            if pd: s.add(pd)
            for d in cal_dates:
                if start <= d <= end: s.add(d)
        exclude_map[code] = s
    return exclude_map

def is_excluded(code, d, exclude_map):
    return bool(exclude_map) and (code in exclude_map) and (d in exclude_map[code])

def get_last_n_non_jail_trade_dates(stock_id, cal_dates, jail_map, exclude_map=None, n=30):
    last_jail_end = date(1900, 1, 1)
    if jail_map and stock_id in jail_map:
        last_jail_end = jail_map[stock_id][-1][1]
    picked = []
    for d in reversed(cal_dates):
        if d <= last_jail_end: break
        if is_excluded(stock_id, d, exclude_map): continue
        if jail_map and is_in_jail(stock_id, d, jail_map): continue
        picked.append(d)
        if len(picked) >= n: break
    return list(reversed(picked))

def check_jail_trigger_now(status_list, clause_list):
    status_list = list(status_list); clause_list = list(clause_list)
    if len(status_list) < 30:
        pad = 30 - len(status_list)
        status_list = [0]*pad + status_list; clause_list = [""]*pad + clause_list
    
    c1_streak = 0
    for c in clause_list[-3:]:
        if 1 in utils.parse_clause_ids_strict(c): c1_streak += 1
    
    cnt_5 = 0; cnt_10 = 0; cnt_30 = 0
    total = len(status_list)
    for i in range(30):
        idx = total - 1 - i
        if idx < 0: break
        if status_list[idx] == 1:
            ids = utils.parse_clause_ids_strict(clause_list[idx])
            if is_valid_accumulation_day(ids):
                if i < 5: cnt_5 += 1
                if i < 10: cnt_10 += 1
                cnt_30 += 1
    
    reasons = []
    if c1_streak == 3: reasons.append("已觸發(連3第一款)")
    if cnt_5 == 5: reasons.append("已觸發(連5)")
    if cnt_10 >= 6: reasons.append(f"已觸發(10日{cnt_10}次)")
    if cnt_30 >= 12: reasons.append(f"已觸發(30日{cnt_30}次)")
    return (len(reasons)>0), " | ".join(reasons)

def simulate_days_to_jail_strict(status_list, clause_list, *, stock_id=None, target_date=None, jail_map=None, enable_safe_filter=True):
    if stock_id and target_date and jail_map and is_in_jail(stock_id, target_date, jail_map):
        return 0, "處置中"
    
    trig, reason = check_jail_trigger_now(status_list, clause_list)
    if trig: return 0, reason.replace("已觸發", "已達標，次一營業日處置")

    if enable_safe_filter:
        valid_10 = 0
        check = min(len(status_list), 10)
        if check > 0:
            for b, c in zip(status_list[-check:], clause_list[-check:]):
                if b==1 and is_valid_accumulation_day(utils.parse_clause_ids_strict(c)): valid_10 += 1
        if valid_10 == 0: return 99, "X"

    status_list = list(status_list); clause_list = list(clause_list)
    if len(status_list) < 30:
        pad = 30 - len(status_list)
        status_list = [0]*pad + status_list; clause_list = [""]*pad + clause_list
    
    days = 0
    while days < 10:
        days += 1
        status_list.append(1); clause_list.append("第1款")
        
        c1 = 0
        for c in clause_list[-3:]:
            if 1 in utils.parse_clause_ids_strict(c): c1 += 1
        
        cnt_5=0; cnt_10=0; cnt_30=0
        total = len(status_list)
        for i in range(30):
            idx = total - 1 - i
            if idx < 0: break
            if status_list[idx] == 1:
                ids = utils.parse_clause_ids_strict(clause_list[idx])
                if is_valid_accumulation_day(ids):
                    if i < 5: cnt_5 += 1
                    if i < 10: cnt_10 += 1
                    cnt_30 += 1
        
        reasons = []
        if c1 == 3: reasons.append(f"再{days}天處置")
        if cnt_5 == 5: reasons.append(f"再{days}天處置(連5)")
        if cnt_10 >= 6: reasons.append(f"再{days}天處置(10日{cnt_10}次)")
        if cnt_30 >= 12: reasons.append(f"再{days}天處置(30日{cnt_30}次)")
        
        if reasons: return days, " | ".join(reasons)
    return 99, ""

def calculate_full_risk(stock_id, hist_df, fund_data, est_days, dt_today, dt_avg6):
    res = {'risk_level': '低', 'trigger_msg': '', 'curr_price': 0, 'limit_price': 0, 'gap_pct': 999.0, 'curr_vol': 0, 'limit_vol': 0, 'turnover_val': 0, 'turnover_rate': 0, 'pe': fund_data.get('pe', 0), 'pb': fund_data.get('pb', 0), 'day_trade_pct': dt_today, 'is_triggered': False}
    
    if hist_df.empty or len(hist_df) < 7:
        if est_days <= 1: res['risk_level'] = '高'
        elif est_days <= 2: res['risk_level'] = '中'
        return res

    close = float(hist_df.iloc[-1]['Close'])
    vol = float(hist_df.iloc[-1]['Volume'])
    shares = fund_data.get('shares', 1)
    turnover = (vol / shares * 100) if shares > 1 else -1.0
    val_money = close * vol

    res['curr_price'] = round(close, 2)
    res['curr_vol'] = int(vol / UNIT_LOT)
    res['turnover_rate'] = round(turnover, 2)
    res['turnover_val'] = round(val_money / 100000000, 2)

    if close < 5: return res
    triggers = []
    w7 = hist_df.tail(7); ref6 = float(w7.iloc[0]['Close'])
    rise6 = calc_pct(close, ref6); diff6 = abs(close - ref6)
    
    if rise6 > 32: triggers.append(f"【第一款】6日漲{rise6:.1f}%")
    elif rise6 > 25 and diff6 >= 50: triggers.append(f"【第一款】6日漲{rise6:.1f}%且價差{diff6:.0f}")
    
    lim1 = ref6 * 1.32; lim2 = ref6 * 1.25 if diff6 >= 50 else 99999
    final_limit = min(lim1, lim2) if (rise6 > 25 and diff6 >= 50) else lim1
    res['limit_price'] = round(final_limit, 2)
    res['gap_pct'] = round(((final_limit - close)/close)*100, 1)

    if len(hist_df) >= 31 and calc_pct(close, float(hist_df.tail(31).iloc[0]['Close'])) > 100: triggers.append("【第二款】30日漲100%")
    if len(hist_df) >= 61 and calc_pct(close, float(hist_df.tail(61).iloc[0]['Close'])) > 130: triggers.append("【第二款】60日漲130%")
    if len(hist_df) >= 91 and calc_pct(close, float(hist_df.tail(91).iloc[0]['Close'])) > 160: triggers.append("【第二款】90日漲160%")

    if len(hist_df) >= 61:
        avg60 = hist_df['Volume'].iloc[-61:-1].mean()
        if avg60 > 0:
            res['limit_vol'] = int(avg60 * 5 / 1000)
            if turnover >= 0.1 and (vol/UNIT_LOT) >= 500 and rise6 > 25 and (vol/avg60) > 5:
                triggers.append(f"【第三款】漲{rise6:.0f}%+量{vol/avg60:.1f}倍")
            
            avg6 = hist_df['Volume'].iloc[-6:].mean()
            if not ((turnover < 0.1) or (vol/UNIT_LOT < 500) or (val_money < 30000000)):
                if (avg6/avg60) > 5: triggers.append(f"【第九款】6日均量放大{avg6/avg60:.1f}倍")
                if (vol/avg60) > 5: triggers.append(f"【第九款】當日量放大{vol/avg60:.1f}倍")

    if turnover > 10 and rise6 > 25: triggers.append(f"【第四款】漲{rise6:.0f}%+轉{turnover:.0f}%")
    
    if turnover > 0:
        acc = (hist_df['Volume'].iloc[-6:].sum() / shares * 100)
        if val_money >= 500000000 and acc > 50 and turnover > 10: triggers.append(f"【第十款】累轉{acc:.0f}%")

    if len(hist_df) >= 6:
        w6 = hist_df.tail(6); gap = w6['High'].max() - w6['Low'].min()
        th = 100 + (int((close-500)/500)+1)*25 if close >= 500 else 100
        if gap >= th: triggers.append(f"【第十一款】6日價差{gap:.0f}")

    if dt_avg6 > 60 and dt_today > 60:
        dt_lot = (vol * dt_today / 100) / 1000
        if not ((turnover < 5) or (val_money < 500000000) or (dt_lot < 5000)):
            triggers.append(f"【第十三款】當沖{dt_today}%")

    if triggers:
        res['is_triggered'] = True; res['risk_level'] = '高'; res['trigger_msg'] = "且".join(triggers)
    elif est_days <= 1: res['risk_level'] = '高'
    elif est_days <= 2: res['risk_level'] = '中'
    return res
