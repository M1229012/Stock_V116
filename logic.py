# -*- coding: utf-8 -*-
import re
from datetime import date

CN_NUM = {"一":"1","二":"2","三":"3","四":"4","五":"5","六":"6","七":"7","八":"8","九":"9","十":"10"}
UNIT_LOT = 1000
KEYWORD_MAP = {"起迄兩個營業日":11, "當日沖銷":13, "借券賣出":12, "累積週轉率":10, "週轉率":4, "成交量":9, "本益比":6, "股價淨值比":6, "溢折價":8, "收盤價漲跌百分比":1, "最後成交價漲跌":1, "最近六個營業日累積":1}

def normalize_clause_text(s):
    if not s: return ""
    s = str(s).replace("第ㄧ款", "第一款")
    for c, d in CN_NUM.items(): s = s.replace(f"第{c}款", f"第{d}款")
    return s.translate(str.maketrans("１２３４５６７８９０", "1234567890"))

def parse_clause_ids_strict(t):
    if not isinstance(t, str): return set()
    t = normalize_clause_text(t)
    ids = set([int(m) for m in re.findall(r'第\s*(\d+)\s*款', t)])
    if not ids:
        for k, c in KEYWORD_MAP.items():
            if k in t: ids.add(c)
    return ids

def merge_clause_text(a, b):
    ids = parse_clause_ids_strict(a) | parse_clause_ids_strict(b)
    if ids: return "、".join([f"第{x}款" for x in sorted(ids)])
    return a if len(a or "") >= len(b or "") else b

def is_valid_accumulation_day(ids): return any(1<=x<=8 for x in ids)
def is_special_risk_day(ids): return any(9<=x<=14 for x in ids)
def calc_pct(c, r): return ((c-r)/r)*100 if r!=0 else 0

# --- 排除日邏輯 ---
def prev_trade_date(d, cal_dates):
    if not cal_dates: return None
    try:
        idx = cal_dates.index(d)
        if idx > 0: return cal_dates[idx-1]
    except: pass
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

def is_in_jail(stock_id, target_date, jail_map):
    if not jail_map or stock_id not in jail_map: return False
    for s, e in jail_map[stock_id]:
        if s <= target_date <= e: return True
    return False

def get_last_n_non_jail_trade_dates(stock_id, cal_dates, jail_map, exclude_map=None, n=30):
    last_end = date(1900,1,1)
    if jail_map and stock_id in jail_map: last_end = jail_map[stock_id][-1][1]
    picked = []
    for d in reversed(cal_dates):
        if d <= last_end: break
        if is_excluded(stock_id, d, exclude_map): continue
        if jail_map and is_in_jail(stock_id, d, jail_map): continue
        picked.append(d)
        if len(picked)>=n: break
    return list(reversed(picked))

# --- 處置預測 ---
def check_jail_trigger_now(status_list, clause_list):
    status_list = list(status_list); clause_list = list(clause_list)
    if len(status_list)<30:
        pad = 30-len(status_list)
        status_list=[0]*pad+status_list; clause_list=[""]*pad+clause_list
    
    c1_streak = sum(1 for c in clause_list[-3:] if 1 in parse_clause_ids_strict(c))
    v5=0; v10=0; v30=0
    
    total = len(status_list)
    for i in range(30):
        idx = total-1-i
        if idx<0: break
        if status_list[idx]==1:
            ids = parse_clause_ids_strict(clause_list[idx])
            if is_valid_accumulation_day(ids):
                if i<5: v5+=1
                if i<10: v10+=1
                v30+=1
    
    reasons = []
    if c1_streak==3: reasons.append("已觸發(連3第一款)")
    if v5==5: reasons.append("已觸發(連5)")
    if v10>=6: reasons.append(f"已觸發(10日{v10}次)")
    if v30>=12: reasons.append(f"已觸發(30日{v30}次)")
    return (len(reasons)>0), " | ".join(reasons)

def simulate_days_to_jail_strict(status_list, clause_list, *, stock_id=None, target_date=None, jail_map=None, enable_safe_filter=True):
    if stock_id and target_date and jail_map and is_in_jail(stock_id, target_date, jail_map): return 0, "處置中"
    trig, reason = check_jail_trigger_now(status_list, clause_list)
    if trig: return 0, reason.replace("已觸發", "已達標，次一營業日處置")
    
    if enable_safe_filter:
        recent_10 = 0
        check = min(len(status_list), 10)
        if check > 0:
            for b, c in zip(status_list[-check:], clause_list[-check:]):
                if b==1 and is_valid_accumulation_day(parse_clause_ids_strict(c)): recent_10+=1
        if recent_10==0: return 99, "X"

    status_list = list(status_list); clause_list = list(clause_list)
    if len(status_list)<30:
        pad=30-len(status_list)
        status_list=[0]*pad+status_list; clause_list=[""]*pad+clause_list
    
    days=0
    while days<10:
        days+=1
        status_list.append(1); clause_list.append("第1款")
        trig, _ = check_jail_trigger_now(status_list, clause_list)
        if trig:
            c1_streak = sum(1 for c in clause_list[-3:] if 1 in parse_clause_ids_strict(c))
            v5=0; v10=0; v30=0
            total=len(status_list)
            for i in range(30):
                idx=total-1-i; 
                if idx>=0 and status_list[idx]==1 and is_valid_accumulation_day(parse_clause_ids_strict(clause_list[idx])):
                    if i<5: v5+=1
                    if i<10: v10+=1
                    v30+=1
            r=[]
            if c1_streak>=3: r.append(f"再{days}天處置")
            if v5>=5: r.append(f"再{days}天處置(連5)")
            if v10>=6: r.append(f"再{days}天處置(10日{v10}次)")
            if v30>=12: r.append(f"再{days}天處置(30日{v30}次)")
            if r: return days, " | ".join(r)
            
    return 99, ""

# --- ✅ 修正：完整風險計算 (回歸原始數值: -1, 0, 999 且變數名稱一致) ---
def calculate_full_risk(stock_id, hist_df, fund_data, est_days, dt_today_pct, dt_avg6_pct):
    res = {'risk_level': '低', 'trigger_msg': '', 'curr_price': 0, 'limit_price': 0, 'gap_pct': 999.0,
           'curr_vol': 0, 'limit_vol': 0, 'turnover_val': 0, 'turnover_rate': 0,
           'pe': fund_data.get('pe', 0), 'pb': fund_data.get('pb', 0),
           'day_trade_pct': dt_today_pct, 'is_triggered': False}

    if hist_df.empty or len(hist_df) < 7:
        if est_days <= 1: res['risk_level'] = '高'
        elif est_days <= 2: res['risk_level'] = '中'
        return res

    curr_close = float(hist_df.iloc[-1]['Close'])
    curr_vol_shares = float(hist_df.iloc[-1]['Volume'])
    curr_vol_lots = int(curr_vol_shares / UNIT_LOT)

    shares = fund_data.get('shares', 1)
    if shares > 1:
        turnover = (curr_vol_shares / shares) * 100
    else:
        turnover = -1.0
    
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

    limit_p = ref_6 * 1.32
    if cond_2: limit_p = min(limit_p, ref_6 * 1.25)
    res['limit_price'] = round(limit_p, 2)
    res['gap_pct'] = round(((limit_p - curr_close)/curr_close)*100, 1)

    if len(hist_df)>=31 and calc_pct(curr_close, float(hist_df.iloc[-31]['Close'])) > 100: triggers.append("【第二款】30日漲>100%")
    if len(hist_df)>=61 and calc_pct(curr_close, float(hist_df.iloc[-61]['Close'])) > 130: triggers.append("【第二款】60日漲>130%")
    if len(hist_df)>=91 and calc_pct(curr_close, float(hist_df.iloc[-91]['Close'])) > 160: triggers.append("【第二款】90日漲>160%")

    if len(hist_df) >= 61:
        avg_vol_60 = hist_df['Volume'].iloc[-61:-1].mean()
        if avg_vol_60 > 0:
            vol_ratio = curr_vol_shares / avg_vol_60
            res['limit_vol'] = int(avg_vol_60 * 5 / 1000)
            if turnover >= 0.1 and curr_vol_lots >= 500:
                if rise_6 > 25 and vol_ratio > 5: triggers.append(f"【第三款】漲{rise_6:.0f}%+量{vol_ratio:.1f}倍")

    if turnover > 10 and rise_6 > 25:
        triggers.append(f"【第四款】漲{rise_6:.0f}%+轉{turnover:.0f}%")

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
            if acc_turn > 50 and turnover > 10:
                triggers.append(f"【第十款】累轉{acc_turn:.0f}%")

    if len(hist_df) >= 6:
        gap = hist_df.iloc[-6:]['High'].max() - hist_df.iloc[-6:]['Low'].min()
        # ✅ 修正變數名稱：統一使用 threshold
        threshold = 100 + (int((curr_close - 500)/500)+1)*25 if curr_close >= 500 else 100
        if gap >= threshold: triggers.append(f"【第十一款】6日價差{gap:.0f}元(>門檻{threshold})")

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
