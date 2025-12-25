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

# --- ✅ 新增：安全的週轉率計算 ---
def calc_turnover_percent(curr_vol_shares, shares):
    try:
        if curr_vol_shares is None: return None
        if shares is None or shares <= 1: return None
        return (float(curr_vol_shares) / float(shares)) * 100.0
    except: return None

# --- ✅ 修正：完整風險計算 (支援 None) ---
def calculate_full_risk(stock_id, hist_df, fund_data, est_days, dt_today_pct, dt_avg6_pct):
    # fund_data 現在包含 shares, pe, pb (可能為 None)
    res = {'risk_level': '低', 'trigger_msg': '', 'curr_price': 0, 'limit_price': 0, 'gap_pct': 999.0, 'curr_vol': 0, 
           'limit_vol': 0, 'turnover_val': 0, 'turnover_rate': None, # 改預設為 None
           'pe': fund_data.get('pe'), 'pb': fund_data.get('pb'), 
           'day_trade_pct': dt_today_pct, 'is_triggered': False}

    if hist_df.empty or len(hist_df) < 7:
        if est_days <= 1: res['risk_level'] = '高'
        elif est_days <= 2: res['risk_level'] = '中'
        return res

    curr_close = float(hist_df.iloc[-1]['Close'])
    curr_vol_shares = float(hist_df.iloc[-1]['Volume'])
    curr_vol_lots = int(curr_vol_shares / UNIT_LOT)

    # ✅ 使用新的安全計算函式
    turnover = calc_turnover_percent(curr_vol_shares, fund_data.get('shares'))
    
    turnover_val_money = curr_close * curr_vol_shares

    res['curr_price'] = round(curr_close, 2)
    res['curr_vol'] = curr_vol_lots
    res['turnover_rate'] = round(turnover, 2) if turnover is not None else None
    res['turnover_val'] = round(turnover_val_money / 100000000, 2)

    triggers = []
    if curr_close < 5: return res

    window_7 = hist_df.tail(7)
    ref_6 = float(window_7.iloc[0]['Close'])
    rise_6 = calc_pct(curr_close, ref_6)
    price_diff_6 = abs(curr_close - ref_6)

    if rise_6 > 32: triggers.append(f"【第一款】6日漲{rise_6:.1f}%(>32%)")
    elif (rise_6 > 25) and (price_diff_6 >= 50): triggers.append(f"【第一款】6日漲{rise_6:.1f}%且價差{price_diff_6:.0f}元")

    limit_p = ref_6 * 1.32
    if (rise_6 > 25) and (price_diff_6 >= 50): limit_p = min(limit_p, ref_6 * 1.25)
    res['limit_price'] = round(limit_p, 2)
    res['gap_pct'] = round(((limit_p - curr_close)/curr_close)*100, 1)

    # 波段漲幅
    if len(hist_df)>=31 and calc_pct(curr_close, float(hist_df.iloc[-31]['Close'])) > 100: triggers.append("【第二款】30日漲>100%")
    if len(hist_df)>=61 and calc_pct(curr_close, float(hist_df.iloc[-61]['Close'])) > 130: triggers.append("【第二款】60日漲>130%")
    if len(hist_df)>=91 and calc_pct(curr_close, float(hist_df.iloc[-91]['Close'])) > 160: triggers.append("【第二款】90日漲>160%")

    # 價量異常 (需 turnover 存在)
    if len(hist_df) >= 61:
        avg_vol_60 = hist_df['Volume'].iloc[-61:-1].mean()
        if avg_vol_60 > 0:
            vol_ratio = curr_vol_shares / avg_vol_60
            res['limit_vol'] = int(avg_vol_60 * 5 / 1000)
            # ✅ 加入 turnover is not None 檢查
            if turnover is not None and turnover >= 0.1 and curr_vol_lots >= 500:
                if rise_6 > 25 and vol_ratio > 5: triggers.append(f"【第三款】漲{rise_6:.0f}%+量{vol_ratio:.1f}倍")

    # 價+週轉 (需 turnover 存在)
    if turnover is not None and turnover > 10 and rise_6 > 25:
        triggers.append(f"【第四款】漲{rise_6:.0f}%+轉{turnover:.0f}%")

    # 量能放大
    if len(hist_df) >= 61:
        avg_vol_60 = hist_df['Volume'].iloc[-61:-1].mean()
        # ✅ 加入 turnover is not None 檢查
        if turnover is not None and not (turnover < 0.1 or curr_vol_lots < 500 or turnover_val_money < 30000000):
            if avg_vol_60 > 0:
                if (hist_df['Volume'].iloc[-6:].mean() / avg_vol_60) > 5: triggers.append("【第九款】6日均量放大5倍")
                if (curr_vol_shares / avg_vol_60) > 5: triggers.append("【第九款】當日量放大5倍")

    # 累積週轉 (需 shares 存在)
    shares = fund_data.get('shares')
    if shares and shares > 1 and turnover_val_money >= 500000000:
        acc_turn = (hist_df['Volume'].iloc[-6:].sum() / shares) * 100
        if acc_turn > 50 and (turnover is not None and turnover > 10):
            triggers.append(f"【第十款】累轉{acc_turn:.0f}%")

    # 價差異常
    if len(hist_df) >= 6:
        gap = hist_df.iloc[-6:]['High'].max() - hist_df.iloc[-6:]['Low'].min()
        th = 100 + (int((curr_close - 500)/500)+1)*25 if curr_close >= 500 else 100
        if gap >= th: triggers.append(f"【第十一款】6日價差>{th}")

    # 當沖
    if dt_avg6_pct > 60 and dt_today_pct > 60:
        # ✅ 加入 turnover is not None 檢查
        if not (turnover is not None and turnover < 5 or turnover_val_money < 500000000 or (curr_vol_shares*dt_today_pct/100/1000) < 5000):
            triggers.append(f"【第十三款】當沖{dt_today_pct}%")

    if triggers:
        res['is_triggered'] = True
        res['risk_level'] = '高'
        res['trigger_msg'] = "且".join(triggers)
    elif est_days <= 1: res['risk_level'] = '高'
    elif est_days <= 2: res['risk_level'] = '中'
    
    return res

# ... (simulate_days_to_jail_strict 等其他函式請保留原樣) ...
