# -*- coding: utf-8 -*-
import re
import pandas as pd
from datetime import date

CN_NUM = {"一":"1","二":"2","三":"3","四":"4","五":"5","六":"6","七":"7","八":"8","九":"9","十":"10"}
KEYWORD_MAP = {"累積週轉率": 10, "週轉率": 4, "成交量": 9, "收盤價漲跌百分比": 1, "當日沖銷": 13}

# --- 文字處理 ---
def parse_clause_ids(text):
    if not text: return set()
    text = str(text)
    for c, n in CN_NUM.items(): text = text.replace(f"第{c}款", f"第{n}款")
    ids = set([int(m) for m in re.findall(r'第\s*(\d+)\s*款', text)])
    if not ids:
        for k, v in KEYWORD_MAP.items():
            if k in text: ids.add(v)
    return ids

def is_valid_accumulation(ids): return any(1 <= x <= 8 for x in ids)
def is_special_risk(ids): return any(9 <= x <= 14 for x in ids)

# --- 處置預測 (核心演算法) ---
def simulate_days_to_jail(status_list, clause_list):
    # status_list: [0, 1, 0, 1...] (1=注意日)
    # clause_list: ["第1款", "", "第4款"...]
    # 回傳: (最快天數, 原因)
    
    # 檢查是否「已達標」(例如連3第一款)
    streak_1 = 0
    cnt_5, cnt_10, cnt_30 = 0, 0, 0
    
    # 轉換為標準 list 並補滿 30 天
    stats = list(status_list)[-30:]
    clauses = list(clause_list)[-30:]
    
    # 現況檢查
    # (此處省略繁瑣迴圈，直接做模擬)
    
    # 模擬未來：每天都觸發「第1款」，看第幾天會爆
    for day in range(10):
        # 假設今天觸發
        current_s = stats + [1] * (day + (1 if day==0 else 0)) # 若day=0(已達標)則不加? 這裡簡化邏輯: day=0代表現況
        current_c = clauses + ["第1款"] * (day + (1 if day==0 else 0))
        
        # 倒推檢查
        s_rev = current_s[::-1]
        c_rev = current_c[::-1]
        
        # 連3第一款
        streak = 0
        for i in range(min(3, len(c_rev))):
            if 1 in parse_clause_ids(c_rev[i]): streak += 1
        
        # 累積次數 (10日6次, 30日12次...)
        c6, c12 = 0, 0
        for i in range(min(10, len(s_rev))):
            if s_rev[i] and is_valid_accumulation(parse_clause_ids(c_rev[i])): c6+=1
        for i in range(min(30, len(s_rev))):
            if s_rev[i] and is_valid_accumulation(parse_clause_ids(c_rev[i])): c12+=1
            
        reasons = []
        if streak >= 3: reasons.append("連3第一款")
        if c6 >= 6: reasons.append("10日6次")
        if c12 >= 12: reasons.append("30日12次")
        
        if reasons:
            return day, " | ".join(reasons)
            
    return 99, ""

# --- 風險計算 (還原 calculate_full_risk) ---
def calculate_risk(yahoo_data, dt_today, dt_avg6, est_days):
    res = {'risk_level': '低', 'trigger_msg': '', 'limit_price': 0, 'limit_vol': 0, 'gap_pct': 999}
    
    hist = yahoo_data['history']
    if hist.empty or len(hist) < 7: return res
    
    curr = yahoo_data['price']
    
    # 簡易計算 (完整邏輯請參照 V116.18，這裡保留關鍵判定)
    ref_6 = hist.iloc[-7]['Close']
    limit_p = ref_6 * 1.32
    res['limit_price'] = round(limit_p, 2)
    res['gap_pct'] = round((limit_p - curr)/curr*100, 1) if curr else 0
    
    avg_vol = hist['Volume'].iloc[-60:].mean()
    res['limit_vol'] = int(avg_vol * 5 / 1000)
    
    triggers = []
    # 漲幅異常
    rise_6 = (curr - ref_6)/ref_6 * 100
    if rise_6 > 32: triggers.append(f"漲幅{rise_6:.1f}%")
    
    # 當沖異常 (FinMind)
    if dt_today > 60 and dt_avg6 > 60: triggers.append(f"當沖{dt_today}%")
    
    if triggers:
        res['risk_level'] = '高'
        res['trigger_msg'] = " ".join(triggers)
    elif est_days <= 1:
        res['risk_level'] = '高'
    elif est_days <= 2:
        res['risk_level'] = '中'
        
    return res
