# -*- coding: utf-8 -*-
import re
from datetime import date
import pandas as pd
from config import CN_NUM, KEYWORD_MAP

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
    
    # 1. 優先嘗試抓取標準格式
    matches = re.findall(r'第\s*(\d+)\s*款', clause_text)
    for m in matches:
        ids.add(int(m))
    
    # 2. 關鍵字補救
    if not ids:
        for keyword, code in KEYWORD_MAP.items():
            if keyword in clause_text:
                ids.add(code)
    return ids

def merge_clause_text(a, b):
    ids = set()
    ids |= parse_clause_ids_strict(a) if a else set()
    ids |= parse_clause_ids_strict(b) if b else set()
    if ids:
        return "、".join([f"第{x}款" for x in sorted(ids)])
    a = a or ""
    b = b or ""
    return a if len(a) >= len(b) else b

def get_ticker_suffix(market_type):
    m = str(market_type).upper().strip()
    keywords = ['上櫃', 'TWO', 'TPEX', 'OTC']
    if any(k in m for k in keywords):
        return '.TWO'
    return '.TW'

def parse_roc_date(roc_date_str):
    try:
        roc_date_str = str(roc_date_str).strip()
        parts = re.split(r'[/-]', roc_date_str)
        if len(parts) == 3:
            year = int(parts[0]) + 1911
            month = int(parts[1])
            day = int(parts[2])
            return date(year, month, day)
    except: return None
    return None

def parse_jail_period(period_str):
    if not period_str: return None, None
    dates = []
    if '～' in period_str: dates = period_str.split('～')
    elif '~' in period_str: dates = period_str.split('~')
    elif '-' in period_str and '/' in period_str:
        if period_str.count('-') == 1: dates = period_str.split('-')
    
    if len(dates) >= 2:
        start_date = parse_roc_date(dates[0].strip())
        end_date = parse_roc_date(dates[1].strip())
        if start_date and end_date:
            return start_date, end_date
    return None, None

def get_or_create_ws(sh, title, headers=None, rows=5000, cols=20):
    need_cols = max(cols, len(headers) if headers else 0)
    try:
        ws = sh.worksheet(title)
        try:
            if headers and ws.col_count < need_cols:
                ws.resize(rows=ws.row_count, cols=need_cols)
        except: pass
        return ws
    except:
        print(f"⚠️ 工作表 '{title}' 不存在，正在建立 (cols={need_cols})...")
        ws = sh.add_worksheet(title=title, rows=str(rows), cols=str(need_cols))
        if headers:
            ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws
