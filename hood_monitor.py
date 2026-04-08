#!/usr/bin/env python3
"""
Yahoo Finance API 응답 구조 확인용 진단 스크립트
실행: python debug_yahoo.py
"""
import json
import requests

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
URL = "https://query1.finance.yahoo.com/v8/finance/chart/HOOD"

def dump_meta(label, params):
    print(f"\n{'='*60}")
    print(f"[{label}]  params={params}")
    print('='*60)
    try:
        r = requests.get(URL, params=params, headers={"User-Agent": UA}, timeout=10)
        print(f"HTTP {r.status_code}")
        if r.status_code != 200:
            return
        meta = r.json()["chart"]["result"][0]["meta"]
        # 가격/상태 관련 키만 출력
        keys = [
            "marketState", "regularMarketPrice", "regularMarketPreviousClose",
            "chartPreviousClose", "previousClose", "preMarketPrice", "postMarketPrice",
            "regularMarketVolume", "regularMarketDayHigh", "regularMarketDayLow",
        ]
        for k in keys:
            if k in meta:
                print(f"  {k}: {meta[k]}")
        # 위에 없는 키 중 price 관련 있으면 추가 출력
        extra = {k: v for k, v in meta.items()
                 if any(x in k.lower() for x in ["price", "close", "market", "pre", "post"])
                 and k not in keys}
        if extra:
            print("  --- 추가 price 관련 필드 ---")
            for k, v in extra.items():
                print(f"  {k}: {v}")
    except Exception as e:
        print(f"ERROR: {e}")

dump_meta("interval=2m range=2d",  {"interval": "2m",  "range": "2d"})
dump_meta("interval=1d range=5d",  {"interval": "1d",  "range": "5d"})
dump_meta("interval=1m range=1d",  {"interval": "1m",  "range": "1d"})
