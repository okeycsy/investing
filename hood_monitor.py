#!/usr/bin/env python3
import requests
from datetime import datetime, timezone

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
URL = "https://query1.finance.yahoo.com/v8/finance/chart/HOOD"

def dump(label, params):
    print(f"\n{'='*60}\n[{label}]\n{'='*60}")
    r = requests.get(URL, params=params, headers={"User-Agent": UA}, timeout=10)
    print(f"HTTP {r.status_code}")
    if r.status_code != 200:
        print(r.text[:200]); return
    result = r.json()["chart"]["result"][0]
    meta = result["meta"]

    for k in ["marketState","regularMarketPrice","regularMarketPreviousClose",
              "chartPreviousClose","previousClose","preMarketPrice","postMarketPrice",
              "regularMarketDayHigh","regularMarketDayLow","hasPrePostMarketData"]:
        print(f"  {k}: {meta.get(k,'— 없음 —')}")

    ts_val = meta.get("regularMarketTime")
    if ts_val:
        print(f"  regularMarketTime: {datetime.fromtimestamp(ts_val, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    timestamps = result.get("timestamp", [])
    closes = result["indicators"]["quote"][0].get("close", [])
    print(f"\n  quotes 배열: {len(timestamps)}개 바 (마지막 5개)")
    for i in range(max(0, len(timestamps)-5), len(timestamps)):
        dt = datetime.fromtimestamp(timestamps[i], tz=timezone.utc).strftime('%m/%d %H:%M UTC')
        print(f"    {dt}  close={closes[i] if i < len(closes) else None}")

dump("1m / 1d (기본)", {"interval":"1m","range":"1d"})
dump("1m / 1d + includePrePost=true", {"interval":"1m","range":"1d","includePrePost":"true"})
dump("2m / 2d + includePrePost=true", {"interval":"2m","range":"2d","includePrePost":"true"})
