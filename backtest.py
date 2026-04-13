#!/usr/bin/env python3
"""
$HOOD V7 Score-Forward Return Backtester + Dynamic DCA Simulator
=================================================================
사용법:
  python backtest.py                        # 기본 ($HOOD, 2년)
  python backtest.py --ticker NVDA --years 3
  python backtest.py --no-slack
"""

import os, sys, json, time, logging, argparse, requests
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from market_scan import score_ticker, SP500
except ImportError:
    print("market_scan.py를 같은 디렉토리에 놓아주세요.")
    sys.exit(1)

try:
    import yfinance as yf
except ImportError:
    print("pip install yfinance")
    sys.exit(1)

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────
SLACK_WEBHOOK   = os.environ.get("MARKET_SCAN_WEBHOOK") or os.environ.get("SLACK_WEBHOOK_URL", "")
IMGUR_CLIENT_ID = os.environ.get("IMGUR_CLIENT_ID", "")
KST             = timezone(timedelta(hours=9))

OUTLIER_CLIP  = 0.01
WARMUP_DAYS   = 130
BUCKETS       = [(0,10),(11,20),(21,30),(31,40),(41,50),(51,60),(61,75),(76,100)]
BUCKET_LABELS = ["0-10","11-20","21-30","31-40","41-50","51-60","61-75","76-100"]
HORIZONS      = [5, 10, 20, 30, 60]
LAYERS        = {"A": {"name": "CMF",  "max": 50},
                 "B": {"name": "EvsR", "max": 30}}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("backtest")


# ═══════════════════════════════════════════════════════════════
# 1. 데이터 다운로드
# ═══════════════════════════════════════════════════════════════
def download_data(ticker: str, years: int = 2) -> pd.DataFrame:
    total_days = years * 365 + WARMUP_DAYS + 80
    log.info(f"${ticker} 다운로드 중 ({years}년 + 워밍업)...")
    df = yf.download(ticker, period=f"{total_days}d", interval="1d",
                     auto_adjust=True, progress=False)
    if df.empty:
        raise ValueError(f"${ticker} 데이터 없음")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Open","High","Low","Close","Volume"]].dropna()
    log.info(f"  다운로드 완료: {len(df)}일 ({df.index[0].date()} ~ {df.index[-1].date()})")
    return df


def download_macro_history(years: int) -> pd.DataFrame:
    """BTC-USD + ^VIX 히스토리. 날짜별 btc_above(bool), vix(float) 반환."""
    total_days = years * 365 + WARMUP_DAYS + 80
    macro = pd.DataFrame()
    try:
        btc = yf.download("BTC-USD", period=f"{total_days}d", interval="1d",
                          auto_adjust=True, progress=False)
        if isinstance(btc.columns, pd.MultiIndex):
            btc.columns = btc.columns.get_level_values(0)
        btc_close = btc["Close"].dropna()
        macro["btc_close"] = btc_close
        macro["btc_sma20"] = btc_close.rolling(20).mean()
        macro["btc_above"] = btc_close > macro["btc_sma20"]
    except Exception as e:
        log.warning(f"BTC 다운로드 실패: {e}")

    try:
        vix = yf.download("^VIX", period=f"{total_days}d", interval="1d",
                          auto_adjust=True, progress=False)
        if isinstance(vix.columns, pd.MultiIndex):
            vix.columns = vix.columns.get_level_values(0)
        macro["vix"] = vix["Close"].reindex(macro.index if not macro.empty else vix.index,
                                            method="ffill")
    except Exception as e:
        log.warning(f"VIX 다운로드 실패: {e}")

    if macro.empty:
        return macro
    macro = macro.dropna(subset=["btc_sma20"] if "btc_sma20" in macro.columns else [])
    log.info(f"Macro 히스토리: {len(macro)}일")
    return macro


# ═══════════════════════════════════════════════════════════════
# 2. 롤링 스코어 계산 (Lookahead-free + Layer C Macro 적용)
# ═══════════════════════════════════════════════════════════════
def compute_rolling_scores(raw_df: pd.DataFrame, ticker: str,
                            years: int, macro_df: pd.DataFrame) -> pd.DataFrame:
    closes  = raw_df["Close"].tolist()
    highs   = raw_df["High"].tolist()
    lows    = raw_df["Low"].tolist()
    volumes = raw_df["Volume"].astype(int).tolist()
    dates   = raw_df.index.tolist()
    n       = len(dates)
    target_start = max(WARMUP_DAYS, n - years * 252)

    log.info(f"롤링 스코어: {dates[target_start].date()} ~ {dates[-1].date()} "
             f"({n - target_start}일)")

    records = []
    for i in range(target_start, n):
        d = dates[i]
        ohlcv = {"closes": closes[:i+1], "highs": highs[:i+1],
                 "lows": lows[:i+1], "volumes": volumes[:i+1]}

        # Macro 파라미터 (날짜별 BTC/VIX)
        btc_above = False
        vix_val   = 0.0
        if not macro_df.empty and d in macro_df.index:
            btc_above = bool(macro_df.loc[d, "btc_above"]) if "btc_above" in macro_df.columns else False
            vix_val   = float(macro_df.loc[d, "vix"])      if "vix" in macro_df.columns else 0.0

        sector = SP500.get(ticker, "Unknown")
        # score_ticker 호출 — btc/vix 파라미터 없는 구버전 호환
        try:
            ts = score_ticker(ticker, sector, ohlcv,
                              btc_above_sma20=btc_above, vix=vix_val)
        except TypeError:
            ts = score_ticker(ticker, sector, ohlcv)

        # Layer C 배수 결정 (VIX 우선) — 외부에서 raw에 직접 적용
        if vix_val >= 25:
            mult = 0.8
            macro_state = f"VIX패닉({vix_val:.0f})"
        elif btc_above:
            mult = 1.2
            macro_state = "BTC순풍"
        else:
            mult = 1.0
            macro_state = "평시"

        # 배수 적용 (score_ticker가 내부에서 안 했을 경우 외부에서 보정)
        if not ts.error and mult != 1.0:
            new_score = min(100, round(ts.raw * mult / 80 * 100))
            ts.score = new_score
            if   new_score >= 80: ts.grade, ts.grade_emoji = "Strong Buy", "🟢🟢"
            elif new_score >= 60: ts.grade, ts.grade_emoji = "Buy",        "🟢"
            elif new_score >= 40: ts.grade, ts.grade_emoji = "Neutral",    "⚪"
            elif new_score >= 20: ts.grade, ts.grade_emoji = "Caution",    "🟡"
            else:                 ts.grade, ts.grade_emoji = "Avoid",      "🔴"

        records.append({
            "date":        d,
            "close":       closes[i],
            "score":       ts.score if not ts.error else None,
            "raw":         ts.raw   if not ts.error else None,
            "cmf":         ts.cmf,
            "evsr":        ts.evsr,
            "upvol":       ts.upvol,
            "rsi":         ts.rsi,
            "layer_A":     ts.layers.get("A", 0),
            "layer_B":     ts.layers.get("B", 0),
            "macro_mult":  mult,
            "macro_state": macro_state,
            "btc_above":   btc_above,
            "vix":         vix_val,
            # CMF 상태 레이블 (교차분석용)
            "cmf_state": (
                "양수유지" if (ts.cmf > 0.02 and ts.layers.get("A", 0) == 50) else
                "전환중"   if (ts.cmf <= 0.02 and ts.layers.get("A", 0) == 30) else
                "전환완료" if ts.layers.get("A", 0) == 10 else
                "중립"     if ts.layers.get("A", 0) == 5  else "매도압력"
            ),
            "evsr_tier": (
                "최강(≥2.5)" if ts.evsr >= 2.5 else
                "강(≥2.0)"   if ts.evsr >= 2.0 else
                "중(≥1.5)"   if ts.evsr >= 1.5 else
                "약(≥1.0)"   if ts.evsr >= 1.0 else "없음(<1.0)"
            ),
        })
        if (i - target_start) % 50 == 0:
            log.info(f"  {i - target_start}/{n - target_start}일 처리 중...")

    result = pd.DataFrame(records).set_index("date")
    result = result.dropna(subset=["score"])
    log.info(f"스코어 계산 완료: {len(result)}개 유효 날짜")
    return result


# ═══════════════════════════════════════════════════════════════
# 3. Forward Return 라벨링 + 이상치 제거
# ═══════════════════════════════════════════════════════════════
def label_forward_returns(scored: pd.DataFrame, raw_df: pd.DataFrame) -> pd.DataFrame:
    closes = raw_df["Close"]
    for h in HORIZONS:
        future = closes.shift(-h)
        scored[f"return_{h}d"] = (future - closes) / closes
    scored = scored.dropna(subset=[f"return_{h}d" for h in HORIZONS])

    for h in HORIZONS:
        col = f"return_{h}d"
        lo  = scored[col].quantile(OUTLIER_CLIP)
        hi  = scored[col].quantile(1 - OUTLIER_CLIP)
        n_out = ((scored[col] < lo) | (scored[col] > hi)).sum()
        if n_out:
            log.info(f"  {col} 이상치 {n_out}개 제거 (< {lo:.1%} or > {hi:.1%})")
        scored.loc[(scored[col] < lo) | (scored[col] > hi), col] = np.nan
    return scored


# ═══════════════════════════════════════════════════════════════
# 4. 버킷 분석
# ═══════════════════════════════════════════════════════════════
def bucket_analysis(df: pd.DataFrame) -> dict:
    def assign(score):
        for (lo, hi), label in zip(BUCKETS, BUCKET_LABELS):
            if lo <= score <= hi:
                return label
        return None
    df = df.copy()
    df["bucket"] = df["score"].apply(assign)
    results = {}
    for label in BUCKET_LABELS:
        sub = df[df["bucket"] == label]
        results[label] = {"count": len(sub), "horizons": {}}
        for h in HORIZONS:
            col  = f"return_{h}d"
            data = sub[col].dropna()
            if len(data) < 3:
                results[label]["horizons"][h] = None
                continue
            avg = data.mean()
            wr  = (data > 0).mean()
            results[label]["horizons"][h] = {
                "count":   len(data),
                "avg":     avg,
                "median":  data.median(),
                "winrate": wr,
                "ev":      avg * wr,
                "std":     data.std(),
            }
    return results


def find_sweet_spot(bucket_results: dict, horizon: int = 10) -> dict:
    candidates = []
    for label, data in bucket_results.items():
        hd = data["horizons"].get(horizon)
        if not hd or hd["count"] < 5:
            continue
        if hd["winrate"] >= 0.50 and hd["avg"] > 0:
            candidates.append({"bucket": label, **hd})
    return max(candidates, key=lambda x: x["ev"]) if candidates else {}


# ═══════════════════════════════════════════════════════════════
# 5. 레이어 분석
# ═══════════════════════════════════════════════════════════════
def layer_correlation_analysis(df: pd.DataFrame) -> dict:
    results = {}
    for lid, meta in LAYERS.items():
        col = f"layer_{lid}"
        if col not in df.columns:
            continue
        h_results = {}
        for h in HORIZONS:
            rc = f"return_{h}d"
            sub = df[[col, rc]].dropna()
            if len(sub) < 10:
                h_results[h] = None
                continue
            spearman = sub[col].rank().corr(sub[rc].rank())
            med = sub[col].median()
            top = sub[sub[col] >= med][rc].mean()
            bot = sub[sub[col] <  med][rc].mean()
            h_results[h] = {"spearman": round(spearman, 4), "edge": round(top - bot, 4)}
        results[lid] = {"name": meta["name"], "max": meta["max"], "horizons": h_results}
    return results


def layer_bucket_analysis(df: pd.DataFrame) -> dict:
    results = {}
    for lid in LAYERS:
        col = f"layer_{lid}"
        if col not in df.columns:
            continue
        df2 = df.copy()
        tier_results = {}
        for tier, (lo_q, hi_q) in [("LOW",(0,.33)),("MID",(.33,.67)),("HIGH",(.67,1.))]:
            lo = df[col].quantile(lo_q); hi = df[col].quantile(hi_q)
            mask = (df2[col] <= lo) if tier=="LOW" else \
                   (df2[col] > hi)  if tier=="HIGH" else \
                   ((df2[col] > lo) & (df2[col] <= hi))
            sub = df2[mask]
            h_res = {}
            for h in HORIZONS:
                data = sub[f"return_{h}d"].dropna()
                if len(data) < 3:
                    h_res[h] = None; continue
                avg = data.mean(); wr = (data > 0).mean()
                h_res[h] = {"count": len(data), "avg": avg, "winrate": wr, "ev": avg*wr}
            tier_results[tier] = {"count": int(mask.sum()), "horizons": h_res}
        results[lid] = tier_results
    return results


def layer_combo_analysis(df: pd.DataFrame) -> list:
    df = df.copy()
    layer_ids = [lid for lid in LAYERS if f"layer_{lid}" in df.columns]
    for lid in layer_ids:
        df[f"{lid}_hi"] = (df[f"layer_{lid}"] >= df[f"layer_{lid}"].median()).astype(int)
    combos = []
    for i, l1 in enumerate(layer_ids):
        for l2 in layer_ids[i+1:]:
            for v1, v2 in [(1,1),(1,0),(0,1)]:
                mask = (df[f"{l1}_hi"]==v1) & (df[f"{l2}_hi"]==v2)
                ret  = df[mask]["return_10d"].dropna()
                if len(ret) < 5: continue
                avg = ret.mean(); wr = (ret > 0).mean()
                combos.append({
                    "combo":   f"{l1}{'↑' if v1 else '↓'} & {l2}{'↑' if v2 else '↓'}",
                    "count":   len(ret),
                    "avg_10d": round(avg, 4),
                    "wr_10d":  round(wr, 4),
                    "ev_10d":  round(avg * wr, 4),
                })
    return sorted(combos, key=lambda x: -x["ev_10d"])


def layer_A_subanalysis(df: pd.DataFrame) -> dict:
    indicators = {
        "CMF":   ("cmf",   [-0.15,-0.05,0.05,0.15],
                            ["강매도(<-0.15)","매도압력","중립","매수압력","강매수(>0.15)"]),
        "EvsR":  ("evsr",  [1.0,1.5,2.0,2.5],
                            ["없음(<1)","약(≥1.0)","중(≥1.5)","강(≥2.0)","최강(≥2.5)"]),
        "UpVol": ("upvol", [0.75,1.0,1.5,2.0],
                            ["강하락거래량","하락우세","중립","상승우세","강상승(>2)"]),
    }
    results = {}
    for name, (col, thresholds, labels) in indicators.items():
        if col not in df.columns: continue
        def assign(val):
            for idx, t in enumerate(thresholds):
                if val <= t: return labels[idx]
            return labels[-1]
        df2 = df.copy(); df2["_b"] = df2[col].apply(assign)
        ind = {"col": col, "labels": labels, "horizons": {}, "buckets": {}}
        for h in HORIZONS:
            sub = df[[col, f"return_{h}d"]].dropna()
            if len(sub) < 10: ind["horizons"][h] = None; continue
            sp = sub[col].rank().corr(sub[f"return_{h}d"].rank())
            med = sub[col].median()
            top = sub[sub[col] >= med][f"return_{h}d"].mean()
            bot = sub[sub[col] <  med][f"return_{h}d"].mean()
            ind["horizons"][h] = {"spearman": round(sp,4), "edge": round(top-bot,4)}
        for lb in labels:
            data = df2[df2["_b"]==lb]["return_10d"].dropna()
            if len(data) < 3: ind["buckets"][lb] = None; continue
            avg = data.mean(); wr = (data > 0).mean()
            ind["buckets"][lb] = {"count":len(data),"avg":avg,"winrate":wr,"ev":avg*wr}
        results[name] = ind
    return results


def cmf_evsr_cross_analysis(df: pd.DataFrame) -> dict:
    cmf_order  = ["양수유지","전환중","전환완료","중립","매도압력"]
    evsr_order = ["최강(≥2.5)","강(≥2.0)","중(≥1.5)","약(≥1.0)","없음(<1.0)"]
    matrix = {}
    for cs in cmf_order:
        sub = df[df["cmf_state"]==cs]
        r10 = sub["return_10d"].dropna(); r20 = sub["return_20d"].dropna()
        matrix[cs] = {"_total": {
            "count": len(r10),
            "avg_10d": round(r10.mean(),4) if len(r10) else None,
            "wr_10d":  round((r10>0).mean(),4) if len(r10) else None,
            "avg_20d": round(r20.mean(),4) if len(r20) else None,
        }}
        for es in evsr_order:
            s2 = sub[sub["evsr_tier"]==es]
            rd = s2["return_10d"].dropna()
            if len(rd) < 3: matrix[cs][es] = None; continue
            avg = rd.mean(); wr = (rd>0).mean()
            matrix[cs][es] = {"count":len(rd),"avg_10d":round(avg,4),
                               "wr_10d":round(wr,4),"ev_10d":round(avg*wr,4)}
    return {"cmf_order":cmf_order,"evsr_order":evsr_order,"matrix":matrix}


# ═══════════════════════════════════════════════════════════════
# 6. Dynamic DCA 시뮬레이터
# ═══════════════════════════════════════════════════════════════
def run_dca_simulation(scored: pd.DataFrame, raw_close: pd.Series) -> dict:
    """
    Dynamic DCA — Cash Pool 방식.
    매일 $100 입금 → 점수별 차등 매수:
      80-100: $200 (cash pool 한도 내)
      60- 79: $100
      40- 59: $50
       0- 39: $0  (현금 누적)
    총 투입 원금 = Baseline과 동일 ($100 × N일).
    """
    cash_pool = 0.0; shares = 0.0; total_input = 0.0
    pv_list = []; inv_list = []; log_rows = []

    for date, row in scored.iterrows():
        if date not in raw_close.index: continue
        price = float(raw_close[date])
        if pd.isna(price) or price <= 0: continue

        score = row["score"]
        cash_pool   += 100.0
        total_input += 100.0

        target = (200.0 if score >= 80 else
                  100.0 if score >= 60 else
                   50.0 if score >= 40 else 0.0)
        buy = min(target, cash_pool)
        if buy > 0:
            shares    += buy / price
            cash_pool -= buy

        pv = shares * price + cash_pool
        pv_list.append((date, pv))
        inv_list.append((date, total_input))
        log_rows.append({"date": date, "score": score, "price": price,
                         "buy": buy, "shares": round(shares,4),
                         "cash_pool": round(cash_pool,2), "portfolio": round(pv,2)})

    if not pv_list:
        return {}
    pv  = pd.Series([v for _,v in pv_list],  index=[d for d,_ in pv_list])
    inv = pd.Series([v for _,v in inv_list], index=[d for d,_ in inv_list])
    peak = pv.cummax()
    mdd  = float(((pv - peak) / peak).min())
    final = float(pv.iloc[-1])
    ret  = (final - total_input) / total_input if total_input else 0.0
    return {
        "portfolio_values":    pv,
        "invested_values":     inv,
        "shares":              round(shares, 4),
        "cash_pool_remaining": round(cash_pool, 2),
        "total_input":         round(total_input, 2),
        "final_value":         round(final, 2),
        "total_return":        round(ret, 4),
        "avg_cost":            round(total_input / shares, 4) if shares > 0 else 0.0,
        "mdd":                 round(mdd, 4),
        "log":                 log_rows,
    }


def run_baseline_dca(scored: pd.DataFrame, raw_close: pd.Series) -> dict:
    """Baseline: 매일 무조건 $100 매수."""
    shares = 0.0; total_input = 0.0
    pv_list = []; inv_list = []

    for date in scored.index:
        if date not in raw_close.index: continue
        price = float(raw_close[date])
        if pd.isna(price) or price <= 0: continue
        total_input += 100.0
        shares      += 100.0 / price
        pv = shares * price
        pv_list.append((date, pv))
        inv_list.append((date, total_input))

    if not pv_list:
        return {}
    pv  = pd.Series([v for _,v in pv_list],  index=[d for d,_ in pv_list])
    inv = pd.Series([v for _,v in inv_list], index=[d for d,_ in inv_list])
    peak = pv.cummax()
    mdd  = float(((pv - peak) / peak).min())
    final = float(pv.iloc[-1])
    ret  = (final - total_input) / total_input if total_input else 0.0
    return {
        "portfolio_values":    pv,
        "invested_values":     inv,
        "shares":              round(shares, 4),
        "cash_pool_remaining": 0.0,
        "total_input":         round(total_input, 2),
        "final_value":         round(final, 2),
        "total_return":        round(ret, 4),
        "avg_cost":            round(total_input / shares, 4) if shares > 0 else 0.0,
        "mdd":                 round(mdd, 4),
    }


# ═══════════════════════════════════════════════════════════════
# 7. 시각화
# ═══════════════════════════════════════════════════════════════
def generate_charts(df, bucket_results, ticker, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    files = []

    # ① 히트맵
    heat = [[bucket_results[lb]["horizons"].get(h, None) for lb in BUCKET_LABELS]
            for h in HORIZONS]
    heat_df = pd.DataFrame(
        [[hd["avg"]*100 if hd else np.nan for hd in row] for row in heat],
        index=[f"{h}d" for h in HORIZONS], columns=BUCKET_LABELS)
    fig, ax = plt.subplots(figsize=(11, 4))
    sns.heatmap(heat_df, annot=True, fmt=".1f", center=0, cmap="RdYlGn",
                linewidths=0.5, cbar_kws={"label":"Avg Return (%)"},
                ax=ax, annot_kws={"size":10,"weight":"bold"})
    ax.set_title(f"${ticker} — V7 Score vs Forward Return (이상치 {OUTLIER_CLIP*100:.0f}% 제거)",
                 fontsize=12)
    plt.tight_layout()
    p = output_dir / f"{ticker}_heatmap.png"
    plt.savefig(p, dpi=150, bbox_inches="tight"); plt.close(); files.append(p)

    # ② 분포
    hi_ret = df[df["score"] >= 61]["return_20d"].dropna() * 100
    lo_ret = df[df["score"] <= 40]["return_20d"].dropna() * 100
    all_vals = pd.concat([hi_ret, lo_ret])
    if len(all_vals):
        bins = np.linspace(all_vals.min(), all_vals.max(), 40)
        fig, ax = plt.subplots(figsize=(10, 5))
        if len(hi_ret): ax.hist(hi_ret, bins=bins, alpha=0.65, color="#2ecc71",
            label=f"고득점 61+ (n={len(hi_ret)}, avg={hi_ret.mean():.1f}%)")
        if len(lo_ret): ax.hist(lo_ret, bins=bins, alpha=0.65, color="#e74c3c",
            label=f"저득점 0-40 (n={len(lo_ret)}, avg={lo_ret.mean():.1f}%)")
        ax.axvline(0, color="black", lw=1.2, linestyle="--", alpha=0.7)
        ax.set_xlabel("20일 Forward Return (%)"); ax.set_ylabel("발생 횟수")
        ax.set_title(f"${ticker} — 고득점 vs 저득점 20일 수익률 분포")
        ax.legend(fontsize=10); plt.tight_layout()
        p = output_dir / f"{ticker}_distribution.png"
        plt.savefig(p, dpi=150, bbox_inches="tight"); plt.close(); files.append(p)
    return files


def generate_layer_charts(df, layer_corr, layer_buckets, combo_results, ticker, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    files = []
    layer_ids   = list(LAYERS.keys())
    layer_names = [LAYERS[l]["name"] for l in layer_ids]
    colors = ["#3498db","#2ecc71","#e67e22","#9b59b6","#e74c3c"]

    # ① 상관계수 히트맵
    corr_data = []
    for h in HORIZONS:
        corr_data.append([layer_corr[l]["horizons"].get(h, {}).get("spearman", np.nan)
                          if l in layer_corr else np.nan for l in layer_ids])
    corr_df = pd.DataFrame(corr_data,
                           index=[f"{h}d" for h in HORIZONS], columns=layer_names)
    fig, ax = plt.subplots(figsize=(7, 4))
    sns.heatmap(corr_df, annot=True, fmt=".3f", center=0, cmap="RdYlGn",
                vmin=-0.3, vmax=0.3, linewidths=0.5,
                cbar_kws={"label":"Spearman ρ"}, ax=ax,
                annot_kws={"size":12,"weight":"bold"})
    ax.set_title(f"${ticker} — 레이어별 순위상관계수 (Spearman ρ)")
    plt.tight_layout()
    p = output_dir / f"{ticker}_layer_corr.png"
    plt.savefig(p, dpi=150, bbox_inches="tight"); plt.close(); files.append(p)

    # ② 엣지 비교
    target_h = [h for h in [10, 20, 30] if h in HORIZONS]
    x = np.arange(len(layer_ids)); w = 0.25
    fig, ax = plt.subplots(figsize=(9, 5))
    for idx, h in enumerate(target_h):
        edges = []
        for lid in layer_ids:
            hi_avg = layer_buckets.get(lid, {}).get("HIGH", {}).get("horizons", {}).get(h)
            lo_avg = layer_buckets.get(lid, {}).get("LOW",  {}).get("horizons", {}).get(h)
            edges.append((hi_avg["avg"] - lo_avg["avg"]) * 100
                         if hi_avg and lo_avg else 0.0)
        offset = (idx - len(target_h)/2 + 0.5) * w
        bars = ax.bar(x + offset, edges, w, label=f"{h}d",
                      color=colors[idx], alpha=0.8)
        for bar, val in zip(bars, edges):
            if abs(val) > 0.3:
                ax.text(bar.get_x()+bar.get_width()/2,
                        bar.get_height()+(0.1 if val >= 0 else -0.4),
                        f"{val:+.1f}%", ha="center", fontsize=8, fontweight="bold")
    ax.axhline(0, color="black", lw=1, linestyle="--", alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels([f"{lid}\n{LAYERS[lid]['name']}" for lid in layer_ids])
    ax.set_ylabel("HIGH − LOW 평균수익률 (%)")
    ax.set_title(f"${ticker} — 레이어별 실질 엣지 (상위 50% − 하위 50%)")
    ax.legend(fontsize=10); plt.tight_layout()
    p = output_dir / f"{ticker}_layer_edge.png"
    plt.savefig(p, dpi=150, bbox_inches="tight"); plt.close(); files.append(p)

    # ③ 콤보 랭킹
    top = [c for c in combo_results if c["ev_10d"] > 0][:12]
    if top:
        labels = [c["combo"] for c in top]
        evs    = [c["ev_10d"]*100 for c in top]
        wrs    = [c["wr_10d"]*100 for c in top]
        counts = [c["count"] for c in top]
        fig, ax = plt.subplots(figsize=(11, 5))
        bar_colors = ["#2ecc71" if e >= 0 else "#e74c3c" for e in evs]
        bars = ax.barh(labels[::-1], evs[::-1], color=bar_colors[::-1], alpha=0.85)
        for bar, wr, cnt in zip(bars, wrs[::-1], counts[::-1]):
            ax.text(bar.get_width()+0.05, bar.get_y()+bar.get_height()/2,
                    f"WR {wr:.0f}%  n={cnt}", va="center", fontsize=8, color="#555")
        ax.axvline(0, color="black", lw=1, linestyle="--", alpha=0.5)
        ax.set_xlabel("EV 10d (%)")
        ax.set_title(f"${ticker} — 레이어 조합 EV 랭킹 (10d)")
        plt.tight_layout()
        p = output_dir / f"{ticker}_layer_combo.png"
        plt.savefig(p, dpi=150, bbox_inches="tight"); plt.close(); files.append(p)
    return files


def generate_subanalysis_chart(sub_results, ticker, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    colors_bar = ["#e74c3c","#e67e22","#f1c40f","#2ecc71","#27ae60"]
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    fig.suptitle(f"${ticker} — Layer A 세부 지표별 10d EV 분석",
                 fontsize=13, fontweight="bold")
    for ax, (name, data) in zip(axes, sub_results.items()):
        labels  = data["labels"]
        evs     = [data["buckets"][l]["ev"]*100 if data["buckets"].get(l) else 0 for l in labels]
        counts  = [data["buckets"][l]["count"]   if data["buckets"].get(l) else 0 for l in labels]
        wrs     = [data["buckets"][l]["winrate"]*100 if data["buckets"].get(l) else 0 for l in labels]
        bc = [colors_bar[min(i,len(colors_bar)-1)] if e >= 0 else "#95a5a6"
              for i, e in enumerate(evs)]
        bars = ax.bar(range(len(labels)), evs, color=bc, alpha=0.85)
        for bar, wr, cnt in zip(bars, wrs, counts):
            if cnt > 0:
                ax.text(bar.get_x()+bar.get_width()/2,
                        bar.get_height()+(0.05 if bar.get_height() >= 0 else -0.3),
                        f"WR{wr:.0f}%\nn={cnt}", ha="center", fontsize=7, color="#333")
        ax.axhline(0, color="black", lw=0.8, linestyle="--", alpha=0.5)
        h10 = data["horizons"].get(10)
        sp  = f"ρ={h10['spearman']:+.3f}" if h10 else ""
        ax.set_title(f"{name}  {sp}", fontsize=11, fontweight="bold")
        ax.set_ylabel("EV 10d (%)"); ax.set_xticks(range(len(labels)))
        ax.set_xticklabels([l.split("(")[0][:8] for l in labels],
                            fontsize=7, rotation=15, ha="right")
    plt.tight_layout()
    p = output_dir / f"{ticker}_subA.png"
    plt.savefig(p, dpi=150, bbox_inches="tight"); plt.close()
    return [p]


def generate_cross_chart(cross_results, ticker, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    cmf_order  = cross_results["cmf_order"]
    evsr_order = cross_results["evsr_order"]
    matrix     = cross_results["matrix"]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    fig.suptitle(f"${ticker} — CMF 상태별 성과 분석", fontsize=12, fontweight="bold")

    states = [s for s in cmf_order if matrix[s]["_total"]["count"] > 0]
    a10 = [matrix[s]["_total"]["avg_10d"]*100 if matrix[s]["_total"]["avg_10d"] else 0 for s in states]
    a20 = [matrix[s]["_total"]["avg_20d"]*100 if matrix[s]["_total"]["avg_20d"] else 0 for s in states]
    counts = [matrix[s]["_total"]["count"] for s in states]
    wrs    = [matrix[s]["_total"]["wr_10d"]*100 if matrix[s]["_total"]["wr_10d"] else 0 for s in states]
    x = range(len(states)); w = 0.35
    b1 = ax1.bar([i-w/2 for i in x], a10, w, label="10d", alpha=0.85,
                 color=["#2ecc71" if v >= 0 else "#e74c3c" for v in a10])
    ax1.bar([i+w/2 for i in x], a20, w, label="20d", alpha=0.6,
            color=["#27ae60" if v >= 0 else "#c0392b" for v in a20])
    for bar, wr, cnt in zip(b1, wrs, counts):
        ax1.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.1,
                 f"WR{wr:.0f}%\nn={cnt}", ha="center", fontsize=7)
    ax1.axhline(0, color="black", lw=0.8, linestyle="--", alpha=0.5)
    ax1.set_xticks(list(x)); ax1.set_xticklabels(states, fontsize=8)
    ax1.set_ylabel("평균 수익률 (%)"); ax1.set_title("CMF 상태별 10d/20d 평균수익률")
    ax1.legend(fontsize=9)

    heat = [[matrix[cs].get(es, {}).get("ev_10d", np.nan)*100 if matrix[cs].get(es) else np.nan
             for es in evsr_order] for cs in cmf_order]
    heat_df = pd.DataFrame(heat, index=cmf_order, columns=evsr_order)
    sns.heatmap(heat_df, annot=True, fmt=".1f", center=0, cmap="RdYlGn",
                linewidths=0.5, mask=heat_df.isna(), ax=ax2,
                cbar_kws={"label":"EV 10d (%)"},
                annot_kws={"size":9,"weight":"bold"})
    ax2.set_title("CMF × EvsR — EV 10d (%)")
    plt.tight_layout()
    p = output_dir / f"{ticker}_cross.png"
    plt.savefig(p, dpi=150, bbox_inches="tight"); plt.close()
    return [p]


def generate_dca_chart(dynamic, baseline, ticker, output_dir):
    if not dynamic or not baseline:
        return []
    output_dir.mkdir(parents=True, exist_ok=True)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle(f"${ticker} — Dynamic DCA vs Baseline DCA",
                 fontsize=13, fontweight="bold")

    ax1.plot(dynamic["portfolio_values"], color="#2ecc71", lw=2,
             label=f"Dynamic ({dynamic['total_return']*100:+.1f}%)")
    ax1.plot(baseline["portfolio_values"], color="#3498db", lw=2, linestyle="--",
             label=f"Baseline ({baseline['total_return']*100:+.1f}%)")
    ax1.plot(dynamic["invested_values"], color="#95a5a6", lw=1, linestyle=":",
             label="투입 원금")
    ax1.set_title("누적 포트폴리오 가치"); ax1.set_ylabel("USD ($)")
    ax1.legend(fontsize=9); ax1.tick_params(axis="x", rotation=30)

    metrics   = ["Total Return (%)", "MDD (%)", "Avg Cost ($)"]
    dyn_vals  = [dynamic["total_return"]*100, dynamic["mdd"]*100, dynamic["avg_cost"]]
    base_vals = [baseline["total_return"]*100, baseline["mdd"]*100, baseline["avg_cost"]]
    xi = np.arange(len(metrics)); w = 0.35
    ax2.bar(xi-w/2, dyn_vals,  w, label="Dynamic",  color="#2ecc71", alpha=0.85)
    ax2.bar(xi+w/2, base_vals, w, label="Baseline", color="#3498db", alpha=0.85)
    for i, (dv, bv) in enumerate(zip(dyn_vals, base_vals)):
        ax2.text(i-w/2, dv+(0.3 if dv >= 0 else -1.5), f"{dv:.1f}", ha="center", fontsize=8)
        ax2.text(i+w/2, bv+(0.3 if bv >= 0 else -1.5), f"{bv:.1f}", ha="center", fontsize=8)
    ax2.axhline(0, color="black", lw=0.8, linestyle="--", alpha=0.5)
    ax2.set_xticks(xi); ax2.set_xticklabels(metrics, fontsize=9)
    ax2.set_title("핵심 지표 비교"); ax2.legend(fontsize=9)
    plt.tight_layout()
    p = output_dir / f"{ticker}_dca.png"
    plt.savefig(p, dpi=150, bbox_inches="tight"); plt.close()
    log.info(f"DCA 차트: {p}")
    return [p]


# ═══════════════════════════════════════════════════════════════
# 8. Imgur 업로드
# ═══════════════════════════════════════════════════════════════
def upload_to_imgur(path: Path) -> str:
    if not IMGUR_CLIENT_ID:
        return ""
    try:
        with open(path, "rb") as f:
            data = f.read()
        r = requests.post("https://api.imgur.com/3/image",
                          headers={"Authorization": f"Client-ID {IMGUR_CLIENT_ID}"},
                          data={"image": data, "type": "file"}, timeout=30)
        if r.status_code == 200:
            url = r.json()["data"]["link"]
            log.info(f"Imgur: {path.name} → {url}")
            return url
    except Exception as e:
        log.warning(f"Imgur 실패 {path.name}: {e}")
    return ""


# ═══════════════════════════════════════════════════════════════
# 9. 콘솔 출력
# ═══════════════════════════════════════════════════════════════
def print_summary(ticker, bucket_results, sweet_spot, df):
    def p(v): return f"{v*100:+.2f}%" if v is not None else "N/A"
    def w(v): return f"{v*100:.1f}%" if v is not None else "N/A"
    print(f"\n{'='*62}")
    print(f"  ${ticker} V7 백테스트 | 샘플 {len(df)}일 | 평균 {df['score'].mean():.1f}점")
    print(f"{'='*62}")
    print(f"{'구간':<8} {'n':>5}  {'5d':>9}  {'10d':>9}  {'20d':>9}  {'WR':>7}  {'EV':>9}")
    print("-" * 62)
    for lb in BUCKET_LABELS:
        cnt = bucket_results[lb]["count"]
        vals = [p(bucket_results[lb]["horizons"].get(h, {}).get("avg") if
                  bucket_results[lb]["horizons"].get(h) else None) for h in HORIZONS[:3]]
        h10 = bucket_results[lb]["horizons"].get(10)
        wr = w(h10["winrate"]) if h10 else "N/A"
        ev = p(h10["ev"]) if h10 else "N/A"
        mark = " ⭐" if lb == sweet_spot.get("bucket") else ""
        print(f"{lb:<8} {cnt:>5}  {vals[0]:>9}  {vals[1]:>9}  {vals[2]:>9}  {wr:>7}  {ev:>9}{mark}")
    print("-" * 62)
    if sweet_spot:
        ss = sweet_spot
        print(f"\n🏆 Sweet Spot: {ss['bucket']}점 | 평균 {p(ss['avg'])} | 승률 {w(ss['winrate'])} | EV {p(ss['ev'])}")


def print_layer_summary(layer_corr, combo_results, sub_results=None):
    if sub_results:
        print(f"\n{'='*62}")
        print("  Layer A 세부 예측력 (Spearman ρ, 10d)")
        print(f"{'='*62}")
        for name, data in sub_results.items():
            h10 = data["horizons"].get(10)
            h30 = data["horizons"].get(30)
            sp  = f"{h10['spearman']:+.3f}" if h10 else " N/A "
            e10 = f"{h10['edge']*100:+.1f}%" if h10 else "N/A"
            e30 = f"{h30['edge']*100:+.1f}%" if h30 else "N/A"
            mark = " ✅" if (h10 and h10["spearman"] > 0.05) else ""
            print(f"  {name:<8} ρ={sp}  edge(10d)={e10}  edge(30d)={e30}{mark}")
    print(f"\n{'='*62}")
    print("  레이어별 Spearman ρ")
    print(f"{'='*62}")
    print(f"{'레이어':<16} " + "  ".join(f"{h}d" for h in HORIZONS))
    print("-" * 55)
    for lid, data in layer_corr.items():
        vals = [f"{data['horizons'].get(h, {}).get('spearman', 0):+.3f}"
                if data["horizons"].get(h) else " N/A " for h in HORIZONS]
        mark = " ✅" if (data["horizons"].get(10) and
                         data["horizons"][10]["spearman"] > 0.1) else ""
        print(f"  {lid}:{data['name']:<14} " + "  ".join(f"{v:>7}" for v in vals) + mark)
    print(f"\n{'='*55}")
    print("  조합 Top 5 (10d EV)")
    print(f"{'='*55}")
    for c in [x for x in combo_results if x["ev_10d"] > 0][:5]:
        print(f"  {c['combo']:<20} n={c['count']:>4}  avg={c['avg_10d']*100:>+6.1f}%  "
              f"WR={c['wr_10d']*100:>4.0f}%  EV={c['ev_10d']*100:>+6.2f}%")


def print_cross_summary(cross_results):
    print(f"\n{'='*55}")
    print("  CMF 상태별 성과")
    print(f"{'='*55}")
    print(f"{'상태':<8} {'n':>5}  {'10d avg':>9}  {'WR':>7}  {'20d avg':>9}")
    print("-" * 44)
    for cs in cross_results["cmf_order"]:
        d = cross_results["matrix"][cs]["_total"]
        if not d["count"]: continue
        a10 = f"{d['avg_10d']*100:+.2f}%" if d["avg_10d"] is not None else " N/A"
        a20 = f"{d['avg_20d']*100:+.2f}%" if d["avg_20d"] is not None else " N/A"
        wr  = f"{d['wr_10d']*100:.1f}%"   if d["wr_10d"]  is not None else " N/A"
        print(f"{cs:<8} {d['count']:>5}  {a10:>9}  {wr:>7}  {a20:>9}")


def print_dca_summary(dynamic, baseline):
    if not dynamic or not baseline:
        return
    print(f"\n{'='*60}")
    print("  Dynamic DCA vs Baseline DCA")
    print(f"{'='*60}")
    print(f"{'항목':<18} {'Baseline':>12}  {'Dynamic':>12}  {'개선':>10}")
    print("-" * 56)
    rows = [
        ("총 투입($)",   f"${baseline['total_input']:,.0f}", f"${dynamic['total_input']:,.0f}", ""),
        ("최종 가치($)", f"${baseline['final_value']:,.0f}", f"${dynamic['final_value']:,.0f}", ""),
        ("총 수익률",    f"{baseline['total_return']*100:+.1f}%", f"{dynamic['total_return']*100:+.1f}%",
                         f"{(dynamic['total_return']-baseline['total_return'])*100:+.1f}%p"),
        ("MDD",         f"{baseline['mdd']*100:.1f}%", f"{dynamic['mdd']*100:.1f}%",
                         f"{(dynamic['mdd']-baseline['mdd'])*100:+.1f}%p"),
        ("평균 단가($)", f"${baseline['avg_cost']:.2f}", f"${dynamic['avg_cost']:.2f}",
                         f"${dynamic['avg_cost']-baseline['avg_cost']:+.2f}"),
        ("보유 주식",    f"{baseline['shares']:.2f}주", f"{dynamic['shares']:.2f}주", ""),
        ("잔여 현금($)", "-", f"${dynamic['cash_pool_remaining']:,.0f}", ""),
    ]
    for name, bv, dv, imp in rows:
        print(f"{name:<18} {bv:>12}  {dv:>12}  {imp:>10}")
    print("-" * 56)


# ═══════════════════════════════════════════════════════════════
# 10. Slack
# ═══════════════════════════════════════════════════════════════
def _sec(t): return {"type":"section","text":{"type":"mrkdwn","text":t}}
def _div():  return {"type":"divider"}
def _ctx(t): return {"type":"context","elements":[{"type":"mrkdwn","text":t}]}


def build_slack_blocks(ticker, bucket_results, sweet_spot, df, years,
                        layer_corr=None, combo_results=None, sub_results=None,
                        cross_results=None, dca_dynamic=None, dca_baseline=None,
                        image_urls=None, layer_image_urls=None, dca_image_urls=None):
    today   = datetime.now(KST).strftime("%Y-%m-%d")
    n_total = len(df)
    blocks  = []

    def p(v):  return f"{v*100:+.1f}%" if v is not None else "N/A"
    def wr(v): return f"{v*100:.0f}%"  if v is not None else "N/A"

    blocks.append({"type":"header","text":{"type":"plain_text",
        "text":f"📊 ${ticker} V7 백테스트 — {today}"}})
    blocks.append(_ctx(
        f"분석 기간: {years}년 | 샘플: {n_total}일 | "
        f"평균 {df['score'].mean():.1f}점 | 이상치 상하위 {OUTLIER_CLIP*100:.0f}% 제거"
    ))
    blocks.append(_div())

    # 버킷 표
    lines = ["*🎯 점수 구간별 10일 Forward Return 통계*", "```",
             f"{'구간':<8} {'횟수':>5}  {'평균수익':>8}  {'중앙값':>8}  {'승률':>6}  {'EV':>8}",
             "─" * 52]
    for lb in BUCKET_LABELS:
        hd  = bucket_results[lb]["horizons"].get(10)
        cnt = bucket_results[lb]["count"]
        if not hd:
            lines.append(f"{lb:<8} {cnt:>5}  {'N/A':>8}  {'N/A':>8}  {'N/A':>6}  {'N/A':>8}")
        else:
            mark = " ⭐" if lb == sweet_spot.get("bucket") else ""
            lines.append(f"{lb:<8} {cnt:>5}  {p(hd['avg']):>8}  {p(hd['median']):>8}  "
                         f"{wr(hd['winrate']):>6}  {p(hd['ev']):>8}{mark}")
    lines.append("```")
    blocks.append(_sec("\n".join(lines)))
    blocks.append(_div())

    # 기간별 비교
    lines2 = ["*📈 기간별 평균 수익률*", "```",
              f"{'구간':<8} " + "  ".join(f"{h}d" for h in HORIZONS[:4]),
              "─" * 44]
    for lb in BUCKET_LABELS:
        vals = [p(bucket_results[lb]["horizons"].get(h, {}).get("avg")
                  if bucket_results[lb]["horizons"].get(h) else None) for h in HORIZONS[:4]]
        lines2.append(f"{lb:<8} " + "  ".join(f"{v:>7}" for v in vals))
    lines2.append("```")
    blocks.append(_sec("\n".join(lines2)))
    blocks.append(_div())

    # Sweet Spot
    if sweet_spot:
        ss = sweet_spot
        blocks.append(_sec(
            f"*🏆 Sweet Spot: {ss['bucket']}점*  |  샘플 {ss['count']}개  |  "
            f"평균 {p(ss['avg'])}  |  승률 {wr(ss['winrate'])}  |  EV {p(ss['ev'])}"
        ))

    # 점수 분포
    high_pct = (df["score"] >= 61).mean() * 100
    blocks.append(_sec(
        f"*📉 V7 점수 분포 (총 {n_total}일)*\n"
        f"평균 {df['score'].mean():.1f}점  |  중앙값 {df['score'].median():.0f}점  |  "
        f"61점↑ 발생: *{high_pct:.1f}%* ({int(high_pct*n_total/100)}일)"
    ))
    blocks.append(_div())

    # 레이어 분석
    if layer_corr:
        lines_lc = ["*🔬 레이어별 예측력 (Spearman ρ, 10d)*", "```",
                    f"{'레이어':<14} {'ρ(10d)':>8}  {'엣지(10d)':>10}  {'엣지(30d)':>10}",
                    "─" * 48]
        for lid, data in layer_corr.items():
            h10 = data["horizons"].get(10)
            h30 = data["horizons"].get(30)
            sp  = f"{h10['spearman']:+.3f}" if h10 else " N/A "
            e10 = f"{h10['edge']*100:+.1f}%" if h10 else "N/A"
            e30 = f"{h30['edge']*100:+.1f}%" if h30 else "N/A"
            mark = " ✅" if (h10 and h10["spearman"] > 0.1) else (" ⚠️" if (h10 and h10["spearman"] < 0) else "")
            lines_lc.append(f"{lid}:{data['name']:<12} {sp:>8}  {e10:>10}  {e30:>10}{mark}")
        lines_lc.append("```")
        blocks.append(_sec("\n".join(lines_lc)))

    if combo_results:
        top5 = [c for c in combo_results if c["ev_10d"] > 0][:5]
        if top5:
            lines_cb = ["*🧩 레이어 조합 Top 5 (10d EV)*", "```",
                        f"{'조합':<18} {'n':>5}  {'평균':>8}  {'승률':>6}  {'EV':>8}",
                        "─" * 52]
            for c in top5:
                lines_cb.append(f"{c['combo']:<18} {c['count']:>5}  "
                                 f"{c['avg_10d']*100:>+7.1f}%  {c['wr_10d']*100:>5.0f}%  "
                                 f"{c['ev_10d']*100:>+7.2f}%")
            lines_cb.append("```")
            blocks.append(_sec("\n".join(lines_cb)))
    blocks.append(_div())

    # Layer A 세부
    if sub_results:
        lines_sub = ["*🔬 Layer A 세부 (10d)*", "```",
                     f"{'지표':<8} {'ρ':>8}  {'엣지10d':>9}  최강구간",
                     "─" * 50]
        for name, data in sub_results.items():
            h10  = data["horizons"].get(10)
            sp   = f"{h10['spearman']:+.3f}" if h10 else " N/A "
            e10  = f"{h10['edge']*100:+.1f}%" if h10 else "N/A"
            best = max([(l,d) for l,d in data["buckets"].items() if d and d["count"]>=5],
                       key=lambda x: x[1]["ev"], default=(None,None))
            bst  = f"{best[0]} WR{best[1]['winrate']*100:.0f}% n={best[1]['count']}" if best[0] else ""
            mark = " ✅" if (h10 and h10["spearman"] > 0.05) else (" ⚠️" if (h10 and h10["spearman"] < 0) else "")
            lines_sub.append(f"{name:<8} {sp:>8}  {e10:>9}  {bst}{mark}")
        lines_sub.append("```")
        blocks.append(_sec("\n".join(lines_sub)))

    # CMF 상태
    if cross_results:
        matrix = cross_results["matrix"]
        lines_cr = ["*🔬 CMF 상태별 성과*", "```",
                    f"{'상태':<8} {'n':>5}  {'10d avg':>8}  {'WR':>6}  {'20d avg':>8}",
                    "─" * 44]
        for cs in cross_results["cmf_order"]:
            d = matrix[cs]["_total"]
            if not d["count"]: continue
            a10 = f"{d['avg_10d']*100:+.1f}%" if d["avg_10d"] is not None else "N/A"
            a20 = f"{d['avg_20d']*100:+.1f}%" if d["avg_20d"] is not None else "N/A"
            w_  = f"{d['wr_10d']*100:.0f}%"   if d["wr_10d"]  is not None else "N/A"
            lines_cr.append(f"{cs:<8} {d['count']:>5}  {a10:>8}  {w_:>6}  {a20:>8}")
        lines_cr.append("```")
        blocks.append(_sec("\n".join(lines_cr)))
    blocks.append(_div())

    # DCA 비교
    if dca_dynamic and dca_baseline:
        dyn = dca_dynamic; bas = dca_baseline
        ret_diff  = (dyn["total_return"] - bas["total_return"]) * 100
        mdd_diff  = (dyn["mdd"] - bas["mdd"]) * 100
        cost_diff = dyn["avg_cost"] - bas["avg_cost"]
        lines_dca = [
            "*💰 Dynamic DCA vs Baseline DCA*", "```",
            f"{'항목':<16} {'Baseline':>12}  {'Dynamic':>12}  {'개선':>9}",
            "─" * 54,
            f"{'총 수익률':<16} {bas['total_return']*100:>+11.1f}%  {dyn['total_return']*100:>+11.1f}%  {ret_diff:>+8.1f}%p",
            f"{'MDD':<16} {bas['mdd']*100:>11.1f}%  {dyn['mdd']*100:>11.1f}%  {mdd_diff:>+8.1f}%p",
            f"{'평균 단가($)':<16} ${bas['avg_cost']:>10.2f}  ${dyn['avg_cost']:>10.2f}  ${cost_diff:>+8.2f}",
            f"{'최종 가치($)':<16} ${bas['final_value']:>10,.0f}  ${dyn['final_value']:>10,.0f}",
            f"{'잔여 현금($)':<16} {'$0':>12}  ${dyn['cash_pool_remaining']:>10,.0f}",
            "```",
            "_Dynamic: 점수 기반 차등 매수 | Baseline: 매일 $100 무조건_"
        ]
        blocks.append(_sec("\n".join(lines_dca)))
        blocks.append(_div())

    # 이미지
    all_imgs = []
    if image_urls:
        all_imgs += [(url, "📊 히트맵") for url in image_urls if url]
    if layer_image_urls:
        labels = ["🔬 레이어 상관계수","📊 레이어 엣지","🧩 콤보 랭킹","🔬 Layer A 세부","🔬 CMF 교차"]
        all_imgs += [(url, lb) for url, lb in zip(layer_image_urls, labels) if url]
    if dca_image_urls:
        all_imgs += [(url, "💰 DCA 비교") for url in dca_image_urls if url]
    for url, label in all_imgs:
        blocks.append({"type":"section","text":{"type":"mrkdwn","text":f"*{label}*"}})
        blocks.append({"type":"image","image_url":url,"alt_text":label})

    blocks.append(_ctx(
        "V7 Scoring 백테스트 | 과거 통계 기반 참고용 | "
        "전체 차트는 outputs/ 또는 Imgur 링크"
    ))
    return blocks


def send_slack(blocks):
    if not SLACK_WEBHOOK:
        log.warning("SLACK_WEBHOOK 없음")
        return
    for i in range(0, len(blocks), 40):
        try:
            r = requests.post(SLACK_WEBHOOK,
                              json={"blocks": blocks[i:i+40], "text": "V7 백테스트"},
                              timeout=15)
            log.info(f"Slack: {r.status_code}")
        except Exception as e:
            log.error(f"Slack 실패: {e}")


# ═══════════════════════════════════════════════════════════════
# 11. 메인
# ═══════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="$HOOD V7 Backtester")
    parser.add_argument("--ticker",   type=str, default="HOOD")
    parser.add_argument("--years",    type=int, default=2)
    parser.add_argument("--no-slack", action="store_true")
    parser.add_argument("--output",   type=str, default="outputs")
    args   = parser.parse_args()
    ticker = args.ticker.upper()

    log.info(f"=== V7 백테스트: ${ticker} ({args.years}년) ===")
    start = time.time()

    # ① 데이터 다운로드
    raw_df   = download_data(ticker, args.years)
    macro_df = download_macro_history(args.years)

    # ② 롤링 스코어 (Lookahead-free + Macro 적용)
    scored = compute_rolling_scores(raw_df, ticker, args.years, macro_df)

    # ③ Forward Return + 이상치 제거
    scored = label_forward_returns(scored, raw_df)
    if len(scored) < 20:
        log.error(f"유효 샘플 부족 ({len(scored)}개)")
        sys.exit(1)

    # ④ 버킷 분석
    bucket_results = bucket_analysis(scored)
    sweet_spot     = find_sweet_spot(bucket_results)

    # ⑤ 레이어 분석
    log.info("레이어 분석 중...")
    layer_corr    = layer_correlation_analysis(scored)
    layer_buckets = layer_bucket_analysis(scored)
    combo_results = layer_combo_analysis(scored)
    sub_results   = layer_A_subanalysis(scored)
    cross_results = cmf_evsr_cross_analysis(scored)

    # ⑥ DCA 시뮬레이션
    log.info("DCA 시뮬레이션 중...")
    dca_dynamic  = run_dca_simulation(scored, raw_df["Close"])
    dca_baseline = run_baseline_dca(scored, raw_df["Close"])

    # ⑦ 콘솔 출력
    print_summary(ticker, bucket_results, sweet_spot, scored)
    print_layer_summary(layer_corr, combo_results, sub_results)
    print_cross_summary(cross_results)
    print_dca_summary(dca_dynamic, dca_baseline)

    # ⑧ 차트 생성
    output_dir = Path(args.output)
    chart_files       = generate_charts(scored, bucket_results, ticker, output_dir)
    layer_chart_files = generate_layer_charts(scored, layer_corr, layer_buckets,
                                               combo_results, ticker, output_dir)
    sub_chart_files   = generate_subanalysis_chart(sub_results, ticker, output_dir)
    cross_chart_files = generate_cross_chart(cross_results, ticker, output_dir)
    dca_chart_files   = generate_dca_chart(dca_dynamic, dca_baseline, ticker, output_dir)
    all_files = chart_files + layer_chart_files + sub_chart_files + cross_chart_files + dca_chart_files
    log.info(f"차트 {len(all_files)}개 저장 완료")

    # ⑨ Slack 전송
    if not args.no_slack:
        image_urls       = [upload_to_imgur(f) for f in chart_files]
        layer_image_urls = [upload_to_imgur(f) for f in layer_chart_files + sub_chart_files + cross_chart_files]
        dca_image_urls   = [upload_to_imgur(f) for f in dca_chart_files]
        blocks = build_slack_blocks(
            ticker, bucket_results, sweet_spot, scored, args.years,
            layer_corr=layer_corr, combo_results=combo_results,
            sub_results=sub_results, cross_results=cross_results,
            dca_dynamic=dca_dynamic, dca_baseline=dca_baseline,
            image_urls=image_urls, layer_image_urls=layer_image_urls,
            dca_image_urls=dca_image_urls,
        )
        send_slack(blocks)

    elapsed = time.time() - start
    log.info(f"=== 완료: {elapsed:.1f}초 ===")

    # ⑩ JSON 저장
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "ticker": ticker, "years": args.years, "n_samples": len(scored),
        "score_mean": round(scored["score"].mean(), 2),
        "sweet_spot": sweet_spot,
        "dca_dynamic":  {k: v for k, v in dca_dynamic.items()
                         if not isinstance(v, (pd.Series, list))},
        "dca_baseline": {k: v for k, v in dca_baseline.items()
                         if not isinstance(v, (pd.Series, list))},
    }
    with open(output_dir / f"{ticker}_backtest.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    log.info(f"JSON 저장: {output_dir}/{ticker}_backtest.json")


if __name__ == "__main__":
    main()
