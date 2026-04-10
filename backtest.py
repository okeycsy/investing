#!/usr/bin/env python3
"""
V3 Score-Forward Return Backtester
====================================
목적: V3 기술지표 스코어와 미래 수익률 간의 통계적 상관관계 검증
방법: Score-Forward Return Bucketing (점수 구간별 미래 성과 분석)

사용법:
  python backtest.py              # 기본 ($HOOD, 2년)
  python backtest.py --ticker NVDA --years 3
  python backtest.py --no-slack   # Slack 전송 없이 로컬만
"""

import os
import sys
import json
import time
import logging
import argparse
import requests
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # 헤드리스 환경
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── market_scan.py 임포트 (같은 디렉토리에 있어야 함) ────────────
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
SLACK_WEBHOOK    = os.environ.get("MARKET_SCAN_WEBHOOK") or os.environ.get("SLACK_WEBHOOK_URL", "")
IMGUR_CLIENT_ID  = os.environ.get("IMGUR_CLIENT_ID", "")   # imgur.com/oauth2/addclient 에서 발급
KST              = timezone(timedelta(hours=9))
OUTLIER_CLIP   = 0.01   # 상하위 1% 이상치 제거
WARMUP_DAYS    = 130    # 지표 워밍업 (EMA50·MACD 안정화에 필요한 최소 일수)
BUCKETS        = [(0,10),(11,20),(21,30),(31,40),(41,50),(51,60),(61,75),(76,100)]
BUCKET_LABELS  = ["0-10","11-20","21-30","31-40","41-50","51-60","61-75","76-100"]
HORIZONS       = [5, 10, 20, 30, 60]   # 확장: 30d·60d 추가

# 레이어별 메타 (독립 예측력 분석용)
# v5: CMF(50) + EvsR(30). UpVol/B 제거.
LAYERS = {
    "A": {"name": "CMF",   "max": 50},
    "B": {"name": "EvsR",  "max": 30},
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("backtest")


# ─────────────────────────────────────────────
# 1. 데이터 다운로드
# ─────────────────────────────────────────────
def download_data(ticker: str, years: int = 2) -> pd.DataFrame:
    """
    (years + 워밍업 여유) 기간 일봉 다운로드.
    forward return 계산을 위해 +30 거래일 추가 확보.
    """
    extra_days = WARMUP_DAYS + 30   # 워밍업 + 미래 수익률 여유
    total_days = years * 365 + extra_days
    period_str = f"{total_days}d"

    log.info(f"${ticker} 다운로드 중 ({years}년 + 워밍업 {WARMUP_DAYS}일)...")
    df = yf.download(ticker, period=period_str, interval="1d",
                     auto_adjust=True, progress=False)
    if df.empty:
        raise ValueError(f"${ticker} 데이터 없음")

    # MultiIndex 플래튼 (yfinance 버전 대응)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df[["Open","High","Low","Close","Volume"]].dropna()
    log.info(f"  다운로드 완료: {len(df)}일 ({df.index[0].date()} ~ {df.index[-1].date()})")
    return df


# ─────────────────────────────────────────────
# 2. 전체 기간 롤링 스코어링 (Lookahead-free)
# ─────────────────────────────────────────────
def compute_rolling_scores(df: pd.DataFrame, ticker: str,
                            years: int = 2) -> pd.DataFrame:
    """
    각 날짜 T마다 T 이전 데이터만 사용해 V3 점수를 계산.
    Lookahead bias 완전 차단.
    """
    closes  = df["Close"].tolist()
    highs   = df["High"].tolist()
    lows    = df["Low"].tolist()
    volumes = df["Volume"].astype(int).tolist()
    dates   = df.index.tolist()
    n       = len(dates)

    # 분석 대상 시작점: 전체에서 뒤에서 years*252 거래일
    target_start = max(WARMUP_DAYS, n - years * 252)

    log.info(f"롤링 스코어 계산: {dates[target_start].date()} ~ {dates[-1].date()} "
             f"({n - target_start}일 대상)")

    records = []
    for i in range(target_start, n):
        # T 이전 데이터만 슬라이싱 (인덱스 i+1 미포함)
        ohlcv = {
            "closes":  closes[:i+1],
            "highs":   highs[:i+1],
            "lows":    lows[:i+1],
            "volumes": volumes[:i+1],
        }
        sector = SP500.get(ticker, "Unknown")
        ts = score_ticker(ticker, sector, ohlcv)

        records.append({
            "date":  dates[i],
            "close": closes[i],
            "score": ts.score if not ts.error else None,
            "raw":   ts.raw   if not ts.error else None,
            "cmf":   ts.cmf,
            "evsr":  ts.evsr,
            "upvol": ts.upvol,
            "adx":   ts.adx,
            "rsi":   ts.rsi,
            "squeeze": ts.squeeze,
            "layer_A": ts.layers.get("A", 0),
            "layer_B": ts.layers.get("B", 0),
            "layer_C": ts.layers.get("C", 0),
            "layer_D": ts.layers.get("D", 0),
        })

        if (i - target_start) % 50 == 0:
            log.info(f"  {i - target_start}/{n - target_start}일 처리 중...")

    result = pd.DataFrame(records).set_index("date")
    result = result.dropna(subset=["score"])
    log.info(f"스코어 계산 완료: {len(result)}개 유효 날짜")
    return result


# ─────────────────────────────────────────────
# 3. Forward Return 라벨링
# ─────────────────────────────────────────────
def label_forward_returns(scored: pd.DataFrame,
                          raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    각 날짜 T에 대해 T+5, T+10, T+20 수익률 계산.
    이상치(상하위 1%) 클리핑 후 별도 컬럼으로 보관.
    """
    closes = raw_df["Close"]

    for h in HORIZONS:
        col = f"return_{h}d"
        # 각 날짜에서 h거래일 후 종가로 수익률 계산
        future_close = closes.shift(-h)
        pct = (future_close - closes) / closes
        scored[col] = pct.reindex(scored.index)

    # 미래 데이터가 없는 끝부분 제거
    scored = scored.dropna(subset=[f"return_{h}d" for h in HORIZONS])

    # 이상치 제거 (상하위 1%) — 각 horizon별 독립 적용
    scored_clean = scored.copy()
    for h in HORIZONS:
        col = f"return_{h}d"
        lo  = scored[col].quantile(OUTLIER_CLIP)
        hi  = scored[col].quantile(1 - OUTLIER_CLIP)
        outlier_mask = (scored[col] < lo) | (scored[col] > hi)
        n_outliers = outlier_mask.sum()
        if n_outliers:
            log.info(f"  {col} 이상치 제거: {n_outliers}개 "
                     f"(< {lo:.1%} or > {hi:.1%})")
        scored_clean.loc[outlier_mask, col] = np.nan

    log.info(f"Forward Return 라벨링 완료: {len(scored_clean)}개 날짜")
    return scored_clean


# ─────────────────────────────────────────────
# 4. 버킷팅 & 통계 집계
# ─────────────────────────────────────────────
def bucket_analysis(df: pd.DataFrame) -> dict:
    """
    점수 구간별 수익률 통계 집계.
    반환: {bucket_label: {horizon: {count, avg, median, winrate, ev, std}}}
    """
    def assign_bucket(score):
        for (lo, hi), label in zip(BUCKETS, BUCKET_LABELS):
            if lo <= score <= hi:
                return label
        return None

    df = df.copy()
    df["bucket"] = df["score"].apply(assign_bucket)

    results = {}
    for label in BUCKET_LABELS:
        sub = df[df["bucket"] == label]
        results[label] = {"count": len(sub), "horizons": {}}
        for h in HORIZONS:
            col = f"return_{h}d"
            data = sub[col].dropna()
            if len(data) < 3:
                results[label]["horizons"][h] = None
                continue
            avg     = data.mean()
            median  = data.median()
            winrate = (data > 0).mean()
            ev      = avg * winrate   # Expected Value
            std     = data.std()
            sharpe  = avg / std if std > 0 else 0   # 단순 Sharpe (비연율화)
            results[label]["horizons"][h] = {
                "count":   len(data),
                "avg":     avg,
                "median":  median,
                "winrate": winrate,
                "ev":      ev,
                "std":     std,
                "sharpe":  sharpe,
            }
    return results


# ─────────────────────────────────────────────
# 5. Sweet Spot 탐지
# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
# 5-A. 레이어별 독립 예측력 분석
# ─────────────────────────────────────────────
def layer_A_subanalysis(df: pd.DataFrame) -> dict:
    """
    Layer A 세부 지표 독립 예측력 분석.
    CMF / EvsR / UpVol 각각의 Spearman ρ, 엣지, 버킷 성과를 분리해서 측정.
    반환: {indicator: {horizon: {corr, spearman, edge, bucket_stats}}}
    """
    indicators = {
        "CMF":   ("cmf",   [-0.15, -0.05, 0.05, 0.15],
                            ["강매도(<-0.15)", "매도압력", "중립", "매수압력", "강매수(>0.15)"]),
        "EvsR":  ("evsr",  [1.0, 1.5, 2.0, 2.5],
                            ["흡수없음(<1)", "약흡수", "중흡수", "강흡수", "최강흡수(>2.5)"]),
        "UpVol": ("upvol", [0.75, 1.0, 1.5, 2.0],
                            ["강하락거래량", "하락거래량우세", "중립", "상승거래량우세", "강상승거래량(>2)"]),
    }
    results = {}
    for name, (col, thresholds, labels) in indicators.items():
        if col not in df.columns:
            continue
        ind_results = {"col": col, "labels": labels, "horizons": {}, "buckets": {}}

        # 버킷 분류
        def assign_bucket(val):
            for idx, t in enumerate(thresholds):
                if val <= t:
                    return labels[idx]
            return labels[-1]
        df_copy = df.copy()
        df_copy["_bucket"] = df_copy[col].apply(assign_bucket)

        for h in HORIZONS:
            ret_col = f"return_{h}d"
            sub = df[[col, ret_col]].dropna()
            if len(sub) < 10:
                ind_results["horizons"][h] = None
                continue
            spearman = sub[col].rank().corr(sub[ret_col].rank())
            median   = sub[col].median()
            top_ret  = sub[sub[col] >= median][ret_col].mean()
            bot_ret  = sub[sub[col] <  median][ret_col].mean()
            ind_results["horizons"][h] = {
                "spearman": round(spearman, 4),
                "edge":     round(top_ret - bot_ret, 4),
            }

        # 버킷별 10d 성과
        for label in labels:
            bucket_sub = df_copy[df_copy["_bucket"] == label]["return_10d"].dropna()
            if len(bucket_sub) < 3:
                ind_results["buckets"][label] = None
                continue
            ind_results["buckets"][label] = {
                "count":   len(bucket_sub),
                "avg":     round(bucket_sub.mean(), 4),
                "winrate": round((bucket_sub > 0).mean(), 4),
                "ev":      round(bucket_sub.mean() * (bucket_sub > 0).mean(), 4),
            }
        results[name] = ind_results
    return results


def layer_correlation_analysis(df: pd.DataFrame) -> dict:
    """
    각 레이어(A/B/C/D)와 미래 수익률의 상관관계 분석.
    반환:
      corr     : Pearson 상관계수 (선형 관계)
      spearman : Spearman 순위 상관계수 (비선형 포함)
      top_avg  : 레이어 상위 50% 날짜의 평균 수익률
      bot_avg  : 레이어 하위 50% 날짜의 평균 수익률
      edge     : top_avg - bot_avg (실질 엣지)
    """
    results = {}
    for lid, meta in LAYERS.items():
        col = f"layer_{lid}"
        if col not in df.columns:
            continue
        layer_results = {}
        for h in HORIZONS:
            ret_col = f"return_{h}d"
            sub = df[[col, ret_col]].dropna()
            if len(sub) < 10:
                layer_results[h] = None
                continue
            corr     = sub[col].corr(sub[ret_col])
            spearman = sub[col].rank().corr(sub[ret_col].rank())
            median_score = sub[col].median()
            top_ret = sub[sub[col] >= median_score][ret_col].mean()
            bot_ret = sub[sub[col] <  median_score][ret_col].mean()
            layer_results[h] = {
                "corr":     round(corr, 4),
                "spearman": round(spearman, 4),
                "top_avg":  round(top_ret, 4),
                "bot_avg":  round(bot_ret, 4),
                "edge":     round(top_ret - bot_ret, 4),
            }
        results[lid] = {"name": meta["name"], "max": meta["max"],
                        "horizons": layer_results}
    return results


def layer_bucket_analysis(df: pd.DataFrame) -> dict:
    """
    레이어별로 점수 구간(낮음/중간/높음)을 3분위수로 나눠 Forward Return 분석.
    점수 범위가 레이어마다 다르므로 동적으로 분위수 사용.
    반환: {layer_id: {tier: {horizon: stats}}}
    """
    results = {}
    tiers = {"LOW": (0, 0.33), "MID": (0.33, 0.67), "HIGH": (0.67, 1.0)}

    for lid in LAYERS:
        col = f"layer_{lid}"
        if col not in df.columns:
            continue
        tier_results = {}
        for tier_name, (lo_q, hi_q) in tiers.items():
            lo = df[col].quantile(lo_q)
            hi = df[col].quantile(hi_q)
            if tier_name == "LOW":
                mask = df[col] <= lo
            elif tier_name == "HIGH":
                mask = df[col] > hi
            else:
                mask = (df[col] > lo) & (df[col] <= hi)
            sub = df[mask]
            h_results = {}
            for h in HORIZONS:
                ret_col = f"return_{h}d"
                data = sub[ret_col].dropna()
                if len(data) < 3:
                    h_results[h] = None
                    continue
                h_results[h] = {
                    "count":   len(data),
                    "avg":     round(data.mean(), 4),
                    "winrate": round((data > 0).mean(), 4),
                    "ev":      round(data.mean() * (data > 0).mean(), 4),
                }
            tier_results[tier_name] = {"count": mask.sum(), "horizons": h_results}
        results[lid] = tier_results
    return results


def layer_combo_analysis(df: pd.DataFrame) -> list:
    """
    레이어 조합 분석: LAYERS dict 기준으로 동적 생성.
    각 레이어를 중앙값 기준 High/Low 이진화 → 2-레이어 조합 전수 탐색.
    반환: 10d EV 기준 내림차순 정렬 리스트
    """
    df = df.copy()
    layer_ids = [lid for lid in LAYERS if f"layer_{lid}" in df.columns]
    for lid in layer_ids:
        median = df[f"layer_{lid}"].median()
        df[f"{lid}_hi"] = (df[f"layer_{lid}"] >= median).astype(int)

    combos = []
    for i, l1 in enumerate(layer_ids):
        for l2 in layer_ids[i+1:]:
            for v1, v2 in [(1,1), (1,0), (0,1)]:
                mask = (df[f"{l1}_hi"] == v1) & (df[f"{l2}_hi"] == v2)
                ret  = df[mask]["return_10d"].dropna()
                if len(ret) < 5:
                    continue
                label = f"{l1}{'↑' if v1 else '↓'} & {l2}{'↑' if v2 else '↓'}"
                combos.append({
                    "combo":   label,
                    "count":   len(ret),
                    "avg_10d": round(ret.mean(), 4),
                    "wr_10d":  round((ret > 0).mean(), 4),
                    "ev_10d":  round(ret.mean() * (ret > 0).mean(), 4),
                })

    return sorted(combos, key=lambda x: -x["ev_10d"])


def find_sweet_spot(bucket_results: dict, horizon: int = 10) -> dict:
    """
    EV(기대값) 기준 최적 점수 구간 탐지.
    조건: sample ≥ 5 && winrate ≥ 55% && avg > 0
    """
    candidates = []
    for label, data in bucket_results.items():
        h_data = data["horizons"].get(horizon)
        if not h_data or h_data["count"] < 5:
            continue
        if h_data["winrate"] >= 0.50 and h_data["avg"] > 0:
            candidates.append({
                "bucket":  label,
                "count":   h_data["count"],
                "avg":     h_data["avg"],
                "winrate": h_data["winrate"],
                "ev":      h_data["ev"],
                "sharpe":  h_data["sharpe"],
            })
    if not candidates:
        return {}
    return max(candidates, key=lambda x: x["ev"])


# ─────────────────────────────────────────────
# 6. 시각화 — Heatmap + Distribution
# ─────────────────────────────────────────────
def generate_charts(df: pd.DataFrame, bucket_results: dict,
                    ticker: str, output_dir: Path) -> list:
    """
    ① Heatmap: X=점수구간 / Y=미래기간 / 색상=평균수익률
    ② Distribution: 고득점(61+) vs 저득점(0-40) 20일 수익률 분포 비교
    생성된 파일 경로 리스트 반환
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    files = []

    # ── ① Heatmap ─────────────────────────────
    heat_data = []
    for h in HORIZONS:
        row = []
        for label in BUCKET_LABELS:
            hd = bucket_results[label]["horizons"].get(h)
            row.append(hd["avg"] * 100 if hd else np.nan)  # % 단위
        heat_data.append(row)

    heat_df = pd.DataFrame(heat_data,
                           index=[f"{h}d" for h in HORIZONS],
                           columns=BUCKET_LABELS)

    fig, ax = plt.subplots(figsize=(10, 4))
    sns.heatmap(heat_df, annot=True, fmt=".1f", center=0,
                cmap="RdYlGn", linewidths=0.5,
                cbar_kws={"label": "Avg Return (%)"},
                ax=ax, annot_kws={"size": 11, "weight": "bold"})
    ax.set_title(f"${ticker} — V3 Score vs Forward Return Heatmap\n"
                 f"(이상치 상하위 {OUTLIER_CLIP*100:.0f}% 제거, 값: 평균수익률 %)",
                 fontsize=12, pad=12)
    ax.set_xlabel("V3 Score 구간", fontsize=11)
    ax.set_ylabel("Forward Return 기간", fontsize=11)
    plt.tight_layout()
    heatmap_path = output_dir / f"{ticker}_heatmap.png"
    plt.savefig(heatmap_path, dpi=150, bbox_inches="tight")
    plt.close()
    files.append(heatmap_path)
    log.info(f"히트맵 저장: {heatmap_path}")

    # ── ② Distribution Plot ───────────────────
    high_score = df[df["score"] >= 61]["return_20d"].dropna() * 100
    low_score  = df[df["score"] <= 40]["return_20d"].dropna() * 100

    fig, ax = plt.subplots(figsize=(10, 5))
    bins = np.linspace(
        min(high_score.min() if len(high_score) else -20,
            low_score.min()  if len(low_score)  else -20),
        max(high_score.max() if len(high_score) else 20,
            low_score.max()  if len(low_score)  else 20),
        40
    )
    if len(high_score):
        ax.hist(high_score, bins=bins, alpha=0.65, color="#2ecc71",
                label=f"고득점 61+ (n={len(high_score)}, avg={high_score.mean():.1f}%)",
                edgecolor="white", linewidth=0.5)
    if len(low_score):
        ax.hist(low_score, bins=bins, alpha=0.65, color="#e74c3c",
                label=f"저득점 0-40 (n={len(low_score)}, avg={low_score.mean():.1f}%)",
                edgecolor="white", linewidth=0.5)
    ax.axvline(0, color="black", linewidth=1.2, linestyle="--", alpha=0.7)
    ax.set_xlabel("20일 Forward Return (%)", fontsize=11)
    ax.set_ylabel("발생 횟수", fontsize=11)
    ax.set_title(f"${ticker} — 고득점 vs 저득점 20일 수익률 분포\n"
                 f"(이상치 상하위 {OUTLIER_CLIP*100:.0f}% 제거)",
                 fontsize=12, pad=12)
    ax.legend(fontsize=10)
    plt.tight_layout()
    dist_path = output_dir / f"{ticker}_distribution.png"
    plt.savefig(dist_path, dpi=150, bbox_inches="tight")
    plt.close()
    files.append(dist_path)
    log.info(f"분포 차트 저장: {dist_path}")

    return files


def generate_layer_charts(df: pd.DataFrame, layer_corr: dict,
                           layer_buckets: dict, combo_results: list,
                           ticker: str, output_dir: Path) -> list:
    """
    레이어 분석 전용 차트 3종:
    ① 레이어별 Spearman 상관계수 (horizon별 막대)
    ② 레이어별 HIGH vs LOW 수익률 비교 (엣지 시각화)
    ③ 상위 콤보 EV 랭킹
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    files = []
    layer_ids  = list(LAYERS.keys())
    layer_names = [LAYERS[l]["name"] for l in layer_ids]
    horizon_labels = [f"{h}d" for h in HORIZONS]
    colors = ["#3498db","#2ecc71","#e67e22","#9b59b6","#e74c3c"]

    # ── ① 레이어별 Spearman 상관계수 히트맵 ──────────────
    corr_data = []
    for h in HORIZONS:
        row = []
        for lid in layer_ids:
            hd = layer_corr[lid]["horizons"].get(h)
            row.append(hd["spearman"] if hd else np.nan)
        corr_data.append(row)

    corr_df = pd.DataFrame(corr_data,
                           index=horizon_labels,
                           columns=layer_names)

    fig, ax = plt.subplots(figsize=(9, 4))
    sns.heatmap(corr_df, annot=True, fmt=".3f", center=0,
                cmap="RdYlGn", linewidths=0.5,
                vmin=-0.3, vmax=0.3,
                cbar_kws={"label": "Spearman ρ"},
                ax=ax, annot_kws={"size": 12, "weight": "bold"})
    ax.set_title(f"${ticker} — 레이어별 순위상관계수 (Spearman ρ)\n양수=예측력 있음 / 음수=역방향 / 0=무관",
                 fontsize=12, pad=10)
    ax.set_xlabel("레이어", fontsize=11)
    ax.set_ylabel("Forward Return 기간", fontsize=11)
    plt.tight_layout()
    p = output_dir / f"{ticker}_layer_corr.png"
    plt.savefig(p, dpi=150, bbox_inches="tight")
    plt.close()
    files.append(p)
    log.info(f"레이어 상관계수 차트: {p}")

    # ── ② 레이어 HIGH vs LOW 엣지 (10d/20d/30d) ─────────
    target_horizons = [h for h in [10, 20, 30] if h in HORIZONS]
    x = np.arange(len(layer_ids))
    width = 0.25
    fig, ax = plt.subplots(figsize=(10, 5))

    for idx, h in enumerate(target_horizons):
        edges = []
        for lid in layer_ids:
            hi_avg = layer_buckets[lid]["HIGH"]["horizons"].get(h)
            lo_avg = layer_buckets[lid]["LOW"]["horizons"].get(h)
            if hi_avg and lo_avg:
                edge = (hi_avg["avg"] - lo_avg["avg"]) * 100
            else:
                edge = 0.0
            edges.append(edge)
        offset = (idx - len(target_horizons)/2 + 0.5) * width
        bars = ax.bar(x + offset, edges, width,
                      label=f"{h}d", color=colors[idx], alpha=0.8,
                      edgecolor="white", linewidth=0.5)
        for bar, val in zip(bars, edges):
            if abs(val) > 0.3:
                ax.text(bar.get_x() + bar.get_width()/2,
                        bar.get_height() + (0.1 if val >= 0 else -0.4),
                        f"{val:+.1f}%", ha="center", va="bottom",
                        fontsize=8, fontweight="bold")

    ax.axhline(0, color="black", linewidth=1, linestyle="--", alpha=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels([f"{lid}\n{LAYERS[lid]['name']}" for lid in layer_ids], fontsize=10)
    ax.set_ylabel("HIGH − LOW 평균수익률 (%)", fontsize=11)
    ax.set_title(f"${ticker} — 레이어별 실질 엣지 (상위 50% − 하위 50%)\n양수 = 해당 레이어가 높을수록 수익률 ↑ (예측력 있음)",
                 fontsize=12, pad=10)
    ax.legend(fontsize=10)
    plt.tight_layout()
    p = output_dir / f"{ticker}_layer_edge.png"
    plt.savefig(p, dpi=150, bbox_inches="tight")
    plt.close()
    files.append(p)
    log.info(f"레이어 엣지 차트: {p}")

    # ── ③ 상위 콤보 EV 랭킹 ──────────────────────────────
    top_combos = [c for c in combo_results if c["ev_10d"] > 0][:12]
    if top_combos:
        labels = [c["combo"] for c in top_combos]
        evs    = [c["ev_10d"] * 100 for c in top_combos]
        wrs    = [c["wr_10d"] * 100 for c in top_combos]
        counts = [c["count"] for c in top_combos]

        fig, ax = plt.subplots(figsize=(11, 5))
        bar_colors = ["#2ecc71" if e > 0 else "#e74c3c" for e in evs]
        bars = ax.barh(labels[::-1], evs[::-1], color=bar_colors[::-1],
                       alpha=0.85, edgecolor="white")
        for bar, wr, cnt in zip(bars, wrs[::-1], counts[::-1]):
            ax.text(bar.get_width() + 0.05,
                    bar.get_y() + bar.get_height()/2,
                    f"WR {wr:.0f}%  n={cnt}",
                    va="center", fontsize=8, color="#555")
        ax.axvline(0, color="black", linewidth=1, linestyle="--", alpha=0.5)
        ax.set_xlabel("Expected Value 10d (%)", fontsize=11)
        ax.set_title(f"${ticker} — 레이어 조합별 EV 랭킹 (10d)\n↑↓ = 해당 레이어가 중앙값 이상/미만",
                     fontsize=12, pad=10)
        plt.tight_layout()
        p = output_dir / f"{ticker}_layer_combo.png"
        plt.savefig(p, dpi=150, bbox_inches="tight")
        plt.close()
        files.append(p)
        log.info(f"콤보 차트: {p}")

    return files


def generate_subanalysis_chart(sub_results: dict, ticker: str, output_dir: Path) -> list:
    """
    Layer A 세부 지표별 버킷 성과 차트 (CMF / EvsR / UpVol 각각).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    files = []
    colors_pos = ["#e74c3c","#e67e22","#f1c40f","#2ecc71","#27ae60"]

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    fig.suptitle(f"${ticker} — Layer A 세부 지표별 10d EV 분석", fontsize=13, fontweight="bold")

    for ax, (name, data) in zip(axes, sub_results.items()):
        labels   = data["labels"]
        buckets  = data["buckets"]
        evs      = [buckets[l]["ev"]*100 if buckets.get(l) else 0 for l in labels]
        counts   = [buckets[l]["count"]  if buckets.get(l) else 0 for l in labels]
        wrs      = [buckets[l]["winrate"]*100 if buckets.get(l) else 0 for l in labels]
        bar_colors = [colors_pos[min(i, len(colors_pos)-1)] if e >= 0
                      else "#95a5a6" for i, e in enumerate(evs)]
        bars = ax.bar(range(len(labels)), evs, color=bar_colors, alpha=0.85,
                      edgecolor="white", linewidth=0.5)
        for bar, wr, cnt in zip(bars, wrs, counts):
            if cnt > 0:
                ax.text(bar.get_x() + bar.get_width()/2,
                        bar.get_height() + (0.05 if bar.get_height() >= 0 else -0.3),
                        f"WR{wr:.0f}%\nn={cnt}", ha="center", fontsize=7, color="#333")
        ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
        h_data = data["horizons"].get(10)
        sp = f"ρ={h_data['spearman']:+.3f}" if h_data else ""
        ax.set_title(f"{name}  {sp}", fontsize=11, fontweight="bold")
        ax.set_ylabel("EV 10d (%)", fontsize=9)
        ax.set_xticks(range(len(labels)))
        short = [l.split("(")[0][:8] for l in labels]
        ax.set_xticklabels(short, fontsize=7, rotation=15, ha="right")

    plt.tight_layout()
    p = output_dir / f"{ticker}_subA.png"
    plt.savefig(p, dpi=150, bbox_inches="tight")
    plt.close()
    files.append(p)
    log.info(f"Layer A 세부 차트: {p}")
    return files


# ─────────────────────────────────────────────
# 7. Slack 텍스트 메시지 생성
# ─────────────────────────────────────────────
def build_slack_text(ticker: str, bucket_results: dict,
                     sweet_spot: dict, df: pd.DataFrame,
                     years: int, image_urls: list = None,
                     layer_corr: dict = None, combo_results: list = None,
                     layer_image_urls: list = None,
                     sub_results: dict = None) -> list:
    """Slack blocks 생성"""
    today   = datetime.now(KST).strftime("%Y-%m-%d")
    n_total = len(df)

    def _sec(text): return {"type": "section", "text": {"type": "mrkdwn", "text": text}}
    def _div():     return {"type": "divider"}
    def _ctx(text): return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}
    def _pct(v):    return f"{v*100:+.1f}%" if v is not None else "N/A"
    def _wr(v):     return f"{v*100:.0f}%" if v is not None else "N/A"

    blocks = []
    blocks.append({"type": "header", "text": {"type": "plain_text",
        "text": f"📊 ${ticker} V3 백테스트 결과 — {today}"}})
    blocks.append(_ctx(
        f"분석 기간: 최근 {years}년 | 유효 샘플: {n_total}일 | "
        f"이상치 제거: 상하위 {OUTLIER_CLIP*100:.0f}%"
    ))
    blocks.append(_div())

    # ── 핵심 요약표 (10d 기준) ────────────────
    lines = ["*🎯 점수 구간별 10일 Forward Return 통계*",
             "```",
             f"{'구간':<8} {'횟수':>5}  {'평균수익':>8}  {'중앙값':>8}  {'승률':>6}  {'EV':>8}",
             "─" * 52]
    for label in BUCKET_LABELS:
        hd = bucket_results[label]["horizons"].get(10)
        cnt = bucket_results[label]["count"]
        if not hd:
            lines.append(f"{label:<8} {cnt:>5}  {'N/A':>8}  {'N/A':>8}  {'N/A':>6}  {'N/A':>8}")
            continue
        mark = " ⭐" if label == sweet_spot.get("bucket") else ""
        lines.append(
            f"{label:<8} {hd['count']:>5}  "
            f"{_pct(hd['avg']):>8}  {_pct(hd['median']):>8}  "
            f"{_wr(hd['winrate']):>6}  {_pct(hd['ev']):>8}{mark}"
        )
    lines.append("```")
    blocks.append(_sec("\n".join(lines)))
    blocks.append(_div())

    # ── 전체 Horizon 비교 ─────────────────────
    lines2 = ["*📈 기간별 평균 수익률 비교 (이상치 제거 후)*", "```",
              f"{'구간':<8} {'5d':>8}  {'10d':>8}  {'20d':>8}",
              "─" * 36]
    for label in BUCKET_LABELS:
        row_vals = []
        for h in HORIZONS:
            hd = bucket_results[label]["horizons"].get(h)
            row_vals.append(_pct(hd["avg"]) if hd else "N/A")
        lines2.append(f"{label:<8} {row_vals[0]:>8}  {row_vals[1]:>8}  {row_vals[2]:>8}")
    lines2.append("```")
    blocks.append(_sec("\n".join(lines2)))
    blocks.append(_div())

    # ── Sweet Spot ────────────────────────────
    if sweet_spot:
        ss = sweet_spot
        blocks.append(_sec(
            f"*🏆 Sweet Spot 분석 (10d EV 기준)*\n"
            f"최적 구간: *{ss['bucket']}점* "
            f"| 샘플 {ss['count']}개 "
            f"| 평균 {_pct(ss['avg'])} "
            f"| 승률 {_wr(ss['winrate'])} "
            f"| EV {_pct(ss['ev'])}\n"
            f"_→ {ss['bucket']}점대 신호 발생 시 통계적으로 가장 유리한 구간_"
        ))
    else:
        blocks.append(_sec(
            "*🏆 Sweet Spot 분석*\n"
            "_데이터 부족 또는 유효한 구간 없음 (샘플 < 5)_"
        ))
    blocks.append(_div())

    # ── 점수 분포 ─────────────────────────────
    score_dist = df["score"].value_counts(bins=5).sort_index()
    score_mean = df["score"].mean()
    score_median = df["score"].median()
    lines3 = [f"*📉 V3 점수 분포 (전체 {n_total}일)*",
              f"평균 {score_mean:.1f}점  |  중앙값 {score_median:.0f}점"]
    high_pct = (df["score"] >= 61).mean() * 100
    lines3.append(f"61점 이상 발생 빈도: *{high_pct:.1f}%* ({int(high_pct * n_total / 100)}일)")
    blocks.append(_sec("\n".join(lines3)))

    # ── 이미지 (Imgur URL 있을 때만) ─────────────
    img_labels = ["📊 히트맵 (점수구간 × 수익률)", "📈 수익률 분포 (고득점 vs 저득점)"]
    if image_urls:
        for url, label in zip(image_urls, img_labels):
            if url:
                blocks.append(_div())
                blocks.append({"type": "section",
                                "text": {"type": "mrkdwn", "text": f"*{label}*"}})
                blocks.append({"type": "image",
                                "image_url": url,
                                "alt_text": label})

    # ── 레이어 분석 요약 (텍스트) ──────────────
    if layer_corr:
        blocks.append(_div())
        lines_lc = ["*🔬 레이어별 예측력 분석 (Spearman ρ, 10d 기준)*", "```",
                    f"{'레이어':<14} {'ρ(10d)':>8}  {'엣지(10d)':>10}  {'엣지(30d)':>10}",
                    "─" * 48]
        for lid, meta in LAYERS.items():
            h10 = layer_corr[lid]["horizons"].get(10)
            h30 = layer_corr[lid]["horizons"].get(30)
            sp  = f"{h10['spearman']:+.3f}" if h10 else " N/A "
            e10 = f"{h10['edge']*100:+.1f}%" if h10 else "N/A"
            e30 = f"{h30['edge']*100:+.1f}%" if h30 else "N/A"
            lines_lc.append(f"{lid}:{meta['name']:<12} {sp:>8}  {e10:>10}  {e30:>10}")
        lines_lc.append("```")
        lines_lc.append("_ρ > 0.1 = 예측력 있음 | ρ < 0 = 역방향 (설계 문제)_")
        blocks.append(_sec("\n".join(lines_lc)))

    # ── 콤보 Top 5 ───────────────────────────
    if combo_results:
        top5 = [c for c in combo_results if c["ev_10d"] > 0][:5]
        if top5:
            blocks.append(_div())
            lines_cb = ["*🧩 레이어 조합 Top 5 (10d EV 기준)*", "```",
                        f"{'조합':<18} {'n':>5}  {'평균':>8}  {'승률':>6}  {'EV':>8}",
                        "─" * 52]
            for c in top5:
                lines_cb.append(
                    f"{c['combo']:<18} {c['count']:>5}  "
                    f"{c['avg_10d']*100:>+7.1f}%  "
                    f"{c['wr_10d']*100:>5.0f}%  "
                    f"{c['ev_10d']*100:>+7.2f}%"
                )
            lines_cb.append("```")
            blocks.append(_sec("\n".join(lines_cb)))

    # ── 레이어 분석 이미지 ────────────────────
    # ── Layer A 세부 분석 텍스트 ────────────────
    if sub_results:
        blocks.append(_div())
        lines_sub = ["*🔬 Layer A 세부 지표 분석 (10d Spearman ρ)*", "```",
                     f"{'지표':<8} {'ρ(10d)':>8}  {'엣지(10d)':>10}  {'엣지(30d)':>10}",
                     "─" * 44]
        for name, data in sub_results.items():
            h10 = data["horizons"].get(10)
            h30 = data["horizons"].get(30)
            sp  = f"{h10['spearman']:+.3f}" if h10 else "N/A"
            e10 = f"{h10['edge']*100:+.1f}%" if h10 else "N/A"
            e30 = f"{h30['edge']*100:+.1f}%" if h30 else "N/A"
            mark = " ✅" if (h10 and h10["spearman"] > 0.05) else (" ⚠️" if (h10 and h10["spearman"] < 0) else "")
            lines_sub.append(f"{name:<8} {sp:>8}  {e10:>10}  {e30:>10}{mark}")
        lines_sub.append("```")

        # 최강 버킷 요약
        for name, data in sub_results.items():
            best = max(
                [(l, d) for l, d in data["buckets"].items() if d and d["count"] >= 5],
                key=lambda x: x[1]["ev"], default=(None, None)
            )
            if best[0]:
                lines_sub.append(
                    f"*{name}* 최강구간: _{best[0]}_ "
                    f"→ WR {best[1]['winrate']*100:.0f}%  EV {best[1]['ev']*100:+.2f}%  n={best[1]['count']}"
                )
        blocks.append(_sec("\n".join(lines_sub)))

    layer_img_labels = ["🔬 레이어 상관계수", "📊 레이어 엣지", "🧩 콤보 랭킹"]
    if layer_image_urls:
        for url, label in zip(layer_image_urls, layer_img_labels):
            if url:
                blocks.append(_div())
                blocks.append({"type": "section",
                                "text": {"type": "mrkdwn", "text": f"*{label}*"}})
                blocks.append({"type": "image",
                                "image_url": url,
                                "alt_text": label})

    blocks.append(_ctx(
        "V3 Scoring 백테스트 | 과거 통계 기반 참고용 | "
        "전체 차트는 outputs/ 폴더 또는 Imgur 링크 확인"
    ))
    return blocks


def upload_to_imgur(image_path: Path) -> str:
    """
    PNG를 Imgur 익명 업로드 → 공개 URL 반환.
    실패 시 빈 문자열 반환 (graceful degradation).
    Client ID 발급: https://imgur.com/oauth2/addclient (Anonymous usage 선택, 1분)
    """
    if not IMGUR_CLIENT_ID:
        log.warning("IMGUR_CLIENT_ID 없음 — 이미지 업로드 생략")
        return ""
    try:
        with open(image_path, "rb") as f:
            image_data = f.read()
        r = requests.post(
            "https://api.imgur.com/3/image",
            headers={"Authorization": f"Client-ID {IMGUR_CLIENT_ID}"},
            data={"image": image_data, "type": "file",
                  "title": image_path.stem, "description": "V3 Backtest Chart"},
            timeout=30,
        )
        if r.status_code == 200:
            url = r.json()["data"]["link"]
            log.info(f"Imgur 업로드 성공: {image_path.name} → {url}")
            return url
        else:
            log.warning(f"Imgur 업로드 실패: {r.status_code} {r.text[:100]}")
            return ""
    except Exception as e:
        log.warning(f"Imgur 업로드 오류: {e}")
        return ""


def send_slack(blocks: list):
    if not SLACK_WEBHOOK:
        log.warning("SLACK_WEBHOOK 없음 — Slack 전송 생략")
        return
    try:
        r = requests.post(SLACK_WEBHOOK,
                          json={"blocks": blocks, "text": "V3 백테스트 결과"},
                          timeout=15)
        log.info(f"Slack 전송: {r.status_code}")
    except Exception as e:
        log.error(f"Slack 전송 실패: {e}")


# ─────────────────────────────────────────────
# 8. 콘솔 요약 출력
# ─────────────────────────────────────────────
def print_summary(ticker: str, bucket_results: dict,
                  sweet_spot: dict, df: pd.DataFrame):
    def _pct(v): return f"{v*100:+.2f}%" if v is not None else "N/A"
    def _wr(v):  return f"{v*100:.1f}%" if v is not None else "N/A"

    print(f"\n{'='*62}")
    print(f"  ${ticker} V3 Score-Forward Return 백테스트 결과")
    print(f"  총 샘플: {len(df)}일  |  점수 평균: {df['score'].mean():.1f}")
    print(f"{'='*62}")
    print(f"{'구간':<8} {'횟수':>5}  {'Avg 5d':>9}  {'Avg 10d':>9}  {'Avg 20d':>9}  {'WR 10d':>7}  {'EV 10d':>9}")
    print("-" * 62)
    for label in BUCKET_LABELS:
        cnt = bucket_results[label]["count"]
        vals = []
        for h in HORIZONS:
            hd = bucket_results[label]["horizons"].get(h)
            vals.append(_pct(hd["avg"]) if hd else "N/A")
        h10 = bucket_results[label]["horizons"].get(10)
        wr  = _wr(h10["winrate"]) if h10 else "N/A"
        ev  = _pct(h10["ev"])     if h10 else "N/A"
        mark = " ⭐" if label == sweet_spot.get("bucket") else ""
        print(f"{label:<8} {cnt:>5}  {vals[0]:>9}  {vals[1]:>9}  {vals[2]:>9}  {wr:>7}  {ev:>9}{mark}")
    print("-" * 62)
    if sweet_spot:
        ss = sweet_spot
        print(f"\n🏆 Sweet Spot: {ss['bucket']}점  "
              f"| 평균 {_pct(ss['avg'])}  | 승률 {_wr(ss['winrate'])}  | EV {_pct(ss['ev'])}")
    print()


# ─────────────────────────────────────────────
# 8-B. 레이어 콘솔 요약
# ─────────────────────────────────────────────
def print_layer_summary(layer_corr: dict, combo_results: list,
                        sub_results: dict = None):
    # Layer A 세부 먼저 출력
    if sub_results:
        print(f"\n{'='*62}")
        print("  Layer A 세부 지표 예측력 (Spearman ρ, 10d)")
        print(f"{'='*62}")
        print(f"{'지표':<8} {'ρ(10d)':>8}  {'엣지(10d)':>10}  {'엣지(30d)':>10}")
        print("-" * 44)
        for name, data in sub_results.items():
            h10 = data['horizons'].get(10)
            h30 = data['horizons'].get(30)
            sp  = f"{h10['spearman']:+.3f}" if h10 else " N/A "
            e10 = f"{h10['edge']*100:+.1f}%" if h10 else "N/A"
            e30 = f"{h30['edge']*100:+.1f}%" if h30 else "N/A"
            mark = " ✅" if (h10 and h10['spearman'] > 0.05) else ""
            print(f"{name:<8} {sp:>8}  {e10:>10}  {e30:>10}{mark}")
        print("-" * 44)
        for name, data in sub_results.items():
            best = max(
                [(l,d) for l,d in data['buckets'].items() if d and d['count']>=5],
                key=lambda x: x[1]['ev'], default=(None,None)
            )
            if best[0]:
                print(f"  {name} 최강: {best[0]} → WR {best[1]['winrate']*100:.0f}% EV {best[1]['ev']*100:+.2f}%")

    print(f"\n{'='*62}")
    print("  레이어별 예측력 (Spearman ρ)")
    print(f"{'='*62}")
    print(f"{'레이어':<16} {'5d':>7}  {'10d':>7}  {'20d':>7}  {'30d':>7}  {'60d':>7}")
    print("-" * 62)
    for lid, data in layer_corr.items():
        vals = []
        for h in HORIZONS:
            hd = data["horizons"].get(h)
            vals.append(f"{hd['spearman']:+.3f}" if hd else "  N/A ")
        mark = ""
        h10 = data["horizons"].get(10)
        if h10:
            if h10["spearman"] > 0.1:  mark = " ✅"
            elif h10["spearman"] < -0.05: mark = " ⚠️ 역방향"
        print(f"{lid}:{data['name']:<14} " + "  ".join(f"{v:>7}" for v in vals) + mark)
    print("-" * 62)

    print(f"\n{'='*62}")
    print("  레이어 조합 Top 10 (10d EV 기준)")
    print(f"{'='*62}")
    print(f"{'조합':<20} {'n':>5}  {'평균':>8}  {'승률':>6}  {'EV':>8}")
    print("-" * 62)
    for c in [x for x in combo_results if x["ev_10d"] > 0][:10]:
        print(f"{c['combo']:<20} {c['count']:>5}  "
              f"{c['avg_10d']*100:>+7.1f}%  "
              f"{c['wr_10d']*100:>5.0f}%  "
              f"{c['ev_10d']*100:>+7.2f}%")
    print("-" * 62)
    print()


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="V3 Score-Forward Return Backtester")
    parser.add_argument("--ticker",   type=str, default="HOOD", help="종목 (기본: HOOD)")
    parser.add_argument("--years",    type=int, default=2,      help="분석 기간 (년, 기본: 2)")
    parser.add_argument("--no-slack", action="store_true",       help="Slack 전송 생략")
    parser.add_argument("--output",   type=str, default="outputs", help="차트 저장 폴더")
    args = parser.parse_args()

    ticker = args.ticker.upper()
    log.info(f"=== V3 백테스트 시작: ${ticker} ({args.years}년) ===")
    start = time.time()

    # ① 데이터 다운로드
    raw_df = download_data(ticker, args.years)

    # ② 롤링 스코어 계산 (핵심: lookahead-free)
    scored = compute_rolling_scores(raw_df, ticker, args.years)

    # ③ Forward Return 라벨링 + 이상치 제거
    scored = label_forward_returns(scored, raw_df)

    if len(scored) < 20:
        log.error(f"유효 샘플 부족 ({len(scored)}개). 기간을 늘려주세요.")
        sys.exit(1)

    # ④ 버킷 분석
    bucket_results = bucket_analysis(scored)

    # ⑤ Sweet Spot 탐지
    sweet_spot = find_sweet_spot(bucket_results, horizon=10)

    # ⑥ 레이어 독립 예측력 분석
    log.info("레이어 상관관계 분석 중...")
    layer_corr    = layer_correlation_analysis(scored)
    layer_buckets = layer_bucket_analysis(scored)
    combo_results = layer_combo_analysis(scored)
    sub_results   = layer_A_subanalysis(scored)

    # ⑦ 콘솔 출력
    print_summary(ticker, bucket_results, sweet_spot, scored)
    print_layer_summary(layer_corr, combo_results, sub_results)

    # ⑧ 차트 생성
    output_dir = Path(args.output)
    chart_files       = generate_charts(scored, bucket_results, ticker, output_dir)
    layer_chart_files = generate_layer_charts(scored, layer_corr, layer_buckets,
                                              combo_results, ticker, output_dir)
    sub_chart_files   = generate_subanalysis_chart(sub_results, ticker, output_dir)
    all_files = chart_files + layer_chart_files + sub_chart_files
    print(f"차트 저장 완료:")
    for f in all_files:
        print(f"  → {f}")

    # ⑨ Imgur 업로드 → Slack 전송
    if not args.no_slack:
        image_urls       = [upload_to_imgur(f) for f in chart_files]
        layer_image_urls = [upload_to_imgur(f) for f in layer_chart_files + sub_chart_files]
        blocks = build_slack_text(
            ticker, bucket_results, sweet_spot, scored, args.years,
            image_urls=image_urls,
            layer_corr=layer_corr,
            combo_results=combo_results,
            layer_image_urls=layer_image_urls,
            sub_results=sub_results,
        )
        send_slack(blocks)

    elapsed = time.time() - start
    log.info(f"=== 완료: {elapsed:.1f}초 ===")

    # ⑩ 결과 JSON 저장
    json_path = output_dir / f"{ticker}_backtest.json"
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "ticker": ticker, "years": args.years,
        "n_samples": len(scored),
        "score_mean": round(scored["score"].mean(), 2),
        "score_median": round(scored["score"].median(), 2),
        "sweet_spot": sweet_spot,
        "top_combos": combo_results[:10],
        "bucket_results": {
            label: {
                "count": data["count"],
                "horizons": {
                    str(h): {k: round(v, 6) if isinstance(v, float) else v
                             for k, v in (hd if hd else {}).items()}
                    for h, hd in data["horizons"].items()
                }
            }
            for label, data in bucket_results.items()
        },
        "layer_corr": {
            lid: {"name": data["name"],
                  "horizons": {str(h): {k: round(v,4) if isinstance(v,float) else v
                                        for k,v in (hd if hd else {}).items()}
                               for h, hd in data["horizons"].items()}}
            for lid, data in layer_corr.items()
        }
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    log.info(f"JSON 결과 저장: {json_path}")


if __name__ == "__main__":
    main()
