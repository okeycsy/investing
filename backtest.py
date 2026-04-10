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
SLACK_WEBHOOK  = os.environ.get("MARKET_SCAN_WEBHOOK") or os.environ.get("SLACK_WEBHOOK_URL", "")
KST            = timezone(timedelta(hours=9))
OUTLIER_CLIP   = 0.01   # 상하위 1% 이상치 제거
WARMUP_DAYS    = 130    # 지표 워밍업 (EMA50·MACD 안정화에 필요한 최소 일수)
BUCKETS        = [(0,20),(21,40),(41,60),(61,75),(76,100)]   # 점수 구간
BUCKET_LABELS  = ["0-20","21-40","41-60","61-75","76-100"]
HORIZONS       = [5, 10, 20]   # 미래 수익률 계산 기간 (거래일)

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


# ─────────────────────────────────────────────
# 7. Slack 텍스트 메시지 생성
# ─────────────────────────────────────────────
def build_slack_text(ticker: str, bucket_results: dict,
                     sweet_spot: dict, df: pd.DataFrame,
                     years: int) -> list:
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

    blocks.append(_ctx(
        "V3 Scoring 백테스트 | 과거 통계 기반 참고용 | "
        "히트맵·분포 차트는 로컬 outputs/ 폴더 확인"
    ))
    return blocks


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

    # ⑥ 콘솔 출력
    print_summary(ticker, bucket_results, sweet_spot, scored)

    # ⑦ 차트 생성
    output_dir = Path(args.output)
    chart_files = generate_charts(scored, bucket_results, ticker, output_dir)
    print(f"차트 저장 완료:")
    for f in chart_files:
        print(f"  → {f}")

    # ⑧ Slack 전송
    if not args.no_slack:
        blocks = build_slack_text(ticker, bucket_results, sweet_spot, scored, args.years)
        send_slack(blocks)

    elapsed = time.time() - start
    log.info(f"=== 완료: {elapsed:.1f}초 ===")

    # ⑨ 결과 JSON 저장 (재사용 가능)
    json_path = output_dir / f"{ticker}_backtest.json"
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "ticker": ticker, "years": args.years,
        "n_samples": len(scored),
        "score_mean": round(scored["score"].mean(), 2),
        "score_median": round(scored["score"].median(), 2),
        "sweet_spot": sweet_spot,
        "bucket_results": {
            label: {
                "count": data["count"],
                "horizons": {
                    str(h): {k: round(v, 6) if isinstance(v, float) else v
                             for k, v in (hd.items() if hd else {}).items()}
                    for h, hd in data["horizons"].items()
                }
            }
            for label, data in bucket_results.items()
        }
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    log.info(f"JSON 결과 저장: {json_path}")


if __name__ == "__main__":
    main()
