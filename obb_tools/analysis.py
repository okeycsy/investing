"""
Derived analytics. Pure functions on DataFrames/dicts — no network.
"""
from __future__ import annotations

import math
import re
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd


def _f(x, default=None):
    try:
        v = float(x)
        return default if math.isnan(v) else v
    except Exception:  # noqa: BLE001
        return default


# ───────────────────────── technicals ─────────────────────────
def rsi(close: pd.Series, period: int = 14) -> float:
    d = close.diff()
    g = d.clip(lower=0).ewm(alpha=1 / period, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(alpha=1 / period, adjust=False).mean()
    r = 100 - 100 / (1 + g / l)
    return round(float(r.iloc[-1]), 1)


def technicals(hist: pd.DataFrame) -> dict:
    c = hist["close"]
    out = {
        "last": round(float(c.iloc[-1]), 2),
        "date": c.index[-1].date().isoformat(),
        "chg_pct": round(float(c.pct_change().iloc[-1] * 100), 2) if len(c) > 1 else None,
        "chg_5d_pct": round(float(c.iloc[-1] / c.iloc[-6] - 1) * 100, 2) if len(c) > 6 else None,
        "chg_1m_pct": round(float(c.iloc[-1] / c.iloc[-22] - 1) * 100, 2) if len(c) > 22 else None,
        "chg_ytd_pct": None,
        "rsi14": rsi(c) if len(c) > 20 else None,
        "ma20": round(float(c.tail(20).mean()), 2),
        "ma50": round(float(c.tail(50).mean()), 2) if len(c) >= 50 else None,
        "ma200": round(float(c.tail(200).mean()), 2) if len(c) >= 200 else None,
        "hi_52w": round(float(c.tail(252).max()), 2),
        "hi_52w_date": c.tail(252).idxmax().date().isoformat(),
        "lo_52w": round(float(c.tail(252).min()), 2),
        "hi_20d": round(float(c.tail(20).max()), 2),
        "lo_20d": round(float(c.tail(20).min()), 2),
        "rv20": round(float(c.pct_change().tail(20).std() * math.sqrt(252) * 100), 1),
        "rv60": round(float(c.pct_change().tail(60).std() * math.sqrt(252) * 100), 1),
        "vol_ratio": None,
    }
    ytd = c[c.index.year == c.index[-1].year]
    if len(ytd) > 1:
        out["chg_ytd_pct"] = round(float(c.iloc[-1] / ytd.iloc[0] - 1) * 100, 1)
    out["drawdown_pct"] = round((out["last"] / out["hi_52w"] - 1) * 100, 1)
    if "volume" in hist:
        v = hist["volume"]
        if len(v) > 21 and v.tail(21).iloc[:-1].mean() > 0:
            out["vol_ratio"] = round(float(v.iloc[-1] / v.tail(21).iloc[:-1].mean()), 2)
    return out


# ───────────────────────── move decomposition ─────────────────────────
def move_decomposition(target: pd.Series, basket: pd.DataFrame, bench: Optional[pd.Series] = None,
                       lookback: int = 120) -> dict:
    """
    Regress target daily returns on an equal-weight basket (and optionally a benchmark) to split
    today's move into "sector" vs "idiosyncratic". Residual drift over 5/20 days tells whether the
    market is repricing the name itself, independent of the theme.
    """
    r = target.pct_change()
    b = basket.pct_change().mean(axis=1)
    df = pd.DataFrame({"t": r, "b": b})
    if bench is not None:
        df["m"] = bench.pct_change()
    df = df.dropna().tail(lookback)
    if len(df) < 40:
        return {}
    X = np.column_stack([np.ones(len(df)), df["b"].values] + ([df["m"].values] if "m" in df else []))
    coef, *_ = np.linalg.lstsq(X, df["t"].values, rcond=None)
    pred = X @ coef
    resid = df["t"].values - pred
    r2 = 1 - resid.var() / df["t"].var() if df["t"].var() > 0 else 0
    today = df.iloc[-1]
    exp_today = float(pred[-1])
    res_today = float(resid[-1])
    out = {
        "beta_basket": round(float(coef[1]), 2),
        "beta_bench": round(float(coef[2]), 2) if len(coef) > 2 else None,
        "r2": round(float(r2), 2),
        "actual_pct": round(float(today["t"]) * 100, 2),
        "basket_pct": round(float(today["b"]) * 100, 2),
        "bench_pct": round(float(today["m"]) * 100, 2) if "m" in df else None,
        "expected_pct": round(exp_today * 100, 2),
        "residual_pct": round(res_today * 100, 2),
        "residual_5d_pct": round(float(resid[-5:].sum()) * 100, 2),
        "residual_20d_pct": round(float(resid[-20:].sum()) * 100, 2),
        "resid_sigma_pct": round(float(resid.std()) * 100, 2),
    }
    z = res_today / resid.std() if resid.std() > 0 else 0
    out["residual_z"] = round(float(z), 2)
    if abs(z) >= 2:
        out["label"] = "BE 고유 요인이 지배 (섹터로 설명 안 되는 큰 움직임)"
    elif abs(z) >= 1:
        out["label"] = "섹터 + 일부 고유 요인"
    else:
        out["label"] = "섹터 동반 움직임"
    return out


def relative_table(closes: pd.DataFrame) -> pd.DataFrame:
    """1d/5d/1m/3m returns + drawdown from 1y high for each column."""
    rows = []
    for s in closes.columns:
        c = closes[s].dropna()
        if len(c) < 25:
            continue
        rows.append({
            "sym": s,
            "1d": c.iloc[-1] / c.iloc[-2] - 1,
            "5d": c.iloc[-1] / c.iloc[-6] - 1,
            "1m": c.iloc[-1] / c.iloc[-22] - 1,
            "3m": c.iloc[-1] / c.iloc[-64] - 1 if len(c) > 64 else np.nan,
            "from_1y_high": c.iloc[-1] / c.tail(252).max() - 1,
        })
    df = pd.DataFrame(rows).set_index("sym")
    return (df * 100).round(1)


# ───────────────────────── options ─────────────────────────
def options_summary(chains: pd.DataFrame, spot: float, earnings: Optional[date] = None) -> dict:
    if chains is None or chains.empty:
        return {}
    c = chains.copy()
    c["mid"] = (c["bid"].fillna(0) + c["ask"].fillna(0)) / 2
    c.loc[c["mid"] <= 0, "mid"] = c["last_trade_price"]
    calls, puts = c[c.option_type == "call"], c[c.option_type == "put"]
    out = {
        "pcr_oi": round(puts.open_interest.sum() / max(calls.open_interest.sum(), 1), 2),
        "pcr_vol": round(puts.volume.sum() / max(calls.volume.sum(), 1), 2),
        "total_oi": int(c.open_interest.sum()),
    }
    # per-expiry ATM IV & straddle implied move
    rows = []
    for exp, g in c.groupby("expiration"):
        dte = int(g["dte"].iloc[0])
        if dte <= 0:
            continue
        k = g.iloc[(g.strike - spot).abs().argsort()].strike.iloc[0]
        a = g[g.strike == k]
        straddle = float(a["mid"].sum())
        rows.append({
            "expiration": exp.date().isoformat(), "dte": dte, "atm_strike": float(k),
            "iv": round(float(a.implied_volatility.mean()) * 100, 1),
            "straddle": round(straddle, 2), "implied_move_pct": round(straddle / spot * 100, 1),
            "oi": int(g.open_interest.sum()),
        })
    term = pd.DataFrame(rows).sort_values("dte")
    out["term"] = term
    # ~30-day IV (closest expiry to 30 dte)
    if not term.empty:
        near30 = term.iloc[(term.dte - 30).abs().argsort()].iloc[0]
        out["iv30"] = float(near30.iv)
        out["move_30d_pct"] = float(near30.implied_move_pct)
        out["exp_30d"] = near30.expiration
    if earnings is not None and not term.empty:
        post = term[pd.to_datetime(term.expiration).dt.date >= earnings]
        pre = term[pd.to_datetime(term.expiration).dt.date < earnings]
        if not post.empty:
            e = post.iloc[0]
            out["earnings_expiry"] = e.expiration
            out["earnings_move_pct"] = float(e.implied_move_pct)
            out["earnings_iv"] = float(e.iv)
            # crude event-vol extraction: var(post) - var(pre) over the extra days
            if not pre.empty:
                p = pre.iloc[-1]
                v_post = (e.iv / 100) ** 2 * e.dte / 365
                v_pre = (p.iv / 100) ** 2 * p.dte / 365
                ev = v_post - v_pre
                if ev > 0:
                    out["event_move_pct"] = round(math.sqrt(ev) * 100 * 0.8, 1)  # ≈ expected |move|
    # gamma walls: largest OI strikes within ±40% of spot, excluding far-dated
    near = c[(c.dte <= 75) & (c.strike.between(spot * 0.6, spot * 1.4))]
    if not near.empty:
        oi_by_k = near.groupby(["strike", "option_type"]).open_interest.sum().reset_index()
        up = oi_by_k[(oi_by_k.strike > spot) & (oi_by_k.option_type == "call")].nlargest(3, "open_interest")
        dn = oi_by_k[(oi_by_k.strike < spot) & (oi_by_k.option_type == "put")].nlargest(3, "open_interest")
        out["call_walls"] = [(float(r.strike), int(r.open_interest)) for r in up.itertuples()]
        out["put_walls"] = [(float(r.strike), int(r.open_interest)) for r in dn.itertuples()]
    return out


# ───────────────────────── convertible / key levels ─────────────────────────
def convertible_watch(close: pd.Series, conv_price: float, trigger_mult: float = 1.30,
                      need_days: int = 20, window: int = 30) -> dict:
    trig = conv_price * trigger_mult
    last30 = close.tail(window)
    n = int((last30 >= trig).sum())
    last = float(close.iloc[-1])
    return {
        "conv_price": conv_price, "trigger": round(trig, 2),
        "dist_conv_pct": round((last / conv_price - 1) * 100, 1),
        "dist_trigger_pct": round((last / trig - 1) * 100, 1),
        "days_above_trigger_30": n, "need_days": need_days,
        "in_the_money": last > conv_price,
    }


# ───────────────────────── short interest ─────────────────────────
def short_interest_summary(si: pd.DataFrame, float_shares: Optional[float]) -> dict:
    if si is None or si.empty:
        return {}
    last = si.iloc[-1]
    out = {
        "settlement_date": last.settlement_date.date().isoformat(),
        "short": int(last.current_short_position),
        "prev": int(last.previous_short_position) if pd.notna(last.previous_short_position) else None,
        "chg_pct": round(float(last.change_pct), 1) if pd.notna(last.change_pct) else None,
        "days_to_cover": round(float(last.days_to_cover), 2) if pd.notna(last.days_to_cover) else None,
    }
    if float_shares:
        out["pct_float"] = round(out["short"] / float_shares * 100, 2)
    # 6-report trend
    hist = si.tail(6)[["settlement_date", "current_short_position"]]
    out["trend"] = [(d.date().isoformat(), int(v)) for d, v in zip(hist.settlement_date, hist.current_short_position)]
    return out


# ───────────────────────── estimates ─────────────────────────
def estimates_summary(eps_trend: pd.DataFrame, eps_rev: pd.DataFrame, recs: pd.DataFrame,
                      cons: dict, rev_est: pd.DataFrame = None) -> dict:
    out = {"consensus": cons}
    if eps_trend is not None and not eps_trend.empty:
        t = eps_trend.set_index("period")
        out["eps_trend"] = {
            p: {"now": _f(t.loc[p, "current"]), "d7": _f(t.loc[p, "days_ago_7"]),
                "d30": _f(t.loc[p, "days_ago_30"]), "d90": _f(t.loc[p, "days_ago_90"])}
            for p in t.index
        }
    if eps_rev is not None and not eps_rev.empty:
        r = eps_rev.set_index("period")
        out["revisions"] = {
            p: {"up30": int(r.loc[p, "up_last_30_days"]), "down30": int(r.loc[p, "down_last_30_days"]),
                "up7": int(r.loc[p, "up_last_7_days"]), "down7": int(r.loc[p, "down_last_7_days"])}
            for p in r.index
        }
    if recs is not None and not recs.empty:
        now = recs.iloc[0]
        out["recs_now"] = {k: int(now[k]) for k in ["strong_buy", "buy", "hold", "sell", "strong_sell"]}
        if len(recs) > 1:
            prev = recs.iloc[1]
            out["recs_prev"] = {k: int(prev[k]) for k in ["strong_buy", "buy", "hold", "sell", "strong_sell"]}
    if rev_est is not None and not rev_est.empty:
        out["rev_est"] = rev_est.set_index("period")[["mean", "low", "high", "growth", "number_of_analysts"]].to_dict("index")
    return out


# ───────────────────────── filings classification ─────────────────────────
ITEM_8K = {
    "1.01": "중요 계약 체결", "1.02": "중요 계약 종료", "2.01": "자산 취득/처분 완료",
    "2.02": "실적 발표", "2.03": "채무 발생(직접 금융의무)", "2.04": "채무 가속화 트리거",
    "2.05": "구조조정 비용", "2.06": "자산 손상", "3.01": "상장 기준 미달 통지",
    "3.02": "비등록 주식 발행(희석)", "3.03": "주주 권리 변경", "4.01": "회계법인 변경",
    "4.02": "과거 재무제표 신뢰 불가(재작성)", "5.01": "지배권 변경", "5.02": "이사/임원 선임·사임",
    "5.03": "정관 변경", "5.07": "주주총회 결과", "7.01": "Reg FD 공시", "8.01": "기타 중요 사건",
    "9.01": "재무제표·첨부",
}
_SEV = {"🚨": 3, "⚠️": 2, "📄": 1, "·": 0}


def classify_filing(report_type: str, items: str = "", desc: str = "") -> tuple[str, str]:
    """Return (severity_emoji, korean label). Severity: 🚨 act now / ⚠️ read today / 📄 fyi / · ignore."""
    rt = (report_type or "").upper()
    items = items or ""
    if rt.startswith(("424B", "S-3", "S-1", "F-3", "F-1")) or "ASR" in rt:
        return "🚨", f"{rt}: 증권 발행 서류 (유상증자/ATM 가능성 → 희석 확인)"
    if rt == "144":
        return "⚠️", "Form 144: 내부자 매도 예정 통지 (Form 4보다 먼저 나옴)"
    if rt.startswith("SC 13D"):
        return "🚨", f"{rt}: 5% 이상 활동주주 보유 신고"
    if rt.startswith("SC 13G"):
        return "📄", f"{rt}: 5% 이상 수동 보유 신고"
    if rt in ("10-Q", "10-K", "10-K/A", "10-Q/A"):
        return "⚠️", f"{rt}: 정기 보고서 → 이익의 질 체크포인트 재계산"
    if rt.startswith("8-K"):
        codes = re.findall(r"\d\.\d\d", items)
        labels = [ITEM_8K.get(c, c) for c in codes]
        sev = "📄"
        if any(c in ("3.02", "4.02", "2.04", "3.01", "5.01", "1.02") for c in codes):
            sev = "🚨"
        elif any(c in ("1.01", "2.02", "2.03", "5.02", "2.06") for c in codes):
            sev = "⚠️"
        return sev, f"8-K ({', '.join(labels) if labels else desc or 'items 미상'})"
    if rt in ("4", "4/A"):
        return "📄", "Form 4: 내부자 거래"
    if rt in ("3", "3/A"):
        return "·", "Form 3: 내부자 최초 보유 신고"
    if rt.startswith("DEF 14"):
        return "📄", f"{rt}: 주주총회 위임장 (보상/안건)"
    if rt in ("13F-HR", "13F-HR/A", "NPORT-P"):
        return "·", rt
    return "📄", f"{rt}: {desc}" if desc else rt


# ───────────────────────── insiders ─────────────────────────
def _title(t, is_director) -> str:
    s = "" if t is None or (isinstance(t, float) and math.isnan(t)) else str(t)
    if s.lower() in ("", "nan", "none"):
        return "Director" if is_director else ""
    return s


def insider_rows(df: pd.DataFrame, since: Optional[str] = None, limit: int = 15) -> list[dict]:
    """Normalise SEC Form 4 rows (obb.sec.insider_trading) into compact dicts."""
    if df is None or df.empty:
        return []
    d = df.copy()
    d["filing_date"] = pd.to_datetime(d["filing_date"])
    if since:
        d = d[d["filing_date"] > pd.to_datetime(since)]
    rows = []
    for r in d.head(limit).itertuples():
        tt = str(getattr(r, "transaction_type", "") or "")
        ad = str(getattr(r, "acquisition_or_disposition", "") or "")
        kind = "매도" if ad == "Disposition" else "매수/취득"
        if "award" in tt.lower() or "grant" in tt.lower():
            kind = "보상 지급"
        elif "exercise" in tt.lower() or "conversion" in tt.lower():
            kind = "옵션 행사/전환"
        elif "gift" in tt.lower():
            kind = "증여"
        elif "tax" in tt.lower() or "withholding" in tt.lower():
            kind = "세금 납부용 처분"
        shares = _f(getattr(r, "securities_transacted", None), 0) or 0
        price = _f(getattr(r, "transaction_price", None), 0) or 0
        foot = str(getattr(r, "footnote", "") or "")
        rows.append({
            "filing_date": r.filing_date.date().isoformat(),
            "tx_date": str(getattr(r, "transaction_date", ""))[:10],
            "name": str(getattr(r, "owner_name", "")).title(),
            "title": _title(getattr(r, "owner_title", None), getattr(r, "director", False)),
            "kind": kind, "shares": int(shares), "price": round(price, 2),
            "value": round(shares * price),
            "rule_10b5_1": "10b5-1" in foot,
            "url": str(getattr(r, "filing_url", "")),
            "key": f"{r.filing_date.date()}|{getattr(r, 'owner_name', '')}|{tt}|{shares}|{price}",
        })
    return rows


# ───────────────────────── earnings quality verdict ─────────────────────────
def quality_verdict(tbl: pd.DataFrame, qm: pd.DataFrame) -> list[str]:
    """Plain-language flags from the XBRL quality table (latest vs prior quarter)."""
    if tbl is None or tbl.empty or len(tbl) < 2:
        return []
    cur, prev = tbl.iloc[-1], tbl.iloc[-2]
    q_cur = qm.iloc[-1] if qm is not None and not qm.empty else {}
    q_prev = qm.iloc[-2] if qm is not None and len(qm) > 1 else {}
    lines = []
    if "contract_assets" in tbl and pd.notna(cur.get("contract_assets")):
        d = cur["contract_assets"] - prev["contract_assets"]
        pct = d / cur["revenue"] * 100 if cur.get("revenue") else 0
        tone = "⚠️" if pct > 15 else ("✅" if d < 0 else "·")
        lines.append(f"{tone} 미청구 매출(계약자산) ${cur['contract_assets']:,.0f}M ({d:+,.0f}M QoQ, 분기 매출의 {pct:+.0f}%)")
    if "unbilled_plus_ar_days" in q_cur:
        d = q_cur["unbilled_plus_ar_days"] - (q_prev.get("unbilled_plus_ar_days") or 0)
        tone = "⚠️" if d > 10 else ("✅" if d < -5 else "·")
        lines.append(f"{tone} 매출채권+미청구 회수 일수 {q_cur['unbilled_plus_ar_days']:.0f}일 ({d:+.0f}일 QoQ)")
    if "ocf" in tbl and pd.notna(cur.get("ocf")):
        ni = cur.get("net_income")
        ratio = f", OCF/순이익 {cur['ocf']/ni:.2f}x" if ni and ni > 0 else ""
        tone = "⚠️" if cur["ocf"] < 0 else "✅"
        lines.append(f"{tone} 영업현금흐름 ${cur['ocf']:,.0f}M{ratio}")
    if "fcf" in q_cur and pd.notna(q_cur.get("fcf")):
        lines.append(f"{'⚠️' if q_cur['fcf'] < 0 else '✅'} FCF ${q_cur['fcf']:,.0f}M (capex ${abs(cur.get('capex', 0)):,.0f}M)")
    if "customer_deposits" in tbl and pd.notna(cur.get("customer_deposits")):
        d = cur["customer_deposits"] - prev["customer_deposits"]
        lines.append(f"{'✅' if d > 0 else '⚠️'} 고객 선수금 ${cur['customer_deposits']:,.0f}M ({d:+,.0f}M QoQ) — 수요 대리 지표")
    if "inventory" in tbl and pd.notna(cur.get("inventory")):
        d = cur["inventory"] - prev["inventory"]
        lines.append(f"{'⚠️' if d / max(cur['revenue'], 1) > 0.15 else '·'} 재고 ${cur['inventory']:,.0f}M ({d:+,.0f}M QoQ)")
    if "payables" in tbl and pd.notna(cur.get("payables")):
        d = cur["payables"] - prev["payables"]
        if d > 0.1 * cur["revenue"]:
            lines.append(f"⚠️ 매입채무 {d:+,.0f}M QoQ — 영업현금흐름이 지급 지연으로 부풀려졌을 가능성")
    if "shares_yoy_pct" in q_cur and pd.notna(q_cur.get("shares_yoy_pct")):
        lines.append(f"{'⚠️' if q_cur['shares_yoy_pct'] > 10 else '·'} 발행주식수 YoY {q_cur['shares_yoy_pct']:+.1f}%")
    return lines


# ───────────────────────── misc ─────────────────────────
def days_until(d: Optional[date]) -> Optional[int]:
    return (d - date.today()).days if d else None


def news_rows(df: pd.DataFrame, limit: int = 12) -> list[dict]:
    if df is None or df.empty:
        return []
    rows = []
    for r in df.head(limit).itertuples():
        title = str(getattr(r, "title", ""))
        rows.append({
            "date": str(getattr(r, "date", ""))[:16],
            "title": title,
            "url": str(getattr(r, "url", "") or ""),
            "key": re.sub(r"\W+", "", title.lower())[:60],
        })
    return rows
