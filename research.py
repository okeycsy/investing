#!/usr/bin/env python
"""
One-command equity research note built on OpenBB.

    .venv/bin/python research.py BE
    .venv/bin/python research.py NVDA --peers AMD,AVGO,MRVL --no-sec
    .venv/bin/python research.py BE --ai            # Claude summary of the latest 10-Q/10-K (needs ANTHROPIC_API_KEY)

Writes research/<TICKER>_<YYYY-MM-DD>.md. Data only — not investment advice.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

from obb_tools import analysis as A
from obb_tools import data as D
from obb_tools import sec_xbrl as X
from obb_tools.report import df_table, money, num, pct, table

log = logging.getLogger("research")
ROOT = Path(__file__).resolve().parent
DEFAULT_BASKET = ["QQQ"]  # sector basket when --peers not given


def _cfg_peers(sym: str) -> list[str]:
    """Peers from <sym>_config.json if present (e.g. be_config.json)."""
    p = ROOT / f"{sym.lower()}_config.json"
    if p.exists():
        try:
            return json.loads(p.read_text()).get("peers", [])
        except Exception:  # noqa: BLE001
            pass
    return []


# ───────────────────────── sections ─────────────────────────
def sec_price(sym: str, hist: pd.DataFrame, q: dict, t: dict) -> str:
    rows = [
        ("종가", f"${t['last']} ({t['date']}, {pct(t['chg_pct'], sign=True)})"),
        ("5일 / 1개월 / YTD", f"{pct(t['chg_5d_pct'], sign=True)} / {pct(t['chg_1m_pct'], sign=True)} / {pct(t['chg_ytd_pct'], sign=True)}"),
        ("52주 고가 / 저가", f"${t['hi_52w']} ({t['hi_52w_date']}) / ${t['lo_52w']} → 고점 대비 {pct(t['drawdown_pct'])}"),
        ("20 / 50 / 200일 이평", f"${t['ma20']} / ${t['ma50']} / ${t['ma200']}"),
        ("RSI(14)", t["rsi14"]),
        ("실현변동성 20d / 60d (연율)", f"{t['rv20']}% / {t['rv60']}%"),
        ("거래량 (20일 평균 대비)", f"{t['vol_ratio']}x" if t.get("vol_ratio") else "–"),
        ("베타 (yfinance)", num(q.get("beta"), 2)),
    ]
    return "## 1. 가격 / 기술적 위치\n\n" + table(["항목", "값"], rows)


def sec_financials(sym: str) -> str:
    out = ["## 2. 실적\n"]
    inc_q = D.income(sym, "quarter", 5)
    inc_a = D.income(sym, "annual", 5)
    cf_q = D.cash(sym, "quarter", 5)
    cols = {
        "total_revenue": "매출", "gross_profit": "매출총이익", "operating_income": "영업이익",
        "net_income_attributable_to_common_shareholders": "순이익", "diluted_earnings_per_share": "희석 EPS",
    }
    for label, df in (("분기", inc_q), ("연간", inc_a)):
        if df is None or df.empty:
            continue
        d = df.set_index("period_ending")[[c for c in cols if c in df.columns]].rename(columns=cols)
        if "매출" in d and "매출총이익" in d:
            d["GM%"] = d["매출총이익"] / d["매출"] * 100
        if "매출" in d and "영업이익" in d:
            d["OPM%"] = d["영업이익"] / d["매출"] * 100
        if label == "분기" and cf_q is not None and not cf_q.empty:
            c = cf_q.set_index("period_ending")
            for k, n in (("operating_cash_flow", "OCF"), ("free_cash_flow", "FCF")):
                if k in c:
                    d[n] = c[k]
        d = d.T  # periods as columns
        d.columns = [str(c)[:10] for c in d.columns]
        fmt_rows = []
        for idx, r in d.iterrows():
            cells = []
            for v in r:
                if idx in ("GM%", "OPM%"):
                    cells.append(pct(v))
                elif idx == "희석 EPS":
                    cells.append(num(v, 2))
                else:
                    cells.append(money(v))
            fmt_rows.append([idx] + cells)
        out.append(f"### {label}\n" + table([""] + list(d.columns), fmt_rows) + "\n")
    he = D.historical_eps(sym)
    if he is not None and not he.empty:
        h = he.dropna(subset=["eps_actual"]).tail(8)
        beats = int((h["surprise_percent"] > 0).sum())
        out.append(f"최근 {len(h)}개 분기 중 EPS 컨센서스 상회 {beats}회. 최근 서프라이즈: "
                   + ", ".join(f"{d.date()} {pct(s, 0, True)}" for d, s in zip(h.index[-4:], h["surprise_percent"].tail(4))))
    return "\n".join(out)


def sec_estimates(sym: str) -> str:
    cons = D.consensus(sym)
    est = A.estimates_summary(D.eps_trend(sym), D.eps_revisions(sym), D.recommendations(sym), cons, D.revenue_estimates(sym))
    out = ["## 3. 컨센서스 / 추정치 변화\n"]
    rows = []
    names = {"0q": "현재 분기", "+1q": "다음 분기", "0y": "현재 연도", "+1y": "다음 연도"}
    for p, n in names.items():
        et = est.get("eps_trend", {}).get(p)
        rv = est.get("rev_est", {}).get(p)
        rev = est.get("revisions", {}).get(p, {})
        if not et and not rv:
            continue
        drift = ""
        if et and et.get("d30"):
            drift = pct((et["now"] / et["d30"] - 1) * 100, 1, True)
        rows.append((
            n, money(rv["mean"]) if rv else "–", pct(rv["growth"] * 100, 0, True) if rv and rv.get("growth") is not None else "–",
            num(et["now"]) if et else "–", drift, f"↑{rev.get('up30', 0)} / ↓{rev.get('down30', 0)}" if rev else "–",
        ))
    out.append(table(["기간", "매출 추정", "성장률", "EPS 추정", "EPS 30일 변화", "30일 리비전 (상향/하향)"], rows))
    if cons:
        r = est.get("recs_now", {})
        out.append(
            f"\n- 투자의견: 애널리스트 {cons.get('number_of_analysts')}명, 평균 {num(cons.get('recommendation_mean'), 2)} "
            f"({cons.get('recommendation')}) — Strong Buy {r.get('strong_buy', '–')} / Buy {r.get('buy', '–')} / Hold {r.get('hold', '–')} / Sell {r.get('sell', '–')}"
        )
        cp = cons.get("current_price") or 1
        out.append(
            f"- 목표가: 평균 ${num(cons.get('target_consensus'))} / 중앙값 ${num(cons.get('target_median'))} / 범위 ${num(cons.get('target_low'))}–{num(cons.get('target_high'))} "
            f"(현재가 대비 평균 {pct((cons.get('target_consensus') or cp) / cp * 100 - 100, 0, True)})"
        )
    return "\n".join(out)


def sec_valuation(sym: str, peers: list[str]) -> str:
    rows = []
    for t in [sym] + [p for p in peers if p != sym]:
        m = D.metrics(t)
        q = D.quote(t)
        if not m:
            continue
        rows.append((
            f"**{t}**" if t == sym else t, money(m.get("market_cap")), num(m.get("enterprise_to_revenue"), 1),
            num(m.get("enterprise_to_ebitda"), 1), num(m.get("forward_pe"), 1), pct((m.get("revenue_growth") or 0) * 100, 0, True),
            pct((m.get("gross_margin") or 0) * 100, 0), pct((m.get("operating_margin") or 0) * 100, 0),
            pct(((q.get("last_price") or 0) / (q.get("year_high") or 1) - 1) * 100, 0) if q else "–",
        ))
    return "## 4. 밸류에이션 & 동종업체\n\n" + table(
        ["티커", "시총", "EV/매출", "EV/EBITDA", "선행 PER", "매출성장(TTM)", "GM", "OPM", "고점 대비"], rows)


def sec_quality(sym: str) -> str:
    tbl = X.quarterly_table(sym, 6)
    if tbl is None or tbl.empty:
        return "## 5. 이익의 질 (SEC XBRL)\n\n데이터 없음"
    qm = X.quality_metrics(tbl)
    keep = [c for c in ["revenue", "net_income", "ocf", "capex", "receivables", "contract_assets", "inventory", "payables", "customer_deposits", "deferred_rev", "cash", "lt_debt"] if c in tbl.columns]
    names = {"revenue": "매출", "net_income": "순이익", "ocf": "OCF", "capex": "Capex", "receivables": "매출채권",
             "contract_assets": "계약자산(미청구)", "inventory": "재고", "payables": "매입채무", "customer_deposits": "고객 선수금",
             "deferred_rev": "이연매출", "cash": "현금", "lt_debt": "장기부채"}
    d = tbl[keep].rename(columns=names).T
    d.columns = [str(c.date()) for c in d.columns]
    rows = [[idx] + [("–" if pd.isna(v) else f"{v:,.0f}") for v in r] for idx, r in d.iterrows()]
    out = ["## 5. 이익의 질 (SEC XBRL, $M)\n", table([""] + list(d.columns), rows), ""]
    q = qm[[c for c in ["unbilled_plus_ar_days", "contract_asset_delta_pct_rev", "ocf_to_ni", "fcf", "payables_delta"] if c in qm.columns]].rename(
        columns={"unbilled_plus_ar_days": "채권+미청구 일수", "contract_asset_delta_pct_rev": "계약자산Δ/매출 %", "ocf_to_ni": "OCF/NI", "fcf": "FCF", "payables_delta": "매입채무Δ"}).T
    q.columns = [str(c.date()) for c in q.columns]
    rows = [[idx] + [("–" if pd.isna(v) else f"{v:,.1f}") for v in r] for idx, r in q.iterrows()]
    out.append(table([""] + list(q.columns), rows))
    out.append("")
    out.extend(f"- {l}" for l in A.quality_verdict(tbl, qm))
    out.append("\n> 계약자산 = 매출로 인식했지만 아직 청구하지 않은 금액. 급증하면 이익과 현금흐름이 벌어진다.")
    return "\n".join(out)


def sec_ownership(sym: str) -> str:
    ss = D.share_stats(sym)
    si = A.short_interest_summary(D.short_interest(sym), ss.get("float_shares"))
    out = ["## 6. 수급\n"]
    if si:
        out.append(f"**공매도 (FINRA, {si['settlement_date']})**: {si['short']:,}주, 유통주식의 {si.get('pct_float', '–')}%, "
                   f"직전 보고 대비 {pct(si['chg_pct'], 1, True)}, 숏커버 {si['days_to_cover']}일")
        out.append("추이: " + " → ".join(f"{d[5:]} {v/1e6:.1f}M" for d, v in si["trend"]))
    if ss:
        out.append(f"\n**보유 구조**: 기관 {pct((ss.get('institution_ownership') or 0) * 100, 1)} ({ss.get('institutions_count')}곳), "
                   f"내부자 {pct((ss.get('insider_ownership') or 0) * 100, 1)}, 유통주식 {money(ss.get('float_shares'))[1:]}주")
    inst = D.institutional(sym)
    if inst is not None and not inst.empty:
        rows = [(r.holder.strip()[:40], pct(r.percent_held * 100, 1), pct(r.percent_change * 100, 0, True) if pd.notna(r.percent_change) else "–")
                for r in inst.head(8).itertuples()]
        out.append(f"\n**주요 기관 (13F {inst.iloc[0].date_reported})**\n" + table(["보유자", "지분", "QoQ"], rows))
    ins = A.insider_rows(D.insider_trading_sec(sym, 40), limit=40)
    if ins:
        cut = (date.today() - pd.Timedelta(days=180)).isoformat()
        recent = [r for r in ins if r["filing_date"] >= cut]
        sells = sum(r["value"] for r in recent if r["kind"] == "매도")
        buys = sum(r["value"] for r in recent if r["kind"] == "매수/취득")
        out.append(f"\n**내부자 (SEC Form 4, 최근 6개월)**: 장내 매도 ${sells/1e6:,.1f}M / 장내 매수 ${buys/1e6:,.1f}M, 거래 {len(recent)}건")
        rows = [(r["filing_date"], r["name"], (r["title"] if r["title"] not in ("nan", "None") else "Director")[:22], r["kind"], f"{r['shares']:,}", f"${r['price']}", f"${r['value']/1e6:,.2f}M" if r["value"] else "–",
                 "10b5-1" if r["rule_10b5_1"] else "")
                for r in recent[:12] if r["kind"] in ("매도", "매수/취득")]
        if rows:
            out.append(table(["공시일", "이름", "직책", "구분", "주식수", "가격", "금액", ""], rows))
    return "\n".join(out)


def sec_options(sym: str, spot: float) -> str:
    ed = D.next_earnings_date(sym)
    o = A.options_summary(D.option_chains(sym), spot, ed)
    if not o:
        return "## 7. 옵션 시장\n\n데이터 없음"
    out = ["## 7. 옵션 시장\n",
           f"- Put/Call: 미결제약정 {o['pcr_oi']} / 거래량 {o['pcr_vol']} (총 OI {o['total_oi']:,})",
           f"- 30일 ATM IV {o.get('iv30')}% → 한 달 예상 변동폭 ±{o.get('move_30d_pct')}%"]
    if ed:
        out.append(f"- 다음 실적 {ed} (D-{A.days_until(ed)}): 실적 포함 만기({o.get('earnings_expiry')}) 스트래들 기준 ±{o.get('earnings_move_pct')}%"
                   + (f", 실적 이벤트만의 예상 변동 ≈ ±{o['event_move_pct']}%" if o.get("event_move_pct") else ""))
    if o.get("call_walls"):
        out.append("- 콜 OI 집중(상방 저항 후보): " + ", ".join(f"${k:.0f} ({oi:,})" for k, oi in o["call_walls"]))
    if o.get("put_walls"):
        out.append("- 풋 OI 집중(하방 지지/헤지): " + ", ".join(f"${k:.0f} ({oi:,})" for k, oi in o["put_walls"]))
    term = o["term"].head(8)
    out.append("\n" + table(["만기", "DTE", "ATM IV", "스트래들", "예상 변동폭"],
                            [(r.expiration, r.dte, f"{r.iv}%", f"${r.straddle}", f"±{r.implied_move_pct}%") for r in term.itertuples()]))
    return "\n".join(out)


def sec_relative(sym: str, peers: list[str], closes: pd.DataFrame) -> str:
    basket = [p for p in peers if p in closes.columns and p != sym]
    out = ["## 8. 상대 성과 / 움직임 분해\n"]
    rel = A.relative_table(closes)
    out.append(table(["티커", "1일", "5일", "1개월", "3개월", "1년 고점 대비"],
                     [(i, pct(r["1d"], 1, True), pct(r["5d"], 1, True), pct(r["1m"], 1, True), pct(r["3m"], 1, True), pct(r["from_1y_high"], 1)) for i, r in rel.iterrows()]))
    if basket and "QQQ" in closes.columns:
        md = A.move_decomposition(closes[sym], closes[basket], closes["QQQ"])
        if md:
            out.append(f"\n**최근 거래일 분해** (120일 회귀, 바스켓 {','.join(basket)} + QQQ, R² {md['r2']}): 실제 {pct(md['actual_pct'], 2, True)} = "
                       f"예상 {pct(md['expected_pct'], 2, True)} (β바스켓 {md['beta_basket']}) + 고유 잔차 **{pct(md['residual_pct'], 2, True)}** (z={md['residual_z']}) → {md['label']}")
            out.append(f"- 고유 잔차 누적: 5일 {pct(md['residual_5d_pct'], 1, True)}, 20일 {pct(md['residual_20d_pct'], 1, True)} "
                       f"(양수면 섹터보다 {sym} 자체가 재평가되는 중, 음수면 {sym} 고유 리스크가 가격에 반영되는 중)")
    return "\n".join(out)


KEY_PATTERNS = [
    ("수주/계약 의무", r"remaining performance obligation|unsatisfied performance obligation|backlog"),
    ("고객 집중도", r"accounted for approximately .{0,40}% .{0,40}(revenue|accounts receivable)"),
    ("구매 약정", r"(purchase )?commitments .{0,120}totaled approximately"),
    ("소송/공매도", r"class action|securities litigation|short[- ]seller"),
    ("자금조달/희석", r"at-the-market|ATM program|shelf registration|equity offering|convertible .{0,30}notes? due"),
    ("계속기업/유동성", r"going concern|sufficient to meet our anticipated cash"),
]


def _sentences(text: str) -> list[str]:
    # split on sentence ends that are not decimals ("$5.8 billion") and not abbreviations like "No."
    parts = re.split(r"(?<=[a-z\)%])\.(?!\d)\s+(?=[A-Z(•])", text)
    return [re.sub(r"\s+", " ", p).strip() for p in parts]


def key_sentences(text: str, max_per: int = 3) -> list[tuple[str, list[str]]]:
    sents = _sentences(text)
    out = []
    for name, pat in KEY_PATTERNS:
        hits = [s for s in sents if re.search(pat, s, flags=re.I) and 60 < len(s) < 700]
        # prefer sentences that carry numbers
        hits.sort(key=lambda s: (("$" in s or "%" in s) is False, len(s)))
        uniq = []
        for h in hits:
            if h[:80] not in {u[:80] for u in uniq}:
                uniq.append(h if h.endswith(".") else h + ".")
            if len(uniq) >= max_per:
                break
        if uniq:
            out.append((name, uniq))
    return out


def sec_filings(sym: str, ai: bool) -> str:
    f = D.company_filings(sym)
    if f is None or f.empty:
        return "## 9. SEC 공시\n\n데이터 없음"
    out = ["## 9. SEC 공시\n"]
    recent = f.head(12)
    rows = []
    for r in recent.itertuples():
        sev, label = A.classify_filing(r.report_type, str(getattr(r, "items", "") or ""), str(getattr(r, "primary_doc_description", "") or ""))
        rows.append((r.filing_date.date().isoformat(), f"{sev} {label}"[:90], f"[link]({r.report_url})"))
    out.append(table(["제출일", "분류", ""], rows))
    periodic = f[f.report_type.isin(["10-Q", "10-K"])]
    if periodic.empty:
        return "\n".join(out)
    latest = periodic.iloc[0]
    text = D.htm_text(latest.report_url)
    out.append(f"\n### 최신 정기 보고서: {latest.report_type} ({latest.filing_date.date()}) 핵심 문장 자동 추출\n")
    found = False
    for name, hits in key_sentences(text):
        found = True
        out.append(f"**{name}**")
        out.extend(f"> {h}" for h in hits)
        out.append("")
    if not found:
        out.append("(패턴 매칭 결과 없음)")
    # guidance from the latest earnings 8-K
    ek = f[(f.report_type == "8-K") & f["items"].astype(str).str.contains("2.02")]
    if not ek.empty:
        idx_url = ek.iloc[0].filing_detail_url if "filing_detail_url" in ek.columns else ek.iloc[0].report_url
        docs = [d for d in D.filing_index_docs(str(idx_url)) if "99" in d.rsplit("/", 1)[-1]]
        if docs:
            pr = D.htm_text(docs[0])
            g = re.search(r"\n\s*((?:financial |full[- ]year )?(?:guidance|outlook)[^\n]{0,60})\n([\s\S]{0,700})", pr, flags=re.I)
            if not g:
                g = re.search(r"((?:raises?|reaffirms?|updates?|lowers?)[^.]{0,120}guidance[^.]{0,200}\.)", pr, flags=re.I)
            if g:
                snippet = re.sub(r"\s*\|\s*", " | ", g.group(0))
                snippet = re.sub(r"(\s*\|\s*){2,}", " | ", snippet)
                snippet = re.sub(r"\s+", " ", snippet)
                snippet = re.split(r"Investor Conference|Conference Call|Webcast Details|Forward-Looking", snippet)[0][:700]
                out.append(f"**실적 8-K 가이던스 발췌 ({ek.iloc[0].filing_date.date()})**\n> {snippet}\n")
    if ai:
        summ = ai_summary(sym, latest.report_type, text)
        if summ:
            out.append("### Claude 요약 (10-Q/10-K MD&A·주석)\n" + summ)
    return "\n".join(out)


def ai_summary(sym: str, form: str, text: str) -> str:
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        log.warning("ANTHROPIC_API_KEY not set — skipping AI summary")
        return ""
    import requests

    # MD&A + notes are the informative part; cap size
    start = text.upper().find("MANAGEMENT’S DISCUSSION")
    body = text[start:start + 60000] if start > 0 else text[:60000]
    prompt = (
        f"다음은 {sym}의 {form} 원문 일부입니다. 투자자가 '이번 분기에 새로 알아야 할 것'을 10줄 이내 한국어 불릿으로 정리하세요. "
        "숫자는 원문 그대로 인용하고, 추측·투자 권유는 하지 마세요. 특히 (1) 매출 인식과 현금흐름의 괴리, (2) 고객 집중도, "
        "(3) 구매·금융 약정, (4) 희석 요인, (5) 회사가 직접 밝힌 신규 위험을 우선하세요.\n\n" + body
    )
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": os.environ.get("CLAUDE_MODEL", "claude-sonnet-5-5"), "max_tokens": 1200,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=120,
        )
        r.raise_for_status()
        return "".join(b.get("text", "") for b in r.json().get("content", []))
    except Exception as e:  # noqa: BLE001
        log.warning("AI summary failed: %s", e)
        return ""


def sec_news(sym: str) -> str:
    rows = A.news_rows(D.news(sym), 15)
    if not rows:
        return "## 10. 최근 뉴스\n\n데이터 없음"
    return "## 10. 최근 뉴스 (헤드라인, 본문 미검증)\n\n" + "\n".join(f"- {r['date']} [{r['title']}]({r['url']})" if r["url"] else f"- {r['date']} {r['title']}" for r in rows)


# ───────────────────────── main ─────────────────────────
def build(sym: str, peers: list[str], do_sec: bool, ai: bool) -> str:
    q = D.quote(sym)
    prof = D.profile(sym)
    hist = D.history(sym, 420)
    if hist is None or hist.empty or not q:
        sys.exit(f"가격 데이터를 받지 못했습니다: {sym}")
    t = A.technicals(hist)
    m = D.metrics(sym)
    closes = D.histories(sorted(set([sym] + peers + ["QQQ"])), 420)
    head = [
        f"# {prof.get('name', sym)} (${sym}) 리서치 노트 — {date.today()}",
        "",
        f"> 데이터: OpenBB Platform (yfinance / SEC / FINRA), {t['date']} 종가 기준. `research.py`가 자동 생성. 투자 조언이 아닌 데이터 정리입니다.",
        "",
        f"**{prof.get('sector', '')} · {prof.get('industry_category', '')} · 시총 {money(m.get('market_cap'))} · 직원 {prof.get('employees', '–')}**  ",
        (prof.get("long_description") or "")[:400] + ("…" if len(prof.get("long_description") or "") > 400 else ""),
        "",
        f"| 주가 | 고점 대비 | 선행 PER | EV/매출 | 매출성장(TTM) | GM / OPM | 베타 |",
        f"|---|---|---|---|---|---|---|",
        f"| ${t['last']} ({pct(t['chg_pct'], 1, True)}) | {pct(t['drawdown_pct'])} | {num(m.get('forward_pe'), 1)} | {num(m.get('enterprise_to_revenue'), 1)} | "
        f"{pct((m.get('revenue_growth') or 0) * 100, 0, True)} | {pct((m.get('gross_margin') or 0) * 100, 0)} / {pct((m.get('operating_margin') or 0) * 100, 0)} | {num(m.get('beta'), 2)} |",
        "",
    ]
    parts = [
        "\n".join(head),
        sec_price(sym, hist, m, t),
        sec_financials(sym),
        sec_estimates(sym),
        sec_valuation(sym, peers),
        sec_quality(sym) if do_sec else "",
        sec_ownership(sym),
        sec_options(sym, t["last"]),
        sec_relative(sym, peers, closes),
        sec_filings(sym, ai) if do_sec else "",
        sec_news(sym),
        "---\n*재현: `.venv/bin/python research.py " + sym + (" --peers " + ",".join(peers) if peers else "") + "`*",
    ]
    return "\n\n".join(p for p in parts if p)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("symbol")
    ap.add_argument("--peers", default="", help="comma-separated peer tickers (default: <sym>_config.json peers or none)")
    ap.add_argument("--no-sec", action="store_true", help="skip SEC XBRL / filing text sections (faster)")
    ap.add_argument("--ai", action="store_true", help="add Claude summary of the latest 10-Q/10-K (ANTHROPIC_API_KEY)")
    ap.add_argument("--out", default="", help="output path (default research/<SYM>_<date>.md)")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO if a.verbose else logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    sym = a.symbol.upper()
    peers = [p.strip().upper() for p in a.peers.split(",") if p.strip()] or _cfg_peers(sym)
    md = build(sym, peers, not a.no_sec, a.ai)
    out = Path(a.out) if a.out else ROOT / "research" / f"{sym}_{date.today()}.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")
    print(f"written: {out.relative_to(ROOT) if out.is_relative_to(ROOT) else out}")


if __name__ == "__main__":
    main()
