#!/usr/bin/env python
"""
$BE Insight Monitor — OpenBB-based Slack bot for a concentrated position.

Modes (RUN_MODE env or argv[1]):
  close     daily after US close: move decomposition, options/IV, key levels, customer proxy,
            new filings/insiders/news, event countdown
  filings   intraday (hourly): only new SEC filings / Form 4 / Form 144 → alert if anything new
  weekly    Monday morning KST: regime summary, estimate drift, SI trend, insider net, calendar
  quarterly earnings-quality checkpoint from the latest 10-Q/10-K (auto-fires when a new one appears)

Config: be_config.json (symbol, peers, key levels, watch dates, alert thresholds)
State:  be_state.json (seen filings / insiders / news, last snapshot)
Env:    SLACK_WEBHOOK_URL (optional → prints to stdout), ANTHROPIC_API_KEY (optional, weekly/quarterly AI note),
        POSITION_SHARES / POSITION_COST (optional, shows $ impact), MONITOR_CONFIG (path, default be_config.json)

Signals are data interpretation, not investment advice.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

from obb_tools import analysis as A
from obb_tools import data as D
from obb_tools import sec_xbrl as X
from obb_tools.report import money, num, pct

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("be_monitor")
for noisy in ("openbb", "openbb.sec", "urllib3", "yfinance", "peewee"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

ROOT = Path(__file__).resolve().parent
KST = timezone(timedelta(hours=9))
CFG = json.loads((ROOT / os.environ.get("MONITOR_CONFIG", "be_config.json")).read_text())
SYM = CFG["symbol"]
STATE_PATH = ROOT / f"{SYM.lower()}_state.json"
SLACK = os.environ.get("SLACK_WEBHOOK_URL", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
POS_SHARES = float(os.environ.get("POSITION_SHARES", "0") or 0)
POS_COST = float(os.environ.get("POSITION_COST", "0") or 0)
AL = CFG.get("alerts", {})
RUN_TAG = os.environ.get("RUN_TAG", "").strip()  # e.g. "테스트" → prefixes every header


# ───────────────────────── state ─────────────────────────
def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception:  # noqa: BLE001
            pass
    return {"seen_filings": [], "seen_insiders": [], "seen_news": [], "snapshots": [], "last_quality_period": ""}


def save_state(st: dict):
    for k in ("seen_filings", "seen_insiders", "seen_news"):
        st[k] = st.get(k, [])[-400:]
    st["snapshots"] = st.get("snapshots", [])[-120:]
    STATE_PATH.write_text(json.dumps(st, ensure_ascii=False, indent=1, default=str))


# ───────────────────────── slack ─────────────────────────
def sec(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text[:2900]}}


def ctx(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text[:2900]}]}


def header(text: str) -> dict:
    if RUN_TAG:
        text = f"[{RUN_TAG}] {text}"
    return {"type": "header", "text": {"type": "plain_text", "text": text[:150]}}


def footer(mode: str) -> list:
    return [{"type": "divider"}, ctx(f"🤖 {SYM} Insight Monitor · {mode} · {datetime.now(KST):%m/%d %H:%M KST} · OpenBB(yfinance/SEC/FINRA) · 데이터 해석이며 투자 조언이 아닙니다")]


def send(blocks: list, text: str):
    blocks = blocks[:48]
    if not SLACK:
        log.warning("SLACK_WEBHOOK_URL not set — printing")
        for b in blocks:
            t = b.get("text", {}).get("text") if isinstance(b.get("text"), dict) else None
            if not t and b.get("elements"):
                t = b["elements"][0].get("text")
            if t:
                print(t)
        return
    r = requests.post(SLACK, json={"text": text, "blocks": blocks}, timeout=15)
    if r.status_code != 200:
        log.error("Slack %s %s", r.status_code, r.text[:200])
    else:
        log.info("Slack sent")


# ───────────────────────── context gathering ─────────────────────────
def gather_market() -> dict:
    peers = CFG.get("peers", [])
    bench = CFG.get("benchmark", "QQQ")
    proxies = list(CFG.get("customer_proxies", {}).keys())
    syms = sorted(set([SYM] + peers + [bench] + proxies))
    closes = D.histories(syms, 420)
    hist = D.history(SYM, 420)
    if hist is None or hist.empty:
        sys.exit("no price history")
    t = A.technicals(hist)
    basket = [p for p in peers if p in closes.columns]
    md = A.move_decomposition(closes[SYM], closes[basket], closes.get(bench)) if basket else {}
    rel = A.relative_table(closes)
    ed = D.next_earnings_date(SYM)
    opt = A.options_summary(D.option_chains(SYM), t["last"], ed)
    ss = D.share_stats(SYM)
    si = A.short_interest_summary(D.short_interest(SYM), ss.get("float_shares"))
    conv = {}
    kl = CFG.get("key_levels", {})
    for k, v in kl.items():
        if "conv_price" in v:
            conv[k] = A.convertible_watch(hist["close"], v["conv_price"], v.get("redemption_trigger_mult", 1.3))
            conv[k]["max_shares_m"] = v.get("max_shares_m")
    return {"hist": hist, "closes": closes, "tech": t, "md": md, "rel": rel, "earnings": ed, "opt": opt,
            "si": si, "ss": ss, "conv": conv, "proxies": CFG.get("customer_proxies", {})}


def gather_filings(st: dict) -> dict:
    f = D.company_filings(SYM)
    new_filings, periodic_new = [], None
    if f is not None and not f.empty:
        seen = set(st.get("seen_filings", []))
        recent = f[f.filing_date >= pd.Timestamp(date.today() - timedelta(days=14))]
        for r in recent.itertuples():
            acc = str(getattr(r, "accession_number", "") or r.report_url)
            if acc in seen:
                continue
            sev, label = A.classify_filing(r.report_type, str(getattr(r, "items", "") or ""), str(getattr(r, "primary_doc_description", "") or ""))
            new_filings.append({"acc": acc, "date": r.filing_date.date().isoformat(), "type": r.report_type, "sev": sev, "label": label, "url": r.report_url})
            if r.report_type in ("10-Q", "10-K") and periodic_new is None:
                periodic_new = r.report_url
    ins = A.insider_rows(D.insider_trading_sec(SYM, 30), limit=30)
    seen_i = set(st.get("seen_insiders", []))
    new_ins = [r for r in ins if r["key"] not in seen_i and r["filing_date"] >= (date.today() - timedelta(days=21)).isoformat()]
    return {"filings": new_filings, "insiders": new_ins, "all_insiders": ins, "periodic_new": periodic_new}


def gather_news(st: dict, limit: int = 8) -> list[dict]:
    rows = A.news_rows(D.news(SYM), 25)
    seen = set(st.get("seen_news", []))
    kws = [k.lower() for k in CFG.get("news_keywords", [])] + [SYM.lower(), CFG.get("name", "").lower()]
    out = []
    for r in rows:
        if r["key"] in seen:
            continue
        hit = any(k and k in r["title"].lower() for k in kws)
        r["relevant"] = hit
        out.append(r)
    out.sort(key=lambda r: (not r["relevant"], r["date"]), reverse=False)
    return out[:limit]


# ───────────────────────── insight engine ─────────────────────────
def insights(m: dict, st: dict, fil: dict) -> list[str]:
    """Rule-based, ranked list of 'what is unusual today' lines (most important first)."""
    t, md, opt, si, conv = m["tech"], m["md"], m["opt"], m["si"], m["conv"]
    prev = st["snapshots"][-1] if st.get("snapshots") else {}
    out: list[tuple[int, str]] = []  # (priority, text) lower = more important

    # 0. filings that need action
    for f in fil["filings"]:
        if f["sev"] == "🚨":
            out.append((0, f"🚨 *새 공시* {f['date']} {f['label']} <{f['url']}|열기>"))
    # 1. move decomposition
    if md:
        z = md["residual_z"]
        line = (f"오늘 {pct(md['actual_pct'], 1, True)} = 섹터 예상 {pct(md['expected_pct'], 1, True)} "
                f"(바스켓 {pct(md['basket_pct'], 1, True)}×β{md['beta_basket']}) + *고유 {pct(md['residual_pct'], 1, True)}* (z {z:+.1f})")
        if abs(z) >= AL.get("residual_z", 1.5):
            out.append((1, f"⚡ *섹터로 설명 안 되는 움직임* — {line}. 뉴스/공시에서 BE 고유 원인을 찾아볼 것"))
        elif abs(md["actual_pct"]) >= AL.get("price_move_pct", 5):
            out.append((2, f"📊 큰 변동이지만 대부분 섹터 요인 — {line}"))
        else:
            out.append((6, f"📊 {line} → {md['label']}"))
        r20 = md["residual_20d_pct"]
        if abs(r20) >= 8:
            tone = "시장이 BE 고유 리스크를 반영하는 중" if r20 < 0 else "섹터와 무관하게 BE 자체가 재평가되는 중"
            out.append((2, f"🧭 20일 누적 고유 잔차 {pct(r20, 1, True)} → {tone} (5일 {pct(md['residual_5d_pct'], 1, True)})"))
    # 2. customer proxy
    rel = m["rel"]
    for sym_p, desc in m["proxies"].items():
        if sym_p in rel.index:
            r = rel.loc[sym_p]
            flag = ""
            if r["5d"] <= -7 and rel.loc[SYM]["5d"] <= -7:
                flag = " ⚠️ 동반 약세 — 수요가 아니라 고객의 자금조달/신용 우려일 수 있음"
            elif r["1d"] <= -4:
                flag = " ⚠️ 프록시 급락"
            if flag or abs(r["1d"]) >= 3:
                out.append((3 if flag else 7, f"🏢 {sym_p} 1일 {pct(r['1d'], 1, True)} / 5일 {pct(r['5d'], 1, True)} ({desc.split(' — ')[0]}){flag}"))
    # 3. options / IV
    if opt:
        iv30, prev_iv = opt.get("iv30"), prev.get("iv30")
        if iv30 and prev_iv and abs(iv30 - prev_iv) >= AL.get("iv30_jump_pts", 10):
            out.append((2, f"🎯 30일 IV {prev_iv:.0f}% → {iv30:.0f}% ({iv30 - prev_iv:+.0f}p) — 가격 이상의 이벤트 대비 포지셔닝 변화"))
        if iv30 and t.get("rv20"):
            spread = iv30 - t["rv20"]
            cheap = spread < -10
            out.append((5, f"🎯 30일 IV {iv30:.0f}% vs 20일 실현변동성 {t['rv20']:.0f}% ({spread:+.0f}p) → 옵션이 {'실현치보다 싸다 (헤지 비용 낮음)' if cheap else ('실현치보다 비싸다' if spread > 10 else '적정 수준')}; 한 달 예상 변동폭 ±{opt.get('move_30d_pct')}%"))
        if opt.get("pcr_oi") and prev.get("pcr_oi") and abs(opt["pcr_oi"] - prev["pcr_oi"]) >= 0.15:
            out.append((4, f"🎯 Put/Call(OI) {prev['pcr_oi']} → {opt['pcr_oi']} — 헤지 수요 {'증가' if opt['pcr_oi'] > prev['pcr_oi'] else '감소'}"))
        if opt.get("earnings_move_pct") and m["earnings"]:
            out.append((6, f"📅 실적 {m['earnings']} (D-{A.days_until(m['earnings'])}): 옵션 내재 변동폭 ±{opt['earnings_move_pct']}%"
                        + (f", 이벤트 자체 ≈ ±{opt['event_move_pct']}%" if opt.get("event_move_pct") else "")))
    # 4. key levels
    for k, c in conv.items():
        if prev.get("in_the_money") is not None and prev.get("in_the_money") != c["in_the_money"]:
            out.append((1, f"💱 전환사채 전환가 ${c['conv_price']} {'상향 돌파 — 전환 시 최대 ' + str(c['max_shares_m']) + 'M주 희석 구간 진입' if c['in_the_money'] else '하향 이탈 — 전환 유인 소멸'}"))
        if c["days_above_trigger_30"] >= 15 or c["in_the_money"]:
            out.append((3, f"💱 전환사채: 전환가 대비 {pct(c['dist_conv_pct'], 1, True)}, 강제상환 트리거 ${c['trigger']} 대비 {pct(c['dist_trigger_pct'], 1, True)} (최근 30일 중 {c['days_above_trigger_30']}일 상회, 조건 20일)"))
    last, ma200, ma50 = t["last"], t.get("ma200"), t.get("ma50")
    pl = prev.get("last")
    if ma200 and pl and (pl - ma200) * (last - ma200) < 0:
        out.append((1, f"📉 200일 이평(${ma200}) {'상향 돌파' if last > ma200 else '하향 이탈'}"))
    if last <= t["lo_20d"] * 1.001:
        out.append((3, f"📉 20일 신저가 (${t['lo_20d']}). 52주 고점 대비 {pct(t['drawdown_pct'])}, RSI {t['rsi14']}"))
    if t.get("rsi14") and (t["rsi14"] <= 30 or t["rsi14"] >= 70):
        out.append((4, f"📈 RSI(14) {t['rsi14']} — {'과매도' if t['rsi14'] <= 30 else '과매수'} 구간"))
    # 5. short interest (new report)
    if si and si.get("settlement_date") and si["settlement_date"] != prev.get("si_date"):
        tone = "⚠️" if si.get("chg_pct") and abs(si["chg_pct"]) >= AL.get("short_interest_chg_pct", 10) else "·"
        out.append((2 if tone == "⚠️" else 5, f"{tone} 공매도 신규 보고 ({si['settlement_date']}): {si['short']/1e6:.1f}M주 ({pct(si['chg_pct'], 1, True)}), 유통주식 {si.get('pct_float')}%, 숏커버 {si['days_to_cover']}일"))
    # 6. insiders / 144
    sells = [r for r in fil["insiders"] if r["kind"] == "매도"]
    buys = [r for r in fil["insiders"] if r["kind"] == "매수/취득"]
    if buys:
        out.append((1, "🟢 *내부자 장내 매수* " + "; ".join(f"{r['name']} {r['shares']:,}주 @${r['price']}" for r in buys[:3])))
    if sells:
        tot = sum(r["value"] for r in sells)
        out.append((3, f"🔴 내부자 매도 {len(sells)}건 ${tot/1e6:.1f}M — " + "; ".join(f"{r['name']}({r['title'][:12]}) {r['shares']:,}주 @${r['price']}{' 10b5-1' if r['rule_10b5_1'] else ''}" for r in sells[:4])))
    f144 = [f for f in fil["filings"] if f["type"] == "144"]
    if f144:
        out.append((3, f"🟠 Form 144 {len(f144)}건 — 내부자 매도 예정 통지 (Form 4보다 먼저 나오는 선행 신호) " + " ".join(f"<{f['url']}|{f['date']}>" for f in f144[:3])))
    for f in fil["filings"]:
        if f["sev"] == "⚠️" and f["type"] != "144":
            out.append((2, f"⚠️ 새 공시 {f['date']} {f['label']} <{f['url']}|열기>"))
    # 7. volume
    if t.get("vol_ratio") and t["vol_ratio"] >= 2:
        out.append((4, f"📦 거래량 20일 평균의 {t['vol_ratio']}x"))
    out.sort(key=lambda x: x[0])
    return [x[1] for x in out]


def position_line(t: dict, opt: dict) -> str:
    if not POS_SHARES:
        return ""
    val = POS_SHARES * t["last"]
    day = POS_SHARES * t["last"] * (t["chg_pct"] or 0) / 100
    s = f"💼 포지션 {POS_SHARES:,.0f}주 = ${val:,.0f} (오늘 {day:+,.0f})"
    if POS_COST:
        s += f", 평단 ${POS_COST} 대비 {pct((t['last'] / POS_COST - 1) * 100, 1, True)}"
    if opt.get("move_30d_pct"):
        s += f" · 옵션 내재 1개월 변동폭 ±${val * opt['move_30d_pct'] / 100:,.0f}"
    return s


def _dday(d: date) -> str:
    n = (d - date.today()).days
    return f"D-{n}" if n > 0 else ("D-Day" if n == 0 else f"D+{-n}")


def calendar_lines(m: dict) -> list[str]:
    items: dict[str, str] = {}
    for w in CFG.get("watch_dates", []):
        d = date.fromisoformat(w["date"])
        if d >= date.today() - timedelta(days=3):
            items[w["date"]] = f"{w['date']} {w['label']} ({_dday(d)})"
    if m["earnings"] and m["earnings"].isoformat() not in items:
        items[m["earnings"].isoformat()] = f"{m['earnings']} 실적 발표 ({_dday(m['earnings'])})"
    return [items[k] for k in sorted(items)]


def snapshot(m: dict) -> dict:
    t, opt, si, conv = m["tech"], m["opt"], m["si"], m["conv"]
    c0 = next(iter(conv.values()), {})
    return {"date": t["date"], "last": t["last"], "chg_pct": t["chg_pct"], "rsi": t["rsi14"], "rv20": t["rv20"],
            "iv30": opt.get("iv30"), "pcr_oi": opt.get("pcr_oi"), "move_30d_pct": opt.get("move_30d_pct"),
            "residual_pct": m["md"].get("residual_pct"), "residual_20d_pct": m["md"].get("residual_20d_pct"),
            "si_date": si.get("settlement_date"), "si_short": si.get("short"), "in_the_money": c0.get("in_the_money"),
            "ma200": t.get("ma200"), "drawdown_pct": t["drawdown_pct"]}


def mark_seen(st: dict, fil: dict, news: list[dict]):
    st["seen_filings"] = st.get("seen_filings", []) + [f["acc"] for f in fil["filings"]]
    st["seen_insiders"] = st.get("seen_insiders", []) + [r["key"] for r in fil["insiders"]]
    st["seen_news"] = st.get("seen_news", []) + [n["key"] for n in news]


# ───────────────────────── modes ─────────────────────────
def run_close():
    st = load_state()
    m = gather_market()
    fil = gather_filings(st)
    news = gather_news(st)
    t, md, opt, rel = m["tech"], m["md"], m["opt"], m["rel"]
    ins = insights(m, st, fil)
    emoji = "🟢" if (t["chg_pct"] or 0) >= 0 else "🔴"
    blocks = [header(f"{emoji} ${SYM} ${t['last']} ({pct(t['chg_pct'], 1, True)}) · {t['date']}")]
    blocks.append(sec("*오늘의 인사이트*\n" + "\n".join(f"• {l}" for l in ins[:8])))
    pos = position_line(t, opt)
    if pos:
        blocks.append(ctx(pos))
    # levels & relative
    lv = (f"52주 고점 ${t['hi_52w']} ({pct(t['drawdown_pct'])}) · 20일 저가 ${t['lo_20d']} · 50일 ${t['ma50']} · 200일 ${t['ma200']} · RSI {t['rsi14']} · 거래량 {t['vol_ratio']}x")
    if opt.get("put_walls") or opt.get("call_walls"):
        lv += ("\n옵션 OI 벽: 풋 " + ", ".join(f"${k:.0f}" for k, _ in opt.get("put_walls", [])) + " / 콜 " + ", ".join(f"${k:.0f}" for k, _ in opt.get("call_walls", [])))
    blocks.append(sec(f"*레벨*\n{lv}"))
    peers_line = " · ".join(f"{s} {pct(rel.loc[s]['1d'], 1, True)}" for s in rel.index if s != SYM)
    blocks.append(ctx(f"*관련 종목 1일*: {peers_line}\n*5일*: {SYM} {pct(rel.loc[SYM]['5d'], 1, True)} vs 바스켓 평균 {pct(rel.drop(index=[SYM] + [s for s in CFG.get('customer_proxies', {}) if s in rel.index], errors='ignore')['5d'].mean(), 1, True)}"))
    # filings + insiders (non-alert listing)
    if fil["filings"]:
        blocks.append(sec("*새 SEC 공시*\n" + "\n".join(f"{f['sev']} {f['date']} <{f['url']}|{f['label'][:80]}>" for f in fil["filings"][:8])))
    # news
    if news:
        blocks.append(sec("*뉴스 (헤드라인, 본문 미검증)*\n" + "\n".join(f"{'•' if n['relevant'] else '·'} <{n['url']}|{n['title'][:110]}>" if n["url"] else f"• {n['title'][:110]}" for n in news)))
    cal = calendar_lines(m)
    if cal:
        blocks.append(ctx("*일정*: " + " | ".join(cal[:4])))
    blocks.extend(footer("close"))
    send(blocks, f"{SYM} {t['last']} {pct(t['chg_pct'], 1, True)} — {ins[0] if ins else ''}")
    # state
    mark_seen(st, fil, news)
    st["snapshots"].append(snapshot(m))
    save_state(st)
    if fil["periodic_new"] and st.get("last_quality_period") != fil["periodic_new"]:
        run_quarterly(st, trigger_url=fil["periodic_new"])


def run_filings():
    st = load_state()
    fil = gather_filings(st)
    if not fil["filings"] and not fil["insiders"]:
        log.info("no new filings")
        return
    blocks = [header(f"📑 ${SYM} 새 공시 {len(fil['filings'])}건 / Form 4 {len(fil['insiders'])}건")]
    if fil["filings"]:
        fil["filings"].sort(key=lambda f: {"🚨": 0, "⚠️": 1, "📄": 2, "·": 3}[f["sev"]])
        blocks.append(sec("\n".join(f"{f['sev']} {f['date']} *{f['type']}* — {f['label']} <{f['url']}|열기>" for f in fil["filings"][:12])))
    if fil["insiders"]:
        blocks.append(sec("*Form 4 상세*\n" + "\n".join(
            f"{'🔴' if r['kind'] == '매도' else ('🟢' if r['kind'] == '매수/취득' else '·')} {r['tx_date']} {r['name']} ({r['title'][:20] or '-'}) {r['kind']} {r['shares']:,}주 @${r['price']} = ${r['value']/1e6:.2f}M{' [10b5-1]' if r['rule_10b5_1'] else ''} <{r['url']}|4>"
            for r in fil["insiders"][:10])))
    blocks.extend(footer("filings"))
    send(blocks, f"{SYM} 새 공시 {len(fil['filings'])}건")
    mark_seen(st, fil, [])
    save_state(st)
    if fil["periodic_new"] and st.get("last_quality_period") != fil["periodic_new"]:
        run_quarterly(st, trigger_url=fil["periodic_new"])


def run_weekly():
    st = load_state()
    m = gather_market()
    t, md, opt, rel, si = m["tech"], m["md"], m["opt"], m["rel"], m["si"]
    est = A.estimates_summary(D.eps_trend(SYM), D.eps_revisions(SYM), D.recommendations(SYM), D.consensus(SYM), D.revenue_estimates(SYM))
    ins_all = A.insider_rows(D.insider_trading_sec(SYM, 40), limit=40)
    cut = (date.today() - timedelta(days=30)).isoformat()
    rec_ins = [r for r in ins_all if r["filing_date"] >= cut]
    snaps = st.get("snapshots", [])
    week_ago = next((s for s in reversed(snaps) if s["date"] <= (date.today() - timedelta(days=6)).isoformat()), {})
    blocks = [header(f"📅 ${SYM} 주간 브리핑 · ${t['last']} · 주간 {pct(rel.loc[SYM]['5d'], 1, True)}")]
    # regime
    basket_5d = rel.drop(index=[SYM] + [s for s in CFG.get("customer_proxies", {}) if s in rel.index], errors="ignore")["5d"].mean()
    regime = [f"주간 {pct(rel.loc[SYM]['5d'], 1, True)} vs 바스켓 {pct(basket_5d, 1, True)} vs {CFG.get('benchmark')} {pct(rel.loc[CFG.get('benchmark')]['5d'], 1, True) if CFG.get('benchmark') in rel.index else '–'}"]
    if md:
        regime.append(f"고유 잔차 누적 5일 {pct(md['residual_5d_pct'], 1, True)} / 20일 {pct(md['residual_20d_pct'], 1, True)} → {'BE 고유 리스크 반영 중' if md['residual_20d_pct'] < -5 else ('BE 자체 재평가 중' if md['residual_20d_pct'] > 5 else '섹터 추종')}")
    if opt.get("iv30"):
        regime.append(f"30일 IV {opt['iv30']:.0f}%" + (f" (주초 {week_ago['iv30']:.0f}%)" if week_ago.get("iv30") else "") + f", 실현 20d {t['rv20']:.0f}%, Put/Call(OI) {opt.get('pcr_oi')}")
    regime.append(f"52주 고점 대비 {pct(t['drawdown_pct'])}, 200일선 대비 {pct((t['last'] / t['ma200'] - 1) * 100, 1, True) if t.get('ma200') else '–'}, RSI {t['rsi14']}")
    blocks.append(sec("*레짐*\n" + "\n".join(f"• {r}" for r in regime)))
    # estimates drift
    et = est.get("eps_trend", {})
    rv = est.get("revisions", {})
    rows = []
    for p, n in (("0q", "이번 분기"), ("0y", "올해"), ("+1y", "내년")):
        if p in et and et[p].get("now") and et[p].get("d7"):
            rows.append(f"{n} EPS {et[p]['now']:.2f} (7일 {pct((et[p]['now'] / et[p]['d7'] - 1) * 100, 1, True)}, 30일 {pct((et[p]['now'] / et[p]['d30'] - 1) * 100, 1, True) if et[p].get('d30') else '–'}; 리비전 ↑{rv.get(p, {}).get('up7', 0)}/↓{rv.get(p, {}).get('down7', 0)} 7일)")
    cons = est.get("consensus", {})
    rn, rp = est.get("recs_now", {}), est.get("recs_prev", {})
    if rn:
        rows.append(f"투자의견 SB{rn['strong_buy']}/B{rn['buy']}/H{rn['hold']}/S{rn['sell'] + rn['strong_sell']}" + (f" (전월 SB{rp['strong_buy']}/B{rp['buy']}/H{rp['hold']}/S{rp['sell'] + rp['strong_sell']})" if rp else "") + f" · 목표가 평균 ${num(cons.get('target_consensus'), 0)} (현재가 대비 {pct((cons.get('target_consensus') or t['last']) / t['last'] * 100 - 100, 0, True)})")
    if rows:
        blocks.append(sec("*추정치 흐름*\n" + "\n".join(f"• {r}" for r in rows)))
    # flows
    flows = []
    if si:
        flows.append(f"공매도 {si['short']/1e6:.1f}M주 ({si['settlement_date']}, {pct(si['chg_pct'], 1, True)}), 유통 {si.get('pct_float')}%, 추이 " + "→".join(f"{v/1e6:.1f}" for _, v in si["trend"][-4:]))
    sells = sum(r["value"] for r in rec_ins if r["kind"] == "매도")
    buys = sum(r["value"] for r in rec_ins if r["kind"] == "매수/취득")
    flows.append(f"내부자 30일: 매도 ${sells/1e6:.1f}M / 매수 ${buys/1e6:.1f}M ({len(rec_ins)}건)" + (" — 장내 매수 없음" if buys == 0 else ""))
    for sym_p, desc in m["proxies"].items():
        if sym_p in rel.index:
            flows.append(f"{sym_p} 주간 {pct(rel.loc[sym_p]['5d'], 1, True)} / 1개월 {pct(rel.loc[sym_p]['1m'], 1, True)} — {desc.split(' — ')[0]}")
    blocks.append(sec("*수급 / 프록시*\n" + "\n".join(f"• {f}" for f in flows)))
    for k, c in m["conv"].items():
        blocks.append(ctx(f"💱 전환사채 전환가 ${c['conv_price']} 대비 {pct(c['dist_conv_pct'], 1, True)} · 트리거 ${c['trigger']} 대비 {pct(c['dist_trigger_pct'], 1, True)} · 최근 30일 중 {c['days_above_trigger_30']}일 상회"))
    # quality checkpoint reminder
    qb = CFG.get("quality_baseline", {})
    if qb:
        blocks.append(ctx(f"📌 다음 10-Q 체크포인트 (기준 {qb.get('as_of')}): 계약자산 ${qb.get('contract_assets_musd')}M · 매출채권 ${qb.get('receivables_musd')}M · 선수금 ${qb.get('customer_deposits_musd')}M · 재고 ${qb.get('inventory_musd')}M · OCF ${qb.get('ocf_q_musd')}M"))
    cal = calendar_lines(m)
    if cal:
        blocks.append(sec("*일정*\n" + "\n".join(f"• {c}" for c in cal[:6])))
    pos = position_line(t, opt)
    if pos:
        blocks.append(ctx(pos))
    ai = ai_note("weekly", "\n".join(regime + rows + flows))
    if ai:
        blocks.append(sec("*Claude 메모*\n" + ai))
    blocks.extend(footer("weekly"))
    send(blocks, f"{SYM} 주간 브리핑 {pct(rel.loc[SYM]['5d'], 1, True)}")
    st["snapshots"].append(snapshot(m))
    save_state(st)


def run_quarterly(st: dict | None = None, trigger_url: str = ""):
    st = st or load_state()
    tbl = X.quarterly_table(SYM, 6)
    if tbl is None or tbl.empty:
        log.warning("no XBRL table")
        return
    qm = X.quality_metrics(tbl)
    verdict = A.quality_verdict(tbl, qm)
    cur = tbl.iloc[-1]
    period = str(tbl.index[-1].date())
    blocks = [header(f"🧪 ${SYM} 이익의 질 체크포인트 · {period}")]
    cols = [("revenue", "매출"), ("net_income", "순이익"), ("ocf", "OCF"), ("capex", "Capex"), ("receivables", "매출채권"), ("contract_assets", "계약자산(미청구)"), ("inventory", "재고"), ("payables", "매입채무"), ("deferred_rev", "이연매출"), ("cash", "현금"), ("lt_debt", "장기부채")]
    lines = []
    for k, n in cols:
        if k in tbl.columns and pd.notna(cur.get(k)):
            prev = tbl.iloc[-2][k] if len(tbl) > 1 else float("nan")
            lines.append(f"{n}: ${cur[k]:,.0f}M" + (f" ({cur[k] - prev:+,.0f} QoQ)" if pd.notna(prev) else ""))
    blocks.append(sec("*XBRL 분기 수치 ($M)*\n" + "\n".join(f"• {l}" for l in lines)))
    blocks.append(sec("*판정*\n" + "\n".join(f"• {v}" for v in verdict)))
    qb = CFG.get("quality_baseline", {})
    if qb and qb.get("as_of") and qb["as_of"] < period:
        cmp = []
        for k, n in (("contract_assets", "계약자산"), ("receivables", "매출채권"), ("inventory", "재고"), ("ocf", "OCF")):
            b = qb.get(f"{k}_musd") or qb.get(f"{k}_q_musd")
            if b is not None and k in tbl.columns and pd.notna(cur.get(k)):
                cmp.append(f"{n} ${b:,.0f}M → ${cur[k]:,.0f}M ({cur[k] - b:+,.0f})")
        if cmp:
            blocks.append(sec(f"*기준점({qb['as_of']}) 대비*\n" + "\n".join(f"• {c}" for c in cmp)))
    if trigger_url:
        blocks.append(ctx(f"트리거: 새 정기 보고서 <{trigger_url}|열기>"))
    ai = ai_note("quarterly", "\n".join(lines + verdict))
    if ai:
        blocks.append(sec("*Claude 메모*\n" + ai))
    blocks.extend(footer("quarterly"))
    send(blocks, f"{SYM} 이익의 질 체크포인트 {period}")
    st["last_quality_period"] = trigger_url or period
    save_state(st)


def ai_note(mode: str, facts: str) -> str:
    if not ANTHROPIC_API_KEY:
        return ""
    prompt = (f"당신은 ${SYM}({CFG.get('name')}) 한 종목에 집중 투자한 개인 투자자를 돕는 리서치 보조입니다. 아래는 봇이 계산한 {mode} 데이터입니다. "
              "흔한 관점이 아니라, 이 데이터 조합에서만 보이는 비대칭(기대와 다른 점, 서로 모순되는 신호, 다음에 확인할 한 가지)을 한국어 4줄 이내로 쓰세요. "
              "매수/매도 권유, 목표가 제시는 하지 마세요. 숫자는 주어진 것만 인용하세요.\n\n" + facts)
    try:
        r = requests.post("https://api.anthropic.com/v1/messages",
                          headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                          json={"model": os.environ.get("CLAUDE_MODEL", "claude-sonnet-5-5"), "max_tokens": 500,
                                "messages": [{"role": "user", "content": prompt}]}, timeout=90)
        r.raise_for_status()
        return "".join(b.get("text", "") for b in r.json().get("content", [])).strip()
    except Exception as e:  # noqa: BLE001
        log.warning("ai_note failed: %s", e)
        return ""


MODES = {"close": run_close, "filings": run_filings, "weekly": run_weekly, "quarterly": run_quarterly}

if __name__ == "__main__":
    mode = (sys.argv[1] if len(sys.argv) > 1 else os.environ.get("RUN_MODE", "close")).lower()
    if mode not in MODES:
        sys.exit(f"unknown mode {mode}; use one of {list(MODES)}")
    log.info("=== %s %s ===", SYM, mode)
    MODES[mode]()
