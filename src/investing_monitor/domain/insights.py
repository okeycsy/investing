"""Insight signals for a concentrated single-stock position.

Pure functions over plain Python records (no pandas, no network), in the same
spirit as domain/situation.py. The adapters fetch; this module interprets.

Signals
-------
* ResidualDrift      — today's move split into the part the sensitivity model
                       explains (benchmark / peer beta) and the name-specific
                       residual, plus the 5/20-day cumulative residual.
* OptionsInsight     — put/call open interest, ~30-day implied volatility versus
                       realized, straddle-implied move, event move into the next
                       earnings expiry, and open-interest "walls".
* ConvertibleWatch   — distance to a convertible's conversion price and its
                       redemption trigger, with the 20-of-30-day count.
* ShortInterestInsight — exchange-reported short interest (not daily short volume).
* ProxyMove          — customer / partner proxies that lead demand or credit risk.
* EarningsQuality    — SEC XBRL quarterly lines that show whether reported
                       profit is turning into cash (contract assets, receivables,
                       operating cash flow, inventory, payables).

Wording rules: see presentation/quality.py FORBIDDEN_TEXT — never emit
"RSI", "MACD", "PCR", "FINRA" or "DCA".
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from math import isfinite, sqrt
from typing import Any

from investing_monitor.domain.models import MarketSensitivity, Position
from investing_monitor.domain.situation import _daily_returns, _peer_average_returns


# ───────────────────────── profile ─────────────────────────
@dataclass(frozen=True)
class ConvertibleNote:
    label: str
    conversion_price: float
    trigger_multiple: float = 1.30
    max_shares_millions: float | None = None
    trigger_days_required: int = 20
    trigger_window_days: int = 30


@dataclass(frozen=True)
class InsightProfile:
    """Everything the insight path needs beyond InstrumentProfile."""

    customer_proxies: tuple[tuple[str, str], ...] = ()  # (symbol, why it matters)
    convertibles: tuple[ConvertibleNote, ...] = ()
    residual_alert_z: float = 1.5
    residual_drift_alert_pct: float = 8.0
    iv_jump_alert_points: float = 10.0
    short_interest_alert_pct: float = 10.0
    proxy_weak_pct: float = -7.0


# ───────────────────────── records ─────────────────────────
@dataclass(frozen=True)
class OptionQuote:
    expiration: date
    strike: float
    option_type: str  # "call" | "put"
    open_interest: int
    volume: int
    bid: float
    ask: float
    last: float
    implied_volatility: float  # 0.95 == 95%


@dataclass(frozen=True)
class ResidualDrift:
    actual_pct: float
    expected_pct: float
    residual_pct: float
    residual_z: float
    residual_5d_pct: float
    residual_20d_pct: float
    basis: str  # "peer" | "benchmark"
    beta: float
    comparison_pct: float

    @property
    def label(self) -> str:
        if abs(self.residual_z) >= 2:
            return "섹터로 설명되지 않는 종목 고유 움직임"
        if abs(self.residual_z) >= 1:
            return "섹터 + 일부 종목 고유 요인"
        return "섹터 동반 움직임"


@dataclass(frozen=True)
class ExpiryMove:
    expiration: date
    days: int
    atm_strike: float
    implied_volatility_pct: float
    straddle: float
    implied_move_pct: float
    open_interest: int


@dataclass(frozen=True)
class OptionsInsight:
    put_call_open_interest: float
    put_call_volume: float
    total_open_interest: int
    iv30_pct: float | None
    move_30d_pct: float | None
    expiry_30d: date | None
    earnings_expiry: date | None
    earnings_move_pct: float | None
    event_move_pct: float | None
    call_walls: tuple[tuple[float, int], ...]
    put_walls: tuple[tuple[float, int], ...]
    term: tuple[ExpiryMove, ...]


@dataclass(frozen=True)
class ConvertibleWatch:
    label: str
    conversion_price: float
    trigger_price: float
    distance_to_conversion_pct: float
    distance_to_trigger_pct: float
    days_above_trigger: int
    days_required: int
    in_the_money: bool
    max_shares_millions: float | None


@dataclass(frozen=True)
class ShortInterestInsight:
    report_date: date
    shares_short: int
    prior_shares_short: int | None
    change_pct: float | None
    percent_of_float: float | None
    days_to_cover: float | None


@dataclass(frozen=True)
class ProxyMove:
    symbol: str
    reason: str
    change_1d_pct: float
    change_5d_pct: float


@dataclass(frozen=True)
class QualityQuarter:
    period_end: date
    values: Mapping[str, float]  # USD millions, keys from FLOW_TAGS/STOCK_TAGS


@dataclass(frozen=True)
class EarningsQuality:
    quarters: tuple[QualityQuarter, ...]
    verdict: tuple[str, ...]
    source_form: str
    source_accession: str
    source_url: str

    @property
    def latest(self) -> QualityQuarter:
        return self.quarters[-1]


@dataclass(frozen=True)
class InsightLine:
    priority: int  # lower first
    text: str
    kind: str


@dataclass(frozen=True)
class InsightBundle:
    ticker: str
    trading_date: date
    close: float
    change_pct: float
    realized_vol_20d_pct: float | None
    drift: ResidualDrift | None
    options: OptionsInsight | None
    convertibles: tuple[ConvertibleWatch, ...]
    short_interest: ShortInterestInsight | None
    proxies: tuple[ProxyMove, ...]
    ticker_5d_pct: float | None
    next_earnings: date | None
    errors: tuple[str, ...] = ()

    def snapshot(self) -> dict[str, Any]:
        """Compact JSON-able state the next run compares against."""
        return {
            "trading_date": self.trading_date.isoformat(),
            "close": self.close,
            "change_pct": self.change_pct,
            "iv30_pct": self.options.iv30_pct if self.options else None,
            "put_call_open_interest": (
                self.options.put_call_open_interest if self.options else None
            ),
            "residual_pct": self.drift.residual_pct if self.drift else None,
            "residual_20d_pct": self.drift.residual_20d_pct if self.drift else None,
            "short_interest_date": (
                self.short_interest.report_date.isoformat() if self.short_interest else None
            ),
            "shares_short": self.short_interest.shares_short if self.short_interest else None,
            "convertible_in_the_money": {
                c.label: c.in_the_money for c in self.convertibles
            },
        }


# ───────────────────────── residual drift ─────────────────────────
def residual_drift(
    *,
    ticker: str,
    daily_closes: Mapping[str, Mapping[date, float]],
    sensitivity: MarketSensitivity | None,
    benchmark_symbol: str,
    peer_symbols: Sequence[str],
) -> ResidualDrift | None:
    """Apply the stored sensitivity model to daily closes.

    Reusing MarketSensitivity keeps this consistent with the intraday
    `민감도 참고` verdicts instead of fitting a second, competing model.
    """
    if sensitivity is None:
        return None
    target = _daily_returns(daily_closes.get(ticker.upper(), {}))
    if len(target) < 25:
        return None
    peer_returns = {
        symbol.upper(): _daily_returns(daily_closes.get(symbol.upper(), {}))
        for symbol in peer_symbols
    }
    comparison: Mapping[date, float]
    if sensitivity.peer_beta is not None and sensitivity.peer_residual_band_pct:
        comparison = _peer_average_returns(peer_returns)
        beta = sensitivity.peer_beta
        band = sensitivity.peer_residual_band_pct
        basis = "peer"
    elif sensitivity.benchmark_beta is not None and sensitivity.benchmark_residual_band_pct:
        comparison = _daily_returns(daily_closes.get(benchmark_symbol.upper(), {}))
        beta = sensitivity.benchmark_beta
        band = sensitivity.benchmark_residual_band_pct
        basis = "benchmark"
    else:
        return None
    dates = sorted(target.keys() & comparison.keys())
    if len(dates) < 25:
        return None
    residuals = [target[d] - beta * comparison[d] for d in dates]
    today = dates[-1]
    expected = beta * comparison[today]
    return ResidualDrift(
        actual_pct=round(target[today], 2),
        expected_pct=round(expected, 2),
        residual_pct=round(residuals[-1], 2),
        residual_z=round(residuals[-1] / band, 2) if band > 0 else 0.0,
        residual_5d_pct=round(sum(residuals[-5:]), 2),
        residual_20d_pct=round(sum(residuals[-20:]), 2),
        basis=basis,
        beta=round(beta, 2),
        comparison_pct=round(comparison[today], 2),
    )


def realized_volatility_pct(closes: Mapping[date, float], sessions: int = 20) -> float | None:
    returns = list(_daily_returns(closes).values())[-sessions:]
    if len(returns) < max(10, sessions // 2):
        return None
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / max(1, len(returns) - 1)
    return round(sqrt(variance) * sqrt(252), 1)


def trailing_change_pct(closes: Mapping[date, float], sessions: int) -> float | None:
    ordered = [closes[d] for d in sorted(closes) if closes[d] > 0]
    if len(ordered) <= sessions:
        return None
    return round((ordered[-1] / ordered[-1 - sessions] - 1) * 100, 2)


# ───────────────────────── options ─────────────────────────
def options_insight(
    quotes: Sequence[OptionQuote],
    *,
    spot: float,
    as_of: date,
    next_earnings: date | None = None,
) -> OptionsInsight | None:
    if not quotes or spot <= 0:
        return None
    calls = [q for q in quotes if q.option_type == "call"]
    puts = [q for q in quotes if q.option_type == "put"]
    call_oi = sum(q.open_interest for q in calls)
    put_oi = sum(q.open_interest for q in puts)
    call_vol = sum(q.volume for q in calls)
    put_vol = sum(q.volume for q in puts)

    by_expiry: dict[date, list[OptionQuote]] = {}
    for q in quotes:
        by_expiry.setdefault(q.expiration, []).append(q)
    term: list[ExpiryMove] = []
    for expiration in sorted(by_expiry):
        days = (expiration - as_of).days
        if days <= 0:
            continue
        group = by_expiry[expiration]
        atm = min((q.strike for q in group), key=lambda k: abs(k - spot))
        legs = [q for q in group if q.strike == atm]
        straddle = sum(_mid(q) for q in legs)
        ivs = [q.implied_volatility for q in legs if isfinite(q.implied_volatility) and q.implied_volatility > 0]
        if straddle <= 0 or not ivs:
            continue
        term.append(
            ExpiryMove(
                expiration=expiration,
                days=days,
                atm_strike=atm,
                implied_volatility_pct=round(sum(ivs) / len(ivs) * 100, 1),
                straddle=round(straddle, 2),
                implied_move_pct=round(straddle / spot * 100, 1),
                open_interest=sum(q.open_interest for q in group),
            )
        )
    if not term:
        return None

    near30 = min(term, key=lambda t: abs(t.days - 30))
    earnings_expiry = earnings_move = event_move = None
    if next_earnings is not None:
        post = [t for t in term if t.expiration >= next_earnings]
        pre = [t for t in term if t.expiration < next_earnings]
        if post:
            first = post[0]
            earnings_expiry, earnings_move = first.expiration, first.implied_move_pct
            if pre:
                last_pre = pre[-1]
                # variance that the earnings-spanning expiry carries beyond the
                # pre-earnings expiry; 0.8 converts a 1-sigma to ~E|move|
                v_post = (first.implied_volatility_pct / 100) ** 2 * first.days / 365
                v_pre = (last_pre.implied_volatility_pct / 100) ** 2 * last_pre.days / 365
                if v_post > v_pre:
                    event_move = round(sqrt(v_post - v_pre) * 100 * 0.8, 1)

    near = [q for q in quotes if 0 < (q.expiration - as_of).days <= 75 and 0.6 * spot <= q.strike <= 1.4 * spot]
    oi_by: dict[tuple[float, str], int] = {}
    for q in near:
        oi_by[(q.strike, q.option_type)] = oi_by.get((q.strike, q.option_type), 0) + q.open_interest
    call_walls = sorted(
        ((k, v) for (k, t), v in oi_by.items() if t == "call" and k > spot),
        key=lambda kv: -kv[1],
    )[:3]
    put_walls = sorted(
        ((k, v) for (k, t), v in oi_by.items() if t == "put" and k < spot),
        key=lambda kv: -kv[1],
    )[:3]
    return OptionsInsight(
        put_call_open_interest=round(put_oi / max(call_oi, 1), 2),
        put_call_volume=round(put_vol / max(call_vol, 1), 2),
        total_open_interest=call_oi + put_oi,
        iv30_pct=near30.implied_volatility_pct,
        move_30d_pct=near30.implied_move_pct,
        expiry_30d=near30.expiration,
        earnings_expiry=earnings_expiry,
        earnings_move_pct=earnings_move,
        event_move_pct=event_move,
        call_walls=tuple(call_walls),
        put_walls=tuple(put_walls),
        term=tuple(term[:8]),
    )


def _mid(q: OptionQuote) -> float:
    if q.bid > 0 and q.ask > 0:
        return (q.bid + q.ask) / 2
    return q.last if q.last > 0 else 0.0


# ───────────────────────── convertible ─────────────────────────
def convertible_watch(note: ConvertibleNote, closes: Mapping[date, float]) -> ConvertibleWatch | None:
    ordered = [closes[d] for d in sorted(closes)]
    if not ordered:
        return None
    last = ordered[-1]
    trigger = note.conversion_price * note.trigger_multiple
    window = ordered[-note.trigger_window_days:]
    return ConvertibleWatch(
        label=note.label,
        conversion_price=note.conversion_price,
        trigger_price=round(trigger, 2),
        distance_to_conversion_pct=round((last / note.conversion_price - 1) * 100, 1),
        distance_to_trigger_pct=round((last / trigger - 1) * 100, 1),
        days_above_trigger=sum(1 for c in window if c >= trigger),
        days_required=note.trigger_days_required,
        in_the_money=last > note.conversion_price,
        max_shares_millions=note.max_shares_millions,
    )


# ───────────────────────── short interest ─────────────────────────
def short_interest_insight(fields: Mapping[str, Any]) -> ShortInterestInsight | None:
    """From Yahoo quoteSummary-style fields (sharesShort, dateShortInterest, ...)."""
    shares = _as_int(fields.get("sharesShort"))
    stamp = fields.get("dateShortInterest")
    if not shares or not stamp:
        return None
    report_date = _as_date(stamp)
    if report_date is None:
        return None
    prior = _as_int(fields.get("sharesShortPriorMonth"))
    pct_float = fields.get("shortPercentOfFloat")
    return ShortInterestInsight(
        report_date=report_date,
        shares_short=shares,
        prior_shares_short=prior,
        change_pct=round((shares / prior - 1) * 100, 1) if prior else None,
        percent_of_float=round(float(pct_float) * 100, 2) if pct_float else None,
        days_to_cover=_as_float(fields.get("shortRatio")),
    )


# ───────────────────────── proxies ─────────────────────────
def proxy_moves(
    proxies: Sequence[tuple[str, str]],
    daily_closes: Mapping[str, Mapping[date, float]],
) -> tuple[ProxyMove, ...]:
    out = []
    for symbol, reason in proxies:
        closes = daily_closes.get(symbol.upper(), {})
        d1 = trailing_change_pct(closes, 1)
        d5 = trailing_change_pct(closes, 5)
        if d1 is None or d5 is None:
            continue
        out.append(ProxyMove(symbol=symbol.upper(), reason=reason, change_1d_pct=d1, change_5d_pct=d5))
    return tuple(out)


# ───────────────────────── earnings quality (XBRL) ─────────────────────────
FLOW_TAGS: dict[str, tuple[str, ...]] = {
    "revenue": ("Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet"),
    "gross_profit": ("GrossProfit",),
    "operating_income": ("OperatingIncomeLoss",),
    "net_income": (
        "NetIncomeLossAvailableToCommonStockholdersBasic",
        "NetIncomeLoss",
        "ProfitLoss",
    ),
    "operating_cash_flow": ("NetCashProvidedByUsedInOperatingActivities",),
    "capex": ("PaymentsToAcquirePropertyPlantAndEquipment",),
}
STOCK_TAGS: dict[str, tuple[str, ...]] = {
    "cash": ("CashAndCashEquivalentsAtCarryingValue",),
    "receivables": ("AccountsReceivableNetCurrent", "ContractWithCustomerReceivableAfterAllowanceForCreditLoss"),
    "contract_assets": ("ContractWithCustomerAssetNetCurrent", "ContractWithCustomerAssetNet"),
    "inventory": ("InventoryNet",),
    "payables": ("AccountsPayableCurrent",),
    "deferred_revenue": ("ContractWithCustomerLiabilityCurrent", "ContractWithCustomerLiability", "DeferredRevenueCurrent"),
    "customer_deposits": ("CustomerAdvancesAndDeposits", "CustomerDepositsCurrent"),
    "long_term_debt": ("LongTermDebtNoncurrent", "LongTermDebt"),
    "shares_outstanding": ("CommonStockSharesOutstanding", "EntityCommonStockSharesOutstanding"),
}
QUALITY_LABELS = {
    "revenue": "매출", "gross_profit": "매출총이익", "operating_income": "영업이익", "net_income": "순이익",
    "operating_cash_flow": "영업현금흐름", "capex": "설비투자", "cash": "현금", "receivables": "매출채권",
    "contract_assets": "계약자산(미청구 매출)", "inventory": "재고", "payables": "매입채무",
    "deferred_revenue": "이연매출", "customer_deposits": "고객 선수금", "long_term_debt": "장기부채",
}
_PERIODIC_FORMS = {"10-Q", "10-K", "10-Q/A", "10-K/A"}


def earnings_quality(
    facts: Mapping[str, Any],
    *,
    quarters: int = 6,
    cik: str = "",
) -> EarningsQuality | None:
    """Build the quarterly quality table from an SEC companyfacts `facts` map
    (us-gaap merged with dei)."""
    columns: dict[str, dict[date, float]] = {}
    latest_source: tuple[str, str, str] = ("", "", "")  # filed, form, accession
    for name, tags in FLOW_TAGS.items():
        rows = _entries(facts, tags)
        if rows:
            columns[name] = _quarterly_flow(rows)
            latest_source = max(latest_source, _latest_source(rows))
    for name, tags in STOCK_TAGS.items():
        rows = _entries(facts, tags)
        if rows:
            columns[name] = {r_end: val for r_end, val in _stock_series(rows).items()}
    revenue = columns.get("revenue")
    if not revenue:
        return None
    period_ends = sorted(revenue)[-quarters:]
    out_quarters = []
    for end in period_ends:
        values: dict[str, float] = {}
        for name, series in columns.items():
            if end in series and series[end] is not None:
                scale = 1e6
                values[name] = round(series[end] / scale, 1)
        out_quarters.append(QualityQuarter(period_end=end, values=values))
    if len(out_quarters) < 2:
        return None
    filed, form, accession = latest_source
    url = ""
    if accession and cik:
        url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/"
    return earnings_quality_from_quarters(
        out_quarters, source_form=f"SEC XBRL ({form})", source_accession=accession, source_url=url
    )


def earnings_quality_from_quarters(
    quarters: Sequence[QualityQuarter],
    *,
    source_form: str,
    source_url: str,
    source_accession: str = "",
) -> EarningsQuality | None:
    """Same table and verdict from any quarterly source (XBRL or a fallback)."""
    ordered = sorted(quarters, key=lambda q: q.period_end)
    if len(ordered) < 2 or not ordered[-1].values.get("revenue"):
        return None
    return EarningsQuality(
        quarters=tuple(ordered),
        verdict=tuple(quality_verdict(ordered)),
        source_form=source_form,
        source_accession=source_accession,
        source_url=source_url,
    )


def quality_verdict(quarters: Sequence[QualityQuarter]) -> list[str]:
    """Plain-language flags, latest quarter versus the prior one. Each line is a
    fact with its arithmetic, never a buy/sell prescription."""
    if len(quarters) < 2:
        return []
    cur, prev = quarters[-1].values, quarters[-2].values
    rev = cur.get("revenue") or 0
    lines: list[str] = []
    ca, ca_prev = cur.get("contract_assets"), prev.get("contract_assets")
    if ca is not None and ca_prev is not None and rev:
        delta = ca - ca_prev
        share = delta / rev * 100
        tone = "⚠️" if share > 15 else ("✅" if delta < 0 else "·")
        lines.append(
            f"{tone} 계약자산(미청구 매출) ${ca:,.0f}M, 전분기 대비 {delta:+,.0f}M — 분기 매출의 {share:+.0f}%"
        )
    ar, ar_prev = cur.get("receivables"), prev.get("receivables")
    if rev and ar is not None and (ca is not None or ar is not None):
        days = ((ar or 0) + (ca or 0)) / rev * 91
        prev_rev = prev.get("revenue") or 0
        prev_days = ((ar_prev or 0) + (ca_prev or 0)) / prev_rev * 91 if prev_rev else None
        delta_days = f", 전분기 대비 {days - prev_days:+.0f}일" if prev_days is not None else ""
        tone = "⚠️" if prev_days is not None and days - prev_days > 10 else "·"
        lines.append(f"{tone} 매출채권+미청구 회수 일수 {days:.0f}일{delta_days}")
    ocf = cur.get("operating_cash_flow")
    if ocf is not None:
        ni = cur.get("net_income")
        ratio = f", 순이익 대비 {ocf / ni:.2f}배" if ni and ni > 0 else ""
        lines.append(f"{'⚠️' if ocf < 0 else '✅'} 영업현금흐름 ${ocf:,.0f}M{ratio}")
        capex = cur.get("capex")
        if capex is not None:
            fcf = ocf - abs(capex)
            lines.append(f"{'⚠️' if fcf < 0 else '✅'} 잉여현금흐름 ${fcf:,.0f}M (설비투자 ${abs(capex):,.0f}M)")
    dep, dep_prev = cur.get("customer_deposits"), prev.get("customer_deposits")
    if dep is not None and dep_prev is not None:
        lines.append(f"{'✅' if dep >= dep_prev else '⚠️'} 고객 선수금 ${dep:,.0f}M, 전분기 대비 {dep - dep_prev:+,.0f}M")
    inv, inv_prev = cur.get("inventory"), prev.get("inventory")
    if inv is not None and inv_prev is not None and rev:
        delta = inv - inv_prev
        lines.append(f"{'⚠️' if delta / rev > 0.15 else '·'} 재고 ${inv:,.0f}M, 전분기 대비 {delta:+,.0f}M")
    ap, ap_prev = cur.get("payables"), prev.get("payables")
    if ap is not None and ap_prev is not None and rev and (ap - ap_prev) > 0.1 * rev:
        lines.append(f"⚠️ 매입채무 전분기 대비 {ap - ap_prev:+,.0f}M — 지급 지연이 영업현금흐름을 떠받친 가능성")
    sh = cur.get("shares_outstanding")
    sh_yoy = quarters[-5].values.get("shares_outstanding") if len(quarters) >= 5 else None
    if sh and sh_yoy:
        pct = (sh / sh_yoy - 1) * 100
        lines.append(f"{'⚠️' if pct > 10 else '·'} 발행주식수 전년 동기 대비 {pct:+.1f}%")
    return lines


def _entries(facts: Mapping[str, Any], tags: Sequence[str]) -> list[dict]:
    for tag in tags:
        fact = facts.get(tag)
        if not fact:
            continue
        units = fact.get("units", {})
        unit = "USD" if "USD" in units else ("shares" if "shares" in units else next(iter(units), None))
        rows = [r for r in units.get(unit, []) if r.get("form") in _PERIODIC_FORMS and r.get("end")]
        if rows:
            return rows
    return []


def _latest_source(rows: Sequence[Mapping[str, Any]]) -> tuple[str, str, str]:
    best = ("", "", "")
    for r in rows:
        key = (str(r.get("filed", "")), str(r.get("form", "")), str(r.get("accn", "")))
        if key[0] > best[0]:
            best = key
    return best


def _stock_series(rows: Sequence[Mapping[str, Any]]) -> dict[date, float]:
    ordered = sorted(rows, key=lambda r: (r["end"], str(r.get("filed", ""))))
    series: dict[date, float] = {}
    for r in ordered:  # later filings restate; last write wins
        series[date.fromisoformat(r["end"])] = float(r["val"])
    return series


def _quarterly_flow(rows: Sequence[Mapping[str, Any]]) -> dict[date, float]:
    """Mixed 3-month / year-to-date / full-year duration facts → quarterly values."""
    spans: dict[tuple[date, date], float] = {}
    for r in sorted(rows, key=lambda r: str(r.get("filed", ""))):
        if not r.get("start"):
            continue
        start, end = date.fromisoformat(r["start"]), date.fromisoformat(r["end"])
        days = (end - start).days
        if 60 < days < 380:
            spans[(start, end)] = float(r["val"])
    quarterly: dict[date, float] = {}
    for (start, end), val in spans.items():
        if (end - start).days <= 100:
            quarterly[end] = val
    by_start: dict[date, list[tuple[date, float]]] = {}
    for (start, end), val in spans.items():
        by_start.setdefault(start, []).append((end, val))
    for start, items in by_start.items():
        items.sort()
        prev_end: date | None = None
        prev_val = 0.0
        for end, val in items:
            if prev_end is not None and end not in quarterly and 70 < (end - prev_end).days < 110:
                quarterly[end] = val - prev_val
            prev_end, prev_val = end, val
    return quarterly


# ───────────────────────── ranking ─────────────────────────
def rank_insight_lines(
    bundle: InsightBundle,
    *,
    previous: Mapping[str, Any] | None,
    profile: InsightProfile,
    position: Position | None = None,
    limit: int = 6,
) -> list[InsightLine]:
    """Turn a bundle into ranked Slack-ready lines (facts + what to check)."""
    previous = previous or {}
    out: list[InsightLine] = []
    d = bundle.drift
    if d is not None:
        basis = "피어 평균" if d.basis == "peer" else "지수"
        core = (
            f"오늘 {_pct(d.actual_pct)} = {basis} 연동분 {_pct(d.expected_pct)}"
            f" ({basis} {_pct(d.comparison_pct)} × 민감도 {d.beta:.2f}) + 종목 고유 *{_pct(d.residual_pct)}*"
        )
        if abs(d.residual_z) >= profile.residual_alert_z:
            out.append(InsightLine(1, f"⚡ *섹터로 설명되지 않는 움직임* — {core}. 오늘 공시·뉴스에서 종목 고유 원인을 확인", "drift"))
        else:
            out.append(InsightLine(6, f"📊 {core} → {d.label}", "drift"))
        if abs(d.residual_20d_pct) >= profile.residual_drift_alert_pct:
            tone = "시장이 종목 고유 리스크를 반영하는 중" if d.residual_20d_pct < 0 else "섹터와 무관하게 종목 자체가 재평가되는 중"
            out.append(InsightLine(2, f"🧭 20거래일 누적 고유 잔차 {_pct(d.residual_20d_pct)} (5일 {_pct(d.residual_5d_pct)}) → {tone}", "drift20"))
    for p in bundle.proxies:
        weak_together = (
            p.change_5d_pct <= profile.proxy_weak_pct
            and bundle.ticker_5d_pct is not None
            and bundle.ticker_5d_pct <= profile.proxy_weak_pct
        )
        if weak_together:
            out.append(InsightLine(3, f"🏢 {p.symbol} 5일 {_pct(p.change_5d_pct)}와 동반 약세 ({p.reason}) — 수요가 아니라 고객 자금조달·신용 우려일 수 있음", "proxy"))
        elif abs(p.change_1d_pct) >= 4:
            out.append(InsightLine(4, f"🏢 {p.symbol} 오늘 {_pct(p.change_1d_pct)} / 5일 {_pct(p.change_5d_pct)} ({p.reason})", "proxy"))
    o = bundle.options
    if o is not None:
        prev_iv = previous.get("iv30_pct")
        if o.iv30_pct is not None and prev_iv is not None and abs(o.iv30_pct - prev_iv) >= profile.iv_jump_alert_points:
            out.append(InsightLine(2, f"🎯 30일 내재변동성 {prev_iv:.0f}% → {o.iv30_pct:.0f}% ({o.iv30_pct - prev_iv:+.0f}p) — 가격보다 큰 옵션 포지셔닝 변화", "iv_jump"))
        if o.iv30_pct is not None and bundle.realized_vol_20d_pct:
            spread = o.iv30_pct - bundle.realized_vol_20d_pct
            verdict = "실현치보다 싸다 (헤지 비용 낮음)" if spread < -10 else ("실현치보다 비싸다" if spread > 10 else "실현치와 비슷")
            out.append(InsightLine(5, f"🎯 30일 내재변동성 {o.iv30_pct:.0f}% vs 20일 실현변동성 {bundle.realized_vol_20d_pct:.0f}% → 옵션이 {verdict}; 한 달 내재 변동폭 ±{o.move_30d_pct}%", "iv"))
        prev_pc = previous.get("put_call_open_interest")
        if prev_pc is not None and abs(o.put_call_open_interest - prev_pc) >= 0.15:
            direction = "증가" if o.put_call_open_interest > prev_pc else "감소"
            out.append(InsightLine(4, f"🎯 풋/콜 미결제약정 비율 {prev_pc:.2f} → {o.put_call_open_interest:.2f} — 하방 헤지 수요 {direction}", "put_call"))
        if o.earnings_move_pct is not None and bundle.next_earnings is not None:
            days = (bundle.next_earnings - bundle.trading_date).days
            extra = f", 실적 이벤트 자체 ≈ ±{o.event_move_pct}%" if o.event_move_pct else ""
            out.append(InsightLine(6, f"📅 다음 실적 {bundle.next_earnings:%m/%d} (D-{days}): 옵션 내재 변동폭 ±{o.earnings_move_pct}%{extra}", "earnings_move"))
    prev_itm = previous.get("convertible_in_the_money") or {}
    for c in bundle.convertibles:
        was = prev_itm.get(c.label)
        if was is not None and was != c.in_the_money:
            if c.in_the_money:
                shares = f" — 전환 시 최대 {c.max_shares_millions}M주 희석 구간" if c.max_shares_millions else ""
                out.append(InsightLine(1, f"💱 {c.label} 전환가 ${c.conversion_price} 상향 돌파{shares}", "convertible_flip"))
            else:
                out.append(InsightLine(1, f"💱 {c.label} 전환가 ${c.conversion_price} 하향 이탈 — 전환 유인 소멸", "convertible_flip"))
        elif c.in_the_money or c.days_above_trigger >= c.days_required * 0.75:
            out.append(InsightLine(3, f"💱 {c.label}: 전환가 대비 {_pct(c.distance_to_conversion_pct)}, 상환 트리거 ${c.trigger_price} 대비 {_pct(c.distance_to_trigger_pct)} (최근 {30}일 중 {c.days_above_trigger}일 상회, 조건 {c.days_required}일)", "convertible"))
    si = bundle.short_interest
    if si is not None and si.report_date.isoformat() != previous.get("short_interest_date"):
        chg = f" ({_pct(si.change_pct)})" if si.change_pct is not None else ""
        flt = f", 유통주식의 {si.percent_of_float}%" if si.percent_of_float is not None else ""
        dtc = f", 숏커버 {si.days_to_cover}일" if si.days_to_cover is not None else ""
        tone = "⚠️" if si.change_pct is not None and abs(si.change_pct) >= profile.short_interest_alert_pct else "·"
        out.append(InsightLine(2 if tone == "⚠️" else 5, f"{tone} 공매도 잔고 신규 보고 ({si.report_date:%m/%d} 기준): {si.shares_short / 1e6:.1f}M주{chg}{flt}{dtc}", "short_interest"))
    if position is not None and position.shares and o is not None and o.move_30d_pct:
        swing = bundle.close * position.shares * o.move_30d_pct / 100
        out.append(InsightLine(7, f"💼 보유 {position.shares:,}주 기준 한 달 내재 변동폭 ≈ ±${swing:,.0f}", "position"))
    out.sort(key=lambda line: line.priority)
    return out[:limit]


def weekly_insight_lines(
    bundle: InsightBundle,
    *,
    week_ago: Mapping[str, Any] | None,
) -> list[str]:
    week_ago = week_ago or {}
    lines: list[str] = []
    d = bundle.drift
    if d is not None:
        tone = (
            "종목 고유 리스크 반영 중" if d.residual_20d_pct < -5
            else ("종목 자체 재평가 중" if d.residual_20d_pct > 5 else "섹터 추종")
        )
        lines.append(f"고유 잔차 누적 5일 {_pct(d.residual_5d_pct)} / 20일 {_pct(d.residual_20d_pct)} → {tone}")
    o = bundle.options
    if o is not None and o.iv30_pct is not None:
        was = week_ago.get("iv30_pct")
        rv = f", 20일 실현 {bundle.realized_vol_20d_pct:.0f}%" if bundle.realized_vol_20d_pct else ""
        lines.append(
            f"30일 내재변동성 {o.iv30_pct:.0f}%" + (f" (주초 {was:.0f}%)" if was is not None else "")
            + rv + f", 풋/콜 미결제약정 {o.put_call_open_interest:.2f}"
        )
        if o.earnings_move_pct is not None and bundle.next_earnings is not None:
            lines.append(f"다음 실적 {bundle.next_earnings:%m/%d}까지 옵션 내재 변동폭 ±{o.earnings_move_pct}%")
    for c in bundle.convertibles:
        lines.append(f"{c.label}: 전환가 ${c.conversion_price} 대비 {_pct(c.distance_to_conversion_pct)}, 상환 트리거 대비 {_pct(c.distance_to_trigger_pct)}, 최근 30일 중 {c.days_above_trigger}일 상회")
    si = bundle.short_interest
    if si is not None:
        chg = f" ({_pct(si.change_pct)})" if si.change_pct is not None else ""
        flt = f", 유통주식의 {si.percent_of_float}%" if si.percent_of_float is not None else ""
        lines.append(f"공매도 잔고 {si.shares_short / 1e6:.1f}M주{chg}{flt} — {si.report_date:%m/%d} 보고")
    for p in bundle.proxies:
        lines.append(f"{p.symbol} 주간 {_pct(p.change_5d_pct)} ({p.reason})")
    return lines


# ───────────────────────── helpers ─────────────────────────
def _pct(value: float | None) -> str:
    return "–" if value is None else f"{value:+.1f}%"


def _as_int(value: Any) -> int | None:
    try:
        v = int(float(value))
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    try:
        v = float(value)
        return round(v, 2) if isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        number = float(value)
        if number > 1e11:  # milliseconds
            number /= 1000
        return (datetime(1970, 1, 1) + timedelta(seconds=number)).date()
    except (TypeError, ValueError):
        pass
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def bundle_from_parts(**kwargs: Any) -> InsightBundle:
    return InsightBundle(**kwargs)


__all__ = [name for name in globals() if not name.startswith("_")]
