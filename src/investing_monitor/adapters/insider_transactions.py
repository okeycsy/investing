from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from typing import Any

from investing_monitor.domain.evidence import (
    EvidenceKind,
    EvidenceProfile,
    RawEvidenceCandidate,
)


class YahooInsiderError(RuntimeError):
    pass


class YahooInsiderTransactionsAdapter:
    def fetch(
        self,
        profile: EvidenceProfile,
        *,
        limit: int = 100,
        lookback_days: int = 120,
        now: datetime | None = None,
    ) -> tuple[RawEvidenceCandidate, ...]:
        now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        try:
            import yfinance as yf

            table = yf.Ticker(profile.ticker).get_insider_transactions()
            rows = table.to_dict("records") if table is not None else []
        except Exception as exc:
            raise YahooInsiderError(
                f"Yahoo insider transactions unavailable: {exc}"
            ) from exc
        return parse_yahoo_insider_transactions(
            rows,
            profile,
            limit=limit,
            lookback_days=lookback_days,
            now=now,
        )


def parse_yahoo_insider_transactions(
    rows: Sequence[Mapping[str, Any]],
    profile: EvidenceProfile,
    *,
    limit: int,
    lookback_days: int,
    now: datetime,
) -> tuple[RawEvidenceCandidate, ...]:
    cutoff = now.date() - timedelta(days=lookback_days)
    source_url = (
        f"https://finance.yahoo.com/quote/{profile.ticker}/insider-transactions/"
    )
    candidates = []
    for row in rows:
        transaction_date = _coerce_date(row.get("Start Date"))
        if transaction_date is None or transaction_date < cutoff:
            continue
        description = " ".join(str(row.get("Text") or "").split())
        code = transaction_code(description)
        if not code:
            continue
        name = " ".join(str(row.get("Insider") or "Unknown insider").split())
        position = " ".join(str(row.get("Position") or "Position unavailable").split())
        shares = max(0, int(_number(row.get("Shares"))))
        value_usd = max(0.0, _number(row.get("Value")))
        ownership = str(row.get("Ownership") or "").strip()
        label = {
            "P": "Open-market purchase",
            "S": "Open-market sale",
            "A": "Stock award",
            "M": "Option exercise",
            "F": "Tax withholding",
        }[code]
        source_text = (
            f"{name} ({position}). {label}: {shares:,} shares; "
            f"reported value ${value_usd:,.0f}; transaction date "
            f"{transaction_date.isoformat()}; ownership {ownership or 'unknown'}. "
            f"Yahoo description: {description}"
        )
        stable_value = "|".join(
            (
                transaction_date.isoformat(),
                name.casefold(),
                code,
                str(shares),
                f"{value_usd:.0f}",
                ownership,
                description.casefold(),
            )
        )
        external_id = "yahoo-insider-" + hashlib.sha256(
            stable_value.encode("utf-8")
        ).hexdigest()[:20]
        candidates.append(
            RawEvidenceCandidate(
                ticker=profile.ticker,
                kind=EvidenceKind.INSIDER,
                headline=f"{name} — {label}",
                source_name="Yahoo Finance insider data",
                source_url=source_url,
                published_at=datetime.combine(
                    transaction_date,
                    datetime.min.time(),
                    timezone.utc,
                ),
                source_text=source_text,
                external_id=external_id,
                metadata={
                    "transaction_code": code,
                    "transaction_codes": (code,),
                    "insider_name": name,
                    "position": position,
                    "shares": shares,
                    "value_usd": value_usd,
                    "ownership": ownership,
                    "transaction_date": transaction_date.isoformat(),
                    "exact_form": False,
                },
            )
        )
        if len(candidates) == limit:
            break
    return tuple(candidates)


def transaction_code(description: str) -> str:
    normalized = re.sub(r"[^a-z]+", " ", description.casefold()).strip()
    if "purchase" in normalized or "open market buy" in normalized:
        return "P"
    if "sale" in normalized or "open market sell" in normalized:
        return "S"
    if "award" in normalized or "grant" in normalized:
        return "A"
    if "exercise" in normalized or "conversion" in normalized:
        return "M"
    if "tax" in normalized or "withholding" in normalized:
        return "F"
    return ""


def _coerce_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value or "")[:10])
    except ValueError:
        return None


def _number(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0
