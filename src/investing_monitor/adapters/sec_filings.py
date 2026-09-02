from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
from typing import Any, Protocol

import requests

from investing_monitor.domain.evidence import (
    EvidenceKind,
    EvidenceCandidate,
    EvidenceProfile,
    RawEvidenceCandidate,
)


MATERIAL_FORMS = {
    "8-K",
    "8-K/A",
    "10-Q",
    "10-Q/A",
    "10-K",
    "10-K/A",
    "6-K",
    "6-K/A",
    "20-F",
    "20-F/A",
    "4",
    "4/A",
}


class SecFilingError(RuntimeError):
    pass


class SecAccessBlocked(SecFilingError):
    pass


@dataclass(frozen=True)
class SecFetchResult:
    candidates: tuple[RawEvidenceCandidate, ...]
    provider: str
    recovered: bool = False


class SecMirror(Protocol):
    def fetch(
        self,
        profile: EvidenceProfile,
        *,
        limit: int,
        lookback_days: int,
        now: datetime,
    ) -> tuple[RawEvidenceCandidate, ...]: ...


class SecSubmissionsClient:
    def __init__(
        self,
        *,
        get: Callable[..., Any] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        minimum_interval: float = 0.2,
        timeout: float = 15,
    ) -> None:
        self._get = get or requests.Session().get
        self._sleep = sleeper
        self._monotonic = monotonic
        self.minimum_interval = minimum_interval
        self.timeout = timeout
        self._last_request_at: float | None = None

    def fetch(
        self,
        profile: EvidenceProfile,
        *,
        limit: int = 30,
        lookback_days: int = 120,
        now: datetime | None = None,
    ) -> tuple[RawEvidenceCandidate, ...]:
        if not profile.cik:
            raise SecFilingError("CIK is unavailable")
        if not profile.sec_contact:
            raise SecFilingError("SEC contact is unavailable")
        now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        headers = {
            "User-Agent": f"investing-monitor/2.0 {profile.sec_contact}",
            "From": profile.sec_contact,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json",
        }
        url = f"https://data.sec.gov/submissions/CIK{profile.cik}.json"
        errors: list[str] = []
        for attempt in range(2):
            self._respect_rate_limit()
            try:
                response = self._get(url, headers=headers, timeout=self.timeout)
            except requests.RequestException as exc:
                errors.append(str(exc))
                if attempt == 0:
                    self._sleep(1)
                    continue
                raise SecFilingError(f"SEC submissions unavailable: {exc}") from exc
            if response.status_code == 200:
                try:
                    return parse_sec_submissions(
                        response.json(),
                        profile,
                        limit=limit,
                        lookback_days=lookback_days,
                        now=now,
                    )
                except (ValueError, TypeError, KeyError) as exc:
                    raise SecFilingError(f"invalid SEC submissions payload: {exc}") from exc
            if response.status_code == 403:
                raise SecAccessBlocked("SEC submissions blocked with HTTP 403")
            if response.status_code == 429:
                if attempt == 0:
                    self._sleep(_retry_after(response.headers.get("Retry-After")) or 2)
                    continue
                raise SecAccessBlocked("SEC submissions rate limited with HTTP 429")
            if response.status_code >= 500 and attempt == 0:
                errors.append(f"HTTP {response.status_code}")
                self._sleep(1)
                continue
            raise SecFilingError(
                f"SEC submissions rejected with HTTP {response.status_code}"
            )
        raise SecFilingError(
            f"SEC submissions unavailable: {'; '.join(errors[-2:]) or 'no response'}"
        )

    def _respect_rate_limit(self) -> None:
        current = self._monotonic()
        if self._last_request_at is not None:
            remaining = self.minimum_interval - (current - self._last_request_at)
            if remaining > 0:
                self._sleep(remaining)
                current = self._monotonic()
        self._last_request_at = current


class YFinanceSecMirror:
    def fetch(
        self,
        profile: EvidenceProfile,
        *,
        limit: int,
        lookback_days: int,
        now: datetime,
    ) -> tuple[RawEvidenceCandidate, ...]:
        try:
            import yfinance as yf

            rows = yf.Ticker(profile.ticker).get_sec_filings() or []
        except Exception as exc:
            raise SecFilingError(f"Yahoo SEC mirror unavailable: {exc}") from exc
        return parse_yahoo_sec_filings(
            rows,
            profile,
            limit=limit,
            lookback_days=lookback_days,
            now=now,
        )


class ResilientSecFilingsAdapter:
    def __init__(
        self,
        primary: SecSubmissionsClient | None = None,
        mirror: SecMirror | None = None,
    ) -> None:
        self.primary = primary or SecSubmissionsClient()
        self.mirror = mirror or YFinanceSecMirror()

    def fetch(
        self,
        profile: EvidenceProfile,
        *,
        limit: int = 30,
        lookback_days: int = 120,
        now: datetime | None = None,
    ) -> SecFetchResult:
        now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        try:
            candidates = self.primary.fetch(
                profile,
                limit=limit,
                lookback_days=lookback_days,
                now=now,
            )
            return SecFetchResult(candidates, provider="sec-submissions")
        except SecFilingError as primary_error:
            try:
                candidates = self.mirror.fetch(
                    profile,
                    limit=limit,
                    lookback_days=lookback_days,
                    now=now,
                )
            except SecFilingError as mirror_error:
                raise SecFilingError(
                    f"SEC primary and mirror failed: {primary_error}; {mirror_error}"
                ) from mirror_error
            return SecFetchResult(candidates, provider="yahoo-sec-mirror", recovered=True)


def parse_sec_submissions(
    payload: Mapping[str, Any],
    profile: EvidenceProfile,
    *,
    limit: int,
    lookback_days: int,
    now: datetime,
) -> tuple[RawEvidenceCandidate, ...]:
    recent = ((payload.get("filings") or {}).get("recent") or {})
    forms = recent.get("form") or []
    cutoff = now.date() - timedelta(days=lookback_days)
    candidates = []

    def value(key: str, index: int) -> str:
        values = recent.get(key) or []
        return str(values[index] or "").strip() if index < len(values) else ""

    for index, raw_form in enumerate(forms):
        form = str(raw_form).upper().strip()
        if form not in MATERIAL_FORMS:
            continue
        filing_date = _iso_date(value("filingDate", index))
        if filing_date is not None and filing_date < cutoff:
            continue
        accession = value("accessionNumber", index)
        primary_document = value("primaryDocument", index).rsplit("/", 1)[-1]
        if not accession or not primary_document:
            continue
        accession_compact = accession.replace("-", "")
        archive_url = (
            f"https://www.sec.gov/Archives/edgar/data/{int(profile.cik)}/"
            f"{accession_compact}/{primary_document}"
        )
        acceptance = _iso_datetime(value("acceptanceDateTime", index))
        published_at = acceptance or datetime.combine(
            filing_date or now.date(),
            datetime.min.time(),
            timezone.utc,
        )
        items = value("items", index)
        description = value("primaryDocDescription", index) or f"{form} filing"
        candidates.append(
            RawEvidenceCandidate(
                ticker=profile.ticker,
                kind=EvidenceKind.INSIDER if form.startswith("4") else EvidenceKind.SEC,
                headline=description,
                source_name="SEC EDGAR",
                source_url=archive_url,
                published_at=published_at,
                source_text=" | ".join(part for part in (description, items) if part),
                external_id=accession,
                metadata={
                    "accession": accession,
                    "form": form,
                    "filing_date": filing_date.isoformat() if filing_date else "",
                    "report_date": value("reportDate", index),
                    "items": items,
                },
            )
        )
        if len(candidates) == limit:
            break
    return tuple(candidates)


def parse_yahoo_sec_filings(
    rows: Sequence[Mapping[str, Any]],
    profile: EvidenceProfile,
    *,
    limit: int,
    lookback_days: int,
    now: datetime,
) -> tuple[RawEvidenceCandidate, ...]:
    cutoff = now.date() - timedelta(days=lookback_days)
    candidates = []
    for row in rows:
        form = str(row.get("type") or "").upper().strip()
        if form not in MATERIAL_FORMS:
            continue
        filing_date = _coerce_date(row.get("date"))
        if filing_date is not None and filing_date < cutoff:
            continue
        edgar_url = str(row.get("edgarUrl") or "")
        exhibits = row.get("exhibits") if isinstance(row.get("exhibits"), dict) else {}
        source_url = str(exhibits.get(form) or "") or edgar_url
        accession_match = re.search(r"(\d{10}-\d{2}-\d{6})", edgar_url)
        accession = accession_match.group(1) if accession_match else edgar_url
        title = str(row.get("title") or f"{form} filing")
        items = ",".join(dict.fromkeys(re.findall(r"\b(\d\.\d{2})\b", title)))
        published_at = datetime.combine(
            filing_date or now.date(),
            datetime.min.time(),
            timezone.utc,
        )
        candidates.append(
            RawEvidenceCandidate(
                ticker=profile.ticker,
                kind=EvidenceKind.INSIDER if form.startswith("4") else EvidenceKind.SEC,
                headline=title,
                source_name="Yahoo SEC mirror",
                source_url=source_url,
                published_at=published_at,
                source_text=" | ".join(part for part in (title, items) if part),
                external_id=accession,
                metadata={
                    "accession": accession,
                    "form": form,
                    "filing_date": filing_date.isoformat() if filing_date else "",
                    "report_date": "",
                    "items": items,
                    "document_urls": tuple(
                        dict.fromkeys(
                            str(url)
                            for url in (source_url, *exhibits.values())
                            if str(url).startswith(("https://", "http://"))
                        )
                    ),
                },
            )
        )
        if len(candidates) == limit:
            break
    return tuple(candidates)


class SecFilingTextClient:
    def __init__(
        self,
        contact: str,
        *,
        get: Callable[..., Any] | None = None,
        timeout: float = 20,
        max_chars: int = 24_000,
    ) -> None:
        self.contact = contact.strip()
        self._get = get or requests.Session().get
        self.timeout = timeout
        self.max_chars = max_chars

    def fetch(self, candidate: EvidenceCandidate) -> str:
        raw_urls = candidate.metadata.get("document_urls") or (candidate.source_url,)
        urls = [str(url) for url in raw_urls if str(url).startswith("http")]
        form = str(candidate.metadata.get("form") or "")
        if form.startswith(("8-K", "6-K")):
            urls.sort(key=lambda value: ("99" not in value.lower(), value))
        errors = []
        parts = []
        for url in urls[:3]:
            try:
                response = self._get(
                    url,
                    headers={
                        "User-Agent": f"investing-monitor/2.0 {self.contact}",
                        "From": self.contact,
                        "Accept": "text/html,application/xhtml+xml,application/xml",
                    },
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                errors.append(str(exc))
                continue
            if response.status_code in (403, 429):
                errors.append(f"HTTP {response.status_code}")
                continue
            if response.status_code != 200:
                errors.append(f"HTTP {response.status_code}")
                continue
            text = parse_sec_document_text(response.text, max_chars=200_000)
            if text:
                parts.append(
                    select_filing_analysis_text(
                        text,
                        form=form,
                        max_chars=self.max_chars - sum(map(len, parts)),
                    )
                )
            if sum(map(len, parts)) >= self.max_chars:
                break
        combined = " ".join(parts)[: self.max_chars]
        if len(combined) < 200:
            raise SecFilingError(
                "SEC filing body unavailable: "
                + ("; ".join(errors[-3:]) or "insufficient document text")
            )
        return combined


def parse_sec_document_text(payload: str, *, max_chars: int = 24_000) -> str:
    parser = _SecDocumentParser(max_chars=max_chars)
    parser.feed(payload)
    parser.close()
    return " ".join(" ".join(parser.parts).split())[:max_chars]


def select_filing_analysis_text(
    text: str,
    *,
    form: str,
    max_chars: int = 24_000,
) -> str:
    """Put decision-useful periodic-report text before tables and boilerplate."""
    normalized_form = form.upper()
    if normalized_form.startswith(("10-Q", "10-K", "20-F")):
        item_pattern = (
            r"item\s+2[.\s:-]+management(?:'|’)?s\s+discussion"
            if normalized_form.startswith("10-Q")
            else r"item\s+7[.\s:-]+management(?:'|’)?s\s+discussion"
        )
        starts = [match.start() for match in re.finditer(item_pattern, text, re.I)]
        if starts:
            discussion = text[starts[-1] :]
            end_match = re.search(
                r"item\s+(?:3|7a|8)[.\s:-]+",
                discussion[200:],
                re.I,
            )
            if end_match:
                discussion = discussion[: 200 + end_match.start()]
            identity = text[:1_200]
            return f"{identity} {discussion}"[:max_chars]
    return text[:max_chars]


class _SecDocumentParser(HTMLParser):
    def __init__(self, *, max_chars: int) -> None:
        super().__init__(convert_charrefs=True)
        self.max_chars = max_chars
        self.ignored_stack: list[str] = []
        self.collected_chars = 0
        self.parts: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: Sequence[tuple[str, str | None]],
    ) -> None:
        attributes = {key.lower(): (value or "") for key, value in attrs}
        style = re.sub(r"\s+", "", attributes.get("style", "").lower())
        if self.ignored_stack or (
            tag in {"script", "style", "noscript", "svg", "ix:header"}
            or "display:none" in style
            or "visibility:hidden" in style
            or "hidden" in attributes
            or attributes.get("aria-hidden", "").lower() == "true"
        ):
            self.ignored_stack.append(tag)

    def handle_endtag(self, tag: str) -> None:
        if self.ignored_stack and self.ignored_stack[-1] == tag:
            self.ignored_stack.pop()

    def handle_data(self, data: str) -> None:
        if self.ignored_stack or self.collected_chars >= self.max_chars:
            return
        value = data.strip()
        if value:
            self.parts.append(value)
            self.collected_chars += len(value)


def _iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _coerce_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return _iso_date(str(value or "")[:10])


def _iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None
