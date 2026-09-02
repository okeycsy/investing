from __future__ import annotations

import re
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from urllib.parse import urlsplit

import requests

from investing_monitor.domain.evidence import (
    EvidenceKind,
    EvidenceProfile,
    RawEvidenceCandidate,
)


FEED_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/rss+xml,application/atom+xml,application/xml,text/xml,*/*",
}


class EvidenceFeedError(RuntimeError):
    pass


class FeedClient:
    def __init__(
        self,
        *,
        get: Callable[..., object] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        timeout: float = 15,
    ) -> None:
        self._get = get or requests.Session().get
        self._sleep = sleeper
        self.timeout = timeout

    def fetch(
        self,
        url: str,
        *,
        profile: EvidenceProfile,
        kind: EvidenceKind,
        default_source: str,
    ) -> tuple[RawEvidenceCandidate, ...]:
        errors: list[str] = []
        for attempt, delay in enumerate((0, 15)):
            if delay:
                self._sleep(delay)
            try:
                response = self._get(
                    url,
                    headers=FEED_HEADERS,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                errors.append(str(exc))
                continue
            if response.status_code == 200:
                try:
                    return parse_evidence_feed(
                        response.text,
                        profile=profile,
                        kind=kind,
                        default_source=default_source,
                    )
                except (ET.ParseError, ValueError) as exc:
                    raise EvidenceFeedError(f"invalid feed payload: {exc}") from exc
            if response.status_code in (429, 503) and attempt == 0:
                errors.append(f"HTTP {response.status_code}")
                continue
            raise EvidenceFeedError(f"feed rejected with HTTP {response.status_code}")
        raise EvidenceFeedError(
            f"feed unavailable: {'; '.join(errors[-2:]) or 'no response'}"
        )


class InvestorRelationsFeedAdapter:
    def __init__(self, client: FeedClient | None = None) -> None:
        self.client = client or FeedClient()

    def fetch(self, profile: EvidenceProfile) -> tuple[RawEvidenceCandidate, ...]:
        if not profile.ir_news_url:
            return ()
        return self.client.fetch(
            profile.ir_news_url,
            profile=profile,
            kind=EvidenceKind.IR,
            default_source=f"{profile.company_name} IR",
        )


def parse_evidence_feed(
    payload: str,
    *,
    profile: EvidenceProfile,
    kind: EvidenceKind,
    default_source: str,
) -> tuple[RawEvidenceCandidate, ...]:
    root = ET.fromstring(payload)
    if _local_name(root.tag) == "feed":
        records = [_atom_record(item) for item in _children(root, "entry")]
    else:
        records = [_rss_record(item) for item in root.findall(".//item")]
    candidates = []
    for record in records:
        link = record.get("link", "")
        source = record.get("source", "") or _source_from_url(link) or default_source
        candidates.append(
            RawEvidenceCandidate(
                ticker=profile.ticker,
                kind=kind,
                headline=record.get("title", ""),
                source_name=source,
                source_url=link,
                published_at=_parse_feed_date(record.get("published", "")),
                source_text=_plain_text(record.get("summary", "")),
                external_id=record.get("external_id", ""),
            )
        )
    return tuple(candidates)


def _rss_record(item: ET.Element) -> dict[str, str]:
    return {
        "title": item.findtext("title", "").strip(),
        "link": item.findtext("link", "").strip(),
        "published": item.findtext("pubDate", "").strip(),
        "summary": item.findtext("description", "").strip(),
        "source": item.findtext("source", "").strip(),
        "external_id": item.findtext("guid", "").strip(),
    }


def _atom_record(item: ET.Element) -> dict[str, str]:
    children = {_local_name(child.tag): child for child in item}
    link_element = children.get("link")
    link = link_element.attrib.get("href", "") if link_element is not None else ""
    summary = children.get("summary")
    if summary is None:
        summary = children.get("content")
    published = children.get("published")
    if published is None:
        published = children.get("updated")
    return {
        "title": _element_text(children.get("title")),
        "link": link.strip(),
        "published": _element_text(published),
        "summary": _element_text(summary),
        "source": "",
        "external_id": _element_text(children.get("id")),
    }


def _children(root: ET.Element, name: str) -> list[ET.Element]:
    return [child for child in root if _local_name(child.tag) == name]


def _element_text(element: ET.Element | None) -> str:
    return "" if element is None else "".join(element.itertext()).strip()


def _local_name(value: str) -> str:
    return value.rsplit("}", 1)[-1]


def _parse_feed_date(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _plain_text(value: str) -> str:
    without_markup = re.sub(r"<[^>]+>", " ", unescape(value))
    return " ".join(without_markup.split())


def _source_from_url(value: str) -> str:
    try:
        host = urlsplit(value).netloc.lower().removeprefix("www.")
    except ValueError:
        return ""
    return host
