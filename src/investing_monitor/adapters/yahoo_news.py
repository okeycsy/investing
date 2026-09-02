from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any

import requests

from investing_monitor.adapters.evidence_feeds import FeedClient
from investing_monitor.adapters.yahoo_market_data import YAHOO_HEADERS
from investing_monitor.domain.evidence import (
    EvidenceKind,
    EvidenceProfile,
    RawEvidenceCandidate,
)


YAHOO_SEARCH_HOSTS = (
    "https://query1.finance.yahoo.com/v1/finance/search",
    "https://query2.finance.yahoo.com/v1/finance/search",
)
YAHOO_RSS_URL = (
    "https://feeds.finance.yahoo.com/rss/2.0/headline"
    "?s={ticker}&region=US&lang=en-US"
)


class YahooNewsError(RuntimeError):
    pass


class YahooNewsSearchClient:
    def __init__(
        self,
        *,
        get: Callable[..., Any] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        timeout: float = 15,
    ) -> None:
        self._get = get or requests.Session().get
        self._sleep = sleeper
        self.timeout = timeout

    def fetch(self, profile: EvidenceProfile) -> tuple[RawEvidenceCandidate, ...]:
        errors: list[str] = []
        for attempt, url in enumerate(YAHOO_SEARCH_HOSTS):
            if attempt:
                self._sleep(15)
            try:
                response = self._get(
                    url,
                    params={
                        "q": profile.ticker,
                        "quotesCount": "1",
                        "newsCount": "15",
                        "enableFuzzyQuery": "false",
                    },
                    headers=YAHOO_HEADERS,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                errors.append(str(exc))
                continue
            if response.status_code == 200:
                try:
                    return parse_yahoo_search(response.json(), profile)
                except (ValueError, TypeError, KeyError) as exc:
                    raise YahooNewsError(f"invalid Yahoo news payload: {exc}") from exc
            if response.status_code in (429, 503):
                errors.append(f"HTTP {response.status_code}")
                continue
            raise YahooNewsError(f"Yahoo news rejected with HTTP {response.status_code}")
        raise YahooNewsError(
            f"Yahoo news unavailable: {'; '.join(errors[-2:]) or 'no response'}"
        )


class YahooNewsAdapter:
    def __init__(
        self,
        search_client: YahooNewsSearchClient | None = None,
        feed_client: FeedClient | None = None,
    ) -> None:
        self.search_client = search_client or YahooNewsSearchClient()
        self.feed_client = feed_client or FeedClient()

    def fetch(self, profile: EvidenceProfile) -> tuple[RawEvidenceCandidate, ...]:
        try:
            return self.search_client.fetch(profile)
        except YahooNewsError as search_error:
            try:
                return self.feed_client.fetch(
                    YAHOO_RSS_URL.format(ticker=profile.ticker),
                    profile=profile,
                    kind=EvidenceKind.NEWS,
                    default_source="Yahoo Finance",
                )
            except Exception as feed_error:
                raise YahooNewsError(
                    f"Yahoo Search and RSS failed: {search_error}; {feed_error}"
                ) from feed_error


class YahooArticleTextClient:
    def __init__(
        self,
        *,
        get: Callable[..., Any] | None = None,
        timeout: float = 15,
        max_chars: int = 12_000,
    ) -> None:
        self._get = get or requests.Session().get
        self.timeout = timeout
        self.max_chars = max_chars

    def fetch(self, url: str) -> str:
        try:
            response = self._get(url, headers=YAHOO_HEADERS, timeout=self.timeout)
        except requests.RequestException as exc:
            raise YahooNewsError(f"Yahoo article unavailable: {exc}") from exc
        if response.status_code != 200:
            raise YahooNewsError(
                f"Yahoo article rejected with HTTP {response.status_code}"
            )
        return parse_yahoo_article_text(response.text, max_chars=self.max_chars)


def parse_yahoo_search(
    payload: Mapping[str, Any],
    profile: EvidenceProfile,
) -> tuple[RawEvidenceCandidate, ...]:
    candidates = []
    for item in payload.get("news") or []:
        timestamp = item.get("providerPublishTime")
        published_at = (
            datetime.fromtimestamp(int(timestamp), timezone.utc)
            if timestamp
            else None
        )
        related = tuple(
            str(symbol).upper() for symbol in item.get("relatedTickers") or []
        )
        candidates.append(
            RawEvidenceCandidate(
                ticker=profile.ticker,
                kind=EvidenceKind.NEWS,
                headline=str(item.get("title") or ""),
                source_name=str(item.get("publisher") or "Yahoo Finance"),
                source_url=str(item.get("link") or ""),
                published_at=published_at,
                source_text=str(item.get("title") or ""),
                external_id=str(item.get("uuid") or ""),
                metadata={"related_tickers": related},
            )
        )
    return tuple(candidates)


def parse_yahoo_article_text(payload: str, *, max_chars: int = 12_000) -> str:
    parser = _YahooArticleBodyParser(max_chars=max_chars)
    parser.feed(payload)
    parser.close()
    text = " ".join(" ".join(parser.parts).split())
    if not text:
        raise YahooNewsError("Yahoo article contains no structured article body")
    return text[:max_chars]


class _YahooArticleBodyParser(HTMLParser):
    VOID_TAGS = {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }

    def __init__(self, *, max_chars: int) -> None:
        super().__init__(convert_charrefs=True)
        self.max_chars = max_chars
        self.body_depth = 0
        self.ignored_depth = 0
        self.parts: list[str] = []
        self.collected_chars = 0

    def handle_starttag(
        self,
        tag: str,
        attrs: Sequence[tuple[str, str | None]],
    ) -> None:
        attributes = dict(attrs)
        if self.body_depth == 0 and attributes.get("data-testid") == "article-body":
            self.body_depth = 1
            return
        if self.body_depth:
            if tag not in self.VOID_TAGS:
                self.body_depth += 1
            if tag in {"script", "style", "noscript", "svg"}:
                self.ignored_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if not self.body_depth:
            return
        if self.ignored_depth and tag in {"script", "style", "noscript", "svg"}:
            self.ignored_depth -= 1
        self.body_depth -= 1

    def handle_data(self, data: str) -> None:
        if (
            self.body_depth
            and not self.ignored_depth
            and self.collected_chars < self.max_chars
        ):
            value = data.strip()
            if value:
                self.parts.append(value)
                self.collected_chars += len(value)
