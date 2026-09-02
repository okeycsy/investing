from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from investing_monitor.adapters.anthropic_evidence import (
    EvidenceAnalysisBatch,
    parse_analysis_response,
)
from investing_monitor.adapters.config import load_evidence_profile
from investing_monitor.adapters.evidence_feeds import parse_evidence_feed
from investing_monitor.adapters.sec_filings import (
    ResilientSecFilingsAdapter,
    SecAccessBlocked,
    SecFetchResult,
    SecSubmissionsClient,
    parse_sec_document_text,
    parse_sec_submissions,
    select_filing_analysis_text,
)
from investing_monitor.adapters.sqlite_repository import SQLiteMonitorRepository
from investing_monitor.adapters.yahoo_news import (
    parse_yahoo_article_text,
    parse_yahoo_search,
)
from investing_monitor.application.evidence import (
    EvidenceIngestionService,
    cluster_candidates,
    screen_candidate,
)
from investing_monitor.application.sec_monitor import SecMonitorService
from investing_monitor.domain.evidence import (
    EvidenceAnalysis,
    EvidenceKind,
    EvidenceProfile,
    EvidenceStatus,
    GroundedFact,
    RawEvidenceCandidate,
)
from investing_monitor.domain.models import (
    MarketCycle,
    MarketFrame,
    MarketSession,
    MarketSnapshot,
)
from investing_monitor.application.monitor import MarketCycleService
from investing_monitor.presentation.evidence_messages import build_evidence_message


NOW = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)
PROFILE = EvidenceProfile(
    ticker="VRT",
    company_name="Vertiv Holdings Co",
    cik="1674101",
    aliases=("Vertiv",),
)


def raw(
    headline: str,
    *,
    kind: EvidenceKind = EvidenceKind.NEWS,
    minute: int = 0,
    source_name: str = "Reuters",
    source_url: str = "https://example.com/story?utm_source=test",
    source_text: str = "Verified source text.",
) -> RawEvidenceCandidate:
    return RawEvidenceCandidate(
        ticker="VRT",
        kind=kind,
        headline=headline,
        source_name=source_name,
        source_url=source_url,
        published_at=NOW + timedelta(minutes=minute),
        source_text=source_text,
    )


class CandidateScreeningTest(unittest.TestCase):
    def test_markdown_loads_evidence_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "monitor.md"
            path.write_text(
                "ticker: $AMD\ncompany_name: Advanced Micro Devices\n"
                "cik: 2488\ncompany_aliases: AMD, Advanced Micro Devices\n"
                "ir_news_url: https://ir.amd.com/rss\n"
                "sec_contact: owner@example.com\n",
                encoding="utf-8",
            )

            profile = load_evidence_profile(path)

            self.assertEqual(profile.ticker, "AMD")
            self.assertEqual(profile.cik, "0000002488")
            self.assertEqual(profile.aliases, ("Advanced Micro Devices", "AMD"))
            self.assertEqual(profile.sec_contact, "owner@example.com")

    def test_missing_traceability_is_quarantined(self):
        decision = screen_candidate(raw("Vertiv expands capacity", source_url=""), PROFILE)

        self.assertEqual(decision.status, EvidenceStatus.QUARANTINED)
        self.assertIn("source_url", decision.reason)
        self.assertIsNone(decision.candidate)

    def test_low_value_title_is_filtered_before_ai(self):
        decision = screen_candidate(
            raw("Is Vertiv or Eaton the Better Buy Right Now?"),
            PROFILE,
        )

        self.assertEqual(decision.status, EvidenceStatus.FILTERED)
        self.assertIsNotNone(decision.candidate)

    def test_routine_ir_dividend_is_filtered_before_ai(self):
        decision = screen_candidate(
            raw(
                "Vertiv Declares Quarterly Dividend",
                kind=EvidenceKind.IR,
                source_name="Vertiv IR",
            ),
            PROFILE,
        )

        self.assertEqual(decision.status, EvidenceStatus.FILTERED)

    def test_canonical_identity_ignores_tracking_query(self):
        first = screen_candidate(raw("Vertiv expands capacity"), PROFILE).candidate
        second = screen_candidate(
            raw(
                "Vertiv expands capacity",
                source_url="https://example.com/story?utm_campaign=duplicate",
            ),
            PROFILE,
        ).candidate

        self.assertEqual(first.candidate_id, second.candidate_id)
        self.assertEqual(first.source_url, "https://example.com/story")

    def test_source_identity_survives_publisher_headline_edit(self):
        first = screen_candidate(
            raw("Vertiv announces an acquisition"),
            PROFILE,
        ).candidate
        second = screen_candidate(
            raw("Vertiv announces a major acquisition"),
            PROFILE,
        ).candidate

        self.assertEqual(first.candidate_id, second.candidate_id)


class EvidenceClusteringTest(unittest.TestCase):
    def test_same_event_within_fifteen_minutes_prefers_official_ir(self):
        news = screen_candidate(
            raw("Vertiv acquires ThermoKey to expand cooling portfolio"),
            PROFILE,
        ).candidate
        ir = screen_candidate(
            raw(
                "Vertiv completes acquisition of ThermoKey",
                kind=EvidenceKind.IR,
                minute=8,
                source_name="Vertiv IR",
                source_url="https://investors.vertiv.com/thermokey",
                source_text="Vertiv completed its acquisition of ThermoKey.",
            ),
            PROFILE,
        ).candidate

        clusters = cluster_candidates([news, ir])

        self.assertEqual(len(clusters), 1)
        self.assertEqual(len(clusters[0].candidates), 2)
        self.assertEqual(clusters[0].representative.kind, EvidenceKind.IR)

    def test_different_event_is_separate_while_late_duplicate_clusters(self):
        acquisition = screen_candidate(
            raw("Vertiv completes acquisition of ThermoKey"), PROFILE
        ).candidate
        guidance = screen_candidate(
            raw("Vertiv raises full year earnings guidance", minute=5), PROFILE
        ).candidate
        late_copy = screen_candidate(
            raw("Vertiv acquires ThermoKey", minute=16), PROFILE
        ).candidate

        clusters = cluster_candidates([acquisition, guidance, late_copy])

        self.assertEqual(len(clusters), 2)
        self.assertEqual(sorted(len(cluster.candidates) for cluster in clusters), [1, 2])

    def test_high_similarity_syndication_clusters_across_same_day(self):
        official = screen_candidate(
            raw(
                "Vertiv Announces Agreement to Acquire UtilityInnovation Group",
                kind=EvidenceKind.IR,
                source_name="Vertiv IR",
            ),
            PROFILE,
        ).candidate
        wire = screen_candidate(
            raw(
                "Vertiv Unit to Acquire Utility Innovation for $1.45 Billion",
                minute=22,
                source_name="MT Newswires",
            ),
            PROFILE,
        ).candidate
        follow_up = screen_candidate(
            raw(
                "The Sentiment Behind Vertiv's $1.45 Billion Deal",
                minute=120,
                source_name="Barrons.com",
            ),
            PROFILE,
        ).candidate

        clusters = cluster_candidates([official, wire, follow_up])

        self.assertEqual(len(clusters), 1)
        self.assertEqual(len(clusters[0].candidates), 3)
        self.assertEqual(clusters[0].representative.kind, EvidenceKind.IR)

    def test_empty_ir_body_yields_to_news_wire_with_source_text(self):
        official = screen_candidate(
            raw(
                "Vertiv acquires UtilityInnovation Group",
                kind=EvidenceKind.IR,
                source_name="Vertiv IR",
                source_text="",
            ),
            PROFILE,
        ).candidate
        wire = screen_candidate(
            raw(
                "Vertiv to acquire Utility Innovation Group",
                minute=5,
                source_name="PR Newswire",
                source_text="Vertiv to acquire Utility Innovation Group",
            ),
            PROFILE,
        ).candidate

        cluster = cluster_candidates([official, wire])[0]

        self.assertEqual(cluster.representative.source_name, "PR Newswire")


class EvidenceFeedTest(unittest.TestCase):
    def test_yahoo_search_normalizes_news_without_rss(self):
        payload = {
            "news": [
                {
                    "uuid": "story-123",
                    "title": "Vertiv announces a major cooling order",
                    "publisher": "Reuters",
                    "link": "https://finance.yahoo.com/story-123",
                    "providerPublishTime": int(NOW.timestamp()),
                    "relatedTickers": ["VRT", "ETN"],
                }
            ]
        }

        candidates = parse_yahoo_search(payload, PROFILE)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].external_id, "story-123")
        self.assertEqual(candidates[0].published_at, NOW)
        self.assertEqual(candidates[0].metadata["related_tickers"], ("VRT", "ETN"))

    def test_yahoo_article_parser_reads_only_structured_body(self):
        payload = """
        <html><head><script>ignore this</script></head><body>
        <nav>navigation noise</nav>
        <div data-testid="article-body">
          <p>Vertiv agreed to acquire UtilityInnovation Group.</p>
          <p>The announced cash consideration is $1.45 billion.</p>
          <script>tracking noise</script>
        </div><footer>footer noise</footer></body></html>
        """

        text = parse_yahoo_article_text(payload)

        self.assertIn("acquire UtilityInnovation", text)
        self.assertIn("$1.45 billion", text)
        self.assertNotIn("navigation", text)
        self.assertNotIn("tracking", text)

    def test_rss_normalizes_required_candidate_fields(self):
        payload = """<?xml version="1.0"?>
        <rss><channel><item>
          <title>Vertiv expands liquid cooling capacity</title>
          <link>https://news.example.com/vrt-capacity?utm_source=yahoo</link>
          <pubDate>Wed, 02 Sep 2026 12:00:00 GMT</pubDate>
          <description><![CDATA[<p>Capacity will double by year end.</p>]]></description>
          <source>Reuters</source><guid>story-123</guid>
        </item></channel></rss>"""

        candidates = parse_evidence_feed(
            payload,
            profile=PROFILE,
            kind=EvidenceKind.NEWS,
            default_source="Yahoo Finance",
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].source_name, "Reuters")
        self.assertEqual(candidates[0].source_text, "Capacity will double by year end.")
        self.assertEqual(candidates[0].published_at, NOW)
        self.assertEqual(candidates[0].external_id, "story-123")

    def test_atom_is_supported_for_company_ir(self):
        payload = """<?xml version="1.0"?>
        <feed xmlns="http://www.w3.org/2005/Atom">
          <entry><id>release-1</id><title>Vertiv raises guidance</title>
          <link href="https://investors.vertiv.com/release-1"/>
          <updated>2026-09-02T12:00:00Z</updated>
          <summary>Revenue guidance increased.</summary></entry>
        </feed>"""

        candidates = parse_evidence_feed(
            payload,
            profile=PROFILE,
            kind=EvidenceKind.IR,
            default_source="Vertiv IR",
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].kind, EvidenceKind.IR)
        self.assertEqual(candidates[0].source_name, "investors.vertiv.com")

    def test_sec_document_parser_excludes_scripts_and_keeps_facts(self):
        payload = """
        <html><body><script>tracking noise</script>
        <p>Item 1.01 Entry into a Material Definitive Agreement.</p>
        <p>Cash consideration is approximately $1.45 billion.</p>
        </body></html>
        """

        text = parse_sec_document_text(payload)

        self.assertIn("Material Definitive Agreement", text)
        self.assertIn("$1.45 billion", text)
        self.assertNotIn("tracking", text)

    def test_sec_document_parser_excludes_hidden_ixbrl_metadata(self):
        payload = """
        <html><body><div style="display:none"><ix:header>
        <xbrli:context>metadata noise</xbrli:context>
        </ix:header></div>
        <p>Net sales increased due to data center demand.</p>
        </body></html>
        """

        text = parse_sec_document_text(payload)

        self.assertEqual(text, "Net sales increased due to data center demand.")

    def test_periodic_filing_prioritizes_real_mda_over_table_of_contents(self):
        payload = (
            "FORM 10-Q TABLE OF CONTENTS Item 2. Management's Discussion "
            "and Analysis page 31. "
            + "financial table " * 500
            + "Item 2. Management's Discussion and Analysis "
            + "Backlog grew and full-year guidance increased. " * 100
            + "Item 3. Quantitative and Qualitative Disclosures"
        )

        text = select_filing_analysis_text(payload, form="10-Q", max_chars=4_000)

        self.assertIn("Backlog grew and full-year guidance increased", text)
        self.assertLess(text.find("Backlog grew"), 1_500)


class EvidenceLedgerTest(unittest.TestCase):
    def test_filtered_quarantined_and_pending_candidates_are_persisted(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            decisions = [
                screen_candidate(raw("Vertiv expands cooling capacity"), PROFILE),
                screen_candidate(raw("Vertiv price target raised"), PROFILE),
                screen_candidate(raw("Vertiv signs major order", source_url=""), PROFILE),
            ]
            candidates = [
                decision.candidate
                for decision in decisions
                if decision.candidate is not None
            ]
            clusters = cluster_candidates(candidates)
            cluster_keys = {
                candidate.candidate_id: cluster.cluster_key
                for cluster in clusters
                for candidate in cluster.candidates
            }

            inserted = repository.record_evidence_decisions(
                decisions,
                cluster_keys,
                NOW,
            )

            self.assertEqual(len(inserted), 1)
            self.assertEqual(
                [item.headline for item in repository.pending_evidence_candidates(NOW)],
                ["Vertiv expands cooling capacity"],
            )

    def test_ai_failure_retries_and_success_is_not_consumed_again(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            decision = screen_candidate(raw("Vertiv expands cooling capacity"), PROFILE)
            candidate_id = decision.candidate.candidate_id
            repository.record_evidence_decisions([decision], {}, NOW)
            repository.mark_evidence_failed(
                candidate_id,
                attempted_at=NOW,
                next_attempt_at=NOW + timedelta(minutes=5),
                error="provider unavailable",
            )

            self.assertEqual(repository.pending_evidence_candidates(NOW), [])
            retried = repository.pending_evidence_candidates(NOW + timedelta(minutes=5))
            self.assertEqual([item.candidate_id for item in retried], [candidate_id])

            repository.mark_evidence_analyzed(
                candidate_id,
                EvidenceAnalysis(candidate_id=candidate_id, relevant=False),
                NOW + timedelta(minutes=5),
            )
            repository.record_evidence_decisions(
                [decision],
                {},
                NOW + timedelta(minutes=10),
            )

            self.assertEqual(
                repository.pending_evidence_candidates(NOW + timedelta(minutes=10)),
                [],
            )


class EvidenceAnalysisValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        decision = screen_candidate(
            raw(
                "Vertiv announces UtilityInnovation acquisition",
                source_text=(
                    "Vertiv agreed to acquire UtilityInnovation Group for "
                    "$1.45 billion in cash."
                ),
            ),
            PROFILE,
        )
        self.candidate = decision.candidate

    def test_grounded_structured_analysis_is_accepted(self):
        response = json.dumps(
            [
                {
                    "candidate_id": self.candidate.candidate_id,
                    "relevant": True,
                    "headline_ko": "버티브, 마이크로그리드 업체 인수 합의",
                    "summary_ko": "버티브가 유틸리티이노베이션 그룹 인수에 합의했다.",
                    "facts": [
                        {
                            "source_text": (
                                "Vertiv agreed to acquire UtilityInnovation Group "
                                "for $1.45 billion in cash."
                            ),
                            "fact_ko": "현금 14.5억 달러 규모의 인수에 합의했다.",
                        }
                    ],
                    "interpretation_ko": "전력 제약 대응 역량을 넓히는 거래다.",
                    "thesis_impact": "strengthen",
                    "impact_reason_ko": "데이터센터 전력 솔루션 범위가 확대된다.",
                    "confidence": "high",
                }
            ],
            ensure_ascii=False,
        )

        batch = parse_analysis_response(response, [self.candidate])

        self.assertEqual(batch.errors, {})
        self.assertTrue(batch.analyses[self.candidate.candidate_id].relevant)

    def test_unsupported_fact_fails_only_that_candidate(self):
        response = json.dumps(
            [
                {
                    "candidate_id": self.candidate.candidate_id,
                    "relevant": True,
                    "headline_ko": "인수 발표",
                    "summary_ko": "인수를 발표했다.",
                    "facts": [
                        {
                            "source_text": "The acquisition doubles annual revenue.",
                            "fact_ko": "연간 매출이 두 배가 된다.",
                        }
                    ],
                    "interpretation_ko": "성장에 기여할 수 있다.",
                    "thesis_impact": "strengthen",
                    "impact_reason_ko": "외형이 확대된다.",
                    "confidence": "high",
                }
            ],
            ensure_ascii=False,
        )

        batch = parse_analysis_response(response, [self.candidate])

        self.assertEqual(batch.analyses, {})
        self.assertIn("not present", batch.errors[self.candidate.candidate_id])

    def test_medium_confidence_damage_is_downgraded_to_risk(self):
        response = json.dumps(
            [
                {
                    "candidate_id": self.candidate.candidate_id,
                    "relevant": True,
                    "headline_ko": "인수 발표",
                    "summary_ko": "인수를 발표했다.",
                    "facts": [
                        {
                            "source_text": "Vertiv agreed to acquire UtilityInnovation Group",
                            "fact_ko": "인수에 합의했다.",
                        }
                    ],
                    "interpretation_ko": "통합 위험이 있다.",
                    "thesis_impact": "damage",
                    "impact_reason_ko": "통합 불확실성이 있다.",
                    "confidence": "medium",
                }
            ],
            ensure_ascii=False,
        )

        batch = parse_analysis_response(response, [self.candidate])

        self.assertEqual(
            batch.analyses[self.candidate.candidate_id].thesis_impact,
            "risk",
        )

    def test_evidence_message_user_text_stays_below_limit(self):
        analysis = EvidenceAnalysis(
            candidate_id=self.candidate.candidate_id,
            relevant=True,
            headline_ko="긴 제목 " * 100,
            summary_ko="긴 요약 " * 500,
            facts=tuple(
                GroundedFact("source", "긴 핵심 사실 " * 200) for _ in range(3)
            ),
            interpretation_ko="긴 해석 " * 400,
            thesis_impact="risk",
            impact_reason_ko="긴 이유 " * 300,
            confidence="high",
        )

        payload = build_evidence_message(self.candidate, analysis)
        visible_text = payload["text"] + "".join(
            block.get("text", {}).get("text", "")
            + "".join(element.get("text", "") for element in block.get("elements", []))
            for block in payload["blocks"]
        )

        self.assertLessEqual(len(visible_text), 2_900)


class EvidenceIngestionServiceTest(unittest.TestCase):
    def test_cluster_duplicate_is_not_analyzed_and_representative_is_enriched(self):
        class Analyzer:
            def __init__(self):
                self.candidates = []

            def analyze(self, candidates, _profile):
                self.candidates.extend(candidates)
                return EvidenceAnalysisBatch(
                    analyses={
                        candidate.candidate_id: EvidenceAnalysis(
                            candidate_id=candidate.candidate_id,
                            relevant=False,
                        )
                        for candidate in candidates
                    },
                    errors={},
                )

        class Articles:
            def __init__(self):
                self.urls = []

            def fetch(self, url):
                self.urls.append(url)
                return "Vertiv completed the acquisition and expanded cooling capacity."

        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            analyzer = Analyzer()
            articles = Articles()
            service = EvidenceIngestionService(
                repository,
                PROFILE,
                analyzer,
                article_text=articles,
            )
            candidates = [
                raw(
                    "Vertiv acquires ThermoKey to expand cooling portfolio",
                    source_name="Reuters",
                    source_url="https://finance.yahoo.com/reuters-thermokey",
                    source_text="Vertiv acquires ThermoKey to expand cooling portfolio",
                ),
                raw(
                    "Vertiv completes acquisition of ThermoKey",
                    minute=5,
                    source_name="PR Newswire",
                    source_url="https://finance.yahoo.com/pr-thermokey",
                    source_text="Vertiv completes acquisition of ThermoKey",
                ),
            ]

            report = service.ingest(candidates, NOW + timedelta(minutes=6))

            self.assertEqual(report.inserted_pending, 1)
            self.assertEqual(report.filtered, 1)
            self.assertEqual(report.enriched, 1)
            self.assertEqual(report.analyzed, 1)
            self.assertEqual(len(analyzer.candidates), 1)
            self.assertEqual(analyzer.candidates[0].source_name, "PR Newswire")
            self.assertEqual(articles.urls, ["https://finance.yahoo.com/pr-thermokey"])

    def test_old_news_is_ledgered_without_ai_call(self):
        class Analyzer:
            def analyze(self, _candidates, _profile):
                raise AssertionError("old news must not reach AI")

        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            service = EvidenceIngestionService(repository, PROFILE, Analyzer())

            report = service.ingest(
                [raw("Vertiv old product release")],
                NOW + timedelta(hours=25),
            )

            self.assertEqual(report.filtered, 1)
            self.assertEqual(report.inserted_pending, 0)

    def test_relevant_analysis_creates_atomic_alert_and_recent_catalyst(self):
        class Analyzer:
            def analyze(self, candidates, _profile):
                candidate = candidates[0]
                analysis = EvidenceAnalysis(
                    candidate_id=candidate.candidate_id,
                    relevant=True,
                    headline_ko="버티브, 냉각 생산능력 확대",
                    summary_ko="생산능력을 두 배로 확대한다고 발표했다.",
                    facts=(
                        GroundedFact(
                            source_text="Capacity will double by year end.",
                            fact_ko="연말까지 생산능력을 두 배로 확대한다.",
                        ),
                    ),
                    interpretation_ko="AI 데이터센터 수요 대응 여력이 커진다.",
                    thesis_impact="strengthen",
                    impact_reason_ko="수요 대응 병목을 완화할 수 있다.",
                    confidence="high",
                )
                return EvidenceAnalysisBatch(
                    analyses={candidate.candidate_id: analysis},
                    errors={},
                )

        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            service = EvidenceIngestionService(
                repository,
                PROFILE,
                Analyzer(),
                alert_builder=build_evidence_message,
            )

            report = service.ingest(
                [
                    raw(
                        "Vertiv expands cooling capacity",
                        source_text="Capacity will double by year end.",
                    )
                ],
                NOW,
            )

            self.assertEqual(report.alerts, 1)
            pending = repository.pending_deliveries(NOW + timedelta(minutes=1))
            self.assertEqual(len(pending), 1)
            rendered = json.dumps(pending[0].payload, ensure_ascii=False)
            self.assertIn("확인된 사실", rendered)
            self.assertIn("논지 강화 근거", rendered)
            self.assertIn("원문 보기", rendered)
            self.assertNotIn("내용 확인 필요", rendered)
            catalysts = repository.recent_catalysts(
                "VRT",
                NOW - timedelta(hours=1),
            )
            self.assertEqual(len(catalysts), 1)
            self.assertEqual(catalysts[0].headline, "버티브, 냉각 생산능력 확대")

            market = MarketCycle(
                ticker="VRT",
                trading_date=NOW.date(),
                frames=(
                    MarketFrame(
                        MarketSnapshot(
                            ticker="VRT",
                            trading_date=NOW.date(),
                            observed_at=NOW + timedelta(minutes=5),
                            session=MarketSession.REGULAR,
                            change_pct=4.2,
                            benchmark_change_pct=0.5,
                            peer_changes={"ETN": 0.4, "GEV": 0.6, "NVT": 0.5},
                        ),
                        close_price=104.2,
                        reference_close=100,
                    ),
                ),
                volume=None,
                source_age_seconds=0,
            )
            move = MarketCycleService(repository).process(market, catalysts)
            move_text = json.dumps(move.messages[0], ensure_ascii=False)
            self.assertIn("버티브, 냉각 생산능력 확대", move_text)
            self.assertIn("https://example.com/story", move_text)


class SecFilingsTest(unittest.TestCase):
    def test_submissions_parser_builds_accession_archive_candidate(self):
        profile = EvidenceProfile(
            ticker="VRT",
            company_name="Vertiv Holdings Co",
            cik="1674101",
            aliases=("Vertiv",),
            sec_contact="owner@example.com",
        )
        payload = {
            "filings": {
                "recent": {
                    "form": ["8-K", "SC 13G"],
                    "filingDate": ["2026-09-02", "2026-09-02"],
                    "reportDate": ["2026-09-02", ""],
                    "acceptanceDateTime": ["2026-09-02T10:35:00Z", ""],
                    "accessionNumber": ["0001674101-26-000123", "ignored"],
                    "primaryDocument": ["vrt-20260902.htm", "ignored.htm"],
                    "primaryDocDescription": ["Current report", "Ownership"],
                    "items": ["1.01,2.01,9.01", ""],
                }
            }
        }

        candidates = parse_sec_submissions(
            payload,
            profile,
            limit=20,
            lookback_days=30,
            now=NOW,
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].external_id, "0001674101-26-000123")
        self.assertIn("/1674101/000167410126000123/", candidates[0].source_url)
        self.assertEqual(candidates[0].metadata["items"], "1.01,2.01,9.01")

    def test_sec_request_identifies_contact_and_403_uses_mirror(self):
        class Response:
            status_code = 403
            headers = {}

        calls = []

        def get(url, **kwargs):
            calls.append((url, kwargs))
            return Response()

        class Mirror:
            def __init__(self):
                self.called = False

            def fetch(self, profile, **_kwargs):
                self.called = True
                return (raw("Vertiv files current report", kind=EvidenceKind.SEC),)

        profile = EvidenceProfile(
            ticker="VRT",
            company_name="Vertiv Holdings Co",
            cik="1674101",
            aliases=("Vertiv",),
            sec_contact="owner@example.com",
        )
        mirror = Mirror()
        adapter = ResilientSecFilingsAdapter(
            SecSubmissionsClient(get=get),
            mirror,
        )

        result = adapter.fetch(profile, now=NOW)

        self.assertTrue(result.recovered)
        self.assertEqual(result.provider, "yahoo-sec-mirror")
        self.assertTrue(mirror.called)
        self.assertEqual(calls[0][1]["headers"]["From"], "owner@example.com")
        self.assertIn("owner@example.com", calls[0][1]["headers"]["User-Agent"])

    def test_sec_429_honors_retry_after_and_recovers(self):
        class Response:
            def __init__(self, status_code, payload=None, headers=None):
                self.status_code = status_code
                self._payload = payload or {"filings": {"recent": {"form": []}}}
                self.headers = headers or {}

            def json(self):
                return self._payload

        responses = iter(
            [
                Response(429, headers={"Retry-After": "3"}),
                Response(200),
            ]
        )
        clock = [0.0]
        sleeps = []

        def sleep(seconds):
            sleeps.append(seconds)
            clock[0] += seconds

        profile = EvidenceProfile(
            ticker="VRT",
            company_name="Vertiv Holdings Co",
            cik="1674101",
            aliases=("Vertiv",),
            sec_contact="owner@example.com",
        )
        client = SecSubmissionsClient(
            get=lambda *_args, **_kwargs: next(responses),
            sleeper=sleep,
            monotonic=lambda: clock[0],
        )

        result = client.fetch(profile, now=NOW)

        self.assertEqual(result, ())
        self.assertEqual(sleeps, [3.0])

    def test_first_sec_poll_is_atomic_baseline_and_next_accession_is_analyzed(self):
        class Adapter:
            def __init__(self):
                self.calls = 0

            def fetch(self, _profile, *, now):
                self.calls += 1
                candidates = [
                    raw(
                        "Vertiv quarterly report",
                        kind=EvidenceKind.SEC,
                        source_url="https://www.sec.gov/old",
                    )
                ]
                if self.calls == 2:
                    candidates.append(
                        raw(
                            "Vertiv signs material agreement",
                            kind=EvidenceKind.SEC,
                            minute=5,
                            source_url="https://www.sec.gov/new",
                        )
                    )
                return SecFetchResult(tuple(candidates), "sec-submissions")

        class Analyzer:
            def analyze(self, candidates, _profile):
                return EvidenceAnalysisBatch(
                    analyses={
                        candidate.candidate_id: EvidenceAnalysis(
                            candidate_id=candidate.candidate_id,
                            relevant=False,
                        )
                        for candidate in candidates
                    },
                    errors={},
                )

        class FilingText:
            def fetch(self, _candidate):
                return "Material agreement facts. " * 20

        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            adapter = Adapter()
            ingestion = EvidenceIngestionService(
                repository,
                PROFILE,
                Analyzer(),
                filing_text=FilingText(),
            )
            service = SecMonitorService(repository, PROFILE, adapter, ingestion)

            first = service.poll(NOW)
            second = service.poll(NOW + timedelta(minutes=10))

            self.assertTrue(first.baseline_created)
            self.assertEqual(first.baseline_candidates, 1)
            self.assertFalse(second.baseline_created)
            self.assertEqual(second.ingestion.inserted_pending, 1)
            self.assertEqual(second.ingestion.analyzed, 1)

    def test_empty_sec_response_cannot_initialize_baseline(self):
        class Adapter:
            def fetch(self, _profile, *, now):
                return SecFetchResult((), "yahoo-sec-mirror", recovered=True)

        class Analyzer:
            def analyze(self, _candidates, _profile):
                raise AssertionError("empty SEC response must not reach AI")

        with tempfile.TemporaryDirectory() as directory:
            repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
            ingestion = EvidenceIngestionService(repository, PROFILE, Analyzer())
            service = SecMonitorService(repository, PROFILE, Adapter(), ingestion)

            with self.assertRaisesRegex(Exception, "initial baseline"):
                service.poll(NOW)
            self.assertFalse(repository.has_source_baseline("sec:VRT:0001674101"))


if __name__ == "__main__":
    unittest.main()
