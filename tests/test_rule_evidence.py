from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from investing_monitor.adapters.rule_evidence import RuleBasedEvidenceAnalyzer
from investing_monitor.adapters.sqlite_repository import SQLiteMonitorRepository
from investing_monitor.application.evidence import EvidenceIngestionService
from investing_monitor.domain.evidence import (
    EvidenceKind,
    EvidenceProfile,
    RawEvidenceCandidate,
)
from investing_monitor.presentation.evidence_messages import build_evidence_message
from investing_monitor.presentation.quality import audit_message

NOW = datetime(2026, 11, 4, 14, 0, tzinfo=timezone.utc)
BLOOM = EvidenceProfile(
    ticker="BE",
    company_name="Bloom Energy Corp",
    cik="1664703",
    aliases=("Bloom Energy",),
    priority_keywords=("orders", "backlog", "guidance", "data center"),
    core_kpis=("product revenue", "gross margin"),
)


def raw(headline, kind, *, text="", minute=0, metadata=None, ticker="BE", url=None):
    return RawEvidenceCandidate(
        ticker=ticker,
        kind=kind,
        headline=headline,
        source_name="Bloom Energy Investor Relations" if kind is EvidenceKind.IR else "SEC EDGAR",
        source_url=url or f"https://example.com/{abs(hash(headline))}",
        published_at=NOW - timedelta(minutes=30) + timedelta(minutes=minute),
        source_text=text or headline,
        metadata=metadata or {},
    )


class RuleBasedAnalyzerTest(unittest.TestCase):
    def service(self, directory):
        repository = SQLiteMonitorRepository(Path(directory) / "monitor.db")
        return repository, EvidenceIngestionService(
            repository,
            BLOOM,
            RuleBasedEvidenceAnalyzer(),
            alert_builder=build_evidence_message,
            enqueue_alerts=False,
        )

    def test_earnings_press_release_alerts_with_verbatim_facts(self):
        text = (
            "Bloom Energy today reported record third quarter results. "
            "Product revenue grew 42% year over year to $480 million. "
            "The company raised full year guidance on strong data center orders. "
            "Bloom will host a conference call today."
        )
        with tempfile.TemporaryDirectory() as directory:
            repository, service = self.service(directory)
            report = service.ingest(
                [raw("Bloom Energy Reports Record Third Quarter 2026 Financial Results", EvidenceKind.IR, text=text)],
                NOW,
            )
            alerts = repository.recent_alerts()

        self.assertEqual(report.alerts, 1)
        payload = alerts[0].payload
        rendered = json.dumps(payload, ensure_ascii=False)
        self.assertTrue(audit_message("catalyst", payload).passed)
        self.assertIn("Product revenue grew 42% year over year to $480 million.", rendered)
        self.assertIn("판단 보류", rendered)
        self.assertNotIn("중립적 변화", rendered)

    def test_community_press_release_and_third_party_news_stay_silent(self):
        with tempfile.TemporaryDirectory() as directory:
            repository, service = self.service(directory)
            report = service.ingest(
                [
                    raw("Bloom Energy Honors Louisville First Responders", EvidenceKind.IR),
                    raw("Why Bloom Energy (BE) Stock Is Soaring Today", EvidenceKind.NEWS, minute=1),
                ],
                NOW,
            )

        self.assertEqual(report.alerts, 0)

    def test_material_sec_filing_alerts_without_a_model(self):
        class FilingText:
            def fetch(self, _candidate):
                return (
                    "Item 1.01 Entry into a Material Definitive Agreement. "
                    "Bloom Energy entered into a supply agreement for 500 MW of fuel cells."
                )

        with tempfile.TemporaryDirectory() as directory:
            repository, service = self.service(directory)
            service.filing_text = FilingText()
            report = service.ingest(
                [
                    raw(
                        "Current report",
                        EvidenceKind.SEC,
                        text="Item 1.01 Entry into a Material Definitive Agreement. "
                        "Bloom Energy entered into a supply agreement for 500 MW of fuel cells.",
                        metadata={"form": "8-K", "items": "1.01"},
                    )
                ],
                NOW,
            )
            alerts = repository.recent_alerts()

        self.assertEqual(report.alerts, 1)
        self.assertEqual(alerts[0].alert_type, "filing")
        self.assertIn("8-K 공시", alerts[0].payload["text"])
        self.assertIn("500 MW", json.dumps(alerts[0].payload, ensure_ascii=False))

    def test_earnings_date_notice_becomes_an_official_event(self):
        from investing_monitor.application.evidence import screen_candidate

        decision = screen_candidate(
            raw(
                "Bloom Energy to Announce Third Quarter 2026 Financial Results on October 22, 2026",
                EvidenceKind.IR,
            ),
            BLOOM,
        )
        candidate = decision.candidate
        self.assertTrue(candidate.metadata.get("calendar_only"))

        analysis = RuleBasedEvidenceAnalyzer().analyze([candidate], BLOOM).analyses[candidate.candidate_id]

        self.assertTrue(analysis.relevant)
        self.assertEqual(analysis.official_events[0].event_date, date(2026, 10, 22))

    def test_retry_queue_ignores_the_previous_ticker(self):
        with tempfile.TemporaryDirectory() as directory:
            repository, service = self.service(directory)
            vrt_profile = EvidenceProfile(
                ticker="VRT", company_name="Vertiv Holdings Co", cik="1674101", aliases=("Vertiv",)
            )
            failing = mock.Mock()
            failing.analyze.side_effect = RuntimeError("credit balance is too low")
            EvidenceIngestionService(repository, vrt_profile, failing).ingest(
                [raw("Vertiv Announces Agreement to Acquire Rival", EvidenceKind.IR, ticker="VRT")],
                NOW - timedelta(minutes=20),
            )

            report = service.ingest([], NOW)

        self.assertEqual(report.analyzed, 0)
        self.assertEqual(report.alerts, 0)


class AnalyzerSwitchTest(unittest.TestCase):
    def test_switch_off_or_missing_key_selects_rule_based(self):
        from investing_monitor import cli
        from investing_monitor.adapters.anthropic_evidence import AnthropicEvidenceAnalyzer

        with mock.patch.dict(os.environ, {"ANTHROPIC_API_KEY": "k", "EVIDENCE_AI": "off"}):
            self.assertIsInstance(cli._evidence_analyzer(), RuleBasedEvidenceAnalyzer)
        with mock.patch.dict(os.environ, {"ANTHROPIC_API_KEY": "", "EVIDENCE_AI": "on"}):
            self.assertIsInstance(cli._evidence_analyzer(), RuleBasedEvidenceAnalyzer)
        with mock.patch.dict(os.environ, {"ANTHROPIC_API_KEY": "k", "EVIDENCE_AI": "on"}):
            self.assertIsInstance(cli._evidence_analyzer(), AnthropicEvidenceAnalyzer)


if __name__ == "__main__":
    unittest.main()
