import inspect
import tempfile
import unittest
import xml.etree.ElementTree as ET
from datetime import date, datetime, time, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

import hood_monitor as hm
import live_smoke as smoke
from monitor_config import MonitorConfig


def visible_text(blocks):
    chunks = []
    for block in blocks:
        text = block.get("text")
        if isinstance(text, dict):
            chunks.append(text.get("text", ""))
        for element in block.get("elements", []):
            if isinstance(element, dict):
                chunks.append(element.get("text", ""))
    return "\n".join(chunks)


class ProductContractTest(unittest.TestCase):
    def test_vrt_profile_contains_business_kpis(self):
        self.assertIn("liquid cooling", hm.NEWS_TERMS)
        self.assertIn("backlog", hm.CORE_KPIS)
        self.assertIn("guidance cut", hm.RISK_KEYWORDS)

    def test_relative_block_hides_prices_and_exact_returns(self):
        blocks = hm.format_beta_block({
            "beta": 1.4,
            "qqq_pct": 1.25,
            "expected_pct": 1.75,
            "actual_pct": 2.50,
            "divergence": 0.75,
            "peer_changes": {"ETN": 0.8},
            "peer_avg": 0.8,
            "peer_diff": 1.7,
        })
        text = visible_text(blocks)
        self.assertIn("양전", text)
        self.assertIn("아웃퍼폼", text)
        self.assertIn("반도체 지수(SOXX)", text)
        self.assertIn("피어 평균(동일가중: 이튼)", text)
        self.assertNotIn("표시하지 않습니다", text)
        self.assertNotIn("2.50", text)
        self.assertNotIn("1.25", text)
        self.assertNotIn("$123.45", text)

    def test_news_block_has_source_and_thesis_impact(self):
        blocks = hm.format_news_block([{
            "title": "Vertiv raises guidance",
            "summary": "가이던스 상향",
            "translation": "Vertiv가 연간 가이던스를 상향했습니다.",
            "sentiment": "positive",
            "thesis_impact": "strengthen",
            "confidence": "high",
            "link": "https://example.com/vrt",
            "source": "example.com",
        }])
        text = visible_text(blocks)
        self.assertIn("논지 강화", text)
        self.assertIn("원문", text)
        self.assertIn("https://example.com/vrt", text)

    def test_failed_ai_news_is_not_marked_seen(self):
        items = [{"hash": "a", "analysis_status": "failed"},
                 {"hash": "b", "analysis_status": "success"}]
        self.assertEqual(hm.analyzed_news_hashes(items), ["b"])

    def test_partial_ai_response_retries_omitted_news(self):
        response = Mock(status_code=200)
        response.json.return_value = {
            "content": [{
                "type": "text",
                "text": '[{"idx": 1, "relevant": false}]',
            }]
        }
        news = [
            {"title": "first", "link": "", "hash": "a"},
            {"title": "second", "link": "", "hash": "b"},
        ]
        with (
            patch.object(hm, "ANTHROPIC_API_KEY", "test-key"),
            patch.object(hm, "_fetch_article_body", return_value=""),
            patch.object(hm.requests, "post", return_value=response),
        ):
            analyzed = hm.translate_news(news)

        self.assertEqual(analyzed[0]["analysis_status"], "success")
        self.assertEqual(analyzed[1]["analysis_status"], "failed")
        self.assertEqual(hm.analyzed_news_hashes(analyzed), ["a"])
        self.assertIn("부분 실패", hm.SOURCE_HEALTH["AI 뉴스"])

    def test_incomplete_relevant_ai_item_is_retried(self):
        response = Mock(status_code=200)
        response.json.return_value = {
            "content": [{
                "type": "text",
                "text": '[{"idx": 1, "relevant": true, "summary": "요약만 있음"}]',
            }]
        }
        news = [{"title": "filing news", "link": "", "hash": "a"}]
        with (
            patch.object(hm, "ANTHROPIC_API_KEY", "test-key"),
            patch.object(hm, "_fetch_article_body", return_value=""),
            patch.object(hm.requests, "post", return_value=response),
        ):
            analyzed = hm.translate_news(news)

        self.assertEqual(analyzed[0]["analysis_status"], "failed")
        self.assertEqual(hm.analyzed_news_hashes(analyzed), [])

    def test_market_detail_blocks_hide_price_levels(self):
        volume = hm.VolumeProfile(
            poc_price=123.45,
            poc_signal="support",
            vol_30m=100,
            vol_avg_30m=80,
            vol_ratio=1.25,
        )
        safety = hm.SafetyMargin(
            sma20=120.0,
            bb_upper=130.0,
            bb_lower=110.0,
            current_price=123.45,
            bb_signal="normal",
            momentum_signal="stable",
        )
        text = visible_text(
            hm.format_volume_profile_block(volume)
            + hm.format_safety_margin_block(safety)
        )
        for forbidden in ("123.45", "120.00", "130.00", "110.00"):
            self.assertNotIn(forbidden, text)

    def test_close_summary_contains_decision_contract(self):
        price = hm.PriceData(change_pct=-1.2, prev_close=10, current=9.88)
        blocks = hm.build_decision_summary_blocks(
            price=price,
            benchmark_pct=-2.0,
            news=[],
            filings=[],
            source_health={"Yahoo": "정상", "SEC": "정상", "AI": "정상"},
        )
        text = visible_text(blocks)
        self.assertIn("음전", text)
        self.assertIn("아웃퍼폼", text)
        self.assertIn("투자 논지", text)
        self.assertIn("변화 없음", text)
        self.assertNotIn("DCA", text)
        self.assertNotIn("-1.2", text)
        self.assertNotIn("-2.0", text)

    def test_thesis_damage_requires_and_displays_strong_evidence(self):
        blocks = hm.build_decision_summary_blocks(
            price=hm.PriceData(change_pct=-1.2),
            benchmark_pct=-0.5,
            news=[{
                "summary": "연간 가이던스 하향",
                "translation": "회사가 연간 매출과 영업이익 전망을 낮췄습니다.",
                "thesis_impact": "damage",
                "impact_reason": "수요와 수익성 기대가 동시에 약화됐습니다.",
                "confidence": "high",
            }],
            filings=[],
            source_health={"Yahoo": "정상", "AI": "정상"},
        )

        text = visible_text(blocks)
        self.assertIn("훼손 가능성", text)
        self.assertIn("투자 논지 훼손 근거", text)
        self.assertIn("연간 가이던스 하향", text)
        self.assertIn("수요와 수익성 기대가 동시에 약화", text)

    def test_medium_confidence_damage_is_downgraded_to_review(self):
        blocks = hm.build_decision_summary_blocks(
            price=hm.PriceData(change_pct=-1.2),
            benchmark_pct=-0.5,
            news=[{
                "summary": "확인되지 않은 우려",
                "translation": "기사에서 우려를 제기했습니다.",
                "thesis_impact": "damage",
                "impact_reason": "추가 확인이 필요합니다.",
                "confidence": "medium",
            }],
            filings=[],
            source_health={"AI": "정상"},
        )

        text = visible_text(blocks)
        self.assertIn("확인 필요", text)
        self.assertNotIn("훼손 가능성", text)

    def test_valuation_and_law_firm_news_is_prefiltered(self):
        news = [
            {"title": "VRT Seen 11% Overvalued After Investigation", "link": "", "hash": "a"},
            {"title": "Shareholder Alert: Law Firm Investigation on Behalf of VRT Investors", "link": "", "hash": "b"},
        ]
        with patch.object(hm.requests, "post") as post:
            analyzed = hm.translate_news(news)

        post.assert_not_called()
        self.assertTrue(all(item["skip"] for item in analyzed))
        self.assertEqual(hm.analyzed_news_hashes(analyzed), ["a", "b"])

    def test_volume_activity_uses_prior_20_sessions(self):
        activity = hm.calculate_volume_activity([100] * 20 + [160])

        self.assertIsNotNone(activity)
        self.assertEqual(activity.average_volume, 100)
        self.assertAlmostEqual(activity.ratio, 1.6)
        self.assertTrue(activity.exploded)

    def test_price_alerts_follow_integer_bands_in_one_direction(self):
        self.assertEqual(hm.next_price_alert_level(-4.4), 4)
        self.assertIsNone(hm.next_price_alert_level(-4.7, 4, "down"))
        self.assertEqual(hm.next_price_alert_level(-5.0, 4, "down"), 5)
        self.assertEqual(hm.next_price_alert_level(-7.2, 5, "down"), 7)

    def test_price_alert_does_not_rearm_on_reversal_or_retracement(self):
        self.assertEqual(hm.next_price_alert_level(8.1), 8)
        self.assertIsNone(hm.next_price_alert_level(5.2, 8, "up"))
        self.assertIsNone(hm.next_price_alert_level(-9.1, 8, "up"))

    def test_price_move_headline_has_direction_emoji_and_threshold(self):
        self.assertEqual(
            hm.format_price_move_headline(4.2, level=4),
            "📈 $VRT +4% 상승 구간 진입",
        )
        self.assertEqual(
            hm.format_price_move_headline(-5.1, level=5),
            "📉 $VRT -5% 하락 구간 진입",
        )

    def test_periodic_sec_filing_is_summarized_and_related_8k_is_deduped(self):
        accession = "0000000000-26-000001"

        def metric(tag, unit, prior, current):
            return tag, {
                "units": {unit: [
                    {
                        "accn": accession,
                        "start": "2025-04-01",
                        "end": "2025-06-30",
                        "val": prior,
                    },
                    {
                        "accn": accession,
                        "start": "2026-04-01",
                        "end": "2026-06-30",
                        "val": current,
                    },
                ]}
            }

        facts = dict([
            metric("RevenueFromContractWithCustomerExcludingAssessedTax", "USD", 2_600, 3_250),
            metric("OperatingIncomeLoss", "USD", 400, 600),
            metric("NetIncomeLoss", "USD", 300, 450),
            metric("EarningsPerShareDiluted", "USD/shares", 0.80, 1.20),
        ])
        company_facts = {"facts": {"us-gaap": facts}}
        ten_q = hm.CompanyFiling(
            form="10-Q",
            filing_date="2026-07-29",
            report_date="2026-06-30",
            accession=accession,
            hash="tenq",
            url="https://sec.example/10q",
        )
        eight_k = hm.CompanyFiling(
            form="8-K",
            filing_date="2026-07-29",
            report_date="2026-07-29",
            accession="0000000000-26-000002",
            hash="eightk",
            items="2.02,7.01,9.01",
            url="https://sec.example/8k",
        )

        analyzed = hm.analyze_company_filings([ten_q, eight_k], company_facts)
        alertable = hm.alertable_company_filings(analyzed)
        text = visible_text(hm.format_company_filings_block(alertable))

        self.assertEqual(alertable, [ten_q])
        self.assertTrue(eight_k.skip)
        self.assertIn("매출", text)
        self.assertIn("전년 동기 대비 +25.0%", text)
        self.assertIn("논지 강화", text)
        self.assertIn("SEC 원문", text)
        self.assertNotIn("내용 확인 필요", text)

    def test_failed_sec_analysis_is_not_marked_seen(self):
        state = {"last_company_filing_hashes": []}
        filing = hm.CompanyFiling(hash="retry", analysis_status="failed")

        hm.remember_company_filings(state, [filing])

        self.assertEqual(state["last_company_filing_hashes"], [])

    def test_sec_cache_migrates_legacy_hashes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "sec_alert_cache.json"
            with patch.object(hm, "SEC_ALERT_CACHE_FILE", cache_path):
                cache = hm.load_sec_alert_cache({
                    "last_company_filing_hashes": ["legacy-a", "legacy-b"],
                })

        self.assertTrue(cache["initialized"])
        self.assertEqual(
            cache["processed_filing_hashes"],
            ["legacy-a", "legacy-b"],
        )

    def test_sec_cache_baselines_existing_filings_and_allows_only_new_ones(self):
        cache = {
            "initialized": False,
            "processed_filing_hashes": [],
        }
        old_filings = [
            hm.CompanyFiling(hash="old-a"),
            hm.CompanyFiling(hash="old-b"),
        ]
        hm.remember_sec_filings(cache, old_filings, baseline=True)

        self.assertEqual(hm.unseen_company_filings(cache, old_filings), [])
        new_filing = hm.CompanyFiling(hash="new", analysis_status="success")
        self.assertEqual(hm.unseen_company_filings(cache, [new_filing]), [new_filing])

        hm.remember_sec_filings(cache, [new_filing])
        self.assertEqual(hm.unseen_company_filings(cache, [new_filing]), [])

    def test_failed_sec_analysis_is_not_written_to_dedicated_cache(self):
        cache = {
            "initialized": True,
            "processed_filing_hashes": [],
        }
        filing = hm.CompanyFiling(hash="retry", analysis_status="failed")

        hm.remember_sec_filings(cache, [filing])

        self.assertEqual(cache["processed_filing_hashes"], [])

    def test_workflow_stages_only_existing_state_files(self):
        workflow = Path(".github/workflows/hood_monitor.yml").read_text()

        self.assertIn('if [ -f "$file" ]', workflow)
        self.assertIn('git add -f -- "$file"', workflow)
        self.assertIn("*_sec_alert_cache.json", workflow)

    def test_workflow_retries_transient_state_push_failures(self):
        workflow = Path(".github/workflows/hood_monitor.yml").read_text()

        self.assertIn("MAX_ATTEMPTS=4", workflow)
        self.assertIn("git pull --rebase origin main && git push origin main", workflow)
        self.assertIn('sleep "$WAIT_SECONDS"', workflow)
        self.assertIn("State push failed after $MAX_ATTEMPTS attempts", workflow)

    def test_workflow_has_ten_minute_realtime_and_off_hour_normal_crons(self):
        workflow = Path(".github/workflows/hood_monitor.yml").read_text()

        self.assertIn("7,17,27,37,47,57 8-23 * * 1-5", workflow)
        self.assertIn("23 8-23 * * 1-5", workflow)
        self.assertNotIn("'0 8-23 * * 1-5'", workflow)
        self.assertIn('echo "mode=realtime"', workflow)

    def test_sec_user_agent_declares_contact_inline(self):
        config = MonitorConfig(sec_contact="owner@example.com")

        self.assertIn("okeycsy TickerMonitor/1.0", config.sec_user_agent)
        self.assertIn("owner@example.com", config.sec_user_agent)
        self.assertEqual(config.sec_headers["From"], "owner@example.com")

    def test_github_hosted_runner_bypasses_raw_sec_archives(self):
        yahoo_trade = hm.InsiderTrade(filer="Fallback", trade_type="Sale")
        with (
            patch.dict("os.environ", {
                "GITHUB_ACTIONS": "true",
                "RUNNER_ENVIRONMENT": "github-hosted",
                "SEC_ARCHIVE_MODE": "auto",
            }),
            patch.object(hm, "_form4_candidates_from_submissions") as submissions,
            patch.object(
                hm,
                "_fetch_insider_trades_from_yahoo",
                return_value=[yahoo_trade],
            ) as yahoo,
        ):
            trades = hm.fetch_insider_trades()

        self.assertEqual(trades, [yahoo_trade])
        submissions.assert_not_called()
        yahoo.assert_called_once_with()

    def test_workflow_disables_raw_sec_archives_on_hosted_runner(self):
        workflow = Path(".github/workflows/hood_monitor.yml").read_text()

        self.assertIn("SEC_ARCHIVE_MODE:   yahoo", workflow)
        self.assertIn("SEC_DATA_MODE:      yahoo", workflow)

    def test_github_mode_uses_yahoo_company_filings_without_sec_request(self):
        yahoo_filing = hm.CompanyFiling(form="10-Q", accession="yahoo")
        with (
            patch.dict("os.environ", {
                "GITHUB_ACTIONS": "true",
                "RUNNER_ENVIRONMENT": "github-hosted",
                "SEC_DATA_MODE": "auto",
            }),
            patch.object(
                hm,
                "_fetch_company_filings_from_yahoo",
                return_value=[yahoo_filing],
            ) as yahoo,
            patch.object(hm, "safe_get") as get,
        ):
            filings = hm.fetch_company_filings()

        self.assertEqual(filings, [yahoo_filing])
        yahoo.assert_called_once_with(20, 21)
        get.assert_not_called()

    def test_yahoo_company_filing_maps_accession_report_date_and_body(self):
        ticker = Mock()
        ticker.get_sec_filings.return_value = [{
            "date": date.today(),
            "type": "10-Q",
            "title": "Periodic Financial Reports",
            "edgarUrl": (
                "https://finance.yahoo.com/sec-filing/VRT/"
                "0001628280-26-050609_1674101"
            ),
            "exhibits": {
                "10-Q": (
                    "https://cdn.yahoofinance.com/prod/sec-filings/"
                    "vrt-20260630.htm"
                ),
            },
        }]
        with patch("yfinance.Ticker", return_value=ticker):
            filings = hm._fetch_company_filings_from_yahoo()

        self.assertEqual(len(filings), 1)
        self.assertEqual(filings[0].accession, "0001628280-26-050609")
        self.assertEqual(filings[0].report_date, "2026-06-30")
        self.assertIn("cdn.yahoofinance.com", filings[0].url)

    def test_yahoo_financials_preserve_periodic_filing_summary(self):
        current = pd.Timestamp("2026-06-30")
        prior = pd.Timestamp("2025-06-30")
        statement = pd.DataFrame({
            current: [3250.0, 600.0, 450.0, 1.20],
            prior: [2600.0, 400.0, 300.0, 0.80],
        }, index=["Total Revenue", "Operating Income", "Net Income", "Diluted EPS"])
        ticker = Mock(quarterly_income_stmt=statement)
        filing = hm.CompanyFiling(
            form="10-Q",
            report_date="2026-06-30",
            accession="0001628280-26-050609",
        )

        with patch("yfinance.Ticker", return_value=ticker):
            analyzed = hm._summarize_periodic_filing_from_yahoo(filing)

        self.assertEqual(analyzed.analysis_status, "success")
        self.assertEqual(analyzed.summary, "분기 매출·영업이익 동반 증가")
        self.assertIn("전년 동기 대비 +25.0%", analyzed.key_facts[0])

    def test_8k_items_are_extracted_from_mirrored_body(self):
        response = Mock(text="<h2>Item&nbsp;5.02</h2><p>Change</p><h2>Item 9.01</h2>")
        filing = hm.CompanyFiling(url="https://cdn.example/vrt-8k.htm")

        with patch.object(hm, "safe_get", return_value=response):
            items = hm._extract_filing_items(filing)

        self.assertEqual(items, "5.02,9.01")

    def test_13f_hosted_mode_never_falls_back_to_blocked_sec_hosts(self):
        with (
            patch.dict("os.environ", {"SEC_DATA_MODE": "yahoo"}),
            patch.object(hm, "SEC_API_KEY", ""),
            patch.object(hm, "safe_get") as get,
            patch.object(hm.requests, "post") as post,
        ):
            filings = hm.fetch_13f_filings()

        self.assertEqual(filings, [])
        get.assert_not_called()
        post.assert_not_called()

    def test_live_smoke_uses_yahoo_in_hosted_mode(self):
        config = MonitorConfig(ticker="VRT", cik="0001674101")
        with (
            patch.dict("os.environ", {"SEC_DATA_MODE": "yahoo"}),
            patch.object(
                smoke,
                "_check_yahoo_sec_filings",
                return_value=(True, "Yahoo mirror OK"),
            ) as filings,
            patch.object(
                smoke,
                "_check_yahoo_insider_fallback",
                return_value=(True, "Yahoo insider OK"),
            ) as insiders,
            patch.object(smoke, "_request_json") as sec_request,
        ):
            self.assertTrue(smoke.check_sec(config)[0])
            self.assertTrue(smoke.check_sec_form4(config)[0])

        filings.assert_called_once_with(config)
        insiders.assert_called_once_with(config, "SEC direct route disabled")
        sec_request.assert_not_called()

    def test_realtime_mode_skips_news_filings_and_insiders(self):
        state = {
            "price_alert_date": date.today().isoformat(),
            "price_alert_max_pct": 0,
            "price_alert_direction": "",
        }
        with (
            patch.object(hm, "load_state", return_value=state),
            patch.object(hm, "load_weekly_state", return_value={}),
            patch.object(hm, "fetch_price", return_value=None),
            patch.object(hm, "fetch_price_history") as history,
            patch.object(hm, "fetch_company_filings") as filings,
            patch.object(hm, "fetch_news") as news,
            patch.object(hm, "fetch_insider_trades") as insiders,
            patch.object(hm, "save_state"),
            patch.object(hm, "save_weekly_state"),
        ):
            hm.run_normal(realtime=True)

        history.assert_not_called()
        filings.assert_not_called()
        news.assert_not_called()
        insiders.assert_not_called()

    def test_workflow_has_no_dca_modes_or_inputs(self):
        workflow = Path(".github/workflows/hood_monitor.yml").read_text()

        self.assertNotIn("dca_status", workflow)
        self.assertNotIn("dca_update", workflow)
        self.assertNotIn("DCA_SHARES", workflow)
        self.assertNotIn("DCA_PRICE", workflow)

    def test_scheduled_alert_paths_do_not_build_dca_outputs(self):
        source = inspect.getsource(hm.run_close) + inspect.getsource(hm.run_weekly)

        self.assertNotIn("dca", source.lower())

    def test_preview_uses_assumed_move_without_mutating_state(self):
        price = hm.PriceData(
            current=272.54,
            prev_close=270.0,
            change_pct=0.94,
            volume=160,
            vol_avg_20d=100,
        )
        with (
            patch.dict("os.environ", {"PREVIEW_CHANGE_PCT": "4.0"}),
            patch.object(hm, "fetch_price", return_value=price),
            patch.object(hm, "fetch_relative_performance", return_value={
                "actual_pct": 4.0,
                "benchmark_pct": 1.0,
                "peer_changes": {"ETN": 0.5, "NVT": 0.7, "GEV": 0.9},
            }),
            patch.object(hm, "analyze_volume_profile", return_value=None),
            patch.object(hm, "fetch_price_history", return_value=[]),
            patch.object(hm, "fetch_company_filings", return_value=[]),
            patch.object(hm, "fetch_news", return_value=[]),
            patch.object(hm, "fetch_insider_trades", return_value=[]),
            patch.object(hm, "save_state") as save_state,
            patch.object(hm, "save_weekly_state") as save_weekly_state,
            patch.object(hm, "send_slack") as send_slack,
        ):
            hm.run_preview()

        blocks = send_slack.call_args.args[0]
        text = visible_text(blocks)
        self.assertIn("📈 +4.0% 상승", text)
        self.assertIn("📈 $VRT +4.0% 상승 가정", text)
        self.assertIn("반도체 지수(SOXX) 대비 *아웃퍼폼*", text)
        self.assertIn("피어 평균", text)
        self.assertIn("평균 대비 급증", text)
        self.assertNotIn("272.54", text)
        save_state.assert_not_called()
        save_weekly_state.assert_not_called()

    def test_preview_workflow_does_not_commit_state(self):
        workflow = Path(".github/workflows/hood_monitor.yml").read_text()
        smoke_workflow = Path(".github/workflows/live_smoke.yml").read_text()

        self.assertIn("normal/preview/close/weekly/13f", workflow)
        self.assertIn("PREVIEW_CHANGE_PCT", workflow)
        self.assertIn("steps.mode.outputs.mode != 'preview'", workflow)
        self.assertIn(".github/alert_preview_request", workflow)
        self.assertIn("preview_change_pct=${PREVIEW_CHANGE:-4.0}", workflow)
        self.assertIn("[alert-preview]", smoke_workflow)

    def test_live_smoke_push_cannot_send_slack(self):
        workflow = Path(".github/workflows/live_smoke.yml").read_text()

        push_step = workflow.split("- name: Run push live checks without Slack", 1)[1]
        push_step = push_step.split("- name: Run manual live checks", 1)[0]
        self.assertIn("if: github.event_name == 'push'", push_step)
        self.assertIn("python live_smoke.py --no-slack", push_step)
        self.assertNotIn("SLACK_WEBHOOK_URL", push_step)
        self.assertNotIn("MARKET_SCAN_WEBHOOK", push_step)

    def test_primary_alert_uses_emoji_only_for_key_information(self):
        blocks = []
        blocks += hm.format_beta_block({
            "actual_pct": -1.0,
            "benchmark_pct": -0.5,
            "peer_changes": {"ETN": -0.2},
        })
        blocks += hm.format_volume_activity_block(
            hm.make_volume_activity(160, 100), finalized=True
        )
        blocks += hm.format_technicals_block(hm.TechnicalSignals(rsi_14=45.0))
        blocks += hm.format_options_block(hm.OptionsData(pcr=1.2, pcr_signal="neutral"))
        blocks += hm.format_short_block(hm.ShortInterestData(short_pct=50.0))
        blocks += hm.format_news_block([{
            "summary": "가이던스 유지",
            "translation": "회사가 기존 가이던스를 유지했습니다.",
            "thesis_impact": "neutral",
            "impact_reason": "장기 전망 변화는 없습니다.",
        }])
        text = visible_text(blocks)

        self.assertIn("📉 $VRT", text)
        self.assertIn("↘️ 반도체 지수", text)
        self.assertIn("↘️ 피어 평균", text)
        self.assertIn("📰 주요 뉴스", text)
        for emoji in "🔔📊📐🏛🧮🛡🩳🔥⚪🔴🟢🟡🚀💥🐋⚠":
            self.assertNotIn(emoji, text)

    def test_volume_below_threshold_is_not_exploded(self):
        activity = hm.calculate_volume_activity([100] * 20 + [149])

        self.assertIsNotNone(activity)
        self.assertFalse(activity.exploded)

    def test_volume_activity_requires_full_baseline(self):
        self.assertIsNone(hm.calculate_volume_activity([100] * 20))

    def test_close_price_uses_latest_daily_volume_and_prior_20_day_average(self):
        today = datetime.now(hm.NY_TZ).date()
        days = [today - timedelta(days=21 - index) for index in range(22)]
        timestamps = [
            int(datetime.combine(day, time(12), tzinfo=hm.NY_TZ).timestamp())
            for day in days
        ]
        response = Mock()
        response.json.return_value = {
            "chart": {
                "result": [{
                    "timestamp": timestamps,
                    "meta": {},
                    "indicators": {
                        "quote": [{
                            "close": [10.0] * 22,
                            "volume": [100] * 21 + [160],
                        }]
                    },
                }]
            }
        }

        with (
            patch.object(hm, "_yahoo_throttle"),
            patch.object(hm, "fetch_yahoo_chart", return_value=response),
        ):
            price = hm.fetch_price(realtime=False)

        self.assertEqual(price.volume, 160)
        self.assertEqual(price.vol_avg_20d, 100)

    def test_volume_block_shows_comparison_and_result(self):
        activity = hm.calculate_volume_activity([100] * 20 + [160])
        text = visible_text(hm.format_volume_activity_block(activity, finalized=True))

        self.assertIn("평균 대비 급증", text)
        self.assertIn("당일 거래량", text)
        self.assertIn("20일 평균", text)
        self.assertIn("1.60x", text)

    def test_intraday_volume_block_does_not_claim_final_result(self):
        activity = hm.calculate_volume_activity([100] * 20 + [149])
        text = visible_text(hm.format_volume_activity_block(activity, finalized=False))

        self.assertIn("장중 기준 미달", text)
        self.assertNotIn("평균 범위", text)

    def test_volume_explosion_counts_as_important_change(self):
        blocks = hm.build_decision_summary_blocks(
            price=hm.PriceData(change_pct=0.2),
            benchmark_pct=0.1,
            news=[],
            filings=[],
            volume_activity=hm.calculate_volume_activity([100] * 20 + [160]),
            source_health={"Yahoo": "정상"},
        )

        self.assertIn("중요 변화 1건", visible_text(blocks))

    def test_source_failure_prevents_false_no_change(self):
        blocks = hm.build_decision_summary_blocks(
            price=hm.PriceData(change_pct=0.2),
            benchmark_pct=0.1,
            news=[],
            filings=[],
            source_health={"Yahoo": "정상", "AI": "실패"},
        )
        text = visible_text(blocks)
        self.assertIn("판정 보류", text)
        self.assertIn("확인 불가", text)
        self.assertNotIn("DCA", text)
        self.assertNotIn("새로운 중요 사건 없음", text)

    def test_open_market_sale_requires_review(self):
        blocks = hm.build_decision_summary_blocks(
            price=hm.PriceData(change_pct=0.2),
            benchmark_pct=0.1,
            news=[],
            filings=[],
            insiders=[hm.InsiderTrade(trade_type="Sale")],
            source_health={"Yahoo": "정상", "SEC": "정상"},
        )
        text = visible_text(blocks)
        self.assertIn("확인 필요", text)
        self.assertIn("내부자 장내 매도", text)

    def test_form4_disposition_is_not_labeled_open_market_sale(self):
        transaction = ET.fromstring("""
        <nonDerivativeTransaction>
          <transactionCoding><transactionCode>D</transactionCode></transactionCoding>
          <transactionAmounts>
            <transactionShares><value>100</value></transactionShares>
            <transactionPricePerShare><value>10</value></transactionPricePerShare>
            <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
          </transactionAmounts>
        </nonDerivativeTransaction>
        """)
        trade = hm._parse_transaction(transaction, "Officer", "CFO", "2026-01-01", "https://sec.example")
        self.assertEqual(trade.trade_type, "Disposition")

    def test_routine_awards_do_not_trigger_immediate_alert(self):
        trades = [
            hm.InsiderTrade(trade_type="Award", shares=10),
            hm.InsiderTrade(trade_type="Disposition", shares=100),
            hm.InsiderTrade(trade_type="Purchase", shares=5),
            hm.InsiderTrade(trade_type="Sale", shares=2_000),
        ]
        material = hm.material_insider_trades(trades)
        self.assertEqual([trade.trade_type for trade in material], ["Purchase", "Sale"])

    def test_stale_insider_sales_do_not_trigger_first_run_alert(self):
        trades = [
            hm.InsiderTrade(trade_type="Sale", shares=2_000, date="2026-06-01"),
            hm.InsiderTrade(trade_type="Sale", shares=2_000, date="2026-08-10"),
        ]
        material = hm.material_insider_trades(
            trades, lookback_days=21, as_of=date(2026, 8, 16)
        )
        self.assertEqual([trade.date for trade in material], ["2026-08-10"])

    def test_first_13f_observation_is_a_baseline(self):
        filing = hm.Filing13F(
            institution="Example Fund",
            shares=100,
            change_type="BASELINE",
            filing_date="2026-08-01",
        )
        text = visible_text(hm.format_13f_block([filing]))
        self.assertIn("기준 설정", text)
        self.assertNotIn("· 신규", text)

    def test_github_run_without_slack_fails(self):
        with patch.object(hm, "SLACK_WEBHOOK", ""), patch.dict("os.environ", {"GITHUB_ACTIONS": "true"}):
            with self.assertRaises(RuntimeError):
                hm.send_slack([])


if __name__ == "__main__":
    unittest.main()
