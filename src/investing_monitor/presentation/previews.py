from __future__ import annotations

import copy
from datetime import datetime, timedelta

from investing_monitor.domain.evidence import (
    EvidenceAnalysis,
    EvidenceCandidate,
    EvidenceKind,
    GroundedFact,
)
from investing_monitor.domain.models import (
    Catalyst,
    Direction,
    MarketSession,
    MarketSnapshot,
    OfficialEvent,
    PriceBandSignal,
    RelativeOutcome,
    SituationVerdict,
    ThesisImpact,
    VolumeSignal,
    VolumeSnapshot,
)
from investing_monitor.domain.policies import (
    RelativeAssessment,
    SituationAssessment,
    VolumeAssessment,
)
from investing_monitor.presentation.close_messages import build_close_message
from investing_monitor.presentation.evidence_messages import (
    build_evidence_message,
    build_move_followup_message,
)
from investing_monitor.presentation.quality import audit_message
from investing_monitor.presentation.slack_messages import (
    build_price_band_message,
    build_volume_message,
)
from investing_monitor.presentation.weekly_messages import build_weekly_message


PREVIEW_KINDS = (
    "move-up",
    "move-down",
    "volume",
    "catalyst",
    "filing",
    "move-followup",
    "close",
    "weekly",
)


def build_preview_message(
    kind: str,
    *,
    ticker: str,
    benchmark: str,
    peers: tuple[str, ...],
    now: datetime,
) -> dict:
    if kind not in PREVIEW_KINDS:
        raise ValueError(f"unsupported preview kind: {kind}")

    source_type, payload = _source_message(
        kind,
        ticker=ticker,
        benchmark=benchmark,
        peers=peers,
        now=now,
    )
    source_audit = audit_message(source_type, payload)
    if not source_audit.passed:
        raise ValueError("invalid preview fixture: " + "; ".join(source_audit.violations))

    preview = copy.deepcopy(payload)
    preview["text"] = f"[V2 미리보기] {preview['text']}"[:3_000]
    for block in preview["blocks"]:
        if block.get("type") != "header":
            continue
        text = block.get("text")
        if isinstance(text, dict) and isinstance(text.get("text"), str):
            text["text"] = f"미리보기 · {text['text']}"[:150]
            break
    preview["blocks"].insert(
        1,
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "fixture 기반 미리보기 · 실제 투자 신호가 아닙니다",
                }
            ],
        },
    )
    preview_audit = audit_message("delivery_canary", preview)
    if not preview_audit.passed:
        raise ValueError("invalid labeled preview: " + "; ".join(preview_audit.violations))
    return preview


def _source_message(
    kind: str,
    *,
    ticker: str,
    benchmark: str,
    peers: tuple[str, ...],
    now: datetime,
) -> tuple[str, dict]:
    relative = RelativeAssessment(
        benchmark=RelativeOutcome.OUTPERFORM,
        benchmark_symbol=benchmark,
        peers=RelativeOutcome.OUTPERFORM,
        peer_symbols=peers,
        peer_average_change_pct=1.2,
    )
    situation = SituationAssessment(
        verdict=SituationVerdict.COMPANY_STRENGTH,
        confidence="high",
        sensitivity_adjusted=True,
    )
    volume = VolumeSnapshot(
        observed_volume=3_095_486,
        expected_volume=1_856_146,
        baseline_sessions=20,
    )
    volume_assessment = VolumeAssessment(
        ratio=volume.ratio,
        is_ready=True,
        is_exploded=True,
    )
    catalyst = Catalyst(
        canonical_id="preview-capacity",
        headline="버티브, AI 데이터센터 냉각 생산능력 확대",
        summary=(
            "회사는 주요 생산 거점의 냉각 설비 증설 일정을 구체화했다. "
            "AI 인프라 수요 대응 능력을 확인할 수 있는 변화다."
        ),
        source_name="Vertiv Investor Relations",
        source_url="https://investors.vertiv.com/",
        published_at=now - timedelta(hours=2),
        impact=ThesisImpact.STRENGTHEN,
        confidence="high",
        facts=("냉각 생산능력 확대 일정이 공식 발표됐다.",),
        source_kind="ir",
        source_tier="official",
        event_type="capacity",
    )

    if kind in {"move-up", "move-down"}:
        direction = Direction.UP if kind == "move-up" else Direction.DOWN
        level = 4
        signal = PriceBandSignal(
            event_key=f"preview:{kind}",
            ticker=ticker,
            trading_date=now.date(),
            direction=direction,
            level=level,
            is_reversal=False,
            observed_at=now,
            session=MarketSession.REGULAR,
        )
        return (
            "price_band",
            build_price_band_message(
                signal,
                relative,
                volume,
                volume_assessment,
                (catalyst,),
                situation=(
                    situation
                    if direction is Direction.UP
                    else SituationAssessment(
                        verdict=SituationVerdict.COMPANY_WEAKNESS,
                        confidence="high",
                        sensitivity_adjusted=True,
                    )
                ),
            ),
        )

    snapshot = MarketSnapshot(
        ticker=ticker,
        trading_date=now.date(),
        observed_at=now,
        session=MarketSession.REGULAR,
        change_pct=4.2,
        benchmark_change_pct=1.0,
        benchmark_symbol=benchmark,
        peer_changes={peer: 1.2 for peer in peers},
    )
    if kind == "volume":
        signal = VolumeSignal(
            event_key="preview:volume",
            ticker=ticker,
            trading_date=now.date(),
            observed_at=now,
        )
        return (
            "volume_spike",
            build_volume_message(
                signal,
                snapshot,
                relative,
                volume,
                volume_assessment,
                situation=situation,
            ),
        )
    if kind == "close":
        return (
            "daily_close",
            build_close_message(
                snapshot,
                relative,
                volume,
                volume_assessment,
                (catalyst,),
                situation,
            ),
        )
    if kind == "weekly":
        return (
            "weekly_review",
            build_weekly_message(
                snapshot,
                relative,
                volume,
                volume_assessment,
                (catalyst,),
                (),
                (
                    OfficialEvent(
                        event_date=now.date() + timedelta(days=4),
                        title_ko="분기 실적 발표",
                        source_url="https://investors.vertiv.com/",
                        source_text="Quarterly earnings call",
                        time_et="08:30",
                    ),
                ),
                period_start=now.date() - timedelta(days=4),
                period_end=now.date(),
                session_count=5,
            ),
        )

    is_filing = kind == "filing"
    evidence_kind = EvidenceKind.SEC if is_filing else EvidenceKind.NEWS
    source_text = (
        "Vertiv announced a material expansion of liquid cooling capacity."
        if is_filing
        else "Vertiv signed a $250 million data center supply contract."
    )
    candidate = EvidenceCandidate(
        candidate_id=f"preview-{kind}",
        ticker=ticker,
        kind=evidence_kind,
        headline=(
            "Vertiv expands liquid cooling capacity"
            if is_filing
            else "Vertiv signs $250 million data center supply contract"
        ),
        source_name="SEC EDGAR" if is_filing else "Reuters",
        source_url=(
            "https://www.sec.gov/Archives/edgar/data/1674101/"
            if is_filing
            else "https://www.reuters.com/technology/"
        ),
        published_at=now,
        source_text=source_text,
        metadata={"form": "8-K"} if is_filing else {},
    )
    analysis = EvidenceAnalysis(
        candidate_id=candidate.candidate_id,
        relevant=True,
        headline_ko=(
            "버티브, 액체냉각 생산능력 확대 확정"
            if is_filing
            else "버티브, 대형 데이터센터 공급 계약 체결"
        ),
        summary_ko=(
            "회사가 증설 규모와 가동 일정을 공식화했다. "
            "AI 데이터센터 냉각 수요 대응 여력이 커진다."
            if is_filing
            else "회사가 2억 5천만 달러 규모의 전력·냉각 공급 계약을 체결했다. "
            "수주 가시성을 직접 뒷받침하는 새 사실이다."
        ),
        facts=(
            GroundedFact(
                source_text=candidate.source_text,
                fact_ko=(
                    "액체냉각 생산능력 확대 계획이 공식 발표됐다."
                    if is_filing
                    else "2억 5천만 달러 규모 공급 계약을 체결했다."
                ),
            ),
        ),
        thesis_impact="strengthen",
        impact_reason_ko=(
            "핵심 냉각 사업의 공급 능력 확대가 확인됐다."
            if is_filing
            else "대형 수주가 backlog와 매출 가시성을 강화한다."
        ),
        confidence="high",
        event_type="capacity" if is_filing else "major_contract",
        company_directness=True,
        new_fact=True,
        materiality="high",
        source_tier="official" if is_filing else "primary_reporting",
        alert_worthy=True,
    )
    if kind == "move-followup":
        return (
            "move_followup",
            build_move_followup_message(
                {
                    "direction": "up",
                    "level": 4,
                },
                now - timedelta(minutes=35),
                candidate,
                analysis,
            ),
        )
    return (
        "filing" if kind == "filing" else "catalyst",
        build_evidence_message(candidate, analysis),
    )
