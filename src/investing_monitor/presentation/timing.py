from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from investing_monitor.domain.models import MarketSession, VolumeSnapshot


KST = ZoneInfo("Asia/Seoul")
DELAYED_DETECTION_SECONDS = 10 * 60


def timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("message timestamp must be timezone-aware")
    return value.astimezone(KST).strftime("%m/%d %H:%M KST")


def session_label(session: MarketSession) -> str:
    return {
        MarketSession.PRE: "프리마켓",
        MarketSession.REGULAR: "정규장",
        MarketSession.POST: "애프터마켓",
        MarketSession.CLOSED: "장외",
    }[session]


def delay_seconds(observed_at: datetime, detected_at: datetime | None) -> int:
    if detected_at is None:
        return 0
    if observed_at.tzinfo is None or detected_at.tzinfo is None:
        raise ValueError("detection timestamps must be timezone-aware")
    return max(0, int((
        detected_at.astimezone(timezone.utc) - observed_at.astimezone(timezone.utc)
    ).total_seconds()))


def duration_label(seconds: int) -> str:
    minutes = max(1, round(seconds / 60))
    hours, remaining = divmod(minutes, 60)
    if not hours:
        return f"{remaining}분"
    if not remaining:
        return f"{hours}시간"
    return f"{hours}시간 {remaining}분"


def observation_context(
    observed_at: datetime,
    label: str,
    detected_at: datetime | None,
    delay: int,
) -> str:
    lines = [f"{label} · 관측 {timestamp(observed_at)}"]
    if detected_at is not None:
        lines.append(f"봇 확인 {timestamp(detected_at)}")
    if delay > DELAYED_DETECTION_SECONDS:
        lines.append(f"⏱️ 지연 확인 · 관측 후 {duration_label(delay)}")
    return "\n".join(lines)


def volume_basis(volume: VolumeSnapshot) -> str:
    if volume.observed_at is None:
        return "정규장 누적 · 자료 기준 시각 미기록"
    return f"정규장 누적 · {timestamp(volume.observed_at)} 봉까지"
