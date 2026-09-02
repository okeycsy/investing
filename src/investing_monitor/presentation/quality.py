from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


MAX_VISIBLE_CHARACTERS = 2_900
MAX_BLOCKS = 50

FORBIDDEN_TEXT = (
    "내용 확인 필요",
    "정확한 가격과 일일 등락률",
    "DCA",
    "RSI",
    "MACD",
    "PCR",
    "FINRA",
    "HTTP 403",
    "HTTP 429",
    "TRACEBACK",
    "ANTHROPIC_API_KEY",
    "SLACK_WEBHOOK",
    "PROVIDER UNAVAILABLE",
    "TIMED OUT",
    "TIMEOUT",
)

REQUIRED_TEXT = {
    "price_band": ("구간 진입", "반도체 지수("),
    "volume_spike": ("거래량", "종목 방향", "반도체 지수("),
    "daily_close": ("장 마감", "종목 방향", "반도체 지수("),
    "weekly_review": ("주간 논지 리뷰", "주간 방향", "반도체 지수("),
    "catalyst": ("확인된 사실",),
    "filing": ("확인된 사실",),
    "insider": ("거래 규모",),
}

SOURCE_REQUIRED_TYPES = {"catalyst", "filing", "insider"}


class MessageQualityError(ValueError):
    pass


@dataclass(frozen=True)
class MessageQualityResult:
    passed: bool
    visible_characters: int
    block_count: int
    violations: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "visible_characters": self.visible_characters,
            "block_count": self.block_count,
            "violations": list(self.violations),
        }


def audit_message(alert_type: str, payload: Mapping[str, object]) -> MessageQualityResult:
    violations = []
    fallback = payload.get("text")
    blocks = payload.get("blocks")
    if not isinstance(fallback, str) or not fallback.strip():
        violations.append("missing non-empty fallback text")
    if not isinstance(blocks, list) or not blocks:
        violations.append("missing Slack blocks")
        blocks = []
    if len(blocks) > MAX_BLOCKS:
        violations.append(f"block count exceeds {MAX_BLOCKS}")

    visible_parts = [fallback] if isinstance(fallback, str) else []
    block_texts = []
    for block in blocks:
        if not isinstance(block, dict):
            violations.append("block must be an object")
            continue
        text_value = block.get("text")
        if isinstance(text_value, dict):
            text = text_value.get("text")
            if isinstance(text, str):
                visible_parts.append(text)
                block_texts.append(" ".join(text.split()))
                if block.get("type") == "header" and len(text) > 150:
                    violations.append("header text exceeds 150 characters")
                if len(text) > 3_000:
                    violations.append("block text exceeds 3000 characters")
        elements = block.get("elements")
        if isinstance(elements, list):
            for element in elements:
                if isinstance(element, dict) and isinstance(element.get("text"), str):
                    text = element["text"]
                    visible_parts.append(text)
                    block_texts.append(" ".join(text.split()))

    visible = "".join(visible_parts)
    if len(visible) > MAX_VISIBLE_CHARACTERS:
        violations.append(
            f"visible text exceeds {MAX_VISIBLE_CHARACTERS} characters"
        )
    upper_visible = visible.upper()
    for forbidden in FORBIDDEN_TEXT:
        if forbidden.upper() in upper_visible:
            violations.append(f"forbidden user text: {forbidden}")
    for required in REQUIRED_TEXT.get(alert_type, ()):
        if required not in visible:
            violations.append(f"missing required user text: {required}")
    if alert_type in SOURCE_REQUIRED_TYPES and "<http" not in visible:
        violations.append("missing traceable source link")

    normalized_blocks = [text for text in block_texts if text]
    if len(normalized_blocks) != len(set(normalized_blocks)):
        violations.append("duplicate visible block")
    unique_violations = tuple(dict.fromkeys(violations))
    return MessageQualityResult(
        passed=not unique_violations,
        visible_characters=len(visible),
        block_count=len(blocks),
        violations=unique_violations,
    )


def require_valid_message(alert_type: str, payload: Mapping[str, object]) -> None:
    result = audit_message(alert_type, payload)
    if not result.passed:
        raise MessageQualityError("; ".join(result.violations))
