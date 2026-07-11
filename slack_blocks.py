from __future__ import annotations

import logging
from collections.abc import Iterable

import requests


def context_block(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


def section_block(text: str, fields: list | None = None) -> dict:
    block = {"type": "section", "text": {"type": "mrkdwn", "text": text}}
    if fields:
        block["fields"] = [{"type": "mrkdwn", "text": field} for field in fields]
    return block


def divider_block() -> dict:
    return {"type": "divider"}


def chunk_blocks(blocks: list, size: int = 40) -> Iterable[tuple[int, list]]:
    for idx in range(0, len(blocks), size):
        yield idx, blocks[idx:idx + size]


def send_slack_blocks(
    blocks: list,
    *,
    webhook: str,
    text: str,
    logger: logging.Logger | None = None,
    timeout: int = 10,
    print_when_missing: bool = False,
) -> bool:
    logger = logger or logging.getLogger(__name__)
    if not webhook:
        logger.warning("SLACK_WEBHOOK_URL not set")
        if print_when_missing:
            for block in blocks:
                text_obj = block.get("text") if isinstance(block, dict) else None
                if isinstance(text_obj, dict):
                    print(text_obj.get("text", ""))
        return False

    ok = True
    for start, chunk in chunk_blocks(blocks):
        try:
            resp = requests.post(webhook, json={"text": text, "blocks": chunk}, timeout=timeout)
            if resp.status_code != 200:
                logger.error(f"Slack error: {resp.status_code} {resp.text}")
                ok = False
            else:
                logger.info(f"Slack sent OK ({start + 1}-{start + len(chunk)}/{len(blocks)})")
        except Exception as exc:
            logger.error(f"Slack send: {exc}")
            ok = False
    return ok
