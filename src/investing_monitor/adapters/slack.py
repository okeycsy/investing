from __future__ import annotations

import asyncio
from collections.abc import Callable

import requests

from investing_monitor.ports.providers import DeliveryOutcomeUnknown, DeliveryRejected


class SlackWebhookNotifier:
    def __init__(
        self,
        webhook_url: str,
        *,
        post: Callable[..., object] | None = None,
        timeout: float = 10,
    ) -> None:
        self.webhook_url = webhook_url.strip()
        if not self.webhook_url.startswith("https://"):
            raise ValueError("a secure Slack webhook URL is required")
        self._post = post or requests.post
        self.timeout = timeout

    async def send(self, payload: dict) -> str:
        return await asyncio.to_thread(self._send_sync, payload)

    def _send_sync(self, payload: dict) -> str:
        try:
            response = self._post(
                self.webhook_url,
                json=payload,
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise DeliveryOutcomeUnknown(
                f"Slack delivery outcome is unknown: {type(exc).__name__}"
            ) from exc

        status_code = int(response.status_code)
        if 200 <= status_code < 300:
            return str(response.headers.get("x-slack-req-id") or "accepted")
        if status_code == 429:
            retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
            raise DeliveryRejected(
                "Slack rate limited the notification",
                retryable=True,
                retry_after_seconds=retry_after,
            )
        if status_code >= 500:
            raise DeliveryRejected(
                f"Slack rejected the notification with HTTP {status_code}",
                retryable=True,
            )
        raise DeliveryRejected(
            f"Slack permanently rejected the notification with HTTP {status_code}",
            retryable=False,
        )


def _retry_after_seconds(value: object) -> int | None:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return None
    return max(1, min(parsed, 15 * 60))
