from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_FILE = ROOT / "monitor_config.md"


@dataclass(frozen=True)
class MonitorConfig:
    ticker: str = "VRT"
    company_name: str = "Vertiv Holdings Co"
    cik: str = "0001674101"
    benchmark: str = "SOXX"
    peer_tickers: tuple[str, ...] = ("ETN", "NVT", "GEV")
    app_store_id: str = ""
    state_dir: str = "."
    market_scan_focus: str = ""
    sec_contact: str = "maybe2213@naver.com"
    company_aliases: tuple[str, ...] = ("Vertiv", "Vertiv Holdings")
    news_terms: tuple[str, ...] = (
        "data center cooling",
        "liquid cooling",
        "AI infrastructure",
        "hyperscaler",
    )
    priority_keywords: tuple[str, ...] = (
        "backlog",
        "orders",
        "organic sales",
        "guidance",
        "operating margin",
        "free cash flow",
    )
    risk_keywords: tuple[str, ...] = (
        "margin pressure",
        "guidance cut",
        "order cancellation",
        "supply chain",
        "tariff",
        "customer concentration",
    )
    core_kpis: tuple[str, ...] = (
        "backlog",
        "organic sales growth",
        "adjusted operating margin",
        "EPS guidance",
        "revenue guidance",
        "free cash flow",
    )
    profile_context: str = (
        "AI data-center power and thermal infrastructure, especially liquid cooling; "
        "watch orders, backlog conversion, capacity, margins, guidance, and cash flow."
    )

    @property
    def display_ticker(self) -> str:
        return f"${self.ticker}"

    @property
    def effective_market_scan_focus(self) -> str:
        return normalize_ticker(self.market_scan_focus or self.ticker)

    @property
    def issuer_keywords(self) -> tuple[str, ...]:
        words = [self.ticker, *self.company_aliases]
        if self.company_name:
            words.extend(re.findall(r"[A-Za-z0-9]+", self.company_name.upper()))
        return tuple(dict.fromkeys(w.upper() for w in words if len(w) >= 3))

    @property
    def sec_user_agent(self) -> str:
        override = os.environ.get("SEC_USER_AGENT", "").strip()
        if override:
            return override
        company = self.company_name.strip() or self.ticker
        return f"TickerMonitor/1.0 ({company}; ticker={self.ticker})"

    @property
    def sec_headers(self) -> dict[str, str]:
        headers = {
            "User-Agent": self.sec_user_agent,
            "Accept-Encoding": "gzip, deflate",
        }
        if "@" in self.sec_contact:
            headers["From"] = self.sec_contact
        return headers

    @property
    def sec_legacy_headers(self) -> dict[str, str]:
        user_agent = os.environ.get("SEC_LEGACY_USER_AGENT", "").strip()
        if not user_agent:
            user_agent = self.sec_user_agent
        headers = {
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
        }
        if "@" in self.sec_contact:
            headers["From"] = self.sec_contact
        return headers


def normalize_ticker(value: str) -> str:
    return (value or "").strip().upper().lstrip("$")


def _clean_value(value: str) -> str:
    value = value.strip().strip("`").strip()
    if " #" in value:
        value = value.split(" #", 1)[0].strip()
    return value.strip().strip("\"'")


def _parse_list(value: str) -> tuple[str, ...]:
    parts = re.split(r"[,/ ]+", value)
    return tuple(dict.fromkeys(normalize_ticker(p) for p in parts if normalize_ticker(p)))


def _parse_text_list(value: str) -> tuple[str, ...]:
    parts = re.split(r"[,;]", value or "")
    return tuple(dict.fromkeys(part.strip() for part in parts if part.strip()))


def _env_or_default(name: str, default: str) -> str:
    return os.environ.get(name) or default


def _read_markdown_config(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("```"):
            continue
        line = line.lstrip("-* ").strip()
        if ":" in line:
            key, value = line.split(":", 1)
        elif "=" in line:
            key, value = line.split("=", 1)
        else:
            continue
        key = key.strip().lower().replace("-", "_").replace(" ", "_")
        values[key] = _clean_value(value)
    return values


def load_monitor_config(path: str | Path | None = None) -> MonitorConfig:
    config_file = Path(
        path
        or os.environ.get("MONITOR_CONFIG_FILE", "")
        or DEFAULT_CONFIG_FILE
    )
    raw = _read_markdown_config(config_file)

    ticker = normalize_ticker(_env_or_default("MONITOR_TICKER", raw.get("ticker", "VRT")))
    peer_source = _env_or_default("MONITOR_PEER_TICKERS", raw.get("peer_tickers", "ETN,NVT,GEV"))

    def text_list(env_name: str, key: str, default: str) -> tuple[str, ...]:
        return _parse_text_list(_env_or_default(env_name, raw.get(key, default)))

    return MonitorConfig(
        ticker=ticker or "VRT",
        company_name=_env_or_default("MONITOR_COMPANY_NAME", raw.get("company_name", "Vertiv Holdings Co")),
        cik=_env_or_default("MONITOR_CIK", raw.get("cik", "0001674101")).strip(),
        benchmark=normalize_ticker(_env_or_default("MONITOR_BENCHMARK", raw.get("benchmark", "SOXX"))) or "SOXX",
        peer_tickers=_parse_list(peer_source),
        app_store_id=_env_or_default("MONITOR_APP_STORE_ID", raw.get("app_store_id", "")).strip(),
        state_dir=_env_or_default("MONITOR_STATE_DIR", raw.get("state_dir", ".")).strip() or ".",
        market_scan_focus=normalize_ticker(_env_or_default("MARKET_SCAN_FOCUS_TICKER", raw.get("market_scan_focus", ""))),
        sec_contact=_env_or_default("SEC_CONTACT", raw.get("sec_contact", "maybe2213@naver.com")).strip(),
        company_aliases=text_list(
            "MONITOR_COMPANY_ALIASES", "company_aliases", "Vertiv, Vertiv Holdings"
        ),
        news_terms=text_list(
            "MONITOR_NEWS_TERMS", "news_terms",
            "data center cooling, liquid cooling, AI infrastructure, hyperscaler",
        ),
        priority_keywords=text_list(
            "MONITOR_PRIORITY_KEYWORDS", "priority_keywords",
            "backlog, orders, organic sales, guidance, operating margin, free cash flow",
        ),
        risk_keywords=text_list(
            "MONITOR_RISK_KEYWORDS", "risk_keywords",
            "margin pressure, guidance cut, order cancellation, supply chain, tariff, customer concentration",
        ),
        core_kpis=text_list(
            "MONITOR_CORE_KPIS", "core_kpis",
            "backlog, organic sales growth, adjusted operating margin, EPS guidance, revenue guidance, free cash flow",
        ),
        profile_context=_env_or_default(
            "MONITOR_PROFILE_CONTEXT",
            raw.get(
                "profile_context",
                "AI data-center power and thermal infrastructure, especially liquid cooling; "
                "watch orders, backlog conversion, capacity, margins, guidance, and cash flow.",
            ),
        ).strip(),
    )


def resolve_runtime_file(config: MonitorConfig, legacy_name: str, env_var: str) -> Path:
    override = os.environ.get(env_var, "").strip()
    if override:
        return Path(override)

    legacy_path = ROOT / legacy_name
    if config.ticker == "HOOD" and legacy_path.exists():
        return legacy_path

    state_dir = Path(config.state_dir)
    if not state_dir.is_absolute():
        state_dir = ROOT / state_dir

    if legacy_name == "state.json":
        filename = f"{config.ticker.lower()}_state.json"
    elif legacy_name == "weekly_state.json":
        filename = f"{config.ticker.lower()}_weekly_state.json"
    elif legacy_name == "beta_cache.json":
        filename = f"{config.ticker.lower()}_beta_cache.json"
    elif legacy_name == "app_rank_cache.json":
        filename = f"{config.ticker.lower()}_app_rank_cache.json"
    else:
        filename = f"{config.ticker.lower()}_{legacy_name}"

    return state_dir / filename
