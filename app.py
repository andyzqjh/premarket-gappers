"""
How to run: `cd traderwillhu_flask_gapper`, install the packages from the header below, set `GEMINI_API_KEY`, then run `flask --app app run --debug` and open `http://127.0.0.1:5000`.

AI Premarket Gappers Screener (Flask + TradingView + Multi-Source News + Gemini + ib_chart)

1. Get a free Gemini API key:
   - Create a free Google AI Studio key at https://aistudio.google.com/app/apikey
   - PowerShell:
       $env:GEMINI_API_KEY="your_api_key_here"

2. Install Python packages:
   - Recommended:
       pip install -U flask pandas tradingview-screener google-genai yfinance
       pip install -U "finvizfinance @ git+https://github.com/lit26/finvizfinance.git"

3. Optional TradingView cookies:
   - If TradingView blocks scanner requests in your region, export your cookies JSON:
       $env:TRADINGVIEW_COOKIES_JSON='[{"name":"sessionid","value":"...","domain":".tradingview.com","path":"/"}]'

4. Run the Flask app:
   - PowerShell:
       cd traderwillhu_flask_gapper
       flask --app app run --debug

5. Run ib_chart in a separate terminal:
   - git clone https://github.com/willhjw/ib_chart.git
   - cd ib_chart
   - pip install -r requirements.txt
   - python ib_server.py
   - Multi-chart links will open at:
       http://127.0.0.1:5001/ib_multichart.html?symbols=AAPL,MSFT,NVDA&tf=D
"""

from __future__ import annotations

import json
import math
import os
import re
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import yfinance as yf
from finvizfinance.quote import finvizfinance
from flask import Flask, jsonify, render_template_string, request
from google import genai
from dotenv import load_dotenv
from tradingview_screener import And, Column, Query

APP_DIR = Path(__file__).resolve().parent
WORKSPACE_ENV_PATH = APP_DIR.parent / ".env"
PROJECT_ENV_PATH = APP_DIR / ".env"

load_dotenv(WORKSPACE_ENV_PATH, override=False)
load_dotenv(PROJECT_ENV_PATH, override=True)


APP_TITLE = "AI Premarket Gappers"
APP_SUBTITLE = "TradingView premarket gappers + merged Yahoo/Finviz news + AI catalyst grading"
SCAN_LIMIT = int(os.getenv("SCAN_LIMIT", "18"))
MIN_PREMARKET_PCT = float(os.getenv("MIN_PREMARKET_PCT", "5"))
MIN_PREMARKET_VOLUME = int(os.getenv("MIN_PREMARKET_VOLUME", "200000"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "1"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "9999"))   # no meaningful price cap for large-caps
MIN_MARKET_CAP = float(os.getenv("MIN_MARKET_CAP", "2_000_000_000"))   # $2B+
AUTO_REFRESH_SECONDS = int(os.getenv("AUTO_REFRESH_SECONDS", "60"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))    # throttle to avoid Gemini 429s
ANALYSIS_CACHE_TTL_SECONDS = int(os.getenv("ANALYSIS_CACHE_TTL_SECONDS", "900"))
NEWS_CACHE_TTL_SECONDS = int(os.getenv("NEWS_CACHE_TTL_SECONDS", "300"))
OPENROUTER_TIMEOUT_SECONDS = int(os.getenv("OPENROUTER_TIMEOUT_SECONDS", "90"))
OPENROUTER_MAX_CONCURRENCY = int(os.getenv("OPENROUTER_MAX_CONCURRENCY", "1"))
AI_PROMPT_VERSION = os.getenv("AI_PROMPT_VERSION", "2026-04-01-date-aware")
AI_SYSTEM_MESSAGE = (
    "You are an elite US stock catalyst analyst for active day traders. "
    "Return only valid JSON. Never infer stale or future timing without checking the supplied dates and timing notes."
)
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"
GEMINI_MODEL_CANDIDATES = [
    model.strip()
    for model in os.getenv("GEMINI_MODEL_CANDIDATES", f"{GEMINI_MODEL},gemini-2.5-flash-lite").split(",")
    if model.strip()
]
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
OPENAI_MODEL_CANDIDATES = [
    model.strip()
    for model in os.getenv("OPENAI_MODEL_CANDIDATES", OPENAI_MODEL).split(",")
    if model.strip()
]
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openai/gpt-5.4-mini").strip() or "openai/gpt-5.4-mini"
OPENROUTER_MODEL_CANDIDATES = [
    model.strip()
    for model in os.getenv(
        "OPENROUTER_MODEL_CANDIDATES",
        f"{OPENROUTER_MODEL},google/gemini-2.5-flash-lite,qwen/qwen3-30b-a3b,deepseek/deepseek-chat-v3-0324",
    ).split(",")
    if model.strip()
]
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514").strip() or "claude-sonnet-4-20250514"
ANTHROPIC_MODEL_CANDIDATES = [
    model.strip()
    for model in os.getenv("ANTHROPIC_MODEL_CANDIDATES", ANTHROPIC_MODEL).split(",")
    if model.strip()
]
AI_PROVIDER_ORDER = [
    provider.strip().lower()
    for provider in os.getenv("AI_PROVIDER_ORDER", "gemini,openrouter,openai,anthropic").split(",")
    if provider.strip()
]
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Asia/Singapore").strip() or "Asia/Singapore"
LOCAL_TIMEZONE = ZoneInfo(APP_TIMEZONE)
DEFAULT_OBSIDIAN_VAULT_PATH = APP_DIR.parent.parent / "STOCKS VAULT"
OBSIDIAN_VAULT_PATH = Path(
    os.getenv("OBSIDIAN_VAULT_PATH", str(DEFAULT_OBSIDIAN_VAULT_PATH))
).expanduser()
OBSIDIAN_EXPORT_ENABLED = os.getenv("OBSIDIAN_EXPORT_ENABLED", "1").lower() in {"1", "true", "yes", "on"}
OBSIDIAN_EXPORT_FOLDER = os.getenv("OBSIDIAN_EXPORT_FOLDER", "Trading Dashboard/Sectors").strip(
) or "Trading Dashboard/Sectors"
OBSIDIAN_EXPORT_TRIGGER_ORIGINS = {
    origin.strip().lower()
    for origin in os.getenv("OBSIDIAN_EXPORT_TRIGGER_ORIGINS", "view,manual,scheduled").split(",")
    if origin.strip()
}
OBSIDIAN_EXPORT_BOARD_KINDS = {
    kind.strip().lower()
    for kind in os.getenv("OBSIDIAN_EXPORT_BOARD_KINDS", "watchlist,premarket,market-brief").split(",")
    if kind.strip()
}
SCHEDULED_SCAN_ENABLED = os.getenv("SCHEDULED_SCAN_ENABLED", "1").lower() in {"1", "true", "yes", "on"}
SCHEDULED_SCAN_TIMES = [
    stamp.strip()
    for stamp in os.getenv("SCHEDULED_SCAN_TIMES", "08:30,20:30").split(",")
    if stamp.strip()
]
SCHEDULED_SCAN_BOARD_KINDS = {
    kind.strip().lower()
    for kind in os.getenv("SCHEDULED_SCAN_BOARD_KINDS", "premarket,watchlist,market-brief").split(",")
    if kind.strip()
}
SCHEDULED_SCAN_STAGGER_SECONDS = int(os.getenv("SCHEDULED_SCAN_STAGGER_SECONDS", "8"))
MARKET_BRIEF_MACRO_SYMBOLS = [
    symbol.strip()
    for symbol in os.getenv("MARKET_BRIEF_MACRO_SYMBOLS", "SPY,QQQ,IWM,DIA,^VIX,TLT").split(",")
    if symbol.strip()
]
IB_CHART_BASE_URL = os.getenv(
    "IB_CHART_BASE_URL",
    "http://127.0.0.1:5001/ib_multichart.html",
).strip() or "http://127.0.0.1:5001/ib_multichart.html"
EXCHANGES = ["NASDAQ", "NYSE", "AMEX"]
DEFAULT_SHARED_WATCHLIST_URLS = [
    "https://www.tradingview.com/watchlists/323151534/",
    "https://www.tradingview.com/watchlists/315855742/",
    "https://www.tradingview.com/watchlists/315856199/",
    "https://www.tradingview.com/watchlists/316344074/",
    "https://www.tradingview.com/watchlists/324104365/",
    "https://www.tradingview.com/watchlists/315855592/",
    "https://www.tradingview.com/watchlists/316749547/",
    "https://www.tradingview.com/watchlists/319031060/",
    "https://www.tradingview.com/watchlists/315855978/",
    "https://www.tradingview.com/watchlists/315857640/",
    "https://www.tradingview.com/watchlists/315857161/",
    "https://www.tradingview.com/watchlists/320852057/",
]
WATCHLIST_HTTP_HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_MERGED_NEWS_ITEMS = int(os.getenv("MAX_MERGED_NEWS_ITEMS", "12"))
MAX_YFINANCE_NEWS_ITEMS = int(os.getenv("MAX_YFINANCE_NEWS_ITEMS", "8"))

CATEGORY_OPTIONS = [
    "Earnings",
    "FDA / Clinical",
    "PR / Contract",
    "Financing / Offering",
    "Analyst / Upgrade",
    "M&A / Rumor",
    "Sympathy / Sector",
    "Low Float Momentum",
    "No Fresh News",
]

CATEGORY_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    ("M&A / Rumor", ("acquire", "acquires", "acquired", "acquisition", "deal", "merger", "buyout", "takeover", "stake", "agrees")),
    ("Financing / Offering", ("offering", "financing", "loan", "credit facility", "notes", "convertible", "raises", "investment", "stake sale", "capital", "funding", "facility", "backed by", "non-dilutive")),
    ("FDA / Clinical", ("fda", "phase", "trial", "clinical", "approval", "pivotal", "bLA", "supplemental", "study", "submission")),
    ("Earnings", ("earnings", "guidance", "revenue", "eps", "quarter", "sales", "results")),
    ("PR / Contract", ("contract", "award", "order", "partnership", "agreement", "launch missions", "secures", "wins", "collaboration", "construct", "factory", "build", "expansion", "capacity", "facility", "unveiling")),
    ("Analyst / Upgrade", ("upgrade", "downgrade", "price target", "initiates", "coverage", "rating")),
    ("Sympathy / Sector", ("sector", "peer", "industry", "stocks jump", "shares rise", "nasdaq", "dow", "market", "war", "fed", "macro")),
]

MACRO_NEWS_HINTS = {
    "nasdaq", "dow", "s&p", "sp500", "market", "stocks jump", "war", "trump",
    "fed", "rates", "macro", "treasury", "yields", "futures", "sector",
}

LEGAL_ALERT_HINTS = {
    "sued", "investigation", "class action", "law group", "law violations",
    "shareholder alert", "discuss your rights", "deadline", "lawsuit",
}

COMMENTARY_HEADLINE_HINTS = {
    "bets big",
    "dear ",
    "explains why",
    "here's what",
    "here’s what",
    "jim cramer",
    "means for",
    "outshining",
    "winner",
    "what investors need to know",
    "what happens when",
    "best buy now",
    "three key reasons",
    "reasons to hold",
    "reasons to buy",
    "stock now",
    "should you buy",
    "is it a buy",
    "neophyte",
    "why the",
    "why it's",
    "why its",
}

HIGH_TRUST_NEWS_SOURCES = {
    "associated press",
    "bloomberg",
    "business wire",
    "dow jones",
    "globenewswire",
    "marketwatch",
    "new york times",
    "pr newswire",
    "reuters",
    "the associated press",
    "the wall street journal",
    "wall street journal",
}

LOW_SIGNAL_NEWS_SOURCES = {
    "24/7 wall st.",
    "gurufocus.com",
    "insider monkey",
    "investorplace",
    "motley fool",
    "schaeffer's research",
    "simply wall st.",
    "stockstory",
    "tipranks",
    "zacks",
}

LOW_SIGNAL_EVENT_NOTICE_HINTS = {
    "conference call",
    "earnings call",
    "webcast",
    "fireside chat",
    "to present",
    "to participate",
    "investor conference",
    "investor day",
    "conference appearance",
    "live at",
    "summit",
    "panel discussion",
    "annual meeting",
}

CONCRETE_EVENT_HINTS = {
    "agrees",
    "approval",
    "award",
    "backed by",
    "build",
    "closes",
    "collaboration",
    "confirms",
    "construct",
    "contract",
    "deal",
    "expansion",
    "factory",
    "financing",
    "funding",
    "guidance",
    "investment",
    "launch",
    "loan",
    "merger",
    "non-dilutive",
    "order",
    "partnership",
    "phase",
    "pivotal",
    "receives",
    "results",
    "secures",
    "stake",
    "submission",
    "trial",
    "unveiling",
    "wins",
}

POSITIVE_FINANCING_HINTS = {
    "backed by",
    "credit facility",
    "facility",
    "funding",
    "gpu-backed",
    "investment",
    "loan",
    "non-dilutive",
    "secured",
    "secures",
}

DILUTIVE_FINANCING_HINTS = {
    "atm",
    "convertible",
    "offering",
    "private placement",
    "registered direct",
    "share sale",
    "warrant",
}

DIRECT_CATALYST_CATEGORIES = {
    "Earnings",
    "FDA / Clinical",
    "PR / Contract",
    "Financing / Offering",
    "Analyst / Upgrade",
    "M&A / Rumor",
}

AI_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "category": {"type": "string", "enum": CATEGORY_OPTIONS},
        "grade": {"type": "string", "enum": ["A", "B", "C", "D"]},
        "reasoning": {"type": "string"},
        "analysis_details": {"type": "string"},
    },
    "required": ["category", "grade", "reasoning", "analysis_details"],
}

GRADE_ORDER = {"A": 0, "B": 1, "C": 2, "D": 3}
ANALYSIS_CACHE: dict[str, dict[str, Any]] = {}
ANALYSIS_CACHE_LOCK = threading.RLock()
MARKET_BRIEF_CACHE: dict[str, dict[str, Any]] = {}
MARKET_BRIEF_CACHE_LOCK = threading.RLock()
NEWS_CACHE: dict[str, dict[str, Any]] = {}
NEWS_CACHE_LOCK = threading.RLock()
GEMINI_CLIENT_LOCK = threading.RLock()
GEMINI_CLIENT: genai.Client | None = None
AI_STATE_LOCK = threading.RLock()
AI_PROVIDER_COOLDOWNS: dict[str, float] = {}
WATCHLIST_CACHE_LOCK = threading.RLock()
WATCHLIST_CACHE: dict[str, dict[str, Any]] = {}
BOARD_DEFINITIONS_LOCK = threading.RLock()
BOARD_DEFINITIONS_CACHE: list[dict[str, Any]] | None = None
OPENROUTER_REQUEST_SEMAPHORE = threading.Semaphore(max(1, OPENROUTER_MAX_CONCURRENCY))


def provider_display_name(provider: str) -> str:
    names = {
        "gemini": "Gemini",
        "openrouter": "OpenRouter",
        "openai": "OpenAI",
        "anthropic": "Anthropic",
        "fallback": "Fallback",
    }
    return names.get(provider.lower(), provider.title())


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def local_now() -> datetime:
    return datetime.now(LOCAL_TIMEZONE)


def to_local_time(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(LOCAL_TIMEZONE)


def to_epoch(value: datetime | None) -> int | None:
    if value is None:
        return None
    return int(value.timestamp())


def to_local_epoch(value: datetime | None) -> int | None:
    local_value = to_local_time(value)
    if local_value is None:
        return None
    return int(local_value.timestamp())


def parse_schedule_times(time_strings: list[str]) -> list[tuple[int, int]]:
    parsed: list[tuple[int, int]] = []
    for stamp in time_strings:
        try:
            hour_text, minute_text = stamp.split(":", 1)
            parsed.append((int(hour_text), int(minute_text)))
        except ValueError:
            continue
    return sorted(set(parsed))


def format_schedule_times_label() -> str:
    labels = []
    for hour, minute in parse_schedule_times(SCHEDULED_SCAN_TIMES):
        labels.append(datetime(2000, 1, 1, hour, minute).strftime("%I:%M %p").lstrip("0"))
    if not labels:
        return "Disabled"
    return " & ".join(labels) + f" {APP_TIMEZONE}"


def next_scheduled_run_local(from_time: datetime | None = None) -> datetime | None:
    schedule_times = parse_schedule_times(SCHEDULED_SCAN_TIMES)
    if not schedule_times:
        return None

    current = from_time or local_now()
    candidates: list[datetime] = []
    for day_offset in (0, 1, 2):
        base_date = (current + timedelta(days=day_offset)).date()
        for hour, minute in schedule_times:
            candidates.append(datetime(base_date.year, base_date.month, base_date.day, hour, minute, tzinfo=LOCAL_TIMEZONE))
    for candidate in sorted(candidates):
        if candidate >= current:
            return candidate
    return None


def current_schedule_slot_key(current: datetime | None = None) -> str | None:
    schedule_times = parse_schedule_times(SCHEDULED_SCAN_TIMES)
    if not schedule_times:
        return None
    now_value = current or local_now()
    for hour, minute in schedule_times:
        slot = now_value.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if 0 <= (now_value - slot).total_seconds() < 60:
            return slot.strftime("%Y-%m-%d %H:%M")
    return None


def finite_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(number):
        return default
    return number


def finite_int(value: Any, default: int = 0) -> int:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(number):
        return default
    return int(number)


def sanitize_json_compatible(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: sanitize_json_compatible(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_json_compatible(item) for item in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def parse_metric_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "-", "--", "n/a"}:
        return None

    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1]

    text = text.replace(",", "").replace("%", "").strip()
    match = re.match(r"^(-?\d+(?:\.\d+)?)([KMBT])?$", text, re.IGNORECASE)
    if match:
        base = float(match.group(1))
        suffix = (match.group(2) or "").upper()
        multiplier = {"": 1, "K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}[suffix]
        result = base * multiplier
        return -result if negative else result

    try:
        result = float(text)
    except ValueError:
        return None
    return -result if negative else result


def compact_number(value: Any, decimals: int = 1) -> str:
    number = parse_metric_number(value)
    if number is None:
        return "-"
    absolute = abs(number)
    suffix = ""
    divisor = 1.0
    if absolute >= 1e12:
        suffix, divisor = "T", 1e12
    elif absolute >= 1e9:
        suffix, divisor = "B", 1e9
    elif absolute >= 1e6:
        suffix, divisor = "M", 1e6
    elif absolute >= 1e3:
        suffix, divisor = "K", 1e3

    if suffix:
        scaled = number / divisor
        if abs(scaled) >= 100:
            return f"{scaled:.0f}{suffix}"
        if abs(scaled) >= 10:
            return f"{scaled:.1f}{suffix}"
        return f"{scaled:.{decimals}f}{suffix}"
    return f"{number:,.0f}"


def format_percent(value: Any, decimals: int = 1, always_sign: bool = True) -> str:
    number = parse_metric_number(value)
    if number is None:
        return "-"
    sign = "+" if always_sign and number > 0 else ""
    return f"{sign}{number:.{decimals}f}%"


def clean_symbol(symbol: str) -> str:
    return re.sub(r"[^A-Z0-9.\-]", "", str(symbol or "").upper())


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def tradingview_request_kwargs() -> dict[str, Any]:
    raw_cookies = os.getenv("TRADINGVIEW_COOKIES_JSON", "").strip()
    if not raw_cookies:
        return {}
    try:
        cookies = json.loads(raw_cookies)
    except json.JSONDecodeError:
        return {}
    return {"cookies": cookies}


def unique_watchlist_urls() -> list[str]:
    raw = os.getenv("TRADINGVIEW_WATCHLIST_URLS", "").strip()
    urls = [item.strip() for item in raw.splitlines() if item.strip()] if raw else DEFAULT_SHARED_WATCHLIST_URLS
    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if url not in seen:
            seen.add(url)
            deduped.append(url)
    return deduped


def watchlist_id_from_url(url: str) -> str:
    match = re.search(r"/watchlists/(\d+)/", url)
    if match:
        return f"watchlist-{match.group(1)}"
    return f"watchlist-{abs(hash(url))}"


def parse_watchlist_payload(html: str) -> dict[str, Any]:
    match = re.search(r'<script type="application/prs.init-data\+json">(.*?)</script>', html, re.S)
    if not match:
        raise ValueError("TradingView watchlist payload was not found in the page.")
    payload = json.loads(match.group(1))
    shared_watchlist = payload.get("sharedWatchlist", {})
    watchlist = shared_watchlist.get("list")
    if not watchlist:
        raise ValueError("TradingView page did not expose a shared watchlist payload.")
    return {"watchlist": watchlist, "author": shared_watchlist.get("author", {})}


def fetch_watchlist_definition(url: str) -> dict[str, Any]:
    cache_key = url.strip()
    with WATCHLIST_CACHE_LOCK:
        cached = WATCHLIST_CACHE.get(cache_key)
        if cached:
            return cached

    response = requests.get(url, headers=WATCHLIST_HTTP_HEADERS, timeout=30)
    response.raise_for_status()
    payload = parse_watchlist_payload(response.text)
    watchlist = payload["watchlist"]
    symbols = [safe_text(symbol) for symbol in watchlist.get("symbols", []) if safe_text(symbol)]
    board = {
        "id": watchlist_id_from_url(url),
        "kind": "watchlist",
        "title": safe_text(watchlist.get("name")) or watchlist_id_from_url(url),
        "description": f"{len(symbols)} symbols from TradingView shared watchlist",
        "url": url,
        "symbols": symbols,
        "symbol_count": len(symbols),
        "watchlist_id": watchlist.get("id"),
    }
    with WATCHLIST_CACHE_LOCK:
        WATCHLIST_CACHE[cache_key] = board
    return board


def get_board_definitions() -> list[dict[str, Any]]:
    global BOARD_DEFINITIONS_CACHE
    with BOARD_DEFINITIONS_LOCK:
        if BOARD_DEFINITIONS_CACHE is not None:
            return BOARD_DEFINITIONS_CACHE

        boards: list[dict[str, Any]] = [
            {
                "id": "premarket",
                "kind": "premarket",
                "title": "Premarket Gappers",
                "description": "TradingView premarket scan with Finviz news and catalyst grading",
                "symbol_count": 0,
            },
            {
                "id": "market-brief",
                "kind": "market-brief",
                "title": "Market Brief",
                "description": "Macro and sector briefs across your watchlists",
                "symbol_count": 0,
            }
        ]

        for url in unique_watchlist_urls():
            try:
                boards.append(fetch_watchlist_definition(url))
            except Exception as exc:
                boards.append(
                    {
                        "id": watchlist_id_from_url(url),
                        "kind": "watchlist",
                        "title": watchlist_id_from_url(url),
                        "description": f"Failed to load watchlist metadata: {exc}",
                        "url": url,
                        "symbols": [],
                        "symbol_count": 0,
                        "load_error": str(exc),
                    }
                )

        BOARD_DEFINITIONS_CACHE = boards
        return BOARD_DEFINITIONS_CACHE


def get_board_definition(board_id: str) -> dict[str, Any]:
    for board in get_board_definitions():
        if board["id"] == board_id:
            return board
    return get_board_definitions()[0]


def get_cell_value(record: dict[str, Any], *keys: str) -> Any:
    lowered = {str(key).lower(): value for key, value in record.items()}
    for key in keys:
        if key in record:
            return record[key]
        lowered_key = key.lower()
        if lowered_key in lowered:
            return lowered[lowered_key]
    return None


def normalize_news_frame(news_frame: Any) -> list[dict[str, str]]:
    if news_frame is None or not isinstance(news_frame, pd.DataFrame) or news_frame.empty:
        return []

    items: list[dict[str, str]] = []
    trimmed = news_frame.head(5).copy()
    for _, row in trimmed.iterrows():
        record = row.to_dict()
        date_value = get_cell_value(record, "Date", "date")
        title = safe_text(get_cell_value(record, "Title", "title"))
        link = safe_text(get_cell_value(record, "Link", "link", "URL", "url"))
        source = safe_text(get_cell_value(record, "Source", "source"))
        if not title:
            continue

        published_at = parse_news_timestamp(date_value)
        if published_at is not None:
            date_text = published_at.strftime("%b %d %I:%M %p")
            published_iso = published_at.isoformat()
        elif hasattr(date_value, "strftime"):
            date_text = date_value.strftime("%b %d %I:%M %p")
            published_iso = ""
        else:
            date_text = safe_text(date_value)
            published_iso = ""

        items.append({"time": date_text, "published_at": published_iso, "title": title, "source": source, "url": link})
    return items


def normalize_yfinance_news(news_items: Any) -> list[dict[str, str]]:
    if not isinstance(news_items, list):
        return []

    normalized: list[dict[str, str]] = []
    for item in news_items[:MAX_YFINANCE_NEWS_ITEMS]:
        content = item.get("content") or {}
        content_type = safe_text(content.get("contentType")).upper()
        if content_type and content_type not in {"STORY", "ARTICLE"}:
            continue

        title = clean_headline_text(content.get("title", ""))
        if not title:
            continue

        provider = content.get("provider") or {}
        source = safe_text(provider.get("displayName")) or safe_text(item.get("publisher")) or "Yahoo Finance"
        url = safe_text((content.get("clickThroughUrl") or {}).get("url")) or safe_text((content.get("canonicalUrl") or {}).get("url"))
        published_at = parse_news_timestamp(content.get("pubDate"))
        time_text = published_at.strftime("%b %d %I:%M %p") if published_at is not None else safe_text(content.get("displayTime"))
        normalized.append(
            {
                "time": time_text,
                "published_at": published_at.isoformat() if published_at is not None else "",
                "title": title,
                "source": source,
                "url": url,
            }
        )
    return normalized


def news_dedupe_key(item: dict[str, str]) -> str:
    title_key = re.sub(r"[^a-z0-9]+", "", clean_headline_text(item.get("title", "")).lower())
    if title_key:
        return f"title:{title_key}"
    url_key = safe_text(item.get("url", "")).strip().lower()
    return f"url:{url_key}"


def merge_news_items(*collections: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: list[dict[str, str]] = []
    seen: set[str] = set()

    all_items: list[dict[str, str]] = []
    for collection in collections:
        all_items.extend(collection or [])

    def sort_key(item: dict[str, str]) -> tuple[float, str]:
        published_at = parse_news_timestamp(item.get("published_at") or item.get("time"))
        published_epoch = published_at.timestamp() if published_at is not None else 0.0
        return (published_epoch, clean_headline_text(item.get("title", "")).lower())

    for item in sorted(all_items, key=sort_key, reverse=True):
        dedupe_key = news_dedupe_key(item)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        merged.append(item)

    return merged[:MAX_MERGED_NEWS_ITEMS]


def fetch_yfinance_news_items(ticker: str) -> list[dict[str, str]]:
    try:
        return normalize_yfinance_news(yf.Ticker(ticker).get_news(count=MAX_YFINANCE_NEWS_ITEMS))
    except Exception:
        return []


def infer_category_from_news(news_items: list[dict[str, str]]) -> str:
    if not news_items:
        return "No Fresh News"

    combined = " ".join(item["title"] for item in news_items).lower()
    keyword_map = [
        ("fda", "FDA / Clinical"),
        ("phase", "FDA / Clinical"),
        ("trial", "FDA / Clinical"),
        ("clinical", "FDA / Clinical"),
        ("earnings", "Earnings"),
        ("guidance", "Earnings"),
        ("eps", "Earnings"),
        ("revenue", "Earnings"),
        ("public offering", "Financing / Offering"),
        ("registered direct", "Financing / Offering"),
        ("private placement", "Financing / Offering"),
        ("offering", "Financing / Offering"),
        ("pricing", "Financing / Offering"),
        ("acquire", "M&A / Rumor"),
        ("merger", "M&A / Rumor"),
        ("buyout", "M&A / Rumor"),
        ("rumor", "M&A / Rumor"),
        ("upgrade", "Analyst / Upgrade"),
        ("initiates", "Analyst / Upgrade"),
        ("price target", "Analyst / Upgrade"),
        ("contract", "PR / Contract"),
        ("order", "PR / Contract"),
        ("partnership", "PR / Contract"),
        ("agreement", "PR / Contract"),
        ("sector", "Sympathy / Sector"),
        ("peer", "Sympathy / Sector"),
    ]
    for needle, category in keyword_map:
        if needle in combined:
            return category
    return "Low Float Momentum"


def clean_headline_text(text: str) -> str:
    return re.sub(r"\s+", " ", safe_text(text)).strip()


def shorten_text(text: str, limit: int = 88) -> str:
    cleaned = clean_headline_text(text)
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(0, limit - 3)].rstrip(". ,;:") + "..."


MONTH_NAME_TO_NUMBER = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
HEADLINE_DATE_PATTERN = re.compile(
    r"\b("
    r"Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:t|tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?"
    r")\.?\s+(\d{1,2})(?:,\s*(\d{4}))?\b",
    re.IGNORECASE,
)


def parse_news_timestamp(date_value: Any, reference: datetime | None = None) -> datetime | None:
    if date_value is None:
        return None

    if reference is None:
        reference = local_now()

    if isinstance(date_value, datetime):
        if date_value.tzinfo is None:
            candidate = date_value.replace(tzinfo=LOCAL_TIMEZONE)
        else:
            candidate = date_value.astimezone(LOCAL_TIMEZONE)
        return candidate

    text = safe_text(date_value)
    if not text:
        return None

    try:
        iso_candidate = datetime.fromisoformat(text)
    except ValueError:
        iso_candidate = None
    if iso_candidate is not None:
        if iso_candidate.tzinfo is None:
            return iso_candidate.replace(tzinfo=LOCAL_TIMEZONE)
        return iso_candidate.astimezone(LOCAL_TIMEZONE)

    for fmt in ("%b %d %I:%M %p", "%b-%d-%y %I:%M%p", "%b %d, %Y %I:%M %p", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue

        if "%Y" not in fmt and "%y" not in fmt:
            parsed = parsed.replace(year=reference.year)
            candidate = parsed.replace(tzinfo=LOCAL_TIMEZONE)
            if candidate > reference + timedelta(days=45):
                candidate = candidate.replace(year=candidate.year - 1)
            return candidate

        candidate = parsed.replace(tzinfo=LOCAL_TIMEZONE)
        return candidate

    return None


def describe_relative_days(target: datetime | None, reference: datetime | None = None) -> str:
    if target is None:
        return ""
    if reference is None:
        reference = local_now()
    day_delta = (target.date() - reference.date()).days
    if day_delta == 0:
        return "today"
    if day_delta == -1:
        return "1 day ago"
    if day_delta == 1:
        return "1 day ahead"
    if day_delta < 0:
        return f"{abs(day_delta)} days ago"
    return f"{day_delta} days ahead"


def extract_headline_dates(title: str, reference: datetime | None = None) -> list[datetime]:
    if reference is None:
        reference = local_now()
    found_dates: list[datetime] = []
    for match in HEADLINE_DATE_PATTERN.finditer(clean_headline_text(title)):
        month_name, day_text, year_text = match.groups()
        month = MONTH_NAME_TO_NUMBER.get(month_name.lower().rstrip("."))
        if month is None:
            continue
        year = int(year_text) if year_text else reference.year
        try:
            candidate = datetime(year, month, int(day_text), tzinfo=LOCAL_TIMEZONE)
        except ValueError:
            continue
        if not year_text and candidate > reference + timedelta(days=180):
            candidate = candidate.replace(year=year - 1)
        found_dates.append(candidate)
    return found_dates


def is_low_signal_event_notice(title: str) -> bool:
    text = clean_headline_text(title).lower()
    if not text:
        return False
    if any(hint in text for hint in LOW_SIGNAL_EVENT_NOTICE_HINTS):
        if any(keyword in text for keyword in {"results", "reports", "beats", "misses", "approval", "awarded", "secures"}):
            return False
        return True
    return False


def headline_signal_note(title: str) -> str:
    if is_low_signal_event_notice(title):
        return "schedule or appearance notice, not the hard catalyst itself"
    if is_commentary_headline(title):
        return "commentary-style headline, not a clean company event"
    if is_concrete_event_headline(title):
        return "hard company event or transaction headline"
    lowered = clean_headline_text(title).lower()
    if any(hint in lowered for hint in MACRO_NEWS_HINTS):
        return "sector or macro readthrough headline"
    return "mixed-signal headline"


def build_news_prompt_line(item: dict[str, str], index: int, reference: datetime | None = None) -> str:
    if reference is None:
        reference = local_now()
    published_at = parse_news_timestamp(item.get("published_at") or item.get("time"), reference=reference)
    notes: list[str] = []
    if published_at is not None:
        notes.append(f"published {published_at.strftime('%b %d %I:%M %p')} ({describe_relative_days(published_at, reference)})")

    explicit_dates = extract_headline_dates(item.get("title", ""), reference=reference)
    if explicit_dates:
        explicit_date = explicit_dates[0]
        notes.append(f"title mentions {explicit_date.strftime('%b %d, %Y')} ({describe_relative_days(explicit_date, reference)})")

    signal_note = headline_signal_note(item.get("title", ""))
    if signal_note:
        notes.append(signal_note)

    note_suffix = f" | {'; '.join(notes)}" if notes else ""
    return f"{index}. [{safe_text(item.get('time'))}] {safe_text(item.get('source'))}: {safe_text(item.get('title'))}{note_suffix} ({safe_text(item.get('url'))})"


def sanitize_note_filename(name: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*]+', " ", safe_text(name))
    cleaned = re.sub(r"\s+", " ", cleaned).strip().rstrip(".")
    return cleaned or "Watchlist Snapshot"


def obsidian_sector_folder_name(board: dict[str, Any]) -> str:
    title = sanitize_note_filename(board.get("title", board.get("id", "Watchlist Snapshot")))
    if board.get("kind") == "premarket":
        return "Premarket Gappers"
    return title


def markdown_escape(value: Any) -> str:
    text = safe_text(value).replace("\r", " ").replace("\n", " ")
    return text.replace("|", r"\|").strip()


def obsidian_vault_is_ready() -> bool:
    return OBSIDIAN_VAULT_PATH.exists() and (OBSIDIAN_VAULT_PATH / ".obsidian").exists()


def should_export_to_obsidian(board: dict[str, Any], origin: str) -> bool:
    return (
        OBSIDIAN_EXPORT_ENABLED
        and board.get("kind", "").lower() in OBSIDIAN_EXPORT_BOARD_KINDS
        and origin.lower() in OBSIDIAN_EXPORT_TRIGGER_ORIGINS
    )


def render_obsidian_snapshot_note(
    board: dict[str, Any],
    snapshot: dict[str, Any],
    exported_at: datetime,
) -> str:
    rows = snapshot.get("rows", [])
    summary = snapshot.get("summary", {})
    market_brief = snapshot.get("market_brief", {})
    scan_completed_at = to_local_time(
        datetime.fromisoformat(snapshot["scan_completed_at"]) if snapshot.get("scan_completed_at") else None
    )
    note_title = f"{board['title']} - {exported_at.strftime('%Y-%m-%d')}"
    lines = [
        "---",
        f'title: "{note_title.replace(chr(34), "")}"',
        f'date: "{exported_at.strftime("%Y-%m-%d")}"',
        f'exported_at: "{exported_at.isoformat()}"',
        f'board_id: "{board["id"]}"',
        f'board_kind: "{board["kind"]}"',
        f'provider_order: "{", ".join(summary.get("provider_order", []))}"',
        f"symbol_count: {summary.get('count', 0)}",
        f"up_count: {summary.get('up_count', 0)}",
        f"down_count: {summary.get('down_count', 0)}",
        "---",
        "",
        f"# {note_title}",
        "",
        f"- Board: [{board['title']}]({board.get('url')})" if board.get("url") else f"- Board: {board['title']}",
        f"- Scan completed: {scan_completed_at.strftime('%Y-%m-%d %I:%M:%S %p %Z') if scan_completed_at else 'Unknown'}",
        f"- Exported: {exported_at.strftime('%Y-%m-%d %I:%M:%S %p %Z')}",
        f"- Symbols analyzed: {summary.get('count', 0)}",
        f"- AI sources: Gemini {summary.get('gemini_rows', 0)}, OpenRouter {summary.get('openrouter_rows', 0)}, OpenAI {summary.get('openai_rows', 0)}, Anthropic {summary.get('anthropic_rows', 0)}, Fallback {summary.get('fallback_rows', 0)}",
        "",
    ]

    if board.get("kind") == "market-brief":
        lines.extend(
            [
                "## Macro Summary",
                "",
                safe_text((market_brief.get("macro_summary") or {}).get("summary")),
                "",
            ]
        )
        for paragraph in [part.strip() for part in re.split(r"\n\s*\n", safe_text((market_brief.get("macro_summary") or {}).get("analysis_details"))) if part.strip()]:
            lines.extend([paragraph, ""])

        lines.extend(["## Macro Headlines", ""])
        for item in market_brief.get("macro_headlines", []):
            headline_bits = [safe_text(item.get("time")), safe_text(item.get("source"))]
            headline_meta = " - ".join(bit for bit in headline_bits if bit)
            bullet = f"- {headline_meta}: {safe_text(item.get('title'))}" if headline_meta else f"- {safe_text(item.get('title'))}"
            lines.append(bullet)
        lines.append("")

        premarket_summary = market_brief.get("premarket_summary") or {}
        lines.extend(["## Premarket Setup", "", safe_text(premarket_summary.get("summary")), ""])
        for paragraph in [part.strip() for part in re.split(r"\n\s*\n", safe_text(premarket_summary.get("analysis_details"))) if part.strip()]:
            lines.extend([paragraph, ""])
        lines.extend(["Top premarket movers:", ""])
        for mover in premarket_summary.get("top_movers", []):
            lines.append(
                f"- {safe_text(mover.get('ticker'))}: {format_percent(mover.get('move_percent'))} | {safe_text(mover.get('category'))} / {safe_text(mover.get('grade'))} | {safe_text(mover.get('reasoning'))}"
            )
        lines.append("")

        lines.extend(["## Sector Briefs", ""])
        for section in market_brief.get("sector_sections", []):
            lines.extend(
                [
                    f"### {safe_text(section.get('title'))}",
                    "",
                    f"- Importance: {safe_text(section.get('importance'))}",
                    f"- Symbols: {safe_text(section.get('symbol_count'))}",
                    f"- AI Source: {provider_display_name(safe_text(section.get('ai_source')) or 'fallback')}",
                    "",
                    safe_text(section.get("summary")),
                    "",
                ]
            )
            for paragraph in [part.strip() for part in re.split(r"\n\s*\n", safe_text(section.get("analysis_details"))) if part.strip()]:
                lines.extend([paragraph, ""])
            if section.get("top_movers"):
                lines.extend(["Top movers:", ""])
                for mover in section.get("top_movers", []):
                    lines.append(
                        f"- {safe_text(mover.get('ticker'))}: {format_percent(mover.get('move_percent'))} | {safe_text(mover.get('category'))} / {safe_text(mover.get('grade'))} | {safe_text(mover.get('reasoning'))}"
                    )
                lines.append("")
            if section.get("headlines"):
                lines.extend(["Sector headlines:", ""])
                for item in section.get("headlines", []):
                    headline_bits = [safe_text(item.get("time")), safe_text(item.get("source"))]
                    headline_meta = " - ".join(bit for bit in headline_bits if bit)
                    bullet = f"- {headline_meta}: {safe_text(item.get('title'))}" if headline_meta else f"- {safe_text(item.get('title'))}"
                    lines.append(bullet)
                lines.append("")
        return "\n".join(lines).strip() + "\n"

    lines.extend(
        [
            "## Board Summary",
            "",
            "| Ticker | Move % | Volume | Float | Short % | Category | Grade | AI | Reasoning |",
            "| --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |",
        ]
    )

    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    markdown_escape(row.get("ticker")),
                    markdown_escape(format_percent(row_move_percent(row))),
                    markdown_escape(compact_number(row_move_volume(row))),
                    markdown_escape(row.get("float_display", "-")),
                    markdown_escape(row.get("short_display", "-")),
                    markdown_escape(row.get("category", "")),
                    markdown_escape(row.get("grade", "")),
                    markdown_escape(provider_display_name(safe_text(row.get("ai_source")) or "fallback")),
                    markdown_escape(row.get("reasoning", "")),
                ]
            )
            + " |"
        )

    lines.extend(["", "## Detailed Analysis", ""])
    for row in rows:
        primary_headline = safe_text(row.get("primary_headline_title"))
        supporting_headline = safe_text(row.get("supporting_headline_title"))
        lines.extend(
            [
                f"### {row.get('ticker', '-')}",
                "",
                f"- Move: {format_percent(row_move_percent(row))} on {compact_number(row_move_volume(row))} shares",
                f"- Category / Grade: {row.get('category', '-')} / {row.get('grade', '-')}",
                f"- AI Source: {provider_display_name(safe_text(row.get('ai_source')) or 'fallback')}",
                f"- Float / Short: {row.get('float_display', '-')} / {row.get('short_display', '-')}",
                f"- Chart: {row.get('chart_url', '-')}",
                "",
            ]
        )
        if primary_headline:
            lines.extend(
                [
                    f"Primary catalyst: {primary_headline}",
                    "",
                ]
            )
        if supporting_headline:
            lines.extend(
                [
                    f"Supporting headline: {supporting_headline}",
                    "",
                ]
            )
        for paragraph in [part.strip() for part in re.split(r"\n\s*\n", safe_text(row.get("analysis_details"))) if part.strip()]:
            lines.extend([paragraph, ""])
        if row.get("news_items"):
            lines.append("Latest Finviz headlines:")
            lines.append("")
            for item in row.get("news_items", [])[:5]:
                headline_bits = [safe_text(item.get("time")), safe_text(item.get("source"))]
                headline_meta = " - ".join(bit for bit in headline_bits if bit)
                headline_text = safe_text(item.get("title"))
                headline_url = safe_text(item.get("url"))
                bullet = f"- {headline_meta}: {headline_text}" if headline_meta else f"- {headline_text}"
                if headline_url:
                    bullet += f" ([link]({headline_url}))"
                lines.append(bullet)
            lines.append("")

    return "\n".join(lines).strip() + "\n"


def export_snapshot_to_obsidian(board: dict[str, Any], snapshot: dict[str, Any]) -> Path:
    if not obsidian_vault_is_ready():
        raise RuntimeError(f"Obsidian vault not found at {OBSIDIAN_VAULT_PATH}")

    exported_at = local_now()
    export_dir = OBSIDIAN_VAULT_PATH / Path(OBSIDIAN_EXPORT_FOLDER) / obsidian_sector_folder_name(board)
    export_dir.mkdir(parents=True, exist_ok=True)
    note_path = export_dir / f"{exported_at.strftime('%Y-%m-%d')}.md"
    note_path.write_text(render_obsidian_snapshot_note(board, snapshot, exported_at), encoding="utf-8")
    return note_path


def financing_flavor_from_headline(title: str) -> str:
    text = clean_headline_text(title).lower()
    if not text:
        return ""
    if any(keyword in text for keyword in DILUTIVE_FINANCING_HINTS):
        return "dilutive"
    if any(keyword in text for keyword in POSITIVE_FINANCING_HINTS):
        return "strategic"
    if re.search(r"\b(secures?|secured|closes?)\b.*(?:\$[0-9]|[0-9][0-9.,]*\s*(?:million|billion))", text):
        return "strategic"
    return ""


def is_commentary_headline(title: str) -> bool:
    text = clean_headline_text(title).lower()
    if not text:
        return False
    if any(keyword in text for keyword in COMMENTARY_HEADLINE_HINTS):
        return True
    if "stock rises" in text and "after " not in text and " on " not in text:
        return True
    if "stock jumps" in text and "after " not in text and " on " not in text:
        return True
    return False


def is_concrete_event_headline(title: str) -> bool:
    text = clean_headline_text(title).lower()
    if not text:
        return False
    if any(keyword in text for keyword in CONCRETE_EVENT_HINTS):
        return True
    if re.search(r"\b(after|following|on)\b.*\b(approval|deal|earnings|factory|financing|loan|results|trial)\b", text):
        return True
    return False


def company_name_tokens(company_name: str) -> list[str]:
    return [
        token.lower()
        for token in re.split(r"[^A-Za-z0-9]+", company_name or "")
        if len(token) >= 4 and token.lower() not in {"holdings", "group", "inc", "corp", "plc", "company", "therapeutics", "biosciences"}
    ]


def headline_category_guess(title: str) -> str:
    text = clean_headline_text(title).lower()
    if is_low_signal_event_notice(title):
        return "No Fresh News"
    financing_flavor = financing_flavor_from_headline(text)
    if financing_flavor:
        return "Financing / Offering"
    for category, keywords in CATEGORY_KEYWORDS:
        if any(keyword.lower() in text for keyword in keywords):
            return category
    return "Low Float Momentum"


def score_news_item(ticker: str, company_name: str, item: dict[str, str], index: int) -> float:
    title = clean_headline_text(item.get("title", ""))
    source = safe_text(item.get("source", "")).lower()
    lowered = title.lower()
    score = max(0, 10 - index)

    if not title:
        return -999

    if ticker.lower() in lowered:
        score += 8

    company_tokens = company_name_tokens(company_name)
    if any(token in lowered for token in company_tokens):
        score += 10

    if any(hint in lowered for hint in LEGAL_ALERT_HINTS):
        score -= 25

    category = headline_category_guess(title)
    if category in {"Earnings", "FDA / Clinical", "PR / Contract", "M&A / Rumor"}:
        score += 8
    elif category in {"Financing / Offering", "Analyst / Upgrade"}:
        score += 5
    elif category == "Sympathy / Sector":
        score -= 10

    if is_concrete_event_headline(title):
        score += 7
    if is_commentary_headline(title):
        score -= 6
    if is_low_signal_event_notice(title):
        score -= 12

    financing_flavor = financing_flavor_from_headline(title)
    if category == "Financing / Offering" and financing_flavor == "strategic":
        score += 3

    if any(hint in lowered for hint in MACRO_NEWS_HINTS):
        score -= 12

    if source in HIGH_TRUST_NEWS_SOURCES:
        score += 4
    elif source in {"business wire", "globenewswire", "pr newswire"}:
        score += 2
    elif source in LOW_SIGNAL_NEWS_SOURCES:
        score -= 8

    if source in LOW_SIGNAL_NEWS_SOURCES and not is_concrete_event_headline(title):
        score -= 6

    if len(re.findall(r"\([A-Z]{1,5}\)", title)) >= 2:
        score -= 6

    published_at = parse_news_timestamp(item.get("published_at") or item.get("time"))
    if published_at is not None:
        days_old = max(0, (local_now().date() - published_at.date()).days)
        if days_old <= 1:
            score += 4
        elif days_old <= 3:
            score += 2
        elif days_old >= 14:
            score -= 8
        elif days_old >= 7:
            score -= 4

    return score


def select_primary_news(ticker: str, company_name: str, news_items: list[dict[str, str]]) -> dict[str, str] | None:
    if not news_items:
        return None
    ranked = sorted(
        enumerate(news_items),
        key=lambda pair: score_news_item(ticker, company_name, pair[1], pair[0]),
        reverse=True,
    )
    return ranked[0][1] if ranked else None


def select_supporting_news(ticker: str, company_name: str, news_items: list[dict[str, str]]) -> dict[str, str] | None:
    candidates: list[tuple[float, dict[str, str]]] = []
    for index, item in enumerate(news_items):
        title = clean_headline_text(item.get("title", ""))
        if infer_headline_relevance(ticker, company_name, [item]) != "direct":
            continue
        score = score_news_item(ticker, company_name, item, index)
        if is_concrete_event_headline(title):
            score += 8
        if is_commentary_headline(title):
            score -= 6
        if "stock jumps" in title.lower() or "stock rises" in title.lower():
            score -= 3
        candidates.append((score, item))

    if not candidates:
        return None
    return max(candidates, key=lambda pair: pair[0])[1]


def select_event_context_news(news_items: list[dict[str, str]], limit: int = 3) -> list[dict[str, str]]:
    context_candidates: list[tuple[float, dict[str, str]]] = []
    for index, item in enumerate(news_items):
        title = clean_headline_text(item.get("title", ""))
        if not is_low_signal_event_notice(title):
            continue
        score = score_news_item("", "", item, index) + 10
        published_at = parse_news_timestamp(item.get("published_at") or item.get("time"))
        if published_at is not None:
            days_old = max(0, (local_now().date() - published_at.date()).days)
            if days_old <= 3:
                score += 4
        context_candidates.append((score, item))

    context_candidates.sort(key=lambda pair: pair[0], reverse=True)
    return [item for _, item in context_candidates[:limit]]


def infer_headline_relevance(ticker: str, company_name: str, news_items: list[dict[str, str]]) -> str:
    if not news_items:
        return "none"

    headline = safe_text(news_items[0].get("title")).lower()
    if not headline:
        return "none"

    if ticker.lower() in headline:
        return "direct"

    tokens = company_name_tokens(company_name)
    if any(token in headline for token in tokens):
        return "direct"

    macro_words = {
        "nasdaq", "dow", "sp500", "s&p", "fed", "market", "stocks", "sector",
        "deal", "deals", "war", "trump", "oil", "treasury", "yield", "futures",
    }
    if any(word in headline for word in macro_words):
        return "macro"
    return "indirect"


def bucket_gap(value: float) -> str:
    if value >= 100:
        return "100+"
    if value >= 50:
        return "50-99"
    if value >= 20:
        return "20-49"
    if value >= 10:
        return "10-19"
    return "<10"


def bucket_volume(value: int) -> str:
    if value >= 10_000_000:
        return "10m+"
    if value >= 3_000_000:
        return "3m-10m"
    if value >= 1_000_000:
        return "1m-3m"
    if value >= 300_000:
        return "300k-1m"
    return "<300k"


def bucket_float(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value <= 20_000_000:
        return "<20m"
    if value <= 80_000_000:
        return "20m-80m"
    if value <= 200_000_000:
        return "80m-200m"
    return "200m+"


def bucket_short(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value >= 25:
        return "25+"
    if value >= 12:
        return "12-25"
    if value >= 6:
        return "6-12"
    return "<6"


def row_move_percent(row: dict[str, Any]) -> float:
    primary = finite_float(row.get("move_percent"), default=float("nan"))
    if math.isfinite(primary):
        return primary
    return finite_float(row.get("premarket_percent"), default=0.0)


def row_move_volume(row: dict[str, Any]) -> int:
    primary = finite_int(row.get("move_volume"), default=-1)
    if primary >= 0:
        return primary
    return finite_int(row.get("premarket_volume"), default=0)


def row_move_label(row: dict[str, Any]) -> str:
    return safe_text(row.get("move_label")) or "Premarket move"


def row_move_sentence(row: dict[str, Any]) -> str:
    move_pct = row_move_percent(row)
    move_vol = row_move_volume(row)
    label = row_move_label(row).lower()
    return f"{row['ticker']} is {format_percent(move_pct)} on the {label} on {compact_number(move_vol)} shares"


def next_ai_cache_key(row: dict[str, Any], bundle: dict[str, Any]) -> str:
    return json.dumps(
        {
            "prompt_version": AI_PROMPT_VERSION,
            "ticker": row["ticker"],
            "headline_titles": [safe_text(item.get("title")) for item in bundle.get("news_items", [])[:5]],
            "headline_sources": [safe_text(item.get("source")) for item in bundle.get("news_items", [])[:5]],
            "analysis_mode": safe_text(row.get("analysis_mode")) or "premarket",
            "move_label": row_move_label(row),
            "gap_bucket": bucket_gap(row_move_percent(row)),
            "volume_bucket": bucket_volume(row_move_volume(row)),
            "float_bucket": bucket_float(row.get("float_shares")),
            "short_bucket": bucket_short(row.get("short_percent")),
            "sector": safe_text(bundle.get("sector")),
            "industry": safe_text(bundle.get("industry")),
        },
        sort_keys=True,
    )


def set_provider_cooldown(provider: str, seconds: int) -> None:
    with AI_STATE_LOCK:
        current = AI_PROVIDER_COOLDOWNS.get(provider, 0.0)
        AI_PROVIDER_COOLDOWNS[provider] = max(current, time.time() + max(60, seconds))


def provider_in_cooldown(provider: str) -> bool:
    with AI_STATE_LOCK:
        return time.time() < AI_PROVIDER_COOLDOWNS.get(provider, 0.0)


def heuristic_grade(premarket_pct: float, volume: int, short_pct: float | None, category: str) -> str:
    score = 0
    if premarket_pct >= 40:
        score += 3
    elif premarket_pct >= 20:
        score += 2
    elif premarket_pct >= 10:
        score += 1

    if volume >= 5_000_000:
        score += 2
    elif volume >= 1_000_000:
        score += 1

    if short_pct is not None:
        if short_pct >= 15:
            score += 2
        elif short_pct >= 8:
            score += 1

    if category in {"Earnings", "FDA / Clinical", "PR / Contract", "M&A / Rumor"}:
        score += 1
    if category == "Financing / Offering":
        score -= 2
    if category == "No Fresh News":
        score -= 2

    if score >= 6:
        return "A"
    if score >= 4:
        return "B"
    if score >= 2:
        return "C"
    return "D"


def describe_volume(volume: int) -> str:
    if volume >= 10_000_000:
        return "already trading with exceptional liquidity"
    if volume >= 3_000_000:
        return "already trading with strong liquidity"
    if volume >= 1_000_000:
        return "trading with usable liquidity"
    if volume >= 300_000:
        return "trading but still a bit thin"
    return "still very thin"


def describe_float(float_shares: float | None) -> str:
    if float_shares is None:
        return "float is unclear"
    if float_shares <= 20_000_000:
        return "float is tight enough to accelerate quickly"
    if float_shares <= 80_000_000:
        return "float is workable for a momentum session"
    if float_shares <= 200_000_000:
        return "float is not especially tight, so the move needs real sponsorship"
    return "float is large enough that the move usually needs broad participation"


def describe_short_interest(short_pct: float | None) -> str:
    if short_pct is None:
        return "short positioning is unclear"
    if short_pct >= 25:
        return "short interest is very elevated, so squeeze pressure can matter"
    if short_pct >= 12:
        return "short interest is elevated enough to help extension"
    if short_pct >= 6:
        return "short interest adds some tactical fuel"
    return "there is not much squeeze fuel in the positioning"


def build_peer_fallback_reasoning(row: dict[str, Any], peer_context: dict[str, Any]) -> str:
    leader_ticker = safe_text(peer_context.get("leader_ticker")) or "the leader"
    leader_stub = shorten_text(peer_context.get("leader_headline", ""), limit=72)
    support_stub = shorten_text(peer_context.get("support_headline", ""), limit=72)
    has_support = bool(peer_context.get("has_company_specific_support") and support_stub)

    if has_support:
        return (
            f"{row['ticker']} is trading as a sector rerate with {leader_ticker} after {leader_stub}, while "
            f"{support_stub.lower()} gives it real company-specific backing; it still needs broad participation after the open to hold."
        )

    return (
        f"{row['ticker']} looks more like sympathy with {leader_ticker} after {leader_stub}; "
        f"without a cleaner standalone trigger, this is second-order flow that can fade fast once the leader cools."
    )


def build_peer_fallback_analysis(row: dict[str, Any], category: str, grade: str, peer_context: dict[str, Any]) -> str:
    leader_ticker = safe_text(peer_context.get("leader_ticker")) or "the group leader"
    leader_headline = safe_text(peer_context.get("leader_headline")) or "the strongest peer headline in the group"
    support_headline = safe_text(peer_context.get("support_headline"))
    has_support = bool(peer_context.get("has_company_specific_support") and support_headline)
    premarket_pct = float(row.get("premarket_percent") or 0.0)
    volume = int(row.get("premarket_volume") or 0)

    paragraph_1 = (
        f"{row['ticker']} is up {format_percent(premarket_pct)} premarket on {compact_number(volume)} shares, "
        f"with float around {row['float_display']} and short interest around {row['short_display']}. "
        f"The better read here is {category.lower()}: {leader_ticker}'s headline, \"{leader_headline}\", is helping rerate the group."
    )

    if has_support:
        paragraph_2 = (
            f"This is not pure no-news sympathy, though. {row['ticker']}'s own best company-specific headline is "
            f"\"{support_headline}\", which gives traders a real company angle even if it is not as catalytic as the leader's news."
        )
    else:
        paragraph_2 = (
            f"There is not a cleaner standalone company-specific trigger than the group rerating itself, so traders are mostly leaning on "
            f"{leader_ticker}'s news to justify the move."
        )

    paragraph_3 = (
        f"The tape still matters. {describe_volume(volume).capitalize()}, {describe_float(row.get('float_shares'))}, and "
        f"{describe_short_interest(row.get('short_percent'))}. If {leader_ticker} stays bid and the group keeps getting rerated, "
        f"{row['ticker']} can trend with it; if the leader fades, this kind of second-order move usually weakens first."
    )

    paragraph_4 = (
        f"That is why the setup sits at {grade} instead of a full standalone upgrade. The stock has enough context to matter, "
        "but it still needs sector follow-through, not just a one-print gap."
    )
    return "\n\n".join([paragraph_1, paragraph_2, paragraph_3, paragraph_4])


def build_fallback_reasoning(row: dict[str, Any], bundle: dict[str, Any], category: str, grade: str) -> str:
    peer_context = bundle.get("peer_context") or row.get("peer_context")
    if peer_context:
        return build_peer_fallback_reasoning(row, peer_context)

    news_items = bundle.get("news_items", [])
    top_headline = safe_text(bundle.get("primary_headline_title")) or (
        safe_text(news_items[0].get("title")) if news_items else "No fresh company-specific headline"
    )
    relevance = infer_headline_relevance(
        row["ticker"],
        row.get("company_name", ""),
        [bundle.get("primary_news")] if bundle.get("primary_news") else news_items,
    )
    premarket_pct = float(row.get("premarket_percent") or 0.0)
    volume = int(row.get("premarket_volume") or 0)
    float_shares = row.get("float_shares")
    short_pct = row.get("short_percent")

    catalyst_read = {
        "Earnings": "earnings can sustain if the numbers truly reset expectations",
        "FDA / Clinical": "clinical-style headlines can hold when the data changes the company narrative",
        "PR / Contract": "contract-style headlines work only if the economics look material",
        "Financing / Offering": "financing usually caps upside unless the raise clearly removes near-term risk",
        "Analyst / Upgrade": "analyst-driven moves need real follow-through from buyers, not just the headline",
        "M&A / Rumor": "M&A-style setups can trend hard if the market believes the headline is actionable",
        "Sympathy / Sector": "sympathy moves can extend early but often fade without a company-specific trigger",
        "Low Float Momentum": "this looks more like tape-driven momentum than a clean fundamental re-rate",
        "No Fresh News": "without a clean fresh catalyst, the move is mostly a liquidity and positioning story",
    }.get(category, "the setup needs a real catalyst to hold")

    tape_read = describe_volume(volume)
    squeeze_read = describe_short_interest(short_pct)
    float_read = describe_float(float_shares)

    caveat_parts: list[str] = []
    if relevance == "macro":
        caveat_parts.append("the top headline reads macro rather than company-specific")
    elif relevance == "indirect":
        caveat_parts.append("the news link is not fully clean or direct")
    elif relevance == "none":
        caveat_parts.append("there is no fresh company-specific news in the feed")

    if premarket_pct >= 80 and category in {"Low Float Momentum", "No Fresh News", "Sympathy / Sector"}:
        caveat_parts.append("the gap is so extreme that chase risk is high if buyers stall after the open")
    elif premarket_pct <= 10 and category not in {"Earnings", "FDA / Clinical", "M&A / Rumor"}:
        caveat_parts.append("the gap is modest enough that it may struggle to attract sustained momentum")

    if volume < 500_000:
        caveat_parts.append("premarket liquidity is still thin")
    if category == "Financing / Offering":
        caveat_parts.append("dilution overhang can cap follow-through")

    caveat = caveat_parts[0] if caveat_parts else "buyers still need to prove the move is more than a first-hour squeeze"

    headline_stub = top_headline[:88].rstrip(". ")
    if relevance == "direct":
        opening = f"{headline_stub} is the real trigger here"
    elif relevance == "macro":
        opening = f"{headline_stub} is being traded as the lead story"
    elif relevance == "indirect":
        opening = f"{headline_stub} is the headline traders are leaning on"
    else:
        opening = "There is no clean company-specific headline in the feed"

    return f"{opening}; {catalyst_read}, with {tape_read}, {float_read}, and {squeeze_read}, but {caveat}."


def build_fallback_analysis(row: dict[str, Any], bundle: dict[str, Any], category: str, grade: str) -> str:
    peer_context = bundle.get("peer_context") or row.get("peer_context")
    if peer_context:
        return build_peer_fallback_analysis(row, category, grade, peer_context)

    news_items = bundle.get("news_items", [])
    headline = safe_text(bundle.get("primary_headline_title")) or (
        safe_text(news_items[0].get("title")) if news_items else "No strong multi-source headline was returned."
    )
    relevance = infer_headline_relevance(
        row["ticker"],
        row.get("company_name", ""),
        [bundle.get("primary_news")] if bundle.get("primary_news") else news_items,
    )
    premarket_pct = float(row.get("premarket_percent") or 0.0)
    volume = int(row.get("premarket_volume") or 0)

    if relevance == "direct":
        headline_quality = "The lead headline looks company-specific, so the move has at least a plausible catalyst behind it."
    elif relevance == "macro":
        headline_quality = "The lead headline reads more macro or sector-wide than company-specific, which makes the catalyst quality weaker than the tape may suggest."
    elif relevance == "indirect":
        headline_quality = "The lead headline is only loosely tied to the company, so traders should assume the tape is doing more work than the news."
    else:
        headline_quality = "There is no clean fresh company-specific headline in the feed, so this should be treated primarily as a momentum or positioning move."

    paragraph_1 = (
        f"{row_move_sentence(row)}, "
        f"with float around {row['float_display']} and short interest around {row['short_display']}. "
        f"The current read tags it as {category.lower()} and grades it {grade}. {headline_quality}"
    )

    paragraph_2 = (
        f"The tape itself is the second part of the story. {describe_volume(volume).capitalize()}, "
        f"{describe_float(row.get('float_shares'))}, and {describe_short_interest(row.get('short_percent'))}. "
        "That combination determines whether the move can keep expanding after the open or whether it likely peaks on the first burst of attention."
    )

    paragraph_3 = (
        f"The main caveat is the quality of the catalyst read: \"{headline}\". "
        "If traders start realizing the headline is stale, indirect, or financially dilutive, the setup can fade fast even after a strong gap. "
        "If the market keeps treating it as a genuine repricing event, then dips can stay shallow and volume can keep recycling higher."
    )

    return "\n\n".join([paragraph_1, paragraph_2, paragraph_3])


def fallback_ai_payload(row: dict[str, Any], bundle: dict[str, Any], error_text: str = "") -> dict[str, str]:
    news_items = bundle.get("news_items", [])
    category = safe_text(bundle.get("primary_category")) or infer_category_from_news(news_items)
    premarket_pct = row_move_percent(row)
    volume = row_move_volume(row)
    short_pct = parse_metric_number(bundle.get("short_float_raw"))
    grade = heuristic_grade(premarket_pct, volume, short_pct, category)

    return {
        "category": category,
        "grade": grade,
        "reasoning": build_fallback_reasoning(row, bundle, category, grade),
        "analysis_details": build_fallback_analysis(row, bundle, category, grade),
        "ai_source": "fallback",
        "ai_error": error_text,
    }


def get_gemini_client() -> genai.Client:
    global GEMINI_CLIENT
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY environment variable.")

    with GEMINI_CLIENT_LOCK:
        if GEMINI_CLIENT is None:
            GEMINI_CLIENT = genai.Client(api_key=api_key)
        return GEMINI_CLIENT


def get_openrouter_api_key() -> str:
    for env_name in ("OPENROUTER_API_KEY", "OPENROUTER_KEY", "OR_API_KEY"):
        api_key = os.getenv(env_name, "").strip()
        if api_key:
            return api_key
    return ""


def available_ai_providers() -> list[str]:
    providers: list[str] = []
    if os.getenv("GEMINI_API_KEY", "").strip():
        providers.append("gemini")
    if get_openrouter_api_key():
        providers.append("openrouter")
    if os.getenv("OPENAI_API_KEY", "").strip():
        providers.append("openai")
    if os.getenv("ANTHROPIC_API_KEY", "").strip():
        providers.append("anthropic")
    return providers


def extract_retry_delay_seconds(error_text: str, default_seconds: int = 1800) -> int:
    match = re.search(r"retry in\s+([0-9]+(?:\.[0-9]+)?)s", error_text, flags=re.IGNORECASE)
    if match:
        return max(60, int(float(match.group(1))) + 5)
    return default_seconds


def call_gemini_provider(prompt: str) -> dict[str, Any]:
    client = get_gemini_client()
    config = {
        "temperature": 0.2,
        "response_mime_type": "application/json",
        "max_output_tokens": 900,
    }
    last_error: Exception | None = None
    for model_name in GEMINI_MODEL_CANDIDATES:
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=config,
            )
            return extract_json_object(response.text)
        except Exception as exc:
            last_error = exc
            continue
    raise last_error or RuntimeError("Gemini request failed.")


def call_openai_provider(prompt: str) -> dict[str, Any]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing OPENAI_API_KEY environment variable.")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    messages = [
        {"role": "system", "content": AI_SYSTEM_MESSAGE},
        {"role": "user", "content": prompt},
    ]
    last_error: Exception | None = None

    for model_name in OPENAI_MODEL_CANDIDATES:
        try:
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json={
                    "model": model_name,
                    "messages": messages,
                    "temperature": 0.2,
                    "response_format": {"type": "json_object"},
                },
                timeout=45,
            )
            if response.status_code >= 400:
                raise RuntimeError(f"OpenAI {response.status_code}: {response.text}")
            payload = response.json()
            content = payload["choices"][0]["message"]["content"]
            return extract_json_object(content)
        except Exception as exc:
            last_error = exc
            continue

    raise last_error or RuntimeError("OpenAI request failed.")


def call_openrouter_provider(prompt: str) -> dict[str, Any]:
    api_key = get_openrouter_api_key()
    if not api_key:
        raise RuntimeError("Missing OPENROUTER_API_KEY environment variable.")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": os.getenv("OPENROUTER_HTTP_REFERER", "http://127.0.0.1:5000"),
        "X-Title": os.getenv("OPENROUTER_APP_NAME", APP_TITLE),
    }
    messages = [
        {"role": "system", "content": AI_SYSTEM_MESSAGE},
        {"role": "user", "content": prompt},
    ]
    last_error: Exception | None = None

    for model_name in OPENROUTER_MODEL_CANDIDATES:
        request_body = {
            "model": model_name,
            "messages": messages,
            "temperature": 0.2,
            "response_format": {"type": "json_object"},
        }
        try:
            with OPENROUTER_REQUEST_SEMAPHORE:
                response = requests.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers=headers,
                    json=request_body,
                    timeout=OPENROUTER_TIMEOUT_SECONDS,
                )
            if response.status_code >= 400:
                raise RuntimeError(f"OpenRouter {response.status_code}: {response.text}")
            payload = response.json()
            content = payload["choices"][0]["message"]["content"]
            return extract_json_object(content)
        except Exception as exc:
            last_error = exc
            if model_name != "openrouter/free":
                continue
            try:
                # Some free endpoints may not support JSON mode on every routed model.
                with OPENROUTER_REQUEST_SEMAPHORE:
                    response = requests.post(
                        "https://openrouter.ai/api/v1/chat/completions",
                        headers=headers,
                        json={
                            "model": model_name,
                            "messages": messages,
                            "temperature": 0.2,
                        },
                        timeout=OPENROUTER_TIMEOUT_SECONDS,
                    )
                if response.status_code >= 400:
                    raise RuntimeError(f"OpenRouter {response.status_code}: {response.text}")
                payload = response.json()
                content = payload["choices"][0]["message"]["content"]
                return extract_json_object(content)
            except Exception as retry_exc:
                last_error = retry_exc
                continue

    raise last_error or RuntimeError("OpenRouter request failed.")


def call_anthropic_provider(prompt: str) -> dict[str, Any]:
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing ANTHROPIC_API_KEY environment variable.")

    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    last_error: Exception | None = None

    for model_name in ANTHROPIC_MODEL_CANDIDATES:
        try:
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json={
                    "model": model_name,
                    "max_tokens": 900,
                    "temperature": 0.2,
                    "system": AI_SYSTEM_MESSAGE,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=45,
            )
            if response.status_code >= 400:
                raise RuntimeError(f"Anthropic {response.status_code}: {response.text}")
            payload = response.json()
            content_items = payload.get("content", [])
            text = "\n".join(item.get("text", "") for item in content_items if item.get("type") == "text")
            return extract_json_object(text)
        except Exception as exc:
            last_error = exc
            continue

    raise last_error or RuntimeError("Anthropic request failed.")


def build_gemini_prompt(row: dict[str, Any], bundle: dict[str, Any]) -> str:
    news_items = bundle.get("news_items", [])
    primary_title = safe_text(bundle.get("primary_headline_title"))
    primary_source = safe_text(bundle.get("primary_headline_source"))
    primary_category = safe_text(bundle.get("primary_category"))
    primary_relevance = safe_text(bundle.get("primary_relevance"))
    event_context_titles = [clean_headline_text(title) for title in bundle.get("event_context_titles", []) if clean_headline_text(title)]
    analysis_mode = safe_text(row.get("analysis_mode")) or "premarket"
    current_local_time = local_now()
    current_local_stamp = current_local_time.strftime("%Y-%m-%d %I:%M %p %Z")
    current_local_date = current_local_time.strftime("%Y-%m-%d")
    primary_headline_dates = extract_headline_dates(primary_title, reference=current_local_time)
    primary_timing_note_parts: list[str] = []
    primary_published_at = parse_news_timestamp(
        bundle.get("primary_news", {}).get("published_at") or bundle.get("primary_headline_time"),
        reference=current_local_time,
    )
    if primary_published_at is not None:
        primary_timing_note_parts.append(
            f"published {primary_published_at.strftime('%b %d %I:%M %p')} ({describe_relative_days(primary_published_at, current_local_time)})"
        )
    if primary_headline_dates:
        event_date = primary_headline_dates[0]
        primary_timing_note_parts.append(
            f"title mentions {event_date.strftime('%b %d, %Y')} ({describe_relative_days(event_date, current_local_time)})"
        )
    primary_signal_note = headline_signal_note(primary_title)
    if primary_signal_note:
        primary_timing_note_parts.append(primary_signal_note)
    primary_timing_note = "; ".join(primary_timing_note_parts) or "-"
    event_context_block = "\n".join(
        f"- {title}"
        for title in event_context_titles[:3]
        if title and title != primary_title
    ) or "-"
    if news_items:
        news_block = "\n".join(
            build_news_prompt_line(item, idx, reference=current_local_time)
            for idx, item in enumerate(news_items, start=1)
        )
    else:
        news_block = "No recent merged headlines returned."

    if analysis_mode == "watchlist":
        return f"""
You are an elite US stock catalyst analyst for active day traders.
Your job is to explain what is moving this stock TODAY and whether that move looks company-specific, sector-driven, macro-driven, or mostly technical.

Return ONLY valid JSON with exactly these keys:
- category
- grade
- reasoning
- analysis_details

Hard rules:
1. category must be exactly one of: {", ".join(CATEGORY_OPTIONS)}
2. grade must be exactly one of: A, B, C, D
3. reasoning must be one sentence, plain English, high signal, max 34 words
4. reasoning must explicitly say what happened to the stock today and the key nuance behind the move
5. analysis_details must be 3-4 short paragraphs, no bullet points, no markdown headings
6. Use only the supplied data. If the move looks like sympathy, stale continuation, or generic sector flow, say so clearly.
7. Grade by catalyst quality, freshness, day-of tape quality, crowding, squeeze potential, dilution risk, and likelihood the move keeps following through today.
8. Favor real catalysts: earnings, trial/FDA, meaningful contracts, M&A, strategic financing, capex buildout, clean analyst upgrades.
9. Penalize weak setups: no fresh news, vague commentary headlines, financing overhang, macro-only moves, or second-order sympathy with no company-specific backup.
10. Use the exact current date below when judging timing. Do not call an upcoming event stale or more than a year away unless the supplied dates actually say that.
11. Conference-call notices, fireside chats, interviews, previews, and conference appearances are usually setup context, not the actual catalyst, unless paired with hard new results or a transaction.
12. If setup-context headlines are present, mention them as the next checkpoint or part of the setup, especially when traders are positioning into that event.

The reasoning sentence should explain:
- what happened to the stock today
- what catalyst or sector rerating is driving it
- the most important caveat keeping the move from a higher grade

The analysis_details paragraphs should cover:
- whether the lead headline is truly company-specific and day-one relevant
- whether the move is standalone or sympathy / sector rerating
- what the tape says about liquidity, float, short crowding, and continuation odds
- why this deserves its exact grade instead of the one above or below

Grading framework:
- A: Clear, fresh, high-quality catalyst with strong same-day follow-through potential
- B: Real catalyst and usable tape, but one meaningful caveat remains
- C: Mixed, sympathy-led, or second-tier catalyst where the tape matters more than the headline
- D: Weak, stale, financing-heavy, or unclear catalyst that likely fades

Current timing context:
- Current analysis timestamp: {current_local_stamp}
- Current analysis date: {current_local_date}

Stock snapshot:
- Ticker: {row["ticker"]}
- Company: {bundle.get("company") or row.get("company_name") or row["ticker"]}
- Day move percent: {format_percent(row.get("day_change_percent"))}
- Day volume: {compact_number(row.get("regular_volume"))}
- Premarket percent: {format_percent(row.get("raw_premarket_percent"))}
- Premarket volume: {compact_number(row.get("raw_premarket_volume"))}
- Active move lens: {row_move_label(row)} {format_percent(row_move_percent(row))}
- Active move volume: {compact_number(row_move_volume(row))}
- Last price: {row.get("price_display")}
- Float: {row.get("float_display")}
- Short percent: {row.get("short_display")}
- Sector: {bundle.get("sector") or "-"}
- Industry: {bundle.get("industry") or "-"}
- Market cap: {bundle.get("market_cap_raw") or "-"}
- Primary catalyst candidate: {primary_title or "-"}
- Primary candidate source: {primary_source or "-"}
- Primary candidate category guess: {primary_category or "-"}
- Primary candidate relevance guess: {primary_relevance or "-"}
- Primary candidate timing note: {primary_timing_note}
- Additional setup-context headlines:
{event_context_block}

Latest headlines:
{news_block}
""".strip()

    return f"""
You are an elite US premarket catalyst analyst for active day traders.
Your job is to judge whether this stock's premarket move is truly tradeable TODAY, not whether it is a good long-term investment.

Return ONLY valid JSON with exactly these keys:
- category
- grade
- reasoning
- analysis_details

Hard rules:
1. category must be exactly one of: {", ".join(CATEGORY_OPTIONS)}
2. grade must be exactly one of: A, B, C, D
3. reasoning must be one sentence, plain English, high signal, max 34 words
4. reasoning must sound like a real catalyst trader, not a generic AI summary
5. analysis_details must be 3-4 short paragraphs, no bullet points, no markdown headings
6. Use only the supplied data. If the news looks stale, weak, ambiguous, or like pure momentum, penalize the grade.
7. Grade by catalyst quality, freshness, tape quality, crowding, squeeze potential, dilution risk, and likelihood of intraday follow-through.
8. Favor real catalysts: earnings, trial/FDA, meaningful contracts, M&A, clean analyst upgrades.
9. Penalize weak setups: no fresh news, sympathy-only action, vague PR, financing/offering overhang, or purely technical squeezes with no clear trigger.
10. Use the exact current date below when judging timing. Do not call an upcoming event stale or more than a year away unless the supplied dates actually say that.
11. Conference-call notices, fireside chats, interviews, previews, and conference appearances are usually setup context, not the actual catalyst, unless paired with hard new results or a transaction.
12. If setup-context headlines are present, mention them as the next checkpoint or part of the setup, especially when traders are positioning into that event.

The reasoning sentence should explain:
- what actually matters about the setup today
- the best reason it could continue
- the most important nuance or caveat holding the grade back

The analysis_details paragraphs should cover:
- whether the headline is truly company-specific and catalyst-grade
- what the tape says about liquidity, float, short crowding, and squeeze odds
- what would make the move hold versus fail after the open
- why this deserves its exact grade instead of the one above or below

Grading framework:
- A: Clear, fresh, high-quality catalyst with strong intraday follow-through potential and clean trader interest
- B: Real catalyst and decent tape, but one meaningful caveat remains
- C: Mixed or second-tier catalyst, or the move may be mostly technical / crowded
- D: Weak, stale, low-quality, financing-heavy, or unclear catalyst that likely fades

Current timing context:
- Current analysis timestamp: {current_local_stamp}
- Current analysis date: {current_local_date}

Stock snapshot:
- Ticker: {row["ticker"]}
- Company: {bundle.get("company") or row.get("company_name") or row["ticker"]}
- Premarket percent: {format_percent(row.get("premarket_percent"))}
- Premarket volume: {compact_number(row.get("premarket_volume"))}
- Premarket price: {row.get("price_display")}
- Float: {row.get("float_display")}
- Short percent: {row.get("short_display")}
- Sector: {bundle.get("sector") or "-"}
- Industry: {bundle.get("industry") or "-"}
- Market cap: {bundle.get("market_cap_raw") or "-"}
- Primary catalyst candidate: {primary_title or "-"}
- Primary candidate source: {primary_source or "-"}
- Primary candidate category guess: {primary_category or "-"}
- Primary candidate relevance guess: {primary_relevance or "-"}
- Primary candidate timing note: {primary_timing_note}
- Additional setup-context headlines:
{event_context_block}

Latest headlines:
{news_block}
""".strip()


def extract_json_object(text: str) -> dict[str, Any]:
    candidate = safe_text(text)
    if not candidate:
        raise ValueError("Empty Gemini response.")

    if candidate.startswith("```"):
        candidate = re.sub(r"^```(?:json)?\s*", "", candidate)
        candidate = re.sub(r"\s*```$", "", candidate)

    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        decoder = json.JSONDecoder()
        start = candidate.find("{")
        while start != -1:
            try:
                obj, _ = decoder.raw_decode(candidate[start:])
                if isinstance(obj, dict):
                    return obj
            except json.JSONDecodeError:
                pass
            start = candidate.find("{", start + 1)
    raise ValueError("Gemini response did not contain valid JSON.")


def normalize_ai_payload(
    row: dict[str, Any],
    bundle: dict[str, Any],
    payload: dict[str, Any],
    provider: str = "gemini",
) -> dict[str, str]:
    category = safe_text(payload.get("category"))
    if category not in CATEGORY_OPTIONS:
        category = infer_category_from_news(bundle.get("news_items", []))

    grade = safe_text(payload.get("grade")).upper()
    if grade not in {"A", "B", "C", "D"}:
        grade = heuristic_grade(
            row_move_percent(row),
            row_move_volume(row),
            parse_metric_number(bundle.get("short_float_raw")),
            category,
        )

    reasoning = " ".join(safe_text(payload.get("reasoning")).split())
    if not reasoning:
        reasoning = fallback_ai_payload(row, bundle)["reasoning"]

    analysis_details = safe_text(payload.get("analysis_details"))
    if not analysis_details:
        analysis_details = fallback_ai_payload(row, bundle)["analysis_details"]

    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", analysis_details) if part.strip()]
    analysis_details = "\n\n".join(paragraphs[:4]) if paragraphs else fallback_ai_payload(row, bundle)["analysis_details"]

    return {
        "category": category,
        "grade": grade,
        "reasoning": reasoning[:240],
        "analysis_details": analysis_details,
        "ai_source": provider,
        "ai_error": "",
    }


def request_ai_json(prompt: str) -> tuple[dict[str, Any] | None, str | None, str]:
    last_error = "No AI provider key is configured."
    provider_handlers = {
        "gemini": call_gemini_provider,
        "openrouter": call_openrouter_provider,
        "openai": call_openai_provider,
        "anthropic": call_anthropic_provider,
    }

    enabled_providers = set(available_ai_providers())
    ordered_providers = [provider for provider in AI_PROVIDER_ORDER if provider in provider_handlers]
    for provider in enabled_providers:
        if provider not in ordered_providers:
            if provider == "openrouter" and "gemini" in ordered_providers:
                ordered_providers.insert(ordered_providers.index("gemini") + 1, provider)
            else:
                ordered_providers.append(provider)

    for provider in ordered_providers:
        if provider not in provider_handlers or provider not in enabled_providers:
            continue
        if provider_in_cooldown(provider):
            last_error = f"{provider_display_name(provider)} temporarily unavailable because quota or rate limit was recently hit."
            continue
        try:
            return provider_handlers[provider](prompt), provider, ""
        except Exception as exc:
            last_error = str(exc)
            err_str = str(exc)
            if (
                "429" in err_str
                or "RESOURCE_EXHAUSTED" in err_str
                or "Quota exceeded" in err_str
                or "insufficient_quota" in err_str
                or "credit balance is too low" in err_str
            ):
                set_provider_cooldown(provider, extract_retry_delay_seconds(err_str))
            continue

    return None, None, last_error


def analyze_with_ai(row: dict[str, Any], bundle: dict[str, Any]) -> dict[str, str]:
    cache_key = next_ai_cache_key(row, bundle)

    with ANALYSIS_CACHE_LOCK:
        cached = ANALYSIS_CACHE.get(cache_key)
        if cached and time.time() - cached["created_at"] < cached.get("ttl", ANALYSIS_CACHE_TTL_SECONDS):
            return cached["payload"]

    prompt = build_gemini_prompt(row, bundle)
    payload, provider, last_error = request_ai_json(prompt)
    normalized: dict[str, str] | None = None
    if payload is not None and provider is not None:
        normalized = normalize_ai_payload(row, bundle, payload, provider=provider)

    if normalized is None:
        normalized = fallback_ai_payload(row, bundle, error_text=last_error)

    with ANALYSIS_CACHE_LOCK:
        ttl = ANALYSIS_CACHE_TTL_SECONDS if normalized.get("ai_source") != "fallback" else 120
        ANALYSIS_CACHE[cache_key] = {"created_at": time.time(), "ttl": ttl, "payload": normalized}

    return normalized


def fetch_finviz_bundle(ticker: str) -> dict[str, Any]:
    with NEWS_CACHE_LOCK:
        cached = NEWS_CACHE.get(ticker)
        if cached and time.time() - cached["created_at"] < NEWS_CACHE_TTL_SECONDS:
            return cached["payload"]

    quote_client = finvizfinance(ticker)

    fundamentals: dict[str, Any] = {}
    company = ""
    sector = ""
    industry = ""
    market_cap_raw = ""
    short_float_raw = ""
    fallback_float_raw = ""

    try:
        fundamentals = quote_client.ticker_fundament(raw=True, output_format="dict") or {}
        company = safe_text(fundamentals.get("Company"))
        sector = safe_text(fundamentals.get("Sector"))
        industry = safe_text(fundamentals.get("Industry"))
        market_cap_raw = safe_text(fundamentals.get("Market Cap"))
        short_float_raw = safe_text(fundamentals.get("Short Float"))
        fallback_float_raw = safe_text(fundamentals.get("Shs Float") or fundamentals.get("Shs Outstand"))
    except Exception:
        fundamentals = {}

    try:
        news_frame = quote_client.ticker_news()
    except Exception:
        news_frame = pd.DataFrame()

    finviz_news_items = normalize_news_frame(news_frame)
    yfinance_news_items = fetch_yfinance_news_items(ticker)
    merged_news_items = merge_news_items(yfinance_news_items, finviz_news_items)

    payload = {
        "company": company,
        "sector": sector,
        "industry": industry,
        "market_cap_raw": market_cap_raw,
        "short_float_raw": short_float_raw,
        "fallback_float_raw": fallback_float_raw,
        "news_items": merged_news_items,
        "finviz_news_items": finviz_news_items,
        "yfinance_news_items": yfinance_news_items,
        "fundamentals": fundamentals,
    }

    with NEWS_CACHE_LOCK:
        NEWS_CACHE[ticker] = {"created_at": time.time(), "payload": payload}

    return payload


def scan_premarket_gappers() -> pd.DataFrame:
    query = (
        Query()
        .select(
            "name",
            "close",
            "premarket_change",
            "premarket_gap",
            "premarket_volume",
            "premarket_close",
            "float_shares_outstanding_current",
            "exchange",
            "type",
        )
        .where2(
            And(
                Column("premarket_change").not_empty(),
                Column("premarket_volume").not_empty(),
                Column("premarket_volume") >= MIN_PREMARKET_VOLUME,
                Column("premarket_close") >= MIN_PRICE,
                Column("premarket_change") >= MIN_PREMARKET_PCT,
                Column("market_cap_basic") > MIN_MARKET_CAP,
                Column("exchange").isin(EXCHANGES),
                Column("type") == "stock",
            )
        )
        .order_by("premarket_change", ascending=False)
        .limit(SCAN_LIMIT)
    )

    _, raw = query.get_scanner_data(**tradingview_request_kwargs())
    if raw.empty:
        return pd.DataFrame()

    frame = raw.copy()
    frame["ticker"] = frame["ticker"].astype(str).str.split(":").str[-1]
    frame["company_name"] = frame["name"].astype(str)
    for column in [
        "close",
        "premarket_change",
        "premarket_gap",
        "premarket_volume",
        "premarket_close",
        "float_shares_outstanding_current",
    ]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["premarket_volume"] = frame["premarket_volume"].fillna(0).astype("int64")
    frame["display_price"] = frame["premarket_close"].fillna(frame["close"])
    return frame


def scan_watchlist_symbols(symbols: list[str]) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()

    query = (
        Query()
        .select(
            "name",
            "close",
            "change",
            "volume",
            "market_cap_basic",
            "float_shares_outstanding_current",
            "premarket_change",
            "premarket_volume",
            "premarket_close",
            "exchange",
            "type",
        )
        .set_tickers(*symbols)
        .limit(len(symbols))
    )

    _, frame = query.get_scanner_data(**tradingview_request_kwargs())
    if frame.empty:
        return pd.DataFrame()

    output = frame.copy()
    output["company_name"] = output["name"].astype(str)
    for column in [
        "close",
        "change",
        "volume",
        "market_cap_basic",
        "float_shares_outstanding_current",
        "premarket_change",
        "premarket_volume",
        "premarket_close",
    ]:
        output[column] = pd.to_numeric(output[column], errors="coerce")

    output["volume"] = output["volume"].fillna(0).astype("int64")
    output["premarket_volume"] = output["premarket_volume"].fillna(0).astype("int64")
    output["display_price"] = output["premarket_close"].fillna(output["close"])
    return output


def make_multichart_url(symbols: list[str], timeframe: str = "D") -> str:
    cleaned_symbols = [clean_symbol(symbol) for symbol in symbols if clean_symbol(symbol)]
    return f"{IB_CHART_BASE_URL}?symbols={quote(','.join(cleaned_symbols), safe=',')}&tf={quote(timeframe)}"


def choose_watchlist_move(record: dict[str, Any]) -> dict[str, Any]:
    day_change = finite_float(record.get("change"), default=0.0)
    day_volume = finite_int(record.get("volume"), default=0)
    premarket_change = finite_float(record.get("premarket_change"), default=0.0)
    premarket_volume = finite_int(record.get("premarket_volume"), default=0)

    use_premarket = premarket_volume >= 10_000 and abs(premarket_change) >= max(1.0, abs(day_change) + 0.5)
    if use_premarket:
        return {"label": "Premarket move", "percent": premarket_change, "volume": premarket_volume, "session": "premarket"}
    return {"label": "Day move", "percent": day_change, "volume": day_volume, "session": "regular"}


def row_analysis_bundle(row: dict[str, Any]) -> dict[str, Any]:
    primary_news = None
    if safe_text(row.get("primary_headline_title")):
        primary_news = {
            "title": safe_text(row.get("primary_headline_title")),
            "source": safe_text(row.get("primary_headline_source")),
            "url": safe_text(row.get("primary_headline_url")),
            "time": safe_text(row.get("primary_headline_time")),
        }

    supporting_news = None
    if safe_text(row.get("supporting_headline_title")):
        supporting_news = {
            "title": safe_text(row.get("supporting_headline_title")),
            "source": safe_text(row.get("supporting_headline_source")),
            "url": safe_text(row.get("supporting_headline_url")),
            "time": safe_text(row.get("supporting_headline_time")),
        }

    return {
        "company": row.get("company_name"),
        "sector": row.get("sector"),
        "industry": row.get("industry"),
        "market_cap_raw": row.get("market_cap"),
        "short_float_raw": row.get("short_display"),
        "news_items": row.get("news_items", []),
        "primary_news": primary_news,
        "primary_headline_title": row.get("primary_headline_title", ""),
        "primary_headline_source": row.get("primary_headline_source", ""),
        "primary_relevance": row.get("primary_relevance", "none"),
        "primary_category": row.get("primary_category", row.get("category", "No Fresh News")),
        "supporting_news": supporting_news,
        "supporting_headline_title": row.get("supporting_headline_title", ""),
        "supporting_headline_source": row.get("supporting_headline_source", ""),
        "peer_context": row.get("peer_context"),
        "event_context_titles": row.get("event_context_titles", []),
    }


def row_direct_catalyst_score(row: dict[str, Any]) -> float:
    title = safe_text(row.get("primary_headline_title"))
    category = safe_text(row.get("primary_category")) or safe_text(row.get("category"))
    relevance = safe_text(row.get("primary_relevance"))
    score = 0.0

    if relevance == "direct":
        score += 12
    elif relevance == "indirect":
        score += 4

    score += {
        "Earnings": 10,
        "FDA / Clinical": 10,
        "M&A / Rumor": 9,
        "PR / Contract": 8,
        "Financing / Offering": 7,
        "Analyst / Upgrade": 5,
        "Sympathy / Sector": 2,
        "Low Float Momentum": 1,
        "No Fresh News": 0,
    }.get(category, 0)

    if is_concrete_event_headline(title):
        score += 6
    if is_commentary_headline(title):
        score -= 6

    financing_flavor = financing_flavor_from_headline(title)
    if category == "Financing / Offering":
        if financing_flavor == "strategic":
            score += 5
        elif financing_flavor == "dilutive":
            score -= 8

    score += min(float(row.get("premarket_percent") or 0.0) / 10.0, 4.0)
    score += min(int(row.get("premarket_volume") or 0) / 1_000_000.0, 4.0)
    return score


def row_can_anchor_sector_rerating(row: dict[str, Any]) -> bool:
    category = safe_text(row.get("primary_category")) or safe_text(row.get("category"))
    title = safe_text(row.get("primary_headline_title"))
    return (
        safe_text(row.get("primary_relevance")) == "direct"
        and category in DIRECT_CATALYST_CATEGORIES
        and financing_flavor_from_headline(title) == "strategic"
    )


def news_mentions_peer(row: dict[str, Any], peer_row: dict[str, Any]) -> bool:
    peer_tokens = {safe_text(peer_row.get("ticker")).lower(), *company_name_tokens(safe_text(peer_row.get("company_name")))}
    peer_tokens = {token for token in peer_tokens if token}
    if not peer_tokens:
        return False

    headline_text = " ".join(
        clean_headline_text(item.get("title", "")).lower()
        for item in row.get("news_items", [])[:5]
    )
    return any(token in headline_text for token in peer_tokens)


def apply_sector_rerating_context(rows: list[dict[str, Any]]) -> None:
    grouped_rows: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        industry = safe_text(row.get("industry"))
        if not industry or industry == "-":
            continue
        grouped_rows.setdefault(industry, []).append(row)

    for group_rows in grouped_rows.values():
        leader_candidates = [row for row in group_rows if row_can_anchor_sector_rerating(row)]
        if not leader_candidates:
            continue

        leader = max(leader_candidates, key=row_direct_catalyst_score)
        leader_score = row_direct_catalyst_score(leader)
        leader_title = safe_text(leader.get("primary_headline_title"))

        for row in group_rows:
            if row is leader:
                continue
            if int(row.get("premarket_volume") or 0) < 300_000:
                continue
            if float(row.get("premarket_percent") or 0.0) <= 0:
                continue

            own_score = row_direct_catalyst_score(row)
            standalone_category = safe_text(row.get("category")) or safe_text(row.get("primary_category"))
            support_title = safe_text(row.get("supporting_headline_title")) or safe_text(row.get("primary_headline_title"))
            has_support = (
                safe_text(row.get("primary_relevance")) == "direct"
                and bool(support_title)
                and is_concrete_event_headline(support_title)
            )
            peer_linked = news_mentions_peer(row, leader) or news_mentions_peer(leader, row)

            if standalone_category in {"Earnings", "FDA / Clinical", "M&A / Rumor"} and not peer_linked:
                continue
            if float(row.get("premarket_percent") or 0.0) >= 20 and own_score >= leader_score - 2.0:
                continue
            if not peer_linked and own_score >= leader_score - 2.0:
                continue
            if not has_support and not peer_linked:
                continue

            row["standalone_category"] = standalone_category
            row["peer_context"] = {
                "kind": "sector_rerating",
                "leader_ticker": leader.get("ticker"),
                "leader_headline": leader_title,
                "leader_category": leader.get("category"),
                "support_headline": support_title,
                "has_company_specific_support": has_support,
            }
            row["category"] = "Sympathy / Sector"

            if row.get("ai_source") == "fallback":
                bundle = row_analysis_bundle(row)
                row["reasoning"] = build_fallback_reasoning(row, bundle, row["category"], row["grade"])
                row["analysis_details"] = build_fallback_analysis(row, bundle, row["category"], row["grade"])


def dedupe_headline_items(items: list[dict[str, str]], limit: int = 6) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in items:
        title = clean_headline_text(item.get("title", ""))
        if not title:
            continue
        normalized = {
            "title": title,
            "source": safe_text(item.get("source")) or "Headline",
            "url": safe_text(item.get("url")),
            "time": safe_text(item.get("time")),
            "published_at": safe_text(item.get("published_at")),
        }
        key = news_dedupe_key(normalized)
        if key in seen:
            continue
        seen.add(key)
        output.append(normalized)
        if len(output) >= limit:
            break
    return output


def collect_top_headlines_from_rows(rows: list[dict[str, Any]], limit: int = 6) -> list[dict[str, str]]:
    ranked_rows = sorted(
        rows,
        key=lambda item: (
            row_direct_catalyst_score(item),
            abs(row_move_percent(item)),
            row_move_volume(item),
        ),
        reverse=True,
    )
    headline_items: list[dict[str, str]] = []
    for row in ranked_rows:
        if safe_text(row.get("primary_headline_title")):
            headline_items.append(
                {
                    "title": safe_text(row.get("primary_headline_title")),
                    "source": safe_text(row.get("primary_headline_source")),
                    "url": safe_text(row.get("primary_headline_url")),
                    "time": safe_text(row.get("primary_headline_time")),
                }
            )
        if safe_text(row.get("supporting_headline_title")):
            headline_items.append(
                {
                    "title": safe_text(row.get("supporting_headline_title")),
                    "source": safe_text(row.get("supporting_headline_source")),
                    "url": safe_text(row.get("supporting_headline_url")),
                    "time": safe_text(row.get("supporting_headline_time")),
                }
            )
        headline_items.extend(row.get("news_items", [])[:2])
    return dedupe_headline_items(headline_items, limit=limit)


def format_brief_movers(rows: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    ranked_rows = sorted(rows, key=lambda item: abs(row_move_percent(item)), reverse=True)[:limit]
    return [
        {
            "ticker": row.get("ticker"),
            "company_name": row.get("company_name"),
            "move_label": row.get("move_label", "Move"),
            "move_percent": round(row_move_percent(row), 2),
            "category": row.get("category"),
            "grade": row.get("grade"),
            "reasoning": row.get("reasoning"),
        }
        for row in ranked_rows
    ]


def build_market_brief_section_prompt(
    section_kind: str,
    title: str,
    headlines: list[dict[str, str]],
    top_rows: list[dict[str, Any]],
    extra_context: str = "",
) -> str:
    current_stamp = local_now().strftime("%Y-%m-%d %I:%M %p %Z")
    headline_block = "\n".join(
        f"{index}. [{item.get('time') or '-'}] {item.get('source') or 'Headline'}: {clean_headline_text(item.get('title', ''))}"
        for index, item in enumerate(headlines, start=1)
    ) or "No fresh headlines supplied."
    movers_block = "\n".join(
        f"- {row.get('ticker')}: {row_move_percent(row):+.1f}% on {compact_number(row_move_volume(row))}, "
        f"{row.get('category', 'No Fresh News')} / {row.get('grade', 'D')} / {safe_text(row.get('reasoning'))}"
        for row in top_rows[:5]
    ) or "- No movers supplied."

    return f"""
You are an elite cross-asset market strategist writing a crisp premarket and sector brief for an active trader dashboard.
Current local time: {current_stamp}

Return ONLY valid JSON with exactly these keys:
- summary
- analysis_details
- importance

Rules:
1. summary must be one sentence, plain English, max 32 words
2. analysis_details must be 2-3 short paragraphs, no bullet points
3. importance must be exactly one of: High, Medium, Low
4. Focus on what is actually driving this section now: hard catalysts, sector rerating, financing spillover, macro pressure, positioning, or no clean driver
5. If the headlines are commentary or stale, say so plainly
6. Mention when the move looks driven by sympathy or tape instead of direct fundamental repricing

Section kind: {section_kind}
Section title: {title}

Top movers:
{movers_block}

Headlines:
{headline_block}

Additional context:
{extra_context or "-"}
""".strip()


def fallback_market_brief_section(
    section_kind: str,
    title: str,
    headlines: list[dict[str, str]],
    top_rows: list[dict[str, Any]],
    error_text: str = "",
) -> dict[str, str]:
    lead_headline = clean_headline_text(headlines[0]["title"]) if headlines else "No fresh high-signal headline surfaced."
    mover = top_rows[0] if top_rows else {}
    move_clause = ""
    if mover:
        move_clause = (
            f" {safe_text(mover.get('ticker'))} is the lead mover at {row_move_percent(mover):+.1f}%, "
            f"which keeps {title.lower()} in play."
        )
    importance = "High" if section_kind in {"macro", "premarket"} or (mover and abs(row_move_percent(mover)) >= 8) else "Medium"
    summary = f"{lead_headline[:120]} is setting the tone for {title.lower()}.{move_clause}".strip()
    details = [
        f"{title} is being framed mainly by {lead_headline.lower()}." if headlines else f"{title} does not have a clean fresh headline yet.",
        "The current read leans on the strongest visible movers and the best-ranked merged headlines, so it is useful context but not as nuanced as the live model pass.",
    ]
    if error_text:
        details.append(f"AI note: {error_text}")
    return {
        "summary": " ".join(summary.split())[:220],
        "analysis_details": "\n\n".join(details),
        "importance": importance,
        "ai_source": "fallback",
    }


def normalize_market_brief_section_payload(
    payload: dict[str, Any],
    section_kind: str,
    title: str,
    headlines: list[dict[str, str]],
    top_rows: list[dict[str, Any]],
    provider: str | None = None,
) -> dict[str, str]:
    fallback = fallback_market_brief_section(section_kind, title, headlines, top_rows)
    summary = " ".join(safe_text(payload.get("summary")).split()) or fallback["summary"]
    analysis_details = safe_text(payload.get("analysis_details")) or fallback["analysis_details"]
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", analysis_details) if part.strip()]
    analysis_details = "\n\n".join(paragraphs[:3]) if paragraphs else fallback["analysis_details"]
    importance = safe_text(payload.get("importance")).title()
    if importance not in {"High", "Medium", "Low"}:
        importance = fallback["importance"]
    return {
        "summary": summary[:220],
        "analysis_details": analysis_details,
        "importance": importance,
        "ai_source": provider or "fallback",
    }


def analyze_market_brief_section(
    section_kind: str,
    title: str,
    headlines: list[dict[str, str]],
    top_rows: list[dict[str, Any]],
    extra_context: str = "",
) -> dict[str, str]:
    cache_key = json.dumps(
        {
            "version": AI_PROMPT_VERSION,
            "section_kind": section_kind,
            "title": title,
            "headlines": [clean_headline_text(item.get("title", "")) for item in headlines],
            "top_rows": [
                {
                    "ticker": row.get("ticker"),
                    "move": round(row_move_percent(row), 2),
                    "category": row.get("category"),
                    "grade": row.get("grade"),
                    "reasoning": row.get("reasoning"),
                }
                for row in top_rows[:5]
            ],
            "extra_context": extra_context,
        },
        sort_keys=True,
    )
    with MARKET_BRIEF_CACHE_LOCK:
        cached = MARKET_BRIEF_CACHE.get(cache_key)
        if cached and time.time() - cached["created_at"] < cached.get("ttl", ANALYSIS_CACHE_TTL_SECONDS):
            return cached["payload"]

    prompt = build_market_brief_section_prompt(section_kind, title, headlines, top_rows, extra_context=extra_context)
    payload, provider, last_error = request_ai_json(prompt)
    if payload is not None:
        normalized = normalize_market_brief_section_payload(payload, section_kind, title, headlines, top_rows, provider=provider)
    else:
        normalized = fallback_market_brief_section(section_kind, title, headlines, top_rows, error_text=last_error)

    with MARKET_BRIEF_CACHE_LOCK:
        ttl = ANALYSIS_CACHE_TTL_SECONDS if normalized.get("ai_source") != "fallback" else 120
        MARKET_BRIEF_CACHE[cache_key] = {"created_at": time.time(), "ttl": ttl, "payload": normalized}
    return normalized


def build_row_payload(record: dict[str, Any]) -> dict[str, Any]:
    ticker = clean_symbol(record.get("ticker"))
    finviz_bundle = fetch_finviz_bundle(ticker)
    company_name = safe_text(finviz_bundle.get("company")) or safe_text(record.get("company_name") or record.get("name"))
    primary_news = select_primary_news(
        ticker,
        company_name,
        finviz_bundle.get("news_items", []),
    )
    supporting_news = select_supporting_news(
        ticker,
        company_name,
        finviz_bundle.get("news_items", []),
    )
    event_context_news = select_event_context_news(finviz_bundle.get("news_items", []))
    if primary_news:
        remaining_news = [item for item in finviz_bundle.get("news_items", []) if item is not primary_news]
        finviz_bundle["news_items"] = [primary_news] + remaining_news
        finviz_bundle["primary_news"] = primary_news
        finviz_bundle["primary_headline_title"] = clean_headline_text(primary_news.get("title", ""))
        finviz_bundle["primary_headline_source"] = safe_text(primary_news.get("source", ""))
        finviz_bundle["primary_headline_url"] = safe_text(primary_news.get("url", ""))
        finviz_bundle["primary_headline_time"] = safe_text(primary_news.get("time", ""))
        finviz_bundle["primary_relevance"] = infer_headline_relevance(
            ticker,
            company_name,
            [primary_news],
        )
        finviz_bundle["primary_category"] = headline_category_guess(primary_news.get("title", ""))
    else:
        finviz_bundle["primary_news"] = None
        finviz_bundle["primary_headline_title"] = ""
        finviz_bundle["primary_headline_source"] = ""
        finviz_bundle["primary_headline_url"] = ""
        finviz_bundle["primary_headline_time"] = ""
        finviz_bundle["primary_relevance"] = "none"
        finviz_bundle["primary_category"] = "No Fresh News"

    if supporting_news:
        finviz_bundle["supporting_news"] = supporting_news
        finviz_bundle["supporting_headline_title"] = clean_headline_text(supporting_news.get("title", ""))
        finviz_bundle["supporting_headline_source"] = safe_text(supporting_news.get("source", ""))
        finviz_bundle["supporting_headline_url"] = safe_text(supporting_news.get("url", ""))
        finviz_bundle["supporting_headline_time"] = safe_text(supporting_news.get("time", ""))
    else:
        finviz_bundle["supporting_news"] = None
        finviz_bundle["supporting_headline_title"] = ""
        finviz_bundle["supporting_headline_source"] = ""
        finviz_bundle["supporting_headline_url"] = ""
        finviz_bundle["supporting_headline_time"] = ""

    finviz_bundle["event_context_news"] = event_context_news
    finviz_bundle["event_context_titles"] = [clean_headline_text(item.get("title", "")) for item in event_context_news]

    float_shares = parse_metric_number(record.get("float_shares_outstanding_current"))
    if float_shares is None:
        float_shares = parse_metric_number(finviz_bundle.get("fallback_float_raw"))

    short_pct = parse_metric_number(finviz_bundle.get("short_float_raw"))
    display_price = finite_float(record.get("display_price"), default=finite_float(record.get("close"), default=0.0))

    row = {
        "ticker": ticker,
        "company_name": company_name,
        "premarket_percent": round(finite_float(record.get("premarket_change"), default=0.0), 2),
        "premarket_gap": round(
            finite_float(record.get("premarket_gap"), default=finite_float(record.get("premarket_change"), default=0.0)),
            2,
        ),
        "premarket_volume": finite_int(record.get("premarket_volume"), default=0),
        "price": round(display_price, 4),
        "price_display": f"${display_price:.2f}",
        "float_shares": float_shares,
        "float_display": compact_number(float_shares),
        "short_percent": short_pct,
        "short_display": format_percent(short_pct, 1, always_sign=False) if short_pct is not None else "-",
        "sector": finviz_bundle.get("sector") or "-",
        "industry": finviz_bundle.get("industry") or "-",
        "market_cap": finviz_bundle.get("market_cap_raw") or "-",
        "news_items": finviz_bundle.get("news_items", []),
        "primary_headline_title": finviz_bundle.get("primary_headline_title", ""),
        "primary_headline_source": finviz_bundle.get("primary_headline_source", ""),
        "primary_headline_url": finviz_bundle.get("primary_headline_url", ""),
        "primary_headline_time": finviz_bundle.get("primary_headline_time", ""),
        "primary_relevance": finviz_bundle.get("primary_relevance", "none"),
        "primary_category": finviz_bundle.get("primary_category", "No Fresh News"),
        "supporting_headline_title": finviz_bundle.get("supporting_headline_title", ""),
        "supporting_headline_source": finviz_bundle.get("supporting_headline_source", ""),
        "supporting_headline_url": finviz_bundle.get("supporting_headline_url", ""),
        "supporting_headline_time": finviz_bundle.get("supporting_headline_time", ""),
        "event_context_titles": finviz_bundle.get("event_context_titles", []),
    }

    ai_payload = analyze_with_ai(row, finviz_bundle)
    row.update(ai_payload)
    row["chart_url"] = make_multichart_url([ticker], timeframe="D")
    return row


def build_watchlist_row_payload(record: dict[str, Any]) -> dict[str, Any]:
    ticker_full = safe_text(record.get("ticker"))
    ticker = clean_symbol(ticker_full.split(":")[-1] if ":" in ticker_full else ticker_full)
    finviz_bundle = fetch_finviz_bundle(ticker)
    company_name = safe_text(finviz_bundle.get("company")) or safe_text(record.get("company_name") or record.get("name"))
    primary_news = select_primary_news(
        ticker,
        company_name,
        finviz_bundle.get("news_items", []),
    )
    supporting_news = select_supporting_news(
        ticker,
        company_name,
        finviz_bundle.get("news_items", []),
    )
    event_context_news = select_event_context_news(finviz_bundle.get("news_items", []))

    if primary_news:
        remaining_news = [item for item in finviz_bundle.get("news_items", []) if item is not primary_news]
        finviz_bundle["news_items"] = [primary_news] + remaining_news
        finviz_bundle["primary_news"] = primary_news
        finviz_bundle["primary_headline_title"] = clean_headline_text(primary_news.get("title", ""))
        finviz_bundle["primary_headline_source"] = safe_text(primary_news.get("source", ""))
        finviz_bundle["primary_headline_url"] = safe_text(primary_news.get("url", ""))
        finviz_bundle["primary_headline_time"] = safe_text(primary_news.get("time", ""))
        finviz_bundle["primary_relevance"] = infer_headline_relevance(ticker, company_name, [primary_news])
        finviz_bundle["primary_category"] = headline_category_guess(primary_news.get("title", ""))
    else:
        finviz_bundle["primary_news"] = None
        finviz_bundle["primary_headline_title"] = ""
        finviz_bundle["primary_headline_source"] = ""
        finviz_bundle["primary_headline_url"] = ""
        finviz_bundle["primary_headline_time"] = ""
        finviz_bundle["primary_relevance"] = "none"
        finviz_bundle["primary_category"] = "No Fresh News"

    if supporting_news:
        finviz_bundle["supporting_news"] = supporting_news
        finviz_bundle["supporting_headline_title"] = clean_headline_text(supporting_news.get("title", ""))
        finviz_bundle["supporting_headline_source"] = safe_text(supporting_news.get("source", ""))
        finviz_bundle["supporting_headline_url"] = safe_text(supporting_news.get("url", ""))
        finviz_bundle["supporting_headline_time"] = safe_text(supporting_news.get("time", ""))
    else:
        finviz_bundle["supporting_news"] = None
        finviz_bundle["supporting_headline_title"] = ""
        finviz_bundle["supporting_headline_source"] = ""
        finviz_bundle["supporting_headline_url"] = ""
        finviz_bundle["supporting_headline_time"] = ""

    finviz_bundle["event_context_news"] = event_context_news
    finviz_bundle["event_context_titles"] = [clean_headline_text(item.get("title", "")) for item in event_context_news]

    float_shares = parse_metric_number(record.get("float_shares_outstanding_current"))
    if float_shares is None:
        float_shares = parse_metric_number(finviz_bundle.get("fallback_float_raw"))

    short_pct = parse_metric_number(finviz_bundle.get("short_float_raw"))
    move = choose_watchlist_move(record)
    display_price = finite_float(record.get("display_price"), default=finite_float(record.get("close"), default=0.0))

    row = {
        "ticker": ticker,
        "company_name": company_name,
        "analysis_mode": "watchlist",
        "move_label": move["label"],
        "move_percent": round(finite_float(move["percent"], default=0.0), 2),
        "move_volume": finite_int(move["volume"], default=0),
        "move_session": move["session"],
        "day_change_percent": round(finite_float(record.get("change"), default=0.0), 2),
        "regular_volume": finite_int(record.get("volume"), default=0),
        "raw_premarket_percent": round(finite_float(record.get("premarket_change"), default=0.0), 2),
        "raw_premarket_volume": finite_int(record.get("premarket_volume"), default=0),
        "premarket_percent": round(finite_float(move["percent"], default=0.0), 2),
        "premarket_gap": round(finite_float(move["percent"], default=0.0), 2),
        "premarket_volume": finite_int(move["volume"], default=0),
        "price": round(display_price, 4),
        "price_display": f"${display_price:.2f}",
        "float_shares": float_shares,
        "float_display": compact_number(float_shares),
        "short_percent": short_pct,
        "short_display": format_percent(short_pct, 1, always_sign=False) if short_pct is not None else "-",
        "sector": finviz_bundle.get("sector") or "-",
        "industry": finviz_bundle.get("industry") or "-",
        "market_cap": finviz_bundle.get("market_cap_raw") or safe_text(record.get("market_cap_basic")) or "-",
        "news_items": finviz_bundle.get("news_items", []),
        "primary_headline_title": finviz_bundle.get("primary_headline_title", ""),
        "primary_headline_source": finviz_bundle.get("primary_headline_source", ""),
        "primary_headline_url": finviz_bundle.get("primary_headline_url", ""),
        "primary_headline_time": finviz_bundle.get("primary_headline_time", ""),
        "primary_relevance": finviz_bundle.get("primary_relevance", "none"),
        "primary_category": finviz_bundle.get("primary_category", "No Fresh News"),
        "supporting_headline_title": finviz_bundle.get("supporting_headline_title", ""),
        "supporting_headline_source": finviz_bundle.get("supporting_headline_source", ""),
        "supporting_headline_url": finviz_bundle.get("supporting_headline_url", ""),
        "supporting_headline_time": finviz_bundle.get("supporting_headline_time", ""),
        "event_context_titles": finviz_bundle.get("event_context_titles", []),
    }

    ai_payload = analyze_with_ai(row, finviz_bundle)
    row.update(ai_payload)
    row["chart_url"] = make_multichart_url([ticker], timeframe="D")
    return row


@dataclass
class BoardScanState:
    lock: threading.RLock = field(default_factory=threading.RLock)
    rows: list[dict[str, Any]] = field(default_factory=list)
    market_brief: dict[str, Any] = field(default_factory=dict)
    scan_in_progress: bool = False
    scan_started_at: datetime | None = None
    scan_completed_at: datetime | None = None
    scan_duration_seconds: float | None = None
    last_error: str = ""
    status_text: str = "Idle"
    progress_total: int = 0
    progress_done: int = 0
    latest_scan_id: int = 0
    last_scan_origin: str = ""
    last_exported_at: datetime | None = None
    last_exported_path: str = ""
    last_export_error: str = ""
    last_scheduled_slot: str = ""

    def snapshot(self, board: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            symbols = [row["ticker"] for row in self.rows]
            grade_counts = {grade: 0 for grade in ["A", "B", "C", "D"]}
            ai_sources = {"gemini": 0, "openrouter": 0, "openai": 0, "anthropic": 0, "fallback": 0}
            up_count = 0
            down_count = 0
            total_move = 0.0

            for row in self.rows:
                grade = safe_text(row.get("grade")).upper()
                if grade in grade_counts:
                    grade_counts[grade] += 1
                source = safe_text(row.get("ai_source")) or "fallback"
                if source in ai_sources:
                    ai_sources[source] += 1

                move_pct = row_move_percent(row)
                total_move += move_pct
                if move_pct >= 0:
                    up_count += 1
                else:
                    down_count += 1

            average_move = round(total_move / len(self.rows), 2) if self.rows else 0.0
            top_mover = "-"
            if self.rows:
                top_mover = max(self.rows, key=lambda item: abs(row_move_percent(item))).get("ticker", "-")
            next_scheduled = next_scheduled_run_local()
            if board.get("kind") == "market-brief":
                brief_sources = []
                if isinstance(self.market_brief.get("macro_summary"), dict):
                    brief_sources.append(self.market_brief["macro_summary"].get("ai_source"))
                if isinstance(self.market_brief.get("premarket_summary"), dict):
                    brief_sources.append(self.market_brief["premarket_summary"].get("ai_source"))
                brief_sources.extend(section.get("ai_source") for section in self.market_brief.get("sector_sections", []))
                for source in brief_sources:
                    if source in ai_sources:
                        ai_sources[source] += 1

            summary_payload = {
                "count": len(self.rows),
                "a_count": grade_counts["A"],
                "avg_gap": average_move,
                "best_setup": self.rows[0]["ticker"] if self.rows else "-",
                "top_mover": top_mover,
                "up_count": up_count,
                "down_count": down_count,
                "provider_order": AI_PROVIDER_ORDER,
                "gemini_rows": ai_sources["gemini"],
                "openrouter_rows": ai_sources["openrouter"],
                "openai_rows": ai_sources["openai"],
                "anthropic_rows": ai_sources["anthropic"],
                "fallback_rows": ai_sources["fallback"],
                "global_chart_url": make_multichart_url(symbols, timeframe="D") if symbols else "",
            }

            if board.get("kind") == "market-brief":
                section_count = len(self.market_brief.get("sector_sections", []))
                summary_payload.update(
                    {
                        "count": section_count,
                        "macro_headline_count": len(self.market_brief.get("macro_headlines", [])),
                        "symbols_covered": int(self.market_brief.get("symbols_covered") or 0),
                        "priority_sector": safe_text(self.market_brief.get("priority_sector")) or "-",
                        "best_setup": safe_text(self.market_brief.get("priority_sector")) or "-",
                        "global_chart_url": "",
                    }
                )

            return {
                "title": APP_TITLE,
                "subtitle": APP_SUBTITLE,
                "board": board,
                "boards": get_board_definitions(),
                "scan_in_progress": self.scan_in_progress,
                "scan_started_at": self.scan_started_at.isoformat() if self.scan_started_at else None,
                "scan_completed_at": self.scan_completed_at.isoformat() if self.scan_completed_at else None,
                "scan_started_epoch": to_epoch(self.scan_started_at),
                "scan_completed_epoch": to_epoch(self.scan_completed_at),
                "scan_completed_local_epoch": to_local_epoch(self.scan_completed_at),
                "scan_duration_seconds": self.scan_duration_seconds,
                "last_error": self.last_error,
                "status_text": self.status_text,
                "progress_total": self.progress_total,
                "progress_done": self.progress_done,
                "latest_scan_id": self.latest_scan_id,
                "last_scan_origin": self.last_scan_origin,
                "last_exported_at": self.last_exported_at.isoformat() if self.last_exported_at else None,
                "last_exported_epoch": to_local_epoch(self.last_exported_at),
                "last_exported_path": self.last_exported_path,
                "last_export_error": self.last_export_error,
                "last_scheduled_slot": self.last_scheduled_slot,
                "scheduled_times_label": format_schedule_times_label(),
                "next_scheduled_local_epoch": to_epoch(next_scheduled),
                "next_scheduled_local_label": next_scheduled.strftime("%Y-%m-%d %I:%M %p %Z") if next_scheduled else "",
                "summary": summary_payload,
                "rows": self.rows,
                "market_brief": self.market_brief,
            }


BOARD_STATES: dict[str, BoardScanState] = {}


def get_board_state(board_id: str) -> BoardScanState:
    state = BOARD_STATES.get(board_id)
    if state is None:
        state = BoardScanState()
        BOARD_STATES[board_id] = state
    return state


def empty_error_row(symbol: str, message: str, exc: Exception | None = None) -> dict[str, Any]:
    details = message
    if exc is not None:
        details = f"{message}: {exc}\n\n{traceback.format_exc(limit=2)}"
    return {
        "ticker": clean_symbol(symbol.split(":")[-1]),
        "company_name": clean_symbol(symbol.split(":")[-1]),
        "analysis_mode": "watchlist",
        "move_label": "Day move",
        "move_percent": 0.0,
        "move_volume": 0,
        "day_change_percent": 0.0,
        "regular_volume": 0,
        "raw_premarket_percent": 0.0,
        "raw_premarket_volume": 0,
        "premarket_percent": 0.0,
        "premarket_gap": 0.0,
        "premarket_volume": 0,
        "price": 0.0,
        "price_display": "$0.00",
        "float_shares": None,
        "float_display": "-",
        "short_percent": None,
        "short_display": "-",
        "sector": "-",
        "industry": "-",
        "market_cap": "-",
        "news_items": [],
        "category": "No Fresh News",
        "grade": "D",
        "reasoning": message,
        "analysis_details": details,
        "chart_url": make_multichart_url([symbol.split(":")[-1]], timeframe="D"),
    }


def sort_rows_for_board(board: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    if board["kind"] == "watchlist":
        rows.sort(
            key=lambda item: (
                -abs(row_move_percent(item)),
                GRADE_ORDER.get(item.get("grade", "D"), 9),
                -row_move_volume(item),
            )
        )
        return

    rows.sort(
        key=lambda item: (
            GRADE_ORDER.get(item.get("grade", "D"), 9),
            -float(item.get("premarket_percent") or 0.0),
            -int(item.get("premarket_volume") or 0),
        )
    )


def publish_partial_rows(
    board: dict[str, Any],
    state: BoardScanState,
    scan_id: int,
    rows: list[dict[str, Any]],
) -> None:
    """Keep the table populated while a slow AI batch is still in flight."""
    published_rows = [dict(row) for row in rows]
    apply_sector_rerating_context(published_rows)
    sort_rows_for_board(board, published_rows)
    with state.lock:
        if state.latest_scan_id != scan_id:
            return
        state.rows = published_rows


def build_board_rows(board: dict[str, Any], state: BoardScanState) -> list[dict[str, Any]]:
    if board["kind"] == "premarket":
        frame = scan_premarket_gappers()
        if frame.empty:
            return []
        records = frame.to_dict(orient="records")
        builder = build_row_payload
        status_template = "Analyzing premarket catalysts {done}/{total}..."
        symbol_accessor = lambda record: record["ticker"]
    elif board["kind"] == "market-brief":
        with state.lock:
            state.progress_total = 0
            state.progress_done = 0
        brief_payload = build_market_brief_payload()
        with state.lock:
            state.market_brief = brief_payload
        return []
    else:
        frame = scan_watchlist_symbols(board.get("symbols", []))
        if frame.empty:
            return []
        records = frame.to_dict(orient="records")
        builder = build_watchlist_row_payload
        status_template = f"Explaining {board['title']} moves " + "{done}/{total}..."
        symbol_accessor = lambda record: record["ticker"]

    with state.lock:
        state.progress_total = len(records)

    enriched_rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_map = {pool.submit(builder, record): symbol_accessor(record) for record in records}
        for future in as_completed(future_map):
            symbol = future_map[future]
            try:
                enriched_rows.append(future.result())
            except Exception as exc:
                enriched_rows.append(
                    empty_error_row(
                        symbol,
                        f"{clean_symbol(symbol.split(':')[-1])} failed to enrich cleanly, so this row is a placeholder until the next refresh.",
                        exc,
                    )
                )

            with state.lock:
                state.progress_done += 1
                state.status_text = status_template.format(done=state.progress_done, total=state.progress_total)
            publish_partial_rows(board, state, state.latest_scan_id, enriched_rows)

    apply_sector_rerating_context(enriched_rows)
    sort_rows_for_board(board, enriched_rows)
    return enriched_rows


def ensure_rows_for_market_brief(board: dict[str, Any]) -> list[dict[str, Any]]:
    state = get_board_state(board["id"])
    with state.lock:
        existing_rows = [dict(row) for row in state.rows]
        completed_at = state.scan_completed_at
    if existing_rows and completed_at and (utc_now() - completed_at).total_seconds() <= max(ANALYSIS_CACHE_TTL_SECONDS, 1800):
        return existing_rows

    rows = build_board_rows(board, state)
    with state.lock:
        state.rows = rows
        state.scan_completed_at = utc_now()
        state.status_text = f"Loaded {board['title']} for market brief context."
    return rows


def collect_macro_headlines() -> list[dict[str, str]]:
    merged: list[dict[str, str]] = []
    for symbol in MARKET_BRIEF_MACRO_SYMBOLS:
        merged.extend(fetch_yfinance_news_items(symbol))
    merged.sort(key=lambda item: safe_text(item.get("published_at")), reverse=True)
    return dedupe_headline_items(merged, limit=10)


def build_market_brief_payload() -> dict[str, Any]:
    watchlist_boards = [board for board in get_board_definitions() if board.get("kind") == "watchlist"]
    sector_sections: list[dict[str, Any]] = []
    symbols_covered = 0

    macro_headlines = collect_macro_headlines()
    premarket_rows = ensure_rows_for_market_brief(get_board_definition("premarket"))
    premarket_headlines = collect_top_headlines_from_rows(premarket_rows, limit=6)
    macro_context = f"Watchlist sectors being tracked: {', '.join(board.get('title', '') for board in watchlist_boards)}"
    macro_summary = analyze_market_brief_section("macro", "US Market Impact Brief", macro_headlines, premarket_rows[:5], extra_context=macro_context)
    premarket_summary = analyze_market_brief_section(
        "premarket",
        "Premarket Setup",
        premarket_headlines,
        premarket_rows[:5],
        extra_context="Focus on whether leadership is catalyst-driven, macro-driven, or mostly tape and squeeze pressure before the cash open.",
    )

    for watchlist_board in watchlist_boards:
        rows = ensure_rows_for_market_brief(watchlist_board)
        symbols_covered += len(rows)
        movers = sorted(rows, key=lambda item: abs(row_move_percent(item)), reverse=True)
        top_movers = format_brief_movers(movers, limit=4)
        headlines = collect_top_headlines_from_rows(rows, limit=6)
        mover_tickers = ", ".join(item["ticker"] for item in top_movers) if top_movers else "-"
        section_ai = analyze_market_brief_section(
            "sector",
            watchlist_board["title"],
            headlines,
            movers[:5],
            extra_context=f"Sector watchlist: {watchlist_board['title']}. Top movers: {mover_tickers}.",
        )
        sector_sections.append(
            {
                "title": watchlist_board["title"],
                "symbol_count": watchlist_board.get("symbol_count", len(watchlist_board.get("symbols", []))),
                "top_movers": top_movers,
                "headlines": headlines,
                "summary": section_ai["summary"],
                "analysis_details": section_ai["analysis_details"],
                "importance": section_ai["importance"],
                "ai_source": section_ai["ai_source"],
            }
        )

    sector_sections.sort(
        key=lambda section: (
            {"High": 0, "Medium": 1, "Low": 2}.get(section.get("importance", "Low"), 3),
            -max((abs(item.get("move_percent", 0.0)) for item in section.get("top_movers", [])), default=0.0),
            section.get("title", ""),
        )
    )

    return {
        "generated_at": local_now().isoformat(),
        "macro_headlines": macro_headlines,
        "macro_summary": macro_summary,
        "premarket_summary": {
            **premarket_summary,
            "top_movers": format_brief_movers(premarket_rows, limit=5),
            "headlines": premarket_headlines,
        },
        "sector_sections": sector_sections,
        "priority_sector": sector_sections[0]["title"] if sector_sections else "-",
        "symbols_covered": symbols_covered,
    }


def run_scan_job(board_id: str, scan_id: int, origin: str = "manual") -> None:
    board = get_board_definition(board_id)
    state = get_board_state(board_id)
    started_at = utc_now()
    with state.lock:
        state.scan_started_at = started_at
        state.last_scan_origin = origin
        state.status_text = (
            "Running TradingView premarket scan..."
            if board["kind"] == "premarket"
            else ("Building market brief..." if board["kind"] == "market-brief" else f"Loading {board['title']} watchlist...")
        )
        state.progress_total = 0
        state.progress_done = 0
        state.last_error = ""
        state.last_export_error = ""
        if board["kind"] == "market-brief":
            state.market_brief = {}

    try:
        enriched_rows = build_board_rows(board, state)
    except Exception as exc:
        with state.lock:
            if state.latest_scan_id != scan_id:
                return
            state.last_error = str(exc)
            state.status_text = "Scan failed."
            state.scan_completed_at = utc_now()
            state.scan_duration_seconds = round((state.scan_completed_at - started_at).total_seconds(), 2)
    else:
        with state.lock:
            if state.latest_scan_id != scan_id:
                return
            state.rows = enriched_rows
            state.scan_completed_at = utc_now()
            state.scan_duration_seconds = round((state.scan_completed_at - started_at).total_seconds(), 2)
            if board["kind"] == "market-brief":
                state.status_text = "Market brief updated."
            elif enriched_rows:
                state.status_text = f"Scan complete: {len(enriched_rows)} names ranked."
            else:
                state.status_text = "Scan complete. No symbols matched the current filters."
            state.last_error = ""

        if should_export_to_obsidian(board, origin):
            try:
                snapshot = state.snapshot(board)
                note_path = export_snapshot_to_obsidian(board, snapshot)
            except Exception as exc:
                with state.lock:
                    if state.latest_scan_id == scan_id:
                        state.last_export_error = str(exc)
                        state.status_text = "Scan complete, but Obsidian export failed."
            else:
                with state.lock:
                    if state.latest_scan_id == scan_id:
                        state.last_exported_at = utc_now()
                        state.last_exported_path = str(note_path)
                        state.last_export_error = ""
                        state.status_text = (
                            "Market brief updated. Synced to Obsidian."
                            if board["kind"] == "market-brief"
                            else (
                                f"Scan complete: {len(enriched_rows)} names ranked. Synced to Obsidian."
                                if enriched_rows
                                else "Scan complete. Synced to Obsidian."
                            )
                        )
    finally:
        with state.lock:
            if state.latest_scan_id == scan_id:
                state.scan_in_progress = False


def board_snapshot(board_id: str) -> dict[str, Any]:
    board = get_board_definition(board_id)
    state = get_board_state(board_id)
    return sanitize_json_compatible(state.snapshot(board))


def default_board_id() -> str:
    boards = get_board_definitions()
    for board in boards:
        if board["kind"] == "watchlist":
            return board["id"]
    return boards[0]["id"]


def start_scan(board_id: str, force: bool = False, origin: str = "manual") -> dict[str, Any]:
    board = get_board_definition(board_id)
    state = get_board_state(board["id"])
    with state.lock:
        if state.scan_in_progress:
            return sanitize_json_compatible(state.snapshot(board))

        state.scan_in_progress = True
        state.latest_scan_id += 1
        current_scan_id = state.latest_scan_id
        state.last_scan_origin = origin
        state.status_text = "Starting scan..."

    worker = threading.Thread(target=run_scan_job, args=(board["id"], current_scan_id, origin), daemon=True)
    worker.start()
    return sanitize_json_compatible(state.snapshot(board))


BACKGROUND_SERVICES_LOCK = threading.Lock()
BACKGROUND_SERVICES_STARTED = False


def scheduled_scan_boards() -> list[dict[str, Any]]:
    boards = [board for board in get_board_definitions() if board.get("kind", "").lower() in SCHEDULED_SCAN_BOARD_KINDS]
    priority = {"premarket": 0, "watchlist": 1, "market-brief": 2}
    return sorted(boards, key=lambda board: (priority.get(board.get("kind", ""), 9), board.get("title", "")))


def wait_for_scan_completion(board_id: str, timeout_seconds: int = 3600) -> None:
    deadline = time.time() + timeout_seconds
    state = get_board_state(board_id)
    while time.time() < deadline:
        with state.lock:
            if not state.scan_in_progress:
                return
        time.sleep(2)


def run_scheduled_scan_cycle(slot_key: str) -> None:
    for board in scheduled_scan_boards():
        state = get_board_state(board["id"])
        with state.lock:
            if state.last_scheduled_slot == slot_key:
                continue
            state.last_scheduled_slot = slot_key
        start_scan(board["id"], force=True, origin="scheduled")
        wait_for_scan_completion(board["id"])
        if SCHEDULED_SCAN_STAGGER_SECONDS > 0:
            time.sleep(SCHEDULED_SCAN_STAGGER_SECONDS)


def scheduled_scan_loop() -> None:
    while True:
        try:
            slot_key = current_schedule_slot_key()
            if slot_key:
                run_scheduled_scan_cycle(slot_key)
        except Exception:
            traceback.print_exc()
        time.sleep(20)


def should_start_background_services() -> bool:
    if not SCHEDULED_SCAN_ENABLED:
        return False
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        return True
    if os.getenv("FLASK_DEBUG", "").lower() in {"1", "true", "yes", "on"}:
        return False
    return True


app = Flask(__name__)


def start_background_services() -> None:
    global BACKGROUND_SERVICES_STARTED
    if not should_start_background_services():
        return
    with BACKGROUND_SERVICES_LOCK:
        if BACKGROUND_SERVICES_STARTED:
            return
        worker = threading.Thread(target=scheduled_scan_loop, daemon=True, name="scheduled-scan-loop")
        worker.start()
        BACKGROUND_SERVICES_STARTED = True


start_background_services()


@app.get("/")
def index() -> str:
    boards = get_board_definitions()
    return render_template_string(
        HTML_TEMPLATE,
        app_title=APP_TITLE,
        app_subtitle=APP_SUBTITLE,
        auto_refresh_seconds=AUTO_REFRESH_SECONDS,
        min_premarket_pct=MIN_PREMARKET_PCT,
        min_premarket_volume=MIN_PREMARKET_VOLUME,
        ai_provider_order=" -> ".join(provider_display_name(provider) for provider in AI_PROVIDER_ORDER),
        boards=boards,
        default_board_id=default_board_id(),
        app_timezone=APP_TIMEZONE,
    )


@app.post("/api/scan")
def trigger_scan() -> Any:
    board_id = request.args.get("board_id", default_board_id())
    force = request.args.get("force", "0").lower() in {"1", "true", "yes"}
    origin = request.args.get("origin", "manual").strip().lower() or "manual"
    payload = start_scan(board_id=board_id, force=force, origin=origin)
    return jsonify(payload)


@app.get("/api/status")
def scan_status() -> Any:
    board_id = request.args.get("board_id", default_board_id())
    return jsonify(board_snapshot(board_id))


HTML_TEMPLATE = (
    r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ app_title }}</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=Space+Grotesk:wght@500;700&display=swap');

    :root {
      --bg: #0b1621;
      --panel: rgba(18, 30, 44, 0.94);
      --panel-2: rgba(26, 40, 56, 0.94);
      --line: rgba(138, 161, 185, 0.2);
      --line-2: rgba(138, 161, 185, 0.12);
      --text: #edf4ff;
      --muted: #9eb4c9;
      --green: #36d399;
      --red: #ff7b7b;
      --amber: #ffbe69;
      --blue: #6fb7ff;
      --shadow: 0 20px 60px rgba(0, 0, 0, 0.35);
    }

    * { box-sizing: border-box; }
    html, body { margin: 0; min-height: 100%; }
    body {
      background:
        radial-gradient(circle at top right, rgba(54, 211, 153, 0.12), transparent 24%),
        radial-gradient(circle at left top, rgba(111, 183, 255, 0.18), transparent 32%),
        linear-gradient(180deg, #132230 0%, #0b1520 100%);
      color: var(--text);
      font-family: "IBM Plex Sans", sans-serif;
      padding: 28px;
    }

    a { color: inherit; text-decoration: none; }
    .shell { max-width: 1440px; margin: 0 auto; }

    .hero {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 18px;
      align-items: start;
      margin-bottom: 18px;
    }

    .hero-card, .summary-card, .status-card, .table-card, .tabs-card {
      background: linear-gradient(180deg, rgba(19, 31, 46, 0.97), rgba(13, 22, 33, 0.95));
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
      border-radius: 24px;
    }

    .hero-card {
      padding: 24px 26px;
      position: relative;
      overflow: hidden;
    }

    .hero-card::after {
      content: "";
      position: absolute;
      inset: auto -40px -60px auto;
      width: 220px;
      height: 220px;
      background: radial-gradient(circle, rgba(111, 183, 255, 0.16), transparent 68%);
      pointer-events: none;
    }

    .eyebrow {
      color: var(--blue);
      font-size: 12px;
      letter-spacing: 0.18em;
      text-transform: uppercase;
      font-weight: 700;
      margin-bottom: 12px;
    }

    .title {
      margin: 0 0 10px;
      font-family: "Space Grotesk", sans-serif;
      font-size: clamp(32px, 4vw, 44px);
      line-height: 0.96;
      letter-spacing: -0.04em;
    }

    .subtitle {
      margin: 0;
      max-width: 740px;
      color: var(--muted);
      font-size: 15px;
      line-height: 1.6;
    }

    .hero-actions {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      justify-content: flex-end;
      align-content: start;
    }

    .button, .button-link {
      border: 1px solid var(--line);
      background: rgba(20, 31, 44, 0.9);
      color: var(--text);
      border-radius: 14px;
      padding: 12px 16px;
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
      transition: transform 0.18s ease, border-color 0.18s ease, background 0.18s ease;
      display: inline-flex;
      align-items: center;
      gap: 9px;
    }

    .button:hover, .button-link:hover { transform: translateY(-1px); border-color: rgba(111, 183, 255, 0.45); }
    .button-primary { background: linear-gradient(180deg, rgba(24, 112, 255, 0.78), rgba(17, 75, 168, 0.9)); }
    .button-success { background: linear-gradient(180deg, rgba(36, 158, 114, 0.85), rgba(26, 112, 82, 0.9)); }

    .button[disabled], .button-link.disabled {
      opacity: 0.45;
      cursor: not-allowed;
      pointer-events: none;
      transform: none;
    }

    .tabs-card {
      padding: 14px;
      margin-bottom: 18px;
    }

    .board-tabs {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 10px;
    }

    .board-tab {
      min-width: 0;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: rgba(20, 32, 46, 0.9);
      color: var(--text);
      padding: 13px 14px;
      cursor: pointer;
      text-align: left;
      transition: transform 0.18s ease, border-color 0.18s ease, background 0.18s ease;
    }

    .board-tab:hover {
      transform: translateY(-1px);
      border-color: rgba(111, 183, 255, 0.38);
    }

    .board-tab.active {
      background: linear-gradient(180deg, rgba(39, 126, 255, 0.26), rgba(24, 88, 188, 0.26));
      border-color: rgba(111, 183, 255, 0.52);
    }

    .board-tab-name {
      font-size: 14px;
      font-weight: 700;
      margin-bottom: 6px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .board-tab-meta {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.4;
    }

    .summary-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 16px;
      margin-bottom: 18px;
    }

    .summary-card {
      padding: 18px 20px;
      min-height: 126px;
    }

    .summary-label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.14em;
      margin-bottom: 12px;
      font-weight: 700;
    }

    .summary-value {
      font-size: clamp(28px, 3vw, 38px);
      line-height: 1;
      font-family: "Space Grotesk", sans-serif;
      letter-spacing: -0.04em;
      margin-bottom: 10px;
    }

    .summary-note {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
    }

    .status-card {
      padding: 18px 20px;
      margin-bottom: 18px;
      display: grid;
      grid-template-columns: auto 1fr auto;
      align-items: center;
      gap: 16px;
    }

    .status-dot {
      width: 12px;
      height: 12px;
      border-radius: 999px;
      background: var(--blue);
      box-shadow: 0 0 0 7px rgba(111, 183, 255, 0.12);
    }

    .status-dot.scanning {
      background: var(--amber);
      box-shadow: 0 0 0 7px rgba(255, 190, 105, 0.14);
      animation: pulse 1.4s infinite;
    }

    .status-dot.error {
      background: var(--red);
      box-shadow: 0 0 0 7px rgba(255, 123, 123, 0.14);
    }

    @keyframes pulse {
      0% { transform: scale(1); opacity: 1; }
      50% { transform: scale(1.14); opacity: 0.75; }
      100% { transform: scale(1); opacity: 1; }
    }

    .status-title {
      font-size: 15px;
      font-weight: 700;
      margin-bottom: 4px;
    }

    .status-meta {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
    }

    .status-badges {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      justify-content: flex-end;
    }

    .chip {
      border-radius: 999px;
      padding: 9px 12px;
      background: rgba(21, 33, 48, 0.95);
      border: 1px solid var(--line);
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    .table-card { overflow: hidden; }

    .table-top {
      padding: 18px 22px 14px;
      border-bottom: 1px solid var(--line-2);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }

    .table-title {
      font-size: 18px;
      font-weight: 700;
      margin: 0 0 4px;
    }

    .table-note {
      color: var(--muted);
      font-size: 13px;
    }

    table { width: 100%; border-collapse: collapse; }

    thead th {
      padding: 14px 22px;
      border-bottom: 1px solid var(--line);
      color: #cfe0f2;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.14em;
      font-weight: 700;
      text-align: left;
      background: rgba(18, 30, 43, 0.97);
      position: sticky;
      top: 0;
      z-index: 2;
    }

    tbody tr.main-row {
      cursor: pointer;
      transition: background 0.18s ease;
    }

    tbody tr.main-row:hover { background: rgba(30, 45, 61, 0.72); }

    tbody td {
      padding: 16px 22px;
      border-bottom: 1px solid var(--line-2);
      vertical-align: top;
      font-size: 14px;
    }

    .ticker-wrap {
      display: flex;
      flex-direction: column;
      gap: 5px;
    }

    .ticker-main {
      font-size: 18px;
      font-weight: 700;
      letter-spacing: -0.03em;
    }

    .ticker-sub {
      color: var(--muted);
      font-size: 12px;
      max-width: 220px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .positive { color: var(--green); font-weight: 700; }
    .negative { color: var(--red); font-weight: 700; }
    .muted { color: var(--muted); }

    .pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      padding: 8px 12px;
      border: 1px solid transparent;
      font-size: 12px;
      font-weight: 700;
      white-space: nowrap;
    }

    .pill::before {
      content: "";
      width: 8px;
      height: 8px;
      border-radius: 999px;
      background: currentColor;
      opacity: 0.88;
    }

    .grade-a { color: #84ffd0; background: rgba(54, 211, 153, 0.14); border-color: rgba(54, 211, 153, 0.34); }
    .grade-b { color: #85c4ff; background: rgba(111, 183, 255, 0.14); border-color: rgba(111, 183, 255, 0.32); }
    .grade-c { color: #ffd18b; background: rgba(255, 190, 105, 0.14); border-color: rgba(255, 190, 105, 0.3); }
    .grade-d { color: #ffacac; background: rgba(255, 123, 123, 0.14); border-color: rgba(255, 123, 123, 0.3); }

    .cat-earnings,
    .cat-analyst-upgrade { color: #7cc7ff; background: rgba(111, 183, 255, 0.14); border-color: rgba(111, 183, 255, 0.3); }
    .cat-fda-clinical,
    .cat-pr-contract,
    .cat-ma-rumor { color: #8ff0c8; background: rgba(54, 211, 153, 0.14); border-color: rgba(54, 211, 153, 0.32); }
    .cat-financing-offering { color: #ffb8b8; background: rgba(255, 123, 123, 0.14); border-color: rgba(255, 123, 123, 0.28); }
    .cat-sympathy-sector,
    .cat-low-float-momentum,
    .cat-no-fresh-news { color: #ffd18b; background: rgba(255, 190, 105, 0.14); border-color: rgba(255, 190, 105, 0.28); }

    .reasoning {
      color: #e7eef7;
      line-height: 1.5;
      max-width: 440px;
    }

    .action-cell {
      white-space: nowrap;
      text-align: right;
    }

    .market-brief-shell {
      display: grid;
      gap: 18px;
      padding: 20px 22px 24px;
    }

    .market-brief-hero,
    .market-brief-card {
      border: 1px solid var(--line);
      background: rgba(17, 28, 40, 0.78);
      border-radius: 18px;
      padding: 18px;
    }

    .market-brief-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
    }

    .market-brief-title {
      font-size: 18px;
      font-weight: 700;
      margin-bottom: 10px;
    }

    .market-brief-summary {
      color: #edf4ff;
      font-size: 15px;
      line-height: 1.65;
      margin-bottom: 12px;
    }

    .market-brief-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 14px;
    }

    .market-brief-pill {
      border-radius: 999px;
      padding: 7px 11px;
      font-size: 12px;
      font-weight: 700;
      border: 1px solid var(--line);
      color: var(--muted);
      background: rgba(12, 20, 30, 0.84);
    }

    .market-brief-headlines,
    .market-brief-movers {
      display: grid;
      gap: 10px;
    }

    .market-brief-item {
      border: 1px solid var(--line-2);
      border-radius: 14px;
      padding: 12px 14px;
      background: rgba(11, 18, 28, 0.72);
    }

    .market-brief-item-title {
      font-weight: 700;
      line-height: 1.5;
      margin-bottom: 5px;
    }

    .market-brief-item-meta {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.5;
    }
"""
    r"""
    .row-button {
      border-radius: 12px;
      padding: 10px 12px;
      border: 1px solid var(--line);
      background: rgba(26, 41, 58, 0.92);
      color: var(--text);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      display: inline-flex;
      align-items: center;
      gap: 8px;
      cursor: pointer;
    }

    .detail-row {
      display: none;
      background:
        linear-gradient(180deg, rgba(16, 27, 39, 0.98), rgba(11, 19, 29, 0.96)),
        radial-gradient(circle at top right, rgba(54, 211, 153, 0.06), transparent 20%);
    }

    .detail-row.open { display: table-row; }

    .detail-shell {
      padding: 24px 26px 26px;
      display: grid;
      grid-template-columns: minmax(0, 1.7fr) minmax(300px, 0.95fr);
      gap: 22px;
    }

    .detail-block {
      border: 1px solid var(--line);
      background: rgba(24, 36, 51, 0.76);
      border-radius: 18px;
      padding: 18px 18px 16px;
    }

    .detail-heading {
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.16em;
      color: var(--muted);
      font-weight: 700;
      margin-bottom: 14px;
    }

    .detail-paragraph {
      margin: 0 0 14px;
      line-height: 1.72;
      color: #d9e6f3;
      font-size: 14px;
    }

    .detail-paragraph:last-child { margin-bottom: 0; }
    .headline-list { display: grid; gap: 12px; }

    .headline-item {
      border: 1px solid var(--line-2);
      border-radius: 14px;
      padding: 14px;
      background: rgba(18, 29, 42, 0.76);
    }

    .headline-meta {
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 6px;
    }

    .headline-title {
      color: var(--text);
      font-size: 14px;
      line-height: 1.5;
      font-weight: 600;
    }

    .headline-link {
      color: var(--blue);
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      display: inline-block;
      margin-top: 10px;
    }

    .empty {
      padding: 42px 22px 50px;
      text-align: center;
      color: var(--muted);
      font-size: 15px;
    }

    .footer-note {
      padding: 16px 22px 20px;
      color: var(--muted);
      font-size: 12px;
      border-top: 1px solid var(--line-2);
      display: flex;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }

    @media (max-width: 1180px) {
      .hero, .status-card, .detail-shell { grid-template-columns: 1fr; }
      .summary-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .market-brief-grid { grid-template-columns: 1fr; }
      .status-badges { justify-content: flex-start; }
      .action-cell { text-align: left; }
    }

    @media (max-width: 780px) {
      body { padding: 16px; }
      .summary-grid { grid-template-columns: 1fr; }
      table, thead, tbody, th, td, tr { display: block; }
      thead { display: none; }
      tbody tr.main-row {
        padding: 14px 0;
        border-bottom: 1px solid var(--line);
      }
      tbody td {
        padding: 8px 18px;
        border: 0;
      }
      .detail-row.open { display: block; }
      .table-card { overflow: visible; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="hero-card">
        <div class="eyebrow">Premarket Catalyst Dashboard</div>
        <h1 class="title">{{ app_title }}</h1>
        <p class="subtitle">
          {{ app_subtitle }}. Your shared TradingView watchlists load as separate tabs, and each tab explains what moved each stock today with Finviz headlines plus catalyst reasoning.
        </p>
      </div>
      <div class="hero-actions">
        <button class="button button-primary" id="refreshButton" type="button">Refresh Scan</button>
        <a class="button-link button-success disabled" id="globalChartButton" href="#" target="_blank" rel="noreferrer">Launch Multi-Chart</a>
      </div>
    </section>

    <section class="tabs-card">
      <div class="board-tabs" id="boardTabs"></div>
    </section>

    <section class="summary-grid">
      <article class="summary-card">
        <div class="summary-label" id="summaryCountLabel">Symbols</div>
        <div class="summary-value" id="summaryCount">-</div>
        <div class="summary-note" id="summaryCountNote">Matching names in the selected board.</div>
      </article>
      <article class="summary-card">
        <div class="summary-label" id="summarySecondLabel">A Grades</div>
        <div class="summary-value" id="summaryAGrades">-</div>
        <div class="summary-note" id="summarySecondNote">Highest-confidence catalyst setups ranked by Gemini.</div>
      </article>
      <article class="summary-card">
        <div class="summary-label" id="summaryAvgLabel">Average Move %</div>
        <div class="summary-value" id="summaryAvgGap">-</div>
        <div class="summary-note" id="summaryAvgNote">Average move across the selected board.</div>
      </article>
      <article class="summary-card">
        <div class="summary-label" id="summaryBestLabel">Best Setup</div>
        <div class="summary-value" id="summaryBestSetup">-</div>
        <div class="summary-note" id="summaryBestNote">Top-ranked name after catalyst and liquidity scoring.</div>
      </article>
    </section>

    <section class="status-card">
      <div class="status-dot" id="statusDot"></div>
      <div>
        <div class="status-title" id="statusTitle">Waiting for first scan...</div>
        <div class="status-meta" id="statusMeta">The page triggers a background scan on load so the UI stays responsive.</div>
      </div>
      <div class="status-badges">
        <div class="chip">AI Order: {{ ai_provider_order }}</div>
        <div class="chip" id="aiModeChip">AI: waiting</div>
        <div class="chip" id="lastScanChip">Last scan: never</div>
        <div class="chip" id="scheduleChip">Schedule: 8:30 AM & 8:30 PM {{ app_timezone }}</div>
        <div class="chip" id="nextRefreshChip">Next refresh: {{ auto_refresh_seconds }}s</div>
      </div>
    </section>

    <section class="table-card">
      <div class="table-top">
        <div>
          <div class="table-title" id="tableTitle">Live Board</div>
          <div class="table-note" id="tableNote">Click any row for the full 3-4 paragraph AI breakdown and the latest merged headlines.</div>
        </div>
      </div>

      <div id="marketBriefShell" class="market-brief-shell" style="display:none;"></div>

      <table id="dataTable">
        <thead>
          <tr>
            <th>Ticker</th>
            <th id="moveHeader">Move %</th>
            <th>Volume</th>
            <th>Float</th>
            <th>Short %</th>
            <th>Category</th>
            <th>Grade</th>
            <th>Reasoning</th>
            <th></th>
          </tr>
        </thead>
        <tbody id="tableBody">
          <tr><td class="empty" colspan="9">Waiting for the first scan...</td></tr>
        </tbody>
      </table>

      <div class="footer-note">
        <span id="footerLeft">TradingView scan uses positive premarket movers only.</span>
        <span id="footerRight">News is merged from Yahoo Finance and the maintained lit26 Finviz fork. ib_chart opens on `tf=D` by default.</span>
      </div>
    </section>
  </div>
"""
    r"""
  <script>
    const AUTO_REFRESH_SECONDS = {{ auto_refresh_seconds | tojson }};
    const INITIAL_BOARDS = {{ boards | tojson }};
    const DEFAULT_BOARD_ID = {{ default_board_id | tojson }};
    let latestPayload = null;
    let selectedBoardId = DEFAULT_BOARD_ID;
    let refreshCountdown = AUTO_REFRESH_SECONDS;
    const boardCache = Object.fromEntries(INITIAL_BOARDS.map((board) => [board.id, null]));

    const boardTabs = document.getElementById("boardTabs");
    const refreshButton = document.getElementById("refreshButton");
    const globalChartButton = document.getElementById("globalChartButton");
    const dataTable = document.getElementById("dataTable");
    const tableBody = document.getElementById("tableBody");
    const tableTitle = document.getElementById("tableTitle");
    const tableNote = document.getElementById("tableNote");
    const marketBriefShell = document.getElementById("marketBriefShell");
    const moveHeader = document.getElementById("moveHeader");
    const footerLeft = document.getElementById("footerLeft");
    const footerRight = document.getElementById("footerRight");
    const summaryCountLabel = document.getElementById("summaryCountLabel");
    const summaryCount = document.getElementById("summaryCount");
    const summaryCountNote = document.getElementById("summaryCountNote");
    const summarySecondLabel = document.getElementById("summarySecondLabel");
    const summaryAGrades = document.getElementById("summaryAGrades");
    const summarySecondNote = document.getElementById("summarySecondNote");
    const summaryAvgLabel = document.getElementById("summaryAvgLabel");
    const summaryAvgGap = document.getElementById("summaryAvgGap");
    const summaryAvgNote = document.getElementById("summaryAvgNote");
    const summaryBestLabel = document.getElementById("summaryBestLabel");
    const summaryBestSetup = document.getElementById("summaryBestSetup");
    const summaryBestNote = document.getElementById("summaryBestNote");
    const statusDot = document.getElementById("statusDot");
    const statusTitle = document.getElementById("statusTitle");
    const statusMeta = document.getElementById("statusMeta");
    const aiModeChip = document.getElementById("aiModeChip");
    const lastScanChip = document.getElementById("lastScanChip");
    const scheduleChip = document.getElementById("scheduleChip");
    const nextRefreshChip = document.getElementById("nextRefreshChip");

    function escapeHtml(value) {
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function slugifyCategory(category) {
      return "cat-" + String(category || "no-fresh-news")
        .toLowerCase()
        .replaceAll("/", " ")
        .replaceAll("&", " ")
        .replace(/[^\w\s-]/g, "")
        .trim()
        .replace(/\s+/g, "-");
    }

    function gradeClass(grade) {
      return "grade-" + String(grade || "d").toLowerCase();
    }

    function formatPercent(value) {
      const number = Number(value || 0);
      const sign = number > 0 ? "+" : "";
      return `${sign}${number.toFixed(1)}%`;
    }

    function formatVolume(value) {
      const number = Number(value || 0);
      if (number >= 1_000_000_000) return `${(number / 1_000_000_000).toFixed(1)}B`;
      if (number >= 1_000_000) return `${(number / 1_000_000).toFixed(1)}M`;
      if (number >= 1_000) return `${(number / 1_000).toFixed(1)}K`;
      return `${Math.round(number)}`;
    }

    function boardById(boardId) {
      return (latestPayload?.boards || INITIAL_BOARDS).find((board) => board.id === boardId) || INITIAL_BOARDS[0];
    }

    function rowMovePercent(row) {
      return Number(row.move_percent ?? row.premarket_percent ?? 0);
    }

    function rowMoveVolume(row) {
      return Number(row.move_volume ?? row.premarket_volume ?? 0);
    }

    function rowMoveLabel(row) {
      return row.move_label || "Premarket move";
    }

    function relativeLastScanText(epochSeconds) {
      if (!epochSeconds) return "Last scan: never";
      const secondsAgo = Math.max(0, Math.floor(Date.now() / 1000 - epochSeconds));
      return `Last scan: ${secondsAgo}s ago`;
    }

    function relativeExportText(epochSeconds) {
      if (!epochSeconds) return "never";
      const secondsAgo = Math.max(0, Math.floor(Date.now() / 1000 - epochSeconds));
      return `${secondsAgo}s ago`;
    }

    function renderBoardTabs(boards) {
      boardTabs.innerHTML = boards.map((board) => `
        <button class="board-tab ${board.id === selectedBoardId ? "active" : ""}" type="button" data-board-id="${escapeHtml(board.id)}">
          <div class="board-tab-name">${escapeHtml(board.title)}</div>
          <div class="board-tab-meta">${escapeHtml(board.description || `${board.symbol_count || 0} symbols`)}</div>
        </button>
      `).join("");

      boardTabs.querySelectorAll("[data-board-id]").forEach((button) => {
        button.addEventListener("click", () => {
          const boardId = button.dataset.boardId;
          if (!boardId || boardId === selectedBoardId) return;
          selectedBoardId = boardId;
          refreshCountdown = AUTO_REFRESH_SECONDS;
          renderBoardTabs(boards);
          ensureBoardLoaded(boardId, false, "view").catch((error) => {
            statusTitle.textContent = "Failed to switch board";
            statusMeta.textContent = String(error);
          });
        });
      });
    }

    function renderHeadlineList(items) {
      if (!items || !items.length) {
        return `<div class="headline-item"><div class="headline-title muted">No recent Finviz headlines were returned for this ticker.</div></div>`;
      }
      return items.map((item) => {
        const sourceBits = [item.source, item.time].filter(Boolean).join(" - ");
        const linkHtml = item.url
          ? `<a class="headline-link" href="${escapeHtml(item.url)}" target="_blank" rel="noreferrer">Open Headline</a>`
          : "";
        return `
          <div class="headline-item">
            <div class="headline-meta">${escapeHtml(sourceBits || "Headline")}</div>
            <div class="headline-title">${escapeHtml(item.title || "")}</div>
            ${linkHtml}
          </div>
        `;
      }).join("");
    }

    function renderAnalysisParagraphs(text) {
      return String(text || "")
        .split(/\n\s*\n/)
        .filter(Boolean)
        .map((paragraph) => `<p class="detail-paragraph">${escapeHtml(paragraph.trim())}</p>`)
        .join("");
    }

    function renderBriefHeadlines(items) {
      if (!items || !items.length) {
        return `<div class="market-brief-item"><div class="market-brief-item-title muted">No fresh headlines captured.</div></div>`;
      }
      return items.map((item) => `
        <div class="market-brief-item">
          <div class="market-brief-item-title">${escapeHtml(item.title || "")}</div>
          <div class="market-brief-item-meta">${escapeHtml([item.source, item.time].filter(Boolean).join(" - "))}</div>
        </div>
      `).join("");
    }

    function renderBriefMovers(items) {
      if (!items || !items.length) {
        return `<div class="market-brief-item"><div class="market-brief-item-title muted">No movers captured for this section.</div></div>`;
      }
      return items.map((item) => `
        <div class="market-brief-item">
          <div class="market-brief-item-title">${escapeHtml(item.ticker || "")} ${escapeHtml(formatPercent(item.move_percent || 0))}</div>
          <div class="market-brief-item-meta">${escapeHtml((item.category || "-") + " / " + (item.grade || "-"))}</div>
          <div class="market-brief-item-meta">${escapeHtml(item.reasoning || "")}</div>
        </div>
      `).join("");
    }

    function renderMarketBrief(brief) {
      const generatedAt = brief?.generated_at ? new Date(brief.generated_at).toLocaleString() : "";
      const macro = brief?.macro_summary || {};
      const premarket = brief?.premarket_summary || {};
      const sectors = brief?.sector_sections || [];

      marketBriefShell.innerHTML = `
        <div class="market-brief-hero">
          <div class="market-brief-title">Market Impact Brief</div>
          <div class="market-brief-summary">${escapeHtml(macro.summary || "Waiting for macro summary...")}</div>
          <div class="market-brief-meta">
            <span class="market-brief-pill">Importance: ${escapeHtml(macro.importance || "-")}</span>
            <span class="market-brief-pill">Priority Sector: ${escapeHtml(brief?.priority_sector || "-")}</span>
            <span class="market-brief-pill">Generated: ${escapeHtml(generatedAt || "-")}</span>
          </div>
          ${renderAnalysisParagraphs(macro.analysis_details || "")}
        </div>
        <div class="market-brief-grid">
          <div class="market-brief-card">
            <div class="market-brief-title">Macro Headlines</div>
            <div class="market-brief-headlines">${renderBriefHeadlines(brief?.macro_headlines || [])}</div>
          </div>
          <div class="market-brief-card">
            <div class="market-brief-title">Premarket Setup</div>
            <div class="market-brief-summary">${escapeHtml(premarket.summary || "Waiting for premarket setup...")}</div>
            ${renderAnalysisParagraphs(premarket.analysis_details || "")}
            <div class="detail-heading">Premarket Leaders</div>
            <div class="market-brief-movers">${renderBriefMovers(premarket.top_movers || [])}</div>
            <div class="detail-heading">Premarket Headlines</div>
            <div class="market-brief-headlines">${renderBriefHeadlines(premarket.headlines || [])}</div>
          </div>
        </div>
        ${sectors.map((section) => `
          <div class="market-brief-card">
            <div class="market-brief-title">${escapeHtml(section.title || "Sector")}</div>
            <div class="market-brief-meta">
              <span class="market-brief-pill">Importance: ${escapeHtml(section.importance || "-")}</span>
              <span class="market-brief-pill">Symbols: ${escapeHtml(section.symbol_count || 0)}</span>
            </div>
            <div class="market-brief-summary">${escapeHtml(section.summary || "")}</div>
            ${renderAnalysisParagraphs(section.analysis_details || "")}
            <div class="market-brief-grid">
              <div>
                <div class="detail-heading">Top Movers</div>
                <div class="market-brief-movers">${renderBriefMovers(section.top_movers || [])}</div>
              </div>
              <div>
                <div class="detail-heading">Sector Headlines</div>
                <div class="market-brief-headlines">${renderBriefHeadlines(section.headlines || [])}</div>
              </div>
            </div>
          </div>
        `).join("")}
      `;
    }

    function renderRows(rows, board) {
      if (!rows || !rows.length) {
        const emptyText = board?.kind === "watchlist"
          ? "No current names were returned for this TradingView watchlist."
          : "No current premarket names matched the scan filters.";
        tableBody.innerHTML = `<tr><td class="empty" colspan="9">${escapeHtml(emptyText)}</td></tr>`;
        return;
      }

      tableBody.innerHTML = rows.map((row, index) => {
        const movePercent = rowMovePercent(row);
        const moveVolume = rowMoveVolume(row);
        const positiveClass = movePercent >= 0 ? "positive" : "negative";
        const categoryClass = slugifyCategory(row.category);
        const detailId = `detail-${index}`;
        return `
          <tr class="main-row" data-detail-id="${detailId}">
            <td>
              <div class="ticker-wrap">
                <span class="ticker-main">${escapeHtml(row.ticker)}</span>
                <span class="ticker-sub">${escapeHtml((row.company_name || "") + " · " + rowMoveLabel(row))}</span>
              </div>
            </td>
            <td><span class="${positiveClass}">${escapeHtml(formatPercent(movePercent))}</span></td>
            <td>${escapeHtml(formatVolume(moveVolume))}</td>
            <td>${escapeHtml(row.float_display || "-")}</td>
            <td>${escapeHtml(row.short_display || "-")}</td>
            <td><span class="pill ${categoryClass}">${escapeHtml(row.category || "No Fresh News")}</span></td>
            <td><span class="pill ${gradeClass(row.grade)}">${escapeHtml(row.grade || "D")}</span></td>
            <td><div class="reasoning">${escapeHtml(row.reasoning || "")}</div></td>
            <td class="action-cell">
              <a class="row-button" href="${escapeHtml(row.chart_url || "#")}" target="_blank" rel="noreferrer" data-stop-row-toggle="true">
                Launch Multi-Chart
              </a>
            </td>
          </tr>
          <tr class="detail-row" id="${detailId}">
            <td colspan="9">
              <div class="detail-shell">
                <div class="detail-block">
                  <div class="detail-heading">Full AI Analysis</div>
                  ${renderAnalysisParagraphs(row.analysis_details || "")}
                </div>
                <div class="detail-block">
                  <div class="detail-heading">Latest Headlines</div>
                  <div class="headline-list">${renderHeadlineList(row.news_items || [])}</div>
                </div>
              </div>
            </td>
          </tr>
        `;
      }).join("");

      tableBody.querySelectorAll("tr.main-row").forEach((rowElement) => {
        rowElement.addEventListener("click", (event) => {
          if (event.target.closest("[data-stop-row-toggle='true']")) return;
          const detailRow = document.getElementById(rowElement.dataset.detailId);
          if (detailRow) detailRow.classList.toggle("open");
        });
      });
    }

    function updateBoardChrome(board, summary) {
      if (board?.kind === "market-brief") {
        summaryCountLabel.textContent = "Sectors";
        summaryCountNote.textContent = "Watchlist sectors summarized in the current brief.";
        summarySecondLabel.textContent = "Macro News";
        summarySecondNote.textContent = "High-impact market headlines pulled into the brief.";
        summaryAvgLabel.textContent = "Symbols Covered";
        summaryAvgNote.textContent = "Total watchlist names included in the sector synthesis.";
        summaryBestLabel.textContent = "Priority Sector";
        summaryBestNote.textContent = "The sector the brief flags as most important right now.";
        tableTitle.textContent = "Market Brief";
        tableNote.textContent = "Macro context, premarket setup, and sector-by-sector news synthesis across your watchlists.";
        moveHeader.textContent = "Move %";
        footerLeft.textContent = "Market brief reuses the same watchlist states, merged headlines, and AI stack as the rest of the dashboard.";
        footerRight.textContent = "Scheduled scans can precompute this brief at 8:30 AM and 8:30 PM Singapore time.";
        summaryAGrades.textContent = summary?.macro_headline_count ?? "-";
        summaryBestSetup.textContent = summary?.priority_sector || "-";
        return;
      }

      if (board?.kind === "watchlist") {
        summaryCountLabel.textContent = "Symbols";
        summaryCountNote.textContent = `${board.symbol_count || 0} names in this shared TradingView watchlist.`;
        summarySecondLabel.textContent = "Up Names";
        summarySecondNote.textContent = "Names green on the selected move lens.";
        summaryAvgLabel.textContent = "Average Move %";
        summaryAvgNote.textContent = "Average selected move across this watchlist.";
        summaryBestLabel.textContent = "Top Mover";
        summaryBestNote.textContent = "Largest absolute move in this watchlist today.";
        tableTitle.textContent = board.title || "Watchlist Board";
        tableNote.textContent = "Fetches merged Yahoo Finance and Finviz headlines for each ticker and explains what happened to the stock on the day.";
        moveHeader.textContent = "Move %";
        footerLeft.textContent = `TradingView watchlist: ${board.title}`;
        footerRight.textContent = "The move lens uses day move by default and flips to premarket when premarket action is clearly leading.";
        summaryAGrades.textContent = summary?.up_count ?? "-";
        summaryBestSetup.textContent = summary?.top_mover || "-";
        return;
      }

      summaryCountLabel.textContent = "Symbols";
      summaryCountNote.textContent = "Matching TradingView premarket gap names above {{ min_premarket_pct }}%.";
      summarySecondLabel.textContent = "A Grades";
      summarySecondNote.textContent = "Highest-confidence catalyst setups ranked by AI.";
      summaryAvgLabel.textContent = "Average Premkt %";
      summaryAvgNote.textContent = "Scanner threshold: {{ min_premarket_volume | int }} shares minimum premarket volume.";
      summaryBestLabel.textContent = "Best Setup";
      summaryBestNote.textContent = "Top-ranked name after catalyst and liquidity scoring.";
      tableTitle.textContent = "Live Premarket Board";
      tableNote.textContent = "Click any row for the full 3-4 paragraph AI breakdown and the latest merged headlines.";
      moveHeader.textContent = "Premkt %";
      footerLeft.textContent = "TradingView scan uses positive premarket movers only.";
      footerRight.textContent = "News is merged from Yahoo Finance and the maintained lit26 Finviz fork. ib_chart opens on `tf=D` by default.";
      summaryAGrades.textContent = summary?.a_count ?? "-";
      summaryBestSetup.textContent = summary?.best_setup || "-";
    }

    function updateSummary(summary, board) {
      updateBoardChrome(board, summary);
      if (board?.kind === "market-brief") {
        summaryCount.textContent = summary?.count ?? "-";
        summaryAvgGap.textContent = summary?.symbols_covered ?? "-";
      } else {
        summaryCount.textContent = summary?.count ?? "-";
        summaryAvgGap.textContent = summary?.avg_gap !== undefined ? `${Number(summary.avg_gap).toFixed(1)}%` : "-";
      }

      const geminiRows = Number(summary?.gemini_rows || 0);
      const openrouterRows = Number(summary?.openrouter_rows || 0);
      const openaiRows = Number(summary?.openai_rows || 0);
      const anthropicRows = Number(summary?.anthropic_rows || 0);
      const fallbackRows = Number(summary?.fallback_rows || 0);
      const liveParts = [];
      if (geminiRows > 0) liveParts.push(`Gemini ${geminiRows}`);
      if (openrouterRows > 0) liveParts.push(`OpenRouter ${openrouterRows}`);
      if (openaiRows > 0) liveParts.push(`OpenAI ${openaiRows}`);
      if (anthropicRows > 0) liveParts.push(`Anthropic ${anthropicRows}`);

      if (liveParts.length > 0 && fallbackRows === 0) {
        aiModeChip.textContent = `AI: ${liveParts.join(" / ")}`;
      } else if (liveParts.length > 0 && fallbackRows > 0) {
        aiModeChip.textContent = `AI: ${liveParts.join(" / ")} / Fallback ${fallbackRows}`;
      } else if (fallbackRows > 0) {
        aiModeChip.textContent = `AI: Fallback ${fallbackRows}`;
      } else {
        aiModeChip.textContent = "AI: waiting";
      }

      if (summary?.global_chart_url) {
        globalChartButton.href = summary.global_chart_url;
        globalChartButton.classList.remove("disabled");
      } else {
        globalChartButton.href = "#";
        globalChartButton.classList.add("disabled");
      }
    }

    function updateStatus(payload) {
      boardCache[payload.board?.id || selectedBoardId] = payload;
      latestPayload = payload;
      const board = payload.board || boardById(selectedBoardId);
      const scanInProgress = Boolean(payload.scan_in_progress);
      const lastError = payload.last_error || "";

      renderBoardTabs(payload.boards || INITIAL_BOARDS);
      statusDot.className = "status-dot";
      if (lastError) {
        statusDot.classList.add("error");
      } else if (scanInProgress) {
        statusDot.classList.add("scanning");
      }

      statusTitle.textContent = payload.status_text || (scanInProgress ? "Scanning..." : "Idle");
      const meta = [];
      if (board?.title) meta.push(`Board: ${board.title}`);
      if (scanInProgress) meta.push(`Progress: ${payload.progress_done || 0}/${payload.progress_total || 0}`);
      if (payload.scan_duration_seconds) meta.push(`Duration: ${payload.scan_duration_seconds}s`);
      if (payload.last_exported_epoch) meta.push(`Obsidian synced ${relativeExportText(payload.last_exported_epoch)}`);
      if (payload.last_export_error) meta.push(`Obsidian export: ${payload.last_export_error}`);
      if (payload.next_scheduled_local_label) meta.push(`Next scheduled ${payload.next_scheduled_local_label}`);
      if (lastError) meta.push(`Error: ${lastError}`);
      statusMeta.textContent = meta.join(" | ") || "Ready.";

      lastScanChip.textContent = relativeLastScanText(payload.scan_completed_epoch);
      scheduleChip.textContent = `Schedule: ${payload.scheduled_times_label || "Disabled"}`;
      refreshButton.disabled = scanInProgress;
      updateSummary(payload.summary || {}, board);
      if (board?.kind === "market-brief") {
        dataTable.style.display = "none";
        marketBriefShell.style.display = "grid";
        renderMarketBrief(payload.market_brief || {});
      } else {
        marketBriefShell.style.display = "none";
        dataTable.style.display = "table";
        renderRows(payload.rows || [], board);
      }
    }

    async function fetchStatus(boardId = selectedBoardId) {
      const response = await fetch(`/api/status?board_id=${encodeURIComponent(boardId)}`);
      const payload = await response.json();
      if (boardId === selectedBoardId) updateStatus(payload);
      return payload;
    }

    async function triggerScan(boardId = selectedBoardId, force = true, origin = "manual") {
      refreshButton.disabled = true;
      const response = await fetch(`/api/scan?board_id=${encodeURIComponent(boardId)}&force=${force ? "1" : "0"}&origin=${encodeURIComponent(origin)}`, { method: "POST" });
      const payload = await response.json();
      if (boardId === selectedBoardId) updateStatus(payload);
      return payload;
    }

    async function ensureBoardLoaded(boardId, forceScan = false, origin = "view") {
      const cached = boardCache[boardId];
      if (cached && !forceScan) {
        if (boardId === selectedBoardId) updateStatus(cached);
        return cached;
      }

      const statusPayload = await fetchStatus(boardId);
      if (forceScan || !statusPayload.scan_completed_epoch) {
        return triggerScan(boardId, true, origin);
      }
      if (boardId === selectedBoardId) updateStatus(statusPayload);
      return statusPayload;
    }

    refreshButton.addEventListener("click", () => {
      refreshCountdown = AUTO_REFRESH_SECONDS;
      triggerScan(selectedBoardId, true, "manual").catch((error) => {
        statusTitle.textContent = "Failed to trigger scan";
        statusMeta.textContent = String(error);
      });
    });

    setInterval(() => {
      refreshCountdown = Math.max(0, refreshCountdown - 1);
      nextRefreshChip.textContent = `Next refresh: ${refreshCountdown}s`;
      if (latestPayload) {
        lastScanChip.textContent = relativeLastScanText(latestPayload.scan_completed_epoch);
      }
      if (refreshCountdown === 0) {
        refreshCountdown = AUTO_REFRESH_SECONDS;
        const activeBoard = boardById(selectedBoardId);
        if (activeBoard?.kind === "market-brief") {
          fetchStatus(selectedBoardId).catch(() => {});
        } else {
          triggerScan(selectedBoardId, true, "auto").catch(() => {});
        }
      }
    }, 1000);

    setInterval(() => {
      fetchStatus(selectedBoardId).catch(() => {});
    }, 2500);

    renderBoardTabs(INITIAL_BOARDS);
    ensureBoardLoaded(selectedBoardId, true, "view").catch(() => triggerScan(selectedBoardId, true, "view"));
  </script>
</body>
</html>
"""
)


if __name__ == "__main__":
    app.run(
        host=os.getenv("APP_HOST", "127.0.0.1"),
        port=int(os.getenv("APP_PORT", "5000")),
        debug=os.getenv("FLASK_DEBUG", "1").lower() in {"1", "true", "yes", "on"},
    )
