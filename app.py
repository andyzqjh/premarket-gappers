"""
Premarket Gappers + All-Time Volume Breaker Watchlist

Install:
    pip install streamlit tradingview-screener finvizfinance yfinance pandas anthropic

Run:
    streamlit run app.py
"""

from __future__ import annotations

import html
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import anthropic
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import yfinance as yf
from finvizfinance.quote import finvizfinance
from tradingview_screener import And, Column, Or, Query


APP_TITLE = "Premarket Gappers + All-Time Volume Breaker Watchlist"
APP_SUBTITLE = (
    "Dark premarket dashboard for US stocks with catalyst headlines, trend grading, "
    "volume-breaker detection, and a persistent local watchlist."
)
WATCHLIST_PATH = Path(__file__).with_name("watchlist.json")
NEW_YORK_TZ = ZoneInfo("America/New_York")
AUTO_REFRESH_SECONDS = 60
DEFAULT_SCAN_LIMIT = 30
MAX_SCAN_LIMIT = 60
EXCHANGES = ["NASDAQ", "NYSE", "AMEX"]


st.set_page_config(
    page_title=APP_TITLE,
    layout="wide",
    initial_sidebar_state="expanded",
)


def inject_css() -> None:
    # Streamlit does not expose a runtime `st.theme = "dark"` setter, so the app
    # enforces a dark trading-dashboard look through page config + CSS.
    st.markdown(
        """
        <style>
        :root {
            --line: rgba(255,255,255,0.08);
            --text: #f5f7fb;
            --muted: #8fa0b5;
            --green: #3ddc97;
            --red: #ff6b6b;
            --orange: #ffb454;
            --blue: #3b82f6;
        }

        .stApp {
            background:
                radial-gradient(circle at top right, rgba(59,130,246,0.12), transparent 24%),
                radial-gradient(circle at left top, rgba(61,220,151,0.08), transparent 20%),
                linear-gradient(180deg, #091018 0%, #06090f 100%);
            color: var(--text);
        }

        [data-testid="stHeader"] {
            background: rgba(0, 0, 0, 0);
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0b1118 0%, #0a0f15 100%);
            border-right: 1px solid var(--line);
        }

        .block-container {
            padding-top: 1.3rem;
            padding-bottom: 2rem;
        }

        .app-hero {
            background: linear-gradient(135deg, rgba(16,23,32,0.95), rgba(12,18,27,0.88));
            border: 1px solid var(--line);
            border-radius: 18px;
            padding: 1.2rem 1.35rem 1.05rem 1.35rem;
            margin-bottom: 1rem;
            box-shadow: 0 18px 55px rgba(0,0,0,0.26);
        }

        .eyebrow {
            color: var(--blue);
            font-size: 0.8rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }

        .hero-title {
            font-size: 1.95rem;
            line-height: 1.12;
            font-weight: 800;
            color: var(--text);
            margin: 0.25rem 0 0.35rem 0;
        }

        .hero-subtitle {
            color: var(--muted);
            font-size: 0.96rem;
            margin-bottom: 0;
            max-width: 72rem;
        }

        .metric-card {
            background: linear-gradient(180deg, rgba(20,31,42,0.95), rgba(12,18,27,0.95));
            border: 1px solid var(--line);
            border-radius: 16px;
            padding: 0.9rem 1rem;
            min-height: 104px;
        }

        .metric-label {
            color: var(--muted);
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            margin-bottom: 0.25rem;
        }

        .metric-value {
            color: var(--text);
            font-size: 1.55rem;
            font-weight: 800;
            line-height: 1.1;
            margin-bottom: 0.2rem;
        }

        .metric-note {
            color: var(--muted);
            font-size: 0.86rem;
        }

        .table-header {
            color: #dbe5f2;
            font-size: 0.76rem;
            font-weight: 800;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            padding: 0.55rem 0;
        }

        .cell {
            color: var(--text);
            font-size: 0.92rem;
            padding-top: 0.42rem;
            padding-bottom: 0.42rem;
            min-height: 2.3rem;
            display: flex;
            align-items: center;
        }

        .cell.ticker {
            font-weight: 800;
            letter-spacing: 0.02em;
        }

        .subtext {
            color: var(--muted);
            font-size: 0.76rem;
        }

        .value-green {
            color: var(--green);
            font-weight: 700;
        }

        .value-red {
            color: var(--red);
            font-weight: 700;
        }

        .value-orange {
            color: var(--orange);
            font-weight: 800;
        }

        .grade-badge {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-width: 2.1rem;
            padding: 0.23rem 0.55rem;
            border-radius: 999px;
            font-size: 0.8rem;
            font-weight: 800;
        }

        .grade-a { color: #062514; background: rgba(61,220,151,0.95); }
        .grade-b { color: #211100; background: rgba(255,180,84,0.95); }
        .grade-c { color: #290707; background: rgba(255,107,107,0.95); }

        .breaker-badge {
            display: inline-flex;
            align-items: center;
            padding: 0.2rem 0.55rem;
            border-radius: 999px;
            background: rgba(255,180,84,0.14);
            color: var(--orange);
            font-size: 0.74rem;
            font-weight: 800;
            margin-top: 0.2rem;
        }

        .catalyst-wrap {
            display: block;
            width: 100%;
            line-height: 1.3;
        }

        .catalyst-summary {
            color: var(--text);
            font-size: 0.9rem;
            font-weight: 600;
            margin-bottom: 0.18rem;
        }

        .reasoning-line {
            color: var(--muted);
            font-size: 0.77rem;
            margin-top: 0.16rem;
            line-height: 1.35;
        }

        .reasoning-label {
            color: #e6eef8;
            font-weight: 700;
        }

        .news-link, .news-link:visited {
            color: #c9d8ea;
            text-decoration: none;
        }

        .news-link:hover {
            color: #ffffff;
            text-decoration: underline;
        }

        .news-line {
            color: var(--muted);
            font-size: 0.76rem;
            margin-top: 0.12rem;
        }

        .sidebar-watch-card {
            background: rgba(255,255,255,0.03);
            border: 1px solid rgba(255,255,255,0.05);
            border-radius: 12px;
            padding: 0.7rem 0.8rem;
            margin-bottom: 0.45rem;
        }

        .sidebar-watch-ticker {
            font-weight: 800;
            color: var(--text);
        }

        .sidebar-watch-meta {
            color: var(--muted);
            font-size: 0.78rem;
            margin-top: 0.1rem;
        }

        .warning-note {
            background: rgba(255,180,84,0.12);
            border: 1px solid rgba(255,180,84,0.22);
            border-radius: 12px;
            color: #ffd9a0;
            padding: 0.85rem 1rem;
            font-size: 0.88rem;
        }

        /* Catalyst cell — new compact layout */
        .catalyst-tags {
            display: flex;
            gap: 0.35rem;
            align-items: center;
            margin-bottom: 0.3rem;
            flex-wrap: wrap;
        }

        .event-badge {
            display: inline-flex;
            align-items: center;
            padding: 0.15rem 0.5rem;
            border-radius: 5px;
            font-size: 0.7rem;
            font-weight: 800;
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }
        .event-ma       { background: rgba(139,92,246,0.2);  color: #c4b5fd; border: 1px solid rgba(139,92,246,0.35); }
        .event-earnings { background: rgba(59,130,246,0.2);  color: #93c5fd; border: 1px solid rgba(59,130,246,0.35); }
        .event-clinical { background: rgba(16,185,129,0.2);  color: #6ee7b7; border: 1px solid rgba(16,185,129,0.35); }
        .event-commercial { background: rgba(245,158,11,0.2); color: #fcd34d; border: 1px solid rgba(245,158,11,0.35); }
        .event-product  { background: rgba(14,165,233,0.2);  color: #7dd3fc; border: 1px solid rgba(14,165,233,0.35); }
        .event-analyst  { background: rgba(99,102,241,0.2);  color: #a5b4fc; border: 1px solid rgba(99,102,241,0.35); }
        .event-financing { background: rgba(239,68,68,0.18); color: #fca5a5; border: 1px solid rgba(239,68,68,0.3); }
        .event-legal    { background: rgba(234,179,8,0.18);  color: #fde047; border: 1px solid rgba(234,179,8,0.3); }
        .event-default  { background: rgba(148,163,184,0.12); color: #94a3b8; border: 1px solid rgba(148,163,184,0.2); }

        .dir-badge {
            display: inline-flex;
            align-items: center;
            padding: 0.15rem 0.45rem;
            border-radius: 5px;
            font-size: 0.7rem;
            font-weight: 700;
        }
        .dir-up    { background: rgba(61,220,151,0.12); color: var(--green); }
        .dir-down  { background: rgba(255,107,107,0.12); color: var(--red); }
        .dir-mixed { background: rgba(148,163,184,0.1); color: #94a3b8; }

        .catalyst-headline {
            font-size: 0.88rem;
            font-weight: 500;
            color: var(--text);
            line-height: 1.35;
            margin-bottom: 0.15rem;
        }

        .catalyst-stats {
            font-size: 0.75rem;
            color: var(--muted);
            margin-top: 0.18rem;
            margin-bottom: 0.18rem;
            letter-spacing: 0.01em;
        }

        .catalyst-edge {
            font-size: 0.82rem;
            color: #c8d8ec;
            font-weight: 500;
            margin-top: 0.22rem;
            line-height: 1.35;
            border-left: 2px solid rgba(59,130,246,0.4);
            padding-left: 0.45rem;
        }

        /* ── LLM analyst breakdown ─────────────────────────── */

        /* Catalyst quality badge */
        .cq-badge {
            display: inline-flex;
            align-items: center;
            padding: 0.18rem 0.52rem;
            border-radius: 999px;
            font-size: 0.68rem;
            font-weight: 800;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            margin-left: 0.35rem;
        }
        .cq-high    { background: rgba(61,220,151,0.15); color: #3ddc97; border: 1px solid rgba(61,220,151,0.3); }
        .cq-medium  { background: rgba(255,180,84,0.12); color: #ffb454; border: 1px solid rgba(255,180,84,0.3); }
        .cq-low     { background: rgba(255,107,107,0.12); color: #ff6b6b; border: 1px solid rgba(255,107,107,0.25); }

        .cq-reason {
            font-size: 0.77rem;
            color: #8fa0b5;
            font-style: italic;
            margin-top: 0.1rem;
            margin-bottom: 0.18rem;
            line-height: 1.35;
        }

        /* Story */
        .catalyst-story {
            font-size: 0.85rem;
            color: #d4e2f0;
            line-height: 1.5;
            margin-top: 0.22rem;
            margin-bottom: 0.2rem;
        }

        /* Mechanism */
        .catalyst-mechanism {
            font-size: 0.79rem;
            color: #7ba8d4;
            line-height: 1.4;
            margin-bottom: 0.22rem;
            padding-left: 0.5rem;
            border-left: 2px solid rgba(59,130,246,0.35);
        }

        /* Bull / Bear grid */
        .bull-bear-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0.4rem;
            margin-top: 0.2rem;
        }

        .bb-block {
            border-radius: 7px;
            padding: 0.32rem 0.48rem;
        }

        .bb-block.bull { background: rgba(61,220,151,0.07); border: 1px solid rgba(61,220,151,0.18); }
        .bb-block.bear { background: rgba(255,107,107,0.07); border: 1px solid rgba(255,107,107,0.18); }

        .bb-label {
            font-size: 0.67rem;
            font-weight: 800;
            letter-spacing: 0.07em;
            text-transform: uppercase;
            display: block;
            margin-bottom: 0.18rem;
        }
        .bb-label.bull { color: var(--green); }
        .bb-label.bear { color: var(--red); }

        .bb-item {
            font-size: 0.78rem;
            color: #b8cfe4;
            line-height: 1.4;
            padding-left: 0.75rem;
            position: relative;
            margin-bottom: 0.14rem;
        }
        .bb-item::before {
            content: "•";
            position: absolute;
            left: 0;
            color: #5a7a99;
        }

        /* Trade structure */
        .trade-structure-row {
            display: flex;
            align-items: flex-start;
            gap: 0.4rem;
            margin-top: 0.28rem;
        }
        .ts-badge {
            display: inline-flex;
            align-items: center;
            padding: 0.18rem 0.52rem;
            border-radius: 999px;
            font-size: 0.68rem;
            font-weight: 800;
            letter-spacing: 0.04em;
            white-space: nowrap;
            flex-shrink: 0;
        }
        .ts-go     { background: rgba(61,220,151,0.13); color: #3ddc97; border: 1px solid rgba(61,220,151,0.28); }
        .ts-fade   { background: rgba(255,107,107,0.12); color: #ff6b6b; border: 1px solid rgba(255,107,107,0.25); }
        .ts-multi  { background: rgba(59,130,246,0.12); color: #7ab3f5; border: 1px solid rgba(59,130,246,0.28); }
        .ts-reason {
            font-size: 0.77rem;
            color: #8fa0b5;
            line-height: 1.38;
        }

        /* Re-rating */
        .catalyst-rerating {
            margin-top: 0.28rem;
            padding: 0.28rem 0.52rem;
            border-radius: 6px;
            font-size: 0.8rem;
            line-height: 1.42;
            display: flex;
            gap: 0.4rem;
            align-items: flex-start;
        }
        .rerating-label {
            font-weight: 800;
            font-size: 0.68rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            white-space: nowrap;
            padding-top: 0.05rem;
            flex-shrink: 0;
        }
        .rerating-yes     { border: 1px solid rgba(61,220,151,0.28); background: rgba(61,220,151,0.07); }
        .rerating-yes     .rerating-label { color: #3ddc97; }
        .rerating-yes     .rerating-reason-text { color: #b0e8ce; }
        .rerating-no      { border: 1px solid rgba(148,163,184,0.18); background: rgba(148,163,184,0.04); }
        .rerating-no      .rerating-label { color: #64748b; }
        .rerating-no      .rerating-reason-text { color: #8fa0b5; }
        .rerating-partial { border: 1px solid rgba(255,180,84,0.25); background: rgba(255,180,84,0.07); }
        .rerating-partial .rerating-label { color: #ffb454; }
        .rerating-partial .rerating-reason-text { color: #f5c97a; }

        .rerating-reason-text { font-size: 0.8rem; line-height: 1.42; }

        .no-api-note {
            font-size: 0.74rem;
            color: #4a6a88;
            margin-top: 0.18rem;
            font-style: italic;
        }

        /* ── Deep-dive expander sections ─────────────────────── */
        .deep-section {
            margin-bottom: 0.9rem;
        }
        .deep-label {
            font-size: 0.68rem;
            font-weight: 800;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #7ba8d4;
            margin-bottom: 0.28rem;
            display: flex;
            align-items: center;
            gap: 0.35rem;
        }
        .deep-label::after {
            content: "";
            flex: 1;
            height: 1px;
            background: rgba(255,255,255,0.06);
        }
        .deep-text {
            font-size: 0.86rem;
            color: #ccdaec;
            line-height: 1.55;
        }
        .deep-text-muted {
            font-size: 0.83rem;
            color: #8fa0b5;
            line-height: 1.5;
        }
        .deep-story-before {
            font-size: 0.83rem;
            color: #6b8299;
            line-height: 1.5;
            padding: 0.3rem 0.55rem;
            border-left: 2px solid rgba(148,163,184,0.2);
            margin-bottom: 0.3rem;
        }
        .deep-story-after {
            font-size: 0.87rem;
            color: #d4e2f0;
            line-height: 1.55;
            padding: 0.3rem 0.55rem;
            border-left: 2px solid rgba(59,130,246,0.4);
        }
        .deep-float-block {
            font-size: 0.84rem;
            color: #b8cfe4;
            line-height: 1.5;
            padding: 0.35rem 0.55rem;
            background: rgba(59,130,246,0.06);
            border-radius: 6px;
            border: 1px solid rgba(59,130,246,0.14);
        }
        .deep-uncertainty {
            font-size: 0.85rem;
            color: #f5c97a;
            line-height: 1.5;
            padding: 0.35rem 0.6rem;
            background: rgba(255,180,84,0.07);
            border-radius: 6px;
            border: 1px solid rgba(255,180,84,0.2);
        }
        .deep-dilution {
            font-size: 0.83rem;
            color: #ff9f9f;
            line-height: 1.5;
            padding: 0.3rem 0.55rem;
            background: rgba(255,107,107,0.06);
            border-radius: 6px;
            border: 1px solid rgba(255,107,107,0.15);
        }
        .deep-dilution.safe {
            color: #8fa0b5;
            background: rgba(148,163,184,0.04);
            border-color: rgba(148,163,184,0.12);
        }
        .deep-entry {
            font-size: 0.84rem;
            color: #b0e8ce;
            line-height: 1.5;
            padding: 0.35rem 0.55rem;
            background: rgba(61,220,151,0.06);
            border-radius: 6px;
            border: 1px solid rgba(61,220,151,0.15);
            margin-bottom: 0.4rem;
        }
        .deep-invalidation {
            font-size: 0.84rem;
            color: #ffb4b4;
            line-height: 1.5;
            padding: 0.35rem 0.55rem;
            background: rgba(255,107,107,0.06);
            border-radius: 6px;
            border: 1px solid rgba(255,107,107,0.15);
        }
        .durability-badge {
            display: inline-flex;
            align-items: center;
            padding: 0.18rem 0.52rem;
            border-radius: 999px;
            font-size: 0.68rem;
            font-weight: 800;
            letter-spacing: 0.04em;
            margin-right: 0.4rem;
            background: rgba(59,130,246,0.12);
            color: #7ab3f5;
            border: 1px solid rgba(59,130,246,0.25);
        }
        .bb-block-wide { border-radius: 8px; padding: 0.5rem 0.65rem; margin-bottom: 0.2rem; }
        .bb-item-wide {
            font-size: 0.84rem;
            color: #c8d8ec;
            line-height: 1.5;
            padding: 0.2rem 0 0.2rem 1rem;
            position: relative;
        }
        .bb-item-wide::before { content: "•"; position: absolute; left: 0; color: #4a6a88; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def now_et() -> datetime:
    return datetime.now(NEW_YORK_TZ)


def market_session_status() -> tuple[str, bool]:
    current = now_et().time()
    if time(4, 0) <= current < time(9, 30):
        return "US Premarket", True
    if time(9, 30) <= current < time(16, 0):
        return "US Regular Session", False
    if time(16, 0) <= current < time(20, 0):
        return "US After Hours", False
    return "US Market Closed", False


def compact_number(value: Any, suffix: str = "") -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/A"
    number = float(value)
    abs_number = abs(number)
    if abs_number >= 1_000_000_000:
        return f"{number / 1_000_000_000:.2f}B{suffix}"
    if abs_number >= 1_000_000:
        return f"{number / 1_000_000:.2f}M{suffix}"
    if abs_number >= 1_000:
        return f"{number / 1_000:.1f}K{suffix}"
    if number.is_integer():
        return f"{int(number)}{suffix}"
    return f"{number:.2f}{suffix}"


def format_price(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"${float(value):.2f}"


def format_ratio(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.2f}x"


def format_gap_html(value: Any) -> str:
    if value is None or pd.isna(value):
        return '<span class="cell">N/A</span>'
    css = "value-green" if float(value) >= 0 else "value-red"
    return f'<span class="{css}">{float(value):+.2f}%</span>'


def grade_html(grade: str) -> str:
    css = {"A": "grade-a", "B": "grade-b", "C": "grade-c"}.get(grade, "grade-b")
    return f'<span class="grade-badge {css}">{html.escape(grade)}</span>'


def clean_headline(headline: str, ticker: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(headline or "")).strip()
    cleaned = re.sub(rf"^{re.escape(ticker)}\s*[:\-]\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*\([^)]*\)\s*$", "", cleaned)
    return cleaned.rstrip(". ")


def get_company_descriptor(ticker: str, industry: str | None, sector: str | None) -> str:
    if industry and str(industry).strip():
        return f"this {str(industry).lower()} name"
    if sector and str(sector).strip():
        return f"this {str(sector).lower()} stock"
    return ticker


def get_float_descriptor(float_shares: Any) -> str:
    if float_shares is None or pd.isna(float_shares):
        return "an unclear float profile"
    float_shares = float(float_shares)
    if float_shares < 5_000_000:
        return "a very tight float"
    if float_shares < 20_000_000:
        return "a low float"
    if float_shares < 100_000_000:
        return "a mid-sized float"
    return "a large float"


def classify_headline(primary: str) -> tuple[str, str]:
    lowered = primary.lower()
    positive_keywords = (
        "acquire",
        "acquisition",
        "merger",
        "takeover",
        "buyout",
        "partnership",
        "agreement",
        "deal",
        "contract",
        "lease",
        "approval",
        "fda",
        "launch",
        "wins",
        "award",
        "grant",
        "beat",
        "raises",
        "ai chip",
    )
    negative_keywords = (
        "offering",
        "dilution",
        "priced",
        "bankruptcy",
        "miss",
        "guidance cut",
        "cuts guidance",
        "withdrawn",
        "delay",
        "investigation",
        "lawsuit",
        "downgrade",
        "resign",
        "going concern",
        "delist",
    )
    event_map = [
        (("acquire", "acquisition", "merger", "takeover", "buyout"), "M&A"),
        (("earnings", "guidance", "revenue", "profit", "forecast", "result", "results"), "Earnings"),
        (("fda", "approval", "trial", "phase", "clinical", "study"), "Clinical"),
        (("partnership", "agreement", "deal", "contract", "lease", "award"), "Commercial"),
        (("launch", "chip", "platform", "product", "ai"), "Product"),
        (("upgrade", "downgrade", "rating", "price target"), "Analyst"),
        (("offering", "financing", "private placement"), "Financing"),
        (("lawsuit", "probe", "investigation"), "Legal"),
    ]
    event_label = "Headline"
    for keywords, label in event_map:
        if any(keyword in lowered for keyword in keywords):
            event_label = label
            break

    has_positive = any(keyword in lowered for keyword in positive_keywords)
    has_negative = any(keyword in lowered for keyword in negative_keywords)
    if has_negative and not has_positive:
        direction = "negative"
    elif has_positive and not has_negative:
        direction = "positive"
    else:
        direction = "mixed"
    return event_label, direction


def clean_metric(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "-":
        return None
    return text


def parse_metric_number(value: Any) -> float | None:
    text = clean_metric(value)
    if text is None:
        return None
    text = text.replace(",", "")
    multiplier = 1.0
    if text.endswith("%"):
        text = text[:-1]
    elif text.endswith("B"):
        multiplier = 1_000_000_000
        text = text[:-1]
    elif text.endswith("M"):
        multiplier = 1_000_000
        text = text[:-1]
    elif text.endswith("K"):
        multiplier = 1_000
        text = text[:-1]
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def build_fundamental_context(fundamentals: dict[str, Any]) -> tuple[str, str]:
    sales_qoq = clean_metric(fundamentals.get("Sales Q/Q"))
    eps_qoq = clean_metric(fundamentals.get("EPS Q/Q"))
    profit_margin = clean_metric(fundamentals.get("Profit Margin"))
    debt_eq = clean_metric(fundamentals.get("Debt/Eq"))
    market_cap = clean_metric(fundamentals.get("Market Cap"))
    cash_per_share = clean_metric(fundamentals.get("Cash/sh"))

    context_parts = []
    if market_cap:
        context_parts.append(f"{market_cap} market cap")
    if sales_qoq:
        context_parts.append(f"sales Q/Q {sales_qoq}")
    if eps_qoq:
        context_parts.append(f"EPS Q/Q {eps_qoq}")
    if profit_margin:
        context_parts.append(f"profit margin {profit_margin}")
    context = ", ".join(context_parts[:4]) if context_parts else "limited disclosed momentum"

    profit_margin_num = parse_metric_number(profit_margin)
    debt_eq_num = parse_metric_number(debt_eq)
    sales_qoq_num = parse_metric_number(sales_qoq)
    eps_qoq_num = parse_metric_number(eps_qoq)
    cash_num = parse_metric_number(cash_per_share)

    health_parts = []
    if profit_margin_num is not None:
        if profit_margin_num < 0:
            health_parts.append("loss-making")
        elif profit_margin_num > 10:
            health_parts.append("already profitable")
        else:
            health_parts.append("near break-even")
    if sales_qoq_num is not None:
        if sales_qoq_num > 25:
            health_parts.append("showing strong top-line acceleration")
        elif sales_qoq_num < 0:
            health_parts.append("still dealing with shrinking sales")
    if eps_qoq_num is not None:
        if eps_qoq_num > 25:
            health_parts.append("with improving earnings leverage")
        elif eps_qoq_num < 0:
            health_parts.append("with deteriorating earnings power")
    if debt_eq_num is not None:
        if debt_eq_num > 1:
            health_parts.append("and a leveraged balance sheet")
        elif debt_eq_num == 0:
            health_parts.append("and essentially no balance-sheet leverage")
    elif cash_num is not None and cash_num > 1:
        health_parts.append("with some cash cushion")

    if not health_parts:
        health = "with a still-developing fundamental profile"
    else:
        health = " ".join(health_parts)
    return context, health


def build_fundamental_change(
    event_label: str,
    direction: str,
    fundamentals: dict[str, Any],
    primary_headline: str,
) -> str:
    context, health = build_fundamental_context(fundamentals)

    if event_label == "M&A":
        return (
            f"On fundamentals, the key shift is from standalone execution risk to a defined deal-value setup. "
            f"This name comes in with {context} and is {health}, so an acquisition headline matters because it can override weak standalone forecasts and anchor valuation to the transaction terms."
        )
    if event_label == "Commercial":
        return (
            f"On fundamentals, the market is asking whether the news can translate into recurring revenue, not just attention. "
            f"With {context} and the business {health}, the partnership/contract only deserves a rerating if it improves revenue visibility, customer quality, or margin absorption."
        )
    if event_label == "Product":
        return (
            f"On fundamentals, the story is about monetization and TAM expansion. "
            f"With {context} and the company {health}, traders will watch whether this product news can lift future sales growth or improve the quality of gross profit rather than remain narrative-only."
        )
    if event_label == "Clinical":
        return (
            f"On fundamentals, this is a probability-of-cash-flow event. "
            f"Given {context} and a company that is {health}, the headline matters because it can materially change the odds of future commercialization, financing terms, or dilution risk."
        )
    if event_label == "Financing":
        if direction == "negative":
            return (
                f"On fundamentals, financing changes the runway but can hurt per-share value. "
                f"With {context} and the business {health}, the market will weigh extra survival time against dilution, cost of capital, and how far the company still is from self-funding."
            )
        return (
            f"On fundamentals, financing can de-risk near-term operations if the company needed capital. "
            f"With {context} and the business {health}, the real test is whether the fresh capital improves execution more than it dilutes ownership."
        )
    if event_label == "Earnings":
        if direction == "negative":
            return (
                f"On fundamentals, the market is de-rating forward earnings power. "
                f"The setup already showed {context} and the company is {health}, so weak earnings-type news pushes investors to pay less for future cash flow until growth or margins stabilize."
            )
        if direction == "mixed":
            return (
                f"On fundamentals, the market is refreshing its earnings model rather than reacting to a clean beat-or-miss headline. "
                f"The setup already showed {context} and the company is {health}, so traders will focus on what changed in margins, guidance credibility, and the path of future estimates."
            )
        return (
            f"On fundamentals, the market is repricing future earnings power higher. "
            f"The setup already showed {context} and the company is {health}, so strong earnings-type news matters if it supports a better sales trajectory, margin expansion, or cleaner guidance base."
        )
    if event_label == "Analyst":
        return (
            f"On fundamentals, analyst headlines do not change the business by themselves. "
            f"Still, with {context} and the company {health}, a rating change can pull new sponsorship into the name if it aligns with improving operating trends."
        )
    if event_label == "Legal":
        return (
            f"On fundamentals, legal or regulatory headlines change the discount rate more than the income statement at first. "
            f"With {context} and the company {health}, traders will assume a lower valuation multiple until the liability, cost, or operational impact becomes clearer."
        )
    return (
        f"On fundamentals, the news only matters if it changes future cash flows, funding risk, or valuation multiples. "
        f"Right now the company screens at {context} and is {health}, so the follow-through depends on whether the headline improves the business outlook instead of just attracting one-day volume."
    )


def get_market_cap_descriptor(market_cap_num: float | None) -> str:
    if market_cap_num is None:
        return "unclear-size"
    if market_cap_num < 100_000_000:
        return "micro-cap"
    if market_cap_num < 2_000_000_000:
        return "small-cap"
    if market_cap_num < 10_000_000_000:
        return "mid-cap"
    return "large-cap"


def get_fundamental_quality_bucket(fundamentals: dict[str, Any]) -> str:
    sales_qoq = parse_metric_number(fundamentals.get("Sales Q/Q"))
    eps_qoq = parse_metric_number(fundamentals.get("EPS Q/Q"))
    profit_margin = parse_metric_number(fundamentals.get("Profit Margin"))
    debt_eq = parse_metric_number(fundamentals.get("Debt/Eq"))

    if profit_margin is not None and profit_margin > 10 and (sales_qoq is None or sales_qoq >= 10):
        return "quality"
    if profit_margin is not None and profit_margin >= 0:
        return "profitable"
    if profit_margin is not None and profit_margin < 0 and sales_qoq is not None and sales_qoq > 25:
        return "speculative-growth"
    if profit_margin is not None and profit_margin < 0 and sales_qoq is not None and sales_qoq < 0:
        return "weak"
    if debt_eq is not None and debt_eq > 1.5:
        return "leveraged"
    return "mixed"


def build_repricing_frame(event_label: str, direction: str, grade: str) -> tuple[str, str]:
    if event_label == "M&A":
        return "standalone execution and valuation uncertainty", "transaction value and deal-completion odds"
    if event_label == "Commercial":
        return "speculation about future demand", "revenue visibility and customer quality"
    if event_label == "Product":
        return "narrative optionality", "adoption, monetization, and TAM credibility"
    if event_label == "Clinical":
        return "binary science risk", "probability-adjusted commercialization value"
    if event_label == "Financing" and direction == "negative":
        return "upside optionality", "dilution math, runway, and cost of capital"
    if event_label == "Financing":
        return "survival risk", "runway extension and execution capacity"
    if event_label == "Earnings" and direction == "negative":
        return "old estimates and prior multiple support", "lower forward earnings power and multiple compression"
    if event_label == "Earnings" and direction == "positive":
        return "old estimates", "higher forward earnings power and a revised guidance base"
    if event_label == "Earnings":
        return "old estimates and assumptions", "fresh evidence about earnings power and guidance credibility"
    if event_label == "Legal":
        return "business execution", "liability risk and a higher discount rate"
    if event_label == "Analyst":
        return "an underfollowed setup", "potential sponsorship and re-anchored expectations"
    if direction == "positive" and grade == "C":
        return "a broken chart and low confidence", "possible change-of-character if the catalyst proves durable"
    if direction == "positive":
        return "background momentum", "a more explicit catalyst-backed repricing"
    if direction == "negative":
        return "prior confidence", "a more defensive valuation framework"
    return "relative neglect", "active price discovery"


def build_positioning_note(
    row: dict[str, Any],
    fundamentals: dict[str, Any],
    direction: str,
    market_cap_desc: str,
) -> str:
    short_float = parse_metric_number(fundamentals.get("Short Float"))
    float_shares = row.get("float_shares_outstanding_current")

    notes = []
    if float_shares is not None and not pd.isna(float_shares):
        float_shares = float(float_shares)
        if float_shares < 20_000_000:
            notes.append("The lower float means the tape can overshoot the fundamental change intraday")
        elif market_cap_desc == "large-cap":
            notes.append("Because this is a larger cap, the move needs real sponsorship rather than just chat-room momentum")

    if short_float is not None and short_float >= 12:
        if direction == "positive":
            notes.append("Elevated short interest adds squeeze fuel on top of genuine buying")
        else:
            notes.append("Elevated short interest can soften the downside later if shorts start harvesting gains")
    elif short_float is not None and short_float <= 3 and direction == "positive":
        notes.append("The move will need fresh longs because there is not much short-covering fuel behind it")

    if not notes:
        return ""
    return ". ".join(notes) + "."


def build_durability_note(
    event_label: str,
    direction: str,
    quality_bucket: str,
    grade: str,
    fundamentals: dict[str, Any],
) -> str:
    debt_eq = parse_metric_number(fundamentals.get("Debt/Eq"))
    cash_sh = parse_metric_number(fundamentals.get("Cash/sh"))

    if direction == "positive":
        if event_label in {"Commercial", "Product"} and quality_bucket in {"weak", "speculative-growth", "mixed"}:
            return (
                "Durability now depends on whether traders believe this headline can turn into signed revenue or a real estimate revision; otherwise it behaves like attention without conversion."
            )
        if event_label == "M&A":
            return (
                "If deal terms and closing odds look clean, upside should start trading off transaction math instead of pure momentum, which usually makes the move more orderly but less open-ended."
            )
        if event_label == "Clinical":
            return (
                "Durability depends on whether the market reads this as reducing future financing risk and increasing commercialization odds, not just as a one-session biotech headline."
            )
        if quality_bucket == "quality":
            return (
                "Because the business already had real operating quality, the bar for holding gains is lower: the market only needs to believe the catalyst meaningfully improves the next few quarters."
            )
        if grade == "C":
            return (
                "Because this is landing on a previously weak chart, the stock still has to prove the move is a regime change and not just trapped shorts plus first-hour momentum."
            )
        return (
            "Durability depends on whether buyers start underwriting higher forward numbers rather than just paying up for the first reaction."
        )

    if event_label == "Financing":
        return (
            "The real question is whether the added runway outweighs the hit to per-share value; if not, the market will keep treating rallies as exits."
        )
    if event_label == "Legal":
        return (
            "Until the liability and operating impact are quantifiable, investors usually demand a lower multiple, so stabilization tends to lag the first headline."
        )
    if quality_bucket in {"quality", "profitable"} and ((debt_eq is None or debt_eq < 0.7) or (cash_sh is not None and cash_sh > 1)):
        return (
            "Stronger underlying fundamentals can eventually attract dip buyers, but usually only after the market has fully repriced the new risk."
        )
    return (
        "With weaker underlying support, the market does not have much reason to defend the old valuation until a new floor is discovered."
    )


def build_what_changes(
    row: dict[str, Any],
    event_label: str,
    direction: str,
    grade: str,
    gap_pct: float,
    volume_note: str,
    fundamentals: dict[str, Any],
) -> str:
    market_cap_num = parse_metric_number(fundamentals.get("Market Cap"))
    market_cap_desc = get_market_cap_descriptor(market_cap_num)
    quality_bucket = get_fundamental_quality_bucket(fundamentals)
    from_frame, to_frame = build_repricing_frame(event_label, direction, grade)
    positioning_note = build_positioning_note(row, fundamentals, direction, market_cap_desc)
    durability_note = build_durability_note(event_label, direction, quality_bucket, grade, fundamentals)
    gap_side = "gap-up" if gap_pct >= 0 else "gap-down"

    opening_sentence = (
        f"The market is reframing this {market_cap_desc} {gap_side} from {from_frame} to {to_frame}."
    )
    parts = [opening_sentence]
    if positioning_note:
        parts.append(positioning_note)
    parts.append(durability_note)
    parts.append(volume_note)
    return " ".join(parts)


def build_ai_reasoning(
    row: dict[str, Any],
    news_payload: dict[str, Any],
    volume_ratio: float | None,
    is_volume_breaker: bool,
) -> dict[str, str]:
    ticker = row["ticker"]
    headlines = news_payload.get("headlines", [])
    primary = clean_headline(headlines[0]["title"], ticker) if headlines else ""
    event_label, direction = classify_headline(primary) if primary else ("Headline", "mixed")
    descriptor = get_company_descriptor(ticker, news_payload.get("industry"), news_payload.get("sector"))
    float_note = get_float_descriptor(row.get("float_shares_outstanding_current"))
    fundamentals = news_payload.get("fundamentals", {})
    grade = compute_grade(row.get("display_price"), row.get("EMA10"), row.get("EMA20"), row.get("SMA50"))
    gap_pct = float(row.get("premarket_change") or 0.0)
    gap_side = "gap-up" if gap_pct >= 0 else "gap-down"

    if grade == "A":
        before = f"{descriptor} was already in a clean uptrend above EMA10, EMA20, and SMA50 with {float_note}."
    elif grade == "C":
        before = f"{descriptor} was under pressure below its key trend averages, so the chart was weak even before today's catalyst hit."
    else:
        before = f"{descriptor} was in a mixed or basing structure without full trend confirmation, with {float_note}."

    if primary:
        news_hit = f"{event_label} catalyst. {primary}."
    else:
        news_hit = "No fresh Finviz headline is showing, so the move may be reacting to older information or pure order flow."

    fundamental_change = build_fundamental_change(event_label, direction, fundamentals, primary)

    if volume_ratio is None:
        volume_note = "Volume confirmation is still developing."
    elif is_volume_breaker:
        volume_note = (
            f"Premarket volume is already {volume_ratio:.2f}x of the stock's all-time daily record, "
            "so this is becoming a full tape reset rather than a normal headline spike."
        )
    elif volume_ratio >= 0.4:
        volume_note = (
            f"Premarket volume is already {volume_ratio:.2f}x of the stock's all-time daily record, "
            "which is strong enough to keep momentum traders involved if price holds."
        )
    else:
        volume_note = (
            f"Premarket volume is only {volume_ratio:.2f}x of the all-time daily record, "
            "so the move still needs follow-through after the open."
        )

    change = build_what_changes(
        row=row,
        event_label=event_label,
        direction=direction,
        grade=grade,
        gap_pct=gap_pct,
        volume_note=volume_note,
        fundamentals=fundamentals,
    )

    summary = (
        f"AI view: {event_label} catalyst driving a {abs(gap_pct):.1f}% {gap_side}; "
        f"{'high-conviction volume' if is_volume_breaker else 'watch for hold above the open'}."
    )
    return {
        "summary": summary,
        "before": before,
        "news_hit": news_hit,
        "fundamental_change": fundamental_change,
        "what_changes": change,
    }


@st.cache_data(ttl=5 * 60, show_spinner=False)
def llm_catalyst_analysis(
    ticker: str,
    sector: str,
    industry: str,
    market_cap: str,
    short_float_str: str,
    float_str: str,
    price_str: str,
    eps_qq: str,
    sales_qq: str,
    gross_margin: str,
    inst_own: str,
    insider_own: str,
    target_price: str,
    gap_pct: float,
    volume_ratio_str: str,
    is_volume_breaker: bool,
    grade: str,
    headline_1: str,
    headline_2: str,
) -> dict[str, Any]:
    """
    Comprehensive 14-field analyst breakdown via Claude Sonnet.
    Covers: why today, story before/after, mechanism, float analysis,
    bull/bear cases, key uncertainty, catalyst durability, dilution risk,
    trade structure, entry thesis, invalidation, and re-rating verdict.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        try:
            api_key = st.secrets.get("ANTHROPIC_API_KEY", "")
        except Exception:
            pass
    if not api_key:
        return {}

    try:
        client = anthropic.Anthropic(api_key=api_key)

        sf_note = ""
        if short_float_str:
            try:
                sf_val = float(short_float_str.replace("%", "").strip())
                if sf_val >= 30:
                    sf_note = "  ← VERY HIGH — forced-covering dynamics almost certain"
                elif sf_val >= 20:
                    sf_note = "  ← HIGH — squeeze risk present, monitor covering pressure"
                elif sf_val >= 10:
                    sf_note = "  ← Elevated — short covering adds buying pressure"
            except ValueError:
                pass

        ctx_lines = [
            f"Ticker: {ticker}",
            f"Sector / Industry: {sector or 'Unknown'} / {industry or 'Unknown'}",
            f"Current price: {price_str}" if price_str else None,
            f"Market cap: {market_cap}" if market_cap else None,
            f"Public float: {float_str}" if float_str else None,
            f"Short float: {short_float_str}{sf_note}" if short_float_str else None,
            f"Institutional ownership: {inst_own}" if inst_own else None,
            f"Insider ownership: {insider_own}" if insider_own else None,
            f"Analyst consensus target: {target_price}" if target_price else None,
            f"EPS Q/Q growth: {eps_qq}" if eps_qq else None,
            f"Sales Q/Q growth: {sales_qq}" if sales_qq else None,
            f"Gross margin: {gross_margin}" if gross_margin else None,
            f"Trend grade: {grade}  (A = EMA10>EMA20>SMA50 uptrend / B = mixed / C = downtrend)",
        ]
        ctx_block = "\n".join(c for c in ctx_lines if c)

        vol_lines = [
            f"Premarket gap: {gap_pct:+.2f}%",
            f"Volume vs all-time daily high: {volume_ratio_str}" if volume_ratio_str else None,
            "⚠ EXTREME TAPE — premarket volume is at or near the all-time daily record for this ticker." if is_volume_breaker else None,
        ]
        vol_block = "\n".join(v for v in vol_lines if v)

        hl_lines = [f"1. {headline_1}" if headline_1 else None,
                    f"2. {headline_2}" if headline_2 else None]
        headlines_block = "\n".join(h for h in hl_lines if h) or "No headlines available."

        prompt = f"""You are the lead analyst at a US prop trading desk. A stock just gapped premarket. Every trader on the desk is reading your analysis right now, before the open. Be specific. Be quantitative. No filler. Every sentence must justify its existence.

=== STOCK PROFILE ===
{ctx_block}

=== TAPE & VOLUME ===
{vol_block}

=== CATALYST HEADLINES ===
{headlines_block}

=== YOUR ANALYSIS — respond ONLY with valid JSON, no markdown fences, no preamble ===
{{
  "catalyst_quality": "High | Medium | Low",

  "catalyst_quality_reason": "Why this quality tier? Reference the specific headline, this stock's setup (float, short interest, sector, fundamentals), and state explicitly whether this changes future cash flows or is just noise. Compare to a typical catalyst for a {sector or 'comparable'} stock of this size.",

  "why_today": "What specifically triggered this gap TODAY? Connect the headline to the timing — earnings release, FDA calendar event, overnight news wire, analyst action, sector rotation, technical level break? If unclear or the headline is weak, say so explicitly.",

  "story_before": "2 sentences. What was the prevailing market narrative about {ticker} BEFORE this headline? What were investors pricing in — broken business, turnaround attempt, speculative binary event, steady compounder?",

  "story_after": "3-4 sentences. How does this headline change that narrative? What new mental model does the market need to adopt? Be specific about what gets repriced and why {ticker} deserves this reaction rather than peers in the sector.",

  "mechanism": "Name the PRIMARY market mechanism and explain the mechanics with numbers. If short squeeze: approximate days-to-cover at current volume, and explain forced-covering dynamics. If institutional rerating: what estimate or multiple changes. If retail momentum on low-float: explain supply/demand math. If sector sympathy: why was this name picked up and not peers. Use actual numbers from the profile above.",

  "float_analysis": "Deep analysis of the float in context of this specific move. For small float (<10M shares): explain the volatility math — buying pressure per share of float, what that means for move sustainability vs snap-back risk, and whether today's gap has already exhausted near-term float turnover. For larger floats: explain how float size limits or enables institutional participation in this move. Connect float explicitly to the gap magnitude ({gap_pct:+.2f}%) and the volume profile.",

  "bullish": [
    "Most important bull argument — quantified, tied directly to the actual news and this ticker's specific numbers",
    "Second bull argument — structural setup (float, short interest, trend grade) that amplifies the catalyst",
    "Third bull argument — a separate angle not already covered",
    "Fourth if genuinely distinct and strong",
    "Fifth only if there is a clearly separate fifth argument"
  ],

  "bearish": [
    "Most important bear risk — specific to this news and this stock's situation, not generic market risk",
    "Dilution/offering risk assessment — for micro/small caps: has this stock historically offered stock post-gap? Is a follow-on offering likely given the gap size and capital needs?",
    "Third specific bear risk",
    "Fourth if clearly present and distinct from the above"
  ],

  "key_uncertainty": "The SINGLE most important unknown that could make either bulls or bears completely wrong. This is the crux of the trade. If resolved bullishly, what changes? If resolved bearishly, what changes? Name it specifically.",

  "catalyst_durability": "Hours | 1-3 Days | 1-2 Weeks | Multi-week",

  "catalyst_durability_reason": "Why that duration? Name specifically what would extend it (follow-up catalyst, earnings revision cycle, FDA approval timeline) and what would cut it short (offering announcement, sector rotation, no follow-through on fundamentals).",

  "dilution_risk": "Specific assessment of secondary offering / ATM share sale risk. Consider market cap ({market_cap or 'unknown'}), gap magnitude ({gap_pct:+.2f}% — larger gaps create more incentive to raise), institutional ownership, and whether this stock type typically uses gaps as financing windows. Give a clear risk level (Low / Medium / High) with specific rationale. If mid/large-cap with strong balance sheet, state N/A and briefly why.",

  "trade_structure": "Gap-and-Go | Gap-and-Fade | Multi-Day Setup | Too Early to Call",

  "trade_structure_reason": "Reasoning using ALL available data: float size, short interest level, catalyst durability, volume character vs ATH, gap magnitude, and whether this type of catalyst in this sector historically sustains or fades intraday.",

  "entry_thesis": "When and how would a disciplined trader enter? Specify: what price action confirmation, what volume pattern, what time window, and what level. e.g. 'First 5-min candle close above the open on volume >2x the prior 5-min average' or 'Pullback to VWAP with a higher-low reversal candle in the first 30 minutes, with volume drying up on the flush.'",

  "invalidation": "What SPECIFIC price action, volume pattern, or news development invalidates the bull thesis? Name exact signals — e.g. 'Break below the opening 5-min candle low on heavy volume', 'Secondary offering announcement', 'Volume dries up below VWAP within first 15 minutes.' Not vague warnings.",

  "rerating": "Yes | Partial | No",

  "rerating_reason": "Does this force sell-side estimate revisions, change the valuation multiple, or reclassify the stock's risk profile? Or is it a sentiment event that fades when the news cycle moves on? Specify exactly what changes in the valuation framework, over what time horizon, and what evidence would confirm the re-rating is sticking."
}}

NON-NEGOTIABLE:
1. Every single field must be specific to {ticker} and this exact news. If it could apply to any other stock, rewrite it.
2. Use the actual numbers from the profile (float, short float %, market cap, gap %, EPS, etc.) throughout.
3. No hedging phrases. No "time will tell." No "investors should monitor." If you don't know, say you don't know.
4. bullish and bearish must be plain string arrays only.
5. Aim for maximum informational density — every word earns its place."""

        message = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=2500,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        parsed = json.loads(raw)

        return {
            "catalyst_quality":           str(parsed.get("catalyst_quality", "")).strip(),
            "catalyst_quality_reason":    str(parsed.get("catalyst_quality_reason", "")).strip(),
            "why_today":                  str(parsed.get("why_today", "")).strip(),
            "story_before":               str(parsed.get("story_before", "")).strip(),
            "story_after":                str(parsed.get("story_after", "")).strip(),
            "mechanism":                  str(parsed.get("mechanism", "")).strip(),
            "float_analysis":             str(parsed.get("float_analysis", "")).strip(),
            "bullish":                    [str(p) for p in parsed.get("bullish", [])],
            "bearish":                    [str(p) for p in parsed.get("bearish", [])],
            "key_uncertainty":            str(parsed.get("key_uncertainty", "")).strip(),
            "catalyst_durability":        str(parsed.get("catalyst_durability", "")).strip(),
            "catalyst_durability_reason": str(parsed.get("catalyst_durability_reason", "")).strip(),
            "dilution_risk":              str(parsed.get("dilution_risk", "")).strip(),
            "trade_structure":            str(parsed.get("trade_structure", "")).strip(),
            "trade_structure_reason":     str(parsed.get("trade_structure_reason", "")).strip(),
            "entry_thesis":               str(parsed.get("entry_thesis", "")).strip(),
            "invalidation":               str(parsed.get("invalidation", "")).strip(),
            "rerating":                   str(parsed.get("rerating", "")).strip(),
            "rerating_reason":            str(parsed.get("rerating_reason", "")).strip(),
        }
    except Exception:
        return {}


def build_trade_edge(
    event_label: str,
    direction: str,
    short_float: float | None,
    quality_bucket: str,
    grade: str,
    gap_pct: float,
    volume_ratio: float | None,
    is_volume_breaker: bool,
) -> str:
    """One punchy, specific sentence describing the trade setup."""
    float_squeeze = short_float is not None and short_float > 20
    vol_confirmed = is_volume_breaker or (volume_ratio is not None and volume_ratio >= 0.4)

    if event_label == "M&A":
        if direction == "positive":
            if float_squeeze:
                return f"Deal math sets a hard floor; {short_float:.1f}% short adds squeeze fuel if arbs pile in."
            return "Deal math sets a hard floor; upside limited to premium unless a competing bid emerges."
        return "Deal collapse — binary gap-down; watch for support at pre-announcement price."

    if event_label == "Clinical":
        if direction == "positive":
            if float_squeeze:
                return f"Positive data + {short_float:.1f}% short = squeeze on top of science; fades fast if volume dries."
            return "Hold durability depends on data depth, not just the headline — watch 30-min volume trend."
        return "Failed trial/FDA rejection — commercialization odds repriced; expect continued distribution."

    if event_label == "Earnings":
        if direction == "positive":
            if quality_bucket in ("quality", "profitable"):
                return "Beat on a profitable name — multiple expansion more likely; watch whether buy-side adds on dips."
            if float_squeeze:
                return f"Earnings pop + {short_float:.1f}% short — could turn into a squeeze; fade risk high if guidance is thin."
            return "Earnings beat on a spec name — sell-the-news risk if guidance doesn't support the gap."
        if direction == "negative":
            return "Miss — forward estimates need resetting; selling pressure typically extends past the first session."
        return "Mixed print — market parsing guidance vs beat/miss; expect choppy two-way price action."

    if event_label == "Commercial":
        if float_squeeze:
            return f"Contract/deal + {short_float:.1f}% short — news + squeeze potential; key is whether revenue terms are disclosed."
        return "Commercial deal: durable if contract size is material to revenue; fades if details stay vague."

    if event_label == "Financing":
        if direction == "negative":
            return "Dilutive offering — downside tied to deal size vs market cap; overhang persists until fully placed."
        return "Financing removes survival risk but shifts narrative to dilution math and burn rate."

    if event_label == "Analyst":
        if direction == "positive":
            return "Upgrade/PT raise — watch for institutional follow-through; retail-only moves fade by midday."
        return "Downgrade/PT cut — new PT becomes near-term resistance; gap-fill attempts likely cap out there."

    if event_label == "Legal":
        return "Legal catalyst — discount applied until liability size is quantifiable; avoid catching falling knives."

    if event_label == "Product":
        if quality_bucket in ("quality", "profitable"):
            return "Product launch on a profitable business — TAM expansion story has an earnings foundation."
        return "Pre-revenue product launch — narrative catalyst; watch 30-min VWAP for momentum durability."

    # Generic fallback
    if float_squeeze and is_volume_breaker:
        return f"{short_float:.1f}% short + near-record volume — high-risk momentum setup in both directions."
    if float_squeeze:
        return f"{short_float:.1f}% short float elevates squeeze risk above the catalyst itself."
    if grade == "A" and vol_confirmed:
        return "Clean uptrend + volume confirmed — lowest-resistance path is continuation; watch first pullback hold."
    if grade == "C":
        return "Gap on a downtrending chart — fade risk elevated; needs VWAP hold into midday to shift bias."
    return "Watch for volume confirmation at the open before committing to direction."


def build_trade_context(
    row: dict[str, Any],
    news_payload: dict[str, Any],
    volume_ratio: float | None,
    is_volume_breaker: bool,
) -> dict[str, Any]:
    ticker = row["ticker"]
    headlines = news_payload.get("headlines", [])
    primary = clean_headline(headlines[0]["title"], ticker) if headlines else ""
    secondary = clean_headline(headlines[1]["title"], ticker) if len(headlines) > 1 else ""
    event_label, direction = classify_headline(primary) if primary else ("Headline", "mixed")
    fundamentals = news_payload.get("fundamentals", {})
    grade = compute_grade(row.get("display_price"), row.get("EMA10"), row.get("EMA20"), row.get("SMA50"))
    gap_pct = float(row.get("premarket_change") or 0.0)
    quality_bucket = get_fundamental_quality_bucket(fundamentals)

    # Core identifiers / sizing
    short_float = parse_metric_number(fundamentals.get("Short Float"))
    short_float_str = f"{short_float:.1f}%" if short_float is not None else ""
    market_cap = clean_metric(fundamentals.get("Market Cap")) or ""
    sector = str(news_payload.get("sector") or "").strip()
    industry = str(news_payload.get("industry") or "").strip()

    # Float and price for LLM context
    float_shares = row.get("float_shares_outstanding_current")
    float_str = compact_number(float_shares, " shares") if float_shares and not pd.isna(float_shares) else ""
    price = row.get("display_price")
    price_str = format_price(price) if price and not pd.isna(price) else ""

    # Volume context
    volume_ratio_str = f"{volume_ratio:.2f}x all-time high" if volume_ratio is not None else ""

    # Fundamentals
    eps_qq = clean_metric(fundamentals.get("EPS Q/Q")) or ""
    sales_qq = clean_metric(fundamentals.get("Sales Q/Q")) or ""
    gross_margin = clean_metric(fundamentals.get("Gross Margin")) or ""
    inst_own = clean_metric(fundamentals.get("Inst Own")) or ""
    insider_own = clean_metric(fundamentals.get("Insider Own")) or ""
    target_price = clean_metric(fundamentals.get("Target Price")) or ""

    # Fallback edge sentence (used when no API key)
    edge = build_trade_edge(
        event_label=event_label,
        direction=direction,
        short_float=short_float,
        quality_bucket=quality_bucket,
        grade=grade,
        gap_pct=gap_pct,
        volume_ratio=volume_ratio,
        is_volume_breaker=is_volume_breaker,
    )

    # LLM analyst breakdown — requires ANTHROPIC_API_KEY env var
    llm = llm_catalyst_analysis(
        ticker=ticker,
        sector=sector,
        industry=industry,
        market_cap=market_cap,
        short_float_str=short_float_str,
        float_str=float_str,
        price_str=price_str,
        eps_qq=eps_qq,
        sales_qq=sales_qq,
        gross_margin=gross_margin,
        inst_own=inst_own,
        insider_own=insider_own,
        target_price=target_price,
        gap_pct=gap_pct,
        volume_ratio_str=volume_ratio_str,
        is_volume_breaker=is_volume_breaker,
        grade=grade,
        headline_1=primary,
        headline_2=secondary,
    )

    catalyst_summary = f"[{event_label}] {primary}" if primary else f"[{event_label}] No headline."
    return {
        "event_label": event_label,
        "direction": direction,
        "headlines": headlines,
        "short_float_str": short_float_str or None,
        "market_cap": market_cap or None,
        "sector": sector,
        "edge": edge,
        "catalyst_summary": catalyst_summary,
        # LLM fields (empty dict / defaults if no API key)
        "catalyst_quality":           llm.get("catalyst_quality", ""),
        "catalyst_quality_reason":    llm.get("catalyst_quality_reason", ""),
        "why_today":                  llm.get("why_today", ""),
        "story_before":               llm.get("story_before", ""),
        "story_after":                llm.get("story_after", ""),
        "mechanism":                  llm.get("mechanism", ""),
        "float_analysis":             llm.get("float_analysis", ""),
        "bullish":                    llm.get("bullish", []),
        "bearish":                    llm.get("bearish", []),
        "key_uncertainty":            llm.get("key_uncertainty", ""),
        "catalyst_durability":        llm.get("catalyst_durability", ""),
        "catalyst_durability_reason": llm.get("catalyst_durability_reason", ""),
        "dilution_risk":              llm.get("dilution_risk", ""),
        "trade_structure":            llm.get("trade_structure", ""),
        "trade_structure_reason":     llm.get("trade_structure_reason", ""),
        "entry_thesis":               llm.get("entry_thesis", ""),
        "invalidation":               llm.get("invalidation", ""),
        "rerating":                   llm.get("rerating", ""),
        "rerating_reason":            llm.get("rerating_reason", ""),
    }


def load_watchlist() -> list[dict[str, Any]]:
    if not WATCHLIST_PATH.exists():
        return []
    try:
        payload = json.loads(WATCHLIST_PATH.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return payload
    except (json.JSONDecodeError, OSError):
        pass
    return []


def save_watchlist(items: list[dict[str, Any]]) -> None:
    deduped: dict[str, dict[str, Any]] = {}
    for item in items:
        ticker = str(item.get("ticker", "")).upper().strip()
        if ticker:
            deduped[ticker] = {**item, "ticker": ticker}
    sorted_items = sorted(
        deduped.values(),
        key=lambda item: item.get("timestamp", ""),
        reverse=True,
    )
    WATCHLIST_PATH.write_text(json.dumps(sorted_items, indent=2), encoding="utf-8")


def add_watchlist_item(
    ticker: str,
    catalyst: str,
    price: float | None,
    gap_pct: float | None,
    source: str,
) -> bool:
    items = load_watchlist()
    by_ticker = {item["ticker"]: item for item in items if "ticker" in item}
    normalized = ticker.upper().strip()
    exists = normalized in by_ticker
    if exists and source == "auto-volume-breaker":
        return False
    by_ticker[normalized] = {
        "ticker": normalized,
        "timestamp": datetime.now().astimezone().isoformat(timespec="seconds"),
        "catalyst": catalyst,
        "price": price,
        "gap_pct": gap_pct,
        "source": source,
    }
    save_watchlist(list(by_ticker.values()))
    return not exists


def remove_watchlist_item(ticker: str) -> None:
    remaining = [item for item in load_watchlist() if item.get("ticker") != ticker]
    save_watchlist(remaining)


def export_watchlist_csv(items: list[dict[str, Any]]) -> bytes:
    if not items:
        return b"ticker,timestamp,catalyst,price,gap_pct,source\n"
    frame = pd.DataFrame(items)
    return frame.to_csv(index=False).encode("utf-8")


def tradingview_request_kwargs() -> dict[str, Any]:
    raw_cookies = os.getenv("TRADINGVIEW_COOKIES_JSON", "").strip()
    if not raw_cookies:
        return {}
    try:
        cookies = json.loads(raw_cookies)
    except json.JSONDecodeError:
        return {}
    return {"cookies": cookies}


@st.cache_data(ttl=12 * 60 * 60, show_spinner=False)
def fetch_all_time_high_volume(ticker: str) -> dict[str, Any]:
    try:
        history = yf.Ticker(ticker).history(period="max", auto_adjust=False)
        if history.empty or "Volume" not in history.columns:
            return {"all_time_high_volume": None}
        volumes = pd.to_numeric(history["Volume"], errors="coerce").dropna()
        if volumes.empty:
            return {"all_time_high_volume": None}
        return {"all_time_high_volume": int(volumes.max())}
    except Exception as exc:  # pragma: no cover
        return {"all_time_high_volume": None, "error": str(exc)}


@st.cache_data(ttl=5 * 60, show_spinner=False)
def fetch_news_bundle(ticker: str) -> dict[str, Any]:
    try:
        quote = finvizfinance(ticker)
        fundamentals = quote.ticker_fundament(raw=True, output_format="dict")
        description = quote.ticker_description()
        news = quote.ticker_news()
        base_payload = {
            "company": fundamentals.get("Company"),
            "sector": fundamentals.get("Sector"),
            "industry": fundamentals.get("Industry"),
            "description": description,
            "fundamentals": {
                "Market Cap": fundamentals.get("Market Cap"),
                "Shs Outstand": fundamentals.get("Shs Outstand"),
                "Short Float": fundamentals.get("Short Float"),
                "P/E": fundamentals.get("P/E"),
                "Forward P/E": fundamentals.get("Forward P/E"),
                "EPS next Y": fundamentals.get("EPS next Y"),
                "EPS next Y Percentage": fundamentals.get("EPS next Y Percentage"),
                "EPS Q/Q": fundamentals.get("EPS Q/Q"),
                "Sales Q/Q": fundamentals.get("Sales Q/Q"),
                "ROA": fundamentals.get("ROA"),
                "ROE": fundamentals.get("ROE"),
                "Gross Margin": fundamentals.get("Gross Margin"),
                "Oper. Margin": fundamentals.get("Oper. Margin"),
                "Profit Margin": fundamentals.get("Profit Margin"),
                "Debt/Eq": fundamentals.get("Debt/Eq"),
                "Cash/sh": fundamentals.get("Cash/sh"),
                "Book/sh": fundamentals.get("Book/sh"),
                "Inst Own": fundamentals.get("Inst Own"),
                "Insider Own": fundamentals.get("Insider Own"),
                "Target Price": fundamentals.get("Target Price"),
            },
        }
        if news is None or news.empty:
            return {**base_payload, "summary": "No fresh catalyst pulled from Finviz.", "headlines": []}

        headlines = []
        for row in news.head(2).itertuples(index=False):
            dt_value = getattr(row, "Date", None)
            dt_text = dt_value.strftime("%b %d %I:%M %p") if hasattr(dt_value, "strftime") else str(dt_value)
            headlines.append(
                {
                    "time": dt_text,
                    "title": str(getattr(row, "Title", "")),
                    "link": str(getattr(row, "Link", "")),
                    "source": str(getattr(row, "Source", "")),
                }
            )
        return {**base_payload, "summary": clean_headline(headlines[0]["title"], ticker), "headlines": headlines}
    except Exception as exc:  # pragma: no cover
        return {
            "summary": "Catalyst temporarily unavailable due to a Finviz request issue.",
            "headlines": [],
            "error": str(exc),
        }


def compute_grade(price: Any, ema10: Any, ema20: Any, sma50: Any) -> str:
    values = [price, ema10, ema20, sma50]
    if any(value is None or pd.isna(value) for value in values):
        return "B"
    price = float(price)
    ema10 = float(ema10)
    ema20 = float(ema20)
    sma50 = float(sma50)
    if price > ema10 > ema20 > sma50:
        return "A"
    if price < ema10 < ema20 < sma50:
        return "C"
    return "B"


def enrich_ticker(row: dict[str, Any]) -> dict[str, Any]:
    ticker = row["ticker"]
    current_volume = row["premarket_volume"] or 0

    volume_payload = fetch_all_time_high_volume(ticker)
    ath_volume = volume_payload.get("all_time_high_volume")
    ratio = None
    is_breaker = False
    if ath_volume and ath_volume > 0:
        ratio = current_volume / ath_volume
        is_breaker = ratio >= 0.8 or current_volume > ath_volume

    news_payload = fetch_news_bundle(ticker)
    context = build_trade_context(row, news_payload, ratio, is_breaker)
    return {
        "all_time_high_volume": ath_volume,
        "volume_ratio": ratio,
        "is_volume_breaker": is_breaker,
        "flag": "VOLUME BREAKER" if is_breaker else "",
        "catalyst_summary": context["catalyst_summary"],
        "event_label": context["event_label"],
        "direction": context["direction"],
        "headlines": context["headlines"],
        "short_float_str": context["short_float_str"],
        "market_cap": context["market_cap"],
        "sector": context["sector"],
        "edge": context["edge"],
        "catalyst_quality":           context["catalyst_quality"],
        "catalyst_quality_reason":    context["catalyst_quality_reason"],
        "why_today":                  context["why_today"],
        "story_before":               context["story_before"],
        "story_after":                context["story_after"],
        "mechanism":                  context["mechanism"],
        "float_analysis":             context["float_analysis"],
        "bullish":                    context["bullish"],
        "bearish":                    context["bearish"],
        "key_uncertainty":            context["key_uncertainty"],
        "catalyst_durability":        context["catalyst_durability"],
        "catalyst_durability_reason": context["catalyst_durability_reason"],
        "dilution_risk":              context["dilution_risk"],
        "trade_structure":            context["trade_structure"],
        "trade_structure_reason":     context["trade_structure_reason"],
        "entry_thesis":               context["entry_thesis"],
        "invalidation":               context["invalidation"],
        "rerating":                   context["rerating"],
        "rerating_reason":            context["rerating_reason"],
    }


def scan_premarket_gappers(
    min_gap_pct: float,
    min_volume: int,
    min_price: float,
    max_price: float,
    limit: int,
) -> pd.DataFrame:
    columns = [
        "name",
        "close",
        "premarket_change",
        "premarket_volume",
        "premarket_close",
        "float_shares_outstanding_current",
        "EMA10",
        "EMA20",
        "SMA50",
        "exchange",
        "type",
    ]

    query = (
        Query()
        .select(*columns)
        .where2(
            And(
                Or(Column("premarket_change") >= min_gap_pct, Column("premarket_change") <= -min_gap_pct),
                Column("premarket_change").not_empty(),
                Column("premarket_volume").not_empty(),
                Column("premarket_volume") >= int(min_volume),
                Column("premarket_close").between(min_price, max_price),
                Column("exchange").isin(EXCHANGES),
                Column("type") == "stock",
            )
        )
        .order_by("premarket_volume", ascending=False)
        .limit(limit)
    )

    _, raw = query.get_scanner_data(**tradingview_request_kwargs())
    if raw.empty:
        return pd.DataFrame()

    frame = raw.copy()
    frame["ticker"] = frame["ticker"].astype(str).str.split(":").str[-1]
    frame["premarket_change"] = pd.to_numeric(frame["premarket_change"], errors="coerce")
    frame["premarket_volume"] = pd.to_numeric(frame["premarket_volume"], errors="coerce").fillna(0).astype("int64")
    frame["premarket_close"] = pd.to_numeric(frame["premarket_close"], errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["float_shares_outstanding_current"] = pd.to_numeric(
        frame["float_shares_outstanding_current"],
        errors="coerce",
    )
    frame["EMA10"] = pd.to_numeric(frame["EMA10"], errors="coerce")
    frame["EMA20"] = pd.to_numeric(frame["EMA20"], errors="coerce")
    frame["SMA50"] = pd.to_numeric(frame["SMA50"], errors="coerce")
    frame["display_price"] = frame["premarket_close"].fillna(frame["close"])
    return frame


_ENRICHED_COLUMNS = [
    "all_time_high_volume", "volume_ratio", "is_volume_breaker", "flag",
    "catalyst_summary", "event_label", "direction", "headlines",
    "short_float_str", "market_cap", "sector", "edge",
    "catalyst_quality", "catalyst_quality_reason",
    "why_today", "story_before", "story_after",
    "mechanism", "float_analysis",
    "bullish", "bearish",
    "key_uncertainty",
    "catalyst_durability", "catalyst_durability_reason",
    "dilution_risk",
    "trade_structure", "trade_structure_reason",
    "entry_thesis", "invalidation",
    "rerating", "rerating_reason",
    "grade",
]


def enrich_scan(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        # Return an empty DataFrame that still has all expected columns so
        # downstream column accesses never raise KeyError.
        empty = frame.copy()
        for col in _ENRICHED_COLUMNS:
            if col not in empty.columns:
                empty[col] = pd.Series(dtype=object)
        return empty

    base_rows = [
        {
            "ticker": row.ticker,
            "premarket_volume": int(row.premarket_volume),
            "display_price": row.display_price,
            "EMA10": row.EMA10,
            "EMA20": row.EMA20,
            "SMA50": row.SMA50,
            "premarket_change": row.premarket_change,
            "float_shares_outstanding_current": row.float_shares_outstanding_current,
        }
        for row in frame.itertuples(index=False)
    ]
    enrichments: dict[str, dict[str, Any]] = {}

    workers = max(1, min(8, len(base_rows)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(enrich_ticker, row): row["ticker"] for row in base_rows}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                enrichments[ticker] = future.result()
            except Exception as exc:  # pragma: no cover
                enrichments[ticker] = {
                    "all_time_high_volume": None,
                    "volume_ratio": None,
                    "is_volume_breaker": False,
                    "flag": "",
                    "catalyst_summary": f"Enrichment failed: {exc}",
                    "event_label": "Headline",
                    "direction": "mixed",
                    "headlines": [],
                    "short_float_str": None,
                    "market_cap": None,
                    "sector": "",
                    "edge": "",
                    "catalyst_quality": "",
                    "catalyst_quality_reason": "",
                    "why_today": "",
                    "story_before": "",
                    "story_after": "",
                    "mechanism": "",
                    "float_analysis": "",
                    "bullish": [],
                    "bearish": [],
                    "key_uncertainty": "",
                    "catalyst_durability": "",
                    "catalyst_durability_reason": "",
                    "dilution_risk": "",
                    "trade_structure": "",
                    "trade_structure_reason": "",
                    "entry_thesis": "",
                    "invalidation": "",
                    "rerating": "",
                    "rerating_reason": "",
                }

    enriched = frame.copy()
    enriched["all_time_high_volume"] = enriched["ticker"].map(
        lambda ticker: enrichments.get(ticker, {}).get("all_time_high_volume")
    )
    enriched["volume_ratio"] = enriched["ticker"].map(
        lambda ticker: enrichments.get(ticker, {}).get("volume_ratio")
    )
    enriched["is_volume_breaker"] = enriched["ticker"].map(
        lambda ticker: enrichments.get(ticker, {}).get("is_volume_breaker", False)
    )
    enriched["flag"] = enriched["ticker"].map(lambda ticker: enrichments.get(ticker, {}).get("flag", ""))
    enriched["catalyst_summary"] = enriched["ticker"].map(
        lambda ticker: enrichments.get(ticker, {}).get("catalyst_summary", "No catalyst.")
    )
    enriched["event_label"] = enriched["ticker"].map(
        lambda ticker: enrichments.get(ticker, {}).get("event_label", "Headline")
    )
    enriched["direction"] = enriched["ticker"].map(
        lambda ticker: enrichments.get(ticker, {}).get("direction", "mixed")
    )
    enriched["headlines"] = enriched["ticker"].map(
        lambda ticker: enrichments.get(ticker, {}).get("headlines", [])
    )
    enriched["short_float_str"] = enriched["ticker"].map(
        lambda ticker: enrichments.get(ticker, {}).get("short_float_str")
    )
    enriched["market_cap"] = enriched["ticker"].map(
        lambda ticker: enrichments.get(ticker, {}).get("market_cap")
    )
    enriched["sector"] = enriched["ticker"].map(
        lambda ticker: enrichments.get(ticker, {}).get("sector", "")
    )
    enriched["edge"] = enriched["ticker"].map(
        lambda ticker: enrichments.get(ticker, {}).get("edge", "")
    )
    for col, default in [
        ("catalyst_quality", ""),
        ("catalyst_quality_reason", ""),
        ("why_today", ""),
        ("story_before", ""),
        ("story_after", ""),
        ("mechanism", ""),
        ("float_analysis", ""),
        ("bullish", []),
        ("bearish", []),
        ("key_uncertainty", ""),
        ("catalyst_durability", ""),
        ("catalyst_durability_reason", ""),
        ("dilution_risk", ""),
        ("trade_structure", ""),
        ("trade_structure_reason", ""),
        ("entry_thesis", ""),
        ("invalidation", ""),
        ("rerating", ""),
        ("rerating_reason", ""),
    ]:
        _col, _def = col, default
        enriched[_col] = enriched["ticker"].map(
            lambda t, c=_col, d=_def: enrichments.get(t, {}).get(c, d)
        )
    enriched["grade"] = enriched.apply(
        lambda row: compute_grade(row["display_price"], row["EMA10"], row["EMA20"], row["SMA50"]),
        axis=1,
    )
    enriched["abs_gap"] = enriched["premarket_change"].abs()
    enriched["sort_ratio"] = enriched["volume_ratio"].fillna(-1)
    return enriched.sort_values(
        by=["is_volume_breaker", "sort_ratio", "abs_gap", "premarket_volume"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)


def apply_filters(frame: pd.DataFrame, min_float_shares: float, volume_breaker_only: bool) -> pd.DataFrame:
    filtered = frame.copy()
    if min_float_shares > 0:
        filtered = filtered[filtered["float_shares_outstanding_current"].fillna(-1) >= min_float_shares]
    if volume_breaker_only:
        filtered = filtered[filtered["is_volume_breaker"]]
    return filtered.reset_index(drop=True)


def sync_auto_watchlist(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0

    added = 0
    for row in frame[frame["is_volume_breaker"]].itertuples(index=False):
        added_now = add_watchlist_item(
            ticker=row.ticker,
            catalyst=row.catalyst_summary,
            price=float(row.display_price) if not pd.isna(row.display_price) else None,
            gap_pct=float(row.premarket_change) if not pd.isna(row.premarket_change) else None,
            source="auto-volume-breaker",
        )
        if added_now:
            added += 1
    return added


def render_auto_refresh(enabled: bool, premarket_only: bool) -> None:
    if not enabled or not premarket_only:
        return
    components.html(
        f"""
        <script>
        setTimeout(function() {{
            window.parent.location.reload();
        }}, {AUTO_REFRESH_SECONDS * 1000});
        </script>
        """,
        height=0,
    )


def render_header() -> None:
    st.markdown(
        f"""
        <div class="app-hero">
            <div class="eyebrow">Live Premarket Workflow</div>
            <div class="hero-title">{html.escape(APP_TITLE)}</div>
            <p class="hero-subtitle">{html.escape(APP_SUBTITLE)}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_metrics(scan_count: int, breaker_count: int, watchlist_count: int, session_label: str) -> None:
    now_text = now_et().strftime("%b %d, %Y %I:%M:%S %p ET")
    cols = st.columns(4)
    cards = [
        ("Scanner Hits", str(scan_count), "Current names passing the live premarket filters."),
        ("Volume Breakers", str(breaker_count), "Names already within 20% of all-time daily volume."),
        ("My Watchlist", str(watchlist_count), "Locally persisted watchlist entries in watchlist.json."),
        ("Session", session_label, now_text),
    ]
    for col, (label, value, note) in zip(cols, cards):
        with col:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-label">{html.escape(label)}</div>
                    <div class="metric-value">{html.escape(value)}</div>
                    <div class="metric-note">{html.escape(note)}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_table_headers() -> None:
    headers = [
        "Ticker",
        "%Chg",
        "Price",
        "Volume (pre)",
        "Float",
        "All-Time High Vol",
        "Vol Ratio",
        "Catalyst/Reason",
        "Grade",
        "Action",
    ]
    widths = [1.0, 0.8, 0.8, 1.0, 0.9, 1.15, 0.95, 3.4, 0.7, 1.2]
    columns = st.columns(widths)
    for column, label in zip(columns, headers):
        with column:
            st.markdown(f'<div class="table-header">{html.escape(label)}</div>', unsafe_allow_html=True)


def render_catalyst_html(
    event_label: str,
    direction: str,
    headlines: list[dict[str, str]],
    short_float_str: str | None,
    market_cap: str | None,
    sector: str,
    edge: str,
    catalyst_quality: str,
    catalyst_quality_reason: str,
    story_after: str,
    bullish: list[str],
    bearish: list[str],
    trade_structure: str,
    rerating: str,
) -> str:
    """Compact summary cell — headline, quality, story_after preview, quick badges."""
    event_css = {
        "M&A": "event-ma", "Earnings": "event-earnings", "Clinical": "event-clinical",
        "Commercial": "event-commercial", "Product": "event-product",
        "Analyst": "event-analyst", "Financing": "event-financing", "Legal": "event-legal",
    }.get(event_label, "event-default")
    dir_css  = {"positive": "dir-up", "negative": "dir-down", "mixed": "dir-mixed"}.get(direction, "dir-mixed")
    dir_icon = {"positive": "↑", "negative": "↓", "mixed": "~"}.get(direction, "~")
    dir_label = {"positive": "Bullish", "negative": "Bearish", "mixed": "Mixed"}.get(direction, "Mixed")
    cq_lower = catalyst_quality.lower()
    cq_css  = "cq-high" if "high" in cq_lower else ("cq-medium" if "medium" in cq_lower else "cq-low")
    cq_icon = "◆" if "high" in cq_lower else ("◇" if "medium" in cq_lower else "○")

    lines = []

    # Tag row
    tag_row = (f'<div class="catalyst-tags">'
               f'<span class="event-badge {event_css}">{html.escape(event_label)}</span>'
               f'<span class="dir-badge {dir_css}">{dir_icon} {html.escape(dir_label)}</span>')
    if catalyst_quality:
        tag_row += f'<span class="cq-badge {cq_css}">{cq_icon} {html.escape(catalyst_quality)}</span>'
    tag_row += '</div>'
    lines.append(tag_row)

    # Quality reason
    if catalyst_quality_reason:
        lines.append(f'<div class="cq-reason">{html.escape(catalyst_quality_reason)}</div>')

    # Top headline (linked)
    if headlines:
        item = headlines[0]
        title = html.escape(item.get("title", ""))
        link  = html.escape(item.get("link", ""))
        src   = html.escape(item.get("source", ""))
        ts    = html.escape(item.get("time", ""))
        meta  = f' <span class="subtext">· {src} · {ts}</span>' if src or ts else ""
        lines.append(
            f'<div class="catalyst-headline">'
            + (f'<a class="news-link" href="{link}" target="_blank">{title}</a>' if link else title)
            + f'{meta}</div>'
        )
    else:
        lines.append('<div class="catalyst-headline subtext">No catalyst headline found.</div>')

    # Stats
    stats = [s for s in [
        f"Short: {short_float_str}" if short_float_str else None,
        f"Cap: {market_cap}" if market_cap else None,
        html.escape(sector) if sector else None,
    ] if s]
    if stats:
        lines.append(f'<div class="catalyst-stats">{" · ".join(stats)}</div>')

    # Story preview (first 2 sentences)
    if story_after:
        preview = ". ".join(story_after.split(". ")[:2])
        if not preview.endswith("."):
            preview += "…"
        lines.append(f'<div class="catalyst-story">{html.escape(preview)}</div>')

    # Compact bull/bear counts + trade structure badge
    footer_parts = []
    if bullish:
        footer_parts.append(f'<span class="cq-badge cq-high">↑ {len(bullish)} bull</span>')
    if bearish:
        footer_parts.append(f'<span class="cq-badge cq-low">↓ {len(bearish)} bear</span>')
    if trade_structure:
        ts_l = trade_structure.lower()
        ts_css = "ts-go" if "go" in ts_l else ("ts-fade" if "fade" in ts_l else "ts-multi")
        ts_txt = "Gap-and-Go" if "go" in ts_l else ("Gap-and-Fade" if "fade" in ts_l else "Multi-Day")
        footer_parts.append(f'<span class="ts-badge {ts_css}" style="font-size:0.66rem;padding:0.14rem 0.4rem;">{ts_txt}</span>')
    if rerating:
        r_l = rerating.lower()
        r_css2 = "cq-high" if r_l.startswith("yes") else ("cq-medium" if r_l.startswith("partial") else "")
        r_label = rerating.split("|")[0].strip().split()[0]
        if r_css2:
            footer_parts.append(f'<span class="cq-badge {r_css2}" style="font-size:0.66rem;">Re-rate: {html.escape(r_label)}</span>')
    if footer_parts:
        lines.append(f'<div style="display:flex;flex-wrap:wrap;gap:0.25rem;margin-top:0.25rem;">{"".join(footer_parts)}</div>')

    # Fallback when no API key
    if not story_after and edge:
        lines.append(f'<div class="catalyst-edge">↳ {html.escape(edge)}</div>')
        lines.append('<div class="no-api-note">Set ANTHROPIC_API_KEY for full analyst breakdown.</div>')

    return f'<div class="catalyst-wrap">{"".join(lines)}</div>'


def render_deep_analysis(row: Any) -> None:
    """Full Streamlit expander with the 14-field deep-dive per ticker."""
    has_llm = bool(getattr(row, "story_after", ""))

    with st.expander(f"📊 Full Analysis — {row.ticker}", expanded=False):
        if not has_llm:
            st.caption("Set `ANTHROPIC_API_KEY` and refresh to see the full analyst breakdown.")
            return

        col_l, col_r = st.columns([1, 1])

        with col_l:
            # Why Today
            if row.why_today:
                st.markdown(
                    f'<div class="deep-section">'
                    f'<div class="deep-label">⚡ Why Today</div>'
                    f'<div class="deep-text">{html.escape(row.why_today)}</div>'
                    f'</div>', unsafe_allow_html=True)

            # Narrative shift
            if row.story_before or row.story_after:
                st.markdown(
                    f'<div class="deep-section">'
                    f'<div class="deep-label">📖 Narrative Shift</div>'
                    + (f'<div class="deep-story-before"><b style="color:#5a7a99;font-size:0.7rem;text-transform:uppercase;letter-spacing:0.05em;">Before</b><br>{html.escape(row.story_before)}</div>' if row.story_before else "")
                    + (f'<div class="deep-story-after"><b style="color:#4a8fc4;font-size:0.7rem;text-transform:uppercase;letter-spacing:0.05em;">After</b><br>{html.escape(row.story_after)}</div>' if row.story_after else "")
                    + '</div>', unsafe_allow_html=True)

            # Market mechanism
            if row.mechanism:
                st.markdown(
                    f'<div class="deep-section">'
                    f'<div class="deep-label">⚙ Market Mechanism</div>'
                    f'<div class="deep-text">{html.escape(row.mechanism)}</div>'
                    f'</div>', unsafe_allow_html=True)

            # Float analysis
            if row.float_analysis:
                st.markdown(
                    f'<div class="deep-section">'
                    f'<div class="deep-label">🔢 Float Analysis</div>'
                    f'<div class="deep-float-block">{html.escape(row.float_analysis)}</div>'
                    f'</div>', unsafe_allow_html=True)

            # Key uncertainty
            if row.key_uncertainty:
                st.markdown(
                    f'<div class="deep-section">'
                    f'<div class="deep-label">❓ Key Uncertainty</div>'
                    f'<div class="deep-uncertainty">{html.escape(row.key_uncertainty)}</div>'
                    f'</div>', unsafe_allow_html=True)

        with col_r:
            # Bull case
            if row.bullish:
                items_html = "".join(f'<div class="bb-item-wide">{html.escape(p)}</div>' for p in row.bullish)
                st.markdown(
                    f'<div class="deep-section">'
                    f'<div class="deep-label" style="color:var(--green);">↑ Bull Case ({len(row.bullish)} arguments)</div>'
                    f'<div class="bb-block bb-block-wide bull">{items_html}</div>'
                    f'</div>', unsafe_allow_html=True)

            # Bear case
            if row.bearish:
                items_html = "".join(f'<div class="bb-item-wide">{html.escape(p)}</div>' for p in row.bearish)
                st.markdown(
                    f'<div class="deep-section">'
                    f'<div class="deep-label" style="color:var(--red);">↓ Bear Risks ({len(row.bearish)} risks)</div>'
                    f'<div class="bb-block bb-block-wide bear">{items_html}</div>'
                    f'</div>', unsafe_allow_html=True)

            # Catalyst durability
            if row.catalyst_durability:
                st.markdown(
                    f'<div class="deep-section">'
                    f'<div class="deep-label">⏱ Catalyst Durability</div>'
                    f'<span class="durability-badge">{html.escape(row.catalyst_durability)}</span>'
                    + (f'<span class="deep-text-muted">{html.escape(row.catalyst_durability_reason)}</span>' if row.catalyst_durability_reason else "")
                    + '</div>', unsafe_allow_html=True)

            # Dilution risk
            if row.dilution_risk:
                dil_lower = row.dilution_risk.lower()
                dil_css = "" if any(w in dil_lower for w in ["high", "medium"]) else "safe"
                st.markdown(
                    f'<div class="deep-section">'
                    f'<div class="deep-label">⚠ Dilution Risk</div>'
                    f'<div class="deep-dilution {dil_css}">{html.escape(row.dilution_risk)}</div>'
                    f'</div>', unsafe_allow_html=True)

            # Trade structure
            if row.trade_structure:
                ts_l = row.trade_structure.lower()
                ts_css = "ts-go" if "go" in ts_l else ("ts-fade" if "fade" in ts_l else "ts-multi")
                ts_txt = "Gap-and-Go" if "go" in ts_l else ("Gap-and-Fade" if "fade" in ts_l else "Multi-Day Setup")
                st.markdown(
                    f'<div class="deep-section">'
                    f'<div class="deep-label">📐 Trade Structure</div>'
                    f'<div class="trade-structure-row">'
                    f'<span class="ts-badge {ts_css}">{ts_txt}</span>'
                    + (f'<span class="ts-reason">{html.escape(row.trade_structure_reason)}</span>' if row.trade_structure_reason else "")
                    + '</div></div>', unsafe_allow_html=True)

        # Entry + Invalidation full width
        if row.entry_thesis or row.invalidation:
            c1, c2 = st.columns(2)
            with c1:
                if row.entry_thesis:
                    st.markdown(
                        f'<div class="deep-section">'
                        f'<div class="deep-label" style="color:var(--green);">✅ Entry Thesis</div>'
                        f'<div class="deep-entry">{html.escape(row.entry_thesis)}</div>'
                        f'</div>', unsafe_allow_html=True)
            with c2:
                if row.invalidation:
                    st.markdown(
                        f'<div class="deep-section">'
                        f'<div class="deep-label" style="color:var(--red);">🚫 Invalidation</div>'
                        f'<div class="deep-invalidation">{html.escape(row.invalidation)}</div>'
                        f'</div>', unsafe_allow_html=True)

        # Re-rating full width
        if row.rerating:
            r_lower = row.rerating.lower()
            r_css = "rerating-yes" if r_lower.startswith("yes") else ("rerating-partial" if r_lower.startswith("partial") else "rerating-no")
            r_label = row.rerating.split("|")[0].strip().split()[0]
            st.markdown(
                f'<div class="deep-section">'
                f'<div class="deep-label">🏷 Re-rating Verdict</div>'
                f'<div class="catalyst-rerating {r_css}">'
                f'<span class="rerating-label">Re-rating: {html.escape(r_label)}</span>'
                f'<span class="rerating-reason-text">{html.escape(row.rerating_reason)}</span>'
                f'</div></div>', unsafe_allow_html=True)


def render_scan_table(frame: pd.DataFrame, table_key: str) -> None:
    if frame.empty:
        st.info("No stocks are currently matching the selected scanner filters.")
        return

    render_table_headers()
    widths = [1.0, 0.8, 0.8, 1.0, 0.9, 1.15, 0.95, 3.4, 0.7, 1.2]

    for row in frame.itertuples(index=False):
        columns = st.columns(widths)

        with columns[0]:
            st.markdown(
                f"""
                <div class="cell ticker">{html.escape(row.ticker)}</div>
                <div class="subtext">{html.escape(str(row.exchange))}</div>
                """,
                unsafe_allow_html=True,
            )

        with columns[1]:
            st.markdown(f'<div class="cell">{format_gap_html(row.premarket_change)}</div>', unsafe_allow_html=True)

        with columns[2]:
            st.markdown(f'<div class="cell">{html.escape(format_price(row.display_price))}</div>', unsafe_allow_html=True)

        with columns[3]:
            st.markdown(
                f'<div class="cell">{html.escape(compact_number(row.premarket_volume))}</div>',
                unsafe_allow_html=True,
            )

        with columns[4]:
            st.markdown(
                f'<div class="cell">{html.escape(compact_number(row.float_shares_outstanding_current))}</div>',
                unsafe_allow_html=True,
            )

        with columns[5]:
            st.markdown(
                f'<div class="cell">{html.escape(compact_number(row.all_time_high_volume))}</div>',
                unsafe_allow_html=True,
            )

        with columns[6]:
            badge = f'<div class="breaker-badge">{html.escape(row.flag)}</div>' if row.is_volume_breaker else ""
            css = "value-orange" if row.is_volume_breaker else ""
            st.markdown(
                f'<div class="cell {css}">{html.escape(format_ratio(row.volume_ratio))}</div>{badge}',
                unsafe_allow_html=True,
            )

        with columns[7]:
            st.markdown(
                render_catalyst_html(
                    event_label=row.event_label,
                    direction=row.direction,
                    headlines=row.headlines,
                    short_float_str=row.short_float_str,
                    market_cap=row.market_cap,
                    sector=row.sector,
                    edge=row.edge,
                    catalyst_quality=row.catalyst_quality,
                    catalyst_quality_reason=row.catalyst_quality_reason,
                    story_after=row.story_after,
                    bullish=row.bullish,
                    bearish=row.bearish,
                    trade_structure=row.trade_structure,
                    rerating=row.rerating,
                ),
                unsafe_allow_html=True,
            )

        # Full deep-dive expander below each row (full width)
        render_deep_analysis(row)

        with columns[8]:
            st.markdown(f'<div class="cell">{grade_html(row.grade)}</div>', unsafe_allow_html=True)

        with columns[9]:
            if st.button(
                "Add to My Watchlist",
                key=f"{table_key}-add-{row.ticker}",
                use_container_width=True,
                type="secondary",
            ):
                was_added = add_watchlist_item(
                    ticker=row.ticker,
                    catalyst=row.catalyst_summary,
                    price=float(row.display_price) if not pd.isna(row.display_price) else None,
                    gap_pct=float(row.premarket_change) if not pd.isna(row.premarket_change) else None,
                    source="manual",
                )
                st.toast(f'{"Added" if was_added else "Updated"} {row.ticker} in watchlist.')
                st.rerun()


def render_sidebar(watchlist_items: list[dict[str, Any]]) -> dict[str, Any]:
    st.sidebar.markdown("## Scanner Controls")
    min_gap_pct = st.sidebar.slider("Min % gap", min_value=1.0, max_value=50.0, value=8.0, step=0.5)
    min_volume = int(
        st.sidebar.number_input("Min premarket volume", min_value=0, value=500_000, step=100_000)
    )
    min_float_millions = st.sidebar.number_input(
        "Min float (millions of shares)",
        min_value=0.0,
        value=0.0,
        step=1.0,
        format="%.1f",
    )
    volume_breaker_only = st.sidebar.toggle("Volume Breaker only", value=False)
    auto_refresh = st.sidebar.toggle("Auto-refresh every 60s", value=True)
    scan_limit = int(
        st.sidebar.slider("Max rows", min_value=10, max_value=MAX_SCAN_LIMIT, value=DEFAULT_SCAN_LIMIT, step=5)
    )
    st.sidebar.button("Refresh Scan", type="primary", use_container_width=True)

    st.sidebar.markdown("---")
    st.sidebar.markdown("## AI Analyst (Claude)")
    has_api_key = bool(os.getenv("ANTHROPIC_API_KEY", "").strip())
    if has_api_key:
        st.sidebar.success("Claude API key detected — full analyst breakdown enabled.", icon="✅")
    else:
        st.sidebar.warning(
            "Set `ANTHROPIC_API_KEY` in your environment to enable the AI story, bull/bear case, and re-rating analysis per stock.\n\n"
            "```\nexport ANTHROPIC_API_KEY=sk-ant-...\nstreamlit run app.py\n```",
            icon="🔑",
        )

    st.sidebar.markdown("---")
    st.sidebar.markdown("## My Watchlist")
    st.sidebar.caption(
        "Volume breaker names are auto-added when they first hit the trigger. "
        "If they keep triggering, they can reappear after removal on a later refresh."
    )

    if watchlist_items:
        st.sidebar.download_button(
            "Export Watchlist CSV",
            data=export_watchlist_csv(watchlist_items),
            file_name="watchlist.csv",
            mime="text/csv",
            use_container_width=True,
        )
        for item in watchlist_items:
            ticker = str(item.get("ticker", ""))
            source = str(item.get("source", "manual")).replace("-", " ").title()
            timestamp = str(item.get("timestamp", ""))[:19].replace("T", " ")
            st.sidebar.markdown(
                f"""
                <div class="sidebar-watch-card">
                    <div class="sidebar-watch-ticker">{html.escape(ticker)}</div>
                    <div class="sidebar-watch-meta">{html.escape(source)} | {html.escape(timestamp)}</div>
                    <div class="sidebar-watch-meta">{html.escape(str(item.get("catalyst", ""))[:120])}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if st.sidebar.button(f"Remove {ticker}", key=f"remove-{ticker}", use_container_width=True):
                remove_watchlist_item(ticker)
                st.toast(f"Removed {ticker} from watchlist.")
                st.rerun()
    else:
        st.sidebar.info("Your watchlist is empty.")

    return {
        "min_gap_pct": min_gap_pct,
        "min_volume": min_volume,
        "min_float_shares": min_float_millions * 1_000_000,
        "volume_breaker_only": volume_breaker_only,
        "auto_refresh": auto_refresh,
        "scan_limit": scan_limit,
    }


def main() -> None:
    inject_css()
    render_header()

    session_label, is_premarket = market_session_status()
    watchlist_items = load_watchlist()
    controls = render_sidebar(watchlist_items)
    render_auto_refresh(controls["auto_refresh"], is_premarket)

    if not os.getenv("TRADINGVIEW_COOKIES_JSON"):
        st.markdown(
            """
            <div class="warning-note">
            TradingView scan is running on an anonymous session. Premarket fields will still load,
            but they can be delayed versus an authenticated TradingView session.
            </div>
            """,
            unsafe_allow_html=True,
        )

    with st.spinner("Scanning TradingView premarket gappers and enriching catalyst + volume history..."):
        scan_error = None
        try:
            scanned = scan_premarket_gappers(
                min_gap_pct=controls["min_gap_pct"],
                min_volume=controls["min_volume"],
                min_price=1,
                max_price=200,
                limit=controls["scan_limit"],
            )
            enriched = enrich_scan(scanned)
        except Exception as exc:
            scan_error = str(exc)
            enriched = pd.DataFrame()

    if scan_error:
        st.error(
            "The scan failed on this refresh. This is usually a temporary TradingView, Yahoo, or Finviz "
            f"request issue.\n\nDetails: {scan_error}"
        )
        return

    auto_added = sync_auto_watchlist(enriched)
    watchlist_items = load_watchlist()
    filtered = apply_filters(
        enriched,
        min_float_shares=controls["min_float_shares"],
        volume_breaker_only=controls["volume_breaker_only"],
    )

    # Guard: no matching stocks (scan returned empty, outside market hours, or
    # all enrichments failed).  Show a friendly notice instead of a KeyError.
    if filtered.empty or "is_volume_breaker" not in filtered.columns:
        render_metrics(
            scan_count=0,
            breaker_count=0,
            watchlist_count=len(watchlist_items),
            session_label=session_label,
        )
        st.info(
            "No premarket gappers matched the current scan settings.\n\n"
            "This is normal outside US premarket hours (4:00 – 9:30 AM ET). "
            "Try lowering the **Min % gap** or **Min premarket volume** filters, "
            "or wait until US premarket opens."
        )
        return

    volume_breakers = filtered[filtered["is_volume_breaker"]].reset_index(drop=True)

    render_metrics(
        scan_count=len(filtered),
        breaker_count=len(volume_breakers),
        watchlist_count=len(watchlist_items),
        session_label=session_label,
    )

    if auto_added:
        st.success(f"Auto-added {auto_added} new volume breaker(s) to watchlist.json.")

    tabs = st.tabs(["Premarket Gappers", "Highest Volume Ever Today"])
    with tabs[0]:
        st.caption(
            "Rows are filtered to US-listed common stocks with premarket gap magnitude, premarket volume, "
            "and price requirements. Grade is derived from EMA10 / EMA20 / SMA50 trend structure."
        )
        render_scan_table(filtered, table_key="main")

    with tabs[1]:
        st.caption(
            "These names are already within 20% of their all-time daily volume high based on yfinance history. "
            "They are also auto-added to the watchlist."
        )
        render_scan_table(volume_breakers, table_key="breaker")


if __name__ == "__main__":
    main()
