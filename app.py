#!/usr/bin/env python3
"""
XAUUSD Quant Terminal -- Streamlit dashboard
===============================================
Pure display layer for the "pre-fetched" architecture: parses the report
gold_terminal.py already wrote to market_data.txt into headline metrics and
renders them as a dashboard. Never calls Yahoo Finance or CFTC itself --
refreshing this page just re-reads the file gold_terminal.py last wrote, so
it loads instantly and can't be rate-limited.

Keep gold_terminal.py running on a schedule (GitHub Actions) to keep
market_data.txt current.

NOTE on the "High-Impact News" tab: this pipeline has no economic-calendar
data source (no CPI/FOMC/NFP scraper exists anywhere in gold_terminal.py).
That tab is a real, honest placeholder rather than fabricated events --
showing fake news badges in a trading tool is worse than showing nothing.
"""

import re
import pathlib
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

DATA_FILE = pathlib.Path(__file__).resolve().parent / "market_data.txt"

st.set_page_config(page_title="Quant Terminal", layout="wide", initial_sidebar_state="collapsed")


# ============================================================================
# PARSER
# ============================================================================

def is_dashed_line(line):
    s = line.strip()
    return bool(s) and set(s) == {"-"}


def parse_report(raw_text):
    """Pull the [1] MARKET SNAPSHOT SUMMARY block out of the report into a
    flat {metric: value} dict, plus the top 'Generated: ...' line.

    Returns (None, generated_line) if the section marker isn't found, so
    the caller can fall back to showing the raw file instead of rendering a
    dashboard built on a guess.
    """
    lines = raw_text.splitlines()
    generated_line = next((l.strip() for l in lines if l.strip().startswith("Generated:")), None)

    try:
        header_idx = next(
            i for i, l in enumerate(lines) if l.strip() == "[1] MARKET SNAPSHOT SUMMARY (CSV)"
        )
    except StopIteration:
        return None, generated_line

    i = header_idx + 1
    if i < len(lines) and is_dashed_line(lines[i]):
        i += 1
    if i < len(lines) and lines[i].strip() == "Metric,Value":
        i += 1

    metrics = {}
    while i < len(lines) and not is_dashed_line(lines[i]):
        line = lines[i].strip()
        if line and "," in line:
            key, _, value = line.partition(",")
            metrics[key.strip()] = value.strip()
        i += 1

    if not metrics:
        return None, generated_line
    return metrics, generated_line


def metric_float(metrics, key):
    v = metrics.get(key)
    if not v or v == "NA":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def metric_str(metrics, key, default="N/A"):
    v = metrics.get(key)
    return v if v and v != "NA" else default


def fmt(value, decimals=2, prefix="", suffix=""):
    if value is None:
        return "N/A"
    return f"{prefix}{value:,.{decimals}f}{suffix}"


def delta_str(value, suffix=""):
    return f"{value:+.3f}{suffix}" if value is not None else None


def parse_generated_dt(generated_line):
    if not generated_line:
        return None
    m = re.search(r"Generated:\s*([\d-]+ [\d:]+)\s*UTC", generated_line)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


# ============================================================================
# HEADER
# ============================================================================

title_col, refresh_col = st.columns([6, 1])
with title_col:
    st.title("XAUUSD Quant Terminal")
with refresh_col:
    st.write("")
    if st.button("🔄 Refresh Data", width="stretch"):
        st.rerun()

if not DATA_FILE.exists():
    st.warning(
        f"{DATA_FILE.name} not found yet. Trigger the 'Fetch Gold Market Data' "
        "GitHub Action (Actions tab -> Run workflow) to generate it, then refresh this page."
    )
    st.stop()

raw_text = DATA_FILE.read_text(encoding="utf-8")
metrics, generated_line = parse_report(raw_text)

gen_dt = parse_generated_dt(generated_line)
if gen_dt:
    age_min = (datetime.now(timezone.utc) - gen_dt).total_seconds() / 60
    badge = f"Data as of {gen_dt.strftime('%Y-%m-%d %H:%M UTC')} ({age_min:.0f} min ago)"
    if age_min <= 20:
        st.success(badge)
    elif age_min <= 60:
        st.warning(badge)
    else:
        st.error(f"{badge} -- feed looks stale, check the GitHub Action")
elif generated_line:
    st.caption(generated_line)


# ============================================================================
# FALLBACK: unparseable report -> raw feed only
# ============================================================================

if metrics is None:
    st.error("Could not parse market_data.txt into dashboard sections -- showing the raw feed instead.")
    with st.expander("Show Raw Feed", expanded=True):
        st.code(raw_text, language="csv")
    st.stop()


# ============================================================================
# TABS
# ============================================================================

tab_tickers, tab_pivots, tab_cot, tab_news = st.tabs(
    ["📊 Live Tickers", "🎯 Pivots & ATR", "💼 Smart Money (COT)", "📰 High-Impact News"]
)

with tab_tickers:
    row1 = st.columns(4)
    row1[0].metric(
        "Gold (GC=F)", fmt(metric_float(metrics, "Gold_Intraday_Last_15m_Close"), 2, "$"),
        delta=delta_str(metric_float(metrics, "Gold_Intraday_Last_VWAP_Z_Score"), "σ vs VWAP"),
    )
    row1[1].metric("Silver (SI=F)", fmt(metric_float(metrics, "Silver_Intraday_Last_15m_Close"), 3, "$"))
    row1[2].metric(
        "DXY", fmt(metric_float(metrics, "DXY_Last_15m_Close"), 3),
        delta=delta_str(metric_float(metrics, "DXY_ROC_45m_pct"), "% (45m)"),
    )
    row1[3].metric("Gold VIX (^GVZ)", fmt(metric_float(metrics, "GVZ_Implied_Vol_Last"), 2))

    row2 = st.columns(4)
    row2[0].metric("10Y Yield (^TNX)", fmt(metric_float(metrics, "TNX_10Y_Yield_Last_pct"), 3, suffix="%"))
    row2[1].metric("AUD/USD", fmt(metric_float(metrics, "AUDUSD_Last_15m_Close"), 5))
    row2[2].metric("USD/JPY", fmt(metric_float(metrics, "USDJPY_Last_15m_Close"), 3))
    row2[3].metric("EUR/USD", fmt(metric_float(metrics, "EURUSD_Last_15m_Close"), 5))

    st.divider()
    c1, c2 = st.columns(2)
    c1.metric("Gold/Silver Ratio", fmt(metric_float(metrics, "Gold_Silver_Ratio_Last"), 3))
    c2.metric("Cumulative Volume Delta", fmt(metric_float(metrics, "Gold_Intraday_Last_CVD"), 2))

with tab_pivots:
    st.caption(f"Basis candle: {metric_str(metrics, 'Pivot_Basis_Date')}")

    m1, m2, m3 = st.columns(3)
    m1.metric("Daily ATR(14)", fmt(metric_float(metrics, "Daily_ATR14"), 2, "$"))
    m2.metric("Yesterday's High", fmt(metric_float(metrics, "Pivot_Basis_High"), 2, "$"))
    m3.metric("Yesterday's Low", fmt(metric_float(metrics, "Pivot_Basis_Low"), 2, "$"))

    st.markdown("**Floor Pivots**")
    pivot_levels = [
        ("R2", metric_float(metrics, "Pivot_R2")),
        ("R1", metric_float(metrics, "Pivot_R1")),
        ("PP", metric_float(metrics, "Pivot_PP")),
        ("S1", metric_float(metrics, "Pivot_S1")),
        ("S2", metric_float(metrics, "Pivot_S2")),
    ]
    pivot_df = pd.DataFrame({
        "Level": [name for name, _ in pivot_levels],
        "Price": [fmt(val, 2, "$") for _, val in pivot_levels],
    })
    st.dataframe(pivot_df, hide_index=True, width="stretch")

with tab_cot:
    st.caption(f"{metric_str(metrics, 'COT_Contract')} — report date {metric_str(metrics, 'COT_Report_Date')}")

    long_pos = metric_float(metrics, "COT_Managed_Money_Long")
    short_pos = metric_float(metrics, "COT_Managed_Money_Short")
    net_pos = metric_float(metrics, "COT_Net_Hedge_Fund_Position")

    c1, c2, c3 = st.columns(3)
    c1.metric("Managed Money Long", fmt(long_pos, 0))
    c2.metric("Managed Money Short", fmt(short_pos, 0))
    c3.metric(
        "Net Hedge Fund Position", fmt(net_pos, 0),
        delta=("Net Long" if net_pos and net_pos > 0 else "Net Short" if net_pos and net_pos < 0 else None),
    )

    if net_pos is None:
        st.info(
            "CFTC data unavailable in the latest fetch -- the weekly report may not have "
            "refreshed yet, or the scrape failed this cycle. CFTC publishes on a weekly lag "
            "(Fridays for the prior Tuesday's positioning), so gaps outside that window are normal."
        )

with tab_news:
    st.info(
        "\U0001F4F0 **Economic calendar feed not wired up yet.** gold_terminal.py doesn't "
        "currently scrape any high-impact news/economic-calendar source (CPI, FOMC, NFP, etc.) "
        "-- this tab is reserved for that feature rather than showing fabricated events. "
        "Ask to have it built (e.g. against an economic calendar source) and it can be wired "
        "into the pipeline the same way the COT scraper was."
    )

with st.expander("Show Raw Feed"):
    st.code(raw_text, language="csv")
