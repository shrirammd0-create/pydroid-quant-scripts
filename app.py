#!/usr/bin/env python3
"""
XAUUSD Quant Terminal -- Streamlit dashboard
===============================================
Pure display layer for the pre-fetched architecture: renders the structured
metrics gold_terminal.py wrote to data.json. Performs zero data fetching, so
the page loads instantly and can never be rate-limited by a refresh.

Dark theme comes from .streamlit/config.toml ([theme] base = "dark").
"""

import json
import pathlib
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

BASE_DIR = pathlib.Path(__file__).resolve().parent
JSON_FILE = BASE_DIR / "data.json"
RAW_FILE = BASE_DIR / "market_data.txt"

st.set_page_config(page_title="Quant Terminal", layout="wide",
                   initial_sidebar_state="collapsed")


def jnum(value):
    """data.json numerics arrive as numbers or the string 'NA'."""
    if value is None or value == "NA":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fmt(value, decimals=2, prefix="", suffix=""):
    return "N/A" if value is None else f"{prefix}{value:,.{decimals}f}{suffix}"


# ---- Header -----------------------------------------------------------------

title_col, refresh_col = st.columns([6, 1])
with title_col:
    st.title("XAUUSD Quant Terminal")
with refresh_col:
    st.write("")
    if st.button("🔄 Refresh Data", width="stretch"):
        st.rerun()

if not JSON_FILE.exists():
    st.warning("Data sync in progress. Trigger GitHub Action to refresh.")
    st.stop()

try:
    data = json.loads(JSON_FILE.read_text(encoding="utf-8"))
except Exception:
    st.error("data.json exists but could not be parsed -- showing raw feed only.")
    if RAW_FILE.exists():
        with st.expander("Show Raw Feed", expanded=True):
            st.code(RAW_FILE.read_text(encoding="utf-8"), language="csv")
    st.stop()

gen_str = data.get("generated_utc")
try:
    gen_dt = datetime.strptime(gen_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    age_min = (datetime.now(timezone.utc) - gen_dt).total_seconds() / 60
    badge = f"Data as of {gen_dt.strftime('%Y-%m-%d %H:%M UTC')} ({age_min:.0f} min ago)"
    if age_min <= 20:
        st.success(badge)
    elif age_min <= 60:
        st.warning(badge)
    else:
        st.error(f"{badge} -- feed looks stale, check the GitHub Action")
except (TypeError, ValueError):
    st.caption(f"Generated: {gen_str}")


# ---- Tabs ---------------------------------------------------------------------

tab_tickers, tab_smc, tab_cot, tab_news, tab_risk = st.tabs(
    ["📊 Tickers", "🏛️ SMC & Structure", "💼 COT Sentiment", "📰 News", "⚙️ Risk Engine"]
)

assets = data.get("assets", {})
pivots = data.get("pivots")
smc = data.get("smc", {})
cot = data.get("cot")
news = data.get("news")          # None = feed down, [] = quiet day
risk = data.get("risk", {})
alert = data.get("alert", {})

with tab_tickers:
    symbols = list(assets.keys())
    for row_start in range(0, len(symbols), 4):
        cols = st.columns(4)
        for col, symbol in zip(cols, symbols[row_start:row_start + 4]):
            a = assets[symbol]
            decimals = int(a.get("decimals", 2))
            atr = jnum(a.get("atr14"))
            col.metric(
                f"{a.get('label', symbol)} ({symbol})",
                fmt(jnum(a.get("last_close")), decimals),
                delta=(f"ATR14 {fmt(atr, decimals)}" if atr is not None else None),
                delta_color="off",
            )
    st.divider()
    gold = assets.get("GC=F", {})
    c1, c2, c3 = st.columns(3)
    c1.metric("Gold 15m VWAP", fmt(jnum(gold.get("vwap")), 2, "$"))
    c2.metric("Gold Volume Z (20)", fmt(jnum(gold.get("volume_z")), 2))
    c3.metric("Gold Candle Body Ratio", fmt(jnum(gold.get("body_ratio")), 2))

with tab_smc:
    st.subheader("Fair Value Gaps (Gold 15m, open only)")
    fvgs = smc.get("fvgs") or []
    if fvgs:
        st.dataframe(pd.DataFrame(fvgs), hide_index=True, width="stretch")
    else:
        st.info("No open fair value gaps detected in the current 15m window.")

    st.subheader("Liquidity Sweep")
    sweep = smc.get("sweep") or {}
    if sweep.get("swept_pdh") or sweep.get("swept_pdl"):
        st.warning(f"⚡ {sweep.get('detail', 'Sweep detected')}")
    else:
        st.info(sweep.get("detail", "No sweep data"))

    st.subheader("Floor Pivots")
    if pivots:
        st.caption(f"Basis candle: {pivots.get('basis_date', 'N/A')}  •  "
                   f"Prev High {fmt(jnum(pivots.get('prev_high')), 2, '$')}  •  "
                   f"Prev Low {fmt(jnum(pivots.get('prev_low')), 2, '$')}")
        pivot_df = pd.DataFrame({
            "Level": ["R2", "R1", "PP", "S1", "S2"],
            "Price": [fmt(jnum(pivots.get(k)), 2, "$") for k in ("R2", "R1", "PP", "S1", "S2")],
        })
        st.dataframe(pivot_df, hide_index=True, width="stretch")
    else:
        st.info("Pivot data unavailable in the latest fetch.")

with tab_cot:
    if cot:
        st.caption(f"{cot.get('contract', '')} — report date {cot.get('report_date', '')} "
                   f"— source: {cot.get('source', '')}")
        net = jnum(cot.get("net"))
        c1, c2, c3 = st.columns(3)
        c1.metric("Hedge Fund Long", fmt(jnum(cot.get("long")), 0))
        c2.metric("Hedge Fund Short", fmt(jnum(cot.get("short")), 0))
        c3.metric("Net Positioning", fmt(net, 0),
                  delta=("Net Long" if net and net > 0 else "Net Short" if net and net < 0 else None))
    else:
        st.info("CFTC data unavailable this run. The report is weekly (published Fridays "
                "for the prior Tuesday), so gaps outside that window are normal.")

with tab_news:
    st.subheader("Today's High-Impact Events (UTC)")
    if news is None:
        st.info("Economic calendar feed was unreachable on the last fetch.")
    elif not news:
        st.success("No high-impact events scheduled today.")
    else:
        for ev in news:
            st.warning(f"🔴 **{ev.get('time_utc', '?')} UTC** — {ev.get('country', '')} "
                       f"**{ev.get('title', '')}**  (forecast: {ev.get('forecast', '-')}, "
                       f"previous: {ev.get('previous', '-')})")

with tab_risk:
    st.caption(risk.get("contract_note", ""))
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Account", fmt(jnum(risk.get("account_usd")), 0, "$"))
    c2.metric("Max Risk (0.5%)", fmt(jnum(risk.get("max_risk_usd")), 2, "$"))
    c3.metric("Stop Distance (1.5x ATR)", fmt(jnum(risk.get("stop_distance_usd")), 2, "$"))
    c4.metric("Position Size", (f"{jnum(risk.get('lot_size')):.2f} lot"
                                if jnum(risk.get("lot_size")) is not None else "N/A"))

    if risk.get("tradeable"):
        st.success(f"✅ {risk.get('status', '')}")
    else:
        st.error(f"⛔ {risk.get('status', 'UN-TRADEABLE')}")

    st.subheader("Breakout Alert Status")
    conditions = alert.get("conditions", {})
    cond_df = pd.DataFrame({
        "Condition": ["Close > R1", "Volume Z > 1.5", "Body Ratio ≥ 0.70", "Passes Risk Limit"],
        "Met": ["✅" if conditions.get(k) else "❌"
                for k in ("close_above_r1", "volume_z_above_1_5",
                          "body_ratio_above_0_70", "passes_risk_limit")],
    })
    st.dataframe(cond_df, hide_index=True, width="stretch")
    if alert.get("active"):
        sent_note = "push alert sent this run" if alert.get("sent") else \
            "already alerted on a previous run (not re-sent)"
        st.warning(f"🚨 Breakout setup ACTIVE — {sent_note}.")
    else:
        st.info("No active breakout setup.")

if RAW_FILE.exists():
    with st.expander("Show Raw Feed"):
        st.code(RAW_FILE.read_text(encoding="utf-8"), language="csv")
