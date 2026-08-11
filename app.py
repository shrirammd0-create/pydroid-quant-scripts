#!/usr/bin/env python3
"""
Gold Quant Terminal -- Streamlit frontend
============================================
Pure display layer for the "pre-fetched" architecture: reads the report
gold_terminal.py already wrote to market_data.txt and shows it. Never
calls Yahoo Finance or CFTC itself, so the page loads instantly and can't
be rate-limited by a page refresh.

Keep gold_terminal.py running on a schedule (cron, GitHub Actions, etc.)
to keep market_data.txt current.
"""

import pathlib

import streamlit as st

DATA_FILE = pathlib.Path(__file__).resolve().parent / "market_data.txt"

if DATA_FILE.exists():
    file_contents = DATA_FILE.read_text(encoding="utf-8")
    st.code(file_contents, language="csv")
else:
    st.warning(
        f"{DATA_FILE.name} not found yet. Trigger the 'Fetch Gold Market Data' "
        "GitHub Action (Actions tab -> Run workflow) to generate it, then refresh this page."
    )
