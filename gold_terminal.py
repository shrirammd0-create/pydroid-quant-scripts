#!/usr/bin/env python3
"""
Gold (XAUUSD) Institutional Quant Data Pipeline
=================================================
Multi-timeframe Gold/Silver/DXY/GVZ/TNX market data + live CFTC Commitments
of Traders (COT) fundamentals, reduced to a dense, copy-pasteable CSV report.

No yfinance / curl_cffi dependency: Yahoo Finance's public chart JSON
endpoint is called directly over plain HTTPS with `requests` + `json`.

REQUIRED PACKAGES:
    pip install streamlit requests pandas numpy

USAGE:
    streamlit run gold_terminal.py
    Wait for the run to finish, then use the copy icon on the code block
    (or long-press -> Select All -> Copy on mobile Safari/Chrome) to grab
    the report between the dashed borders.
"""

import io
import time
import json
import urllib.parse
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import streamlit as st

try:
    import requests
except ImportError:
    st.error("Missing dependency 'requests'. Add it to requirements.txt: pip install requests")
    st.stop()

pd.options.mode.chained_assignment = None

st.set_page_config(page_title="Gold Quant Terminal", layout="wide")
st.title("Gold (XAUUSD) Institutional Quant Data Pipeline")

# Collects every line that used to go to the terminal via print() so it can
# be rendered as one block at the end via st.code(), byte-for-byte identical
# to the original CLI output.
_output_lines = []


def emit(line=""):
    _output_lines.append(line)


# Live network/fetch diagnostics (retries, cooldowns, schema warnings) are
# noisy and only useful while the pipeline is running, so they go to their
# own collapsible log instead of cluttering the final report.
_log_panel = st.expander("Network / fetch log", expanded=False)


def log_info(msg):
    _log_panel.info(msg)


def log_warn(msg):
    _log_panel.warning(msg)


def log_error(msg):
    _log_panel.error(msg)

# ============================================================================
# CONFIG
# ============================================================================

RETRY_ATTEMPTS = 4
RETRY_BASE_DELAY_SEC = 4        # generic transient errors: 4s, 8s, 16s exponential backoff
FETCH_PACING_SEC = 3            # proactive gap before every outbound request (avoid bursts)
RATE_LIMIT_COOLDOWN_SEC = 45    # long cooldown once Yahoo/CFTC signals a rate limit
REQUEST_TIMEOUT = 20

RATE_LIMIT_KEYWORDS = ("rate limit", "ratelimit", "too many requests", "429")

# Rows kept in each printed CSV block (mobile-copy friendly, still dense)
TAIL_DAILY = 15
TAIL_HOURLY = 20
TAIL_15M = 40

TICKERS = {
    "GOLD_MACRO":      {"symbol": "GC=F",      "period": "1mo", "interval": "1d"},
    "GOLD_STRUCT":     {"symbol": "GC=F",      "period": "7d",  "interval": "1h"},
    "GOLD_INTRADAY":   {"symbol": "GC=F",      "period": "3d",  "interval": "15m"},
    "SILVER_INTRADAY": {"symbol": "SI=F",      "period": "3d",  "interval": "15m"},
    "DXY_INTRADAY":    {"symbol": "DX-Y.NYB",  "period": "3d",  "interval": "15m"},
    "GVZ_INTRADAY":    {"symbol": "^GVZ",      "period": "3d",  "interval": "15m"},
    "TNX_MACRO":       {"symbol": "^TNX",      "period": "1mo", "interval": "1d"},
}

# Yahoo's chart endpoint is queried with explicit period1/period2 (unix epoch)
# rather than the "range" keyword, since only a small documented set of range
# values (1d,5d,1mo,3mo,...) is guaranteed valid -- arbitrary day counts like
# "3d"/"7d" are not. A small buffer is added so weekends/holidays don't starve
# the window.
PERIOD_LOOKBACK = {
    "1mo": timedelta(days=32),
    "7d": timedelta(days=8),
    "3d": timedelta(days=4),
}

CFTC_DISAGG_URL = "https://www.cftc.gov/dea/newcot/f_disagg.txt"

COT_NAME_COL_CANDIDATES = ["Market_and_Exchange_Names", "Market_and_Exchange_Name"]
COT_DATE_COL_CANDIDATES = [
    "Report_Date_as_YYYY-MM-DD",
    "As_of_Date_In_Form_YYYY-MM-DD",
    "As_of_Date_In_Form_YYMMDD",
]
COT_MM_LONG_CANDIDATES = ["M_Money_Positions_Long_All", "MMoney_Positions_Long_All"]
COT_MM_SHORT_CANDIDATES = ["M_Money_Positions_Short_All", "MMoney_Positions_Short_All"]


# ============================================================================
# NETWORK LAYER  (mobile-stability: 5G <-> WiFi handoffs drop connections)
# ============================================================================

# Shared across every retry_call invocation in this run: once ANY request gets
# rate-limited, every subsequent request (any ticker) waits out the same
# cooldown instead of immediately re-hammering an endpoint that just blocked us.
_net_state = {"cooldown_until": 0.0}


def _is_rate_limit_error(exc):
    msg = str(exc).lower()
    return any(k in msg for k in RATE_LIMIT_KEYWORDS)


def _throttle_before_request():
    """Wait out any active rate-limit cooldown, else apply baseline pacing."""
    now = time.time()
    if now < _net_state["cooldown_until"]:
        wait = _net_state["cooldown_until"] - now
        log_info(f"Rate-limit cooldown active -- waiting {wait:.0f}s before next request...")
        time.sleep(wait)
    else:
        time.sleep(FETCH_PACING_SEC)


def retry_call(func, *args, attempts=RETRY_ATTEMPTS, label="operation", **kwargs):
    """Retry wrapper with proactive pacing, exponential backoff for generic
    errors, and a long dedicated cooldown whenever the failure looks like a
    server-side rate limit (short retries are futile against those)."""
    last_err = None
    for i in range(1, attempts + 1):
        _throttle_before_request()
        try:
            result = func(*args, **kwargs)
            if result is None:
                raise ValueError("empty result")
            if isinstance(result, pd.DataFrame) and result.empty:
                raise ValueError("empty dataframe")
            if isinstance(result, str) and len(result.strip()) == 0:
                raise ValueError("empty text payload")
            return result
        except Exception as e:
            last_err = e
            log_warn(f"{label}: attempt {i}/{attempts} failed -> {e}")
            if _is_rate_limit_error(e):
                _net_state["cooldown_until"] = time.time() + RATE_LIMIT_COOLDOWN_SEC
            elif i < attempts:
                time.sleep(RETRY_BASE_DELAY_SEC * (2 ** (i - 1)))
    log_error(f"{label}: all {attempts} attempts failed -> {last_err}")
    return None


# ---- Yahoo Finance raw JSON chart endpoint (no yfinance / curl_cffi) ------

YAHOO_HOSTS = ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]
YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

_yahoo_session = requests.Session()
_yahoo_session.headers.update(YAHOO_HEADERS)
_crumb_state = {"value": None, "attempted": False}


def _get_yahoo_crumb():
    """Best-effort auth crumb. Plain historical chart requests usually work
    without one; if Yahoo starts requiring it we already have a session with
    warmed-up cookies, so try once per run and cache the result either way."""
    if _crumb_state["attempted"]:
        return _crumb_state["value"]
    _crumb_state["attempted"] = True
    try:
        _yahoo_session.get("https://fc.yahoo.com", timeout=REQUEST_TIMEOUT)
    except Exception:
        pass
    for host in YAHOO_HOSTS:
        try:
            r = _yahoo_session.get(f"https://{host}/v1/test/getcrumb", timeout=REQUEST_TIMEOUT)
            text = (r.text or "").strip()
            if r.status_code == 200 and text and "<html" not in text.lower():
                _crumb_state["value"] = text
                break
        except Exception:
            continue
    return _crumb_state["value"]


def fetch_yahoo_chart(symbol, lookback, interval):
    """Pull raw OHLCV chart JSON straight from Yahoo Finance's public
    /v8/finance/chart endpoint -- the same backend yfinance itself scrapes --
    using only `requests` + `json` (no compiled extensions required)."""
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - lookback
    encoded_symbol = urllib.parse.quote(symbol, safe="")

    params = {
        "period1": int(start_dt.timestamp()),
        "period2": int(end_dt.timestamp()),
        "interval": interval,
        "includePrePost": "false",
        "events": "div,splits",
    }
    crumb = _get_yahoo_crumb()
    if crumb:
        params["crumb"] = crumb

    last_err = None
    for host in YAHOO_HOSTS:
        url = f"https://{host}/v8/finance/chart/{encoded_symbol}"
        try:
            resp = _yahoo_session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 429:
                raise RuntimeError(f"HTTP 429 Too Many Requests from {host}")
            resp.raise_for_status()
            payload = json.loads(resp.text)
            chart = payload.get("chart", {})
            if chart.get("error"):
                raise ValueError(f"Yahoo chart API error for {symbol}: {chart['error']}")
            results = chart.get("result")
            if not results:
                raise ValueError(f"empty chart result for {symbol}")
            return results[0]
        except Exception as e:
            last_err = e
            continue
    raise last_err if last_err is not None else RuntimeError(f"failed to fetch {symbol}")


def parse_chart_result(result, symbol):
    """Turn one Yahoo chart 'result' object into an Open/High/Low/Close/Volume
    DataFrame indexed by UTC datetime -- identical shape to what the rest of
    this pipeline (quant engine, reporting) already expects."""
    timestamps = result.get("timestamp")
    quotes = result.get("indicators", {}).get("quote", [{}])
    if not timestamps or not quotes:
        raise ValueError(f"malformed chart payload for {symbol}")
    quote = quotes[0]
    idx = pd.to_datetime(timestamps, unit="s", utc=True)
    df = pd.DataFrame(
        {
            "Open": quote.get("open"),
            "High": quote.get("high"),
            "Low": quote.get("low"),
            "Close": quote.get("close"),
            "Volume": quote.get("volume"),
        },
        index=idx,
    )
    df = df.dropna(how="all")
    if df.empty:
        raise ValueError(f"no usable bars for {symbol}")
    return df


def download_yf(symbol, period, interval):
    lookback = PERIOD_LOOKBACK.get(period, timedelta(days=31))

    def _dl():
        result = fetch_yahoo_chart(symbol, lookback, interval)
        return parse_chart_result(result, symbol)

    result = retry_call(_dl, label=f"Yahoo chart {symbol} [{interval}]")
    return result if result is not None else pd.DataFrame()


def download_cftc_text(url=CFTC_DISAGG_URL):
    def _dl():
        headers = {"User-Agent": YAHOO_HEADERS["User-Agent"]}
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
        resp.raise_for_status()
        return resp.text

    return retry_call(_dl, label="CFTC COT report download")


# ============================================================================
# CFTC COMMITMENTS OF TRADERS (COT) SCRAPER
# ============================================================================

def _first_present(columns, candidates):
    cols = set(columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def parse_cot_gold(raw_text):
    """Resilient parse of the CFTC disaggregated COT text file for GOLD (COMEX)."""
    if not raw_text:
        return None
    try:
        df = pd.read_csv(io.StringIO(raw_text), engine="python")
    except Exception as e:
        log_error(f"CFTC text failed CSV parse: {e}")
        return None

    df.columns = [str(c).strip().strip('"') for c in df.columns]

    name_col = _first_present(df.columns, COT_NAME_COL_CANDIDATES)
    date_col = _first_present(df.columns, COT_DATE_COL_CANDIDATES)
    mm_long_col = _first_present(df.columns, COT_MM_LONG_CANDIDATES)
    mm_short_col = _first_present(df.columns, COT_MM_SHORT_CANDIDATES)

    if not name_col or not mm_long_col or not mm_short_col:
        log_warn("CFTC schema drift detected -- required columns not found, skipping COT block.")
        return None

    names = df[name_col].astype(str)
    gold_df = df[names.str.contains("GOLD", case=False, na=False) &
                 names.str.contains("COMMODITY EXCHANGE", case=False, na=False)]
    if gold_df.empty:
        gold_df = df[names.str.contains("GOLD", case=False, na=False)]
    if gold_df.empty:
        log_warn("No GOLD rows located in CFTC report.")
        return None

    if date_col:
        gold_df = gold_df.sort_values(date_col)
        report_date = str(gold_df.iloc[-1][date_col])
    else:
        report_date = "UNKNOWN"

    latest = gold_df.iloc[-1]
    mm_long = pd.to_numeric(latest[mm_long_col], errors="coerce")
    mm_short = pd.to_numeric(latest[mm_short_col], errors="coerce")
    net = (mm_long - mm_short) if pd.notna(mm_long) and pd.notna(mm_short) else np.nan

    return {
        "contract_name": str(latest[name_col]).strip(),
        "report_date": report_date,
        "managed_money_long": mm_long,
        "managed_money_short": mm_short,
        "net_hedge_fund_position": net,
    }


# ============================================================================
# QUANT ENGINE  (fully vectorized - no python-level loops)
# ============================================================================

def _strip_tz(index):
    return index.tz_convert(None) if getattr(index, "tz", None) is not None else index


def true_range(df):
    prev_close = df["Close"].shift(1)
    tr = pd.concat(
        [
            df["High"] - df["Low"],
            (df["High"] - prev_close).abs(),
            (df["Low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr


def compute_intraday_quant_engine(df):
    """VWAP, rolling-20 VWAP Z-Score, Cumulative Volume Delta proxy, Liquidity Density."""
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df.index = _strip_tz(df.index)
    day_key = df.index.date

    typical_price = (df["High"] + df["Low"] + df["Close"]) / 3.0
    pv = typical_price * df["Volume"]

    cum_pv = pv.groupby(day_key).cumsum()
    cum_vol = df["Volume"].groupby(day_key).cumsum()
    df["VWAP"] = cum_pv / cum_vol.replace(0, np.nan)

    vwap_dev = df["Close"] - df["VWAP"]
    roll_mean = vwap_dev.rolling(20).mean()
    roll_std = vwap_dev.rolling(20).std()
    df["VWAP_Z"] = (vwap_dev - roll_mean) / roll_std.replace(0, np.nan)

    candle_range = (df["High"] - df["Low"]).replace(0, np.nan)
    buy_vol = df["Volume"] * (df["Close"] - df["Low"]) / candle_range
    sell_vol = df["Volume"] * (df["High"] - df["Close"]) / candle_range
    delta = (buy_vol - sell_vol).fillna(0.0)
    df["CVD"] = delta.cumsum()

    tr = true_range(df).replace(0, np.nan)
    df["Liquidity_Density"] = df["Volume"] / tr

    return df


def compute_dxy_roc(df, bars=3):
    """45-minute Rate of Change on 15-minute bars (3 bars = 45 minutes)."""
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df.index = _strip_tz(df.index)
    df["ROC_45m_pct"] = df["Close"].pct_change(periods=bars) * 100.0
    return df


def compute_gold_silver_ratio(gold_df, silver_df):
    if gold_df is None or silver_df is None or gold_df.empty or silver_df.empty:
        return pd.DataFrame()

    g = gold_df[["Close"]].rename(columns={"Close": "Gold_Close"}).sort_index()
    s = silver_df[["Close"]].rename(columns={"Close": "Silver_Close"}).sort_index()
    g.index = _strip_tz(g.index)
    s.index = _strip_tz(s.index)
    g.index.name = "Datetime"
    s.index.name = "Datetime"

    merged = pd.merge_asof(
        g.reset_index(),
        s.reset_index(),
        on="Datetime",
        direction="nearest",
        tolerance=pd.Timedelta("20min"),
    )
    merged["GSR"] = merged["Gold_Close"] / merged["Silver_Close"]
    merged = merged.set_index("Datetime").dropna(subset=["GSR"])
    return merged


def compute_daily_pivots(daily_df):
    """Standard floor pivots (PP, R1, S1) from the last fully completed daily candle."""
    if daily_df is None or daily_df.empty or len(daily_df) < 2:
        return None

    df = daily_df.copy()
    df.index = _strip_tz(df.index)

    today = datetime.now().date()
    last_date = df.index[-1].date()
    basis_row = df.iloc[-2] if last_date == today else df.iloc[-1]

    H, L, C = float(basis_row["High"]), float(basis_row["Low"]), float(basis_row["Close"])
    pp = (H + L + C) / 3.0
    r1 = 2 * pp - L
    s1 = 2 * pp - H

    return {"date": basis_row.name.date().isoformat(), "High": H, "Low": L, "Close": C,
            "PP": pp, "R1": r1, "S1": s1}


# ============================================================================
# REPORTING / OUTPUT LAYER
# ============================================================================

def safe_num(x, nd=4):
    try:
        if x is None:
            return "NA"
        xf = float(x)
        if np.isnan(xf) or np.isinf(xf):
            return "NA"
        return round(xf, nd)
    except (TypeError, ValueError):
        return "NA"


def last_val(df, col):
    try:
        if df is None or df.empty or col not in df.columns:
            return None
        s = df[col].dropna()
        return s.iloc[-1] if not s.empty else None
    except Exception:
        return None


def print_csv_section(title, df, tail=None):
    emit("-" * 80)
    emit(f"[{title}")
    if df is None or df.empty:
        emit("NO_DATA_AVAILABLE")
        emit("-" * 80)
        return
    out = df.tail(tail) if tail else df
    out = out.reset_index()
    out.columns = ["Datetime"] + list(out.columns[1:])
    csv_text = out.to_csv(index=False, float_format="%.6f")
    emit(csv_text.rstrip("\n"))
    emit("-" * 80)


def build_summary_rows(gold_macro, gold_struct, gold_intra_q, silver_intra,
                        gsr_df, dxy_intra_q, gvz_intra, tnx_macro, pivots, cot_data):
    rows = [
        ("Gold_Macro_Last_Daily_Close", safe_num(last_val(gold_macro, "Close"), 2)),
        ("Gold_Struct_Last_Hourly_Close", safe_num(last_val(gold_struct, "Close"), 2)),
        ("Gold_Intraday_Last_15m_Close", safe_num(last_val(gold_intra_q, "Close"), 2)),
        ("Gold_Intraday_Last_VWAP", safe_num(last_val(gold_intra_q, "VWAP"), 2)),
        ("Gold_Intraday_Last_VWAP_Z_Score", safe_num(last_val(gold_intra_q, "VWAP_Z"), 3)),
        ("Gold_Intraday_Last_CVD", safe_num(last_val(gold_intra_q, "CVD"), 2)),
        ("Gold_Intraday_Last_Liquidity_Density", safe_num(last_val(gold_intra_q, "Liquidity_Density"), 3)),
        ("Silver_Intraday_Last_15m_Close", safe_num(last_val(silver_intra, "Close"), 3)),
        ("Gold_Silver_Ratio_Last", safe_num(last_val(gsr_df, "GSR"), 3)),
        ("DXY_Last_15m_Close", safe_num(last_val(dxy_intra_q, "Close"), 3)),
        ("DXY_ROC_45m_pct", safe_num(last_val(dxy_intra_q, "ROC_45m_pct"), 4)),
        ("GVZ_Implied_Vol_Last", safe_num(last_val(gvz_intra, "Close"), 2)),
        ("TNX_10Y_Yield_Last_pct", safe_num(last_val(tnx_macro, "Close"), 3)),
    ]

    if pivots:
        rows += [
            ("Pivot_Basis_Date", pivots["date"]),
            ("Pivot_PP", safe_num(pivots["PP"], 2)),
            ("Pivot_R1", safe_num(pivots["R1"], 2)),
            ("Pivot_S1", safe_num(pivots["S1"], 2)),
        ]
    else:
        rows += [("Pivot_Basis_Date", "NA"), ("Pivot_PP", "NA"), ("Pivot_R1", "NA"), ("Pivot_S1", "NA")]

    if cot_data:
        rows += [
            ("COT_Contract", cot_data["contract_name"]),
            ("COT_Report_Date", cot_data["report_date"]),
            ("COT_Managed_Money_Long", safe_num(cot_data["managed_money_long"], 0)),
            ("COT_Managed_Money_Short", safe_num(cot_data["managed_money_short"], 0)),
            ("COT_Net_Hedge_Fund_Position", safe_num(cot_data["net_hedge_fund_position"], 0)),
        ]
    else:
        rows += [
            ("COT_Contract", "NA"), ("COT_Report_Date", "NA"),
            ("COT_Managed_Money_Long", "NA"), ("COT_Managed_Money_Short", "NA"),
            ("COT_Net_Hedge_Fund_Position", "NA"),
        ]

    return rows


# ============================================================================
# DATA FETCH ORCHESTRATION
# ============================================================================

# Streamlit reruns this whole script top-to-bottom on every page load and
# every widget interaction. Without caching, that means a fresh round of
# Yahoo/CFTC requests every single rerun -- which is what was tripping the
# 429s. @st.cache_data memoizes the return value for `ttl` seconds (15 min
# here), so within that window a rerun reuses the cached DataFrames/dict
# instead of hitting the network again. An explicit pause between each
# ticker request additionally spreads out the requests that DO happen on a
# cache miss, on top of the existing per-attempt pacing/backoff inside
# retry_call().
TICKER_FETCH_DELAY_SEC = 2


@st.cache_data(ttl=900, show_spinner="Fetching market data...")
def fetch_all_market_data():
    gold_macro = download_yf(**TICKERS["GOLD_MACRO"])
    time.sleep(TICKER_FETCH_DELAY_SEC)
    gold_struct = download_yf(**TICKERS["GOLD_STRUCT"])
    time.sleep(TICKER_FETCH_DELAY_SEC)
    gold_intra = download_yf(**TICKERS["GOLD_INTRADAY"])
    time.sleep(TICKER_FETCH_DELAY_SEC)
    silver_intra = download_yf(**TICKERS["SILVER_INTRADAY"])
    time.sleep(TICKER_FETCH_DELAY_SEC)
    dxy_intra = download_yf(**TICKERS["DXY_INTRADAY"])
    time.sleep(TICKER_FETCH_DELAY_SEC)
    gvz_intra = download_yf(**TICKERS["GVZ_INTRADAY"])
    time.sleep(TICKER_FETCH_DELAY_SEC)
    tnx_macro = download_yf(**TICKERS["TNX_MACRO"])
    time.sleep(TICKER_FETCH_DELAY_SEC)

    cot_text = download_cftc_text()
    cot_data = parse_cot_gold(cot_text)

    return gold_macro, gold_struct, gold_intra, silver_intra, dxy_intra, gvz_intra, tnx_macro, cot_data


# ============================================================================
# MAIN
# ============================================================================

def main():
    emit("=" * 80)
    emit("GOLD (XAUUSD) INSTITUTIONAL QUANT DATA PIPELINE")
    emit(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC  |  Yahoo raw-JSON (no yfinance)")
    emit("=" * 80)

    # ---- 1. Fetch phase (cached 15 min; each call retries up to RETRY_ATTEMPTS times) ----
    (gold_macro, gold_struct, gold_intra, silver_intra,
     dxy_intra, gvz_intra, tnx_macro, cot_data) = fetch_all_market_data()

    # ---- 2. Quant engine (vectorized) ----
    gold_intra_q = compute_intraday_quant_engine(gold_intra)
    dxy_intra_q = compute_dxy_roc(dxy_intra, bars=3)
    gsr_df = compute_gold_silver_ratio(gold_intra, silver_intra)
    pivots = compute_daily_pivots(gold_macro)

    # ---- 3. Report ----
    summary_rows = build_summary_rows(
        gold_macro, gold_struct, gold_intra_q, silver_intra,
        gsr_df, dxy_intra_q, gvz_intra, tnx_macro, pivots, cot_data,
    )

    emit("-" * 80)
    emit("[1] MARKET SNAPSHOT SUMMARY (CSV)")
    emit("-" * 80)
    emit("Metric,Value")
    for k, v in summary_rows:
        emit(f"{k},{v}")
    emit("-" * 80)

    print_csv_section("2] GOLD MACRO - 1MO / 1D (tail)", gold_macro, tail=TAIL_DAILY)
    print_csv_section("3] GOLD STRUCTURAL - 7D / 1H (tail)", gold_struct, tail=TAIL_HOURLY)
    print_csv_section("4] GOLD INTRADAY QUANT ENGINE - 3D / 15M (tail)", gold_intra_q, tail=TAIL_15M)
    print_csv_section("5] SILVER INTRADAY - 3D / 15M (tail)", silver_intra, tail=TAIL_15M)

    gsr_out = gsr_df[["Gold_Close", "Silver_Close", "GSR"]] if not gsr_df.empty else gsr_df
    print_csv_section("6] GOLD-SILVER RATIO - 15M (tail)", gsr_out, tail=TAIL_15M)

    print_csv_section("7] US DOLLAR INDEX (DXY) - 3D / 15M + 45M ROC (tail)", dxy_intra_q, tail=TAIL_15M)
    print_csv_section("8] GOLD VIX (GVZ) IMPLIED VOL - 3D / 15M (tail)", gvz_intra, tail=TAIL_15M)
    print_csv_section("9] US 10Y TREASURY YIELD (TNX) - 1MO / 1D (tail)", tnx_macro, tail=TAIL_DAILY)

    emit("=" * 80)
    emit("END OF OUTPUT -- LONG-PRESS ABOVE, SELECT ALL, COPY")
    emit("=" * 80)

    output_string = "\n".join(_output_lines)
    st.code(output_string, language="csv")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error("Unhandled exception in pipeline:")
        st.exception(e)
