#!/usr/bin/env python3
"""
Gold (XAUUSD) Institutional Quant Data Pipeline
=================================================
Multi-timeframe Gold/Silver/DXY/GVZ/TNX market data + live CFTC Commitments
of Traders (COT) fundamentals, reduced to a dense, copy-pasteable CSV report.

Built to run unmodified inside Pydroid 3 on Android (tested target: Pixel 6).

REQUIRED PACKAGES (install once inside Pydroid 3 -> Pip):
    pip install yfinance pandas numpy requests

USAGE:
    Run the script (Pydroid 3 "Run" button, or `python gold_quant_pipeline.py`
    in the Pydroid terminal). Wait for the final report, then long-press to
    "Select All" + "Copy" the block between the dashed borders.
"""

import io
import sys
import time
import traceback
from datetime import datetime, timezone

import numpy as np
import pandas as pd

try:
    import yfinance as yf
except ImportError:
    print("Missing dependency 'yfinance'. In Pydroid 3, open Pip and install: yfinance")
    sys.exit(1)

try:
    # Force real exceptions (incl. rate limits) to surface instead of being
    # silently logged-and-swallowed, so our retry/backoff logic can react.
    yf.config.debug.hide_exceptions = False
except Exception:
    pass

try:
    import requests
    _HAVE_REQUESTS = True
except ImportError:
    _HAVE_REQUESTS = False
    import urllib.request

pd.options.mode.chained_assignment = None

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
    if type(exc).__name__ == "YFRateLimitError":
        return True
    msg = str(exc).lower()
    return any(k in msg for k in RATE_LIMIT_KEYWORDS)


def _throttle_before_request():
    """Wait out any active rate-limit cooldown, else apply baseline pacing."""
    now = time.time()
    if now < _net_state["cooldown_until"]:
        wait = _net_state["cooldown_until"] - now
        print(f"[INFO] Rate-limit cooldown active -- waiting {wait:.0f}s before next request...")
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
            print(f"[WARN] {label}: attempt {i}/{attempts} failed -> {e}")
            if _is_rate_limit_error(e):
                _net_state["cooldown_until"] = time.time() + RATE_LIMIT_COOLDOWN_SEC
            elif i < attempts:
                time.sleep(RETRY_BASE_DELAY_SEC * (2 ** (i - 1)))
    print(f"[ERROR] {label}: all {attempts} attempts failed -> {last_err}")
    return None


def download_yf(symbol, period, interval):
    def _dl():
        # yf.download() silently swallows per-ticker errors (incl. rate
        # limits) into a log line and returns an empty frame, which defeats
        # our retry/backoff logic. Ticker.history() raises real exceptions
        # (YFRateLimitError always propagates regardless of config), so we
        # call it directly and can react to what actually went wrong.
        df = yf.Ticker(symbol).history(
            period=period,
            interval=interval,
            auto_adjust=False,
            actions=False,
            timeout=REQUEST_TIMEOUT,
        )
        if df is None or df.empty:
            raise ValueError(f"no data for {symbol}")
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df.dropna(how="all")

    result = retry_call(_dl, label=f"yfinance {symbol} [{interval}]")
    return result if result is not None else pd.DataFrame()


def download_cftc_text(url=CFTC_DISAGG_URL):
    def _dl():
        headers = {"User-Agent": "Mozilla/5.0 (Linux; Android 13; Pixel 6) GoldQuantPipeline/1.0"}
        if _HAVE_REQUESTS:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
            resp.raise_for_status()
            return resp.text
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as r:
            return r.read().decode("utf-8", errors="ignore")

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
        print(f"[ERROR] CFTC text failed CSV parse: {e}")
        return None

    df.columns = [str(c).strip().strip('"') for c in df.columns]

    name_col = _first_present(df.columns, COT_NAME_COL_CANDIDATES)
    date_col = _first_present(df.columns, COT_DATE_COL_CANDIDATES)
    mm_long_col = _first_present(df.columns, COT_MM_LONG_CANDIDATES)
    mm_short_col = _first_present(df.columns, COT_MM_SHORT_CANDIDATES)

    if not name_col or not mm_long_col or not mm_short_col:
        print("[WARN] CFTC schema drift detected -- required columns not found, skipping COT block.")
        return None

    names = df[name_col].astype(str)
    gold_df = df[names.str.contains("GOLD", case=False, na=False) &
                 names.str.contains("COMMODITY EXCHANGE", case=False, na=False)]
    if gold_df.empty:
        gold_df = df[names.str.contains("GOLD", case=False, na=False)]
    if gold_df.empty:
        print("[WARN] No GOLD rows located in CFTC report.")
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
    print("-" * 80)
    print(f"[{title}")
    if df is None or df.empty:
        print("NO_DATA_AVAILABLE")
        print("-" * 80)
        return
    out = df.tail(tail) if tail else df
    out = out.reset_index()
    out.columns = ["Datetime"] + list(out.columns[1:])
    csv_text = out.to_csv(index=False, float_format="%.6f")
    print(csv_text.rstrip("\n"))
    print("-" * 80)


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
# MAIN
# ============================================================================

def main():
    print("=" * 80)
    print("GOLD (XAUUSD) INSTITUTIONAL QUANT DATA PIPELINE")
    print(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC  |  Pydroid3 / Pixel6")
    print("=" * 80)

    # ---- 1. Fetch phase (each call retries up to RETRY_ATTEMPTS times) ----
    gold_macro = download_yf(**TICKERS["GOLD_MACRO"])
    gold_struct = download_yf(**TICKERS["GOLD_STRUCT"])
    gold_intra = download_yf(**TICKERS["GOLD_INTRADAY"])
    silver_intra = download_yf(**TICKERS["SILVER_INTRADAY"])
    dxy_intra = download_yf(**TICKERS["DXY_INTRADAY"])
    gvz_intra = download_yf(**TICKERS["GVZ_INTRADAY"])
    tnx_macro = download_yf(**TICKERS["TNX_MACRO"])

    cot_text = download_cftc_text()
    cot_data = parse_cot_gold(cot_text)

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

    print("-" * 80)
    print("[1] MARKET SNAPSHOT SUMMARY (CSV)")
    print("-" * 80)
    print("Metric,Value")
    for k, v in summary_rows:
        print(f"{k},{v}")
    print("-" * 80)

    print_csv_section("2] GOLD MACRO - 1MO / 1D (tail)", gold_macro, tail=TAIL_DAILY)
    print_csv_section("3] GOLD STRUCTURAL - 7D / 1H (tail)", gold_struct, tail=TAIL_HOURLY)
    print_csv_section("4] GOLD INTRADAY QUANT ENGINE - 3D / 15M (tail)", gold_intra_q, tail=TAIL_15M)
    print_csv_section("5] SILVER INTRADAY - 3D / 15M (tail)", silver_intra, tail=TAIL_15M)

    gsr_out = gsr_df[["Gold_Close", "Silver_Close", "GSR"]] if not gsr_df.empty else gsr_df
    print_csv_section("6] GOLD-SILVER RATIO - 15M (tail)", gsr_out, tail=TAIL_15M)

    print_csv_section("7] US DOLLAR INDEX (DXY) - 3D / 15M + 45M ROC (tail)", dxy_intra_q, tail=TAIL_15M)
    print_csv_section("8] GOLD VIX (GVZ) IMPLIED VOL - 3D / 15M (tail)", gvz_intra, tail=TAIL_15M)
    print_csv_section("9] US 10Y TREASURY YIELD (TNX) - 1MO / 1D (tail)", tnx_macro, tail=TAIL_DAILY)

    print("=" * 80)
    print("END OF OUTPUT -- LONG-PRESS ABOVE, SELECT ALL, COPY")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("-" * 80)
        print("[FATAL] Unhandled exception in pipeline:")
        traceback.print_exc()
        print("-" * 80)
        sys.exit(1)
