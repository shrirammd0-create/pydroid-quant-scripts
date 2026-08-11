#!/usr/bin/env python3
"""
XAUUSD Quant Data & Alerting Pipeline -- pre-fetch worker
============================================================
Fetches multi-asset market data (Gold, Silver, DXY, GVZ, AUD/EUR/JPY) from
Yahoo Finance's public chart JSON endpoint, computes standard quant metrics
(ATR, floor pivots, VWAP, volume z-score, body ratio), maps Smart Money
Concepts (fair value gaps, liquidity sweeps), pulls CFTC COT positioning and
the high-impact economic calendar, runs a fixed-fraction position sizer, and
fires a push alert to ntfy.sh when a breakout setup passes the risk gate.

Outputs:
    market_data.txt -- dense human-readable CSV report (copy-friendly)
    data.json       -- structured metrics consumed by app.py (Streamlit)

This script has no Streamlit dependency; run it on a schedule (GitHub
Actions) and let app.py just read the files it writes.

REQUIRED:  pip install requests pandas numpy
OPTIONAL:  pip install cot_reports   (COT falls back to the CFTC disagg
           text feed when the library is missing or its download fails)

NOTE: ATR is computed natively with pandas (true-range rolling mean).
pandas-ta cannot be installed on Python 3.11 -- its current releases
require Python >= 3.12 and no 3.11-compatible build remains on PyPI.
"""

import io
import sys
import time
import json
import math
import pathlib
import traceback
import urllib.parse
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

try:
    import requests
except ImportError:
    print("Missing dependency 'requests': pip install requests")
    sys.exit(1)

pd.options.mode.chained_assignment = None

BASE_DIR = pathlib.Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "market_data.txt"
JSON_FILE = BASE_DIR / "data.json"

# ============================================================================
# CONFIG
# ============================================================================

RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY_SEC = 5        # exponential for generic errors: 5s, 10s
FETCH_PACING_SEC = 3            # proactive gap before every outbound request
RATE_LIMIT_COOLDOWN_SEC = 5     # short flat cooldown on 429s (see fail-fast note below)
REQUEST_TIMEOUT = 20
TICKER_FETCH_DELAY_SEC = 2      # mandatory pause between ticker requests

RATE_LIMIT_KEYWORDS = ("rate limit", "ratelimit", "too many requests", "429")

# Rows kept in each printed CSV block (mobile-copy friendly, still dense)
TAIL_DAILY = 15
TAIL_15M = 40

# Every asset gets a 1-month daily series (ATR/pivots) and a 3-day 15m series
# (last price, VWAP/volume metrics, SMC scanning for gold).
ASSETS = {
    "GC=F":      {"label": "Gold",     "decimals": 2},
    "SI=F":      {"label": "Silver",   "decimals": 3},
    "DX-Y.NYB":  {"label": "DXY",      "decimals": 3},
    "^GVZ":      {"label": "Gold VIX", "decimals": 2},
    "AUDUSD=X":  {"label": "AUD/USD",  "decimals": 5},
    "EURUSD=X":  {"label": "EUR/USD",  "decimals": 5},
    "USDJPY=X":  {"label": "USD/JPY",  "decimals": 3},
}

PERIOD_LOOKBACK = {
    "1mo": timedelta(days=32),
    "3d": timedelta(days=4),
}

# ---- Account risk model (XAUUSD position sizer) ---------------------------
ACCOUNT_SIZE_USD = 5000.0
MAX_RISK_PCT = 0.005                      # 0.5% -> $25
MAX_RISK_USD = ACCOUNT_SIZE_USD * MAX_RISK_PCT
STOP_ATR_MULT = 1.5
GOLD_USD_PER_POINT_PER_LOT = 100.0        # 1 lot XAUUSD = 100 oz -> $100 per $1 move
MIN_LOT = 0.01

# ---- Alert engine ----------------------------------------------------------
NTFY_URL = "https://ntfy.sh/shriram_quant_alerts_99"
ALERT_VOL_Z_MIN = 1.5
ALERT_BODY_RATIO_MIN = 0.70

# ---- Economic calendar ------------------------------------------------------
CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

# ---- CFTC -------------------------------------------------------------------
CFTC_DISAGG_URL = "https://www.cftc.gov/dea/newcot/f_disagg.txt"
COT_GOLD_NAME = "GOLD - COMMODITY EXCHANGE INC."


# ============================================================================
# NETWORK LAYER
# ============================================================================

_net_state = {"cooldown_until": 0.0}


def _is_rate_limit_error(exc):
    msg = str(exc).lower()
    return any(k in msg for k in RATE_LIMIT_KEYWORDS)


def _throttle_before_request():
    now = time.time()
    if now < _net_state["cooldown_until"]:
        wait = _net_state["cooldown_until"] - now
        print(f"[INFO] Rate-limit cooldown active -- waiting {wait:.0f}s before next request...")
        time.sleep(wait)
    else:
        time.sleep(FETCH_PACING_SEC)


def retry_call(func, *args, attempts=RETRY_ATTEMPTS, label="operation", **kwargs):
    """Retry wrapper: proactive pacing, exponential backoff for generic
    errors, and a short flat cooldown on rate limits. Deliberately fail-fast:
    this runs unattended in CI where a hung retry loop is worse than a gap in
    the data -- if Yahoo is blocking the runner IP outright, waiting longer
    doesn't help, and the cooldown is shared across every remaining request."""
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


# ---- Yahoo Finance raw JSON chart endpoint ---------------------------------

YAHOO_HOSTS = ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

_yahoo_session = requests.Session()
_yahoo_session.headers.update(BROWSER_HEADERS)
_crumb_state = {"value": None, "attempted": False}


def _get_yahoo_crumb():
    """Warm up session cookies on finance.yahoo.com, then pull the crumb
    token from the getcrumb endpoint. Best-effort: chart requests usually
    work without a crumb, so failure here is not fatal. Tried once per run."""
    if _crumb_state["attempted"]:
        return _crumb_state["value"]
    _crumb_state["attempted"] = True
    try:
        _yahoo_session.get("https://finance.yahoo.com", timeout=REQUEST_TIMEOUT)
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
        return parse_chart_result(fetch_yahoo_chart(symbol, lookback, interval), symbol)

    result = retry_call(_dl, label=f"Yahoo chart {symbol} [{interval}]")
    return result if result is not None else pd.DataFrame()


# ============================================================================
# COT (Commitments of Traders)
# ============================================================================

def _first_matching_col(columns, *required_substrings):
    for c in columns:
        cl = str(c).lower()
        if all(s in cl for s in required_substrings):
            return c
    return None


def fetch_cot_legacy():
    """Primary path: cot_reports library, legacy_fut report, non-commercial
    (hedge fund) positioning for COMEX gold. Raises on any failure so the
    caller can fall back."""
    import cot_reports  # imported lazily: optional dependency

    year = datetime.now(timezone.utc).year
    df = cot_reports.cot_year(year=year, cot_report_type="legacy_fut")
    if df is None or len(df) == 0:
        raise ValueError("cot_reports returned no rows")

    name_col = _first_matching_col(df.columns, "market", "exchange", "name")
    long_col = _first_matching_col(df.columns, "noncommercial", "long", "all")
    short_col = _first_matching_col(df.columns, "noncommercial", "short", "all")
    date_col = _first_matching_col(df.columns, "as of date", "yyyy-mm-dd") or \
        _first_matching_col(df.columns, "report_date")
    if not name_col or not long_col or not short_col:
        raise ValueError(f"legacy_fut schema drift, columns: {list(df.columns)[:8]}...")

    names = df[name_col].astype(str)
    gold = df[names.str.contains("GOLD", case=False, na=False) &
              names.str.contains("COMMODITY EXCHANGE", case=False, na=False)]
    if gold.empty:
        raise ValueError("no COMEX GOLD rows in legacy_fut report")

    if date_col:
        gold = gold.sort_values(date_col)
    latest = gold.iloc[-1]

    long_pos = float(pd.to_numeric(latest[long_col], errors="coerce"))
    short_pos = float(pd.to_numeric(latest[short_col], errors="coerce"))
    if math.isnan(long_pos) or math.isnan(short_pos):
        raise ValueError("non-numeric COT positions")

    return {
        "source": "cot_reports legacy_fut (non-commercial)",
        "contract": str(latest[name_col]).strip(),
        "report_date": str(latest[date_col]) if date_col else "UNKNOWN",
        "long": long_pos,
        "short": short_pos,
        "net": long_pos - short_pos,
    }


def fetch_cot_disagg_fallback():
    """Fallback path: the CFTC disaggregated text feed (managed money =
    the hedge-fund category in disaggregated reports). Raises on failure."""
    def _dl():
        resp = requests.get(CFTC_DISAGG_URL, timeout=REQUEST_TIMEOUT, headers=BROWSER_HEADERS)
        resp.raise_for_status()
        return resp.text

    raw_text = retry_call(_dl, label="CFTC disagg COT download")
    if not raw_text:
        raise ValueError("CFTC disagg feed unavailable")

    df = pd.read_csv(io.StringIO(raw_text), engine="python")
    df.columns = [str(c).strip().strip('"') for c in df.columns]

    name_col = _first_matching_col(df.columns, "market_and_exchange_name")
    long_col = _first_matching_col(df.columns, "m_money", "long_all")
    short_col = _first_matching_col(df.columns, "m_money", "short_all")
    date_col = _first_matching_col(df.columns, "report_date_as_yyyy-mm-dd")
    if not name_col or not long_col or not short_col:
        raise ValueError("disagg schema drift -- required columns not found")

    names = df[name_col].astype(str)
    gold = df[names.str.contains("GOLD", case=False, na=False) &
              names.str.contains("COMMODITY EXCHANGE", case=False, na=False)]
    if gold.empty:
        gold = df[names.str.contains("GOLD", case=False, na=False)]
    if gold.empty:
        raise ValueError("no GOLD rows in disagg report")

    if date_col:
        gold = gold.sort_values(date_col)
    latest = gold.iloc[-1]

    long_pos = float(pd.to_numeric(latest[long_col], errors="coerce"))
    short_pos = float(pd.to_numeric(latest[short_col], errors="coerce"))
    if math.isnan(long_pos) or math.isnan(short_pos):
        raise ValueError("non-numeric COT positions in disagg feed")

    return {
        "source": "CFTC f_disagg.txt (managed money)",
        "contract": str(latest[name_col]).strip(),
        "report_date": str(latest[date_col]) if date_col else "UNKNOWN",
        "long": long_pos,
        "short": short_pos,
        "net": long_pos - short_pos,
    }


def fetch_cot():
    """COT with graceful degradation: cot_reports legacy_fut first, then the
    disagg text feed, then None -- the pipeline must survive CFTC being down."""
    try:
        return fetch_cot_legacy()
    except Exception as e:
        print(f"[WARN] cot_reports path failed ({e}); trying CFTC disagg fallback...")
    try:
        return fetch_cot_disagg_fallback()
    except Exception as e:
        print(f"[WARN] CFTC disagg fallback also failed ({e}); COT unavailable this run.")
    return None


# ============================================================================
# QUANT ENGINE  (vectorized pandas/numpy)
# ============================================================================

def _strip_tz(index):
    return index.tz_convert(None) if getattr(index, "tz", None) is not None else index


def true_range(df):
    prev_close = df["Close"].shift(1)
    return pd.concat(
        [df["High"] - df["Low"],
         (df["High"] - prev_close).abs(),
         (df["Low"] - prev_close).abs()],
        axis=1,
    ).max(axis=1)


def compute_atr14(daily_df, period=14):
    """Daily ATR: rolling mean of true range (native pandas -- see module
    docstring for why pandas-ta is not used)."""
    if daily_df is None or daily_df.empty or len(daily_df) < 2:
        return None
    atr = true_range(daily_df).rolling(period).mean().dropna()
    if atr.empty:  # not enough bars for the full window; fall back to what we have
        atr = true_range(daily_df).expanding(min_periods=2).mean().dropna()
    return float(atr.iloc[-1]) if not atr.empty else None


def compute_intraday_metrics(df):
    """15m session VWAP, 20-period volume z-score, latest candle body ratio.
    Volume-based metrics come out None for volumeless feeds (FX pairs, ^GVZ)."""
    if df is None or df.empty:
        return {"vwap": None, "volume_z": None, "body_ratio": None, "last_close": None}

    df = df.copy()
    df.index = _strip_tz(df.index)

    out = {"vwap": None, "volume_z": None, "body_ratio": None, "last_close": None}
    last = df.iloc[-1]
    out["last_close"] = float(last["Close"])

    rng = float(last["High"] - last["Low"])
    if rng > 0:
        out["body_ratio"] = abs(float(last["Close"] - last["Open"])) / rng

    vol = df["Volume"].fillna(0.0)
    if float(vol.sum()) > 0:
        day_key = df.index.date
        typical = (df["High"] + df["Low"] + df["Close"]) / 3.0
        cum_pv = (typical * vol).groupby(day_key).cumsum()
        cum_vol = vol.groupby(day_key).cumsum().replace(0, np.nan)
        vwap = cum_pv / cum_vol
        if pd.notna(vwap.iloc[-1]):
            out["vwap"] = float(vwap.iloc[-1])

        vol_mean = vol.rolling(20).mean()
        vol_std = vol.rolling(20).std().replace(0, np.nan)
        vol_z = (vol - vol_mean) / vol_std
        if pd.notna(vol_z.iloc[-1]):
            out["volume_z"] = float(vol_z.iloc[-1])

    return out


def compute_daily_pivots(daily_df):
    """Floor pivots (PP/R1/R2/S1/S2) + basis (yesterday's) High/Low from the
    last fully completed daily candle."""
    if daily_df is None or daily_df.empty or len(daily_df) < 2:
        return None

    df = daily_df.copy()
    df.index = _strip_tz(df.index)

    today = datetime.now(timezone.utc).date()
    basis_row = df.iloc[-2] if df.index[-1].date() == today else df.iloc[-1]

    H, L, C = float(basis_row["High"]), float(basis_row["Low"]), float(basis_row["Close"])
    if any(math.isnan(v) for v in (H, L, C)):
        return None
    pp = (H + L + C) / 3.0
    rng = H - L
    return {
        "basis_date": basis_row.name.date().isoformat(),
        "prev_high": H, "prev_low": L, "prev_close": C,
        "PP": pp,
        "R1": 2 * pp - L, "R2": pp + rng,
        "S1": 2 * pp - H, "S2": pp - rng,
    }


# ---- Smart Money Concepts ---------------------------------------------------

def detect_fvgs(df, max_reported=5):
    """3-candle fair value gaps on the 15m series.
    Bullish: candle-1 High < candle-3 Low (gap zone [High1, Low3]).
    Bearish: candle-1 Low > candle-3 High (gap zone [High3, Low1]).
    A gap is OPEN until later price fully trades through the zone."""
    if df is None or df.empty or len(df) < 3:
        return []

    df = df.copy()
    df.index = _strip_tz(df.index)
    high1 = df["High"].shift(2)
    low1 = df["Low"].shift(2)

    bull_mask = high1 < df["Low"]
    bear_mask = low1 > df["High"]

    lows_after = df["Low"].values
    highs_after = df["High"].values
    n = len(df)

    gaps = []
    for i in np.flatnonzero(bull_mask.values | bear_mask.values):
        if bool(bull_mask.iloc[i]):
            direction, bottom, top = "Bullish", float(high1.iloc[i]), float(df["Low"].iloc[i])
            filled = bool(i + 1 < n and lows_after[i + 1:].min() <= bottom)
        else:
            direction, bottom, top = "Bearish", float(df["High"].iloc[i]), float(low1.iloc[i])
            filled = bool(i + 1 < n and highs_after[i + 1:].max() >= top)
        gaps.append({
            "time": df.index[i].strftime("%Y-%m-%d %H:%M"),
            "direction": direction,
            "zone_low": round(min(bottom, top), 2),
            "zone_high": round(max(bottom, top), 2),
            "status": "FILLED" if filled else "OPEN",
        })

    open_gaps = [g for g in gaps if g["status"] == "OPEN"]
    return open_gaps[-max_reported:]


def detect_liquidity_sweep(intraday_df, pivots):
    """Did the latest 15m candle wick past the previous daily high/low but
    close back inside the prior day's range?"""
    result = {"swept_pdh": False, "swept_pdl": False, "detail": "No sweep on current candle"}
    if intraday_df is None or intraday_df.empty or not pivots:
        result["detail"] = "Insufficient data"
        return result

    last = intraday_df.iloc[-1]
    pdh, pdl = pivots["prev_high"], pivots["prev_low"]

    if float(last["High"]) > pdh and float(last["Close"]) < pdh:
        result["swept_pdh"] = True
        result["detail"] = (f"Swept PDH {pdh:.2f} (high {float(last['High']):.2f}, "
                            f"closed back below at {float(last['Close']):.2f})")
    if float(last["Low"]) < pdl and float(last["Close"]) > pdl:
        result["swept_pdl"] = True
        detail = (f"Swept PDL {pdl:.2f} (low {float(last['Low']):.2f}, "
                  f"closed back above at {float(last['Close']):.2f})")
        result["detail"] = detail if not result["swept_pdh"] else result["detail"] + "; " + detail
    return result


# ============================================================================
# ECONOMIC CALENDAR
# ============================================================================

def fetch_high_impact_news():
    """Today's high-impact events from the ForexFactory weekly JSON mirror.
    Returns a list of dicts, or None when the feed itself is unreachable
    (distinct from a genuinely empty news day)."""
    def _dl():
        resp = requests.get(CALENDAR_URL, timeout=REQUEST_TIMEOUT, headers=BROWSER_HEADERS)
        resp.raise_for_status()
        return resp.text

    raw = retry_call(_dl, label="economic calendar download")
    if raw is None:
        return None

    try:
        events = json.loads(raw)
    except Exception as e:
        print(f"[WARN] calendar JSON parse failed: {e}")
        return None

    today_utc = datetime.now(timezone.utc).date()
    out = []
    for ev in events if isinstance(events, list) else []:
        try:
            if str(ev.get("impact", "")).lower() != "high":
                continue
            ev_dt = datetime.fromisoformat(str(ev.get("date")))
            ev_utc = ev_dt.astimezone(timezone.utc)
            if ev_utc.date() != today_utc:
                continue
            out.append({
                "time_utc": ev_utc.strftime("%H:%M"),
                "country": str(ev.get("country", "")),
                "title": str(ev.get("title", "")),
                "forecast": str(ev.get("forecast", "") or "-"),
                "previous": str(ev.get("previous", "") or "-"),
            })
        except Exception:
            continue
    out.sort(key=lambda e: e["time_utc"])
    return out


# ============================================================================
# RISK ENGINE  ($5k account, 0.5% max risk, 1.5x ATR stop, XAUUSD)
# ============================================================================

def compute_risk(gold_atr14):
    risk = {
        "account_usd": ACCOUNT_SIZE_USD,
        "max_risk_usd": round(MAX_RISK_USD, 2),
        "atr14": None, "stop_distance_usd": None,
        "lot_size": None, "tradeable": False,
        "status": "UN-TRADEABLE (No ATR data)",
        "contract_note": f"1 lot XAUUSD = 100 oz (${GOLD_USD_PER_POINT_PER_LOT:.0f} per $1 move)",
    }
    if gold_atr14 is None or gold_atr14 <= 0:
        return risk

    stop_distance = STOP_ATR_MULT * gold_atr14
    raw_lots = MAX_RISK_USD / (stop_distance * GOLD_USD_PER_POINT_PER_LOT)
    lots = math.floor(raw_lots * 100) / 100  # broker step 0.01

    risk.update({
        "atr14": round(gold_atr14, 2),
        "stop_distance_usd": round(stop_distance, 2),
        "lot_size": lots,
    })
    if lots >= MIN_LOT:
        risk["tradeable"] = True
        risk["status"] = "TRADEABLE"
    else:
        risk["status"] = "UN-TRADEABLE (Exceeds Risk Limit)"
    return risk


# ============================================================================
# ALERT ENGINE  (ntfy.sh push, deduplicated across runs via previous data.json)
# ============================================================================

def load_previous_alert_state():
    try:
        prev = json.loads(JSON_FILE.read_text(encoding="utf-8"))
        return bool(prev.get("alert", {}).get("active", False))
    except Exception:
        return False


def evaluate_breakout_alert(gold_metrics, pivots, risk):
    conditions = {
        "close_above_r1": False,
        "volume_z_above_1_5": False,
        "body_ratio_above_0_70": False,
        "passes_risk_limit": bool(risk.get("tradeable")),
    }
    close = gold_metrics.get("last_close")
    vol_z = gold_metrics.get("volume_z")
    body = gold_metrics.get("body_ratio")

    if pivots and close is not None:
        conditions["close_above_r1"] = close > pivots["R1"]
    if vol_z is not None:
        conditions["volume_z_above_1_5"] = vol_z > ALERT_VOL_Z_MIN
    if body is not None:
        conditions["body_ratio_above_0_70"] = body >= ALERT_BODY_RATIO_MIN

    return {"active": all(conditions.values()), "conditions": conditions}


def send_ntfy_alert(gold_metrics, pivots, risk):
    msg = (
        f"XAUUSD BREAKOUT: close {gold_metrics['last_close']:.2f} > R1 {pivots['R1']:.2f}\n"
        f"Vol Z: {gold_metrics['volume_z']:.2f} | Body: {gold_metrics['body_ratio']:.2f}\n"
        f"Risk: {risk['lot_size']:.2f} lot, stop ~${risk['stop_distance_usd']:.2f} "
        f"(1.5x ATR), max ${risk['max_risk_usd']:.2f}"
    )
    try:
        resp = requests.post(
            NTFY_URL,
            data=msg.encode("utf-8"),
            headers={"Title": "XAUUSD Breakout Setup", "Priority": "high",
                     "Tags": "chart_with_upwards_trend"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        print(f"[INFO] ntfy alert sent ({resp.status_code})")
        return True
    except Exception as e:
        print(f"[WARN] ntfy alert failed: {e}")
        return False


# ============================================================================
# REPORT / OUTPUT LAYER
# ============================================================================

_output_lines = []


def emit(line=""):
    _output_lines.append(line)


def safe_num(x, nd=4):
    try:
        if x is None:
            return "NA"
        xf = float(x)
        if math.isnan(xf) or math.isinf(xf):
            return "NA"
        return round(xf, nd)
    except (TypeError, ValueError):
        return "NA"


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
    emit(out.to_csv(index=False, float_format="%.6f").rstrip("\n"))
    emit("-" * 80)


# ============================================================================
# MAIN
# ============================================================================

def main():
    generated = datetime.now(timezone.utc)
    emit("=" * 80)
    emit("XAUUSD QUANT DATA & ALERTING PIPELINE")
    emit(f"Generated: {generated.strftime('%Y-%m-%d %H:%M:%S')} UTC  |  Yahoo raw-JSON (no yfinance)")
    emit("=" * 80)

    # ---- 1. Fetch: daily + 15m for every asset (2s between requests) ------
    daily, intraday = {}, {}
    for symbol in ASSETS:
        daily[symbol] = download_yf(symbol, "1mo", "1d")
        time.sleep(TICKER_FETCH_DELAY_SEC)
        intraday[symbol] = download_yf(symbol, "3d", "15m")
        time.sleep(TICKER_FETCH_DELAY_SEC)

    cot = fetch_cot()
    news = fetch_high_impact_news()

    # ---- 2. Quant engine ---------------------------------------------------
    asset_metrics = {}
    for symbol, cfg in ASSETS.items():
        m = compute_intraday_metrics(intraday[symbol])
        m["atr14"] = compute_atr14(daily[symbol])
        m["label"] = cfg["label"]
        m["decimals"] = cfg["decimals"]
        asset_metrics[symbol] = m

    gold_metrics = asset_metrics["GC=F"]
    pivots = compute_daily_pivots(daily["GC=F"])
    fvgs = detect_fvgs(intraday["GC=F"])
    sweep = detect_liquidity_sweep(intraday["GC=F"], pivots)
    risk = compute_risk(gold_metrics["atr14"])

    # ---- 3. Alert (dedup: only fire on inactive -> active transition) -----
    was_active = load_previous_alert_state()
    alert = evaluate_breakout_alert(gold_metrics, pivots, risk)
    alert["previously_active"] = was_active
    alert["sent"] = False
    if alert["active"] and not was_active:
        alert["sent"] = send_ntfy_alert(gold_metrics, pivots, risk)
    elif alert["active"]:
        print("[INFO] Breakout still active from previous run -- alert already sent, not repeating.")

    # ---- 4. market_data.txt ------------------------------------------------
    emit("-" * 80)
    emit("[1] MARKET SNAPSHOT SUMMARY (CSV)")
    emit("-" * 80)
    emit("Metric,Value")
    for symbol, m in asset_metrics.items():
        key = m["label"].replace("/", "").replace(" ", "_")
        emit(f"{key}_Last_15m_Close,{safe_num(m['last_close'], m['decimals'])}")
        emit(f"{key}_Daily_ATR14,{safe_num(m['atr14'], m['decimals'])}")
    emit(f"Gold_VWAP,{safe_num(gold_metrics['vwap'], 2)}")
    emit(f"Gold_Volume_Z_20,{safe_num(gold_metrics['volume_z'], 3)}")
    emit(f"Gold_Body_Ratio,{safe_num(gold_metrics['body_ratio'], 3)}")
    if pivots:
        emit(f"Pivot_Basis_Date,{pivots['basis_date']}")
        emit(f"Prev_Day_High,{safe_num(pivots['prev_high'], 2)}")
        emit(f"Prev_Day_Low,{safe_num(pivots['prev_low'], 2)}")
        for k in ("PP", "R1", "R2", "S1", "S2"):
            emit(f"Pivot_{k},{safe_num(pivots[k], 2)}")
    else:
        for k in ("Pivot_Basis_Date", "Prev_Day_High", "Prev_Day_Low",
                  "Pivot_PP", "Pivot_R1", "Pivot_R2", "Pivot_S1", "Pivot_S2"):
            emit(f"{k},NA")
    if cot:
        emit(f"COT_Source,{cot['source']}")
        emit(f"COT_Report_Date,{cot['report_date']}")
        emit(f"COT_HedgeFund_Long,{safe_num(cot['long'], 0)}")
        emit(f"COT_HedgeFund_Short,{safe_num(cot['short'], 0)}")
        emit(f"COT_Net_Position,{safe_num(cot['net'], 0)}")
    else:
        for k in ("COT_Source", "COT_Report_Date", "COT_HedgeFund_Long",
                  "COT_HedgeFund_Short", "COT_Net_Position"):
            emit(f"{k},NA")
    emit(f"Risk_Status,{risk['status']}")
    emit(f"Risk_Lot_Size,{safe_num(risk['lot_size'], 2)}")
    emit(f"Alert_Active,{alert['active']}")
    emit("-" * 80)

    emit("-" * 80)
    emit("[2] SMART MONEY CONCEPTS (GOLD 15M)")
    emit("-" * 80)
    emit("Type,Time,Direction,Zone_Low,Zone_High,Status")
    if fvgs:
        for g in fvgs:
            emit(f"FVG,{g['time']},{g['direction']},{g['zone_low']},{g['zone_high']},{g['status']}")
    else:
        emit("FVG,none,-,-,-,-")
    emit(f"Liquidity_Sweep,,,,,\"{sweep['detail']}\"")
    emit("-" * 80)

    emit("-" * 80)
    emit("[3] TODAY'S HIGH-IMPACT NEWS (UTC)")
    emit("-" * 80)
    emit("Time,Country,Event,Forecast,Previous")
    if news is None:
        emit("FEED_UNAVAILABLE,,,,")
    elif not news:
        emit("NO_HIGH_IMPACT_EVENTS_TODAY,,,,")
    else:
        for ev in news:
            emit(f"{ev['time_utc']},{ev['country']},\"{ev['title']}\",{ev['forecast']},{ev['previous']}")
    emit("-" * 80)

    print_csv_section("4] GOLD DAILY - 1MO / 1D (tail)", daily.get("GC=F"), tail=TAIL_DAILY)
    print_csv_section("5] GOLD INTRADAY - 3D / 15M (tail)", intraday.get("GC=F"), tail=TAIL_15M)
    section_no = 6
    for symbol, cfg in ASSETS.items():
        if symbol == "GC=F":
            continue
        print_csv_section(f"{section_no}] {cfg['label'].upper()} - 3D / 15M (tail)",
                          intraday.get(symbol), tail=TAIL_15M)
        section_no += 1

    emit("=" * 80)
    emit("END OF OUTPUT")
    emit("=" * 80)

    OUTPUT_FILE.write_text("\n".join(_output_lines), encoding="utf-8")
    print(f"[INFO] Wrote {OUTPUT_FILE.stat().st_size} bytes to {OUTPUT_FILE}")

    # ---- 5. data.json for the Streamlit frontend ---------------------------
    payload = {
        "generated_utc": generated.strftime("%Y-%m-%d %H:%M:%S"),
        "assets": {
            symbol: {
                "label": m["label"],
                "decimals": m["decimals"],
                "last_close": safe_num(m["last_close"], m["decimals"]),
                "atr14": safe_num(m["atr14"], m["decimals"]),
                "vwap": safe_num(m["vwap"], 2),
                "volume_z": safe_num(m["volume_z"], 3),
                "body_ratio": safe_num(m["body_ratio"], 3),
            }
            for symbol, m in asset_metrics.items()
        },
        "pivots": (
            {k: (safe_num(v, 2) if k != "basis_date" else v) for k, v in pivots.items()}
            if pivots else None
        ),
        "smc": {"fvgs": fvgs, "sweep": sweep},
        "cot": (
            {**cot, "long": safe_num(cot["long"], 0), "short": safe_num(cot["short"], 0),
             "net": safe_num(cot["net"], 0)}
            if cot else None
        ),
        "news": news,          # None = feed down, [] = no events today
        "risk": risk,
        "alert": alert,
    }
    JSON_FILE.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"[INFO] Wrote {JSON_FILE.stat().st_size} bytes to {JSON_FILE}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("-" * 80)
        print("[FATAL] Unhandled exception in pipeline:")
        traceback.print_exc()
        print("-" * 80)
        sys.exit(1)
