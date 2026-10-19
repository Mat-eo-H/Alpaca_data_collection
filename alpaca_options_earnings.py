r"""alpaca_options_earnings.py - historical 1-minute OPTION bars around earnings announcements, from Alpaca, into
C:\StockData\optionsHistory (user request 2026-10-14: "collect data on options from 2024 to present across earnings
announcements ... the last hour of the day before the announcement, then the 1st RTH hour (9:30-10:29), then round
hours until 11, 12, etc but use 3:45 instead of 4 on the day after the announcement").

WHAT ONE EVENT IS. One reporter on one entry session (the session that trades the reaction; the B1 book's Date).
The pre-announcement session is the previous trading session (SPY_daily.csv in the daily store is the calendar).

WHAT IS PULLED PER EVENT (all times US/Eastern; Alpaca is asked in UTC):
  1. The stock's 1-minute bars (GET /v2/stocks/bars, adjustment=raw so prices match the strikes, feed sip), one
     request from the pre-session 15:00 to the entry session 16:00; kept: pre-session 15:00-16:00 and entry session
     09:30-16:00. S_ref = the pre-session 15:59 bar's close, else the last pre-session bar at or before 16:00, else the
     daily store's close for the pre-session (s_ref_source says which).
  2. Expirations: every listed expiry AFTER the pre-session up to --days (60) calendar days after the entry session.
  3. Strikes per expiry: sigma = S_ref x IV30 x sqrt(calendar days from the pre-session to the expiry / 365), IV30 =
     the 30-day IV close in C:\StockData\IV_Daily\<SYM>_IV_Daily.csv on the pre-session date (else the last date before
     it within 10 rows). Every strike within +/- 2 sigma of S_ref, calls and puts. No IV: +/- 10% (sigma_source no_iv).
     Big move: if the entry session's high or low is more than 7% from S_ref, the window reaches 5% past that high / low.
  4. Contracts: GET /v2/options/contracts (trading API), status active AND inactive (past events are expired), the
     expiry window and the widest strike window; every field is saved; in_range = 1 marks the per-expiry window.
  5. Option 1-minute bars (GET /v1beta1/options/bars, 100 contracts a request, paged) for the in-range contracts, one
     window from the pre-session 15:00 to the entry session 15:46; EVERY bar inside the pre-session 15:00-16:00 and the
     entry session 09:30-15:45 is kept, with `moment` marking the requested moments (pre_lasthour, rth_firsthour, hour,
     close_1545) and "" elsewhere.
  6. Files are written, the event's row in _universe\events.csv is updated (status done / no_contracts / no_stock_bars /
     error + reason), the run moves on. A restart skips done events. Order: newest entry session first.

RATE. The X-RateLimit-* headers are honoured per host (data vs trading API), a sliding 60-second window keeps the run
under RPM_CAP requests a minute, 429 and 5xx back off and retry. Nothing is requested between 09:20 and 09:55 ET on a
weekday (the trading bot's decision window). Requests per minute and events per hour are logged every LOG_EVERY events.

ALPACA FACTS (probed 2026-10-13/14, Basic plan): history since February 2024; bars and trades, no historical quotes on any
plan; data older than 15 minutes only (an event's entry session must be over for 16 minutes before it is pulled); raw
option bars include multi-leg prints (about 1.36x IBKR's volume on PEP 2026-10-07..13; the trades endpoint without the
multi-leg condition codes matched IBKR exactly - not pulled here); the contracts list carries ONE open_interest value
with its date (the latest known; an expired contract keeps its last one), no open-interest history anywhere.

KEYS: config_local.py of this repo (API_KEY / API_SECRET / BASE_URL), the same keys alpaca_daily_refresh.py uses.
Never printed, logged or written.

usage:
  python alpaca_options_earnings.py build-universe [--since 2024-02-01]       # events.csv from the B1 book + the journal
  python alpaca_options_earnings.py run [--limit N] [--events PEP_2026-10-08,...] [--symbols A,B] [--days 60]
                                        [--retry-errors] [--oldest-first]
  python alpaca_options_earnings.py verify-ibkr --events PEP_2026-10-08,STZ_2026-10-07,JPM_2026-10-13
  python alpaca_options_earnings.py status
  (long runs: alpaca_options_earnings.ps1 opens the visible window with the Tee log in _logs)
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zoneinfo
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config_local import API_KEY, API_SECRET, BASE_URL  # noqa: E402

OUT_DIR = r"C:\StockData\optionsHistory"
BOOK = r"C:\Users\matth\OneDrive\Documents\_Backtest_CSVs\EarnLab1m_B1_book_20261010_news_dilbyb_anyinfo_ex.csv"
DAILY_DIR = r"C:\StockData\IQbars_Daily"
IV_DIR = r"C:\StockData\IV_Daily"
JOURNAL_DB = r"C:\Users\matth\OneDrive\VSCodeProjects\Earnings_Execution_Bot\data\journal\earnings_bot.db"
SNAPSHOTS_DIR = r"C:\StockData\optionsSnapshots"          # the IBKR collector's store: READ ONLY here
DATA_BASE = "https://data.alpaca.markets"
TRADING_BASE = BASE_URL.rsplit("/v2", 1)[0]
HEADERS = {"APCA-API-KEY-ID": API_KEY, "APCA-API-SECRET-KEY": API_SECRET, "Accept": "application/json"}
NY = zoneinfo.ZoneInfo("America/New_York")
UTC = dt.timezone.utc

RPM_CAP = 190                 # Alpaca's limit is 200 a minute
LOG_EVERY = 25
SAVE_EVERY = 20               # events.csv rewrites (a crash re-pulls at most this many events; the pull is idempotent)
PAUSE_FROM, PAUSE_TO = dt.time(9, 20), dt.time(9, 55, 5)
SETTLE_MINUTES = 16           # the free plan refuses data younger than 15 minutes
PRE_START, PRE_END = dt.time(15, 0), dt.time(16, 0)
ENTRY_START, ENTRY_END_OPT, ENTRY_END_STK = dt.time(9, 30), dt.time(15, 46), dt.time(16, 0)
HOUR_MOMENTS = {dt.time(11, 0), dt.time(12, 0), dt.time(13, 0), dt.time(14, 0), dt.time(15, 0)}
TIMING = {"0": "premarket", "1": "afterclose", "2": "insession"}
EVENT_COLS = ["event_id", "symbol", "entry_session", "pre_session", "timing", "source", "s_ref", "s_ref_source", "iv30",
              "sigma_source", "strike_lo", "strike_hi", "expiry_days", "n_expiries", "n_contracts", "n_option_bars",
              "n_requests", "status", "reason", "pulled_at"]
CONTRACT_FIELDS = ["id", "symbol", "name", "status", "tradable", "expiration_date", "root_symbol", "underlying_symbol",
                   "underlying_asset_id", "type", "style", "strike_price", "multiplier", "size", "open_interest",
                   "open_interest_date", "close_price", "close_price_date", "ppind"]
STOCK_COLS = ["event_id", "symbol", "session", "pre_or_entry", "time_et", "moment", "open", "high", "low", "close",
              "volume", "trade_count", "vwap"]
OPTION_COLS = ["event_id", "symbol", "session", "pre_or_entry", "time_et", "moment", "contract_symbol", "expiry", "dte",
               "strike", "right", "open", "high", "low", "close", "volume", "trade_count", "vwap", "stock_close",
               "moneyness_pct"]


def log(msg: str) -> None:
    print(f"[{dt.datetime.now(NY).strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# ---------------------------------------------------------------------------------------------------------------------
# requests: rate limit, the decision-window pause, retries
# ---------------------------------------------------------------------------------------------------------------------

class ApiError(Exception):
    """A non-retryable HTTP answer (4xx other than 429)."""


_recent = deque()                      # request times (monotonic) in the last 60 s, both hosts together
_limits: dict = {}                     # host -> (remaining, reset epoch) from the last answer
_counters = {"requests": 0, "retries": 0, "paused_s": 0.0}


def pause_for_decision_window() -> None:
    """Sleep through 09:20-09:55 ET on a weekday: the trading bot decides then and nothing of ours may compete."""
    now = dt.datetime.now(NY)
    if now.weekday() < 5 and PAUSE_FROM <= now.time() < PAUSE_TO:
        until = now.replace(hour=PAUSE_TO.hour, minute=PAUSE_TO.minute, second=PAUSE_TO.second, microsecond=0)
        secs = (until - now).total_seconds()
        log(f"paused for the bot's decision window until {until.strftime('%H:%M:%S')} ET ({secs / 60:.1f} min)")
        time.sleep(secs)
        _counters["paused_s"] += secs


def throttle(host: str) -> None:
    """Keep under RPM_CAP requests in any 60-second window and wait for the server's reset when it says 0-3 left."""
    while True:
        now = time.monotonic()
        while _recent and now - _recent[0] > 60.0:
            _recent.popleft()
        if len(_recent) < RPM_CAP:
            break
        time.sleep(0.25 + (60.0 - (now - _recent[0])))
    rem, reset = _limits.get(host, (None, None))
    if rem is not None and rem <= 3 and reset:
        wait = max(0.0, reset - time.time()) + 1.0
        if wait > 0:
            log(f"rate limit: {rem} left on {host}, waiting {wait:.0f}s for the reset")
            time.sleep(wait)


def get_json(base: str, path: str, params: dict, retries: int = 6) -> dict:
    """One GET -> parsed JSON. Honours the pause window, the sliding window, X-RateLimit-*, 429 and 5xx retries.
    Raises ApiError on other 4xx and after the retries."""
    host = base.split("//", 1)[1]
    url = base + path + "?" + urllib.parse.urlencode(params)
    for attempt in range(retries):
        pause_for_decision_window()
        throttle(host)
        _recent.append(time.monotonic())
        _counters["requests"] += 1
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=60) as r:
                hdr, raw, status = r.headers, r.read(), r.status
        except urllib.error.HTTPError as e:
            hdr, raw, status = e.headers, e.read(), e.code
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            _counters["retries"] += 1
            log(f"  network: {type(e).__name__} {str(e)[:80]}; retry {attempt + 1}/{retries}")
            time.sleep(5 * (attempt + 1))
            continue
        try:
            rem, reset = hdr.get("X-RateLimit-Remaining"), hdr.get("X-RateLimit-Reset")
            if rem is not None:
                _limits[host] = (int(rem), int(reset) if reset else None)
        except ValueError:
            pass
        if status == 200:
            return json.loads(raw.decode("utf-8"))
        body = raw[:200].decode("utf-8", "replace")
        if status == 429:
            _counters["retries"] += 1
            reset = _limits.get(host, (None, None))[1]
            wait = max(2.0, (reset - time.time()) + 1.0) if reset else 15.0
            log(f"  429 on {host}: waiting {wait:.0f}s")
            time.sleep(min(wait, 90.0))
            continue
        if status >= 500:
            _counters["retries"] += 1
            log(f"  HTTP {status} ({body}); retry {attempt + 1}/{retries}")
            time.sleep(5 * (attempt + 1))
            continue
        raise ApiError(f"HTTP {status} {path}: {body}")
    raise ApiError(f"gave up after {retries} tries: {path}")


def get_pages(base: str, path: str, params: dict, key: str, merge_dict: bool) -> dict | list:
    """Every page of a paged endpoint. merge_dict: the page's `key` is {symbol: [rows]} (bars); else a list."""
    out: dict | list = {} if merge_dict else []
    token = None
    while True:
        q = dict(params)
        if token:
            q["page_token"] = token
        r = get_json(base, path, q)
        chunk = r.get(key) or ({} if merge_dict else [])
        if merge_dict:
            for s, rows in chunk.items():
                out.setdefault(s, []).extend(rows)
        else:
            out.extend(chunk)
        token = r.get("next_page_token")
        if not token:
            return out


# ---------------------------------------------------------------------------------------------------------------------
# time helpers
# ---------------------------------------------------------------------------------------------------------------------

def et_to_utc_param(day: dt.date, t: dt.time) -> str:
    """ET wall time on `day` -> Alpaca's RFC-3339 UTC parameter."""
    return dt.datetime.combine(day, t, tzinfo=NY).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def bar_et(t: str) -> dt.datetime:
    """Alpaca bar time '2024-02-01T20:34:00Z' -> aware ET datetime."""
    return dt.datetime.fromisoformat(t.replace("Z", "+00:00")).astimezone(NY)


def moment_of(pre_or_entry: str, t: dt.time) -> str:
    """The user's requested moments; '' for every other kept bar."""
    if pre_or_entry == "pre":
        return "pre_lasthour" if PRE_START <= t < PRE_END else ""
    if ENTRY_START <= t < dt.time(10, 30):
        return "rth_firsthour"
    if t in HOUR_MOMENTS:
        return "hour"
    if t == dt.time(15, 45):
        return "close_1545"
    return ""


def session_complete(entry_session: dt.date) -> bool:
    """The free plan serves data older than 15 minutes: the entry session must be over for SETTLE_MINUTES."""
    close = dt.datetime.combine(entry_session, dt.time(16, 0), tzinfo=NY)
    return dt.datetime.now(NY) >= close + dt.timedelta(minutes=SETTLE_MINUTES)


# ---------------------------------------------------------------------------------------------------------------------
# local stores: the session calendar, the daily close, IV30
# ---------------------------------------------------------------------------------------------------------------------

def load_calendar() -> list:
    """Every session the market was open, from SPY's daily bars, extended with plain weekdays for 40 days past its
    last date (so an entry session the store has not reached yet still has a place; holidays in that tail are not
    known)."""
    days = []
    with open(os.path.join(DAILY_DIR, "SPY_daily.csv"), encoding="ascii", errors="ignore", newline="") as fh:
        for row in csv.reader(fh):
            if len(row) >= 2 and len(row[1]) == 10 and row[1][2] == "/":
                days.append(dt.date(int(row[1][6:10]), int(row[1][0:2]), int(row[1][3:5])))
    days.sort()
    d = days[-1]
    for _ in range(40):
        d += dt.timedelta(days=1)
        if d.weekday() < 5:
            days.append(d)
    return days


def previous_session(cal: list, day: dt.date) -> dt.date | None:
    """The last calendar session strictly before `day`."""
    import bisect
    i = bisect.bisect_left(cal, day)
    return cal[i - 1] if i > 0 else None


def entry_session_of(cal: list, t_et: dt.datetime) -> tuple:
    """(entry session, timing) by the lab's rule: before 09:30 on a session = that day (premarket); 09:30-16:00 = that
    day (insession); otherwise the next session (afterclose)."""
    import bisect
    day = t_et.date()
    i = bisect.bisect_left(cal, day)
    on_session = i < len(cal) and cal[i] == day
    minutes = t_et.hour * 60 + t_et.minute
    if on_session and minutes < 570:
        return day, "premarket"
    if on_session and minutes < 960:
        return day, "insession"
    j = i + 1 if on_session else i
    return (cal[j], "afterclose") if j < len(cal) else (None, None)


def daily_close(symbol: str, day: dt.date) -> float | None:
    """The daily store's close for `symbol` on `day` (None without a file or a row)."""
    path = os.path.join(DAILY_DIR, f"{symbol}_daily.csv")
    if not os.path.exists(path):
        return None
    key = day.strftime("%m/%d/%Y")
    with open(path, encoding="ascii", errors="ignore", newline="") as fh:
        for row in csv.reader(fh):
            if len(row) >= 7 and row[1] == key:
                try:
                    return float(row[6])
                except ValueError:
                    return None
    return None


def iv30_on(symbol: str, day: dt.date, lookback_rows: int = 10) -> tuple:
    """(IV30 close, source) from the IV store: the row on `day`, else the last row before it within lookback_rows
    rows; (None, 'no_iv') without a file or a usable row."""
    path = os.path.join(IV_DIR, f"{symbol}_IV_Daily.csv")
    if not os.path.exists(path):
        return None, "no_iv"
    key = day.isoformat()
    before = []
    with open(path, encoding="ascii", errors="ignore", newline="") as fh:
        rd = csv.reader(fh)
        next(rd, None)
        for row in rd:
            if len(row) < 5 or len(row[0]) != 10:
                continue
            if row[0] == key:
                try:
                    v = float(row[4])
                    return (v, "iv30") if v > 0 else (None, "no_iv")
                except ValueError:
                    return None, "no_iv"
            if row[0] < key:
                before.append(row)
            else:
                break
    for row in reversed(before[-lookback_rows:]):
        try:
            v = float(row[4])
        except ValueError:
            continue
        if v > 0:
            return v, f"iv30_{row[0]}"
    return None, "no_iv"


# ---------------------------------------------------------------------------------------------------------------------
# the universe
# ---------------------------------------------------------------------------------------------------------------------

def events_path() -> str:
    return os.path.join(OUT_DIR, "_universe", "events.csv")


def read_events() -> list:
    """events.csv as a list of dicts (empty when it does not exist yet)."""
    p = events_path()
    if not os.path.exists(p):
        return []
    with open(p, encoding="utf-8", newline="") as fh:
        return [dict(r) for r in csv.DictReader(fh)]


def write_events(rows: list) -> None:
    """Rewrite events.csv atomically (temp file + replace) so a crash never leaves a half file."""
    p = events_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=EVENT_COLS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in EVENT_COLS})
    os.replace(tmp, p)


def book_events(since: dt.date) -> dict:
    """{(symbol, entry session): timing} from the unfiltered 1-minute B1 book (one row per reporter per event)."""
    import pandas as pd
    df = pd.read_csv(BOOK, usecols=["Symbol", "Date", "ERN_TimingCode"], dtype=str)
    out = {}
    for sym, date, code in zip(df["Symbol"], df["Date"], df["ERN_TimingCode"]):
        d = dt.date(int(date[6:10]), int(date[0:2]), int(date[3:5]))
        if d < since:
            continue
        out.setdefault((str(sym).strip().upper(), d), TIMING.get(str(code).strip(), ""))
    return out


def journal_events(cal: list, after: dt.date) -> dict:
    """{(symbol, entry session): timing} from the trading bot's journal (report headlines, read-only), for entry
    sessions after `after`. {} when the journal is missing."""
    if not os.path.exists(JOURNAL_DB):
        return {}
    uri = "file:" + os.path.abspath(JOURNAL_DB).replace("\\", "/") + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        rows = conn.execute("SELECT symbol, COALESCE(published_utc, received_utc) FROM headlines WHERE is_report = 1 "
                            "ORDER BY received_utc ASC").fetchall()
    finally:
        conn.close()
    out = {}
    for sym, t in rows:
        s = str(sym or "").upper()
        if not re.fullmatch(r"[A-Z][A-Z.]{0,5}", s):
            continue
        try:
            t_et = dt.datetime.fromisoformat(str(t)).astimezone(NY)
        except ValueError:
            continue
        entry, timing = entry_session_of(cal, t_et)
        if entry and entry > after:
            out.setdefault((s, entry), timing)
    return out


def snapshot_universe_events(after: dt.date) -> dict:
    """{(symbol, entry session): timing} from the IBKR collector's universe files (read-only), entry sessions after
    `after`."""
    d = os.path.join(SNAPSHOTS_DIR, "_universe")
    out = {}
    if not os.path.isdir(d):
        return out
    for name in sorted(os.listdir(d)):
        if not (name.startswith("universe_") and name.endswith(".csv")):
            continue
        with open(os.path.join(d, name), encoding="utf-8", newline="") as fh:
            for r in csv.DictReader(fh):
                try:
                    entry = dt.date.fromisoformat(r.get("entry_session", ""))
                except ValueError:
                    continue
                if entry > after:
                    out.setdefault((r["symbol"].upper(), entry), r.get("timing", ""))
    return out


def build_universe(since: dt.date) -> None:
    """Build or refresh _universe/events.csv: the book's events from `since`, plus the journal's and the IBKR
    collector's events after the book's last session. Existing rows keep their status and results; new rows are
    pending. Order: newest entry session first."""
    cal = load_calendar()
    book = book_events(since)
    book_end = max(d for _, d in book) if book else since
    jour = journal_events(cal, book_end)
    snap = snapshot_universe_events(book_end)
    merged = {k: ("b1_book", v) for k, v in book.items()}
    for k, v in jour.items():
        merged.setdefault(k, ("journal", v))
    for k, v in snap.items():
        merged.setdefault(k, ("snapshots_universe", v))
    existing = {r["event_id"]: r for r in read_events()}
    rows = []
    for (sym, entry), (src, timing) in merged.items():
        eid = f"{sym}_{entry.isoformat()}"
        if eid in existing:
            rows.append(existing[eid])
            continue
        pre = previous_session(cal, entry)
        rows.append({"event_id": eid, "symbol": sym, "entry_session": entry.isoformat(),
                     "pre_session": pre.isoformat() if pre else "", "timing": timing, "source": src,
                     "status": "pending" if pre else "error", "reason": "" if pre else "no previous session"})
    rows.sort(key=lambda r: (r["entry_session"], r["symbol"]), reverse=True)
    write_events(rows)
    by_src = {}
    for r in rows:
        by_src[r["source"]] = by_src.get(r["source"], 0) + 1
    log(f"universe: {len(rows):,} events ({by_src}); book {len(book):,} from {since} to {book_end}; "
        f"journal after the book {len(jour):,}; IBKR universe files after the book {len(snap):,}; "
        f"gap: no source covers {book_end + dt.timedelta(days=1)} to the journal's first day")
    print_status(rows)


def print_status(rows: list | None = None) -> None:
    """Counts by status and by year, the pace from the pulled_at stamps of this calendar day."""
    rows = read_events() if rows is None else rows
    by_status, by_year = {}, {}
    for r in rows:
        by_status[r.get("status", "")] = by_status.get(r.get("status", ""), 0) + 1
        y = r["entry_session"][:4]
        by_year.setdefault(y, {"events": 0, "done": 0, "no_contracts": 0, "no_stock_bars": 0, "error": 0, "pending": 0})
        by_year[y]["events"] += 1
        st = r.get("status", "")
        if st in by_year[y]:
            by_year[y][st] += 1
    print(f"status: {by_status}")
    for y in sorted(by_year):
        print(f"  {y}: {by_year[y]}")


# ---------------------------------------------------------------------------------------------------------------------
# one event
# ---------------------------------------------------------------------------------------------------------------------

def strike_window(s_ref: float, iv30: float | None, pre: dt.date, expiry: dt.date,
                  entry_high: float | None, entry_low: float | None) -> tuple:
    """(lo, hi, sigma) for one expiry: +/- 2 sigma (sigma = S x IV30 x sqrt(days / 365)), +/- 10% without IV, widened
    to 5% past the entry session's high / low when that is more than 7% from S_ref."""
    days = max(1, (expiry - pre).days)
    if iv30:
        sigma = s_ref * iv30 * math.sqrt(days / 365.0)
        lo, hi = s_ref - 2 * sigma, s_ref + 2 * sigma
    else:
        sigma = 0.0
        lo, hi = s_ref * 0.90, s_ref * 1.10
    if entry_high and entry_high > s_ref * 1.07:
        hi = max(hi, entry_high * 1.05)
    if entry_low and entry_low < s_ref * 0.93:
        lo = min(lo, entry_low * 0.95)
    return max(0.0, lo), hi, sigma


def fetch_stock_bars(symbol: str, pre: dt.date, entry: dt.date) -> dict:
    """{'pre': [bars], 'entry': [bars]} of the stock's raw SIP 1-minute bars inside the two windows (one request)."""
    bars = get_pages(DATA_BASE, "/v2/stocks/bars",
                     {"symbols": symbol, "timeframe": "1Min", "adjustment": "raw", "feed": "sip", "limit": 10000,
                      "sort": "asc", "start": et_to_utc_param(pre, PRE_START), "end": et_to_utc_param(entry, ENTRY_END_STK)},
                     "bars", True).get(symbol, [])
    out = {"pre": [], "entry": []}
    for b in bars:
        t = bar_et(b["t"])
        if t.date() == pre and PRE_START <= t.time() <= PRE_END:
            out["pre"].append((t, b))
        elif t.date() == entry and ENTRY_START <= t.time() <= ENTRY_END_STK:
            out["entry"].append((t, b))
    return out


def reference_price(symbol: str, pre: dt.date, pre_bars: list) -> tuple:
    """(S_ref, source): the 15:59 close, else the last pre-session bar at or before 16:00, else the daily close."""
    at_1559 = [b for t, b in pre_bars if t.time() == dt.time(15, 59)]
    if at_1559:
        return float(at_1559[-1]["c"]), "pre_1559_close"
    before = [(t, b) for t, b in pre_bars if t.time() <= dt.time(16, 0)]
    if before:
        t, b = before[-1]
        return float(b["c"]), f"pre_last_bar_{t.strftime('%H:%M')}"
    c = daily_close(symbol, pre)
    return (c, "daily_store_close") if c else (None, "")


def fetch_contracts(symbol: str, exp_from: dt.date, exp_to: dt.date, lo: float, hi: float) -> list:
    """Every contract (active and inactive) of `symbol` expiring in [exp_from, exp_to] with a strike in [lo, hi].
    When the whole expiry window is in the past every contract is expired (inactive): the active query is skipped."""
    out = []
    statuses = ("inactive",) if exp_to < dt.datetime.now(NY).date() else ("inactive", "active")
    for status in statuses:
        out.extend(get_pages(TRADING_BASE, "/v2/options/contracts",
                             {"underlying_symbols": symbol, "status": status, "expiration_date_gte": exp_from.isoformat(),
                              "expiration_date_lte": exp_to.isoformat(), "strike_price_gte": f"{lo:.2f}",
                              "strike_price_lte": f"{hi:.2f}", "limit": 10000}, "option_contracts", False))
    seen, uniq = set(), []
    for c in out:
        if c["symbol"] not in seen:
            seen.add(c["symbol"])
            uniq.append(c)
    return uniq


def fetch_option_bars(contract_symbols: list, pre: dt.date, entry: dt.date) -> dict:
    """{contract symbol: [bars]} for the window pre-session 15:00 .. entry session 15:46, 100 contracts a request."""
    out = {}
    for i in range(0, len(contract_symbols), 100):
        got = get_pages(DATA_BASE, "/v1beta1/options/bars",
                        {"symbols": ",".join(contract_symbols[i:i + 100]), "timeframe": "1Min", "limit": 10000,
                         "sort": "asc", "start": et_to_utc_param(pre, PRE_START), "end": et_to_utc_param(entry, ENTRY_END_OPT)},
                        "bars", True)
        for s, rows in got.items():
            out.setdefault(s, []).extend(rows)
    return out


def event_dir(ev: dict) -> str:
    return os.path.join(OUT_DIR, ev["entry_session"], ev["symbol"])


def write_csv(path: str, cols: list, rows: list, append: bool = False) -> None:
    mode = "a" if append and os.path.exists(path) else "w"
    with open(path, mode, encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        if mode == "w":
            w.writeheader()
        w.writerows(rows)


def stock_rows(ev: dict, bars: dict) -> tuple:
    """(rows for the stock CSV, {ET 'YYYY-MM-DD HH:MM': close}, entry high, entry low)."""
    rows, closes, hi, lo = [], {}, None, None
    for kind in ("pre", "entry"):
        for t, b in bars[kind]:
            key = t.strftime("%Y-%m-%d %H:%M")
            closes[key] = float(b["c"])
            rows.append({"event_id": ev["event_id"], "symbol": ev["symbol"], "session": t.strftime("%Y-%m-%d"),
                         "pre_or_entry": kind, "time_et": key, "moment": moment_of(kind, t.time()), "open": b["o"],
                         "high": b["h"], "low": b["l"], "close": b["c"], "volume": b["v"], "trade_count": b.get("n", ""),
                         "vwap": b.get("vw", "")})
            if kind == "entry":
                hi = max(hi, float(b["h"])) if hi is not None else float(b["h"])
                lo = min(lo, float(b["l"])) if lo is not None else float(b["l"])
    return rows, closes, hi, lo


def option_rows(ev: dict, bars: dict, meta: dict, closes: dict, pre: dt.date, entry: dt.date) -> list:
    """Rows for the option-bars CSV: every bar inside the two windows, with moment, dte, stock close, moneyness."""
    rows = []
    for sym, lst in bars.items():
        m = meta[sym]
        for b in lst:
            t = bar_et(b["t"])
            if t.date() == pre and PRE_START <= t.time() <= PRE_END:
                kind = "pre"
            elif t.date() == entry and ENTRY_START <= t.time() < ENTRY_END_OPT:
                kind = "entry"
            else:
                continue
            key = t.strftime("%Y-%m-%d %H:%M")
            sc = closes.get(key)
            rows.append({"event_id": ev["event_id"], "symbol": ev["symbol"], "session": t.strftime("%Y-%m-%d"),
                         "pre_or_entry": kind, "time_et": key, "moment": moment_of(kind, t.time()), "contract_symbol": sym,
                         "expiry": m["expiry"].isoformat(), "dte": (m["expiry"] - t.date()).days, "strike": m["strike"],
                         "right": m["right"], "open": b["o"], "high": b["h"], "low": b["l"], "close": b["c"],
                         "volume": b["v"], "trade_count": b.get("n", ""), "vwap": b.get("vw", ""),
                         "stock_close": sc if sc is not None else "",
                         "moneyness_pct": round((m["strike"] - sc) / sc * 100.0, 3) if sc else ""})
    rows.sort(key=lambda r: (r["time_et"], r["expiry"], r["strike"], r["right"]))
    return rows


def pull_event(ev: dict, days: int) -> dict:
    """Pull one event end to end and write its three files. Returns the fields to merge into the event's row."""
    sym, pre, entry = ev["symbol"], dt.date.fromisoformat(ev["pre_session"]), dt.date.fromisoformat(ev["entry_session"])
    req0 = _counters["requests"]
    bars = fetch_stock_bars(sym, pre, entry)
    if not bars["pre"] and not bars["entry"]:
        return {"status": "no_stock_bars", "reason": "no stock bars in either window", "n_requests": _counters["requests"] - req0}
    s_ref, s_src = reference_price(sym, pre, bars["pre"])
    if not s_ref:
        return {"status": "no_stock_bars", "reason": "no pre-session bar and no daily close", "n_requests": _counters["requests"] - req0}
    iv30, iv_src = iv30_on(sym, pre)
    srows, closes, entry_high, entry_low = stock_rows(ev, bars)
    exp_from, exp_to = pre + dt.timedelta(days=1), entry + dt.timedelta(days=days)
    lo_all, hi_all, _ = strike_window(s_ref, iv30, pre, exp_to, entry_high, entry_low)
    if iv30 is None:
        lo_all, hi_all = min(lo_all, s_ref * 0.9), max(hi_all, s_ref * 1.1)
    contracts = fetch_contracts(sym, exp_from, exp_to, lo_all, hi_all)
    d = event_dir(ev)
    os.makedirs(d, exist_ok=True)
    write_csv(os.path.join(d, f"{sym}_stock_bars.csv"), STOCK_COLS, srows)
    base = {"s_ref": round(s_ref, 4), "s_ref_source": s_src, "iv30": round(iv30, 6) if iv30 else "", "sigma_source": iv_src,
            "strike_lo": round(lo_all, 3), "strike_hi": round(hi_all, 3), "expiry_days": days}
    if not contracts:
        return {**base, "status": "no_contracts", "reason": "no listed contract in the expiry and strike window",
                "n_expiries": 0, "n_contracts": 0, "n_option_bars": 0, "n_requests": _counters["requests"] - req0}
    crow, meta, windows = [], {}, {}
    for c in contracts:
        expiry = dt.date.fromisoformat(c["expiration_date"])
        if expiry not in windows:
            windows[expiry] = strike_window(s_ref, iv30, pre, expiry, entry_high, entry_low)
        lo, hi, sigma = windows[expiry]
        strike = float(c["strike_price"])
        in_range = int(lo <= strike <= hi)
        crow.append({**{k: c.get(k, "") for k in CONTRACT_FIELDS}, "contract_symbol": c["symbol"], "event_id": ev["event_id"],
                     "underlying": sym, "entry_session": ev["entry_session"], "pre_session": ev["pre_session"],
                     "dte_entry": (expiry - entry).days, "sigma": round(sigma, 4), "strike_lo": round(lo, 3),
                     "strike_hi": round(hi, 3), "in_range": in_range})
        if in_range:
            meta[c["symbol"]] = {"expiry": expiry, "strike": strike, "right": "C" if c["type"] == "call" else "P"}
    ccols = ["event_id", "underlying", "entry_session", "pre_session", "contract_symbol", "in_range", "dte_entry", "sigma",
             "strike_lo", "strike_hi"] + [k for k in CONTRACT_FIELDS if k != "symbol"]
    crow.sort(key=lambda r: (r["expiration_date"], float(r["strike_price"]), r["type"]))
    write_csv(os.path.join(d, f"{sym}_contracts.csv"), ccols, crow)
    obars = fetch_option_bars(sorted(meta), pre, entry) if meta else {}
    orows = option_rows(ev, obars, meta, closes, pre, entry)
    write_csv(os.path.join(d, f"{sym}_option_bars.csv"), OPTION_COLS, orows)
    return {**base, "status": "done", "reason": "" if orows else "no_option_bars", "n_expiries": len(windows),
            "n_contracts": len(meta), "n_option_bars": len(orows), "n_requests": _counters["requests"] - req0}


# ---------------------------------------------------------------------------------------------------------------------
# the run
# ---------------------------------------------------------------------------------------------------------------------

def run(limit: int | None, only_events: set | None, only_symbols: set | None, days: int, retry_errors: bool,
        oldest_first: bool) -> None:
    rows = read_events()
    if not rows:
        raise SystemExit("no events.csv yet: run build-universe first")
    pick = {"pending"} | ({"error"} if retry_errors else set())
    todo = [r for r in rows if r.get("status") in pick
            and (not only_events or r["event_id"] in only_events)
            and (not only_symbols or r["symbol"] in only_symbols)]
    todo.sort(key=lambda r: (r["entry_session"], r["symbol"]), reverse=not oldest_first)
    if limit:
        todo = todo[:limit]
    by_id = {r["event_id"]: r for r in rows}
    log(f"run: {len(todo):,} events to pull ({'oldest' if oldest_first else 'newest'} first), expiries to {days} days; "
        f"{sum(1 for r in rows if r.get('status') == 'done'):,} already done")
    t0, done, skipped_open, bars_total = time.time(), 0, 0, 0
    try:
        for i, ev in enumerate(todo, 1):
            if not session_complete(dt.date.fromisoformat(ev["entry_session"])):
                skipped_open += 1
                continue
            t1 = time.time()
            try:
                res = pull_event(ev, days)
            except ApiError as e:
                res = {"status": "error", "reason": str(e)[:160]}
            except Exception as e:                     # one bad event must not end a 20-hour run
                res = {"status": "error", "reason": f"{type(e).__name__}: {str(e)[:140]}"}
            res["pulled_at"] = dt.datetime.now(NY).strftime("%Y-%m-%d %H:%M:%S")
            by_id[ev["event_id"]].update(res)
            done += 1
            if done % SAVE_EVERY == 0:                 # events.csv is rewritten every SAVE_EVERY events and at the end
                write_events(rows)
            bars_total += int(res.get("n_option_bars") or 0)
            log(f"{i:,}/{len(todo):,} {ev['event_id']} {res['status']}"
                f"{' (' + res['reason'] + ')' if res.get('reason') else ''}: S_ref {res.get('s_ref', '')} "
                f"iv30 {res.get('iv30', '')} exp {res.get('n_expiries', '')} contracts {res.get('n_contracts', '')} "
                f"bars {res.get('n_option_bars', '')} req {res.get('n_requests', '')} ({time.time() - t1:.1f}s)")
            if done % LOG_EVERY == 0:
                log_pace(t0, done, len(todo) - i, bars_total)
    except KeyboardInterrupt:
        log("stopped by Ctrl+C")
    write_events(rows)
    log_pace(t0, done, len(todo) - done - skipped_open, bars_total)
    if skipped_open:
        log(f"{skipped_open} events left pending: their entry session is not over for {SETTLE_MINUTES} minutes yet")
    print("OPTIONS-HISTORY-DONE", flush=True)


def log_pace(t0: float, done: int, remaining: int, bars_total: int) -> None:
    """Requests per minute (this run and the last 60 s), events per hour, the projected finish."""
    el = max(1.0, time.time() - t0 - _counters["paused_s"])
    per_hour = done / el * 3600.0
    finish = (dt.datetime.now(NY) + dt.timedelta(hours=remaining / per_hour)).strftime("%Y-%m-%d %H:%M") if per_hour else "?"
    log(f"PACE: {done:,} events in {el / 60:.1f} min = {per_hour:,.0f} events/h; requests {_counters['requests']:,} "
        f"({_counters['requests'] / el * 60:.0f}/min over the run, {len(_recent)} in the last 60 s; retries "
        f"{_counters['retries']}); option bars {bars_total:,}; remaining {remaining:,} -> projected finish {finish}")


# ---------------------------------------------------------------------------------------------------------------------
# verification against the IBKR collector's store (read-only)
# ---------------------------------------------------------------------------------------------------------------------

MULTI_LEG = set("fghijlmt")   # multi-leg trade condition codes (/v1beta1/options/meta/conditions/trade, probed 2026-10-13)
STOCK_OPT = set("knopqrs")    # stock-option (tied to a stock leg) condition codes


def trades_minutes(contract_symbols: list, pre: dt.date, entry: dt.date, drop: set) -> dict:
    """{(contract, ET minute): (last price, volume)} rebuilt from Alpaca's TRADES over the two windows, leaving out
    trades whose condition code is in `drop` (the way IBKR's TRADES bars leave out multi-leg prints)."""
    out = {}
    for i in range(0, len(contract_symbols), 100):
        got = get_pages(DATA_BASE, "/v1beta1/options/trades",
                        {"symbols": ",".join(contract_symbols[i:i + 100]), "limit": 10000, "sort": "asc",
                         "start": et_to_utc_param(pre, PRE_START), "end": et_to_utc_param(entry, ENTRY_END_OPT)},
                        "trades", True)
        for s, tl in got.items():
            for t in tl:
                if t.get("c") in drop:
                    continue
                m = bar_et(t["t"]).strftime("%Y-%m-%d %H:%M")
                _, v = out.get((s, m), (None, 0))
                out[(s, m)] = (float(t["p"]), v + float(t["s"]))
    return out


def verify_ibkr(event_ids: list, trades_check: bool = False) -> None:
    """For each event, compare Alpaca's option bars with the IBKR snapshots (kinds pre1545 / entry: close vs `last`,
    volume vs `minute_volume`, where IBKR's minute_volume > 0) and with IBKR's raw TRADES bars (close and volume where
    both have the minute). --trades-check also rebuilds the minutes from Alpaca's trades without multi-leg and
    stock-option prints and compares those with IBKR's TRADES bars (a few requests per event)."""
    print(f"{'event':<18}{'cells both':>11}{'close =':>9}{'vol =':>8}{'close%':>8}{'vol%':>7}   "
          f"{'TRADES both':>12}{'close =':>9}{'vol =':>8}{'close%':>8}{'vol%':>7}")
    by_id = {r["event_id"]: r for r in read_events()}
    for eid in event_ids:
        sym, entry = eid.rsplit("_", 1)
        pre = by_id.get(eid, {}).get("pre_session", "")
        a_path = os.path.join(OUT_DIR, entry, sym, f"{sym}_option_bars.csv")
        s_path = os.path.join(SNAPSHOTS_DIR, entry, sym, f"{sym}_snapshots.csv")
        b_path = os.path.join(SNAPSHOTS_DIR, entry, sym, f"{sym}_bars.csv")
        if not os.path.exists(a_path) or not os.path.exists(s_path):
            print(f"{eid:<18} missing: {'' if os.path.exists(a_path) else a_path} {'' if os.path.exists(s_path) else s_path}")
            continue
        al = {}
        with open(a_path, encoding="utf-8", newline="") as fh:
            for r in csv.DictReader(fh):
                al[(r["contract_symbol"], r["time_et"])] = (float(r["close"]), float(r["volume"]))
        snap = {}
        with open(s_path, encoding="utf-8", newline="") as fh:
            for r in csv.DictReader(fh):
                if r["kind"] not in ("pre1545", "entry") or not r["minute_volume"] or float(r["minute_volume"]) <= 0 or not r["last"]:
                    continue
                snap[(r["local_symbol"].replace(" ", ""), f"{r['snap_date']} {r['moment_et']}")] = (float(r["last"]), float(r["minute_volume"]))
        both = set(al) & set(snap)
        ce = sum(1 for k in both if abs(al[k][0] - snap[k][0]) < 1e-6)
        ve = sum(1 for k in both if abs(al[k][1] - snap[k][1]) < 1e-6)
        tr = {}                                     # IBKR TRADES bars inside the two pull windows only
        if os.path.exists(b_path):
            with open(b_path, encoding="utf-8", newline="") as fh:
                for r in csv.DictReader(fh):
                    if r["what"] != "TRADES" or not r["volume"] or float(r["volume"]) <= 0:
                        continue
                    day, hhmm = r["time_et"][:10], r["time_et"][11:16]
                    if (day == pre and "15:00" <= hhmm <= "16:00") or (day == entry and "09:30" <= hhmm < "15:46"):
                        tr[(r["local_symbol"].replace(" ", ""), r["time_et"])] = (float(r["close"]), float(r["volume"]))
        both2 = set(al) & set(tr)
        ce2 = sum(1 for k in both2 if abs(al[k][0] - tr[k][0]) < 1e-6)
        ve2 = sum(1 for k in both2 if abs(al[k][1] - tr[k][1]) < 1e-6)
        pct = lambda a, b: f"{100.0 * a / b:.1f}" if b else "-"  # noqa: E731
        print(f"{eid:<18}{len(both):>11,}{ce:>9,}{ve:>8,}{pct(ce, len(both)):>8}{pct(ve, len(both)):>7}   "
              f"{len(both2):>12,}{ce2:>9,}{ve2:>8,}{pct(ce2, len(both2)):>8}{pct(ve2, len(both2)):>7}")
        only_ib = len(set(snap) - set(al))
        only_al = sum(1 for k in al if k[0] in {s for s, _ in snap} and k not in snap and al[k][1] > 0)
        print(f"{'':<18}IBKR snapshot cells with a trade: {len(snap):,}; Alpaca bars: {len(al):,}; snapshot cells Alpaca lacks: "
              f"{only_ib:,}; Alpaca minutes (same contracts) IBKR lacks: {only_al:,}; IBKR TRADES minutes Alpaca lacks: "
              f"{len(set(tr) - set(al)):,}")
        if trades_check and tr and pre:
            syms = sorted({s for s, _ in tr})
            rebuilt = trades_minutes(syms, dt.date.fromisoformat(pre), dt.date.fromisoformat(entry), MULTI_LEG | STOCK_OPT)
            both3 = set(rebuilt) & set(tr)
            ce3 = sum(1 for k in both3 if abs(rebuilt[k][0] - tr[k][0]) < 1e-6)
            ve3 = sum(1 for k in both3 if abs(rebuilt[k][1] - tr[k][1]) < 1e-6)
            print(f"{'':<18}TRADES minus multi-leg / stock-option prints ({len(syms)} contracts): minutes both {len(both3):,}; "
                  f"close = {ce3:,} ({pct(ce3, len(both3))}%); volume = {ve3:,} ({pct(ve3, len(both3))}%); "
                  f"only IBKR {len(set(tr) - set(rebuilt)):,}; only Alpaca {len(set(rebuilt) - set(tr)):,}")


# ---------------------------------------------------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build-universe", help="events.csv from the B1 book, the journal and the IBKR universe files")
    b.add_argument("--since", default="2024-02-01", help="first entry session (Alpaca's option history starts Feb 2024)")
    r = sub.add_parser("run", help="pull every pending event, newest first")
    r.add_argument("--limit", type=int, help="stop after N events")
    r.add_argument("--events", help="comma list of event ids SYM_YYYY-MM-DD")
    r.add_argument("--symbols", help="comma list of symbols")
    r.add_argument("--days", type=int, default=60, help="expiries up to N calendar days after the entry session")
    r.add_argument("--retry-errors", action="store_true", help="also re-pull events in status error")
    r.add_argument("--oldest-first", action="store_true")
    v = sub.add_parser("verify-ibkr", help="compare pulled events with the IBKR collector's snapshots")
    v.add_argument("--events", required=True)
    v.add_argument("--trades-check", action="store_true", help="also rebuild minutes from trades without multi-leg prints")
    sub.add_parser("status", help="counts by status and year")
    a = ap.parse_args()
    if a.cmd == "build-universe":
        build_universe(dt.date.fromisoformat(a.since))
    elif a.cmd == "run":
        run(a.limit, set(a.events.split(",")) if a.events else None,
            {s.strip().upper() for s in a.symbols.split(",")} if a.symbols else None, a.days, a.retry_errors, a.oldest_first)
    elif a.cmd == "verify-ibkr":
        verify_ibkr(a.events.split(","), a.trades_check)
    else:
        print_status()
    return 0


if __name__ == "__main__":
    sys.exit(main())
