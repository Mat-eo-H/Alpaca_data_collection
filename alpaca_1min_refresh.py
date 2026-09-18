r"""alpaca_1min_refresh.py - append the latest 1-minute SIP bars from Alpaca to every <SYM>_1.csv in the two 1-minute
stores (the DTN/IQFeed subscription that fed them ended 2026-09-09 and both stores stop on 2026-08-14; user decision
2026-09-18: "top the 1-minute store off from Alpaca, the way the daily store already is").

Two stores, same layout and the same symbol list, refreshed independently (--store):
  C:\StockData\IQbars_1min_adjusted\<SYM>_1.csv   split-adjusted to today's basis  (Alpaca adjustment=split)
  C:\StockData\IQbars_1min\<SYM>_1.csv            raw, as traded                   (Alpaca adjustment=raw)
Rows are SYM,MM/DD/YYYY,HH:MM,O,H,L,C,V - 4-decimal prices, whole volume, Time = the bar's START minute in US/Eastern
(09:30 holds the opening print, 16:00 the closing auction), 04:00..19:59 only, in time order. The AmiBroker 1-minute
database is rebuilt from the adjusted store by a separate tool; this one only appends rows.

Per run (hours for ~12,900 symbols x 2 stores):
  1. Lists the store's <SYM>_1.csv files (DTN names), skipping the breadth / index families (.Z .X .XO are not on
     Alpaca) and, unless --all, files whose last bar is older than STALE_DAYS (delisted). Maps DTN names to Alpaca
     names against Alpaca's active asset list (identical for common stock, class shares, .WS warrants and .U units;
     preferreds ABR-D -> ABR.PRD). Names Alpaca does not know go to _refresh_unmatched_1min.txt and are left alone
     (one unknown symbol fails the whole request).
  2. Reads each file's final day from its last TAIL_BYTES only (the largest file is 3.7M rows and only its tail
     matters) and asks Alpaca for 1-minute SIP bars from 04:00 ET on that date through the last completed session
     (today only after 20:10 ET, else yesterday), in batches of BATCH symbols, paged with limit 10000 and
     next_page_token. Bars are formatted as each page arrives so the page's JSON can be freed.
  3. BASIS RULE - the last stored date is also the first fetched date, so the stored closes and Alpaca's are compared
     minute by minute on it. |median ratio - 1| <= SPLIT_TOL = same basis, so the bars after the last stored minute
     are appended. A constant ratio away from 1 = a split since the store ended -> NOTHING is appended and NOTHING is
     rescaled; the symbol and the ratio go to _refresh_split_pending.txt for AmiBroker_AFL/scripts/split_adjust.py,
     which owns every rescale. Fewer than MIN_OVERLAP shared minutes -> appended with a note in the log.
  4. Appends in one write, in append mode, only after the checks, so a crash can neither truncate nor rewrite stored
     history. Each file's own last bar is the state, so a stopped run just continues where it left off.
Key: config_local.py, imported with the shared helpers from alpaca_daily_refresh (the paper key works for market
data). Never printed.

usage:  python alpaca_1min_refresh.py [--dry-run] [--symbols AAPL,KR,SPY] [--all] [--store adjusted|raw|both]
        (own visible window; ends with REFRESH-1MIN-DONE)
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import statistics
import sys
import time
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from alpaca_daily_refresh import (BARS_URL, NY, SKIP_SUFFIX, STALE_DAYS, alpaca_assets,  # noqa: E402
                                  alpaca_name, get_json, log, parse_mdy)

STORES = {"adjusted": (r"C:\StockData\IQbars_1min_adjusted", "split"),
          "raw": (r"C:\StockData\IQbars_1min", "raw")}
PENDING = os.path.join(STORES["adjusted"][0], "_refresh_split_pending.txt")  # basis changes, for split_adjust.py
UNMATCHED_NAME = "_refresh_unmatched_1min.txt"
BATCH = 25                     # symbols per request; ~4,700 new bars each, so a batch is ~12 pages and ~15 MB
PAGE_LIMIT = 10000
SPLIT_TOL = 0.002              # 0.2% - wider than a rounding difference, far narrower than any split
MIN_OVERLAP = 20               # shared minutes needed before the ratio is trusted
TAIL_BYTES = 131072            # covers a full 04:00-19:59 day (960 bars) of any symbol's rows
SESSION_END = dt.time(20, 10)  # today's bars are complete only after this
FIRST_MINUTE = "04:00"
LAST_MINUTE = "19:59"
MAX_PER_MIN = 180              # Alpaca's free market-data plan allows 200/min; this tool makes ~12,000 a run
PROGRESS_EVERY = 200
_REQUEST_TIMES: list = []


def pace() -> None:
    """Block until one more Alpaca request keeps the last 60 seconds under MAX_PER_MIN.

    The daily refresher makes a handful of requests and needs no limiter; a 1-minute top-off makes thousands.
    """
    now = time.time()
    while _REQUEST_TIMES and now - _REQUEST_TIMES[0] > 60:
        _REQUEST_TIMES.pop(0)
    if len(_REQUEST_TIMES) >= MAX_PER_MIN:
        time.sleep(max(0.0, 61 - (now - _REQUEST_TIMES[0])))
    _REQUEST_TIMES.append(time.time())


def store_symbols(store_dir: str) -> list:
    """The DTN symbols of a store's <SYM>_1.csv files, without the .Z / .X / .XO families Alpaca does not carry."""
    return sorted(f[:-len("_1.csv")] for f in os.listdir(store_dir)
                  if f.endswith("_1.csv") and not f[:-len("_1.csv")].endswith(SKIP_SUFFIX))


def tail_day_rows(path: str) -> tuple:
    """(last date MM/DD/YYYY, last time HH:MM, {HH:MM: close}) for a store file's final day, read from its tail.

    Only the last TAIL_BYTES are read, so a 3.7-million-row file costs one seek. Returns ('', '', {}) for a file
    with no usable data rows.
    """
    with open(path, "rb") as fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        fh.seek(max(0, size - TAIL_BYTES))
        chunk = fh.read().decode("ascii", "ignore")
    lines = chunk.splitlines()
    if size > TAIL_BYTES and lines:
        lines = lines[1:]                                   # the window may have cut the first line in half
    rows = []
    for line in lines:
        p = line.split(",")
        if len(p) >= 8 and len(p[1]) == 10 and len(p[2]) == 5 and p[1][2] == "/":
            rows.append(p)
    if not rows:
        return "", "", {}
    last_date = rows[-1][1]
    return last_date, rows[-1][2], {p[2]: float(p[6]) for p in rows if p[1] == last_date}


def et_date_time(ts: str) -> tuple:
    """('MM/DD/YYYY', 'HH:MM') in US/Eastern for an Alpaca RFC3339 UTC bar timestamp."""
    d = dt.datetime.fromisoformat(ts).astimezone(NY)
    return f"{d.month:02d}/{d.day:02d}/{d.year}", f"{d.hour:02d}:{d.minute:02d}"


def request_window(last_date: dt.date, allowed: dt.date) -> tuple:
    """(start, end) RFC3339 UTC for a fetch: 04:00 ET on the last stored date through 20:00 ET on the newest
    accepted session, but never inside the last 16 minutes - the free plan refuses SIP queries that reach into the
    most recent 15 minutes."""
    start = dt.datetime.combine(last_date, dt.time(4, 0), tzinfo=NY).astimezone(dt.timezone.utc)
    day_end = dt.datetime.combine(allowed, dt.time(20, 0), tzinfo=NY).astimezone(dt.timezone.utc)
    end = min(dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=16), day_end)
    return start.strftime("%Y-%m-%dT%H:%M:%SZ"), end.strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_bars(names: dict, start: str, end: str, adjustment: str) -> dict:
    """{DTN symbol: [(MM/DD/YYYY, HH:MM, close, store row), ...]} for one batch, all pages, 04:00..19:59 ET only.

    `names` maps Alpaca symbol -> DTN symbol. Each page's bars are formatted immediately so the JSON can be freed;
    holding 10,000 bar dicts per page instead would cost hundreds of megabytes over a batch.
    """
    out = {d: [] for d in names.values()}
    token = None
    while True:
        q = {"symbols": ",".join(names), "timeframe": "1Min", "start": start, "end": end, "limit": PAGE_LIMIT,
             "adjustment": adjustment, "feed": "sip", "sort": "asc"}
        if token:
            q["page_token"] = token
        pace()
        r = get_json(BARS_URL + "?" + urllib.parse.urlencode(q))
        for a, bars in r.get("bars", {}).items():
            sym = names.get(a)
            if sym is None:
                continue
            rows = out[sym]
            for b in bars:
                d, t = et_date_time(b["t"])
                if FIRST_MINUTE <= t <= LAST_MINUTE:
                    rows.append((d, t, b["c"], f"{sym},{d},{t},{b['o']:.4f},{b['h']:.4f},{b['l']:.4f},"
                                              f"{b['c']:.4f},{int(b['v'])}"))
        token = r.get("next_page_token")
        if not token:
            return out


def append_rows(path: str, lines: list) -> None:
    """Append formatted rows in one write. Append mode after the checks: existing rows are never rewritten and a
    crash cannot truncate the file."""
    with open(path, "a", encoding="ascii", newline="") as fh:
        fh.write("".join(line + "\n" for line in lines))
        fh.flush()
        os.fsync(fh.fileno())


def record_split_pending(store: str, sym: str, ratio: float, overlap: int, last_date: str) -> None:
    """Park a detected basis change for AmiBroker_AFL/scripts/split_adjust.py; this tool never rescales a store file.
    One line per store+symbol; a symbol already waiting is not written twice."""
    key = f"{store},{sym},"
    if os.path.exists(PENDING):
        with open(PENDING, encoding="utf-8") as fh:
            if any(line.startswith(key) for line in fh):
                return
    new = not os.path.exists(PENDING)
    with open(PENDING, "a", encoding="utf-8", newline="") as fh:
        if new:
            fh.write("# store,symbol,store_over_alpaca_ratio,overlap_minutes,store_last_date,detected_on\n")
        fh.write(f"{store},{sym},{ratio:.6f},{overlap},{last_date},{dt.date.today().isoformat()}\n")


def refresh_store(store: str, only: list | None, dry_run: bool, include_stale: bool, assets: set,
                  allowed: dt.date) -> None:
    """Top one store off: scan its files' last bars, fetch, basis-check, append. Logs its own summary."""
    store_dir, adjustment = STORES[store]
    syms = only or store_symbols(store_dir)
    log(f"[{store}] {len(syms):,} symbols in {store_dir}; adjustment={adjustment}; bars accepted through {allowed}")
    work, unmatched, stale, current, missing = [], [], 0, 0, 0
    t_scan = time.time()
    for s in syms:
        path = os.path.join(store_dir, f"{s}_1.csv")
        if not os.path.exists(path):
            missing += 1
            continue
        last_date, last_time, _ = tail_day_rows(path)
        if not last_date:
            continue
        last = parse_mdy(last_date)
        if last >= allowed:
            current += 1
            continue
        if not include_stale and (allowed - last).days > STALE_DAYS:
            stale += 1
            continue
        a = alpaca_name(s, assets)
        if not a:
            unmatched.append(s)
            continue
        work.append((s, a, last, last_date, last_time))
    if only is None and not dry_run:                        # a --symbols run must not clobber the full list
        with open(os.path.join(store_dir, UNMATCHED_NAME), "w", encoding="utf-8") as fh:
            fh.write("\n".join(unmatched))
    log(f"[{store}] to refresh {len(work):,}; already current {current:,}; stale (> {STALE_DAYS} days, delisted?) "
        f"{stale:,}; not on Alpaca {len(unmatched):,} -> {UNMATCHED_NAME}; no store file {missing:,} "
        f"(scan {time.time() - t_scan:.0f}s)")
    by_last = {}
    for item in work:
        by_last.setdefault(item[2], []).append(item)
    done = appended = files = pending = thin = empty = 0
    t0 = time.time()
    mark = PROGRESS_EVERY
    for last, items in sorted(by_last.items()):
        start, end = request_window(last, allowed)
        log(f"[{store}] {len(items):,} symbols ending {last}: requesting {start} .. {end}")
        for i in range(0, len(items), BATCH):
            batch = items[i:i + BATCH]
            bars = fetch_bars({a: s for s, a, _, _, _ in batch}, start, end, adjustment)
            for s, _, _, last_date, last_time in batch:
                got = bars.get(s, [])
                new = [line for d, t, _, line in got if d != last_date or t > last_time]
                if not new:
                    empty += 1
                    continue
                path = os.path.join(store_dir, f"{s}_1.csv")
                _, _, closes = tail_day_rows(path)
                ratios = [closes[t] / c for d, t, c, _ in got if d == last_date and c and t in closes]
                ratio = statistics.median(ratios) if ratios else 1.0
                if ratios and abs(ratio - 1) > SPLIT_TOL:
                    log(f"  [{store}] split basis change {s}: store/Alpaca close ratio {ratio:.4f} on {len(ratios)} "
                        f"overlap minutes of {last_date} -> {os.path.basename(PENDING)}, nothing appended, nothing "
                        f"rescaled{' (dry run)' if dry_run else ''}")
                    pending += 1
                    if not dry_run:
                        record_split_pending(store, s, ratio, len(ratios), last_date)
                    continue
                if len(ratios) < MIN_OVERLAP:
                    log(f"  [{store}] {s}: only {len(ratios)} overlap minutes on {last_date} - basis unverified, "
                        f"appending {len(new):,} bars anyway")
                    thin += 1
                if not dry_run:
                    append_rows(path, new)
                appended += len(new)
                files += 1
            done += len(batch)
            if done >= mark or done == len(work):
                el = max(time.time() - t0, 0.001)
                log(f"  [{store}] {done:,}/{len(work):,} symbols  files +{files:,} bars +{appended:,}  "
                    f"pending {pending}  thin {thin}  no-new {empty:,}  {el:.0f}s  {done / el * 60:.0f} sym/min")
                mark = done + PROGRESS_EVERY
    log(f"[{store}] {'DRY RUN: would append' if dry_run else 'appended'} {appended:,} bars to {files:,} files; "
        f"split basis changes {pending} (nothing appended, see {os.path.basename(PENDING)}); thin overlap {thin}; "
        f"no new bars {empty:,}; {time.time() - t0:.0f}s")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="fetch and report, write nothing")
    ap.add_argument("--symbols", help="comma list (smoke test)")
    ap.add_argument("--all", action="store_true", help=f"include files older than {STALE_DAYS} days (the full run)")
    ap.add_argument("--store", choices=["adjusted", "raw", "both"], default="both")
    a = ap.parse_args()
    now = dt.datetime.now(NY)
    allowed = now.date() if now.time() >= SESSION_END else now.date() - dt.timedelta(days=1)
    assets = alpaca_assets()
    log(f"Alpaca active assets {len(assets):,}; newest session accepted {allowed} (now {now:%Y-%m-%d %H:%M} ET)")
    only = [s.strip().upper() for s in a.symbols.split(",")] if a.symbols else None
    for store in (["adjusted", "raw"] if a.store == "both" else [a.store]):
        refresh_store(store, only, a.dry_run, a.all, assets, allowed)
    print("REFRESH-1MIN-DONE", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
