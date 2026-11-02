r"""alpaca_today_pull.py - pull the last TWO sessions' 1-minute SIP bars from Alpaca (yesterday whole + today so far) for
the daily report's symbol list, hours before the 21:30 store refresh has today (2026-10-21, the report chat's
"REPORT -> DATA 2026-10-21 (18:45)"; the user's timing: the report writes its list ~16:15, this pull starts 16:20, the
report builds at 16:30 or when this says DONE).

The bars land BESIDE the store, never in it: C:\StockData\alpaca_today\<YYYY-MM-DD>\<SYM>_1.csv in the store's columns
(Symbol,Date,Time,O,H,L,C,V with the header row, 4-decimal prices, Time = the bar's start minute ET, 04:00..19:59).
Appending to IQbars_1min_adjusted instead would make the 21:30 refresh see the file as current and skip that session's
after-hours bars - each file's last bar is that tool's state. A READER takes every date a folder file holds from the
folder and only the earlier dates from the store.

Per run:
  1. Waits up to --wait-list minutes for the list (default C:\StockData\report_requests\alpaca_today_<date>.txt, one
     symbol per line, '#' starts a comment) and maps the names to Alpaca's (alpaca_name; unknown -> not-on-alpaca).
  2. Reads each symbol's last bar in the adjusted store and fetches, with the 21:30 refresher's own request code, every
     bar from 04:00 on that date through the newest the free plan serves (now - 16 minutes), and writes ALL of them:
     the store's last session (normally yesterday) re-pulled whole + today so far (user 2026-10-21: "always get 2 days
     of data so that if any of yesterday's early data was incorrect or modified it's updated"; more sessions if the
     store missed one). No store file, or a last bar older than LOOKBACK_DAYS -> the day's session only (status
     no-store / store-stale). early_changed = yesterday's minutes, up to the last one yesterday's pull reached, that
     differ from the early copy in alpaca_today\<yesterday>\ (blank without one) - how often an early pull is revised.
  3. BASIS CHECK, the refresher's rule: the store/Alpaca close ratio on the overlap day; away from 1 by more than
     SPLIT_TOL = a split since the store's last bar -> the bars are still written AS PUBLISHED (the day's basis) and the
     symbol is flagged basis-change with its ratio, for the reader to apply at join time. The store is never touched
     (the 21:30 refresher parks the same change for split_adjust.py).
  4. Each file is written whole to .tmp and renamed, so a reader never sees half a file. _symbols.csv in the date
     folder: symbol,alpaca,store_last,first_bar,last_bar,bars,ratio,overlap,status,early_changed per listed symbol.
Marker C:\StockData\alpaca_today\_status.txt (Sync_AmiBroker/run_monitor.py --status):
  RUNNING <date> <step>  ->  DONE <date> through=HH:MM symbols=N written=N bars=N basis_change=N not_on_alpaca=N
  no_bars=N at=HH:MM:SS  |  FAILED <date> <reason>.   A reader checks the first word AND the date.

usage:  python alpaca_today_pull.py [--date 2026-10-22] [--list PATH | --symbols AAPL,KR] [--wait-list 10]
        (own visible window via alpaca_today_pull.ps1; task "Alpaca today pull" weekdays 16:20 = alpaca_today_pull_task.ps1)
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from alpaca_daily_refresh import LOCK_BUSY_EXIT, NY, alpaca_assets, alpaca_name, lock_log, log, parse_mdy  # noqa: E402
from alpaca_1min_refresh import (MIN_OVERLAP, SPLIT_TOL, STORES, fetch_bars, request_window,  # noqa: E402
                                 tail_day_rows)
from common_trading_lib import StoreLockTimeout, store_lock  # noqa: E402  (REQ-007, v0.10.0, 2026-10-22)

OUT_ROOT = r"C:\StockData\alpaca_today"
STATUS = os.path.join(OUT_ROOT, "_status.txt")
LIST_PATH = r"C:\StockData\report_requests\alpaca_today_{date}.txt"
HEADER = "Symbol,Date,Time,Open,High,Low,Close,Volume\n"
BATCH = 25                      # symbols per request, the refresher's size (~2 sessions of bars each, a few pages)
LOOKBACK_DAYS = 10              # a store tail older than this is not fetched from (a reused ticker could ask for years)
LOCK_TIMEOUT_S = 300            # the folder's lock (REQ-007): the report's pull and the watcher's pulls share it
FIELDS = ["symbol", "alpaca", "store_last", "first_bar", "last_bar", "bars", "ratio", "overlap", "status",
          "early_changed"]


def mark(text: str) -> None:
    """Write the run_monitor status marker (one line) and echo it to the log."""
    with open(STATUS, "w", encoding="utf-8") as fh:
        fh.write(text + "\n")
    log(f"=== {text}")


def read_list(path: str, wait_min: float) -> list:
    """The listed symbols, upper case, unique, in file order; waits up to wait_min minutes for the file to appear.
    Returns [] when it never does. The writer is asked to write it whole and rename, so it is never read half-done."""
    deadline = time.time() + wait_min * 60
    while not os.path.exists(path):
        if time.time() >= deadline:
            return []
        time.sleep(15)
    with open(path, encoding="utf-8-sig") as fh:
        syms = [line.split("#")[0].strip().upper() for line in fh]
    return list(dict.fromkeys(s for s in syms if s))


def write_whole(path: str, text: str) -> None:
    """Write a file to .tmp and rename it into place, so a reader sees the old file or the new one, never half."""
    with open(path + ".tmp", "w", encoding="ascii", newline="") as fh:
        fh.write(text)
    os.replace(path + ".tmp", path)


def et_time(utc_stamp: str) -> dt.datetime:
    """A request window's RFC3339 UTC end as a US/Eastern datetime (the marker's through=)."""
    return dt.datetime.strptime(utc_stamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).astimezone(NY)


def pull(day: dt.date, syms: list, out_dir: str, side_files: bool = True) -> str:
    """Fetch, basis-check and write every listed symbol into out_dir; returns the DONE marker text. `side_files=False`
    (--no-status, the refresh watcher's calls) leaves the date folder's _symbols.csv as the report's pull wrote it."""
    assets = alpaca_assets()
    store_dir, adjustment = STORES["adjusted"]
    info, groups = {}, {}
    for s in syms:
        path = os.path.join(store_dir, f"{s}_1.csv")
        last_date, last_time, closes = tail_day_rows(path) if os.path.exists(path) else ("", "", {})
        r = {"symbol": s, "alpaca": alpaca_name(s, assets), "store_last": f"{last_date} {last_time}".strip(),
             "first_bar": "", "last_bar": "", "bars": 0, "ratio": "", "overlap": 0, "status": "ok",
             "early_changed": "", "_last": (last_date, last_time), "_closes": closes}
        start = day
        if not last_date:
            r["status"] = "no-store"
        elif (day - parse_mdy(last_date)).days > LOOKBACK_DAYS:
            r["status"], r["_last"], r["_closes"] = "store-stale", ("", ""), {}
        else:
            start = min(parse_mdy(last_date), day)
        if not r["alpaca"]:
            r["status"] = "not-on-alpaca"
        else:
            groups.setdefault(start, []).append(s)
        info[s] = r
    through = et_time(request_window(day, day)[1])
    for start, members in sorted(groups.items()):
        w_start, w_end = request_window(start, day)
        through = et_time(w_end)
        log(f"{len(members):,} symbols with store tail from {start}: requesting {w_start} .. {w_end}")
        for i in range(0, len(members), BATCH):
            batch = members[i:i + BATCH]
            bars = fetch_bars({info[s]["alpaca"]: s for s in batch}, w_start, w_end, adjustment)
            for s in batch:
                r, got = info[s], bars.get(s, [])
                last_date = r["_last"][0]
                new = [(d, t, line) for d, t, _, line in got]   # the store's last session whole + today: 2 sessions
                if last_date:
                    r["early_changed"] = early_changes(os.path.join(OUT_ROOT, parse_mdy(last_date).isoformat(),
                                                                    f"{s}_1.csv"), last_date, got)
                ratios =[r["_closes"][t] / c for d, t, c, _ in got if d == last_date and c and t in r["_closes"]]
                r["overlap"] = len(ratios)
                if ratios:
                    ratio = statistics.median(ratios)
                    r["ratio"] = f"{ratio:.6f}"
                    if abs(ratio - 1) > SPLIT_TOL:
                        r["status"] = "basis-change"
                        log(f"  {s}: store/Alpaca close ratio {ratio:.4f} on {len(ratios)} minutes of {last_date} "
                            f"- written as published, flagged basis-change")
                if r["status"] == "ok" and len(ratios) < MIN_OVERLAP:
                    r["status"] = "thin-overlap"
                if not new:
                    r["status"] = "no-bars" if r["status"] in ("ok", "thin-overlap", "no-store") else r["status"]
                    continue
                write_whole(os.path.join(out_dir, f"{s}_1.csv"), HEADER + "".join(line + "\n" for _, _, line in new))
                r["first_bar"], r["last_bar"] = f"{new[0][0]} {new[0][1]}", f"{new[-1][0]} {new[-1][1]}"
                r["bars"] = len(new)
            log(f"  {min(i + BATCH, len(members)):,}/{len(members):,} symbols")
    rows = list(info.values())
    if side_files:
        write_whole(os.path.join(out_dir, "_symbols.csv"),
                    ",".join(FIELDS) + "\n" + "".join(",".join(str(r[f]) for f in FIELDS) + "\n" for r in rows))
    else:                                            # the watcher reads the per-symbol outcome from this log
        for r in rows:
            log(f"  {r['symbol']}: {r['status']} bars={r['bars']} first={r['first_bar']} last={r['last_bar']}")
    count = {k: sum(1 for r in rows if r["status"] == k) for k in ("basis-change", "not-on-alpaca", "no-bars")}
    early = [r["early_changed"] for r in rows if r["early_changed"] != ""]
    log(f"early copies compared {len(early)}: {sum(1 for n in early if n)} revised, {sum(early):,} minutes changed")
    return (f"DONE {day} through={through:%H:%M} symbols={len(rows)} written={sum(1 for r in rows if r['bars'])} "
            f"bars={sum(r['bars'] for r in rows)} basis_change={count['basis-change']} "
            f"not_on_alpaca={count['not-on-alpaca']} no_bars={count['no-bars']} at={dt.datetime.now(NY):%H:%M:%S}")


def early_changes(early_path: str, mdy: str, got: list):
    """How many of date `mdy`'s minutes, up to the last one the early copy at `early_path` holds, differ between that
    copy and the fresh pull `got` (a changed O/H/L/C/V, a bar added late, a bar withdrawn); '' when there is no copy."""
    if not os.path.exists(early_path):
        return ""
    with open(early_path, encoding="ascii") as fh:
        early = {p[2]: line for line in fh.read().splitlines() if len(p := line.split(",")) >= 8 and p[1] == mdy}
    fresh = {t: line for d, t, _, line in got if d == mdy}
    upto = max(early, default="")
    return sum(1 for t in early.keys() | fresh.keys() if t <= upto and early.get(t) != fresh.get(t))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", help="session YYYY-MM-DD (default today, US/Eastern)")
    ap.add_argument("--list", help=f"symbol list file (default {LIST_PATH})")
    ap.add_argument("--symbols", help="comma list instead of the list file (smoke test)")
    ap.add_argument("--wait-list", type=float, default=10, help="minutes to wait for the list file (default 10)")
    ap.add_argument("--no-status", action="store_true",
                    help="with --symbols (the refresh watcher): leave _status.txt and _symbols.csv to the report's pull")
    ap.add_argument("--lock-timeout", type=float, default=LOCK_TIMEOUT_S,
                    help=f"seconds to wait for another pull of the folder (default {LOCK_TIMEOUT_S}; 0 = one try)")
    a = ap.parse_args()
    day = dt.date.fromisoformat(a.date) if a.date else dt.datetime.now(NY).date()
    out_dir = os.path.join(OUT_ROOT, day.isoformat())
    os.makedirs(out_dir, exist_ok=True)
    if a.symbols:
        syms = list(dict.fromkeys(s.strip().upper() for s in a.symbols.split(",") if s.strip()))
    else:
        path = a.list or LIST_PATH.format(date=day.isoformat())
        mark(f"RUNNING {day} waiting-for-list {path}")
        syms = read_list(path, a.wait_list)
        if not syms:
            mark(f"FAILED {day} no-list (nothing at {path} after {a.wait_list:g} min)")
            return 1
    note = log if a.no_status else mark             # --no-status: the report's marker is never touched
    note(f"RUNNING {day} pulling {len(syms)} symbols")
    t0 = time.time()
    try:
        with store_lock(OUT_ROOT, "alpaca_today_pull", timeout_s=a.lock_timeout, log=lock_log):
            done = pull(day, syms, out_dir, side_files=not a.no_status)
    except StoreLockTimeout as e:                    # another pull holds the folder: busy, not broken
        note(f"FAILED {day} lock busy - {e}")
        print("LOCK-BUSY", flush=True)
        return LOCK_BUSY_EXIT
    except (Exception, SystemExit) as e:
        note(f"FAILED {day} {str(e).strip('! ') or type(e).__name__}")
        raise
    note(done)
    log(f"{time.time() - t0:.0f}s")
    print("ALPACA-TODAY-DONE", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
