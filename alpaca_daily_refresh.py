r"""alpaca_daily_refresh.py - append the latest split-adjusted daily bars from Alpaca to every <SYM>_daily.csv in
C:\StockData\IQbars_Daily (the DTN/IQFeed subscription ended 2026-09-09; user decision 2026-09-10: "build the Alpaca daily
refresher for IQbars_Daily and the ETF store").

Per run (a few minutes for ~13,000 symbols):
  1. Lists the store's <SYM>_daily.csv files (DTN names), skipping the breadth / index families (.Z .X .XO are not on Alpaca;
     the CBOE .XO set is refreshed by AmiBroker_AFL/symbol_list_update/cboe_vix_download.py).
  2. Maps DTN names to Alpaca names against Alpaca's active asset list (identical for common stock, class shares, .WS warrants
     and .U units; preferreds ABR-D -> ABR.PRD). Names Alpaca does not know go to _refresh_unmatched.txt and are left alone
     (one unknown symbol fails the whole request). Files whose last bar is older than STALE_DAYS are skipped as delisted.
  3. Reads each file's last bar date, asks Alpaca for split-adjusted SIP daily bars from OVERLAP_DAYS before it (batches of
     BATCH symbols, paged) and compares the closes on the overlapping days: a constant ratio away from 1 = a split since the
     last refresh -> the whole file is rescaled to Alpaca's basis (prices divided by the ratio, volume multiplied) before the
     new bars are appended. DTN's basis and Alpaca's agreed to the cent on the overlap days checked 2026-09-10.
  4. Appends the bars after the last date in the store's own format: SYM,MM/DD/YYYY,00:00,O,H,L,C,V (4-decimal prices, whole
     volume). Today's bar is taken only after 16:10 ET (before that Alpaca serves the partial session).
Key: config_local.py (API_KEY / API_SECRET, the paper key works for market data). Never printed.

usage:  python alpaca_daily_refresh.py [--dry-run] [--symbols AAPL,XLK,BRK.B] [--all]
        (own visible window; Sync_AmiBroker/daily_refresh.ps1 runs it every weekday evening; ends with REFRESH-DONE)
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zoneinfo

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config_local import API_KEY, API_SECRET  # noqa: E402

STORE = r"C:\StockData\IQbars_Daily"
UNMATCHED = os.path.join(STORE, "_refresh_unmatched.txt")
BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
ASSETS_URL = "https://paper-api.alpaca.markets/v2/assets?status=active&asset_class=us_equity"
HEADERS = {"APCA-API-KEY-ID": API_KEY, "APCA-API-SECRET-KEY": API_SECRET}
NY = zoneinfo.ZoneInfo("America/New_York")
SKIP_SUFFIX = (".Z", ".X", ".XO")
BATCH = 150
OVERLAP_DAYS = 10
STALE_DAYS = 45
SPLIT_TOL = 0.005
CLOSE_TIME = dt.time(16, 10)


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def get_json(url: str, retries: int = 5):
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=HEADERS), timeout=90) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            body = e.read()[:200].decode(errors="replace")
            if e.code == 400:
                raise SystemExit(f"!! Alpaca rejected the request: {body}")
            log(f"  HTTP {e.code} ({body}); retry {attempt + 1}/{retries}")
            time.sleep(5 * (attempt + 1))
        except (urllib.error.URLError, TimeoutError) as e:
            log(f"  {e}; retry {attempt + 1}/{retries}")
            time.sleep(5 * (attempt + 1))
    raise SystemExit("!! Alpaca unreachable")


def alpaca_assets() -> set:
    return {a["symbol"] for a in get_json(ASSETS_URL)}


def store_symbols() -> list:
    return sorted(f[:-len("_daily.csv")] for f in os.listdir(STORE)
                  if f.endswith("_daily.csv") and not f[:-len("_daily.csv")].endswith(SKIP_SUFFIX))


def alpaca_name(sym: str, assets: set) -> str:
    """The Alpaca symbol for a DTN symbol, or '' when Alpaca has no such asset."""
    if sym in assets:
        return sym
    if "-" in sym:                                   # DTN preferred ABR-D = Alpaca ABR.PRD
        base, _, suffix = sym.rpartition("-")
        cand = f"{base}.PR{suffix}"
        if cand in assets:
            return cand
    return ""


def tail_rows(path: str, n: int = 40) -> list:
    """The last n data rows of a store file as lists of fields (newest last)."""
    with open(path, "rb") as fh:
        fh.seek(0, os.SEEK_END)
        size = fh.tell()
        fh.seek(max(0, size - 16384))
        chunk = fh.read().decode("ascii", "ignore")
    lines = [l for l in chunk.splitlines() if l and not l.startswith("Symbol,")]
    rows = [l.split(",") for l in lines[-n:]]
    return [r for r in rows if len(r) >= 8 and len(r[1]) == 10]


def parse_mdy(s: str) -> dt.date:
    return dt.date(int(s[6:10]), int(s[0:2]), int(s[3:5]))


def fmt_row(sym: str, b: dict) -> str:
    d = b["t"][:10]
    return f"{sym},{d[5:7]}/{d[8:10]}/{d[0:4]},00:00,{b['o']:.4f},{b['h']:.4f},{b['l']:.4f},{b['c']:.4f},{int(b['v'])}"


def request_end(allowed: dt.date) -> str:
    """The request's end timestamp: the end of the newest accepted day, but never inside the last 16 minutes - the free
    plan refuses SIP queries that reach into the most recent 15 minutes ("subscription does not permit querying recent
    SIP data"), and a bare date is read as the end of that day, which is in the future during the session."""
    now_utc = dt.datetime.now(dt.timezone.utc)
    day_end = dt.datetime.combine(allowed, dt.time(23, 59, 59), tzinfo=dt.timezone.utc)
    return min(now_utc - dt.timedelta(minutes=16), day_end).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_bars(symbols: list, start: dt.date, end: str) -> dict:
    """{alpaca symbol: [bar, ...]} for the batch, all pages."""
    out, token = {}, None
    while True:
        q = {"symbols": ",".join(symbols), "timeframe": "1Day", "start": start.isoformat(), "end": end,
             "limit": 10000, "adjustment": "split", "feed": "sip", "sort": "asc"}
        if token:
            q["page_token"] = token
        r = get_json(BARS_URL + "?" + urllib.parse.urlencode(q))
        for s, bars in r.get("bars", {}).items():
            out.setdefault(s, []).extend(bars)
        token = r.get("next_page_token")
        if not token:
            return out


def rescale_file(path: str, ratio: float) -> int:
    """Store close / Alpaca close = ratio on the overlap: bring the whole file onto Alpaca's basis."""
    with open(path, encoding="ascii", errors="ignore", newline="") as fh:
        lines = fh.read().splitlines()
    out, n = [], 0
    for line in lines:
        p = line.split(",")
        if len(p) >= 8 and len(p[1]) == 10 and not line.startswith("Symbol,"):
            for i in (3, 4, 5, 6):
                p[i] = f"{float(p[i]) / ratio:.4f}"
            p[7] = str(int(round(float(p[7]) * ratio)))
            n += 1
            out.append(",".join(p))
        else:
            out.append(line)
    with open(path, "w", encoding="ascii", newline="") as fh:
        fh.write("\n".join(out) + "\n")
    return n


def refresh(only: list | None, dry_run: bool, include_stale: bool) -> None:
    now = dt.datetime.now(NY)
    allowed = now.date() if now.time() >= CLOSE_TIME else now.date() - dt.timedelta(days=1)   # newest bar accepted
    assets = alpaca_assets()
    syms = only or store_symbols()
    log(f"store symbols {len(syms):,}; Alpaca active assets {len(assets):,}; bars accepted through {allowed}")
    work, unmatched, stale, current = [], [], 0, 0
    for s in syms:
        path = os.path.join(STORE, f"{s}_daily.csv")
        if not os.path.exists(path):
            log(f"  no store file for {s} - skipped")
            continue
        rows = tail_rows(path)
        if not rows:
            continue
        last = parse_mdy(rows[-1][1])
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
        work.append((s, a, last, {r[1]: float(r[6]) for r in rows}))
    with open(UNMATCHED, "w", encoding="utf-8") as fh:
        fh.write("\n".join(unmatched))
    log(f"to refresh {len(work):,}; already current {current:,}; stale (> {STALE_DAYS} days, delisted?) {stale:,}; "
        f"not on Alpaca {len(unmatched):,} -> {UNMATCHED}")
    appended = files = splits = empty = 0
    t0 = time.time()
    end = request_end(allowed)
    log(f"request window ends {end}")
    by_last = {}
    for item in work:
        by_last.setdefault(item[2], []).append(item)
    for last, items in sorted(by_last.items()):
        for i in range(0, len(items), BATCH):
            batch = items[i:i + BATCH]
            bars = fetch_bars([a for _, a, _, _ in batch], last - dt.timedelta(days=OVERLAP_DAYS), end)
            for s, a, last_date, closes in batch:
                got = bars.get(a, [])
                new = [b for b in got if last_date < parse_mdy(f"{b['t'][5:7]}/{b['t'][8:10]}/{b['t'][0:4]}") <= allowed]
                if not new:
                    empty += 1
                    continue
                ratios = []
                for b in got:
                    key = f"{b['t'][5:7]}/{b['t'][8:10]}/{b['t'][0:4]}"
                    if key in closes and b["c"]:
                        ratios.append(closes[key] / b["c"])
                path = os.path.join(STORE, f"{s}_daily.csv")
                if ratios and abs(statistics.median(ratios) - 1) > SPLIT_TOL:
                    ratio = statistics.median(ratios)
                    log(f"  split basis change {s}: store/Alpaca close ratio {ratio:.4f} on {len(ratios)} overlap days"
                        f"{' (dry run)' if dry_run else ' -> file rescaled'}")
                    splits += 1
                    if not dry_run:
                        rescale_file(path, ratio)
                if not dry_run:
                    with open(path, "a", encoding="ascii", newline="") as fh:
                        fh.write("".join(fmt_row(s, b) + "\n" for b in new))
                appended += len(new)
                files += 1
            done = i + len(batch)
            if done % (BATCH * 10) == 0 or done == len(items):
                log(f"  {done:,}/{len(items):,} symbols ending {last}  files +{files:,} bars +{appended:,}  {time.time() - t0:.0f}s")
    log(f"{'DRY RUN: would append' if dry_run else 'appended'} {appended:,} bars to {files:,} files; splits {splits}; "
        f"no new bars {empty:,}; {time.time() - t0:.0f}s")
    print("REFRESH-DONE", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--symbols", help="comma list (smoke test)")
    ap.add_argument("--all", action="store_true", help=f"include files older than {STALE_DAYS} days")
    a = ap.parse_args()
    refresh([s.strip().upper() for s in a.symbols.split(",")] if a.symbols else None, a.dry_run, a.all)
    return 0


if __name__ == "__main__":
    sys.exit(main())
