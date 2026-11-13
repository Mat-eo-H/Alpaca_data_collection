r"""store_state.py - what a store's nightly refresh already knows about each file, so it does not open the file again
to learn it (2026-11-12, the efficiency review's finding 5; the user: "keep a state file per store as you described it
but add the last split date processed").

Before, each refresh opened every store file and read its end to learn its last date: about 10 minutes for the 1-minute
store and 7 to 14 for the daily store, every night. Now each store keeps `<store>\_refresh_state.csv`, one row per
symbol the Alpaca refresher handles:

    symbol,size,mtime_ns,last_date,last_time,last_split,closes
  size, mtime_ns     the file's size and modified time when the row was written
  last_date, _time   the file's last bar (MM/DD/YYYY, HH:MM) - what the scan read each file's end for
  last_split         the newest split date (YYYY-MM-DD) the split tool has applied to this file, from the store's ledger
                     `_adjustments_applied.csv`; '' = none. The basis the row was written on.
  closes             the daily store only: its last closes as 'MM/DD/YYYY:close|...', what the split check compares with
                     Alpaca's overlap days ('' in the 1-minute store, whose check reads the last day's minutes itself)

A row is TRUSTED only while the folder listing still shows the same size and modified time (os.scandir: no file is
opened) AND the ledger still shows the same last split. Anything else - no row, a file changed by the split tool or by
hand, a newer split in the ledger - falls back to reading the file's end, as before, and the row is rewritten from what
was read. The file is written whole to .tmp and renamed; the refreshers hold the store's writer lock while they do it.
"""
from __future__ import annotations

import csv
import os

STATE_NAME = "_refresh_state.csv"
LEDGER_NAME = "_adjustments_applied.csv"      # AmiBroker_AFL/scripts/split_adjust.py's record of what it applied
FIELDS = ["symbol", "size", "mtime_ns", "last_date", "last_time", "last_split", "closes"]


def load_state(store_dir: str) -> dict:
    """{symbol: row} from the store's state file; {} when there is none yet or its header is not this version's (every
    file then falls back to a read)."""
    path = os.path.join(store_dir, STATE_NAME)
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames != FIELDS:
            return {}
        return {r["symbol"]: r for r in reader}


def save_state(store_dir: str, state: dict) -> None:
    """Write the state whole to .tmp and rename it into place (a reader sees the old file or the new one)."""
    path = os.path.join(store_dir, STATE_NAME)
    with open(path + ".tmp", "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        for sym in sorted(state):
            w.writerow(state[sym])
    os.replace(path + ".tmp", path)


def folder_stats(store_dir: str, suffix: str) -> dict:
    """{symbol: (size, mtime_ns)} for every `<SYM><suffix>` file, from ONE folder listing: on Windows os.scandir hands
    back each entry's size and modified time from the listing itself, so no file is opened."""
    out = {}
    with os.scandir(store_dir) as entries:
        for e in entries:
            if e.name.endswith(suffix):
                st = e.stat()
                out[e.name[:-len(suffix)]] = (st.st_size, st.st_mtime_ns)
    return out


def file_stats(path: str):
    """(size, mtime_ns) of one file, or None when it does not exist."""
    try:
        st = os.stat(path)
    except FileNotFoundError:
        return None
    return st.st_size, st.st_mtime_ns


def ledger_last_splits(store_dir: str) -> dict:
    """{symbol: 'YYYY-MM-DD'}: the newest split date the split tool has applied (its price leg or its volume leg) to
    each symbol's file in this store; {} when the store has no ledger."""
    path = os.path.join(store_dir, LEDGER_NAME)
    out: dict = {}
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8", errors="replace", newline="") as fh:
        for r in csv.DictReader(fh):
            if "1" in (r["price_applied"][:1], r["volume_applied"][:1]) and r["date"] > out.get(r["symbol"], ""):
                out[r["symbol"]] = r["date"]
    return out


def trusted(row, stats, last_split: str) -> bool:
    """True while a state row still describes its file: the same size, the same modified time and the same last
    applied split."""
    return (row is not None and stats is not None and row["size"] == str(stats[0])
            and row["mtime_ns"] == str(stats[1]) and row["last_split"] == last_split)


def pack_closes(pairs) -> str:
    """[(MM/DD/YYYY, close), ...] -> 'MM/DD/YYYY:close|...' (repr of the float, so it reads back exactly)."""
    return "|".join(f"{d}:{c!r}" for d, c in pairs)


def unpack_closes(text: str) -> dict:
    """'MM/DD/YYYY:close|...' -> {MM/DD/YYYY: close} in the stored (date) order."""
    return {d: float(c) for d, c in (item.split(":") for item in text.split("|") if item)}


def make_row(symbol: str, stats, last_date: str, last_time: str, last_split: str, closes=()) -> dict:
    """One state row for a file as it is now: `stats` = its (size, mtime_ns), the last bar's date and time, the
    ledger's last split, and (the daily store) the closes the split check will want."""
    return {"symbol": symbol, "size": str(stats[0]), "mtime_ns": str(stats[1]), "last_date": last_date,
            "last_time": last_time, "last_split": last_split, "closes": pack_closes(closes)}


def row_after_write(symbol: str, path: str, lines: list, last_split: str, closes=None, keep: int = 0) -> dict:
    """The state row for a file this run has just written `lines` (formatted store rows, oldest first) to the end of:
    its size and modified time now, its last bar from the last line, and - `keep` > 0, the daily store - the last
    `keep` closes of `closes` ({date: close} the file held before) plus the new rows'. A close is taken from the row's
    own text, so it is the value a later read of the file would give."""
    fields = [line.split(",") for line in lines]
    pairs = (list((closes or {}).items()) + [(p[1], float(p[6])) for p in fields])[-keep:] if keep else []
    return make_row(symbol, file_stats(path), fields[-1][1], fields[-1][2], last_split, pairs)
