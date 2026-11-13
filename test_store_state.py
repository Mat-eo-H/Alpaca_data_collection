r"""test_store_state.py - the refresh state file (store_state.py) and the two refreshers that use it, offline (2026-11-12).

Temp folders hold real store-format files; Alpaca is replaced by fakes (the asset list, the bar fetch); every read of a
file's end is counted. Nothing under C:\StockData is touched and no request leaves the machine.
Run: C:\Python314\python.exe test_store_state.py   -> PASS / FAIL per case, then TESTS-PASSED or TESTS-FAILED.
"""
import datetime as dt
import math
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import alpaca_1min_refresh as M  # noqa: E402
import alpaca_daily_refresh as D  # noqa: E402
import store_state as S  # noqa: E402
from common_trading_lib import write_store_bars  # noqa: E402

HEADER = "Symbol,Date,Time,Open,High,Low,Close,Volume\n"
RESULTS = []
READS = {"daily": [], "1min": []}
REAL_DAILY_TAIL, REAL_MIN_TAIL = D.tail_rows, M.tail_day_rows


def counted_daily_tail(path, n=40):
    READS["daily"].append(os.path.basename(path))
    return REAL_DAILY_TAIL(path, n)


def counted_min_tail(path):
    READS["1min"].append(os.path.basename(path))
    return REAL_MIN_TAIL(path)


D.tail_rows, M.tail_day_rows = counted_daily_tail, counted_min_tail


def check(name, ok, info=""):
    RESULTS.append(bool(ok))
    print(f"{'PASS' if ok else 'FAIL'}  {name}{'' if ok else '  -> ' + str(info)[:900]}", flush=True)


def mdy(day: dt.date) -> str:
    return f"{day:%m/%d/%Y}"


def weekdays(first: dt.date, last: dt.date) -> list:
    return [first + dt.timedelta(days=k) for k in range((last - first).days + 1)
            if (first + dt.timedelta(days=k)).weekday() < 5]


def same_closes(a: dict, b: dict) -> bool:
    """The same dates in the same order, each close equal to within float rounding (pandas' text-to-float and Python's
    can differ in the last bit; the split check's tolerance is 0.5%)."""
    return list(a) == list(b) and all(math.isclose(a[k], b[k], rel_tol=1e-12) for k in a)


# ---- the daily store ---------------------------------------------------------------------------------------------

DAYS = weekdays(dt.date(2026, 9, 1), dt.date(2026, 11, 10))
STORED_TO, THROUGH, LATER = dt.date(2026, 11, 6), dt.date(2026, 11, 10), dt.date(2026, 11, 11)


def daily_close(sym: str, day: dt.date) -> float:
    """A price with more than 4 decimals, so the stored text (4 decimals) differs from Alpaca's number."""
    return 10 + len(sym) + DAYS.index(day) * 0.123456789


def daily_bar(sym: str, day: dt.date, factor: float = 1.0) -> dict:
    c = daily_close(sym, day) * factor
    return {"t": f"{day.isoformat()}T05:00:00Z", "o": c, "h": c, "l": c, "c": c, "v": 1000 + DAYS.index(day)}


def fake_daily_fetch(symbols, start, end):
    return {a: [daily_bar(a, d) for d in DAYS if d >= start] for a in symbols}


def halved_daily_fetch(symbols, start, end):
    return {a: [daily_bar(a, d, 0.5) for d in DAYS if d >= start] for a in symbols}


def daily_store(syms: list) -> str:
    """A temp daily store holding `syms` through STORED_TO; the daily refresher is pointed at it."""
    tmp = tempfile.mkdtemp(prefix="state_daily_")
    D.STORE = tmp
    D.UNMATCHED = os.path.join(tmp, "_refresh_unmatched.txt")
    D.PENDING = os.path.join(tmp, "_pending_basis_changes.csv")
    D.alpaca_assets = lambda: set(syms) | {"NEWCO"}
    D.fetch_bars = fake_daily_fetch
    for s in syms:
        with open(os.path.join(tmp, f"{s}_daily.csv"), "w", encoding="ascii", newline="") as fh:
            fh.write(HEADER + "".join(D.fmt_row(s, daily_bar(s, d)) + "\n" for d in DAYS if d <= STORED_TO))
    return tmp


def read_daily(tmp: str, sym: str) -> tuple:
    """(last date, {date: close} of the last KEEP_CLOSES rows) from a fresh read of the file: what the old scan learned."""
    rows = REAL_DAILY_TAIL(os.path.join(tmp, f"{sym}_daily.csv"))
    return rows[-1][1], {r[1]: float(r[6]) for r in rows[-D.KEEP_CLOSES:]}


def test_daily() -> None:
    syms = ["AAA", "BBBB", "CC"]
    tmp = daily_store(syms)

    READS["daily"].clear()
    D.refresh(None, False, False, through=THROUGH)
    state = S.load_state(tmp)
    rows_ok = all(state[s]["last_date"] == read_daily(tmp, s)[0] == mdy(THROUGH)
                  and same_closes(S.unpack_closes(state[s]["closes"]), read_daily(tmp, s)[1]) for s in syms)
    check("daily run 1: every file read once, 2 bars appended, the state row = a fresh read (last date + 11 closes)",
          sorted(READS["daily"]) == sorted(f"{s}_daily.csv" for s in syms) and rows_ok, (READS["daily"], state))
    check("daily: after the append the folder listing shows the size and time the row recorded",
          all(S.trusted(state[s], S.folder_stats(tmp, "_daily.csv")[s], "") for s in syms), state)
    check("daily: the stored close is the file's 4-decimal value, not Alpaca's longer number",
          list(S.unpack_closes(state["AAA"]["closes"]).values())[-1] == float(f"{daily_close('AAA', THROUGH):.4f}"),
          state["AAA"]["closes"])

    READS["daily"].clear()
    D.refresh(None, False, False, through=THROUGH)
    check("daily run 2: nothing changed -> 0 files read", READS["daily"] == [], READS["daily"])

    time.sleep(0.05)
    with open(os.path.join(tmp, "BBBB_daily.csv"), "a", encoding="ascii", newline="") as fh:
        fh.write("BBBB,11/11/2026,00:00,1.0000,1.0000,1.0000,1.0000,5\n")
    READS["daily"].clear()
    D.refresh(None, False, False, through=LATER)
    check("daily: a file changed outside the refresher -> that file alone is read, its row follows",
          READS["daily"] == ["BBBB_daily.csv"] and S.load_state(tmp)["BBBB"]["last_date"] == "11/11/2026", READS["daily"])

    with open(os.path.join(tmp, S.LEDGER_NAME), "w", encoding="utf-8", newline="") as fh:
        fh.write("symbol,date,ratio,price_applied,volume_applied,source,evidence,applied_on\n"
                 "CC,2026-11-09,2.0,1,1,test,\"a, quoted\",20261112_000000\n"
                 "AAA,2026-11-09,2.0,0,0,test,not applied,20261112_000000\n")
    READS["daily"].clear()
    D.refresh(None, False, False, through=LATER)
    st = S.load_state(tmp)
    check("daily: a newer APPLIED split in the ledger -> that file is read; an unapplied ledger row changes nothing",
          READS["daily"] == ["CC_daily.csv"] and st["CC"]["last_split"] == "2026-11-09" and st["AAA"]["last_split"] == "",
          (READS["daily"], st["CC"], st["AAA"]))

    READS["daily"].clear()
    D.refresh(["AAA", "NEWCO"], False, False, through=LATER, create_missing=True)
    st = S.load_state(tmp)
    check("daily --symbols: the other rows are kept, a created file gets its row, an unchanged file is not read",
          set(st) == {"AAA", "BBBB", "CC", "NEWCO"} and READS["daily"] == [] and st["NEWCO"]["last_date"] == mdy(THROUGH)
          and S.trusted(st["NEWCO"], S.file_stats(os.path.join(tmp, "NEWCO_daily.csv")), "")
          and same_closes(S.unpack_closes(st["NEWCO"]["closes"]), read_daily(tmp, "NEWCO")[1]), (READS["daily"], st.get("NEWCO")))

    os.remove(os.path.join(tmp, "NEWCO_daily.csv"))
    D.refresh(None, False, False, through=LATER)
    check("daily: a full run drops the row of a file that no longer exists", set(S.load_state(tmp)) == {"AAA", "BBBB", "CC"},
          set(S.load_state(tmp)))

    # the split check must still catch a basis change from the state's closes alone
    tmp2 = daily_store(syms)
    D.refresh(None, False, False, through=STORED_TO)          # seeds the state; nothing to append
    D.fetch_bars = halved_daily_fetch                         # Alpaca now serves every close at half: a 2:1 split
    READS["daily"].clear()
    D.refresh(None, False, False, through=THROUGH)
    pend = open(D.PENDING, encoding="utf-8").read() if os.path.exists(D.PENDING) else ""
    check("daily: a 2:1 basis change is caught from the state's closes (parked, nothing appended, 0 files read)",
          all(f"\n{s}," in "\n" + pend for s in syms) and READS["daily"] == []
          and all(read_daily(tmp2, s)[0] == mdy(STORED_TO) for s in syms), (pend, READS["daily"]))

    tmp3 = daily_store(syms)
    D.refresh(None, True, False, through=THROUGH)
    check("daily: a dry run writes no state file and appends nothing",
          not os.path.exists(os.path.join(tmp3, S.STATE_NAME)) and read_daily(tmp3, "AAA")[0] == mdy(STORED_TO))


# ---- the 1-minute store ------------------------------------------------------------------------------------------

DAY1, DAY2, DAY3 = dt.date(2026, 11, 9), dt.date(2026, 11, 10), dt.date(2026, 11, 11)
MINUTES = [f"{h:02d}:{m:02d}" for h in (9, 10) for m in range(0, 60, 2)][15:55]      # 40 bars, 09:30 onward


def min_item(sym: str, day: dt.date, t: str) -> tuple:
    c = 20 + len(sym) + int(t[3:]) * 0.0123456
    return mdy(day), t, c, f"{sym},{mdy(day)},{t},{c:.4f},{c:.4f},{c:.4f},{c:.4f},100"


def min_fetch(days: tuple):
    """A fake fetch serving the given days' bars for every asked symbol."""
    return lambda names, start, end, adjustment: {d: [min_item(d, day, t) for day in days for t in MINUTES]
                                                  for d in names.values()}


def test_1min() -> None:
    tmp = tempfile.mkdtemp(prefix="state_1min_")
    syms = ["AAA", "BBBB"]
    M.STORES = {"adjusted": (tmp, "split")}
    M.PENDING = os.path.join(tmp, "_refresh_split_pending.txt")
    M.fetch_bars = min_fetch((DAY1, DAY2))
    for s in syms:
        with open(os.path.join(tmp, f"{s}_1.csv"), "w", encoding="ascii", newline="") as fh:
            fh.write(HEADER + "".join(min_item(s, DAY1, t)[3] + "\n" for t in MINUTES))
    assets = set(syms)

    READS["1min"].clear()
    M.refresh_store("adjusted", None, False, False, assets, DAY2)
    st = S.load_state(tmp)
    fresh = {s: REAL_MIN_TAIL(os.path.join(tmp, f"{s}_1.csv"))[:2] for s in syms}
    check("1min run 1: each file read in the scan and once for the split check, a day appended, the row = a fresh read",
          sorted(READS["1min"]) == sorted([f"{s}_1.csv" for s in syms] * 2)
          and all((st[s]["last_date"], st[s]["last_time"]) == fresh[s] == (mdy(DAY2), MINUTES[-1]) for s in syms),
          (READS["1min"], st, fresh))
    check("1min: after the append the folder listing shows the size and time the row recorded",
          all(S.trusted(st[s], S.folder_stats(tmp, "_1.csv")[s], "") for s in syms), st)

    READS["1min"].clear()
    M.refresh_store("adjusted", None, False, False, assets, DAY2)
    check("1min run 2: nothing changed -> 0 files read", READS["1min"] == [], READS["1min"])

    M.fetch_bars = min_fetch((DAY2, DAY3))
    READS["1min"].clear()
    M.refresh_store("adjusted", None, False, False, assets, DAY3)
    st = S.load_state(tmp)
    check("1min next night: the scan opens no file; the split check reads each file it appends to, once",
          sorted(READS["1min"]) == sorted(f"{s}_1.csv" for s in syms)
          and all(st[s]["last_date"] == mdy(DAY3) for s in syms), (READS["1min"], st))

    # a row that lags its file (trusted by size and time, wrong about the last bar): caught by the split check's read
    stats = S.file_stats(os.path.join(tmp, "AAA_1.csv"))
    S.save_state(tmp, dict(st, AAA=S.make_row("AAA", stats, mdy(DAY2), MINUTES[-1], "")))
    before = open(os.path.join(tmp, "AAA_1.csv"), "rb").read()
    M.refresh_store("adjusted", None, False, False, assets, DAY3)
    check("1min: a row that lags its file -> nothing is appended, the file is untouched, the row is dropped",
          open(os.path.join(tmp, "AAA_1.csv"), "rb").read() == before and set(S.load_state(tmp)) == {"BBBB"},
          set(S.load_state(tmp)))
    READS["1min"].clear()
    M.refresh_store("adjusted", None, False, False, assets, DAY3)
    check("1min: the dropped row is rebuilt from a read on the next run",
          READS["1min"] == ["AAA_1.csv"] and S.load_state(tmp)["AAA"]["last_date"] == mdy(DAY3), READS["1min"])

    READS["1min"].clear()
    M.refresh_store("adjusted", ["BBBB", "NEWCO"], False, False, assets | {"NEWCO"}, DAY3, create_missing=True)
    st = S.load_state(tmp)
    check("1min --symbols: the other rows are kept, a created file gets its row",
          set(st) == {"AAA", "BBBB", "NEWCO"} and st["NEWCO"]["last_date"] == mdy(DAY3)
          and S.trusted(st["NEWCO"], S.file_stats(os.path.join(tmp, "NEWCO_1.csv")), ""), st.get("NEWCO"))


# ---- the module itself -------------------------------------------------------------------------------------------

def test_module() -> None:
    tmp = tempfile.mkdtemp(prefix="state_unit_")
    pairs = [("11/09/2026", 12.3457), ("11/10/2026", 0.1 + 0.2), ("11/11/2026", 1e-05)]
    check("closes pack / unpack reads back exactly", S.unpack_closes(S.pack_closes(pairs)) == dict(pairs)
          and S.unpack_closes("") == {}, S.pack_closes(pairs))
    row = S.make_row("X", (100, 5), "11/10/2026", "00:00", "2026-01-02", pairs)
    check("trusted: the same size, time and split", S.trusted(row, (100, 5), "2026-01-02"))
    check("not trusted: another size / another time / another split / no row / no file",
          not S.trusted(row, (101, 5), "2026-01-02") and not S.trusted(row, (100, 6), "2026-01-02")
          and not S.trusted(row, (100, 5), "2026-03-04") and not S.trusted(None, (100, 5), "")
          and not S.trusted(row, None, "2026-01-02"))
    blank = S.make_row("A", (1, 2), "", "", "")
    S.save_state(tmp, {"X": row, "A": blank})
    check("save / load: the same rows, sorted by symbol, no .tmp left",
          S.load_state(tmp) == {"X": row, "A": blank} and not os.path.exists(os.path.join(tmp, S.STATE_NAME + ".tmp"))
          and open(os.path.join(tmp, S.STATE_NAME)).read().splitlines()[1].startswith("A,"), S.load_state(tmp))
    with open(os.path.join(tmp, S.STATE_NAME), "w", encoding="utf-8") as fh:
        fh.write("symbol,size,last_date\nX,1,2\n")
    check("a state file with another header is ignored (every file falls back to a read)", S.load_state(tmp) == {})
    check("no state file, no ledger -> empty", S.load_state(tempfile.mkdtemp()) == {} and S.ledger_last_splits(tmp) == {})
    with open(os.path.join(tmp, S.LEDGER_NAME), "w", encoding="utf-8", newline="") as fh:
        fh.write("symbol,date,ratio,price_applied,volume_applied,source,evidence,applied_on\n"
                 "X,2020-01-02,2,1,1,s,e,a\nX,2024-06-10,0.1,0,1,s,e,a\nX,2025-01-01,3,0,0,s,e,a\nY,2023-03-03,2,1.0,0,s,e,a\n")
    check("ledger: the newest APPLIED split per symbol (a price leg or a volume leg; an unapplied row is not one)",
          S.ledger_last_splits(tmp) == {"X": "2024-06-10", "Y": "2023-03-03"}, S.ledger_last_splits(tmp))
    lines = [f"ZZ,11/{d:02d}/2026,00:00,1.0000,1.0000,1.0000,1.0000,1" for d in (9, 10)]
    write_store_bars(tmp, "ZZ", "1 day", lines[:1])
    write_store_bars(tmp, "ZZ", "1 day", lines[1:])
    path = os.path.join(tmp, "ZZ_daily.csv")
    check("folder listing (os.scandir) = the file itself (os.stat) right after the library's writer closes it",
          S.folder_stats(tmp, "_daily.csv")["ZZ"] == S.file_stats(path), (S.folder_stats(tmp, "_daily.csv"), S.file_stats(path)))
    r = S.row_after_write("ZZ", path, lines[1:], "", {"11/09/2026": 1.0}, keep=1)
    check("row_after_write: the last line's date and time, the last `keep` closes, the file's size and time now",
          (r["last_date"], r["last_time"], r["closes"]) == ("11/10/2026", "00:00", "11/10/2026:1.0")
          and S.trusted(r, S.file_stats(path), ""), r)


def main() -> int:
    test_module()
    test_daily()
    test_1min()
    print(f"{sum(RESULTS)}/{len(RESULTS)} passed", flush=True)
    print("TESTS-PASSED" if all(RESULTS) else "TESTS-FAILED", flush=True)
    return 0 if all(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())
