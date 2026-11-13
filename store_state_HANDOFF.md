# store_state.py - handoff

The refresh state file of the two Alpaca refreshers (`alpaca_daily_refresh.py`, `alpaca_1min_refresh.py`). Newest first.

## 2026-11-12 - built, seeded and verified (uncommitted)

**Why:** the efficiency review's finding 5 - each nightly refresh opened every store file and read its end to learn its
last date (measured that night: 331 s for the daily store, 467 s for the 1-minute store). The user: "keep a state file
per store as you described it but add the last split date processed".

**What:** `<store>\_refresh_state.csv`, one row per symbol: `size, mtime_ns, last_date, last_time, last_split, closes`.
A row is trusted only while the folder listing shows the same size and modified time and the split ledger
(`_adjustments_applied.csv`) the same newest applied split date; otherwise the file's end is read as before and the row
rewritten. The daily row keeps the last `KEEP_CLOSES` = 11 closes, so the daily split check opens no file. The
1-minute split check still reads the last day's minutes of each file it appends to; that read also checks the row.
Rows are updated from the lines just written (`row_after_write`), saved whole at the end of a run, never in a dry run.

**Measured after seeding:** both scans 0 s with 0 files read; the same work lists and split detections as before.
**Checks:** `test_store_state.py` 26/26; `C:\StockData\bar_store_check\state_file\verify_state.py` (every row against
its file, the folder listing against os.stat on every file, 1,000 fresh reads per store).

**To watch:** the first scheduled runs on it, 2026-11-13 (17:45 chain and 21:30): their "scan" log lines should say
0 or a handful of files read (a file the split tool rescaled, a new file).

## Future decisions to explore
- The 1-minute split check's read (about 12,000 files a night) could go if the row kept a sample of the last day's
  closes; that changes what the split check compares, so it is the user's call.
- The module is local to this repo; if a second repo wants a state file it would be a library request.

## Related docs
- `Trade_Column_Analysis/CHATS/DATA_PIPELINES_MEMORY.md` ("2026-11-12: THE RAW 1-MINUTE STORE IS DELETED; the refresh
  STATE FILE"), `alpaca_daily_refresh_HANDOFF.md`.
