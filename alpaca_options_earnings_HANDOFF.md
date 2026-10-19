# alpaca_options_earnings.py - HANDOFF

Historical 1-minute option bars around earnings announcements from Alpaca into `C:\StockData\optionsHistory`
(the data set's README.md there has the rules, the columns and the Alpaca facts). Launcher
`alpaca_options_earnings.ps1` (own visible window, Tee log in `optionsHistory\_logs`). Built 2026-10-14 on the
user's request of ~02:00 ET: "collect data on options from 2024 to present across earnings announcements ... the
last hour of the day before the announcement, the 1st RTH hour (9:30-10:29), then round hours ... 3:45 instead of 4".
Uncommitted.

## IN FLIGHT (2026-10-14 ~03:00 ET)
- The full run (`run`, newest first, expiries to 60 days) started 2026-10-14 in its own window; log
  `optionsHistory\_logs\alpaca_options_earnings_run_<stamp>.log`, ends with `OPTIONS-HISTORY-DONE`.
  Watch: `Sync_AmiBroker\run_monitor.py --log <log> --regex "PACE:|Traceback|429|paused|rate limit|stopped|error" --done-regex OPTIONS-HISTORY-DONE`.
- A restart (same launcher, no arguments) skips done events. `run --retry-errors` re-pulls the errors.
- `events.csv` is rewritten every 20 events and at the end (a crash re-pulls at most 20 events).

## 2026-10-14 - built, verified, started

### Universe (`_universe\events.csv`, 33,200 events)
| source | events | entry sessions |
|---|---|---|
| B1 book (unfiltered 1-minute, one row per reporter per event, 33,025 rows from 2024-02-01, 9 duplicate symbol+day rows) | 33,016 | 2024-02-01 .. 2026-08-11 |
| the trading bot's journal (report headlines, read-only) | 172 | 2026-09-09 .. 2026-10-14 |
| the IBKR collector's universe files (`optionsSnapshots\_universe`), those not already in the journal | 12 | 2026-10-06 .. 2026-10-14 |

2024: 11,136 events; 2025: 12,578; 2026: 9,486. No source covers 2026-08-12 to 2026-09-08 (the book ends 08-11, the
journal starts 09-09). The Benzinga headline store (`earnings_headlines_alpaca.py`) could fill it - not done.
Timing: the book's ERN_TimingCode (0 premarket, 1 afterclose, 2 insession); journal events by the lab's rule
(before 09:30 on a session = that day; 09:30-16:00 = that day; later = the next session). The pre-session = the
previous session in SPY_daily.csv (extended with plain weekdays past its last date, holidays unknown there).

### Smoke test vs the IBKR collector (PEP 10-08, STZ 10-07, JPM 10-13) - in the README's parity table
Raw bars vs IBKR TRADES bars: close equal 94.8 / 78.7 / 90.6%, volume equal 86.0 / 90.9 / 74.5%. With Alpaca's
trades rebuilt per minute without multi-leg and stock-option prints: close 100 / 81.2 / 99.9%, volume 100 / 100 /
99.4%, no minute missing on either side (JPM: 8 IBKR-only). Alpaca's raw bar volume carries the multi-leg legs
(1.36x IBKR on PEP over 5 sessions, probe 2026-10-13). STZ's last price differs in 19% of minutes with the volume
identical - not explained.

### History depth
AAPL_2024-08-02 (436 contracts, 30,189 bars), NVDA_2024-02-22 (1,664 contracts, 101,802 bars, the earliest month
Alpaca has) and NVDA_2025-08-28 (546 contracts, 56,017 bars) pulled whole; the pre-session 15:00 bars are the first
rows.

### Pace (measured on the 6 test events)
Small names 2.3-2.7 s and 6-8 requests an event; NVDA 2024 (1,664 contracts) 12.7 s and 24 requests. The sliding
window (190 requests in 60 s) is the binding limit: at ~7 requests an event that is ~27 events a minute, ~1,600 an
hour, ~21 hours for 33,200 events if every event were small. The PACE lines in the log (every 25 events) carry the
measured events/hour and the projected finish.

### Design notes
- Keys: `config_local.py` (API_KEY / API_SECRET / BASE_URL), the same the daily refresh uses; the trading base
  (contracts list) is derived from BASE_URL. Never printed. The config_local keys reach the options endpoints
  (probed 2026-10-14).
- Request plumbing is the module's own `get_json` (sliding window, X-RateLimit-* per host, 429 / 5xx retries, the
  09:20-09:55 pause): `alpaca_daily_refresh.get_json` returns no headers and exits on a 400, so it could not be reused.
  `app/alpaca_client.py` is alpaca-py's stock client and has no options calls.
- One stock request per event (pre-session 15:00 .. entry 16:00, kept in the two windows); the contracts list is
  asked once per status with the widest strike window and filtered per expiry locally; when the whole expiry window
  is in the past only `status=inactive` is asked (every contract is expired).
- Option bars: one window per request (pre-session 15:00 .. entry 15:46): options do not trade in between, so
  nothing extra comes back.
- An event whose entry session is not over for 16 minutes stays pending (the free plan's 15-minute rule).
- `verify-ibkr --events ... [--trades-check]` compares with `optionsSnapshots` (read-only).

## Future decisions / open items
- `run --days 90`: today it would re-pull a done event entirely only if its status were reset; an extension pass
  that pulls just the expiries 61-90 days is NOT built (the user said "a later --days 90 pass can add the rest").
  Simplest route: reset the status of the events wanted (status pending, expiry_days blank) and run with --days 90.
- The 2026-08-12 .. 2026-09-08 gap (above).
- Trades (the multi-leg-free volume) are not pulled; `--trades-check` shows the rebuild.
- Daily reruns: the journal adds new events; `build-universe` keeps every existing row's status and appends the new
  ones; a scheduled task is not registered (the user decides).
- Memory: the user keeps `C:\StockData\optionsSnapshots` for the IBKR collector; this store must stay separate.

## Related docs
- `C:\StockData\optionsHistory\README.md` - the data set's rules, columns, Alpaca facts, the parity table
- `Earnings_Execution_Bot\earnings_bot\options_snapshots_HANDOFF.md` - the IBKR collector (the universe files, the journal)
- `alpaca_daily_refresh_HANDOFF.md` - the daily refresh that shares the keys and the free-plan 16-minute rule
- scratchpad probes 2026-10-13 (`alpaca_options\scripts\probe_alpaca_options.py`, `trades_no_multileg.py`,
  `probe_keys_contracts.py`) - the entitlement, depth and multi-leg measurements
