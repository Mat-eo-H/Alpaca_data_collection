# alpaca_daily_refresh.py — HANDOFF

Appends Alpaca's split-adjusted SIP daily bars to `C:\StockData\IQbars_Daily\<SYM>_daily.csv` (the DTN/IQFeed subscription
ended 2026-09-09). Docstring = mechanics; `Sync_AmiBroker/daily_refresh.ps1` runs it every weekday evening (Task Scheduler
"Daily data refresh", registered by `daily_refresh_task.ps1`).

## Active work — 2026-09-10

**Done**
- First real run 03:09: 222,146 bars appended to 12,799 files (2026-08-17..2026-09-09), 96 files rescaled to Alpaca's split
  basis (all clean fractions, constant over the 9-10 overlap days; audit trail in `IQbars_Daily\_refresh_dryrun_20260910b.log`),
  177 DTN names Alpaca does not carry (`_refresh_unmatched.txt`), 194 files older than 45 days skipped as delisted.
- Verified: DTN and Alpaca closes agree to the cent on the overlap days; class shares, `.WS` warrants and `.U` units share
  the DTN name; preferreds map `ABR-D` → `ABR.PRD`; one unknown symbol fails a whole batch, hence the asset-list check.
- Free-plan rule: the request end must be ≥ 16 minutes in the past (`request_end()`), else HTTP 403 "recent SIP data".

**Open / decisions to explore**
- DTN breadth (`.Z`: RINT.Z / RIQT.Z TRIN, TICK) has no free replacement yet → MY_TRIN_* regime columns freeze at
  2026-08-14. Candidates: TradingView's USI:TRIN via the screener endpoint (unprobed), or compute TRIN from the store
  (advancers/decliners and their volume from the ~6,000 US stocks — needs a definition the user accepts).
- New listings after 2026-08-14 are not in the store (the universe came from DTN's symbol list); Alpaca's asset list could
  seed them.
- Aux1 (IBKR implied-volatility close) stays 0 on new bars until the IV pull (`ibkr-iv-daily-pull`) runs again.
- 1-minute refresh (Alpaca SIP 1-min bars work) — user: "later".
- Volume differs slightly from DTN's (consolidated tape vs DTN's count); prices identical.

## Related
- `Sync_AmiBroker/daily_refresh.ps1`, `daily_refresh_task.ps1`, `ab_db_rebuild.py stage-daily --since / import-daily --incremental`,
  `Trade_Column_Analysis/tv_fundamentals_snapshot.py`, `industry_sector_data.py rs`, `symbol_list_update/cboe_vix_download.py`.
- Memory: daily-data-refresh-chain, dtn-daily-data-collection.
