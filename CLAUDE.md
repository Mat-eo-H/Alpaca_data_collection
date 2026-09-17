# CLAUDE.md — Alpaca_data_collection

Master behavioral guidelines live in the global `~/.claude/CLAUDE.md` and apply first. This file adds project-specific context.

## Purpose
Download free historical 1-minute US equity bars from Alpaca and save one CSV per symbol for AmiBroker backtesting. Education/personal use, no warranty. Free Alpaca account → only "yesterday" and older data.

## Stack
Python 3, `alpaca-py`, `pandas`, `numpy` (see `requirements.txt`).

## Layout
- `main.py` — orchestrator (connect, fetch symbols, download bars).
- `app/alpaca_client.py` — Alpaca API access (connect, bars, symbol universe, gap repair).
- `app/data_handler.py`, `app/state_persistence.py`, `app/config.py`, `app/utils.py` — support modules.
- `config_local.py` — your API keys (gitignored); created by copying `config_local_template.py`.

## Run
```powershell
python main.py
```

## Conventions & gotchas
- **Never commit secrets.** Put API keys only in `config_local.py`; never edit `config_local_template.py` with real keys. If keys leak, regenerate them at Alpaca.
- Set `BASE_DATA_DIR` in `config_local.py` to a **non-synced** folder (not OneDrive) — the dataset grows to 100+ GB.
- Filters to tradable/shortable/active US equities (~11–12k symbols). Saves one `<SYMBOL>.csv` per symbol for AmiBroker import (col 1 = Date, col 2 = Time).
