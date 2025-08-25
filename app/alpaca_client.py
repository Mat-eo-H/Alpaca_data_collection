# All Alpaca API connection functions

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.trading.client import TradingClient
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.common.exceptions import APIError
from alpaca.common.enums import Sort
try:
    # Newer alpaca-py
    from alpaca.data.enums import DataFeed as StockDataFeed
except Exception:
    try:
        # Older alpaca-py
        from alpaca.data.enums import StockDataFeed  # type: ignore
    except Exception:
        StockDataFeed = None  # feed selection not available in this version
from requests.exceptions import RequestException
from datetime import datetime, timedelta
import time
import os
import sys
import pandas as pd
import pytz
import colorama
from typing import Optional
from config_local import API_KEY, API_SECRET, BASE_URL, BASE_DATA_DIR, MAX_RETRIES, RETRY_DELAY, CHUNK_DAYS, NY_TZ, RESYNC_STATE_FROM_CSVS
from app.utils import ensure_tz_aware
from app.data_handler import save_bars_to_csv


_data_connected_once = False
_aapl_days_loaded = False
_aapl_days: set = set()
_aapl_earliest_day: Optional[pd.Timestamp] = None
_aapl_latest_day: Optional[pd.Timestamp] = None

def _parse_csv_times(df: pd.DataFrame) -> pd.Series:
    """Return tz-aware (NY) pandas Series of timestamps from a bars CSV.
    Preference order:
      1. 'timestamp' column (assumed ISO / offset aware or naive UTC) -> parse utc=True then convert to NY.
      2. Reconstruct from 'date' + 'time' columns (naive local) -> localize to NY directly.
    Falls back to empty Series on failure.
    """
    try:
        if 'timestamp' in df.columns:
            ts = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
            ts = ts.dropna()
            if ts.empty:
                return ts
            return ts.dt.tz_convert(NY_TZ)
        if 'date' in df.columns and 'time' in df.columns:
            raw = df['date'].astype(str) + ' ' + df['time'].astype(str)
            ts = pd.to_datetime(raw, errors='coerce')
            ts = ts.dropna()
            if ts.empty:
                return ts
            # Treat combined date+time as already in NY local clock
            if ts.dt.tz is None:
                ts = ts.dt.tz_localize(NY_TZ, nonexistent='NaT', ambiguous='NaT')
            else:
                ts = ts.dt.tz_convert(NY_TZ)
            return ts
    except Exception:
        return pd.Series([], dtype='datetime64[ns]')
    return pd.Series([], dtype='datetime64[ns]')

def _get_aapl_trading_days(start: datetime, end: datetime) -> set:
    """Return set of NY date objects where AAPL traded between start/end using local AAPL.csv.
    Only fetch from API when: (a) AAPL.csv missing, or (b) we require dates earlier than current earliest coverage.
    We do NOT fetch forward/newer because main process walks backward.
    """
    global _aapl_days_loaded, _aapl_days, _aapl_earliest_day, _aapl_latest_day
    try:
        data_dir = os.path.join(BASE_DATA_DIR, "1mintrades")
        os.makedirs(data_dir, exist_ok=True)
        aapl_path = os.path.join(data_dir, 'AAPL.csv')
        # Initial load from CSV if exists and not loaded
        if not _aapl_days_loaded and os.path.exists(aapl_path):
            try:
                df_existing = pd.read_csv(aapl_path)
                if 'date' in df_existing.columns:
                    date_series = pd.to_datetime(df_existing['date'].astype(str), errors='coerce')
                    date_series = date_series.dropna()
                    if not date_series.empty:
                        _aapl_days = set(date_series.dt.date)
                        _aapl_earliest_day = pd.Timestamp(min(_aapl_days), tz=NY_TZ)
                        _aapl_latest_day = pd.Timestamp(max(_aapl_days), tz=NY_TZ)
                else:
                    # fallback: attempt to parse any timestamp-like column
                    ts_col = None
                    for c in ['timestamp','datetime','t','time']:
                        if c in df_existing.columns:
                            ts_col = c
                            break
                    if ts_col:
                        ts = pd.to_datetime(df_existing[ts_col], errors='coerce', utc=True).dropna()
                        if not ts.empty:
                            ts = ts.tz_convert(NY_TZ)
                            _aapl_days = set(ts.date)
                            _aapl_earliest_day = pd.Timestamp(min(_aapl_days), tz=NY_TZ)
                            _aapl_latest_day = pd.Timestamp(max(_aapl_days), tz=NY_TZ)
                _aapl_days_loaded = True
            except Exception as ee:
                print(colorama.Fore.RED + f"[AAPL] Error loading existing AAPL.csv: {ee}" + colorama.Style.RESET_ALL)
                _aapl_days_loaded = True  # prevent repeated attempts

        # If no data loaded, fetch current requested window once
        if not _aapl_days:
            feed_enum = StockDataFeed.SIP if StockDataFeed is not None else None
            df_new, _ = fetch_1min_bars('AAPL', start=start, end=end, feed=feed_enum)
            if df_new is not None and not df_new.empty:
                # Save for reuse
                save_bars_to_csv(df_new, 'AAPL', data_dir)
                # Derive days
                if isinstance(df_new.index, pd.MultiIndex):
                    ts_idx = df_new.index.get_level_values('timestamp') if 'timestamp' in df_new.index.names else df_new.index.get_level_values(-1)
                else:
                    ts_idx = df_new.index
                ts_idx = pd.DatetimeIndex(ts_idx).tz_convert(NY_TZ)
                _aapl_days = set(ts_idx.date)
                _aapl_earliest_day = pd.Timestamp(min(_aapl_days), tz=NY_TZ)
                _aapl_latest_day = pd.Timestamp(max(_aapl_days), tz=NY_TZ)
                print(colorama.Fore.CYAN + f"[AAPL] Initialized trading day mask covering {_aapl_earliest_day.date()} -> {_aapl_latest_day.date()} ({len(_aapl_days)} days)." + colorama.Style.RESET_ALL)
            return {d for d in _aapl_days if start.date() <= d <= end.date()}

        # If we need earlier days than we have, backfill ONLY the missing earlier segment
        if _aapl_earliest_day is None or _aapl_latest_day is None:
            return {d for d in _aapl_days if start.date() <= d <= end.date()}

        if start.date() < _aapl_earliest_day.date():
            # Fetch from new start to one day before current earliest
            backfill_end = _aapl_earliest_day - pd.Timedelta(seconds=1)
            feed_enum = StockDataFeed.SIP if StockDataFeed is not None else None
            try:
                df_old, _ = fetch_1min_bars('AAPL', start=start, end=backfill_end, feed=feed_enum)
                if df_old is not None and not df_old.empty:
                    save_bars_to_csv(df_old, 'AAPL', data_dir)
                    if isinstance(df_old.index, pd.MultiIndex):
                        old_ts_idx = df_old.index.get_level_values('timestamp') if 'timestamp' in df_old.index.names else df_old.index.get_level_values(-1)
                    else:
                        old_ts_idx = df_old.index
                    old_ts_idx = pd.DatetimeIndex(old_ts_idx).tz_convert(NY_TZ)
                    new_days = set(old_ts_idx.date)
                    _aapl_days.update(new_days)
                    _aapl_earliest_day = pd.Timestamp(min(_aapl_days), tz=NY_TZ)
                    print(colorama.Fore.CYAN + f"[AAPL] Backfilled earlier AAPL days now covering {_aapl_earliest_day.date()} -> {_aapl_latest_day.date()} ({len(_aapl_days)} days)." + colorama.Style.RESET_ALL)
            except Exception as be:
                print(colorama.Fore.RED + f"[AAPL] Backfill error: {be}" + colorama.Style.RESET_ALL)

        # Return days subset for window
        return {d for d in _aapl_days if start.date() <= d <= end.date()}
    except Exception as e:
        print(colorama.Fore.RED + f"[AAPL] Unexpected trading day mask error: {e}" + colorama.Style.RESET_ALL)
        return set()

def _fill_edge_days(symbol: str, bars_df: pd.DataFrame, start: datetime, end: datetime, trading_days: set, feed, max_edge_days: int = 10) -> pd.DataFrame:
    """Attempt to fetch missing edge trading days (earliest or latest) inside the 30-day chunk window.
    We only look at full missing days vs AAPL mask. If illiquid (no bars returned on single-day fetch) we accept and move on.
    Limits to max_edge_days each side to bound API usage.
    """
    if bars_df is None or bars_df.empty or not trading_days:
        return bars_df
    # Normalize index to timestamps
    if isinstance(bars_df.index, pd.MultiIndex):
        sym_ts = bars_df.index.get_level_values('timestamp') if 'timestamp' in bars_df.index.names else bars_df.index.get_level_values(-1)
    else:
        sym_ts = bars_df.index
    sym_ts = pd.DatetimeIndex(sym_ts).tz_convert(NY_TZ)
    symbol_days = set(sym_ts.date)
    if not symbol_days:
        return bars_df
    first_trade_day = min(trading_days)
    last_trade_day = max(trading_days)
    sym_first = min(symbol_days)
    sym_last = max(symbol_days)

    feed_enum = feed
    # Fill missing earlier edge days
    if sym_first > first_trade_day:
        earlier_days = sorted([d for d in trading_days if first_trade_day <= d < sym_first])[:max_edge_days]
        for d in earlier_days:
            day_start = pd.Timestamp(d, tz=NY_TZ)
            day_end = day_start + pd.Timedelta(days=1)
            add_df, _ = fetch_1min_bars(symbol, start=day_start, end=day_end, feed=feed_enum)
            if add_df is not None and not add_df.empty:
                if isinstance(add_df.index, pd.MultiIndex):
                    add_ts = add_df.index.get_level_values('timestamp') if 'timestamp' in add_df.index.names else add_df.index.get_level_values(-1)
                else:
                    add_ts = add_df.index
                bars_df = pd.concat([bars_df, add_df]).sort_index()
                bars_df = bars_df[~bars_df.index.duplicated(keep='first')]
                print(colorama.Fore.GREEN + f"[EDGE] Filled earlier day {d} for {symbol}" + colorama.Style.RESET_ALL)
            else:
                print(colorama.Fore.YELLOW + f"[EDGE] No trades for {symbol} on {d}; accepting as illiquid day." + colorama.Style.RESET_ALL)

    # Fill missing later edge days
    if sym_last < last_trade_day:
        later_days_all = sorted([d for d in trading_days if sym_last < d <= last_trade_day])
        later_days = later_days_all[:max_edge_days]
        for d in later_days:
            day_start = pd.Timestamp(d, tz=NY_TZ)
            day_end = day_start + pd.Timedelta(days=1)
            add_df, _ = fetch_1min_bars(symbol, start=day_start, end=day_end, feed=feed_enum)
            if add_df is not None and not add_df.empty:
                if isinstance(add_df.index, pd.MultiIndex):
                    add_ts = add_df.index.get_level_values('timestamp') if 'timestamp' in add_df.index.names else add_df.index.get_level_values(-1)
                else:
                    add_ts = add_df.index
                bars_df = pd.concat([bars_df, add_df]).sort_index()
                bars_df = bars_df[~bars_df.index.duplicated(keep='first')]
                print(colorama.Fore.GREEN + f"[EDGE] Filled later day {d} for {symbol}" + colorama.Style.RESET_ALL)
            else:
                print(colorama.Fore.YELLOW + f"[EDGE] No trades for {symbol} on {d}; accepting as illiquid day." + colorama.Style.RESET_ALL)
    return bars_df

def ensure_aapl_forward_fill(max_back_days: int = 180):
    """Ensure AAPL.csv (mask source) is forward-filled through 'yesterday' (NY).
    If file missing: fetch last `max_back_days` days (yesterday inclusive) as seed.
    If existing but stale (latest < yesterday): fetch missing forward span only.
    Updates global mask variables so subsequent _get_aapl_trading_days calls see new days.
    """
    global _aapl_days_loaded, _aapl_days, _aapl_earliest_day, _aapl_latest_day
    try:
        data_dir = os.path.join(BASE_DATA_DIR, "1mintrades")
        os.makedirs(data_dir, exist_ok=True)
        aapl_path = os.path.join(data_dir, 'AAPL.csv')
        yesterday_ny = (pd.Timestamp.now(tz=NY_TZ) - pd.Timedelta(days=1)).normalize()
        # Load existing if not loaded
        if (not _aapl_days_loaded) and os.path.exists(aapl_path):
            try:
                df_exist = pd.read_csv(aapl_path)
                if 'date' in df_exist.columns:
                    date_series = pd.to_datetime(df_exist['date'].astype(str), errors='coerce').dropna()
                    if not date_series.empty:
                        _aapl_days = set(date_series.dt.date)
                        _aapl_earliest_day = pd.Timestamp(min(_aapl_days), tz=NY_TZ)
                        _aapl_latest_day = pd.Timestamp(max(_aapl_days), tz=NY_TZ)
                else:
                    ts_col = None
                    for c in ['timestamp','datetime','t','time']:
                        if c in df_exist.columns:
                            ts_col = c
                            break
                    if ts_col:
                        ts = pd.to_datetime(df_exist[ts_col], errors='coerce', utc=True).dropna()
                        if not ts.empty:
                            ts = ts.tz_convert(NY_TZ)
                            _aapl_days = set(ts.date)
                            _aapl_earliest_day = pd.Timestamp(min(_aapl_days), tz=NY_TZ)
                            _aapl_latest_day = pd.Timestamp(max(_aapl_days), tz=NY_TZ)
                _aapl_days_loaded = True
            except Exception as le:
                print(colorama.Fore.RED + f"[AAPL-FWD] Error loading existing AAPL.csv: {le}" + colorama.Style.RESET_ALL)
                _aapl_days_loaded = True
        # If missing entirely or empty -> seed fetch
        if not _aapl_days:
            seed_start = yesterday_ny - pd.Timedelta(days=max_back_days)
            seed_end = yesterday_ny + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            feed_enum = StockDataFeed.SIP if StockDataFeed is not None else None
            df_seed, _ = fetch_1min_bars('AAPL', start=seed_start, end=seed_end, feed=feed_enum)
            if df_seed is not None and not df_seed.empty:
                save_bars_to_csv(df_seed, 'AAPL', data_dir)
                if isinstance(df_seed.index, pd.MultiIndex):
                    ts_idx = df_seed.index.get_level_values('timestamp') if 'timestamp' in df_seed.index.names else df_seed.index.get_level_values(-1)
                else:
                    ts_idx = df_seed.index
                ts_idx = pd.DatetimeIndex(ts_idx).tz_convert(NY_TZ)
                _aapl_days = set(ts_idx.date)
                _aapl_earliest_day = pd.Timestamp(min(_aapl_days), tz=NY_TZ)
                _aapl_latest_day = pd.Timestamp(max(_aapl_days), tz=NY_TZ)
                print(colorama.Fore.CYAN + f"[AAPL-FWD] Seeded AAPL mask { _aapl_earliest_day.date()} -> {_aapl_latest_day.date()} ({len(_aapl_days)} days)." + colorama.Style.RESET_ALL)
            return
        # Forward fill if stale
        if _aapl_latest_day is not None and _aapl_latest_day.date() < yesterday_ny.date():
            fwd_start = _aapl_latest_day + pd.Timedelta(days=1)
            fwd_end = yesterday_ny + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            feed_enum = StockDataFeed.SIP if StockDataFeed is not None else None
            df_new, _ = fetch_1min_bars('AAPL', start=fwd_start, end=fwd_end, feed=feed_enum)
            if df_new is not None and not df_new.empty:
                # append
                save_bars_to_csv(df_new, 'AAPL', data_dir)
                if isinstance(df_new.index, pd.MultiIndex):
                    ts_idx2 = df_new.index.get_level_values('timestamp') if 'timestamp' in df_new.index.names else df_new.index.get_level_values(-1)
                else:
                    ts_idx2 = df_new.index
                ts_idx2 = pd.DatetimeIndex(ts_idx2).tz_convert(NY_TZ)
                new_days = set(ts_idx2.date)
                _aapl_days.update(new_days)
                _aapl_latest_day = pd.Timestamp(max(_aapl_days), tz=NY_TZ)
                _aapl_earliest_day = pd.Timestamp(min(_aapl_days), tz=NY_TZ)
                print(colorama.Fore.CYAN + f"[AAPL-FWD] Forward-filled AAPL mask now {_aapl_earliest_day.date()} -> {_aapl_latest_day.date()} ({len(_aapl_days)} days)." + colorama.Style.RESET_ALL)
        else:
            # Already up-to-date
            if _aapl_latest_day is not None:
                print(colorama.Fore.CYAN + f"[AAPL-FWD] AAPL mask already current through {_aapl_latest_day.date()}." + colorama.Style.RESET_ALL)
    except Exception as e:
        print(colorama.Fore.RED + f"[AAPL-FWD] Unexpected forward-fill error: {e}" + colorama.Style.RESET_ALL)

def sync_state_from_csvs(state: pd.DataFrame, data_dir: str, symbols: list[str]) -> pd.DataFrame:
    """Reconcile state file with actual CSV contents: CSVs are source of truth.
    For each symbol CSV present:
      - Load date/time (and timestamp if present)
      - Derive earliest and latest trading day
      - Update state.oldest_date/newest_date if CSV shows broader coverage
      - If last_end is NaT but we have data, set last_end to earliest timestamp (so backward fetch starts earlier only if needed)
    Avoid heavy parsing by reading only needed columns.
    """
    updated_oldest = 0
    updated_newest = 0
    initialized_last_end = 0
    for sym in symbols:
        csv_path = os.path.join(data_dir, f"{sym}.csv")
        if not os.path.exists(csv_path):
            continue
        try:
            # Read minimal columns. We don't know order, so read all then subset.
            df = pd.read_csv(csv_path)
            if df.empty:
                continue
            # Clean: drop duplicate date+time (or timestamp) and sort
            cleaned = False
            if 'date' in df.columns and 'time' in df.columns:
                before = len(df)
                df = df.drop_duplicates(subset=['date','time']).sort_values(['date','time'])
                if len(df) != before:
                    cleaned = True
            elif 'timestamp' in df.columns:
                before = len(df)
                df = df.drop_duplicates(subset=['timestamp']).sort_values(['timestamp'])
                if len(df) != before:
                    cleaned = True
            if cleaned:
                # Rewrite cleaned CSV to establish canonical ordering
                try:
                    df.to_csv(csv_path, index=False)
                    print(colorama.Fore.LIGHTBLUE_EX + f"[SYNC] Cleaned duplicates in {sym} (rewrote CSV)." + colorama.Style.RESET_ALL)
                except Exception as e_w:
                    print(colorama.Fore.YELLOW + f"[SYNC] Failed rewrite for {sym}: {e_w}" + colorama.Style.RESET_ALL)
            ts = _parse_csv_times(df)
            if ts.empty:
                continue
            earliest_ts = ts.min()
            latest_ts = ts.max()
            earliest_day = earliest_ts.normalize()
            latest_day = latest_ts.normalize()
            # Update oldest_date
            if pd.isna(state.loc[sym, 'oldest_date']) or earliest_day < state.loc[sym, 'oldest_date']:
                state.loc[sym, 'oldest_date'] = earliest_day
                updated_oldest += 1
            # Update newest_date
            if 'newest_date' in state.columns:
                if pd.isna(state.loc[sym, 'newest_date']) or latest_day > state.loc[sym, 'newest_date']:
                    state.loc[sym, 'newest_date'] = latest_day
                    updated_newest += 1
            # Initialize last_end if NaT so we don't refetch already covered range
            if pd.isna(state.loc[sym, 'last_end']):
                # Set last_end to earliest_ts so algorithm will detect completion if oldest_date equals this day
                state.loc[sym, 'last_end'] = earliest_ts
                initialized_last_end += 1
        except Exception as e_sym_sync:
            print(colorama.Fore.YELLOW + f"[SYNC] Skipped {sym} due to error: {e_sym_sync}" + colorama.Style.RESET_ALL)
    print(colorama.Fore.CYAN + f"[SYNC] State reconciled with CSVs: oldest_date updates={updated_oldest}, newest_date updates={updated_newest}, initialized last_end={initialized_last_end}." + colorama.Style.RESET_ALL)
    return state

def connect_trading():
    """Connect to Alpaca trading API."""
    client = TradingClient(API_KEY, API_SECRET, paper=True)
    print("✅ Connected to Alpaca Trading API")
    return client

def connect_data():
    """Connect to Alpaca market data API."""
    global _data_connected_once  # tell Python we're using the global flag

    client = StockHistoricalDataClient(API_KEY, API_SECRET)
    if not _data_connected_once:
        print("✅ Connected to Alpaca Market Data API")
        _data_connected_once = True
    return client

def get_recent_bars(symbol: str, days: int = 90):
    """Fetch recent daily bars for a symbol."""
    data_client = connect_data()
    end_time = datetime.now(pytz.UTC) - timedelta(days=1)  # Exclude today
    start_time = end_time - timedelta(days=days)
    
    request = StockBarsRequest(
        symbol_or_symbols=[symbol],
        start=start_time,
        end=end_time,
        timeframe=TimeFrame.Minute  # Use TimeFrame.Minute instead of "1Day"
    )
    
    bars = data_client.get_stock_bars(request)
    return bars

def get_tradeable_symbols_df(
    trading_client: TradingClient,
    asset_class: Optional[AssetClass] = AssetClass.US_EQUITY,
    status: Optional[AssetStatus] = AssetStatus.ACTIVE,
    tradable: bool = True,
    shortable: Optional[bool] = None,
    fractionable: Optional[bool] = None
) -> pd.DataFrame:
    """
    Get all tradeable symbols from Alpaca and return as a pandas DataFrame.
    
    Parameters:
    -----------
    trading_client : TradingClient
        The Alpaca trading client instance
    asset_class : AssetClass, optional
        The asset class to filter by (default: US_EQUITY)
    status : AssetStatus, optional
        The status of assets to include (default: ACTIVE)
    tradable : bool, optional
        Whether to include only tradable assets (default: True)
    shortable : bool, optional
        Filter by shortable assets (default: None - no filter)
    fractionable : bool, optional
        Filter by fractionable assets (default: None - no filter)
    
    Returns:
    --------
    pd.DataFrame
        DataFrame containing symbol information with columns:
        - symbol: Stock symbol
        - name: Company name
        - exchange: Exchange where it's listed
        - asset_class: Type of asset
        - status: Current status
        - tradable: Whether it's tradable
        - marginable: Whether it's marginable
        - shortable: Whether it's shortable
        - easy_to_borrow: Whether it's easy to borrow
        - fractionable: Whether fractional shares are supported
    """
    
    try:
        # Create the request
        search_params = GetAssetsRequest(
            asset_class=asset_class,
            status=status
        )
        
        # Get all assets
        assets = trading_client.get_all_assets(search_params)
        
        # Convert to list of dictionaries
        asset_data = []
        for asset in assets:
            # Apply additional filters if specified
            if tradable and not asset.tradable:
                continue
            if shortable is not None and asset.shortable != shortable:
                continue
            if fractionable is not None and asset.fractionable != fractionable:
                continue
                
            asset_dict = {
                'symbol': asset.symbol,
                'name': asset.name,
                'exchange': asset.exchange.value if asset.exchange else None,
                'asset_class': asset.asset_class.value,
                'status': asset.status.value,
                'tradable': asset.tradable,
                'marginable': asset.marginable,
                'shortable': asset.shortable,
                'easy_to_borrow': asset.easy_to_borrow,
                'fractionable': asset.fractionable
            }
            asset_data.append(asset_dict)
        
        # Create DataFrame
        df = pd.DataFrame(asset_data)
        
        # Sort by symbol for easier viewing
        if not df.empty:
            df = df.sort_values('symbol').reset_index(drop=True)
        
        print(f"✅ Retrieved {len(df)} tradeable symbols")
        return df
        
    except Exception as e:
        print(f"❌ Error fetching symbols: {e}")
        return pd.DataFrame()
    
def sanitize_data_dir(data_dir: str):
    """
    Keeps only symbol CSVs in data_dir.
    All other files (including state CSVs, backups, etc.) are moved to a 'misc' folder.
    """
    misc_dir = os.path.join(os.path.dirname(data_dir), "misc")
    os.makedirs(misc_dir, exist_ok=True)

    for file in os.listdir(data_dir):
        file_path = os.path.join(data_dir, file)

        # Skip directories
        if os.path.isdir(file_path):
            continue

        # Check if it's a CSV and looks like a stock symbol
        if file.lower().endswith(".csv"):
            symbol = os.path.splitext(file)[0]
            if symbol.replace(".", "").isalnum():  # allow tickers like BRK.B
                continue  # keep this one

        # Everything else → move to misc
        new_path = os.path.join(misc_dir, file)
        print(f"[MOVE] {file} → {new_path}")
        os.replace(file_path, new_path)
    
def fetch_1min_bars(symbol, start: datetime, end: datetime, feed=None, limit=10000) -> pd.DataFrame:
    """
    Fetch all 1-minute bars for a symbol in the specified range using pagination.
    Returns a DataFrame with timestamp as index.
    """
    data_client = connect_data()

    all_bars = []
    total_rows = 0
    pages = 0
    saw_full_page_without_token = False
    current_start = start
    APIrequestDaysSize = 30  # max days per individual request (loops)

    while current_start < end:
        current_end = min(current_start + timedelta(days=APIrequestDaysSize), end)
        page_token = None
        while True:
            kwargs = dict(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Minute,
                start=current_start,
                end=current_end,
                adjustment="split",
            )
            # Add feed and limit if supported by installed SDK
            if feed is not None and StockDataFeed is not None:
                try:
                    request_params = StockBarsRequest(**{**kwargs, "feed": feed, "limit": limit, "page_token": page_token})
                except TypeError:
                    # Older SDKs may not accept feed/limit/page_token
                    try:
                        request_params = StockBarsRequest(**{**kwargs, "limit": limit, "page_token": page_token})
                    except TypeError:
                        request_params = StockBarsRequest(**kwargs)
            else:
                try:
                    request_params = StockBarsRequest(**{**kwargs, "limit": limit, "page_token": page_token})
                except TypeError:
                    request_params = StockBarsRequest(**kwargs)

            bars = data_client.get_stock_bars(request_params)
            df = bars.df.copy() if bars else pd.DataFrame()
            pages += 1
            if not df.empty:
                total_rows += len(df)
                all_bars.append(df)
                # Heuristic: page returned full limit but no token → may be truncated silently
                if len(df) >= limit:
                    # We'll also look for explicit next_page_token
                    pass
            # pagination token extraction across SDK variants
            next_token = getattr(bars, "next_page_token", None)
            if next_token in ("", None):
                raw = getattr(bars, "raw", None)
                if isinstance(raw, dict):
                    next_token = raw.get("next_page_token")
            # if limit hit and no explicit token, mark possible truncation
            if next_token in ("", None) and (not df.empty) and len(df) >= limit:
                saw_full_page_without_token = True
            if not next_token:
                break
            page_token = next_token
        current_start = current_end

    if all_bars:
        full_df = pd.concat(all_bars).sort_index()
        full_df = full_df[~full_df.index.duplicated(keep='first')]
        # Compute coverage meta
        if isinstance(full_df.index, pd.MultiIndex):
            ts_index = full_df.index.get_level_values('timestamp') if 'timestamp' in full_df.index.names else full_df.index.get_level_values(-1)
        else:
            ts_index = full_df.index
        try:
            earliest = pd.to_datetime(ts_index.min()).tz_convert(NY_TZ)
            latest = pd.to_datetime(ts_index.max()).tz_convert(NY_TZ)
        except Exception:
            earliest = pd.NaT
            latest = pd.NaT
        # Truncation signal: saw_full_page_without_token OR coverage gap relative to requested window
        coverage_gap = False
        try:
            if pd.notna(earliest) and pd.notna(latest):
                # allow a 2-minute slack at edges
                coverage_gap = (earliest > (start + timedelta(minutes=2))) or (latest < (end - timedelta(minutes=2)))
        except Exception:
            coverage_gap = False

        # Simplified truncation: only consider edge coverage or pagination heuristic
        truncated = bool(saw_full_page_without_token or coverage_gap)
        meta = {"truncated": truncated, "earliest": earliest, "latest": latest, "pages": pages, "count": int(total_rows)}
        return full_df, meta
    # No bars accumulated
    return pd.DataFrame(), {"truncated": False, "earliest": pd.NaT, "latest": pd.NaT, "pages": 0, "count": 0}

def _get_symbol_data_span_days(data_dir: str, symbol: str) -> float:
    """Return the span in days of existing CSV for symbol; 0 if none/unknown."""
    try:
        path = os.path.join(data_dir, f"{symbol}.csv")
        if not os.path.exists(path):
            return 0.0
        df = pd.read_csv(path)
        # Try to find a timestamp column, else use index
        ts_col = None
        for c in ['timestamp', 'time', 't', 'date', 'datetime']:
            if c in df.columns:
                ts_col = c
                break
        if ts_col is None:
            # assume first column is timestamp/index
            ts_col = df.columns[0]
        ts = pd.to_datetime(df[ts_col], errors='coerce', utc=True)
        ts = ts.dropna()
        if ts.empty:
            return 0.0
        span = (ts.max() - ts.min()).total_seconds() / 86400.0
        return float(span)
    except Exception:
        return 0.0

def fetch_oldest_bar_date(symbol: str) -> pd.Timestamp:
    """
    Query Alpaca for the *earliest available* daily bar for `symbol`.
    Returns a tz-aware (NY) pd.Timestamp at the *bar's timestamp* (not just date).
    If not found or error, returns pd.NaT.
    """
    try:
        data_client = connect_data()
        # Ask for the very earliest bar (limit=1, sorted ASC)
        req = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=TimeFrame.Day,
            start=datetime(1900, 1, 1),   # naive OK; server treats as UTC
            limit=1,
            adjustment="split",
            sort=Sort.ASC
        )
        bars = data_client.get_stock_bars(req)
        if bars and not bars.df.empty:
            df = bars.df.copy()
            # df index is usually MultiIndex (symbol, timestamp) → take timestamp level
            if isinstance(df.index, pd.MultiIndex):
                ts = df.index.get_level_values('timestamp')[0]
            else:
                ts = df.index[0]
            # Convert to NY tz
            ts = pd.Timestamp(ts).tz_convert(NY_TZ)
            return ts
    except Exception as e:
        print(f"[WARN] API error fetching oldest date for {symbol}: {e}")
    return pd.NaT

def download_all_symbols(trading_client, symbols_df: pd.DataFrame):
    """
    Downloads 1-minute bars for all symbols in sync (most recent backwards),
    """
    # Ensure directories exist
    os.makedirs(BASE_DATA_DIR, exist_ok=True)
    DATA_DIR = os.path.join(BASE_DATA_DIR, "1mintrades")
    os.makedirs(DATA_DIR, exist_ok=True)
    STATE_FILE = os.path.join(BASE_DATA_DIR, "download_state.csv")

    # Load or init state
    if os.path.exists(STATE_FILE):
        state = pd.read_csv(STATE_FILE, index_col='symbol')

        # Parse to UTC (handles mixed/naive tz safely), then convert to NY time
        if 'last_end' in state.columns:
            state['last_end'] = pd.to_datetime(state['last_end'], errors='coerce', utc=True).dt.tz_convert(NY_TZ)
        else:
            state['last_end'] = pd.NaT

        # oldest_date normalize
        if 'oldest_date' in state.columns:
            state['oldest_date'] = pd.to_datetime(state['oldest_date'], errors='coerce', utc=True).dt.tz_convert(NY_TZ)
        else:
            state['oldest_date'] = pd.Series(pd.NaT, dtype="datetime64[ns, America/New_York]")

        # complete fallback only if missing
        if 'complete' not in state.columns:
            state['complete'] = False

        # newest_date fallback (preserve existing)
        if 'newest_date' in state.columns:
            state['newest_date'] = pd.to_datetime(state['newest_date'], errors='coerce', utc=True).dt.tz_convert(NY_TZ).dt.normalize()
        else:
            state['newest_date'] = pd.NaT

        try:
            completed_count = int(state['complete'].sum()) if 'complete' in state.columns else 0
            print(f"[STATE] Loaded {len(state)} symbols from {STATE_FILE}; {completed_count} marked complete.")
        except Exception:
            print(f"[STATE] Loaded {len(state)} symbols from {STATE_FILE}.")
    else:
        # fresh state
        state = pd.DataFrame(index=symbols_df['symbol'].tolist())
        state['last_end'] = pd.NaT
        state['oldest_date'] = pd.NaT
        state['complete'] = False
        state['newest_date'] = pd.NaT

    # --- normalize and align state with symbols_df ---

    # 1) normalize symbols from input list
    symbols = (
        symbols_df['symbol']
        .astype(str)
        .str.strip()
        .str.upper()
        .dropna()
        .unique()
        .tolist()
    )

    # 2) normalize state index
    if not state.index.empty:
        state.index = (
            pd.Index(state.index)
            .astype(str)
            .str.strip()
            .str.upper()
        )

    # 3) ensure required columns exist
    for col, default in [('last_end', pd.NaT), ('oldest_date', pd.NaT), ('complete', False), ('feed', ''), ('newest_date', pd.NaT)]:
        if col not in state.columns:
            state[col] = default

    # 4) add missing symbols to state with defaults
    missing = [s for s in symbols if s not in state.index]
    if missing:
        new_rows = pd.DataFrame({
            'last_end': pd.NaT,
            'oldest_date': pd.NaT,
            'complete': False,
            'feed': '',
            'newest_date': pd.NaT
        }, index=pd.Index(missing, name='symbol'))
        state = pd.concat([state, new_rows], axis=0)

    # 5) optional: sort & de-dup the index
    state = state[~state.index.duplicated(keep='first')].sort_index()

    # 6) persist immediately only if brand new state (i.e., file did not exist before)
    if not os.path.exists(STATE_FILE):
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            state.to_csv(f)
            f.flush()
            os.fsync(f.fileno())
        state.to_csv(STATE_FILE)
        

    # Fill oldest_date via API only for new symbols, unless it's the first day of a quarter.
    oldestDateAPICallCounter = 0
    today_ny = pd.Timestamp.now(tz=NY_TZ)
    is_quarter_start = (today_ny.month in (1, 4, 7, 10)) and (today_ny.day == 1)

    # Ensure helper column exists for caching oldest-date fetch
    if 'oldest_fetched_at' not in state.columns:
        state['oldest_fetched_at'] = pd.NaT

    for symbol in symbols:  # normalized list
        # Ensure row exists
        if symbol not in state.index:
            state.loc[symbol, ['last_end', 'oldest_date', 'complete', 'feed', 'newest_date', 'oldest_fetched_at']] = [pd.NaT, pd.NaT, False, '', pd.NaT, pd.NaT]

        # Determine if symbol is "new" to the database: no CSV in DATA_DIR
        sym_csv = os.path.join(DATA_DIR, f"{symbol}.csv")
        is_new_symbol = not os.path.exists(sym_csv)

        # Fetch oldest_date only if:
        # - quarter start (global refresh), OR
        # - oldest_date not yet cached (regardless of CSV presence)
        # Avoid refetching solely because symbol is new if we've already cached oldest_date in state.
        oldest_cached = pd.notna(state.loc[symbol, 'oldest_date'])
        need_api = is_quarter_start or (not oldest_cached)

        # Optional: throttle re-fetches using oldest_fetched_at (e.g., skip if fetched today)
        if need_api and pd.notna(state.loc[symbol, 'oldest_fetched_at']):
            oldest_fetched_at = pd.to_datetime(state.loc[symbol, 'oldest_fetched_at'])
            last_fetch_age_days = (today_ny.normalize() - oldest_fetched_at.normalize()).days
            if last_fetch_age_days < 1 and not is_quarter_start and oldest_cached:
                need_api = False  # already fetched recently

        if need_api:
            try:
                api_oldest_ts = fetch_oldest_bar_date(symbol)
                if pd.notna(api_oldest_ts):
                    state.loc[symbol, 'oldest_date'] = api_oldest_ts.tz_convert(NY_TZ).normalize()
                state.loc[symbol, 'oldest_fetched_at'] = today_ny
                print(f"[INFO] Oldest date for {symbol}: {state.loc[symbol, 'oldest_date']}")
                oldestDateAPICallCounter += 1
                if oldestDateAPICallCounter % 100 == 0:
                    print(f"[INFO] Fetched oldest date for {oldestDateAPICallCounter} symbols so far, updating STATE_FILE")
                    state.to_csv(STATE_FILE)
            except Exception as e:
                print(f"[WARN] Failed to fetch oldest date for {symbol}: {e}")

                


    # Save state atomically only after modifications to oldest_date bootstrap; avoid redundant writes
    tmp = STATE_FILE + ".tmp"
    state = state.sort_index()
    state.to_csv(tmp)
    state.to_csv(STATE_FILE)

    symbols_remaining = set(state.index[state['complete'] == False])
    now = pd.Timestamp.now(tz=NY_TZ) - timedelta(days=1)  # one-day buffer

    fixed_cutoff = pd.Timestamp("2022-01-01", tz=NY_TZ).normalize()
    for symbol in symbols:
        if pd.isna(state.loc[symbol, 'oldest_date']):
            state.loc[symbol, 'oldest_date'] = fixed_cutoff
        else:
            state.loc[symbol, 'oldest_date'] = max(state.loc[symbol, 'oldest_date'], fixed_cutoff)

    # Reconcile state with existing CSVs before fetching (CSV is source of truth)
    if RESYNC_STATE_FROM_CSVS:
        
        state = sync_state_from_csvs(state, DATA_DIR, symbols)
        # Persist after sync
        tmp = STATE_FILE + ".tmp"
        state.to_csv(tmp)
        state.to_csv(STATE_FILE)

    while symbols_remaining:
        # Iterate in alphabetical order for determinism
        for symbol in sorted(list(symbols_remaining)):
            oldest_date = state.loc[symbol, 'oldest_date']
            last_end = state.loc[symbol, 'last_end']
            feed_str = ''
            try:
                feed_str = str(state.loc[symbol, 'feed']) if not pd.isna(state.loc[symbol, 'feed']) else ''
            except Exception:
                feed_str = ''
            # lock feed if set in state
            chosen_feed = None
            if StockDataFeed is not None:
                if feed_str.upper() == 'IEX':
                    chosen_feed = StockDataFeed.IEX
                elif feed_str.upper() == 'SIP':
                    chosen_feed = StockDataFeed.SIP
                else:
                    chosen_feed = None  # will default to SIP attempt first

            # Determine end_date boundary (move backward)
            end_date = now if pd.isna(last_end) else last_end

            # Determine start_date = max(oldest_date, end_date - CHUNK_DAYS)
            start_date = end_date - timedelta(days=CHUNK_DAYS)
            if pd.notna(oldest_date) and start_date < oldest_date:
                start_date = oldest_date

            # Already complete?
            if pd.notna(oldest_date) and start_date >= end_date:
                print(f"[DONE] {symbol} — all data fetched.")
                state.loc[symbol, 'complete'] = True
                # forward fill to yesterday if possible
                try:
                    yesterday_ny = (pd.Timestamp.now(tz=NY_TZ) - pd.Timedelta(days=1)).normalize()
                    newest_date = state.loc[symbol, 'newest_date'] if 'newest_date' in state.columns else pd.NaT
                    if pd.notna(newest_date) and newest_date < yesterday_ny:
                        ff_start = newest_date + pd.Timedelta(days=1)
                        ff_end = yesterday_ny + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                        feed_enum = None
                        if StockDataFeed is not None:
                            if feed_str.upper() == 'IEX':
                                feed_enum = StockDataFeed.IEX
                            elif feed_str.upper() == 'SIP':
                                feed_enum = StockDataFeed.SIP
                        print(colorama.Fore.BLUE + f"[FWD] {symbol} forward fill {ff_start.date()} -> {yesterday_ny.date()}" + colorama.Style.RESET_ALL)
                        ff_df, ff_meta = fetch_1min_bars(symbol, start=ff_start, end=ff_end, feed=feed_enum)
                        if ff_df is not None and not ff_df.empty:
                            save_bars_to_csv(ff_df, symbol, DATA_DIR)
                            if isinstance(ff_df.index, pd.MultiIndex):
                                ff_ts = ff_df.index.get_level_values('timestamp') if 'timestamp' in ff_df.index.names else ff_df.index.get_level_values(-1)
                            else:
                                ff_ts = ff_df.index
                            ff_ts = pd.DatetimeIndex(ff_ts).tz_convert(NY_TZ)
                            state.loc[symbol, 'newest_date'] = ff_ts.max().normalize()
                            print(colorama.Fore.GREEN + f"[FWD] {symbol} up-to-date through {state.loc[symbol, 'newest_date'].date()}" + colorama.Style.RESET_ALL)
                except Exception as e_ff_sym:
                    print(colorama.Fore.RED + f"[FWD] {symbol} forward fill error: {e_ff_sym}" + colorama.Style.RESET_ALL)
                symbols_remaining.remove(symbol)
                continue

            retries = 0
            while retries < MAX_RETRIES:
                try:
                    print(f"[FETCH] {symbol} from {start_date} to {end_date}")
                    # Decide primary feed for this attempt
                    primary_feed = chosen_feed if chosen_feed is not None else (StockDataFeed.SIP if StockDataFeed is not None else None)
                    bars_df, meta = fetch_1min_bars(symbol, start=start_date, end=end_date, feed=primary_feed)

                    if bars_df.empty:
                        # Summarize fetch result even when empty
                        print(f"[FETCH] {symbol} received 0 bars; oldest=N/A; days=0.00")
                        # Do not advance dates on empty; only mark complete if we're at or past the oldest boundary
                        if pd.notna(oldest_date) and (start_date <= oldest_date):
                            print(colorama.Fore.LIGHTCYAN_EX, f"[INFO] No more bars for {symbol}; marking complete.", colorama.Style.RESET_ALL)
                            state.loc[symbol, 'complete'] = True
                            # forward fill attempt
                            try:
                                yesterday_ny = (pd.Timestamp.now(tz=NY_TZ) - pd.Timedelta(days=1)).normalize()
                                newest_date = state.loc[symbol, 'newest_date'] if 'newest_date' in state.columns else pd.NaT
                                if pd.notna(newest_date) and newest_date < yesterday_ny:
                                    ff_start = newest_date + pd.Timedelta(days=1)
                                    ff_end = yesterday_ny + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                                    feed_enum = None
                                    if StockDataFeed is not None:
                                        if feed_str.upper() == 'IEX':
                                            feed_enum = StockDataFeed.IEX
                                        elif feed_str.upper() == 'SIP':
                                            feed_enum = StockDataFeed.SIP
                                    print(colorama.Fore.BLUE + f"[FWD] {symbol} forward fill {ff_start.date()} -> {yesterday_ny.date()}" + colorama.Style.RESET_ALL)
                                    ff_df, ff_meta = fetch_1min_bars(symbol, start=ff_start, end=ff_end, feed=feed_enum)
                                    if ff_df is not None and not ff_df.empty:
                                        save_bars_to_csv(ff_df, symbol, DATA_DIR)
                                        if isinstance(ff_df.index, pd.MultiIndex):
                                            ff_ts = ff_df.index.get_level_values('timestamp') if 'timestamp' in ff_df.index.names else ff_df.index.get_level_values(-1)
                                        else:
                                            ff_ts = ff_df.index
                                        ff_ts = pd.DatetimeIndex(ff_ts).tz_convert(NY_TZ)
                                        state.loc[symbol, 'newest_date'] = ff_ts.max().normalize()
                                        print(colorama.Fore.GREEN + f"[FWD] {symbol} up-to-date through {state.loc[symbol, 'newest_date'].date()}" + colorama.Style.RESET_ALL)
                            except Exception as e_ff2:
                                print(colorama.Fore.RED + f"[FWD] {symbol} forward fill error: {e_ff2}" + colorama.Style.RESET_ALL)
                            symbols_remaining.remove(symbol)
                        # Persist state and continue
                        tmp = STATE_FILE + ".tmp"
                        state.to_csv(tmp)
                        state.to_csv(STATE_FILE)
                        break

                    # Summarize fetch result when we have data
                    try:
                        count = int(meta.get('count', len(bars_df)))
                        earliest = meta.get('earliest')
                        latest = meta.get('latest')
                        if pd.notna(earliest) and pd.notna(latest):
                            days_covered = max(0.0, (latest - earliest).total_seconds() / 86400.0)
                        else:
                            # Fallback compute from DataFrame if meta missing
                            if isinstance(bars_df.index, pd.MultiIndex):
                                ts_idx = bars_df.index.get_level_values('timestamp') if 'timestamp' in bars_df.index.names else bars_df.index.get_level_values(-1)
                            else:
                                ts_idx = bars_df.index
                            earliest = pd.to_datetime(ts_idx.min()).tz_convert(NY_TZ)
                            latest = pd.to_datetime(ts_idx.max()).tz_convert(NY_TZ)
                            days_covered = max(0.0, (latest - earliest).total_seconds() / 86400.0)
                        print(f"[FETCH] {symbol} received {count} bars; oldest={earliest}; days={days_covered:.2f}")
                    except Exception:
                        print(f"[FETCH] {symbol} received {len(bars_df)} bars; oldest=unknown; days=unknown")

                    save_bars_to_csv(bars_df, symbol, DATA_DIR)

                    # Edge-only missing detection using AAPL mask
                    if meta.get('earliest') is not None and pd.notna(meta.get('earliest')) and meta.get('latest') is not None and pd.notna(meta.get('latest')):
                        try:
                            aapl_days = _get_aapl_trading_days(start_date, end_date)
                        except Exception:
                            aapl_days = set()
                        # Determine symbol day coverage
                        if isinstance(bars_df.index, pd.MultiIndex):
                            ts_idx_all = bars_df.index.get_level_values('timestamp') if 'timestamp' in bars_df.index.names else bars_df.index.get_level_values(-1)
                        else:
                            ts_idx_all = bars_df.index
                        ts_idx_all = pd.DatetimeIndex(ts_idx_all).tz_convert(NY_TZ)
                        symbol_days = set(ts_idx_all.normalize().date)
                        if symbol_days:
                            first_sym_day = min(symbol_days)
                            last_sym_day = max(symbol_days)
                            older_missing_days = sorted([d for d in aapl_days if d < first_sym_day])
                            newer_missing_days = sorted([d for d in aapl_days if d > last_sym_day])
                        else:
                            older_missing_days = sorted(list(aapl_days))
                            newer_missing_days = []
                        # Earlier edge fill (older_missing_days)
                        if older_missing_days:
                            # Adjust oldest_date boundary forward; we won't chase beyond first AAPL day seen missing
                            first_available = pd.Timestamp(first_sym_day, tz=NY_TZ)
                            state.loc[symbol, 'oldest_date'] = max(state.loc[symbol, 'oldest_date'], first_available) if pd.notna(state.loc[symbol, 'oldest_date']) else first_available
                            print(colorama.Fore.LIGHTBLUE_EX + f"[EDGE-OLD] Missing {len(older_missing_days)} earlier AAPL trading day(s) for {symbol} before {first_sym_day}; boundary set to {state.loc[symbol, 'oldest_date'].date()}." + colorama.Style.RESET_ALL)
                        # Newer edge forward fill attempts only for full missing trading days
                        if newer_missing_days:
                            attempts = 0
                            attempted_days = []
                            latest_cov = meta.get('latest')
                            chunk_last_day = min(end_date.normalize().date(), max(aapl_days) if aapl_days else end_date.normalize().date())
                            for d in newer_missing_days:
                                if d > chunk_last_day:
                                    break  # outside chunk scope
                                day_start = pd.Timestamp(d, tz=NY_TZ)
                                day_end = day_start + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
                                print(colorama.Fore.YELLOW + f"[EDGE-NEW] Filling missing trading day {d} for {symbol}" + colorama.Style.RESET_ALL)
                                add_df, add_meta = fetch_1min_bars(symbol, start=day_start, end=day_end, feed=primary_feed)
                                attempted_days.append(d)
                                if add_df is not None and not add_df.empty:
                                    if isinstance(add_df.index, pd.MultiIndex):
                                        add_idx = add_df.index.get_level_values('timestamp') if 'timestamp' in add_df.index.names else add_df.index.get_level_values(-1)
                                    else:
                                        add_idx = add_df.index
                                    bars_df = pd.concat([bars_df, add_df]).sort_index()
                                    bars_df = bars_df[~bars_df.index.duplicated(keep='first')]
                                    latest_cov = add_meta.get('latest', latest_cov)
                                    print(colorama.Fore.GREEN + f"[EDGE-NEW] Added {len(add_df)} bars for {symbol} on {d}" + colorama.Style.RESET_ALL)
                                else:
                                    # treat illiquid day as covered: update newest_date if this day is newer
                                    if 'newest_date' in state.columns:
                                        try:
                                            nd_day = pd.Timestamp(d, tz=NY_TZ)
                                            if pd.isna(state.loc[symbol, 'newest_date']) or nd_day > state.loc[symbol, 'newest_date']:
                                                state.loc[symbol, 'newest_date'] = nd_day
                                        except Exception:
                                            pass
                                    print(colorama.Fore.YELLOW + f"[EDGE-NEW] No trades for {symbol} on {d}; treating as illiquid." + colorama.Style.RESET_ALL)
                                attempts += 1
                            # Recompute earliest/latest after any added bars
                            if isinstance(bars_df.index, pd.MultiIndex):
                                ts_idx_all = bars_df.index.get_level_values('timestamp') if 'timestamp' in bars_df.index.names else bars_df.index.get_level_values(-1)
                            else:
                                ts_idx_all = bars_df.index
                            if len(bars_df):
                                meta['earliest'] = pd.to_datetime(ts_idx_all.min()).tz_convert(NY_TZ)
                                meta['latest'] = pd.to_datetime(ts_idx_all.max()).tz_convert(NY_TZ)
                            # Warn only if there are AAPL days within chunk not attempted
                            remaining_unattempted = [d for d in newer_missing_days if d <= chunk_last_day and d not in attempted_days]
                            if remaining_unattempted:
                                print(colorama.Fore.MAGENTA + f"[WARN] {symbol} still missing {len(remaining_unattempted)} AAPL trading day(s) in chunk up to {chunk_last_day}." + colorama.Style.RESET_ALL)
                        # Update state newest_date after fills
                        latest_for_newest = meta.get('latest')
                        if latest_for_newest is not None and pd.notna(latest_for_newest):
                            try:
                                nd = latest_for_newest.tz_convert(NY_TZ).normalize()
                                if pd.isna(state.loc[symbol, 'newest_date']) or nd > state.loc[symbol, 'newest_date']:
                                    state.loc[symbol, 'newest_date'] = nd
                            except Exception:
                                pass
                    # Move boundary older using the earliest actual bar saved (prevents gaps when coverage is sparse)
                    earliest_ts = meta.get('earliest', None)
                    state.loc[symbol, 'last_end'] = earliest_ts if pd.notna(earliest_ts) else start_date
                    # update newest_date from meta.latest
                    latest_for_newest = meta.get('latest')
                    if latest_for_newest is not None and pd.notna(latest_for_newest):
                        try:
                            nd = latest_for_newest.tz_convert(NY_TZ).normalize()
                            if pd.isna(state.loc[symbol, 'newest_date']) or nd > state.loc[symbol, 'newest_date']:
                                state.loc[symbol, 'newest_date'] = nd
                        except Exception:
                            pass

                    # Save state atomically after each success
                    tmp = STATE_FILE + ".tmp"
                    state.to_csv(tmp)
                    state.to_csv(STATE_FILE)
                    break

                except (RequestException, APIError, ConnectionError) as e:
                    # If SIP is failing and we're not locked to IEX, consider fallback based on 3-month rule
                    if StockDataFeed is not None:
                        is_primary_sip = (chosen_feed is None) or (chosen_feed == StockDataFeed.SIP)
                    else:
                        is_primary_sip = False

                    if is_primary_sip and (feed_str.upper() != 'IEX') and StockDataFeed is not None:
                        span_days = _get_symbol_data_span_days(DATA_DIR, symbol)
                        has_3_months = span_days >= 90.0
                        if has_3_months:
                            print(colorama.Fore.YELLOW + f"[INFO] SIP failed for {symbol}, but >=3 months of data exists; not switching to IEX." + colorama.Style.RESET_ALL)
                            state.loc[symbol, 'complete'] = True
                            symbols_remaining.remove(symbol)
                            tmp = STATE_FILE + ".tmp"
                            state.to_csv(tmp)
                            state.to_csv(STATE_FILE)
                            break
                        else:
                            # Try IEX; if it works, lock symbol to IEX and rebuild from beginning
                            try:
                                df_probe, _ = fetch_1min_bars(symbol, start=start_date, end=end_date, feed=StockDataFeed.IEX)
                                # Lock to IEX and reset state
                                if df_probe is not None and not df_probe.empty:
                                    state.loc[symbol, 'feed'] = 'IEX'
                                    state.loc[symbol, 'last_end'] = pd.NaT
                                    state.loc[symbol, 'complete'] = False
                                    # Remove existing CSV to avoid mixing feeds
                                    sym_csv = os.path.join(DATA_DIR, f"{symbol}.csv")
                                    if os.path.exists(sym_csv):
                                        os.remove(sym_csv)
                                    print(colorama.Fore.CYAN + f"[SWITCH] Switching {symbol} to IEX and rebuilding entire dataset from beginning." + colorama.Style.RESET_ALL)
                                    tmp = STATE_FILE + ".tmp"
                                    state.to_csv(tmp)
                                    state.to_csv(STATE_FILE)
                                    # Break retry loop; next outer iteration will fetch using IEX
                                    break
                                else:
                                    # IEX probe yielded no data; fall back to normal retry
                                    retries += 1
                                    wait_time = RETRY_DELAY * retries
                                    print(f"[ERROR] {symbol}: {e} — retry {retries}/{MAX_RETRIES} in {wait_time}s...")
                                    time.sleep(wait_time)
                                    continue
                            except Exception as e2:
                                # Fallback failed; proceed with normal retry
                                retries += 1
                                wait_time = RETRY_DELAY * retries
                                print(f"[ERROR] {symbol}: {e} — retry {retries}/{MAX_RETRIES} in {wait_time}s...")
                                time.sleep(wait_time)
                                continue
                    else:
                        retries += 1
                        wait_time = RETRY_DELAY * retries
                        print(f"[ERROR] {symbol}: {e} — retry {retries}/{MAX_RETRIES} in {wait_time}s...")
                        time.sleep(wait_time)

            else:
                print(f"[FATAL] Skipping {symbol} after {MAX_RETRIES} retries.")
                symbols_remaining.remove(symbol)

def repair_symbol_gaps(symbol: str, data_dir: str, feed: str = '', max_days_window: int = 30):
    """Scan a symbol's CSV for missing 1-minute bars and attempt to backfill gaps.
    Strategy:
      - Load CSV
      - Build expected 1-minute timeline between earliest & latest (market hours only optional future enhancement)
      - Find gaps > 1 minute (missing minutes)
      - Batch adjacent missing minutes into contiguous ranges
      - For each range, request bars via fetch_1min_bars and merge
      - Preserve existing feed lock (do not mix feeds)
    """
    path = os.path.join(data_dir, f"{symbol}.csv")
    if not os.path.exists(path):
        print(f"[REPAIR] No data file for {symbol}")
        return
    try:
        df = pd.read_csv(path)
        # Infer timestamp col
        ts_col = None
        for c in ['timestamp','time','t','date','datetime']:
            if c in df.columns:
                ts_col = c
                break
        if ts_col is None:
            ts_col = df.columns[0]
        ts = pd.to_datetime(df[ts_col], errors='coerce', utc=True).dropna()
        if ts.empty:
            print(f"[REPAIR] No valid timestamps in {symbol} CSV")
            return
        ts = ts.sort_values()
        earliest = ts.min()
        latest = ts.max()
        # Limit repair to last max_days_window days to control API usage
        window_start = max(earliest, latest - pd.Timedelta(days=max_days_window))
        ts_window = ts[ts >= window_start]
        # Expected full minute index
        full_index = pd.date_range(start=ts_window.min(), end=ts_window.max(), freq='T', tz=ts_window.dtype.tz)
        missing = full_index.difference(ts_window)
        if missing.empty:
            print(f"[REPAIR] No gaps detected for {symbol} in last {max_days_window} days")
            return
        # Group missing into contiguous ranges
        ranges = []
        start = prev = missing[0]
        for ts_m in missing[1:]:
            if ts_m - prev > pd.Timedelta(minutes=1):
                ranges.append((start, prev))
                start = ts_m
            prev = ts_m
        ranges.append((start, prev))
        print(colorama.Fore.MAGENTA + f"[REPAIR] {symbol} found {len(ranges)} gap range(s) covering {len(missing)} missing minutes." + colorama.Style.RESET_ALL)
        # Determine feed enum
        feed_enum = None
        if StockDataFeed is not None:
            if feed.upper() == 'IEX':
                feed_enum = StockDataFeed.IEX
            elif feed.upper() == 'SIP':
                feed_enum = StockDataFeed.SIP
        repaired_rows = 0
        for a,b in ranges:
            try:
                # Expand a bit on boundaries to ensure coverage
                rng_start = a - pd.Timedelta(minutes=2)
                rng_end = b + pd.Timedelta(minutes=2)
                add_df, _ = fetch_1min_bars(symbol, start=rng_start, end=rng_end, feed=feed_enum)
                if add_df is None or add_df.empty:
                    continue
                # Normalize index for merge
                merged = pd.concat([df, add_df.reset_index() if not set(add_df.columns).issubset(df.columns) else add_df])
                # Drop duplicates by timestamp column heuristic
                merged[ts_col] = pd.to_datetime(merged[ts_col], errors='coerce', utc=True)
                merged = merged.dropna(subset=[ts_col]).drop_duplicates(subset=[ts_col]).sort_values(ts_col)
                df = merged
                repaired_rows += len(add_df)
                print(colorama.Fore.CYAN + f"[REPAIR] Filled gap {a} -> {b} with {len(add_df)} rows" + colorama.Style.RESET_ALL)
            except Exception as e:
                print(colorama.Fore.RED + f"[REPAIR] Error repairing gap {a}->{b} for {symbol}: {e}" + colorama.Style.RESET_ALL)
        # Save updated CSV
        if repaired_rows > 0:
            df.to_csv(path, index=False)
            print(colorama.Fore.GREEN + f"[REPAIR] Completed repair for {symbol}; added {repaired_rows} rows." + colorama.Style.RESET_ALL)
        else:
            print(f"[REPAIR] No rows added for {symbol}")
    except Exception as e:
        print(colorama.Fore.RED + f"[REPAIR] Unexpected error repairing {symbol}: {e}" + colorama.Style.RESET_ALL)