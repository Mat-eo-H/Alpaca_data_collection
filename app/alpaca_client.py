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
from config_local import API_KEY, API_SECRET, BASE_URL, BASE_DATA_DIR, MAX_RETRIES, RETRY_DELAY, CHUNK_DAYS, NY_TZ
from app.utils import ensure_tz_aware
from app.data_handler import save_bars_to_csv


_data_connected_once = False

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
        truncated = bool(saw_full_page_without_token or coverage_gap)
        meta = {"truncated": truncated, "earliest": earliest, "latest": latest, "pages": pages, "count": int(total_rows)}
        return full_df, meta
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
        state['last_end'] = pd.to_datetime(state['last_end'], errors='coerce', utc=True)\
                            .dt.tz_convert(NY_TZ)

        # If oldest_date is date-only or mixed tz, do the same; .normalize() -> midnight NY
        if 'oldest_date' not in state.columns:
            state['oldest_date'] = pd.Series(pd.NaT, dtype="datetime64[ns, America/New_York]")
        else:
            state['oldest_date'] = pd.to_datetime(state['oldest_date'], errors='coerce', utc=True).dt.tz_convert(NY_TZ)


        # 'complete' column fallback
        if 'complete' not in state.columns:
            state['complete'] = False
    else:
        # fresh state
        state = pd.DataFrame(index=symbols_df['symbol'].tolist())
        state['last_end'] = pd.NaT
        state['oldest_date'] = pd.NaT
        state['complete'] = False

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
    for col, default in [('last_end', pd.NaT), ('oldest_date', pd.NaT), ('complete', False), ('feed', '')]:
        if col not in state.columns:
            state[col] = default

    # 4) add missing symbols to state with defaults
    missing = [s for s in symbols if s not in state.index]
    if missing:
        new_rows = pd.DataFrame({
            'last_end': pd.NaT,
            'oldest_date': pd.NaT,
            'complete': False,
            'feed': ''
        }, index=pd.Index(missing, name='symbol'))
        state = pd.concat([state, new_rows], axis=0)

    # 5) optional: sort & de-dup the index
    state = state[~state.index.duplicated(keep='first')].sort_index()

    # 6) persist immediately (two writes; leave .tmp)
    tmp = STATE_FILE + ".tmp"

    # write tmp with fsync
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        state.to_csv(f)
        f.flush()
        os.fsync(f.fileno())

    # write final (no fsync)
    state.to_csv(STATE_FILE)
        

    # Fill oldest_date via API only for new symbols, unless it's the first day of a quarter.
    oldestDateAPICallCounter = 0
    today_ny = pd.Timestamp.now(tz=NY_TZ)
    is_quarter_start = (today_ny.month in (1, 4, 7, 10)) and (today_ny.day == 1)

    for symbol in symbols:  # normalized list
        # Ensure row exists
        if symbol not in state.index:
            state.loc[symbol, ['last_end', 'oldest_date', 'complete', 'feed']] = [pd.NaT, pd.NaT, False, '']

        # Determine if symbol is "new" to the database: no CSV in DATA_DIR
        sym_csv = os.path.join(DATA_DIR, f"{symbol}.csv")
        is_new_symbol = not os.path.exists(sym_csv)

        # Only fetch oldest_date from API if:
        # - it's the first day of a quarter (refresh all), OR
        # - this is a new symbol with no existing CSV (bootstrap), OR
        # - oldest_date is missing and there is no CSV yet
        need_api = is_quarter_start or is_new_symbol or (pd.isna(state.loc[symbol, 'oldest_date']) and is_new_symbol)

        if need_api:
            try:
                api_oldest_ts = fetch_oldest_bar_date(symbol)
                if pd.notna(api_oldest_ts):
                    state.loc[symbol, 'oldest_date'] = api_oldest_ts.tz_convert(NY_TZ).normalize()
                print(f"[INFO] Oldest date for {symbol}: {state.loc[symbol, 'oldest_date']}")
                oldestDateAPICallCounter += 1
                if oldestDateAPICallCounter % 100 == 0:
                    print(f"[INFO] Fetched oldest date for {oldestDateAPICallCounter} symbols so far, updating STATE_FILE")
                    state.to_csv(STATE_FILE)
            except Exception as e:
                print(f"[WARN] Failed to fetch oldest date for {symbol}: {e}")

                


    # Save state atomically
    tmp = STATE_FILE + ".tmp"
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

    while symbols_remaining:
        for symbol in list(symbols_remaining):
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
                        # Do not advance dates on empty; only mark complete if we're at or past the oldest boundary
                        if pd.notna(oldest_date) and (start_date <= oldest_date):
                            print(colorama.Fore.LIGHTCYAN_EX, f"[INFO] No more bars for {symbol}; marking complete.", colorama.Style.RESET_ALL)
                            state.loc[symbol, 'complete'] = True
                            symbols_remaining.remove(symbol)
                        # Persist state and continue
                        tmp = STATE_FILE + ".tmp"
                        state.to_csv(tmp)
                        state.to_csv(STATE_FILE)
                        break

                    save_bars_to_csv(bars_df, symbol, DATA_DIR)

                    # If truncated/incomplete coverage, warn loudly and use the earliest timestamp actually saved
                    if meta.get('truncated', False):
                        print(colorama.Fore.MAGENTA + f"[WARN] Bars possibly truncated for {symbol} ({meta.get('count')} rows across {meta.get('pages')} page(s))." + colorama.Style.RESET_ALL)
                        print(colorama.Fore.CYAN + f"[WARN] Coverage from {meta.get('earliest')} to {meta.get('latest')} may not span the requested window {start_date} to {end_date}." + colorama.Style.RESET_ALL)
                    # Move boundary older using the earliest actual bar saved (prevents gaps when coverage is sparse)
                    earliest_ts = meta.get('earliest', None)
                    state.loc[symbol, 'last_end'] = earliest_ts if pd.notna(earliest_ts) else start_date

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