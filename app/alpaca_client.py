# All Alpaca API connection functions

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.trading.client import TradingClient
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.common.exceptions import APIError
from alpaca.common.enums import Sort
from requests.exceptions import RequestException
from datetime import datetime, timedelta
import time
import os
import sys
import pandas as pd
import pytz
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

def get_recent_bars(symbol: str, days: int = 1):
    """Fetch recent daily bars for a symbol."""
    data_client = connect_data()
    end_time = datetime.now(pytz.UTC) - timedelta(days=1)  # Exclude today
    start_time = end_time - timedelta(days=days)
    
    request = StockBarsRequest(
        symbol_or_symbols=[symbol],
        start=start_time,
        end=end_time,
        timeframe=TimeFrame.Day  # Use TimeFrame.Day instead of "1Day"
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
    
def fetch_1min_bars(symbol, start: datetime, end: datetime, limit=10000) -> pd.DataFrame:
    """
    Fetch all 1-minute bars for a symbol in the specified range using pagination.
    Returns a DataFrame with timestamp as index.
    """
    data_client = connect_data()

    all_bars = []
    current_start = start
    APIrequestDaysSize = 30  # max days per individual request (loops)

    while current_start < end:
        current_end = min(current_start + timedelta(days=APIrequestDaysSize), end)  # fetch max 30 days per request
        try:
            request_params = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Minute,
                start=current_start,
                end=current_end,
                adjustment="split"  # adjust for splits
            )
            bars = data_client.get_stock_bars(request_params)
            df = bars.df.copy() if bars else pd.DataFrame()
            if not df.empty:
                all_bars.append(df)
            current_start = current_end  # move to next chunk
        except Exception as e:
            print(f"Error fetching {symbol} from {current_start} to {current_end}: {e}")
            break  # stop on error to retry later
    if all_bars:
        full_df = pd.concat(all_bars).sort_index()
        # Remove duplicates if any
        full_df = full_df[~full_df.index.duplicated(keep='first')]
        return full_df
    return pd.DataFrame()

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
            adjustment="raw",
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
    maintains a state CSV, and **performs a one-time state migration** from local CSVs.
    NOTE: this version keeps the hard exit after migration as requested.
    """

    # Ensure directories exist
    os.makedirs(BASE_DATA_DIR, exist_ok=True)
    DATA_DIR = os.path.join(BASE_DATA_DIR, "1mintrades")
    os.makedirs(DATA_DIR, exist_ok=True)
    STATE_FILE = os.path.join(DATA_DIR, "download_state.csv")

    # Load or init state
    if os.path.exists(STATE_FILE):
        state = pd.read_csv(STATE_FILE, index_col='symbol')

        # Parse to UTC (handles mixed/naive tz safely), then convert to NY time
        state['last_end'] = pd.to_datetime(state['last_end'], errors='coerce', utc=True)\
                            .dt.tz_convert(NY_TZ)

        # If oldest_date is date-only or mixed tz, do the same; .normalize() -> midnight NY
        state['oldest_date'] = pd.to_datetime(state['oldest_date'], errors='coerce', utc=True)\
                                .dt.tz_convert(NY_TZ).dt.normalize()

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
    for col, default in [('last_end', pd.NaT), ('oldest_date', pd.NaT), ('complete', False)]:
        if col not in state.columns:
            state[col] = default

    # 4) add missing symbols to state with defaults
    missing = [s for s in symbols if s not in state.index]
    if missing:
        new_rows = pd.DataFrame({
            'last_end': pd.NaT,
            'oldest_date': pd.NaT,
            'complete': False
        }, index=pd.Index(missing, name='symbol'))
        state = pd.concat([state, new_rows], axis=0)

    # 5) optional: sort & de-dup the index
    state = state[~state.index.duplicated(keep='first')].sort_index()

    # 6) persist immediately (atomic write)
    tmp = STATE_FILE + ".tmp"
    state.to_csv(tmp)
    os.replace(tmp, STATE_FILE)
        

    # Fill missing oldest_date via API, once
    for symbol in symbols_df['symbol']:
        if pd.isna(state.loc[symbol, 'oldest_date']):
            try:
                api_oldest_ts = fetch_oldest_bar_date(symbol)
                if pd.notna(api_oldest_ts):
                    state.loc[symbol, 'oldest_date'] = api_oldest_ts.tz_convert(NY_TZ).normalize()
                    # print(f"[INFO] Oldest data for {symbol} starts {state.loc[symbol,'oldest_date']}")
            except Exception as e:
                print(f"[WARN] Failed to fetch oldest date for {symbol}: {e}")

    # Save state atomically
    tmp = STATE_FILE + ".tmp"
    state.to_csv(tmp)
    os.replace(tmp, STATE_FILE)

    symbols_remaining = set(state.index[state['complete'] == False])
    now = pd.Timestamp.now(tz=NY_TZ) - timedelta(days=1)  # one-day buffer

    while symbols_remaining:
        for symbol in list(symbols_remaining):
            oldest_date = state.loc[symbol, 'oldest_date']
            last_end = state.loc[symbol, 'last_end']

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
                    # IMPORTANT: Correct call signature (no trading_client as first arg)
                    bars_df = fetch_1min_bars(symbol, start=start_date, end=end_date)

                    if bars_df.empty:
                        print(f"[INFO] No more bars for {symbol}; marking complete.")
                        state.loc[symbol, 'complete'] = True
                        symbols_remaining.remove(symbol)
                        break

                    save_bars_to_csv(bars_df, symbol, DATA_DIR)

                    # Move boundary older by one chunk: next loop will fetch further back
                    state.loc[symbol, 'last_end'] = start_date

                    # Save state atomically after each success
                    tmp = STATE_FILE + ".tmp"
                    state.to_csv(tmp)
                    os.replace(tmp, STATE_FILE)
                    break

                except (RequestException, APIError, ConnectionError) as e:
                    retries += 1
                    wait_time = RETRY_DELAY * retries
                    print(f"[ERROR] {symbol}: {e} — retry {retries}/{MAX_RETRIES} in {wait_time}s...")
                    time.sleep(wait_time)

            else:
                print(f"[FATAL] Skipping {symbol} after {MAX_RETRIES} retries.")
                symbols_remaining.remove(symbol)