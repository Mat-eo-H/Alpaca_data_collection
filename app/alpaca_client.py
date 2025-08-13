# All Alpaca API connection functions

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.trading.client import TradingClient
from alpaca.data.timeframe import TimeFrame  # Add this import
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus
from datetime import datetime, timedelta
import os
import pandas as pd
import pytz
from typing import Optional
from config_local import API_KEY, API_SECRET, BASE_URL
from app.data_handler import save_bars_to_csv

def connect_trading():
    """Connect to Alpaca trading API."""
    client = TradingClient(API_KEY, API_SECRET, paper=True)
    print("✅ Connected to Alpaca Trading API")
    return client

def connect_data():
    """Connect to Alpaca market data API."""
    client = StockHistoricalDataClient(API_KEY, API_SECRET)
    print("✅ Connected to Alpaca Market Data API")
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
            # if tradable and not asset.tradable:
            #     continue
            # if shortable is not None and asset.shortable != shortable:
            #     continue
            # if fractionable is not None and asset.fractionable != fractionable:
            #     continue
                
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
    while current_start < end:
        current_end = min(current_start + timedelta(days=30), end)  # fetch max 30 days per request
        try:
            request_params = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=TimeFrame.Minute,
                start=current_start,
                end=current_end,
                adjustment="raw"
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

def download_all_symbols(trading_client: TradingClient,symbols_df):
    """
    Downloads 1-minute bars for all symbols, starting with the last 90 days,
    going further back, and keeps track of progress in a state CSV.
    """
    
    # Directories
    BASE_DATA_DIR = "data"
    DATA_DIR = os.path.join(BASE_DATA_DIR, "1mintrades")
    os.makedirs(DATA_DIR, exist_ok=True)    
    STATE_FILE = os.path.join(DATA_DIR, "download_state.csv")

    # Load state file or initialize
    if os.path.exists(STATE_FILE):
        state = pd.read_csv(STATE_FILE, index_col='symbol')
    else:
        state = pd.DataFrame({'last_end': pd.NaT}, index=symbols_df['symbol'])

    chunk_days = 90
    current_end = datetime.now() - timedelta(days=1)

    symbols_remaining = set(symbols_df['symbol'])

    while symbols_remaining:
        current_start = current_end - timedelta(days=chunk_days)
        symbols_to_remove = set()

        for symbol in symbols_remaining:
            last_end = state.loc[symbol, 'last_end']
            if pd.notna(last_end):
                last_end_dt = datetime.fromisoformat(last_end)
                if current_start <= last_end_dt:
                    continue  # already downloaded

            print(f"Fetching {symbol} from {current_start.date()} to {current_end.date()}...")
            bars_df = fetch_1min_bars(trading_client, symbol, start=current_start, end=current_end)

            if bars_df.empty:
                print(f"No more data for {symbol}, removing from active list.")
                symbols_to_remove.add(symbol)
                continue

            save_bars_to_csv(bars_df, symbol, DATA_DIR)
            state.loc[symbol, 'last_end'] = current_start.isoformat()

        # Save progress
        state.to_csv(STATE_FILE)

        # Remove symbols with no more data
        symbols_remaining -= symbols_to_remove

        # Move window back
        current_end = current_start
