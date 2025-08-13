from app.alpaca_client import connect_trading, get_recent_bars, get_tradeable_symbols_df, download_all_symbols
from app.data_handler import DataHandler

# from app.data_handler.py import get_available_symbols

def main():
    trading_client = connect_trading()

    # Example: get account info
    account = trading_client.get_account()
    print(f"Account status: {account.status}")

    # Example: get recent bars
    bars = get_recent_bars("AAPL", days=3)
    for bar in bars["AAPL"]:
        print(bar)

    # Get tradeable symbols
    symbols_df = get_tradeable_symbols_df(trading_client=trading_client)

    if not symbols_df.empty:
        print(f"\nFound {len(symbols_df)} tradeable symbols")
        print("\nFirst 10 symbols:")
        print(symbols_df.head(10))
        
        # Save to CSV for future reference
        symbols_df.to_csv('alpaca_tradeable_symbols.csv', index=False)
        print("✅ Symbols saved to alpaca_tradeable_symbols.csv")
    else:
        print("❌ No symbols retrieved")   

    # handler = DataHandler("data")
    # loaded_symbols = handler.load_csv("Lavernes_full_list.csv")

    # if loaded_symbols is not None:
    #     print(f"Loaded {len(loaded_symbols)} symbols from CSV")     
    #     print(loaded_symbols.head())

    # """Simple test to print missing symbols"""
    # symbols_set = set(symbols_df['symbol'])
    # loaded_set = set(loaded_symbols['symbol'])
    
    # missing_symbols = loaded_set - symbols_set
    
    # if missing_symbols:
    #     print(f"⚠️  Found {len(missing_symbols)} symbols in loaded_symbols that are NOT in symbols_df:")
    #     for symbol in sorted(missing_symbols):
    #         print(f"   - {symbol}")
    # else:
    #     print("✅ All symbols in loaded_symbols are also present in symbols_df")

    download_all_symbols(trading_client=account, symbols_df=symbols_df)


if __name__ == "__main__":
    main()
