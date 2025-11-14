API_KEY = "GET_YOUR_API_KEY_AND_PASTE_HERE"
API_SECRET = "GET_YOUR_API_SECRET_AND_PASTE_HERE"
BASE_URL = "https://paper-api.alpaca.markets/v2"
BASE_DATA_DIR = "c:/Users/YOUR_WINDOWS_USER/StockData"  # update to the file path where you want to save data.  Be aware "Documents" and "Desktop"
                                                        # may be synced folders with OneDrive and cause issues
MAX_RETRIES = 5
RETRY_DELAY = 10  # seconds between retries
CHUNK_DAYS = 360  # days per data chunk request
NY_TZ = "America/New_York"  # Timezone for New York
RESYNC_STATE_FROM_CSVS = False  # Whether to resync state from existing CSV files on startup
DAYS_B4_FORWARD_FILL = 5  # Number of days back before running forward fill (per symbol)
FIXED_CUTOFF_DATE = "2022-01-01"  # Fixed cutoff date for historical data
