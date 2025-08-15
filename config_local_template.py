API_KEY = "GET_YOUR_API_KEY_AND_PASTE_HERE"
API_SECRET = "GET_YOUR_API_SECRET_AND_PASTE_HERE"
BASE_URL = "https://paper-api.alpaca.markets/v2"
BASE_DATA_DIR = "c:/Users/matth/StockData"
MAX_RETRIES = 5
RETRY_DELAY = 10  # seconds between retries
CHUNK_DAYS = 90  # The number of days to get per symbol per loop, the code starts with most recent data, gets this number of days,
                 # when finished with all symbols, it will loop again with the next chunk of days
NY_TZ = "America/New_York"  # Timezone for New York