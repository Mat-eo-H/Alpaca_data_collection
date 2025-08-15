# Misc helper functions
import pandas as pd
from datetime import datetime, timedelta
from config_local import NY_TZ

def ensure_tz_aware(series: pd.Series) -> pd.Series:
    """
    Ensure a datetime series is tz-aware (NY). If tz-naive, localize to NY.
    If already tz-aware but not NY, leave as-is (comparisons still work).
    """
    if series.dtype == 'datetime64[ns]':  # naive
        return series.dt.tz_localize(NY_TZ)
    try:
        # pandas extension dtype with tz info
        _ = series.dt.tz
        if series.dt.tz is None:
            return series.dt.tz_localize(NY_TZ)
    except Exception:
        pass
    return series
