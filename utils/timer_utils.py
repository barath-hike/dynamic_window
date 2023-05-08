from datetime import datetime, timedelta
import pytz
import numpy as np

def nearest_10_minutes_ist():
    # Get the current time in IST
    ist_timezone = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(ist_timezone)

    # Round to the nearest 10-minute interval
    minutes = round(now_ist.minute / 10) * 10
    nearest_10_minutes = now_ist.replace(minute=minutes % 60, second=0, microsecond=0)
    if minutes // 60 > 0:
        nearest_10_minutes += timedelta(hours=1)

    return int(np.floor((nearest_10_minutes.hour * 60 + nearest_10_minutes.minute)/10) + 1)

def nearest_minute(dt, rounding=10):
    return dt + timedelta(minutes=(rounding - dt.minute % 10))

def nearest_midnight_noon(dt):
    if dt.hour < 12:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        return dt.replace(hour=12, minute=0, second=0, microsecond=0)