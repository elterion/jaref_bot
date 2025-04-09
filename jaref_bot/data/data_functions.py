from datetime import datetime
import polars as pl

def is_data_up_to_date(current_data, time_thresh):
    sorted_df = current_data.sort(by='ts')
    oldest_ts_exc = sorted_df.select("exchange").head(1).item()
    last_entry = sorted_df.filter(pl.col('exchange') == oldest_ts_exc).select("ts").tail(1).item()
    time_delta = int(datetime.timestamp(datetime.now())) - last_entry

    if time_delta > time_thresh:
        return False
    return True
