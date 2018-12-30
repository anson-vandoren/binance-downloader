"""Save or load data with different file types"""
import os
from enum import Enum

import pandas as pd

# Set up LogBook logging
from logbook import Logger

log = Logger(__name__)

BASE_DATA_DIR = "./downloaded/"


class Kline(Enum):
    OPEN_TIME = "open_time"
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    VOLUME = "volume"
    CLOSE_TIME = "close_time"
    QUOTE_ASSET_VOLUME = "quote_asset_volume"
    NUMBER_OF_TRADES = "number_of_trades"
    TAKER_BY_BAV = "taker_by_bav"
    TAKER_BY_QAV = "taker_by_qav"
    IGNORED = "ignored"

    @classmethod
    def col_names(cls):
        return [k for k in cls]

    @classmethod
    def str_names(cls):
        return [str(k.value) for k in cls]


def get_file_name(symbol: str, interval: str, extension=None, timestamped=True):
    timestamp = pd.Timestamp("now").strftime("%Y-%m-%d_%H%M%S") if timestamped else ""
    extension = extension or ""
    file_name = f"{symbol}_{interval}.{extension}"
    if timestamped:
        file_name = f"{timestamp}_{file_name}"
    return os.path.join(BASE_DATA_DIR, file_name)


def from_hdf(symbol, interval):
    file_name = get_file_name(symbol, interval, extension="h5", timestamped=False)
    interval_key = f"interval_{interval}"

    if not os.path.isfile(file_name):
        log.warn(f"{file_name} does not exist")
        return None

    with pd.HDFStore(file_name, mode="r") as store:
        if interval_key not in store:
            log.warn(f"{symbol}/{interval} data not stored in HDF at {file_name}")
            return None
        try:
            df = store.get(interval_key)
        except (KeyError, AttributeError):
            log.warn(f"Corrupted/missing data for {symbol}/{interval} at {file_name}")
            return None
        return df


def to_hdf(df: pd.DataFrame, symbol: str, interval: str):
    # Ensure sufficient data is available
    if df is None or len(df) == 0:
        log.error("Cannot save to HDF file since the supplied DataFrame was empty")
        return
    if not symbol or not interval:
        log.error("Cannot save to HDF file without both symbol and interval specified")
        return

    # Create appropriate file name based on given parameters
    file_name = get_file_name(symbol, interval, extension="h5", timestamped=False)

    # Store in HDF5 by key, based on interval:

    # Try to open the store and read in previous data:
    interval_key = f"interval_{interval}"
    with pd.HDFStore(file_name, "a") as store:
        if interval_key not in store:
            store.put(interval_key, df, format="table")
            log.notice(
                f"Interval {interval_key} for {symbol} not already in HDF5 file, adding key"
            )
            return
        else:
            # Check whether given data is already stored
            old_df = store.get(interval_key)
            new_start = df[Kline.OPEN_TIME].iloc[0]
            new_end = df[Kline.CLOSE_TIME].iloc[-1]
            old_start = old_df[Kline.OPEN_TIME].iloc[0]
            old_end = old_df[Kline.CLOSE_TIME].iloc[-1]
            if old_start > new_start or old_end < new_end:
                # Don't already have this full data set
                # Combine the two and remove any duplicates
                new_df = (
                    pd.concat([old_df, df], ignore_index=True)
                    .drop_duplicates(Kline.OPEN_TIME)
                    .sort_values(Kline.OPEN_TIME)
                    .reset_index(drop=True)
                )
                store.put(interval_key, new_df, format="table")
                log.notice(
                    f"Merged with previous data for {symbol}/{interval_key}. "
                    f"data_frame: {len(old_df)} lines -> {len(new_df)} lines"
                )
            else:
                log.notice(f"No new data not already contained in HDF5 store")
