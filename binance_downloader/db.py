"""Save or load data with different file types"""
import os
from collections import namedtuple
from typing import Optional, Union

import pandas as pd

from .utils import from_ms_utc

# Set up LogBook logging
from logbook import Logger

log = Logger(__name__.split(".", 1)[-1])

BASE_DATA_DIR = "./downloaded/"

KlineCols = namedtuple(
    "KlineCols",
    [
        "OPEN_TIME",
        "OPEN",
        "HIGH",
        "LOW",
        "CLOSE",
        "VOLUME",
        "CLOSE_TIME",
        "QUOTE_ASSET_VOLUME",
        "NUMBER_OF_TRADES",
        "TAKER_BY_BAV",
        "TAKER_BY_QAV",
        "IGNORED",
    ],
)

Kline = KlineCols(
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_by_qav",
    "taker_by_bav",
    "ignored",
)


def from_hdf(symbol: str, interval: str) -> Optional[pd.DataFrame]:
    """Try to load a DataFrame from .h5 store for a given symbol and interval

    :param symbol: Binance symbol pair, e.g. `ETHBTC` for the klines to retrieve
    :param interval: Binance kline interval to retrieve
    :return: Pandas.DataFrame with requested data if found, otherwise None
    """

    file_name = _get_file_name(symbol, interval, ext="h5", with_ts=False)

    if not os.path.isfile(file_name):
        log.info(f"{file_name} does not exist, returning None")
        return None

    # Python will complain if we just use interval name, since it goes against naming
    # conventions of not starting a variable name with a number
    interval_key = f"interval_{interval}"

    with pd.HDFStore(file_name, mode="r") as store:
        if interval_key not in store:
            log.info(f"{symbol}/{interval} data not stored in HDF at {file_name}")
            return None
        try:
            df = store.get(interval_key)
        except (KeyError, AttributeError):
            log.notice(f"Corrupted/missing data for {symbol}/{interval} at {file_name}")
            return None
        else:
            return df


def range_from_hdf(symbol, interval, start, end) -> Optional[pd.DataFrame]:
    full_df = from_hdf(symbol, interval).set_index(Kline.OPEN_TIME, drop=False)
    if isinstance(start, int):
        start = from_ms_utc(start)
    if isinstance(end, int):
        end = from_ms_utc(end)
    df = full_df.loc[start:end]
    return df


def to_hdf(df: pd.DataFrame, symbol: str, interval: str, force_merge=False):
    """Store kline data to HDF5 store.
    This function will try to avoid rewriting data if it already exists, since merging,
    de-duplicating, sorting, and re-indexing can be expensive.

    :param df: DataFrame with the (possibly) new klines
    :param symbol: Binance symbol (used for file name generation)
    :param interval: Binance kline interval (used for key lookup in the HDF5 store)
    :param force_merge: default is False. If True, will merge, de-duplicate, sort, and
        re-index even if it's likely all data is already contained in the HDF5 store.
    :return: None
    """

    # Ensure there is something to save and we know where to save at
    if df is None or len(df) == 0:
        log.notice("Cannot save to HDF file since the supplied DataFrame was empty")
        return
    if not symbol or not interval:
        log.error("Cannot save to HDF file without both symbol and interval specified")
        return

    file_name = _get_file_name(symbol, interval, ext="h5", with_ts=False)

    with pd.HDFStore(file_name, "a") as store:

        key = f"interval_{interval}"

        if key not in store:
            store.put(key, df, format="table")
            log.notice(f"Interval {key} not in {symbol} HDF5 file, adding key")
            return
        else:
            # Check whether given data is already stored
            old_df = store.get(key)
            has_start = old_df[Kline.OPEN_TIME].iloc[0] <= df[Kline.OPEN_TIME].iloc[0]
            has_end = old_df[Kline.CLOSE_TIME].iloc[-1] >= df[Kline.CLOSE_TIME].iloc[-1]

            same_length = len(old_df) == len(df)
            matching_data = has_start and has_end and same_length

            # TODO: May result in false positives; should be OK for small data sets
            if not matching_data or force_merge:
                new_df = (
                    pd.concat([old_df, df], ignore_index=True)
                    .drop_duplicates(Kline.OPEN_TIME)
                    .sort_values(Kline.OPEN_TIME)
                    .reset_index(drop=True)
                )

                store.put(key, new_df, format="table")

                log.notice(
                    f"Merged {symbol}/{key}: {len(old_df)} lines -> {len(new_df)} lines"
                )

            else:
                log.notice(f"No new data not already contained in HDF5 store")


def _get_file_name(symbol: str, interval: str, ext: str = "", with_ts: bool = True):
    """Get an appropriate storage file path and name based on data being stored and format

    :param symbol: Binance symbol pair, e.g. `ETHBTC` for the klines being stored
    :param interval: Binance kline interval for data being stored, e.g. `1m`
    :param ext: desired file extension, e.g. .csv or .h5
    :param with_ts: if True, current timestamp will be prepended to the filename
        Default: True
    :return: string representation of the file path
    """

    # Normalise the file extension
    if ext[0] != ".":
        ext = f".{ext}"

    if ext == ".h5":
        # HDF files will store all intervals in the same file (different keys)
        file_name = f"{symbol}{ext}"
    else:
        # CSV (and other formats) will create separate file for different intervals
        file_name = f"{symbol}_{interval}{ext}"

    if with_ts:
        timestamp = pd.Timestamp("now").strftime("%Y-%m-%d_%H%M%S")
        file_name = f"{timestamp}_{file_name}"

    return os.path.join(BASE_DATA_DIR, file_name)
