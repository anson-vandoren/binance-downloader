# MIT License
#
# Copyright (C) 2018 Anson VanDoren
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice (including the next paragraph) shall
# be included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

"""Save or load data with different file types"""

import os
from collections import namedtuple
from typing import Optional

import numpy as np
import pandas as pd
from logbook import Logger
from tqdm import tqdm

from binance_downloader import util

log = Logger(__name__.split(".", 1)[-1])

_BASE_DATA_DIR = "./downloaded/"

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
            data_frame = store.get(interval_key)
        except (KeyError, AttributeError):
            log.notice(f"Corrupted/missing data for {symbol}/{interval} at {file_name}")
            return None
        else:
            return data_frame


def range_from_hdf(symbol, interval, start, end) -> Optional[pd.DataFrame]:
    """Lookup a symbol/interval in local cache and return all values between start/end

    :param symbol: Binance symbol pair, e.g. `ETHBTC` for the klines to retrieve
    :param interval: Binance kline interval to retrieve
    :param start: Start date, either as timestamp (millisecond, UTC) or datetime-like
    :param end: End date, either as timestamp (millisecond, UTC) or datetime-like
    :return: pandas.DataFrame with requested data if found, otherwise None
    """

    if isinstance(start, int):
        start = util.from_ms_utc(start)
    if isinstance(end, int):
        end = util.from_ms_utc(end)

    full_df = from_hdf(symbol, interval).set_index(Kline.OPEN_TIME, drop=False)

    return full_df.loc[start:end]


def to_csv(data_frame: pd.DataFrame, symbol: str, interval: str, show_progress=True):
    """Write a pandas DataFrame out to CSV

    :param data_frame: DataFrame to be written
    :param symbol: Binance symbol pair, e.g. `ETHBTC` for the klines to retrieve
    :param interval: Binance kline interval to retrieve
    :param show_progress: Show a progress bar while writing? Default True
    :return: None
    """

    if data_frame is None or data_frame.empty:
        log.notice(f"Not writing CSV: empty DataFrame")
        return

    f_name = _get_file_name(symbol, interval, ext="csv", with_ts=True)

    log.info(f"Writing CSV output to {f_name}")

    csv_opts = {"index": False, "float_format": "%.9f"}

    if not show_progress:
        data_frame.to_csv(f_name, **csv_opts)
    else:
        # Split data into chunks to allow for updating progress bar
        num_chunks = 100
        chunks = np.array_split(data_frame.index, num_chunks)
        bar_params = {"total": num_chunks, "desc": "Write CSV", "unit": " pct"}

        # tqdm handles the progress bar display automatically
        for i, chunk in tqdm(enumerate(chunks), **bar_params):
            if i == 0:  # For the first chunk, create file and write header
                data_frame.loc[chunk].to_csv(f_name, mode="w", **csv_opts, header=True)
            else:  # For subsequent chunks, append and don't write header
                data_frame.loc[chunk].to_csv(f_name, mode="a", **csv_opts, header=False)

    log.notice(f"Done writing {f_name} for {len(data_frame)} lines")


def to_hdf(data_frame: pd.DataFrame, symbol: str, interval: str, force_merge=False):
    """Store kline data to HDF5 store.
    This function will try to avoid rewriting data if it already exists, since merging,
    de-duplicating, sorting, and re-indexing can be expensive.

    :param data_frame: DataFrame with the (possibly) new klines
    :param symbol: Binance symbol (used for file name generation)
    :param interval: Binance kline interval (used for key lookup in the HDF5 store)
    :param force_merge: default is False. If True, will merge, de-duplicate, sort, and
        re-index even if it's likely all data is already contained in the HDF5 store.
    :return: None
    """

    # Ensure there is something to save and we know where to save at
    if data_frame is None or data_frame.empty:
        log.notice("Cannot save to HDF file since the supplied DataFrame was empty")
        return
    if not symbol or not interval:
        log.error("Cannot save to HDF file without both symbol and interval specified")
        return

    file_name = _get_file_name(symbol, interval, ext="h5", with_ts=False)

    with pd.HDFStore(file_name, "a") as store:

        key = f"interval_{interval}"

        if key not in store:
            store.put(key, data_frame, format="table")
            log.info(f"Adding interval {key} to {symbol} HDF5 file")
            return

        # Check whether given data is already stored
        old_df = store.get(key)
        has_start = (
            old_df[Kline.OPEN_TIME].iloc[0] <= data_frame[Kline.OPEN_TIME].iloc[0]
        )
        has_end = (
            old_df[Kline.CLOSE_TIME].iloc[-1] >= data_frame[Kline.CLOSE_TIME].iloc[-1]
        )

        same_length = len(old_df) == len(data_frame)
        matching_data = has_start and has_end and same_length

        if not matching_data or force_merge:
            new_df = (
                pd.concat([old_df, data_frame], ignore_index=True)
                .drop_duplicates(Kline.OPEN_TIME)
                .sort_values(Kline.OPEN_TIME)
                .reset_index(drop=True)
            )

            store.put(key, new_df, format="table")

            log.notice(f"Merged cache: {len(old_df)} lines -> {len(new_df)} lines")
        else:
            log.info(f"No new data not already contained in HDF5 store")


def _get_file_name(symbol: str, interval: str, ext: str, with_ts: bool = True):
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

    return os.path.join(_BASE_DATA_DIR, file_name)
