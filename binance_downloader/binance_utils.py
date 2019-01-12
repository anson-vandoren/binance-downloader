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

"""Utility functions that are specific to the Binance API"""

from typing import List, Optional

import pandas as pd
import requests
from logbook import Logger

from binance_downloader import util

log = Logger(__name__.split(".", 1)[-1])

BASE_URL = "https://api.binance.com/api/v1"
KLINE_URL = BASE_URL + "/klines"

KLINE_INTERVALS = (
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
)

EXCHANGE_INFO_FILE = "exchange_info.json"
EARLIEST_TIMESTAMPS_FILE = "earliest_timestamps.json"


def max_request_freq(req_weight: int = 1) -> float:
    """Get smallest allowable frequency for API calls.

    The return value is the maximum number of calls allowed per second

    :param req_weight: (int) weight assigned to this type of request
        Default: 1-weight
    :return: float of the maximum calls permitted per second
    """

    # Pull Binance exchange metadata (including limits) either from cached value, or
    # from the server if cached data is too old
    request_limits = _req_limits(get_exchange_info())

    for rate in request_limits:
        # Convert JSON response components (e.g.) "5" "minutes" to a Timedelta
        interval = pd.Timedelta(f"{rate['intervalNum']} {rate['interval']}")
        # Frequency (requests/second) is, e.g., 5000 / 300 sec or 1200 / 60 sec
        rate["req_freq"] = int(rate["limit"]) / interval.total_seconds()

    max_allowed_freq = None

    for limit in request_limits:
        # RAW_REQUESTS type should be treated as a request weight of 1
        weight = req_weight if limit["rateLimitType"] == "REQUEST_WEIGHT" else 1
        this_allowed_freq = limit["req_freq"] / weight

        if max_allowed_freq is None:
            max_allowed_freq = this_allowed_freq
        else:
            max_allowed_freq = min(max_allowed_freq, this_allowed_freq)

    log.info(
        f"Maximum permitted request frequency for weight {req_weight} is "
        f"{max_allowed_freq} / sec"
    )

    return 0 if max_allowed_freq is None else max_allowed_freq


def _req_limits(exchange_info: dict) -> List:
    return [
        rate
        for rate in (exchange_info["rateLimits"])
        if "REQUEST" in rate["rateLimitType"]
    ]


def get_exchange_info(force_update=False) -> dict:
    """Pull `exchange_info` data from local cache (if fresh) or Binance API otherwise

    :param force_update: if True, will always pull from the Binance API instead of
        first attempting to pull from cache
    :return: dict with the API response
    """

    prev_json = util.json_from_cache(EXCHANGE_INFO_FILE) if not force_update else {}

    if prev_json:
        cache_time = util.from_ms_utc(prev_json.get("serverTime", 0))
        age = pd.Timestamp("now", tz="utc") - cache_time

        if age <= pd.Timedelta("1 day"):
            log.info(f"Using cached exchange info (age={age})")
            return prev_json

    log.notice("Fetching new exchange info from API")

    response = requests.get(BASE_URL + "/exchangeInfo")
    _validate_api_response(response)
    data = response.json()

    if not isinstance(data, dict):
        raise ConnectionError("No exchange info returned from Binance")

    # Write out to disk for next time
    util.json_to_cache(data, EXCHANGE_INFO_FILE)

    return data


def interval_to_milliseconds(interval) -> Optional[int]:
    """Try to get milliseconds from an interval input

    :param interval: (str, pandas.Timedelta, int)
        Interval in one of several types. Attempt to convert this value into
        milliseconds for a Binance API call
    :return: (int) milliseconds of the interval if successful, otherwise None
    """

    if isinstance(interval, pd.Timedelta):
        return int(interval.total_seconds() * 1000)

    if isinstance(interval, int):
        log.info(f"Assuming interval '{interval}' is already in milliseconds")
        return interval

    # Try to convert from a string
    seconds_per_unit = {"m": 60, "h": 60 * 60, "d": 24 * 60 * 60, "w": 7 * 24 * 60 * 60}
    try:
        return int(interval[:-1]) * seconds_per_unit[interval[-1]] * 1000
    except (ValueError, KeyError):
        return None


def interval_to_timedelta(interval) -> Optional[pd.Timedelta]:
    """Convert a string Binance kline interval to a pandas.Timedelta

    :param interval: string matching one of the allowed kline intervals
    :return: pandas.Timedelta representing the interval if valid, otherwise None
    """

    msec = interval_to_milliseconds(interval)

    return None if msec is None else pd.Timedelta(msec, unit="ms")


def get_klines(symbol, interval, start=None, end=None, limit=1000) -> List:
    """Helper function to get klines from Binance for a single request

    :param symbol: (str)
        Symbol pair of interest (e.g. 'XRPBTC')
    :param interval: (str)
        Valid kline interval (e.g. '1m').
    :param start: (int, str, pandas.Timestamp)
        First kline open time desired. If int, should be in milliseconds since
        Epoch. If string or pandas.Timestamp, will assume UTC unless otherwise
        specified.
    :param end: (int, str, pandas.Timestamp)
        Last kline open time desired. If int, should be in milliseconds since
        Epoch. If string or pandas.Timestamp, will assume UTC unless otherwise
        specified.
    :param limit: (int)
        Maximum number of klines to fetch. Will be clamped to 1000 if higher
        due to current maximum Binance limit. A value <= 0 will be assumed as
        no limit, and 1000 will be used.
    :return: List[List]]
        Returns a list of klines in list format if successful (may be empty list)
    """
    if not isinstance(symbol, str):
        raise ValueError(f"Cannot get kline for symbol {symbol}")
    if not isinstance(start, int) and start is not None:
        start = util.date_to_milliseconds(start)
    if not isinstance(end, int) and end is not None:
        end = util.date_to_milliseconds(end)

    if not limit or (1 > limit > 1000):
        log.warn(f"Invalid limit ({limit}), using 1000 instead")
        limit = 1000

    # Set parameters and make the request
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if end is not None:
        params["endTime"] = end
    if start is not None:
        params["startTime"] = start

    response = requests.get(KLINE_URL, params=params)

    # Check for valid response
    _validate_api_response(response)

    return response.json()


def _validate_api_response(response):
    if response.status_code in [429, 418]:
        raise ConnectionError(f"Rate limits exceeded or IP banned: {response.json()}")
    elif response.status_code // 100 == 4:
        raise ConnectionError(f"Request error: {response.json()}")
    elif response.status_code // 100 == 5:
        raise ConnectionError(f"API error, status is unknown: {response.json()}")
    elif response.status_code != 200:
        raise ConnectionError(f"Unknown error on kline request: {response.json()}")


def earliest_valid_timestamp(symbol: str, interval: str) -> int:
    """Get the first open time for which Binance has symbol/interval klines

    :param symbol: (str)
        Symbol pair of interest (e.g. 'XRPBTC')
    :param interval: (str)
        Valid kline interval (e.g. '1m')
    :return: timestamp (in milliseconds) for the open time of the first available kline
    """

    if interval not in KLINE_INTERVALS:
        raise ValueError(f"{interval} is not a valid kline interval")

    # Check for locally cached response
    identifier = f"{symbol}_{interval}"
    prev_json = util.json_from_cache(EARLIEST_TIMESTAMPS_FILE)
    if prev_json:
        # Loaded JSON from disk, check if we already have this value:
        timestamp = prev_json.get(identifier, None)
        if timestamp is not None:
            log.info(
                f"Found cached earliest timestamp for {identifier}: "
                f"{util.from_ms_utc(timestamp)}"
            )
            return timestamp
    prev_json = {}
    log.info(f"No cached earliest timestamp for {identifier}, so fetching from server")

    # This will return the first recorded k-line for this interval and symbol
    kline = get_klines(symbol, interval, start=0, limit=1)

    # Get just the OpenTime value (timestamp in milliseconds)
    earliest_timestamp = int(kline[0][0])

    # Cache on disk
    prev_json[identifier] = earliest_timestamp
    util.json_to_cache(prev_json, EARLIEST_TIMESTAMPS_FILE)
    log.info(f"Wrote new data to {EARLIEST_TIMESTAMPS_FILE} for {identifier}")

    return earliest_timestamp
