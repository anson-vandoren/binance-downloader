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

"""Utilities that are not specific to Binance API"""

import json
import os
import threading
import time
from functools import wraps
from typing import Dict, Optional

import dateparser
import pandas as pd
import pytz
from logbook import Logger

CACHE_DIR = "cache/"

log = Logger(__name__.split(".", 1)[-1])


def rate_limited(max_per_second):
    """Prevents the decorated function from being called more than
    `max_per_second` times per second, locally, for one process
    """

    lock = threading.Lock()
    min_interval = 1.0 / max_per_second

    def decorate(func):
        last_time_called = time.perf_counter()

        @wraps(func)
        def rate_limited_function(*args, **kwargs):
            with lock:
                nonlocal last_time_called
                elapsed = time.perf_counter() - last_time_called
                left_to_wait = min_interval - elapsed
                if left_to_wait > 0:
                    time.sleep(left_to_wait)
                last_time_called = time.perf_counter()
            return func(*args, **kwargs)

        return rate_limited_function

    return decorate


def ensure_dir(file_path) -> None:
    """Convenience function to make a folder if the path doesn't already exist

    :param file_path: fully qualified file path
    :return: None
    """

    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)


def json_from_cache(file_name: str) -> Optional[Dict]:
    """Try to read JSON in from a given filename in a pre-defined folder

    :param file_name: desired file name. Appropriate folder will be prepended
    :return: JSON (as a dict) if file is present, otherwise None
    """

    json_path = os.path.join(CACHE_DIR, file_name)

    try:
        with open(json_path, "r") as cache_file:
            return json.load(cache_file)
    except IOError:
        log.notice(f"Error reading JSON from {json_path}")
        return None


def json_to_cache(new_json: Dict, file_name: str) -> None:
    """Write some JSON to disk in a pre-defined folder

    :param new_json: JSON to cache
    :param file_name: file name in which to cache (will be overwritten)
        Appropriate folder is prepended to the file name
    :return: None
    """

    json_path = os.path.join(CACHE_DIR, file_name)
    ensure_dir(json_path)
    with open(json_path, "w") as outfile:
        json.dump(new_json, outfile, ensure_ascii=False)


def from_ms_utc(binance_time: int) -> pd.Timestamp:
    """Convert Binance timestamps (milliseconds) to a datetime-like representation

    :param binance_time: integer number of milliseconds since epoch
    :return: pandas.Timestamp representing the integer timestamp
    """

    return pd.to_datetime(binance_time, unit="ms", utc=True)


def date_to_milliseconds(date_str, date_format="YMD") -> int:
    """Convert a date-like string to milliseconds since epoch

    :param date_str: string representing a date
    :param date_format: format order for the date. Defaults to YMD (e.g., 2018-01-30)
    :return: milliseconds since epoch
    """

    epoch = pd.Timestamp(0, tz="utc")
    to_date = dateparser.parse(date_str, settings={"DATE_ORDER": date_format})

    if to_date is None:
        raise ValueError(f"Unable to parse valid date from '{date_str}'")

    if to_date.tzinfo is None or to_date.tzinfo.utcoffset(to_date) is None:
        to_date = to_date.replace(tzinfo=pytz.utc)
    return int((to_date - epoch).total_seconds() * 1000.0)
