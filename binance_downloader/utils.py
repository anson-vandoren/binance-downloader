import json
import os
import threading
import time
from functools import wraps
from typing import Optional, Dict

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


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)


def _json_from_cache(file_name: str) -> Optional[Dict]:
    json_path = os.path.join(CACHE_DIR, file_name)

    try:
        with open(json_path, "r") as cache_file:
            return json.load(cache_file)
    except IOError:
        log.notice(f"Error reading JSON from {json_path}")
        return None


def json_to_cache(new_json: Dict, file_name: str) -> None:
    json_path = os.path.join(CACHE_DIR, file_name)
    ensure_dir(json_path)
    with open(json_path, "w") as outfile:
        json.dump(new_json, outfile, ensure_ascii=False)


def from_ms_utc(binance_time: int) -> pd.Timestamp:
    return pd.to_datetime(binance_time, unit="ms", utc=True)


def date_to_milliseconds(date_str, date_format="YMD") -> int:
    epoch = pd.Timestamp(0, tz="utc")
    d = dateparser.parse(date_str, settings={"DATE_ORDER": date_format})

    if d is None:
        raise ValueError(f"Unable to parse valid date from '{date_str}'")

    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)
    return int((d - epoch).total_seconds() * 1000.0)
