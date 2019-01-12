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

"""Classes for interacting with the Binance API"""

from multiprocessing.pool import ThreadPool
from typing import Optional, Tuple

import pandas as pd
from logbook import Logger
from tqdm import tqdm

from binance_downloader import binance_utils, db, util
from binance_downloader.db import Kline


class KlineFetcher(object):
    """Convenience class to fetch all klines within a given range (in parallel)"""

    REQ_LIMIT = 1000

    def __init__(
        self,
        interval: str,
        symbol: str,
        start_date,
        end_date,
        logger=None,
        max_per_second=1,
        cache_file=None,
    ):
        if logger is None:
            self.log = Logger(__name__.split(".", 1)[-1])
        else:
            self.log = logger

        self.cache_file = cache_file
        self.symbol = symbol.upper()

        if not interval or interval not in binance_utils.KLINE_INTERVALS:
            raise ValueError(f"'{interval}' not a valid Binance kline interval.")

        self.interval = interval
        self.interval_ms = binance_utils.interval_to_milliseconds(interval)
        self.interval_td = binance_utils.interval_to_timedelta(interval)

        self.start_time, self.end_time = self._fill_dates(start_date, end_date)

        self.kline_df: Optional[pd.DataFrame] = None

        self.rate_limiter = util.rate_limited(max_per_second)

    @property
    def ohlcv(self) -> pd.DataFrame:
        if self.kline_df is None:
            self.fetch_parallel()

        ohlcv = self.kline_df[
            :,
            [
                Kline.OPEN_TIME,
                Kline.OPEN,
                Kline.HIGH,
                Kline.LOW,
                Kline.CLOSE,
                Kline.VOLUME,
            ],
        ]
        return ohlcv.set_index(Kline.OPEN_TIME)

    def fetch_parallel(self) -> Optional[pd.DataFrame]:
        """Fetch klines in specified range from Binance API.

        Splits the requested range up into chunks and processes them in parallel, while
        respecting the API rate limits.
        """

        # Create list of all start and end timestamps
        ranges = self._get_chunk_ranges()
        if not ranges:
            self.log.warn(
                f"There are no klines for {self.symbol} at {self.interval} intervals "
                f"on Binance between {pd.to_datetime(self.start_time, unit='ms')} "
                f"and {pd.to_datetime(self.end_time, unit='ms')}"
            )
            return

        # Check if any needed chunks aren't already cached
        needed_ranges = self._uncached_ranges(ranges)
        if not needed_ranges:
            self.log.notice("All requested chunks already cached")
            return

        # At least some chunks actually need to be downloaded
        self.log.notice(f"Downloading {len(needed_ranges)} chunks...")

        # Create workers for all needed requests and create iterator
        pool = ThreadPool()
        rate_limited_fetch = self.rate_limiter(self._fetch_chunks)
        results = pool.imap(rate_limited_fetch, needed_ranges)
        pool.close()

        result_df = pd.DataFrame()

        # Show progress meter
        with tqdm(total=len(needed_ranges), desc="Download ", unit=" chunk") as pbar:
            for chunk in results:
                pbar.update(1)
                result_df = pd.concat([result_df, chunk], ignore_index=True)

        pool.join()  # Block until all workers are done

        self.kline_df = (
            result_df.drop_duplicates(Kline.OPEN_TIME)
            .sort_values(Kline.OPEN_TIME)
            .reset_index(drop=True)
        )

        self.log.info(f"Download complete for {len(self.kline_df)} klines from API")
        return self.kline_df

    def write_to_csv(self):
        """Write k-lines retrieved from Binance into a csv file"""
        if self.cache_file is None:
            raise ValueError("No cache file given, cannot write CSV")

        data_frame = db.range_from_hdf(
            self.symbol, self.interval, self.start_time, self.end_time
        )

        if data_frame is None or data_frame.empty:
            self.log.notice(
                f"Not writing CSV: no data between {self.start_time} and {self.end_time}"
            )
            return

        db.to_csv(data_frame, self.symbol, self.interval)

    def write_to_hdf(self):
        """If data was retrieved from API, will write to local cache (HDF5)
        Future calls for same data will be retrieved from cache instead of making an
        unneeded call to the API.
        """
        if self.cache_file is None:
            raise ValueError("No cache file given, cannot write to HDF")

        if self.kline_df is None or self.kline_df.empty:
            self.log.notice("Not writing to .h5 since no data was received from API")
            return

        db.to_hdf(self.kline_df, self.symbol, self.interval)

    def _uncached_ranges(self, desired_ranges):
        if self.cache_file is None:
            return desired_ranges

        cached_df = db.from_hdf(self.symbol, self.interval)

        if cached_df is None or cached_df.empty:
            return desired_ranges  # Need all

        cached_df.set_index(Kline.OPEN_TIME, inplace=True)

        uncached_ranges = []
        for chunk_range in desired_ranges:
            start, end = [util.from_ms_utc(timestamp) for timestamp in chunk_range]
            expected_rows = 1 + (end - start) // self.interval_td
            tolerance = self.interval_td / 2

            same_start = len(cached_df.loc[start - tolerance : start + tolerance]) > 0
            same_end = len(cached_df.loc[end - tolerance : end + tolerance]) > 0
            same_rows = len(cached_df.loc[start:end]) == expected_rows

            if not (same_start and same_end and same_rows):
                uncached_ranges.append(chunk_range)

        num_cached = len(desired_ranges) - len(uncached_ranges)
        self.log.notice(f"Found {num_cached} chunks already cached")

        return uncached_ranges

    def _get_chunk_ranges(self):
        # Get [(chunk_start_ms, chunk_end_ms)] for all 1000-kline chunks needed
        # to fill the requested (or clamped) period
        ranges = []

        period_start = self._get_valid_start()
        period_end = self._get_valid_end()

        if period_start > self.start_time:
            self.log.notice(
                "First available kline starts on {from_ms_utc(period_start)}"
            )
            if period_start >= period_end:
                # No valid ranges due to later available start time, so return early
                return []

        chunk_start = chunk_end = period_start
        chunk_span = (self.REQ_LIMIT - 1) * self.interval_ms

        while chunk_end < period_end:
            chunk_end = min(chunk_start + chunk_span, period_end)
            ranges.append((chunk_start, chunk_end))
            # Add overlap to ensure we don't miss any of the range if Binance
            # screwed up some of their data
            chunk_start = chunk_end - self.interval_ms * 10

        return ranges

    def _get_valid_end(self):
        # End date cannot be later than current time
        end = min(self.end_time, util.date_to_milliseconds("now"))
        # Subtract one interval from the end since it's really a start time
        end -= self.interval_ms
        return end

    def _get_valid_start(self):
        # Get earliest possible kline (may be later than desired start date)
        earliest = binance_utils.earliest_valid_timestamp(self.symbol, self.interval)

        start = max(self.start_time, earliest)
        return start

    def _fill_dates(self, start: Optional[int], end: Optional[int]) -> Tuple[int, int]:
        # Get interval (in milliseconds) for limit * interval
        # (i.e. 1000 * 1m = 60,000,000 milliseconds)
        span = self.REQ_LIMIT * self.interval_ms

        if start and end:
            self.log.info("Found start and end dates. Fetching full interval")
            return start, end

        if start:
            # No end date, so go forward by 1000 intervals
            self.log.notice(
                f"Found start date but no end: fetching {self.REQ_LIMIT} klines"
            )
            end = start + span
        elif end:
            # No start date, so go back 1000 intervals
            self.log.notice(
                f"Found end date but no start. Fetching previous {self.REQ_LIMIT} klines"
            )
            start = end - span
        else:
            # Neither start nor end date. Get most recent 1000 intervals
            self.log.notice(
                f"Neither start nor end dates found. Fetching most recent {self.REQ_LIMIT} klines"
            )
            end = util.date_to_milliseconds("now")
            start = end - span

        return start, end

    def _fetch_chunks(self, chunk_range: Tuple[int, int]):
        start, end = chunk_range  # In milliseconds

        # Get results from API into a pandas DataFrame
        result_list = binance_utils.get_klines(
            self.symbol, self.interval, start=start, end=end
        )
        data_frame = pd.DataFrame(result_list, columns=list(Kline))

        # Set data types correctly
        for col in list(Kline):
            if col in [Kline.OPEN_TIME, Kline.CLOSE_TIME]:
                data_frame[col] = util.from_ms_utc(data_frame[col])
            else:
                data_frame[col] = pd.to_numeric(data_frame[col])

        return self._clean_data(data_frame, chunk_range)

    def _clean_data(self, data_frame, chunk_range):
        start, end = chunk_range

        # Sometimes Binance shifts off the bottom of the minute; normalize to minutes
        clean_start = util.from_ms_utc(start).replace(second=0, microsecond=0)

        expected_rows = 1 + (end - start) // self.interval_ms

        # Temporarily index off of open_time to allow for reindexing
        data_frame.set_index(Kline.OPEN_TIME, inplace=True)

        # Generate new indexes to account for missing data or klines that don't start
        # on the bottom of the minute
        new_idx = [clean_start + i * self.interval_td for i in range(expected_rows)]

        if data_frame.empty:
            # Return a data frame with full range of open_time, but NaN for all others
            return data_frame.reindex(index=new_idx).reset_index()

        new_df = data_frame.reindex(
            index=new_idx, tolerance=self.interval_td / 2, method="nearest", limit=1
        )
        return new_df.reset_index()  # Other code is expecting an integer index
