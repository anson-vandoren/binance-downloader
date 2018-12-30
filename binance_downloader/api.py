from multiprocessing.pool import ThreadPool
from typing import Tuple, Optional

import pandas as pd
from logbook import Logger
from tqdm import tqdm

from .db import Kline, to_hdf, from_hdf
from .binance_utils import (
    max_request_freq,
    KLINE_INTERVALS,
    interval_to_milliseconds,
    date_to_milliseconds,
    get_klines,
    earliest_valid_timestamp,
    kline_df_from_flat_list,
    KLINE_URL,
)
from .utils import ensure_dir, rate_limited

# Set up LogBook logging
log = Logger(__name__)


class BinanceAPI:

    max_per_sec = max_request_freq(req_weight=1)

    def __init__(self, interval, symbol, start_date, end_date):
        self.base_url = KLINE_URL
        # Binance limit per request is 1000 items
        self.req_limit = 1000
        self.symbol: str = symbol
        if (
            not interval
            or not isinstance(interval, str)
            or interval not in KLINE_INTERVALS
        ):
            raise ValueError(
                f"'{interval}' not recognized as valid Binance k-line interval."
            )
        self.interval = interval

        self.start_time, self.end_time = self._fill_dates(start_date, end_date)

        self.kline_df: Optional[pd.DataFrame] = None
        self.download_successful = False
        self.found_cached = False

    @rate_limited(max_per_sec)
    def fetch_blocks(self, start_end_times):
        start, end = start_end_times
        return get_klines(
            self.symbol,
            self.interval,
            start_time=start,
            end_time=end,
            limit=self.req_limit,
        )

    def fetch_parallel(self):
        # Create list of all start and end timestamps
        ranges = self._get_chunk_ranges()
        needed_ranges = self._uncached_ranges(ranges)

        # Create workers for all needed requests and start them
        pool = ThreadPool()
        # Fetch in parallel, but block until all requests are received

        flat_results = []
        it = pool.imap(self.fetch_blocks, needed_ranges)
        # Prevent more tasks being added to the pool
        pool.close()

        # Show progress meter
        with tqdm(total=len(needed_ranges) * self.req_limit) as pbar:
            for r in it:
                pbar.update(self.req_limit)
                flat_results.extend(r)

        # Block until all workers are done
        pool.join()

        self.kline_df = kline_df_from_flat_list(flat_results)
        if len(self.kline_df) == 0 and not self.found_cached:
            log.warn(
                f"there are no k-lines for {self.symbol} at {self.interval} "
                f"intervals on Binance between {pd.to_datetime(self.start_time, unit='ms')} "
                f"and {pd.to_datetime(self.end_time, unit='ms')}"
            )
        elif self.found_cached:
            log.info('Found cached results and did not download new')
            return
        else:
            log.info("Done fetching in parallel")
            self.download_successful = True

    def _uncached_ranges(self, desired_ranges):

        cached_df = from_hdf(self.symbol, self.interval)
        if cached_df is None or len(cached_df) == 0:
            return desired_ranges  # Need all
        cached_df.set_index(Kline.OPEN_TIME, inplace=True)
        uncached_ranges = []
        for r in desired_ranges:
            start, end = [pd.to_datetime(timestamp, unit='ms') for timestamp in r]
            try:
                if len(cached_df.loc[start]) > 0 and len(cached_df.loc[end]) > 0:
                    continue
                else:
                    uncached_ranges.append(r)
            except KeyError:
                # Didn't find this row. Possibly missed before, or possibly no data
                uncached_ranges.append(r)
        log.notice(f'Found {len(desired_ranges) - len(uncached_ranges)} chunks already cached')
        self.found_cached = uncached_ranges < desired_ranges
        return uncached_ranges

    def _get_chunk_ranges(self):
        # Get [(chunk_start_ms, chunk_end_ms)] for all 1000-kline chunks needed
        # to fill the requested (or clamped) period
        ranges = []

        period_start = self._get_valid_start()
        period_end = self._get_valid_end()
        interval_ms = interval_to_milliseconds(self.interval)

        chunk_start = chunk_end = period_start
        while chunk_end < period_end:
            # Add some overlap to allow for small changes in interval on Binance's side
            chunk_end = min(
                chunk_start + (self.req_limit - 1) * interval_ms, period_end
            )
            # Add to list of all intervals we need to request
            ranges.append((chunk_start, chunk_end))
            # Add overlap (duplicates filtered out later) to ensure we don't miss
            # any of the range if Binance screwed up some of their data
            chunk_start = chunk_end - interval_ms * 10
        return ranges

    def _get_valid_end(self):
        # End date cannot be later than current time
        end = min(self.end_time, date_to_milliseconds("now"))
        # Subtract one interval from the end since it's really a start time
        end -= interval_to_milliseconds(self.interval)
        return end

    def _get_valid_start(self):
        # Get earliest possible kline (may be later than desired start date)
        earliest = earliest_valid_timestamp(self.symbol, self.interval)

        start = max(self.start_time, earliest)
        if start > self.start_time:
            log.notice(
                f"Data not available to start on {pd.to_datetime(self.start_time, unit='ms')}, "
                f"starting from {pd.to_datetime(start, unit='ms')} instead"
            )
        return start

    def write_to_csv(self, output=None):
        """Write k-lines retrieved from Binance into a csv file

        :param output: output file path. If none, will be stored in ./downloaded
            directory with a timestamped filename based on symbol pair and interval
        :return: None
        """
        if not self.download_successful:
            log.notice("Not writing to .csv since no data was received from API")
            return

        if self.kline_df is None:
            raise ValueError("Must read in data from Binance before writing to disk!")

        # Generate default file name/path if none given
        output = output or self.output_file

        with open(output, "w") as csv_file:
            # Ensure 9 decimal places  (most prices are to 8 places)
            self.kline_df.to_csv(
                csv_file, index=False, float_format="%.9f", header=list(Kline)
            )
        log.notice(f"Done writing {output} for {len(self.kline_df)} lines")

    def write_to_hdf(self):
        if not self.download_successful:
            log.notice("Not writing to .h5 since no data was received from API")
            return
        to_hdf(self.kline_df, self.symbol, self.interval)

    @property
    def output_file(self, extension="csv"):
        timestamp = pd.Timestamp("now").strftime("%Y-%m-%d_%H%M%S")
        outfile = (
            f"./downloaded/{timestamp}_{self.symbol}_{self.interval}_klines.{extension}"
        )

        # Create the subdirectory if not present:
        ensure_dir(outfile)
        return outfile

    def _fill_dates(self, start: Optional[int], end: Optional[int]) -> Tuple[int, int]:

        # Get interval (in milliseconds) for limit * interval
        # (i.e. 1000 * 1m = 60,000,000 milliseconds)
        span = int(self.req_limit) * interval_to_milliseconds(self.interval)

        if start and end:
            log.info("Found start and end dates. Fetching full interval")
            return start, end
        elif start:
            # No end date, so go forward by 1000 intervals
            log.notice(f"Found start date but no end: fetching {self.req_limit} klines")
            end = start + span
        elif end:
            # No start date, so go back 1000 intervals
            log.notice(
                f"Found end date but no start. Fetching previous {self.req_limit} klines"
            )
            start = end - span
        else:
            # Neither start nor end date. Get most recent 1000 intervals
            log.notice(
                f"Neither start nor end dates found. Fetching most recent {self.req_limit} klines"
            )
            end = date_to_milliseconds("now")
            start = end - span

        return start, end
