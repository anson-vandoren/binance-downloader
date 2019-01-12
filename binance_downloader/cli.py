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

"""CLI to parse arguments and run appropriate API calls"""

import argparse

from logbook import Logger

from binance_downloader import api, util, binance_utils

log = Logger(__name__.split(".", 1)[-1])


def main():
    """Parse arguments, download data from API, write to cache and CSV"""
    log.info("*" * 80)
    log.info("***" + "Starting CLI Parser for binance-downloader".center(74) + "***")
    log.info("*" * 80)
    parser = argparse.ArgumentParser(
        description="CLI for downloading Binance Candlestick (k-line) data in bulk"
    )
    parser.add_argument("symbol", help="(Required) Binance symbol pair, e.g. ETHBTC")
    parser.add_argument(
        "interval",
        help="(Required) Frequency interval in minutes(m); hours(h); days(d); weeks(w); months(M);"
        " All possibles values: 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M",
    )
    parser.add_argument(
        "--start", help="Start date to get data (inclusive). Format: yyyy/mm/dd"
    )
    parser.add_argument(
        "--end", help="End date to get data (exclusive). Format: yyyy/mm/dd"
    )
    # Allow to choose MM/DD/YYYY for date input
    parser.add_argument(
        "--dtfmt",
        metavar="DATE_FORMAT",
        help="Format to use for dates (DMY, MDY, YMD, etc). Default: YMD",
        default="YMD",
    )

    args = parser.parse_args()

    if args.dtfmt:
        if args.dtfmt in ["DMY", "MDY", "YMD"]:
            date_format = args.dtfmt
        else:
            log.warn(f"Date format given ({args.dtfmt}) not known. Using YMD")
            date_format = "YMD"
    else:
        date_format = "YMD"

    if args.start:
        start_date = util.date_to_milliseconds(args.start, date_format=date_format)
    else:
        start_date = None

    if args.end:
        end_date = util.date_to_milliseconds(args.end, date_format=date_format)
    else:
        end_date = None

    symbol = str(args.symbol)
    interval = str(args.interval)
    max_per_second = binance_utils.max_request_freq(1)
    binance = api.KlineFetcher(
        interval, symbol, start_date, end_date, max_per_second=max_per_second
    )
    binance.fetch_parallel()
    binance.write_to_hdf()
    binance.write_to_csv()
