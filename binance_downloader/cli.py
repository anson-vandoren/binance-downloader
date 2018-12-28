"""Command Line Interface"""
import argparse

from binance_downloader.api import BinanceAPI
from binance_downloader.binance_utils import date_to_milliseconds


def main():
    """Initial Command Line Function."""
    parser = argparse.ArgumentParser(
        description="Python tool to download Binance Candlestick (k-line) data from REST API"
    )
    parser.add_argument(
        "--interval",
        "-i",
        help="""
                        frequency interval in minutes(m); hours(h); days(d); weeks(w); months(M);
                        all possibles values: 
                        1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M""",
        required=True,
    )
    parser.add_argument(
        "--symbol", "-s", help="pair. default: 'ETHBTC'.", default="ETHBTC"
    )
    parser.add_argument("--limit", "-l", help="quantity of items downloaded;")
    parser.add_argument(
        "--start", "-st", help="Start period to get data. format: yyyy/mm/dd"
    )
    parser.add_argument(
        "--end", "-e", help="End period to get data (exclusive). format: yyyy/mm/dd"
    )
    parser.add_argument("--output", "-o", help="File Name. default: binance")
    # Allow to choose MM/DD/YYYY for date input
    parser.add_argument(
        "--dateformat",
        "-df",
        help="Format to use for dates (DMY, MDY, YMD, etc). Defaults to `YMD`",
        default="YMD",
    )

    args = parser.parse_args()
    kwargs = {}

    if args.dateformat:
        if args.dateformat in ["DMY", "MDY", "YMD"]:
            date_format = args.dateformat
        else:
            print(f"dateformat given ({args.dateformat}) not known. Using DMY")
            date_format = "DMY"
    else:
        date_format = "DMY"

    if args.limit:
        kwargs["limit"] = args.limit
    else:
        kwargs["limit"] = 500

    if args.start and args.end:
        kwargs["startTime"] = date_to_milliseconds(args.start, date_format=date_format)
        kwargs["endTime"] = date_to_milliseconds(args.end, date_format=date_format)

    if int(kwargs["limit"]) > 500 and not (args.start and args.end):
        parser.exit(
            0, "You must pass startTime and endTime because limit is bigger than 500.\n"
        )

    symbol = args.symbol
    interval = args.interval
    binance = BinanceAPI(interval, symbol, kwargs)
    output = args.output if args.output else "binance.csv"
    # binance_downloader.consult(output)
    binance.fetch_parallel()
    binance.write_to_csv()
    print("download finished successfully.")