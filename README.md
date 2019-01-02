Binance Downloader
==================

[![Build Status](https://travis-ci.com/anson-vandoren/binance-downloader.svg?branch=master)](https://travis-ci.com/anson-vandoren/binance-downloader)

Python tool to download Binance Candlestick (k-line) data from REST API

Originally forked from [bullsignals/binance-downloader](https://github.com/bullsignals/binance-downloader),
that project does not seem to be maintained any longer and I wanted to actually make use
of this project and allow others to contribute. At this point, I have re-written almost
all the code from scratch, but would like to thank the original authors for their ideas
that got me started.



Installation
-----------

### Prerequisites
You will need Poetry installed in order to install this package and run from the command line.
Poetry is a Python package and dependency manager that makes installation and distribution
really easy. Installation instructions [can be found here](https://poetry.eustace.io/docs/#installation)
for macOS/Linux/Windows
- Verify Poetry installation
```console
$ poetry --version
Poetry 0.12.10
```

### Download and install
##### Clone the repository
```console
$ git clone https://github.com/anson-vandoren/binance-downloader.git
$ cd binance-downloader
```
##### Activate your virtual environment
Poetry will try to automatically enable a virtual environment for you if it detects
you are not already using one.
> If you already use virtualenvwrapper (or similar), go ahead and make (or switch to)
> your working environment beforehand:
>
>```console
>$ mkvirtualenv binance-downloader
>```
>or
>```console
>$ workon binance-downloader
>```

##### Install dependencies
```console
$ poetry install
Installing dependencies from lock file

Package operations: 12 installs, 0 updates, 0 removals

  - Installing six (1.12.0)
  - Installing certifi (2008.11.29)
  - Installing chardet (3.0.4)
  - Installing idna (2.8)
  - Installing numpy (1.15.4)
  - Installing python-dateutil (2.7.5)
  - Installing pytz (2018.7)
  - Installing urllib3 (1.22)
  - Installing logbook (1.4.1)
  - Installing pandas (0.23.4)
  - Installing requests (2.21.0)
  - Installing tqdm (4.28.1)
  - Installing binance-downloader (0.2.0)
```


Using the Command Line Interface
-----------------------------
##### Show available options
```console
$  kline-binance --help
usage: kline-binance [-h] [--start START] [--end END] [--dtfmt DATE_FORMAT]
                     symbol interval

CLI for downloading Binance Candlestick (k-line) data in bulk

positional arguments:
  symbol               (Required) Binance symbol pair, e.g. ETHBTC
  interval             (Required) Frequency interval in minutes(m); hours(h);
                       days(d); weeks(w); months(M); All possibles values: 1m
                       3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M

optional arguments:
  -h, --help           show this help message and exit
  --start START        Start date to get data (inclusive). Format: yyyy/mm/dd
  --end END            End date to get data (exclusive). Format: yyyy/mm/dd
  --dtfmt DATE_FORMAT  Format to use for dates (DMY, MDY, YMD, etc). Default:
                       YMD
```

##### Downloading data
```console
$  kline-binance XRPBTC 1m --start 2016-01-01 --end now
[2019-01-02 05:12:40.941301] NOTICE: api: First available kline starts on {from_ms_utc(period_start)}
[2019-01-02 05:12:40.941867] NOTICE: api: Downloading 620 chunks...
Download : 100%|█████████████████████████████████████████████████| 620/620 [00:48<00:00, 12.73 chunk/s]
Write CSV: 100%|███████████████████████████████████████████████████| 100/100 [00:14<00:00,  7.04 pct/s]
[2019-01-02 05:13:44.784379] NOTICE: db: Done writing ./downloaded/2019-01-01_211330_XRPBTC_1m.csv for 612794 lines
```

License
-------
This code is made available under the MIT License. See LICENSE file for detail.
