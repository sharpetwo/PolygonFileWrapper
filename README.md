# PolygonFileWrapper

Basic wrapper around Polygon's flat file API.

# Install

    pip install "polygon_wrapper @ git+ssh://git@github.com/sharpetwo/PolygonFileWrapper"

# Usage

You will need a Polygion.io account and an access key and secret key from [Polygon's Dashboard](https://polygon.io/dashboard/flat-files).

There are 2 main ways to use this package:

1. As a Python library, as part of a larger system to download flat files from Polygon, using `PolygonFileWrapper` directly. Credentials can either be passed in when instantiating `PolygonFileWrapper` or will be read from the environment variables.
2. As a command line script to do a one-off download of flat files by date range.

### Examples

For brevity, we will assume `ACCESS_KEY` and `SECRET_KEY` are defined in the environment.

Example code that downloads options trades for 2 days and returns a Polars DataFrame:

```python
from polygon_wrapper import PolygonFileWrapper, PolygonEndpoint

wrapper = PolygonFileWrapper()

start_date = dt.date(2024, 2, 1)
end_date = dt.date(2024, 2, 2)
df = wrapper.download_options(PolygonEndpoint.TRADES, start_date, end_date)
```

Example command to do the same as above, but save the files as parquet into a `options_trades` folder:

    polygon_download --endpoint trades --market options --start_date 20240201 --end_date 20240202 --output_dir options_trades/


Download options daily bars for entire February 2024:

    polygon_download --endpoint day --market options --start_date 20240201 --end_date 20240229 --output_dir testdir


#### Available data

Define two env variables `POLYGON_MARKET` and `POLYGON_ENDPOINT` depending on the use cases:

    POLYGON_MARKET -> OPTIONS, STOCKS, CRYPTO, FOREX, INDEX
    POLYGON_ENDPOINT -> DAY, MINUTES, TRADES, QUOTES

# Dev setup

Install dependencies with `pip install -r requirements.txt`

If new dependencies are added, `requirements.txt` can be regenerated with `pip-compile` from `pip-tools` package:

    pip-compile -o requirements.txt pyproject.toml


# Run tests

    ACCESS_KEY=... SECRET_KEY=... POLYGON_MARKET=OPTIONS  POLYGON_ENDPOINT=MINUTES pytest test.py
