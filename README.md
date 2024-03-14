# PolygonFileWrapper

Basic wrapper around Polygon's flat file API.

# Install

    pip install "polygon_wrapper @ git+ssh://git@github.com/sharpetwo/PolygonFileWrapper"

# Dev setup

Install dependencies with `pip install -r requirements.txt`

If new dependencies are added, `requirements.txt` can be regenerated with `pip-compile` from `pip-tools` package:

    pip-compile -o requirements.txt pyproject.toml

# Polygon Mapping

Define two env variables `POLYGON_MARKET` and `POLYGON_ENDPOINT` depending on the use cases:

    POLYGON_MARKET -> OPTIONS, STOCKS, CRYPTO, FOREX, INDEX
    POLYGON_ENDPOINT -> DAY, MINUTES, TRADES, QUOTES

# Run tests

Make sure the desination folder for the downloaded data exist, eg. `options`.

    ACCESS_KEY=... SECRET_KEY=... POLYGON_MARKET=OPTIONS  POLYGON_ENDPOINT=MINUTES DATADIR=. pytest test.py
