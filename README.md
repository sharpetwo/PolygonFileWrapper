# PolygonFileWrapper

Basic wrapper around Polygon's flat file API.

# Install

    pip install "polygon_wrapper @ git+ssh://git@github.com/sharpetwo/PolygonFileWrapper"

# Dev setup

Install dependencies with `pip install -r requirements.txt`

If new dependencies are added, `requirements.txt` can be regenerated with `pip-compile` from `pip-tools` package:

    pip-compile -o requirements.txt pyproject.toml

# Run tests

    ACCESS_KEY=... SECRET_KEY=... ENDPOINT=https://files.polygon.io INSTRUMENT=us_options_opra DATADIR=. pytest test.py
