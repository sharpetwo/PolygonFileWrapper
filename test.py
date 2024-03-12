import datetime as dt

from polygon_wrapper import PolygonFileWrapper


def test_get_options_trades():
    wrapper = PolygonFileWrapper()
    df = wrapper.download_trades_parquet(dt.date(2024, 3, 8), dt.date(2024, 3, 10))
    assert len(df) > 1_000_000
