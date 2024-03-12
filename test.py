import datetime as dt
from polygon_wrapper import PolygonFileWrapper

def test_download_list_objects():
    """ This test assumes that at least a full month of data will be returned """
    wrapper = PolygonFileWrapper()
    objects = wrapper.get_list_objects(verbose=True)
    assert len(objects) > 30

def test_download_from_list_objects():
    """ This test assumes that at least a 1000 rows will be returned"""
    wrapper = PolygonFileWrapper()
    df = wrapper.download_from_list_objects(partition=False,save_disk=False)
    assert len(df) > 1_000

def test_get_options_trades():
    wrapper = PolygonFileWrapper()
    df = wrapper.download_trades_parquet(dt.date(2024, 3, 8), dt.date(2024, 3, 10))
    assert len(df) > 1_000_000
