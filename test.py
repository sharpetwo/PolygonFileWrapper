import datetime as dt
import os
from polygon_wrapper import PolygonFileWrapper



def test_get_list_objects(year,month,partition):
    """ This test will download the files and save them in a tmp directory"""
    wrapper = PolygonFileWrapper()
    list_objects = wrapper.get_list_objects(year,month,partition)
    assert len(list_objects) > 10

def test_download_history_on_disk(start_date, end_date):
    """This test will download a full history between a starting date and an end date
    and then asserts that at least one .parquet file is stored in wrapper.datadir."""
    wrapper = PolygonFileWrapper()
    wrapper.download_history_on_disk(start_date, end_date)

    # Count .parquet files in the directory
    parquet_files = [f for f in os.listdir(wrapper.datadir) if f.endswith(".parquet")]
    number_of_parquet_files = len(parquet_files)

    assert number_of_parquet_files > 0

def test_download_single_date(date):
    """ This test will download a full history for a given date"""
    wrapper = PolygonFileWrapper()
    df = wrapper.download_single_date(date)
    assert len(df) > 100_000

def test_download_history_in_memory(start_date, end_date=None):
    """ This test will download the history between the two dates in memory"""
    wrapper = PolygonFileWrapper()
    df = wrapper.download_history_in_memory(start_date, end_date)
    assert len(df) > 1_000_000
