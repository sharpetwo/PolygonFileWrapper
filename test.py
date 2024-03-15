import datetime as dt
import os
from polygon_wrapper import PolygonFileWrapper



def test_get_list_objects():
    """ This test will download the files and save them in a tmp directory"""
    wrapper = PolygonFileWrapper()
    list_objects = wrapper.get_list_objects(2024, 2)
    assert len(list_objects) > 10

def test_download_history_on_disk():
    """This test will download a full history between a starting date and an end date
    and then asserts that at least one .parquet file is stored in wrapper.datadir."""
    wrapper = PolygonFileWrapper()
    wrapper.download_history_on_disk("20240201", "20240204")

    # Count .parquet files in the directory
    parquet_files = [f for f in os.listdir(wrapper.datadir) if f.endswith(".parquet")]
    number_of_parquet_files = len(parquet_files)

    assert number_of_parquet_files > 0

def test_download_single_date():
    """ This test will download a full history for a given date"""
    wrapper = PolygonFileWrapper()
    df = wrapper.download_single_date("20240201")
    assert len(df) > 100_000

def test_download_history_in_memory():
    """ This test will download the history between the two dates in memory"""
    wrapper = PolygonFileWrapper()
    df = wrapper.download_history_in_memory("20240130", "20240131")
    assert len(df) > 1_000_000
