from typing import Optional, List
import datetime as dt
import dateparser
from io import BytesIO
import gzip
import os
from enum import Enum

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import polars as pl
import pandas as pd


class PolygonMarket(Enum):
    OPTIONS = "us_options_opra"
    STOCKS = "us_stocks_sip"
    CRYPTO = "global_crypto"
    FOREX = "global_forex"
    INDEX = "index_placeholder"


class PolygonEndpoint(Enum):
    DAY = "day_aggs_v1"
    MINUTES = "minute_aggs_v1"
    QUOTES = "quotes_v1"
    TRADES = "trades_v1"


def format_year(year: int) -> int:
    """Helper function to format the year value."""

    if isinstance(year, int) and 2000 <= year < 2100:
        return year
    else:
        raise ValueError("Year must be an integer between 2000 and 2099 inclusive")


def format_month(month: int) -> str:
    """Helper function to format the month value."""

    if isinstance(month, int) and 1 <= month <= 12:
        return f"{month:02}"
    else:
        raise ValueError("Month must be an integer between 1 and 12 inclusive")


def format_day(day: int) -> str:
    """Helper function to format the day value."""

    if isinstance(day, int) and 1 <= day <= 31:
        return f"{day:02}"
    else:
        raise ValueError("Day must be an integer between 1 and 31 inclusive")


def parse_date(date: str) -> dt.date:
    """
    Format date as "YYYYMMDD".

    Args:
        date (str): The date string to format.

    Returns:
        datetime.date: The formatted date.

    """
    try:
        return dt.datetime.strptime(date, '%Y%m%d')
    except ValueError:
        _date = dateparser.parse(date)
        if not _date:
            raise ValueError(f"Date format isn't correct and (ideally) should be YYYYMMDD - currently {date}")

        return _date


def is_date_range_valid(start_date: str, end_date: str):
    """ Helper function checking if the start_date < end_date."""

    if not parse_date(start_date) <= parse_date(end_date):
        raise ValueError("end_date must be greater than start_date")
    else:
        return True


class PolygonFileWrapper():
    def __init__(self, polygon_market=None, polygon_endpoint=None, access_key=None, secret_key=None, datadir = '.'):
        self._base_bucket = 'flatfiles'
        self._endpoint_url = 'https://files.polygon.io'
        self._type = 's3'
        self._signature_version = 's3v4'
        self._env_market = polygon_market if polygon_market else os.environ["POLYGON_MARKET"]
        self._env_endpoint = polygon_endpoint if polygon_endpoint else os.environ["POLYGON_ENDPOINT"]

        self.polygon_market = self._get_polygon_market(self._env_market.upper())
        self.polygon_endpoint = self._get_polygon_endpoint(self._env_endpoint.upper())

        self.access_key = access_key if access_key else os.environ["ACCESS_KEY"]
        self.secret_key = secret_key if secret_key else os.environ["SECRET_KEY"]
        self.datadir = datadir if datadir else os.environ["DATADIR"]

        self.download_path = f'{self.polygon_market}/{self.polygon_endpoint}'
        self.s3 = self._init_session()

    def _get_polygon_market(self, market: str) -> str:
        """Get the market value from the environment variable."""

        try:
            return PolygonMarket[market].value
        except KeyError:
            raise ValueError(f"Invalid POLYGON_MARKET value: {self._env_market}")

    def _get_polygon_endpoint(self, endpoint: str) -> str:
        """Get the endpoint value from the environment variable."""

        try:
            return PolygonEndpoint[endpoint].value
        except KeyError:
            raise ValueError(f"Invalid POLYGON_ENDPOINT value: {endpoint}")

    def _get_date_range(self, start_date: str, end_date: str | None = None) -> pd.DataFrame :
        """ Helper function returning a formated date range from a start_date and end_date"""

        if not end_date:
            end_date = (dt.date.today() - dt.timedelta(days=1)).strftime('%Y%m%d')

        is_date_range_valid(start_date, end_date)
        start_date = parse_date(start_date)
        end_date = parse_date(end_date)

        # Generate a range of business days between start_date and end_date
        return pd.date_range(start=start_date, end=end_date, freq='B')

    def _init_session(self) -> boto3.client:
        """Initialize the S3 session using the provided credentials and configuration."""
        session = boto3.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

        # Create a client with your session and specify the endpoint
        s3 = session.client(
            self._type,
            endpoint_url=self._endpoint_url,
            config=Config(signature_version=self._signature_version),
        )
        return s3

    def _get_prefix(self, year: Optional[int] = None, month: Optional[int] = None) -> str:
        """Helper function to build the download path (prefix) needed after the bucket name."""
        year = format_year(year) if year else None
        month = format_month(month) if month else None

        if not year and not month:
            return self.download_path
        elif year and not month:
            return f'{self.download_path}/{year}'
        elif year and month:
            return f'{self.download_path}/{year}/{month}'
        else:
            raise ValueError("Month cannot come without a year")

    @staticmethod
    def _get_date_from_key(key: str) -> str:
        """Helper function to return the date from a key"""
        return key.split('/')[-1].split('.')[0]

    def _create_object_key(self, year: int, month: int, day: int) -> str:
        """Create an object key respecting Polygon name policies."""
        year = format_year(year)
        month = format_month(month)
        day = format_day(day)
        return f'{self.download_path}/{year}/{month}/{year}-{month}-{day}.csv.gz'

    def _get_filepath_parquet(self, key: str) -> str:
        """Helper function to get the file path for the parquet file based on the object key."""
        date = self._get_date_from_key(key)
        return f"{self.datadir}/{date}.parquet"

    def get_list_objects(self, year: Optional[int] = None, month: Optional[int] = None, verbose: bool = False) -> List[str]:
        """Download a list of object partial or total based on parameters year and month."""
        prefix = self._get_prefix(year, month)
        print(f'[+] Listing from {self._base_bucket}/{prefix}')
        objects = self.s3.list_objects(Bucket=self._base_bucket, Prefix=prefix)
        contents = [obj.get('Key') for obj in objects.get('Contents', [])]
        if verbose:
            print(contents)
        return contents

    def _clean_options_df(self, df: pl.DataFrame) -> pl.DataFrame:
        """Basic data cleaning for a DataFrame containing options trades."""

        return (
            df
            .with_columns(
                pl.from_epoch(pl.col("sip_timestamp"), time_unit="ns").dt.replace_time_zone("UTC").alias("timestamp"),
            )
            .with_columns(
                pl.col("timestamp").dt.convert_time_zone("America/New_York")
            )
        )

    def _clean_stocks_df(self, df: pl.DataFrame) -> pl.DataFrame:
        """Basic data cleaning for a DataFrame containing options trades."""

        return (
            df
            .with_columns(
                pl.from_epoch(pl.col("window_start"), time_unit="ns").dt.replace_time_zone("UTC").alias("timestamp"),
            )
            .with_columns(
                pl.col("timestamp").dt.convert_time_zone("America/New_York")
            )
        )

    def _clean_df(self, df : pl.DataFrame) ->  pl.DataFrame:
        """ Helper function to clean the dataset based on the dataformat inputed from polygon_market"""
        if self._env_market.lower() == 'stocks':
            return self._clean_stocks_df(df)
        elif self._env_market.lower() == 'options':
            return self._clean_options_df(df)

    def _download_parquet(self, key: str) -> Optional[pl.DataFrame]:
        """Helper function downloading a parquet file from S3, handling errors and return it as a DataFrame."""
        with BytesIO() as data:
            try:
                self.s3.download_fileobj(self._base_bucket, key, data)
                data.seek(0)
                csv_file = gzip.decompress(data.read())
                df = pl.read_csv(csv_file)
                return df
            except ClientError as e:
                error_code = e.response['Error']['Code']
                date = self._get_date_from_key(key)
                if error_code == '404':
                    print(f"404 - File not found for date {date} ")
                    return None
                else:
                    print(f"Error in _download_parquet for date {date}: {e}")
                    raise

    def _download_single_key(self, key: str, save_partition: bool = True,  clean: bool = False ) -> Optional[pl.DataFrame]:
        """ Helper function going through the necessary steps for downloading and processing of single key."""
        df = self._download_parquet(key)
        if df is None:
            return None

        if clean:
            df = self._clean_df(df)

        if save_partition:
            filepath = self._get_filepath_parquet(key)
            print(f"[+] Saving partition at: {filepath}")
            df.write_parquet(filepath, compression='snappy')

        return df

    def download_single_date(self, date: str, save_partition: bool = True ,clean: bool = False) -> Optional[pl.DataFrame]:
        """Download data from a single file specified by a date str format YYYYMMDD.
        If save_partition is true - it will save in the datadir.
        """
        date = parse_date(date)
        key = self._create_object_key(date.year, date.month, date.day)
        return self._download_single_key(key,save_partition,clean)

    def download_history_on_disk(self, start_date: str, end_date: str = None, clean: bool = False):
        """ Download history between start_date and end_date in format YYYYMMDD and save in the datadir.
            If no end_date provided we assume the day of yesterday.
            If clean is true - it will perform basic cleaning operations.
        """
        save_partition = True
        date_range = self._get_date_range(start_date, end_date)

        for current_date in date_range:
            key = self._create_object_key(current_date.year, current_date.month, current_date.day)
            _ = self._download_single_key(key, save_partition, clean)

    def download_history_in_memory(self, start_date:str, end_date:str = None, clean: bool = False ) -> Optional[pl.DataFrame]:

        """ Download history between start_date and end_date in format YYYYMMDD in memory.
            If no end_date provided we assume the day of yesterday.
            If clean is true - it will perform basic cleaning operations.
        """
        df_accumulated = None
        save_partition = False
        date_range = self._get_date_range(start_date, end_date)

        for current_date in date_range:
            key = self._create_object_key(current_date.year, current_date.month, current_date.day)
            df = self._download_single_key(key,save_partition,clean)

            if df is not None:
                df_accumulated = df if df_accumulated is None else pl.concat([df_accumulated,df])

        return df
