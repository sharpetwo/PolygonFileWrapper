import datetime as dt
import dateparser
from enum import Enum
import gzip
from io import BytesIO
import os
from typing import Optional, List

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


def clean_options_trades(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .with_columns(
            pl.from_epoch(pl.col("sip_timestamp"), time_unit="ns").dt.replace_time_zone("UTC").alias("timestamp"),
        )
        .with_columns(
            pl.col("timestamp").dt.convert_time_zone("America/New_York")
        )
    )

def clean_options_bars(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .with_columns(
            pl.from_epoch(pl.col("window_start"), time_unit="ns").dt.replace_time_zone("UTC").alias("timestamp"),
        )
        .with_columns(
            pl.col("timestamp").dt.convert_time_zone("America/New_York")
        )
    )


def clean_stock_trades(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .with_columns(
            pl.from_epoch(pl.col("window_start"), time_unit="ns").dt.replace_time_zone("UTC").alias("timestamp"),
        )
        .with_columns(
            pl.col("timestamp").dt.convert_time_zone("America/New_York")
        )
    )


CLEANING_FUNCTIONS = {
    (PolygonMarket.OPTIONS, PolygonEndpoint.TRADES): clean_options_trades,
    (PolygonMarket.OPTIONS, PolygonEndpoint.MINUTES): clean_options_bars,
    (PolygonMarket.OPTIONS, PolygonEndpoint.DAY): clean_options_bars,
    (PolygonMarket.STOCKS, PolygonEndpoint.TRADES): clean_stock_trades,
    (PolygonMarket.STOCKS, PolygonEndpoint.MINUTES): clean_stock_trades,
    (PolygonMarket.STOCKS, PolygonEndpoint.DAY): clean_stock_trades,
}


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


def is_date_range_valid(start_date: dt.date, end_date: dt.date):
    """ Helper function checking if the start_date < end_date."""

    if not start_date <= end_date:
        raise ValueError("end_date must be greater than start_date")
    else:
        return True


class PolygonFileWrapper():
    def __init__(self, access_key=None, secret_key=None):
        self._base_bucket = 'flatfiles'
        self._endpoint_url = 'https://files.polygon.io'
        self._type = 's3'
        self._signature_version = 's3v4'
        self.access_key = access_key if access_key else os.environ["ACCESS_KEY"]
        self.secret_key = secret_key if secret_key else os.environ["SECRET_KEY"]

        self.s3 = self._init_session()

    def _get_date_range(self, start_date: dt.date, end_date: dt.date | None = None) -> pd.DatetimeIndex :
        """Helper function returning a formated date range from a start_date and end_date"""

        if not end_date:
            end_date = dt.date.today() - dt.timedelta(days=1)

        is_date_range_valid(start_date, end_date)
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

    def _get_prefix(self,
                    market: PolygonMarket,
                    endpoint: PolygonEndpoint,
                    year: Optional[int] = None,
                    month: Optional[int] = None
                    ) -> str:
        """Helper function to build the download path (prefix) needed after the bucket name."""
        year = format_year(year) if year else None
        month = format_month(month) if month else None

        path_root = f"{market.value}/{endpoint.value}"
        if not year and not month:
            return path_root
        elif year and not month:
            return f'{path_root}/{year}'
        elif year and month:
            return f'{path_root}/{year}/{month}'
        else:
            raise ValueError("Month cannot come without a year")

    @staticmethod
    def _get_date_from_key(key: str) -> str:
        """Helper function to return the date from a key"""
        return key.split('/')[-1].split('.')[0]

    def _create_object_key(self,
                           market: PolygonMarket,
                           endpoint: PolygonEndpoint,
                           year: int,
                           month: int,
                           day: int
                           ) -> str:

        """Create an object key respecting Polygon name policies."""
        year = format_year(year)
        month = format_month(month)
        day = format_day(day)
        return f'{market.value}/{endpoint.value}/{year}/{month}/{year}-{month}-{day}.csv.gz'

    def get_list_objects(self,
                         market: PolygonMarket,
                         endpoint: PolygonEndpoint,
                         year: Optional[int] = None,
                         month: Optional[int] = None,
                         verbose: bool = False
                         ) -> List[str]:
        """Download a list of object partial or total based on parameters year and month."""

        prefix = self._get_prefix(market, endpoint, year, month)
        print(f'[+] Listing from {self._base_bucket}/{prefix}')
        objects = self.s3.list_objects(Bucket=self._base_bucket, Prefix=prefix)
        contents = [obj.get('Key') for obj in objects.get('Contents', [])]
        if verbose:
            print(contents)
        return contents

    def _download_gzipped_csv(self, key: str) -> Optional[pl.DataFrame]:
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
                    print(f"404 - File not found for key - {key} ")
                    return None
                if error_code == '403':
                    print(f"403 - Forbidden for key - {key}")
                else:
                    print(f"Error in _download_parquet for date {key}: {e}")
                    raise

    def _download_single_key(self, key: str) -> Optional[pl.DataFrame]:
        """Download single flat file for a given key."""

        df = self._download_gzipped_csv(key)
        if df is None:
            return None
        return df

    def download_and_save_options(self,
                                  endpoint: PolygonEndpoint,
                                  start_date: dt.date,
                                  end_date: dt.date | None = None,
                                  dir: str | None = ".",
                                  clean: bool = True
                                  ):
        """Download options data and save to disk."""

        df = self.download_options(endpoint, start_date, end_date, clean)
        if df is not None and len(df) > 0:
            first_date = df.item(0, "timestamp").date()
            last_date = df.item(-1, "timestamp").date()
            fname = f"{first_date}_{last_date}.parquet" if first_date != last_date else f"{first_date}.parquet"
            df.write_parquet(os.path.join(dir, fname))

    def download_options(
            self,
            endpoint: PolygonEndpoint,
            start_date: dt.date,
            end_date: dt.date | None = None,
            clean: bool = True
        ) -> Optional[pl.DataFrame]:

        """Download options data for a given date range."""

        dfs_list = []
        date_range = self._get_date_range(start_date, end_date)

        for current_date in date_range:
            key = self._create_object_key(
                PolygonMarket.OPTIONS,
                endpoint,
                current_date.year,
                current_date.month,
                current_date.day
            )
            df = self._download_single_key(key)
            if clean and df is not None:
                cleaning_f = CLEANING_FUNCTIONS[(PolygonMarket.OPTIONS, endpoint)]
                df = cleaning_f(df)

            if df is not None:
                dfs_list.append(df)

        complete = pl.concat(dfs_list)
        return complete
    
    def download_and_save_stocks(self,
                                  endpoint: PolygonEndpoint,
                                  start_date: dt.date,
                                  end_date: dt.date | None = None,
                                  dir: str | None = ".",
                                  clean: bool = True
                                  ):
        """Download options data and save to disk."""

        df = self.download_stocks(endpoint, start_date, end_date, clean)
        if df is not None and len(df) > 0:
            first_date = df.item(0, "timestamp").date()
            last_date = df.item(-1, "timestamp").date()
            fname = f"{first_date}_{last_date}.parquet" if first_date != last_date else f"{first_date}.parquet"
            df.write_parquet(os.path.join(dir, fname))    
    
    def download_stocks(
            self,
            endpoint: PolygonEndpoint,
            start_date: dt.date,
            end_date: dt.date | None = None,
            clean: bool = True
        ) -> Optional[pl.DataFrame]:

        """Download stock data for a given date range."""

        dfs_list = []
        date_range = self._get_date_range(start_date, end_date)

        for current_date in date_range:
            key = self._create_object_key(
                PolygonMarket.STOCKS,
                endpoint,
                current_date.year,
                current_date.month,
                current_date.day
            )
            df = self._download_single_key(key)
            if clean and df is not None:
                cleaning_f = CLEANING_FUNCTIONS[(PolygonMarket.STOCKS, endpoint)]
                df = cleaning_f(df)

            if df is not None:
                dfs_list.append(df)

        complete = pl.concat(dfs_list)
        return complete    
