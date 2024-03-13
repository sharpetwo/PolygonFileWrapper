from typing import Optional, List
import datetime as dt
from io import BytesIO
import gzip
import os
from enum import Enum 

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import polars as pl

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

class PolygonFileWrapper():
    def __init__(self, polygon_market=None, polygon_endpoint=None, access_key=None, secret_key=None, datadir='.'):
        self._base_bucket = 'flatfiles'
        self._endpoint_url = 'https://files.polygon.io'
        self._type = 's3'
        self._signature_version = 's3v4'
        self._env_market = polygon_market if polygon_market else os.environ["POLYGON_MARKET"]
        self._env_endpoint = polygon_endpoint if polygon_endpoint else os.environ["POLYGON_ENDPOINT"]

        self.polygon_market = self._get_polygon_market()
        self.polygon_endpoint = self._get_polygon_endpoint()

        self.access_key = access_key if access_key else os.environ["ACCESS_KEY"]
        self.secret_key = secret_key if secret_key else os.environ["SECRET_KEY"]
        self.datadir = os.environ["DATADIR"] if os.environ["DATADIR"] else datadir



        self.download_path = f'{self.polygon_market}/{self.polygon_endpoint}'
        self.s3 = self._init_session()


    def _get_polygon_market(self) -> str:
        """Get the market value from the environment variable."""

        env_to_enum = {
            "OPTIONS": PolygonMarket.OPTIONS,
            "STOCKS": PolygonMarket.STOCKS,
            "CRYPTO": PolygonMarket.CRYPTO,
            "FOREX": PolygonMarket.FOREX,
            "INDEX": PolygonMarket.INDEX
        }
        # Get the enum value, defaulting to None if not found
        market_enum = env_to_enum.get(self._env_market.upper(), None)
        if market_enum:
            return market_enum.value
        else:
            raise ValueError(f"Invalid POLYGON_MARKET value: {self._env_market}")    


    def _get_polygon_endpoint(self) -> str:
        """Get the endpoint value from the environment variable."""


        env_to_enum = {
            "DAY": PolygonEndpoint.DAY,
            "MINUTES": PolygonEndpoint.MINUTES,
            "QUOTES": PolygonEndpoint.QUOTES,
            "TRADES": PolygonEndpoint.TRADES
        }
        # Get the enum value, defaulting to None if not found
        market_enum = env_to_enum.get(self._env_endpoint.upper(), None)
        if market_enum:
            return market_enum.value
        else:
            raise ValueError(f"Invalid POLYGON_ENDPOINT value: {self._env_endpoint}")

    @staticmethod
    def _format_year(year: int) -> int:
        """Format the year value."""
        if isinstance(year, int) and 2000 <= year < 2100:
            return year
        else:
            raise ValueError("Year must be an integer between 2000 and 2099 inclusive")

    @staticmethod
    def _format_month(month: int) -> str:
        """Format the month value."""
        if isinstance(month, int) and 1 <= month <= 12:
            return f"{month:02}"
        else:
            raise ValueError("Month must be an integer between 1 and 12 inclusive")

    @staticmethod
    def _format_day(day: int) -> str:
        """Format the day value."""

        if isinstance(day, int) and 1 <= day <= 31:
            return f"{day:02}"
        else:
            raise ValueError("Day must be an integer between 1 and 31 inclusive")

    def _init_session(self) -> boto3.client:
        """Initialize the S3 session using the provided credentials and configuration."""
        session = boto3.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

        # Create a client with your session and specify the endpoint
        s3 = session.client(
            self._type,
            endpoint_url= self._endpoint_url,
            config=Config(signature_version=self._signature_version),
        )
        return s3

    def get_prefix(self, year: Optional[int] = None, month: Optional[int] = None) -> str:
        """Build the download path (prefix) needed after the bucket name."""
        year = self._format_year(year) if year else None
        month = self._format_month(month) if month else None

        if not year and not month:
            return self.download_path
        elif year and not month:
            return f'{self.download_path}/{year}'
        elif year and month:
            return f'{self.download_path}/{year}/{month}'
        else:
            raise ValueError("Month cannot come without a year")


    def get_list_objects(self, year: Optional[int] = None, month: Optional[int] = None, verbose: bool = False) -> List[str]:
        """Download a list of object partial or total based on parameters year and month."""
        prefix = self.get_prefix(year, month)
        print(f'[+] Listing from {self._base_bucket}/{prefix}')
        objects = self.s3.list_objects(Bucket=self._base_bucket, Prefix=prefix)
        contents = [obj.get('Key') for obj in objects.get('Contents', [])]
        if verbose:
            print(contents)
        return contents

    def create_object_key(self, year: int, month: int, day: int) -> str:
        """Create an object key respecting Polygon name policies."""
        year = self._format_year(year)
        month = self._format_month(month)
        day = self._format_day(day)
        return f'{self.download_path}/{year}/{month}/{year}-{month}-{day}.csv.gz'

    def _get_filepath_parquet(self, object_key: str) -> str:
        """Get the file path for the parquet file based on the object key."""
        filename = object_key.split('/')[-1].split('.')[0]
        return f"{self.datadir}/{self._env_market.lower()}/{filename}.parquet"

    def _download_parquet(self, key: str) -> Optional[pl.DataFrame]:
        """Download a parquet file from S3 and return it as a DataFrame."""
        with BytesIO() as data:
            try:
                self.s3.download_fileobj(self._base_bucket, key, data)
            except ClientError:
                # Couldn't find a file for a given key
                return None
            data.seek(0)
            csv_file = gzip.decompress(data.read())
            df = pl.read_csv(csv_file)
            return df
        
    def _clean_options_df(self, df: pl.DataFrame) -> pl.DataFrame:
        """Basic data cleaning for a DataFrame containing options trades."""


        ## This break if sip_timestamp not in df - which is the case for /stocks/minutes

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


        ## This break if sip_timestamp not in df - which is the case for /stocks/minutes

        return (
            df
            .with_columns(
                pl.from_epoch(pl.col("window_start"), time_unit="ns").dt.replace_time_zone("UTC").alias("timestamp"),
            )
            .with_columns(
                pl.col("timestamp").dt.convert_time_zone("America/New_York")
            )
        )          

    def clean_df(self, df : pl.DataFrame) ->  pl.DataFrame: 
        """ Basic mapper to clean the dataset based on the dataformat inputed from polygon_market"""
        if self._env_market.lower() == 'stocks':
            return self._clean_stocks_df(df)
        elif self._env_market.lower() == 'options':
            return self._clean_options_df(df)
            
    def download_from_list_objects(self, year: Optional[int] = None, month: Optional[int] = None
                                   , partition: bool = True, clean: bool = False, save_disk: bool = False) -> Optional[pl.DataFrame]:
        """Download data from a list of objects defined by year and month."""
        dfs_per_day = []
        list_objects = self.get_list_objects(year, month)
        for obj in list_objects:
            df = self._download_parquet(obj)
            if df is None:
                continue

            # If 'clean' is True, clean the df
            if clean:
                df = self.clean_df(df)

            # Save the df to disk if 'partition' is True
            if partition:
                filepath = self._get_filepath_parquet(obj)
                print(f"[+] Saving partition at: {filepath}")
                df.write_parquet(filepath, compression='snappy')

            dfs_per_day.append(df)


        if not dfs_per_day:
            print('[+] WARNING - no data downloaded from list objects')
            return None

        df = pl.concat(dfs_per_day)
        if save_disk:
            filepath = f"{self.datadir}/{self._env_market}/{self._env_endpoint}.parquet"
            df.write_parquet(filepath)

        return df

    def download_single_file(self, year: int, month: int, day: int, save_disk: bool = True) -> Optional[pl.DataFrame]:
        """Download data from a single file specified by year, month, and day."""
        obj = self.create_object_key(year, month, day)
        df = self._download_parquet(obj)

        if df is None:
            return None
        elif save_disk: 
            filepath = self._get_filepath_parquet(obj)
            df.write_parquet(filepath,compression='snappy')
            return df 


    def download_trades_parquet(self, start_date: dt.date, end_date: dt.date) -> pl.DataFrame | None:
        """Fetch trades for a given instrument and date range.

        Ignores weekends and holidays.

        Returns a single DataFrame with all the data combined.

        """
        dfs_per_day = []
        current_date = start_date
        while current_date <= end_date:
            key = self.create_object_key(current_date.year, current_date.month, current_date.day)
            df = self._download_parquet(key)
            if df is not None:
                dfs_per_day.append(self._clean_options_df(df))
            current_date += dt.timedelta(days=1)

        if dfs_per_day:
            return pl.concat(dfs_per_day)


if __name__ == '__main__':
    wrapper = PolygonFileWrapper()
    files = wrapper.get_list_files()
    print(files)
