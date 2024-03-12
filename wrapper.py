import boto3
import os
from botocore.config import Config
import pandas as pd



class PolygonFileWrapper():
    def __init__(self):

        self.access_key = os.environ["ACCESS_KEY"]
        self.secret_key = os.environ["SECRET_KEY"]
        self.instrument = os.environ["INSTRUMENT"]
        self.endpoint = os.environ["ENDPOINT"]
        self.datadir = os.environ["DATADIR"]
        self.download_path = f'{self.instrument}/{self.endpoint}'

        self._base_bucket = 'flatfiles'
        self._endpoint_url = 'https://files.polygon.io'
        self._type = 's3'
        self._signature_version = 's3v4'


        self._instrument = self._get_instrument()
        self.s3 = self._init_session()

    def _get_instrument(self):
        _instr = self.instrument.split('_')[1]
        if _instr in ['options','stocks']:
            return _instr
        else:
            raise ValueError(f'Instrument should be options or _stocks. Now it is {_instr}.')

    def _format_year(self,year):
        # check that year is an int and 4 long and before 2000 and below 2100. If yes return year. Else raise error
        if isinstance(year, int) and 2000 <= year < 2100:
            return year
        else:
            raise ValueError("Year must be an integer between 2000 and 2099 inclusive")

    def _format_month(month):
        # Check that month is an integer below 12. If yes, return a string with 2 characters like 1 -> 01. Else raise error
        if isinstance(month, int) and 1 <= month <= 12:
            return f"{month:02}"
        else:
            raise ValueError("Month must be an integer between 1 and 12 inclusive")

    def _format_day(day):
        # Check that day is an integer below 31. If yes, return a string with 2 characters like 1 -> 01. Else raise error
        if isinstance(day, int) and 1 <= day <= 31:
            return f"{day:02}"
        else:
            raise ValueError("Day must be an integer between 1 and 31 inclusive")            

    def _init_session(self):
        # Initialize a session using your credentials
        session = boto3.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

        # Create a client with your session and specify the endpoint
        s3 = session.client(
            self._type,
            endpoint_url= self.endpoint_url,
            config=Config(signature_version=self._signature_version),
        )
        return s3
    
    def get_download_path(self,year=None,month=None):
        # Build the download path (prefix) needed after the bucket name
        year = self._format_year(year)
        month = self._format_month(month)

        if not year and not month:
            return self.download_path
        elif year :
            return f'{self.download_path}/{year}'
        elif year and month:
            return f'{self.download_path}/{year}/{month}'
        else:
            raise ValueError("Month cannot come without a year")
        
    def download_list_objects(self,year=None,month=None,verbose=False):
        # Download a list of object partial or total based on parameters year and month
        download_path = self.get_download_path(year=None,month=None)
        print(f'[+] Listing from {self._base_bucket}/{download_path}')
        objects = self.s3.list_objects(Bucket=self._base_bucket, Prefix=download_path)
        contents = [obj.get('Key') for obj in objects.get('Contents',[])]
        if verbose:
            print(contents)
        return contents
    
    def create_object_key(self,year,month,day):
        # Create an object key respecting Polygon name policies 
        year = self._format_year(year)
        month = self._format_month(month)
        day = self._format_day(day)        
        return f'{self.instrument}/{self.endpoint}/{year}/{month}/{year}-{month}-{day}.csv.gz'
    
    def _download_file_from_object_key(self,object_key):
        # Helper function to download a file from the object key 
        filename = object_key.split('/')[-1]
        filepath = f"{self.datadir}/{self._instrument}/{filename}.csv.gz"

        self.s3.download_file(self._base_bucket,object_key,filepath)
        return True
    
    def download_history(self,year=None,month=None):
        # Download from a list of objects defined by year and month
        list_objects = self.get_list_objects(year,month)
        for _object in list_objects:
            self._download_file_from_object_key(_object)
        return True

    def download_file(self,year,month,day):
        # Download from year month and day passed by the user
        _object = self.create_object_key(year,month,day)
        self._download_file_from_object_key(_object)
        return True


    

    
if __name__ == '__main__':
    wrapper = PolygonFileWrapper()
    files = wrapper.get_list_files()
    print(files)
        

        
    

        




    

        

