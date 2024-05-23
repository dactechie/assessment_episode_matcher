import os
from abc import ABC, abstractmethod
import pandas as pd

from assessment_episode_matcher.azutil.az_blob_query import AzureBlobQuery

class FileSource(ABC):
    def __init__(self, path: str):
        self.path = path

    @abstractmethod
    def list_files(self, prefix: str, suffix: str) ->  list[str]:
        pass

    @abstractmethod
    def load_csv_file_to_df(self, filepath: str, dtype) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def load_parquet_file_to_df(self, filepath: str) -> pd.DataFrame:
        pass
    
    # def get_full_filepath(self) -> str:
        
       

class LocalFileSource(FileSource):    
    # def __init__(self, path: str):
    #     self.path = path

    def list_files(self, prefix: str, suffix: str) -> list[str]:
        files = os.listdir(self.path)
        return [file for file in files if file.startswith(prefix) and file.endswith(suffix)]

   
    def load_csv_file_to_df(self, filepath: str, dtype) -> pd.DataFrame:
        full_path = os.path.join(self.path, filepath)
        if os.path.isfile(full_path):
            return pd.read_csv(full_path, dtype=dtype)
        else:
            raise FileNotFoundError(f"File not found: {full_path}")

    def load_parquet_file_to_df(self, filepath: str) -> pd.DataFrame:
        full_path = os.path.join(self.path, filepath)
        if os.path.isfile(full_path):
            return pd.read_parquet(full_path)
        else:
            raise FileNotFoundError(f"File not found: {full_path}")
        

class BlobFileSource(FileSource):
    
    blobClient:AzureBlobQuery

    def __init__(self, container_name: str, folder_path:str=""):
        self.container_name = container_name
        self.folder_path = folder_path
        self.blobClient = AzureBlobQuery()

    def list_files(self, prefix: str, suffix: str) -> list[str]:
        files = self.blobClient.list_files(self.container_name,
                                           self.folder_path,
                                           prefix, suffix)
        
        return files
    
    def load_csv_file_to_df(self, filename: str, dtype)-> pd.DataFrame:
        
        blob_bytes = self.blobClient.load_data(self.container_name,blob_url=filename)
        if blob_bytes:
            return pd.read_csv(blob_bytes, dtype=dtype)
        else:
            filepath = f"{self.container_name}/{filename}"
            raise ValueError(f"Failed to load blob data from URL: {filepath}")        


    def load_parquet_file_to_df(self, filename: str) -> pd.DataFrame:
        
        blob_bytes = self.blobClient.load_data(self.container_name, blob_url=filename)
        if blob_bytes:
            return pd.read_parquet(blob_bytes)
        else:
            filepath = f"{self.container_name}/{filename}"
            raise ValueError(f"Failed to load blob data from URL: {filepath}")  