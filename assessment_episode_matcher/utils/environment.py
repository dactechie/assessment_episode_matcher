import os
from enum import Enum
from pathlib import Path
# from typing import Protocol
# from typing import TypedDict
from dotenv import load_dotenv


class ConfigKeys(Enum):
  REFRESH_ATOM_DATA = 'REFRESH_ATOM_DATA'
  TABLES_STORAGE_ENDPOINT_SUFFIX = 'TABLES_STORAGE_ENDPOINT_SUFFIX'
  TABLES_STORAGE_ACCOUNT_NAME = 'TABLES_STORAGE_ACCOUNT_NAME'
  AZURE_STORAGE_CONNECTION_STRING = 'AZURE_STORAGE_CONNECTION_STRING'
  AZURE_BLOB_CONNECTION_STRING ='AZURE_BLOB_CONNECTION_STRING'
  SURVEY_TABLE_NAME =  'SURVEY_TABLE_NAME'
  MATCHING_NDAYS_SLACK = 'MATCHING_NDAYS_SLACK'
  AZURE_BLOB_CONTAINER = 'AZURE_BLOB_CONTAINER'


class ConfigManager:
    _instance = None
    env:str
    env_config:dict

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            # cls._instance.env = 'local'
        return cls._instance
  
    @classmethod
    def setup(cls, root, env:str):
        cls.env = env
        env_file = Path(root) / f".env.{cls.env}"
        if  os.path.isfile(env_file):
            load_dotenv(env_file)

        cls.env_config = {key: value for key, value in os.environ.items()}
        
    @property
    def config(self):
        return self.env_config        
    # @classmethod
    # def get_new_config_value(cls):
    #     return cls.new_config_value