
from abc import ABC, abstractmethod
from typing import Optional
# from pathlib import Path
import pandas as pd
from assessment_episode_matcher.azutil.az_blob_query import AzureBlobQuery

class DataExporter(ABC):

  def __init__(self, config) -> None:
    self.config = config

  @abstractmethod
  def export_data(self, data_name:str, data:pd.DataFrame):
    pass


class CSVExporter(DataExporter):

  def export_data(self, data_name:str, data:pd.DataFrame):
    path = self.config.get("location")
    if not path:
      raise FileNotFoundError("CSVExporter:No file-path was passed in")
    
    data.to_csv(f"{path}{data_name}.csv", index=False)


class ParquetExporter(DataExporter):

  def export_data(self, data_name:str, data:pd.DataFrame):
    path = self.config.get("location")
    if not path:
      raise FileNotFoundError("ParquetExporter:No file-path was passed in")
    
    data.to_parquet(f"{path}{data_name}.parquet", index=False)
    

class AzureBlobExporter(DataExporter):
  blobClient:AzureBlobQuery

  def __init__(self, container_name:str, config:Optional[dict]=None) -> None:
    if config:
      super().__init__(config)
    self.container_name = container_name
    self.blobClient = AzureBlobQuery()

  def export_data(self, data_name:str, data:pd.DataFrame):
    full_path = data_name
    if hasattr(self, "config"):
      folder_path = self.config.get("location")
      if folder_path:
        full_path = f"{folder_path}/{data_name}"
  
    result = self.blobClient.write_data(container_name=self.container_name
                                        , blob_url=full_path
                                        ,data=data)
    print(result)
    


# class AuditExporter(DataExporter):
#   container_prefix = "audit-matching"

#   def __init__(self, config) -> None:
#     self.sink_config = config
    
#   def export_data(self, data):
#     pass


# class MatchedDataExporter(DataExporter):
#   container_prefix = "matched-data"

#   def __init__(self, config) -> None:
#     self.sink_config = config
    

#   def export_data(self, data):
#     pass




# class SurveyTxtExporter(DataExporter):
#   container_prefix = "SurveyTxt"

#   def __init__(self, config) -> None:
#     self.sink_config = config
    

#   def export_data(self, data):
#     pass


