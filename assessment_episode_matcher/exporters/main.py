
from abc import ABC, abstractmethod
from typing import Optional
# from pathlib import Path
import pandas as pd
from assessment_episode_matcher.azutil.az_blob_query import AzureBlobQuery
from assessment_episode_matcher.mytypes import CSVTypeObject

class DataExporter(ABC):

  def __init__(self, config) -> None:
    self.config = config

  @abstractmethod
  def export_dataframe(self, data_name:str, data:pd.DataFrame):
    pass


class CSVExporter(DataExporter):

  def export_dataframe(self, data_name:str, data:pd.DataFrame):
    path = self.config.get("location")
    if not path:
      raise FileNotFoundError("CSVExporter:No file-path was passed in")
    
    data.to_csv(f"{path}{data_name}.csv", index=False)


class ParquetExporter(DataExporter):

  def export_dataframe(self, data_name:str, data:pd.DataFrame):
    path = self.config.get("location")
    if not path:
      raise FileNotFoundError("ParquetExporter:No file-path was passed in")
    
    data.to_parquet(f"{path}{data_name}.parquet", index=False)


class LocalFileExporter(DataExporter):

  def __init__(self, config) -> None:
    self.config = config

  """
  similar to azutil.az_blob_query.write_data()
  """
  def export_dataframe(self, data_name:str, data:pd.DataFrame):
    if data_name[-3:] =='csv':
      p = CSVExporter(self.config)    
    else:
      p = ParquetExporter(self.config)
    
    p.export_dataframe(data_name, data)
    

# class ConstructorRequirementError(Exception):
#   msg:str
#   source:str

#   def __init__(self, msg: str, source:str="") -> None:
#     self.msg = msg
#     self.source = source

    # super().__init__(*args)
  

class AzureBlobExporter(DataExporter):
  blobClient:AzureBlobQuery

  def __init__(self, container_name:str, config:Optional[dict]=None) -> None:
    if config:
      super().__init__(config)
      # if not hasattr(self.config, 'container_name'):
      #   raise ConstructorRequirementError(
      #       msg="Must pass container_name to create a Blob Exporter"
      #     , source="AzureBlobExporter")
      # self.container_name = self.config['container_name']
    self.container_name = container_name
    self.blobClient = AzureBlobQuery()

  def export_dataframe(self, data_name:str, data:pd.DataFrame):   
    full_path = data_name
    if hasattr(self, "config"):
      folder_path = self.config.get("location")
      if folder_path:
        full_path = f"{folder_path}/{data_name}"
  
    result = self.blobClient.write_dataframe(container_name=self.container_name
                                        , blob_url=full_path
                                        ,data=data)    
    return result
    
  def export_csv(self, data_name:str, data:CSVTypeObject):   
    full_path = data_name
    if hasattr(self, "config"):
      folder_path = self.config.get("location")
      if folder_path:
        full_path = f"{folder_path}/{data_name}"
  
    result = self.blobClient.write_csv(container_name=self.container_name
                                        , blob_url=full_path
                                        ,data=data)    
    return result

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


