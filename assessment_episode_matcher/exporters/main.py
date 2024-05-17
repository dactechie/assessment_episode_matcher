
from abc import ABC, abstractmethod
# from pathlib import Path
import pandas as pd
from azutil.az_blob_query import AzureBlobQuery

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

    
class AzureBlobExporter(DataExporter):
  blobClient:AzureBlobQuery

  def __init__(self, config) -> None:
    super().__init__(config)
    self.blobClient = AzureBlobQuery()

  def export_data(self, data_name:str, data:pd.DataFrame):
    result = self.blobClient.write_data(data, data_name)
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


