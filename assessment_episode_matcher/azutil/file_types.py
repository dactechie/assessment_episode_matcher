import os
from abc import ABC, abstractmethod
from typing import Any
# from enum import Enum, auto
from io import StringIO
import tempfile
import pandas as pd

# class BlobFileType(Enum):
#    CSV =  auto()
#    PARQUET = auto()

class BlobFilePrepper(ABC):
  # file_type: BlobFileType
  
  @abstractmethod
  def get_file_for_blob(self, data:pd.DataFrame) -> Any:
    pass   
   

class BlobParquetFilePrepper(BlobFilePrepper):
  
  def get_file_for_blob(self, data:pd.DataFrame):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file_path = os.path.join(temp_dir, "temp.parquet")
        data.to_parquet(temp_file_path, engine="pyarrow")

        # Read the Parquet file into memory
        with open(temp_file_path, "rb") as file:
            parquet_data = file.read()
            return parquet_data
        

class BlobCSVFilePrepper(BlobFilePrepper):
  
  def get_file_for_blob(self, data:pd.DataFrame):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()
