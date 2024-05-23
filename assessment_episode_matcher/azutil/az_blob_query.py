import os
import logging
from typing import Any
from io import BytesIO, StringIO
import tempfile
import pandas as pd
from azure.storage.blob import BlobServiceClient #, BlobClient, ContainerClient
from assessment_episode_matcher.utils.environment import ConfigKeys, ConfigManager

# import mylogging

# logging = mylogging.get('azure.storage')

class AzureBlobQuery(object):
  def __init__(self):
      config = ConfigManager().config
    
      # self.connection_string = str(config.get(ConfigKeys.AZURE_STORAGE_CONNECTION_STRING,"Help"))
      self.connection_string = str(config.get(ConfigKeys.AZURE_BLOB_CONNECTION_STRING.value,"Help"))
      
      if self.connection_string == 'Help':
        logging.error("Blob Connection string not found.")
        self.connection_string = ""
        # st.warning("An error occurred while loading the data. Please try again later.")
        return None
      self.blob_service_client =  BlobServiceClient.from_connection_string(self.connection_string)


  def list_files(self, container_name: str, folder_path:str , prefix: str, suffix: str) -> list[str]:
      # blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
      container_client = self.blob_service_client.get_container_client(container_name)
      if folder_path:
        final_prefix = folder_path + "/" + prefix
      else:
        final_prefix = prefix
      blobs = container_client.list_blobs(name_starts_with=final_prefix)
      return [blob.name for blob in blobs if blob.name.endswith(suffix)]
      

  # @st.cache
  def load_data(self, container_name, blob_url):
      
      try:
        # blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_url)
        blob_data = blob_client.download_blob().readall()

        logging.debug(f"Loaded blob bytes of length {len(blob_data)}.")
   
        return BytesIO(blob_data)
      
      except Exception as e:
        # Log the exception
        logging.error(f"An error occurred while loading data from Blob Storage: {str(e)}")
        # You may want to display a user-friendly message in the Streamlit app
        # st.warning("An error occurred while loading the data. Please try again later.")
        return None       
      
  def _get_parquet(self, df:pd.DataFrame) -> bytes:
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file_path = os.path.join(temp_dir, "temp.parquet")
        df.to_parquet(temp_file_path, engine="pyarrow")

        # Read the Parquet file into memory
        with open(temp_file_path, "rb") as file:
            parquet_data = file.read()
            return parquet_data



  def write_data(self, container_name:str, blob_url:str
                 , data:pd.DataFrame) -> dict[str, Any]:
    
    # csv_buffer = StringIO()
    # data.to_csv(csv_buffer, index=False)
    

    # Define the blob client
    # container_name = "atom-matching"
    # blob_name = f"{container_name}/{blob_url}"
    blob_client = self.blob_service_client.get_blob_client(container=container_name
                                                      , blob=blob_url)
    pq_data = self._get_parquet(data)
    result_dict = blob_client.upload_blob(pq_data, overwrite=True)
    # Upload the CSV to blob storage
    # result_dict = blob_client.upload_blob(csv_buffer.getvalue()
    #                                       , overwrite=True)
    return result_dict 

    # return func.HttpResponse(
    #     "DataFrame stored in blob storage successfully.",
    #     status_code=200
    # )     
       
# data = load_data('path/to/yourfile.parquet')


