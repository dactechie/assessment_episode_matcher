import logging
from typing import Any
from io import BytesIO, StringIO
import pandas as pd
from azure.storage.blob import BlobServiceClient #, BlobClient, ContainerClient
from assessment_episode_matcher.utils.environment import ConfigKeys, ConfigManager
# import mylogging

# logging = mylogging.get('azure.storage')

class AzureBlobQuery(object):
  def __init__(self):
      config = ConfigManager().config
    
      # self.connection_string = str(config.get(ConfigKeys.AZURE_STORAGE_CONNECTION_STRING,"Help"))
      self.connection_string = str(config.get(ConfigKeys.AZURE_BLOB_CONNECTION_STRING,"Help"))
      
      if self.connection_string == 'Help':
        logging.error("Blob Connection string not found.")
        self.connection_string = ""
        # st.warning("An error occurred while loading the data. Please try again later.")
        return None

  # @st.cache
  def load_data(self, blob_url):
      
      try:
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        blob_client = blob_service_client.get_blob_client(container='reporting', blob=blob_url)
        blob_data = blob_client.download_blob().readall()

        logging.debug(f"Loaded blob bytes of length {len(blob_data)}.")
        # Assuming the blob_data is a parquet file
        # data = pd.read_parquet(BytesIO(blob_data))
        return BytesIO(blob_data)
      
      except Exception as e:
        # Log the exception
        logging.error(f"An error occurred while loading data from Blob Storage: {str(e)}")
        # You may want to display a user-friendly message in the Streamlit app
        # st.warning("An error occurred while loading the data. Please try again later.")
        return None       
      

  def write_data(self, df:pd.DataFrame, data_type:str) -> dict[str, Any]:
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Create a blob service client
    blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

    # Define the blob client
    container_name = "atom-matching"
    blob_name = f"{data_type}.csv"
    blob_client = blob_service_client.get_blob_client(container=container_name
                                                      , blob=blob_name)

    # Upload the CSV to blob storage
    result_dict = blob_client.upload_blob(csv_buffer.getvalue()
                                          , overwrite=True)
    return result_dict 

    # return func.HttpResponse(
    #     "DataFrame stored in blob storage successfully.",
    #     status_code=200
    # )     
       
# data = load_data('path/to/yourfile.parquet')


