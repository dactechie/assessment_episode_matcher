
from assessment_episode_matcher.importers.main import BlobFileSource


def load_blob_config(container: str) -> dict:
  """
  Load configuration from a JSON file in an Azure Blob Storage container.

  Parameters:
  container (str): The name of the Azure Blob Storage container.

  Returns:
  dict: The loaded configuration.

  Raises:
  FileNotFoundError: If the configuration file does not exist in the container.
  """
  try:
    config_file_source = BlobFileSource(container_name=container, folder_path=".")
    config = config_file_source.load_json_file(filename="configuration.json", dtype=str)
    return config
  except FileNotFoundError:
    raise FileNotFoundError(f"Configuration file not found in container {container}")