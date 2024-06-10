
from assessment_episode_matcher.importers.main import BlobFileSource


def load_blob_config(container_name):
  config_file_source = BlobFileSource(container_name=container_name
                                            , folder_path=".")
  config = config_file_source.load_json_file(filename="configuration.json", dtype=str)
  # print(config)
  return config
