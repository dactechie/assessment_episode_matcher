import logging
import pandas as pd
from assessment_episode_matcher.importers.main import FileSource
from assessment_episode_matcher.utils import io

#NADA
def import_data(start_date:str, end_date:str
                , file_source:FileSource
                    , prefix:str, suffix:str) -> tuple [pd.DataFrame, str|None]:

  file_path, best_start_date, best_end_date = io.load_for_period(
                           file_source
                          , start_date
                          , end_date
                          , prefix=f"{prefix}"
                          , suffix=f"{suffix}.csv"
                          )
  if file_path:
    processed_df = file_source.load_csv_file_to_df(file_path, str)
    # processed_df = io.read_parquet_to_df(Path(file_path))
    if not(isinstance(processed_df, type(None)) or processed_df.empty):
      logging.debug(f"found & returning parquet file. {file_path}")
      return processed_df, None
    
  logging.error(f"no nada-indexed file was found at {file_path}")
  return pd.DataFrame(), file_path


# def main():
#   # "%Y%m%d"
#   st_dt, end_dt= "20240101", "20240331"
#   file_source = BlobFileSource("atom-matching",folder_path="NADA")
#   df , fname = import_data(st_dt, end_dt, file_source
#               , prefix=f"{st_dt}-{end_dt}"
#               , suffix=".parquet")
#   print(df)

# if __name__ =='__main__':
#   main()
