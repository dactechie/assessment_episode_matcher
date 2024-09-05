import logging
import pandas as pd
from assessment_episode_matcher.configs import episodes as EpCfg
from assessment_episode_matcher.importers.main import FileSource
from assessment_episode_matcher.utils.dtypes import blank_to_today_str, convert_to_datetime
from assessment_episode_matcher.utils.df_ops_base import has_data
from assessment_episode_matcher.utils import io
# from assessment_episode_matcher.setup.bootstrap import Bootstrap

# from utils.io import read_parquet, write_parquet
def get_cols_of_interest(df_columns) -> list[str]:
  cols = [] 
  for c in EpCfg.columns_of_interest:
    if c in df_columns:
      cols.append(c)
    else:
      logging.warn(f"Skipping Column from MDS dataset: {c}")
  return cols
  

def prepare(ep_df1:pd.DataFrame, config) -> pd.DataFrame:
  # processed_folder = Bootstrap.get_path("processed_dir")
  cols = get_cols_of_interest(ep_df1.columns)
  ep_df = ep_df1[cols].copy()
  map_establishmentID_program = config.get("EstablishmentID_Program")
  if not map_establishmentID_program:
    raise Exception( " No Establishment ID - program mapping in configuration")
  ep_df['Program'] = ep_df['ESTABLISHMENT IDENTIFIER'].map(map_establishmentID_program)
  
#  convert_to_datetime(atom_df['AssessmentDate'], format='%Y%m%d')
  ep_df[EpCfg.date_cols[0]] = convert_to_datetime(ep_df[EpCfg.date_cols[0]],  format='%d%m%Y'
                                                  , fill_blanks=False)
  ep_df[EpCfg.date_cols[1]] = convert_to_datetime(ep_df[EpCfg.date_cols[1]],  format='%d%m%Y')

  # ep_df[EpCfg.date_cols] = ep_df[EpCfg.date_cols] \
  #                           .apply(lambda x: x.apply(parse_date))
  ep_df.rename(columns=EpCfg.rename_columns
            , inplace=True)
  
  # file_path =  processed_folder.joinpath(f"MDS_{start_date}-{end_date}_AllPrograms.parquet")
  
  # io.write_parquet(ep_df, file_path)
  return ep_df



def import_data(eps_st:str,  eps_end:str, file_source:FileSource
                    , prefix:str, suffix:str, config:dict) -> tuple  [pd.DataFrame, str|None]:
                
                 
  """
    Load processed episodes dataframe from disk
    If not available, load raw, process and save, and then return processed_df
    prefix: MDS
    suffix: AllPrograms
  """  

  file_path, best_start_date, best_end_date = io.load_for_period(
                         file_source 
                          , eps_st
                          , eps_end
                          ,prefix=f"{prefix}_"
                           , suffix=f"{suffix}.csv"
                          )
  if not file_path:
    raise FileNotFoundError(f"No MDS file {prefix}_{eps_st}-{eps_end}_{suffix} was found.")
  
  raw_df = file_source.load_csv_file_to_df(file_path, dtype=str)
  # raw_df = io.read_csv_to_df(Path(file_path), dtype=str)
  if not has_data(raw_df):
    logging.info(f"No Raw episode Data. Returning empty. {file_path}")
    return raw_df, None
  
  raw_df.dropna(subset=['START DATE'], inplace=True)
  # TODO: log the dropped episodes
  raw_df['END DATE'] = raw_df['END DATE'].apply(lambda x: blank_to_today_str(x))

  processed_df = prepare(raw_df, config)
  return processed_df, file_path
