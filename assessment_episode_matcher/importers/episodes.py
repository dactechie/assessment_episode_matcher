from pathlib import Path
import logging
import pandas as pd
from assessment_episode_matcher.configs import episodes as EpCfg
from assessment_episode_matcher.data_config import EstablishmentID_Program
from assessment_episode_matcher.utils.dtypes import blank_to_today_str, convert_to_datetime
from assessment_episode_matcher.utils.df_ops_base import has_data
from assessment_episode_matcher.utils import io
from assessment_episode_matcher.setup.bootstrap import Bootstrap

# from utils.io import read_parquet, write_parquet

def prepare(ep_df1:pd.DataFrame, start_date:str, end_date:str) -> pd.DataFrame:
  processed_folder = Bootstrap.processed_dir

  ep_df = ep_df1[EpCfg.columns_of_interest].copy()
  ep_df['Program'] = ep_df['ESTABLISHMENT IDENTIFIER'].map(EstablishmentID_Program)
  
#  convert_to_datetime(atom_df['AssessmentDate'], format='%Y%m%d')
  ep_df[EpCfg.date_cols[0]] = convert_to_datetime(ep_df[EpCfg.date_cols[0]],  format='%d%m%Y'
                                                  , fill_blanks=False)
  ep_df[EpCfg.date_cols[1]] = convert_to_datetime(ep_df[EpCfg.date_cols[1]],  format='%d%m%Y')

  # ep_df[EpCfg.date_cols] = ep_df[EpCfg.date_cols] \
  #                           .apply(lambda x: x.apply(parse_date))
  ep_df.rename(columns=EpCfg.rename_columns
            , inplace=True)
  
  file_path =  processed_folder.joinpath(f"MDS_{start_date}-{end_date}_AllPrograms.parquet")
  
  io.write_parquet(ep_df, file_path)
  return ep_df


def import_data(eps_st:str, eps_end:str, prefix:str, suffix:str) -> pd.DataFrame:
  """
    Load processed episodes dataframe from disk
    If not available, load raw, process and save, and then return processed_df
    prefix: MDS
    suffix: AllPrograms
  """  
  processed_folder = Bootstrap.processed_dir
  source_folder =  Bootstrap.in_dir
  fname_eps =  f'{prefix}_{eps_st}-{eps_end}_{suffix}' #NSW_
  # fname_eps =  f'{source_folder}{filename}' #NSW_MDS_1jan2020-31dec2023.csv'#TEST_NSWMDS.csv'
  
  processed_df = io.read_parquet(processed_folder.joinpath(f"{fname_eps}.parquet"))
  if not(isinstance(processed_df, type(None)) or processed_df.empty):
    logging.debug("found & returning pre-processed parquet file.")
    return processed_df
  
  
  raw_df = io.read_csv_to_df(source_folder.joinpath(f"{fname_eps}.csv"), dtype=str)
  if not has_data(raw_df):
    logging.info("No Raw episode Data. Returning empty.")
    return raw_df
  
  raw_df.dropna(subset=['START DATE'], inplace=True)
  # TODO: log the dropped episodes
  raw_df['END DATE'] = raw_df['END DATE'].apply(lambda x: blank_to_today_str(x))

  processed_df = prepare(raw_df, eps_st, eps_end)
  return processed_df