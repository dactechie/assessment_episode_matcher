import logging
import pandas as pd
from configs import episodes as EpCfg
from data_config import EstablishmentID_Program
from utils.dtypes import blank_to_today_str, convert_to_datetime
from utils.df_ops_base import has_data
from utils import io
# from utils.io import read_parquet, write_parquet

def prepare(ep_df1:pd.DataFrame, start_date:str, end_date:str) -> pd.DataFrame:
  processed_folder = 'data/processed/'
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
  
  io.write_parquet(ep_df, f"{processed_folder}/MDS_{start_date}-{end_date}_AllPrograms.parquet")
  return ep_df


def import_data(eps_st:str, eps_end:str) -> pd.DataFrame:
  
  processed_folder = 'data/processed/'
  source_folder = 'data/in/'
  fname_eps =  f'MDS_{eps_st}-{eps_end}_AllPrograms' #NSW_
  # fname_eps =  f'{source_folder}{filename}' #NSW_MDS_1jan2020-31dec2023.csv'#TEST_NSWMDS.csv'

  processed_df = io.read_parquet(f"{processed_folder}{fname_eps}.parquet")
  if not(isinstance(processed_df, type(None)) or processed_df.empty):
    logging.debug("found & returning pre-processed parquet file.")
    return processed_df
  
  raw_df = io.read_csv_to_df( f"{source_folder}{fname_eps}.csv", dtype=str)
  if not has_data(raw_df):
    return raw_df
  
  raw_df.dropna(subset=['START DATE'], inplace=True)
  # TODO: log the dropped episodes
  raw_df['END DATE'] = raw_df['END DATE'].apply(lambda x: blank_to_today_str(x))

  processed_df = prepare(raw_df, eps_st, eps_end)
  return processed_df