import logging
from datetime import datetime
import pandas as pd
from assessment_episode_matcher.utils import io
from assessment_episode_matcher.mytypes import Purpose
from assessment_episode_matcher import data_config
from assessment_episode_matcher.utils.df_ops_base import has_data


def filter_by_purpose(df:pd.DataFrame, filters:dict|None) -> pd.DataFrame:
  if not filters:
     return df
  return df[df ['Program'].isin(filters['Program'])]



def get_filename(prefix:str ,start:str, end:str, suffix:str="", sep:str ="_"):
  
  period_range = f"{start}-{end}"
  fname =  f"{prefix}{sep}{period_range}{sep}{suffix}"
  return fname


def import_data(asmt_st:str, asmt_end:str
                ,purpose:Purpose, refresh:bool=True
                ) -> pd.DataFrame:
  
  """
    1. If processed file for the period exists:
        if asking to be refreshed, go to #2
        else return file

    2. check if raw.parquet file exists
        Raw_Exists:
          - if refresh , then refresh raw data

        if timestamp on raw.parquet is more recent than processed.parquet,
          (or same period processed.parquet does not exist)
          call process(raw_df) and write to /processed

    3. Raw Does NOT exist:

  """
  processed_folder = 'data/processed'
  preprocessed_folder = 'data/in'
  filters = data_config.ATOM_DB_filters[purpose]
  
  # period_range = f"{asmt_st}-{asmt_end}"
  fname = get_filename("ATOM", asmt_st
                       , asmt_end, "AllPrograms")
                       
  processed_filepath = f"{processed_folder}/{fname}"
  
  logging.info(f"Attempting to load processed data from {processed_filepath}")

  processed_df, best_start_date, best_end_date = \
    io.load_for_period(processed_folder
                          , asmt_st
                          , asmt_end
                          ,prefix="ATOM_"
                          )
  if best_start_date and best_end_date:
    fname = get_filename("ATOM", best_start_date.strftime("%Y%m%d")
                        , best_end_date.strftime("%Y%m%d"), "AllPrograms")
    
  # TO DO check if recent or needs to be refreshed

  if isinstance(processed_df, type(None)) or processed_df.empty:
    # logging.debug("found & returning processed parquet file.")  
    print ("Loading from Raw.parquet")
    
    raw_df, best_start_date, best_end_date = \
      io.load_for_period(preprocessed_folder
                          , asmt_st
                          , asmt_end
                          ,prefix="ATOM_"
                          )
    # io.read_parquet(f"{preprocessed_folder}/{fname}.parquet")
    # TO DO check if recent or needs to be refreshed
    if not(isinstance(raw_df, type(None)) or raw_df.empty):
      processed_df = io.process_assment(raw_df)
      # use the same start and end date as the raw filename
      if best_start_date and best_end_date:
        # period_range = f"{best_start_date.strftime("%Y%m%d")}-{best_end_date.strftime("%Y%m%d")}"
        fname = get_filename("ATOM", best_start_date.strftime("%Y%m%d")
                        , best_end_date.strftime("%Y%m%d"), "AllPrograms")

      processed_filepath = f"{processed_folder}/{fname}"
      io.write_parquet(processed_df,f"{processed_filepath}.parquet")
    else:
        logging.info("Raw file doesn't exist either. load from DB. " \
               + "\n Hardcoding 20160701 as start date and today as end date.")
        today = datetime.now()
        today_str = today.strftime('%Y%m%d')
        today_int = int(today_str)
        
        raw_df = io.get_from_source("ATOM", 20160701
                                    , today_int, filters=filters)     
        fname =   get_filename("ATOM", asmt_st
                       , today_str, "AllPrograms")        
        io.write_parquet(raw_df,f"{preprocessed_folder}/{fname}.parquet")
        processed_df = io.process_assment(raw_df)
        # no need to refresh
        return processed_df
      
  #   # get the last modified date of the file
  #   # get the last modified date of ATOMs in the period of interest (assessmentDate)
  #   # if the last modified date of the file is after the last modified date of ATOMs, then return the processed_df
  #   # else query Azure data to get the latest ATOMs and merge them into the processed_df and save to disk to override
    
  #   # TODO :

  if not refresh:
    logging.info("Returning cached (processed) assessment data.")
    return processed_df
  # Data has been loaded in processed_df, but needs refresh. 
  # refresh only processed file?
  processed_filepath = f"{processed_folder}/{fname}" 
  fetched_df, was_refreshed = io.get_data('ATOM'
                    ,int(asmt_st), int(asmt_end)
                    , processed_df
                    , f"{processed_filepath}.parquet"
                    , filters=filters                
                    , refresh=refresh)
  
  # if not has_data(atom_df):
  #    logging.info("Returning Empty Dataframe.")
  #    return pd.DataFrame()
  
  if was_refreshed:
    today = datetime.today()
    today_str = today.strftime("%Y%m%d")
    fname = get_filename("ATOM", asmt_st
                       , today_str, "AllPrograms")
    
    io.write_parquet(fetched_df, f"{processed_folder}/{fname}.parquet")
    processed_df = fetched_df
    logging.info("Caching and returning refreshed assessment data.")

  return processed_df

  
if __name__ == '__main__':
  from assessment_episode_matcher.utils.environment import ConfigManager
  ConfigManager.setup('dev')
  cfg = ConfigManager().config
  asmt_st, asmt_end = "20220101", "20240411"
  preprocessed_folder = 'data/in'
   
  
  period_range = f"{asmt_st}-{asmt_end}"
  fname =  f'ATOM_{period_range}_AllPrograms'
  # processed_filepath = f"{processed_folder}/{fname}"
  
  df = import_data(asmt_st, asmt_end, Purpose.NADA )
  # df = io.load_for_period(preprocessed_folder
  #                         , asmt_st
  #                         , asmt_end
  #                         ,prefix="ATOM_"
  #                         )
  
  print(df)

# def get_filename(data_type:DataType, purpose:Optional[Purpose]) -> str:
#   if data_type == DataType.ATOM:
#     return f"./data/raw/atom_{purpose}.csv"
  
#   filepath = f"./data/processed/atom_{purpose}_{period_range}.parquet"
#   return filepath


# if there is no pre-processed data, then extract and process it
# Processing it includes:
#   - dropping fields that are not needed
#   - limiting to clients who have at least 1 assessment in the period of interest
#   - limiting to clients who have completed at least 3 surveys
#   - converting data types
#   - normalizing data (if there are nested data structures like SurveyData)
#   - caching the processed version

# Note:  purpose =matching :  may be ACT also 


# """
#   returns processed (cached) or un_processed data 
#   if returning processed data, the 2nd param is True
# """
# def extract_atom_data(extract_start_date, extract_end_date                              
#                             , purpose:Purpose) -> tuple[pd.DataFrame, bool] :
#   # warnings = None
#   is_processed = False
#   xtr_start_str, xtr_end_str = get_period_range(extract_start_date, extract_end_date)
#   period_range = f"{xtr_start_str}-{xtr_end_str}"
#   processed_filepath = f"./data/processed/atom_{purpose}_{period_range}.parquet"
  
#   logging.info(f"Attempting to load processed data from {processed_filepath}")

#   processed_df = io.read_parquet(processed_filepath)
  
#   if not(isinstance(processed_df, type(None)) or processed_df.empty):
#     logging.debug("found & returning pre-processed parquet file.")
#     # TODO chec if the timestamp on this the file is recent
#     # get the last modified date of the file
#     # get the last modified date of ATOMs in the period of interest (assessmentDate)
#     # if the last modified date of the file is after the last modified date of ATOMs, then return the processed_df
#     # else query Azure data to get the latest ATOMs and merge them into the processed_df and save to disk to override
#     return processed_df, True
  
#   logging.info("No processed data found, loading from raw data.")
  
  
#   # cache data for all programs
#   raw_df  = io.get_data('ATOM'
#                     ,int(xtr_start_str), int(xtr_end_str)
#                     , f"./data/in/atom_{period_range}.parquet"
#                     ,filters=None
#                     , cache=True)
  
#   if not has_data(raw_df):
#      return pd.DataFrame(), is_processed
     
 
#   raw_df = filter_by_purpose(raw_df, ATOM_DB_filters[purpose])
  
#   if isinstance(raw_df, type(None)) or raw_df.empty:
#     logging.error("No data found. Exiting.")
#     return pd.DataFrame(), is_processed
  
#   raw_df['AssessmentDate'] = convert_to_datetime(raw_df['AssessmentDate'], format='%Y%m%d'
#                                                  , fill_blanks=False)
  
#   return raw_df, is_processed

#   # TODO: getting an error when caching processed results
#     # processed_df = prep_dataframe(raw_df, prep_type=purpose) # only one filter: PDCSubstanceOrGambling has to have a value
    
#   # if active_clients_start_date and active_clients_end_date:
#   #   processed_df = limit_clients_active_inperiod(processed_df, active_clients_start_date, active_clients_end_date)
    
#   # cache the processed data
#   # processed_df.to_parquet(f"{processed_filepath}")
#   # try:
#   #   write_parquet(processed_df, processed_filepath) # don't force overwrite
#   #   logger.info(f"Done saving processed data to {processed_filepath}")
#   # except ArrowTypeError as re:
#   #   logger.error(f"ArrowTypeError: {re}. unable to save parquet file.")     
#   # except Exception as ae:
#   #   logger.error(f"ArrowTypeError: {ae}. unable to save parquet file.")    
#   # finally: