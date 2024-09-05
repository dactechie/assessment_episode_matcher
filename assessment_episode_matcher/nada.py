import os
import logging
import pandas as pd

from assessment_episode_matcher import project_directory
from assessment_episode_matcher.configs import load_blob_config

from assessment_episode_matcher.setup.bootstrap import Bootstrap
from assessment_episode_matcher.utils.environment import ConfigKeys
from assessment_episode_matcher.exporters.main import AzureBlobExporter
from assessment_episode_matcher.data_prep import prep_dataframe_nada
from assessment_episode_matcher.exporters import NADAbase as nada_df_generator
from assessment_episode_matcher.importers.main import  BlobFileSource
from assessment_episode_matcher.mytypes import AODWarning, CSVTypeObject
import assessment_episode_matcher.utils.df_ops_base as utdf
import assessment_episode_matcher.importers.nada_indexed as io


def generate_nada_export(
    matched_assessments:pd.DataFrame, config:dict) \
        -> tuple[pd.DataFrame,list[AODWarning]]:
    res, warnings_aod = prep_dataframe_nada(matched_assessments, config)

    st = nada_df_generator.generate_finaloutput_df(res)        
    return st, warnings_aod

#  = "atom-matching"
def save_nada_data(data:pd.DataFrame, container:str, outfile:str):
  exp = AzureBlobExporter(container_name=container) #
  exp.export_dataframe(data_name=outfile, data=data) 


def save_aod_warnings(data:list[AODWarning], container:str, outfile:str):
  header = ["SLK","RowKey","drug_name","field_name", "field_value"]
  warnings_list = CSVTypeObject(header=header, rows=data)
  exp = AzureBlobExporter(container_name=container) #
  exp.export_csv(data_name=outfile, data=warnings_list)   


def get_matched_assessments(container_name, st_dt, end_dt):
  # st_dt, end_dt= "20240101", "20240331"
  file_source = BlobFileSource(container_name,folder_path="NADA")
  df , fname = io.import_data(st_dt, end_dt, file_source
              , prefix="forstxt_" #f"{st_dt}-{end_dt}"
              , suffix="_reindexed")
  return df, fname


def generate_nada_save(reporting_start_str:str
                       , reporting_end_str :str
                       , config:dict
                       , container:str) -> list[AODWarning]|None:

  p_str = f"{reporting_start_str}-{reporting_end_str}"

  df_reindexed, fname  = get_matched_assessments(container, reporting_start_str, reporting_end_str)
  if not utdf.has_data(df_reindexed):
    logging.error(f"No Indexed NADA data file with name {fname} could be found")
    return None
  
  nada, warnings_aod = generate_nada_export(df_reindexed, config)
  outfile = f"{p_str}/surveytxt_{p_str}.csv"

  save_nada_data(nada, container=container, outfile=outfile)
  msg = f"saved {len(nada)} NADA COMS records to {outfile}"
  logging.info(msg)
  return warnings_aod
  # print("Done. New file : ", nada_importfile.absolute())


def write_aod_warnings(data:list[AODWarning]
                       , container:str, period_str:str) -> str:

  outfile = f"NADA/{period_str}/aod_{period_str}_warn.csv"
  logging.info("Going to write AOD Warnings")
  save_aod_warnings(data, container, outfile=outfile)
  return outfile


def main ():
    # reporting_start_str, reporting_end_str =  '20231001', '20231231' # Q4 2023
  container  = os.environ.get(str(ConfigKeys.AZURE_BLOB_CONTAINER.value))
  if not container:
      logging.exception(f"unable to proceed without app config {ConfigKeys.AZURE_BLOB_CONTAINER.value} ")
      return
  config = load_blob_config(container)
  reporting_start_str, reporting_end_str =  '20240101', '20240331' # Q1 2024
  warnings_aod = generate_nada_save(reporting_start_str, reporting_end_str, config, container)
  if warnings_aod:    
    p_str = f"{reporting_start_str}-{reporting_end_str}"
    outfile = write_aod_warnings(warnings_aod, container, period_str=p_str)
    logging.info(f"Wrote AOD Warnings to {outfile}") 


if __name__ == "__main__":
  bstrap = Bootstrap.setup(project_directory, env="dev")
  res = main()
  