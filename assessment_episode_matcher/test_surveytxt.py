import os
import logging
import json

from assessment_episode_matcher import project_directory
from assessment_episode_matcher.importers.main import BlobFileSource, FileSource
from assessment_episode_matcher.setup.bootstrap import Bootstrap
from assessment_episode_matcher.utils.environment import ConfigKeys
from assessment_episode_matcher.utils.fromstr import get_date_from_str
from assessment_episode_matcher.matching import main as match_helper
from assessment_episode_matcher.matching.errors import process_errors_warnings

from assessment_episode_matcher.importers import episodes as EpisodesImporter
from assessment_episode_matcher.importers import assessments as ATOMsImporter
from assessment_episode_matcher.exporters.main import AzureBlobExporter #, CSVExporter as AuditExporter
import assessment_episode_matcher.utils.df_ops_base as utdf
from assessment_episode_matcher.mytypes import DataKeys as dk, Purpose

"""
 TODO:
  A. Add to Matching:
    1. ClientType
    2. PDC

  B. Add to Audit Report:
    1. More than 1 year

  C. Add Logging :
    1. 
"""

# def generate_nada_export(matched_assessments:pd.DataFrame, outfile:Path):
#     res, warnings_aod = prep_dataframe_nada(matched_assessments)

#     st = out_exporter.generate_finaloutput_df(res)
#     # st.to_parquet('/data/out/surveytxt.parquet')
#     st.to_csv(outfile, index=False)
        
#     return st


def main3():
    # reporting_start_str, reporting_end_str =  '20220101', '20231231'
    reporting_start_str, reporting_end_str =  '20190101', '20231231'
    # TODO:
    # envinronemnt setup : Config setup, Expected Directories create, logging setup
    bstrap = Bootstrap.setup(project_directory, env="dev")
    container  = os.environ.get(str(ConfigKeys.AZURE_BLOB_CONTAINER.value))
    if not container:
       logging.exception(f"unable to proceed without app config {ConfigKeys.AZURE_BLOB_CONTAINER.value} ")
       return

    ep_folder, asmt_folder = "MDS", "ATOM"
    
    cfg = bstrap.config #, bstrap.logger
    # ConfigManager.setup('dev')
    # cfg = ConfigManager().config
    slack_for_matching = int(cfg.get(ConfigKeys.MATCHING_NDAYS_SLACK.value, 7))
    
    

    reporting_start, reporting_end = get_date_from_str (reporting_start_str,"%Y%m%d") \
                                      , get_date_from_str (reporting_end_str,"%Y%m%d")

    ep_file_source:FileSource = BlobFileSource(container_name=container
                                            , folder_path=ep_folder)

    episode_df, ep_cache_to_path = EpisodesImporter.import_data(
                            reporting_start_str, reporting_end_str
                            , ep_file_source
                            , prefix=ep_folder, suffix="AllPrograms")
    if not utdf.has_data(episode_df):
      logging.error("No episodes")
      return json.dumps({"result":"no episode data"})
    #TODO:
    if ep_cache_to_path:
      if ep_cache_to_path[-3:] =='csv':
         ep_cache_to_path = f"{ep_cache_to_path[:-3]}parquet"
         
      exp = AzureBlobExporter(container_name=ep_file_source.container_name) #
      exp.export_dataframe(data_name=ep_cache_to_path, data=episode_df)   
                        # func.HttpResponse(body=json.dumps({"result":"no episode data"}),
                        #         mimetype="application/json", status_code=200)

    
    atom_file_source:FileSource = BlobFileSource(container_name=container
                                            , folder_path=asmt_folder)
    atoms_df, atom_cache_to_path = ATOMsImporter.import_data(
                            reporting_start_str, reporting_end_str
                            , atom_file_source
                            , prefix=asmt_folder, suffix="AllPrograms"
                            , purpose=Purpose.NADA, refresh=True)
    
    if atom_cache_to_path:
      exp = AzureBlobExporter(container_name=atom_file_source.container_name) #
      exp.export_dataframe(data_name=atom_cache_to_path, data=atoms_df)    
                            # , prefix="MDS", suffix="AllPrograms")
    if not utdf.has_data(atoms_df):
      logging.error("No ATOMs")
      return json.dumps({"result":"no ATOM data"})

    a_df, e_df, inperiod_atomslk_notin_ep, inperiod_epslk_notin_atom = \
      match_helper.get_data_for_matching2(episode_df, atoms_df
                                        , reporting_start, reporting_end, slack_for_matching=7)    
    if not utdf.has_data(a_df) or not utdf.has_data(e_df):
        print("No data to match. Ending")
        return None    
   
    config = {}
    final_good, ew = match_helper.match_and_get_issues(e_df, a_df
                                          , inperiod_atomslk_notin_ep
                                          , inperiod_epslk_notin_atom
                                          , slack_for_matching
                                          , config)

    warning_asmt_ids  = final_good.SLK_RowKey.unique()
   
    ae = AzureBlobExporter(container_name=atom_file_source.container_name
                           ,config={'location' : 'errors_warnings'})

    process_errors_warnings(ew, warning_asmt_ids, dk.client_id.value
                            , period_start=reporting_start
                            , period_end=reporting_end
                            , audit_exporter=ae)
  

    df_reindexed = final_good.reset_index(drop=True)

    exp = AzureBlobExporter(container_name=atom_file_source.container_name) #
    p_str = f"{reporting_start_str}-{reporting_end_str}"
    exp.export_dataframe(data_name=f"NADA/{p_str}/forstxt_{p_str}_matched.csv", data=df_reindexed)
    # exp.export_data(data_name=f"NADA/{reporting_start_str}-{reporting_end_str}_reindexed.parquet", data=df_reindexed)      

    #   # logging.info("Result object", json.dumps(result))
       
    # finally:
    return df_reindexed
    # nada_importfile:Path = Path("data/out") / \
    #                        f"{reporting_start}_{reporting_end}_surveytxt.csv"
    # nada = generate_nada_export(df_reindexed, outfile=nada_importfile)

    # return nada

  
       

if __name__ == "__main__":
    res = main3()
