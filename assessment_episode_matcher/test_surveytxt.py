# import logging
from datetime import date
# from pathlib import Path
import pandas as pd
from assessment_episode_matcher.utils.environment import ConfigManager, ConfigKeys
from assessment_episode_matcher.matching import main as match_helper
from assessment_episode_matcher.matching.errors import process_errors_warnings

# from assessment_episode_matcher.data_prep import prep_dataframe_nada
# from assessment_episode_matcher.exporters import NADAbase as out_exporter
from assessment_episode_matcher.importers import episodes as imptr_episodes
from assessment_episode_matcher.importers import assessments as imptr_atoms
from assessment_episode_matcher.exporters.main import CSVExporter as AuditExporter
# from assessment_episode_matcher.exporters.main import AzureBlobExporter as AuditExporter
import assessment_episode_matcher.utils.df_ops_base as utdf
from assessment_episode_matcher.mytypes import DataKeys as dk

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


def exclude_mismatched_dupe_assessments(slkprog_datematched:pd.DataFrame
                                        , slk_datematched:pd.DataFrame) -> pd.DataFrame:
    """
    For a given client(SLK), there may be 2+ assessments on the same day,
      one/some with the 'correct' program (matching an episode)
      and one/some with a different program - i.e. doesn't match any episode
    Inspect the SLK-only date-matched set 
          to EXCLUDE any ATOMs with the same assessmentDate that matched to the same episode
        , but with a different program contained in the slk+prog-matched set 
      TODO: Remove SLKs (PII) from examples 
    Examples: 
      * slk_datematched : AZKND150719831   MURMICE_ITSP_20230619  
        (WRONG PROGRAM - should be MURMPP) -> EXCLUDE
      * slkprog_datematched: AZKND150719831   MURMPP_ITSP_20230619

      * slk_datematched : RIGAM080820061  GOLBGNRL_INAS_20230926  
        (should be GOLBICE) -> EXCLUDE
      * slkprog_datematched: RIGAM080820061  GOLBICE_INAS_20230926

    """
    # match common SLK+EpId+Asssme_date 
    slk_datematched['Ep_AsDate'] = slk_datematched['SLK'] + \
                                '_' + slk_datematched.PMSEpisodeID + \
                                    '_' + slk_datematched.PMSEpisodeID_SLK_RowKey.str[-8:]
    slkprog_datematched['Ep_AsDate'] = slkprog_datematched['SLK'] + \
                                       '_' + slkprog_datematched.PMSEpisodeID + \
                                 '_' + slkprog_datematched.PMSEpisodeID_SLK_RowKey.str[-8:]
   
    # keep only what is in slk_datematched
    slk_datematched_v2 = utdf.filter_out_common(slk_datematched, slkprog_datematched, key='Ep_AsDate')
    
    # good_df2_v2 =  good_df2[~good_df2.Ep_AsDate.isin(good_df.Ep_AsDate)]
    print("fix_incorrect_program: moving rows from slk_datematched becauase they exist in slkprog_datematched: \n"
          , slk_datematched[~slk_datematched.Ep_AsDate.isin(slk_datematched_v2.Ep_AsDate)])
    slk_datematched_v2 = utdf.drop_fields(slk_datematched_v2, ['Ep_AsDate'])

    return slk_datematched_v2


def fix_incorrect_program( slk_datematched:pd.DataFrame) -> pd.DataFrame:
    """
      SLK-only-matched won't have the right program from the assessment:
        use the Program from the matched episode.
    """
    # for matches based only on SLK, use the program information of the episode. log the changes
    print(f"SLK-only based matched: ({len(slk_datematched)})")
    print("Adding program information from matched episode. Before :"
          , slk_datematched['Program_x'].value_counts())
    print("Adding program information from matched episode. AFTER :"
          , slk_datematched['Program_y'].value_counts())
    
    slk_datematched['Program'] = slk_datematched['Program_y'] 
    
    return slk_datematched


def match_and_get_issues(e_df, a_df, inperiod_atomslk_notin_ep, inperiod_epslk_notin_atom, slack_for_matching):

    a_ineprogs , a_notin_eprogs = match_helper.filter_asmt_by_ep_programs (e_df, a_df)
    # print(f"Assessments not in any of the programs of the episode {len(a_notin_eprogs)}")

    # XXA this assumes Assessment's program is always Correct 
    slkprog_datematched, dates_ewdf \
    , slk_prog_onlyinass, slk_prog_onlyin_ep  = match_helper.do_matches_slkprog(
                                                          a_ineprogs 
                                                          , e_df
                                                          , slack_for_matching
                                                        )
    a_key = dk.assessment_id.value # SLK +RowKey
    # ATOMs that could not be date matched with episode, when merging on SLK+Program
    unmatched_asmt_by_slkprog, _ = utdf.get_delta_by_key(a_ineprogs
                                                         , slkprog_datematched
                                                         , key=a_key)
    # unmatched_asmt_by_slkprog = utdf.filter_out_common(a_ineprogs, slkprog_datematched, a_key)

    slkonly_datematched, dates_ewdf2 \
      , slk_onlyinass, merge_key2  = match_helper.do_matches_slk(unmatched_asmt_by_slkprog 
                                                            , e_df
                                                            , slack_for_matching
                                                            )

    slkonly_datematched_v2 = exclude_mismatched_dupe_assessments(slkprog_datematched
                                                                 , slkonly_datematched)
    slkonly_datematched_v2 = fix_incorrect_program(slkonly_datematched_v2)

    final_good = pd.concat([slkprog_datematched, slkonly_datematched_v2])

    # can't use the result above (_)as we are only using the un-merged assesemtns (slk_prog_onlyinass) as input
    slk_onlyin_ep, _ = utdf.get_delta_by_key(e_df, a_ineprogs, key='SLK')
    # slk_onlyin_ep = utdf.filter_out_common(e_df, a_ineprogs, key='SLK')

    # TODO: explain why these are two different things (pre date-matching vs post date-matching errors)
    print("concating pre-match missing SLK errors of lengths :")
    print(f"\n\t only-in-ATOM: {len(inperiod_atomslk_notin_ep)}  ; only in Episode: {len(inperiod_epslk_notin_atom)} ")
    slk_onlyinass = pd.concat([slk_onlyinass, inperiod_atomslk_notin_ep])
    slk_onlyin_ep = pd.concat([slk_onlyin_ep, inperiod_epslk_notin_atom])

    ew = {
        'slk_onlyinass': slk_onlyinass,
        'slk_onlyin_ep': slk_onlyin_ep,
        'slk_prog_onlyinass': slk_prog_onlyinass,
        'slk_prog_onlyin_ep': slk_prog_onlyin_ep,
        'dates_ewdf': dates_ewdf,
        'dates_ewdf2': dates_ewdf2
    }    
    return final_good, ew
    

# def generate_nada_export(matched_assessments:pd.DataFrame, outfile:Path):
#     res, warnings_aod = prep_dataframe_nada(matched_assessments)

#     st = out_exporter.generate_finaloutput_df(res)
#     # st.to_parquet('/data/out/surveytxt.parquet')
#     st.to_csv(outfile, index=False)
        
#     return st
    

def main2():
    # TODO:
    # envinronemnt setup : Config setup, Expected Directories create, logging setup

    ConfigManager.setup('dev')
    cfg = ConfigManager().config
    slack_for_matching = int(cfg.get(ConfigKeys.MATCHING_NDAYS_SLACK, 7))
    refresh_assessments = False #cfg.get( ConfigKeys.REFRESH_ATOM_DATA, True )
    reporting_start = date(2024, 1, 1)
    reporting_end = date(2024, 3, 31)
    # source_folder = 'data/in/'
    eps_st, eps_end = '20220101', '20240331'    
    asmt_st, asmt_end = "20160701",  "20240508"

    a_df, e_df, inperiod_atomslk_notin_ep, inperiod_epslk_notin_atom = \
        match_helper.get_data_for_matching( imptr_episodes \
                                       , imptr_atoms \
                                       , eps_st, eps_end \
                                       , reporting_start, reporting_end \
                                       , assessment_start=asmt_st, assessment_end=asmt_end \
                                       , slack_for_matching=slack_for_matching \
                                       , refresh=refresh_assessments
                                      )
    if not utdf.has_data(a_df) or not utdf.has_data(e_df):
        print("No data to match. Ending")
        return None    
    # e_df.to_csv('data/out/active_episodes.csv')
    final_good, ew = match_and_get_issues(e_df, a_df
                                          , inperiod_atomslk_notin_ep
                                          , inperiod_epslk_notin_atom, slack_for_matching)

    warning_asmt_ids  = final_good.SLK_RowKey.unique()

    ae = AuditExporter(config={'location' : 'data/out/errors_warnings/'})
    process_errors_warnings(ew, warning_asmt_ids, dk.client_id.value
                            , period_start=reporting_start
                            , period_end=reporting_end
                            , audit_exporter=ae)
  

    df_reindexed = final_good.reset_index(drop=True)
    df_reindexed.to_csv('data/out/reindexed.csv', index_label="index")
    return df_reindexed
    # nada_importfile:Path = Path("data/out") / \
    #                        f"{reporting_start}_{reporting_end}_surveytxt.csv"
    # nada = generate_nada_export(df_reindexed, outfile=nada_importfile)

    # return nada

if __name__ == "__main__":
    res = main2()

