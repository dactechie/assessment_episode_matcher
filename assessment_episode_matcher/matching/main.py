import logging
from datetime import date
import pandas as pd
from assessment_episode_matcher.mytypes import DataKeys as dk, IssueLevel, IssueType, Purpose #, ValidationIssue
# from utils.environment import MyEnvironmentConfig, ConfigKeys
import assessment_episode_matcher.utils.df_ops_base as utdf
from assessment_episode_matcher.utils import base as utbase
import assessment_episode_matcher.matching.date_checks as dtchk
from assessment_episode_matcher.matching import increasing_slack as mis
from assessment_episode_matcher.setup.bootstrap import Bootstrap

def get_data_for_matching2(episode_df, atom_df, start_date:date
                           , end_date:date, slack_for_matching) \
              -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
  a_df, e_df,inperiod_atomslk_notin_ep, inperiod_epslk_notin_atom \
      = get_asmts_4_active_eps2(
                  episode_df, atom_df, start_date=start_date
                , end_date=end_date, slack_ndays=slack_for_matching)

    # SLK_RowKey
  a_df, _ = utdf.merge_keys_new_field(
      a_df, [dk.client_id.value, dk.per_client_asmt_id.value])
  print("filtered ATOMs shape ", a_df.shape)
  print("filtered Episodes shape ", e_df.shape)
  return a_df, e_df, inperiod_atomslk_notin_ep, inperiod_epslk_notin_atom


def get_data_for_matching(ep_imptr, asmt_imptr, eps_st:str, eps_end:str
                          , reporting_start, reporting_end
                          , assessment_start, assessment_end
                          , slack_for_matching:int, refresh:bool=True) \
                            -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
  """
    1. imports Episode and ATOM data using the start and end dates passed in
    2. Limits the assessments to only those episodes that were active during the period
    3. Prepares a unique key for Assessments : SLK + RowKey
    4. Returns filtered assessment and epsideo lists along with: 
        - a. Clients Assessments for clients who are NOT in the final Episode list.
        - b. Client Episodes for clients who are NOT in the final Assessment list.
  """
  # Bootstrap.config
  episode_df = ep_imptr.import_data(eps_st, eps_end)
  if not utdf.has_data(episode_df):
      logging.error("No episodes")
      return pd.DataFrame(), pd.DataFrame(),pd.DataFrame(), pd.DataFrame()
  print("Episodes shape ", episode_df.shape)

  atom_df = asmt_imptr.import_data(assessment_start
                                   , assessment_end
                                   , purpose=Purpose.NADA
                                   , refresh=refresh
            )
  # atom_df.to_csv('data/out/atoms.csv')
  print("ATOMs shape ", atom_df.shape)
  # FIX ME: multiple atoms on the same day EACAR171119722 16/1/2024

  a_df, e_df,inperiod_atomslk_notin_ep, inperiod_epslk_notin_atom  = get_asmts_4_active_eps2(
                episode_df, atom_df, start_date=reporting_start
              , end_date=reporting_end, slack_ndays=slack_for_matching)

  # SLK_RowKey
  a_df, _ = utdf.merge_keys_new_field(
      a_df, [dk.client_id.value, dk.per_client_asmt_id.value])
  print("filtered ATOMs shape ", a_df.shape)
  print("filtered Episodes shape ", e_df.shape)
  return a_df, e_df, inperiod_atomslk_notin_ep, inperiod_epslk_notin_atom



def get_asmts_4_active_eps2(episode_df: pd.DataFrame,
                           atoms_df: pd.DataFrame,
                           start_date: date,
                           end_date: date,
                           slack_ndays: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
      Q: Why do we need to extract ATOMs before the reporting period?
      A: To ensure the stage-number is accurate. 

      1. Get all episodes that were active at any point during the period.
      2. To get the list of ATOMs active in the period, give the AssessmentDate range of:
          a. the start date of the earliest episode in step 1, minus n days for some slack.
          b. the end date of the reporting period.

        Important: 
        1. There may be ATOM assessments for clients who are NOT in the list from step 1
        we return them anyway as the 'atoms_active_inperiod' and the validation steps later 
        would flag them.

        2. There may be no ATOM assessments in the reporting period, even though the matched episode
        had an active period (> say 28 days) in the reporting period.
    """

    ep_stfield, edfield = dk.episode_start_date.value, dk.episode_end_date.value
    asmtdt_field = dk.assessment_date.value

    eps_active_inperiod =\
        utdf.in_period(episode_df, ep_stfield, edfield,
                         start_date, end_date)
    # active_clients_eps, eps_active_inperiod =\
    #     get_clients_for_eps_active_in_period(episode_df, start_date, end_date)

    # all_eps_4clients_w_active_eps_inperiod = episode_df[episode_df.SLK.isin(active_clients_eps)]

    # TODO : Do this one year later , it is irrelevant here
    # long running episodes should be eliminated as well.
    mask_within_ayear = (pd.to_datetime(eps_active_inperiod['EndDate']) - pd.to_datetime(
        eps_active_inperiod['CommencementDate'])).dt.days <= 366
    eps_active_inperiod = eps_active_inperiod.assign(
        within_one_year=mask_within_ayear)
    # eps_morethan_ayear = eps_active_inperiod[~mask_within_ayear]
    # logging.error(f"There are {len(eps_morethan_ayear)} episodes (active in reporting period) that were longer than a year")

    # eps_active_inperiod = eps_active_inperiod[mask_within_ayear]
    # logging.warning("Filtered out episodes if they were more than a year long")

    # TODO: parameterize
    min_asmt_date = min(
        eps_active_inperiod[ep_stfield]) - pd.Timedelta(days=slack_ndays)

    atoms_active_inperiod =\
        utdf.in_period(atoms_df, asmtdt_field, asmtdt_field,
                         min_asmt_date, end_date)

    mask_comnslk_asminprd_epinprd = atoms_active_inperiod.SLK.isin(
        eps_active_inperiod.SLK.unique())
    atoms_slk_not_in_ep = atoms_active_inperiod[~mask_comnslk_asminprd_epinprd]
    inperiodatom_slknot_inep = utdf.in_period(
        atoms_slk_not_in_ep, asmtdt_field, asmtdt_field, start_date, end_date)
    print("Inperiod atoms , SLK not in episde", set(inperiodatom_slknot_inep.loc[:,'SLK']))

    commonslk_ep_mask = eps_active_inperiod.SLK.isin(atoms_active_inperiod.SLK.unique())
    ep_slk_not_in_atom = eps_active_inperiod[~commonslk_ep_mask]
    inperiodep_slk_not_inatom = utdf.in_period(
       ep_slk_not_in_atom, ep_stfield, edfield, start_date, end_date)
    print("Inperiod episode , SLK not in ATOM: ", len(set(inperiodep_slk_not_inatom.loc[:,'SLK'])))

    return atoms_active_inperiod[mask_comnslk_asminprd_epinprd], eps_active_inperiod[commonslk_ep_mask], \
        inperiodatom_slknot_inep, inperiodep_slk_not_inatom


def setup_df_for_check(episode_df: pd.DataFrame, \
                       assessment_df: pd.DataFrame,\
                          k_tup:list[str]) -> \
                          tuple[pd.DataFrame, pd.DataFrame, str]:
 
    # Get unique SLKs from episode and assessment dataframes
    # fixme: is a copy necessary?
    ep_df = episode_df.copy()
    as_df = assessment_df.copy()    

    if len(k_tup) > 1:
        ep_df, key = utdf.merge_keys_new_field(episode_df, k_tup)
        as_df, _ = utdf.merge_keys_new_field(assessment_df, k_tup)
    else:
        key = k_tup[0]
  
    return ep_df, as_df, key


# TODO: refactor with df_ops_base.get_lr_mux_unmatched
def merge_check_keys(episode_df: pd.DataFrame, assessment_df: pd.DataFrame, k_tup: list[str]):
    
    epdf_mkey, asdf_mkey, key = setup_df_for_check(episode_df,assessment_df, k_tup)
    
    only_in_ep, only_in_as, in_both = utbase.check_if_exists_in_other(
       set(epdf_mkey[key]),   set(asdf_mkey[key])
    )
    if only_in_as:  #len(assessment_df[asdf_mkey[key].isin(only_in_as)] )
      logging.info(f" (mergkey:{key}) only in assessment: {len(only_in_as)}")
    if only_in_ep: # len(episode_df[epdf_mkey[key].isin(only_in_ep)])
      logging.info(f"(mergkey:{key})  only in episode  {len(only_in_ep)}")

    return   epdf_mkey[epdf_mkey[key].isin(only_in_ep)] \
                 , asdf_mkey[asdf_mkey[key].isin(only_in_as)] \
                 , epdf_mkey[epdf_mkey[key].isin(in_both)]\
                 , asdf_mkey[asdf_mkey[key].isin(in_both)], \
                  key



def merge_datasets(episode_df:pd.DataFrame
                   , assessment_df:pd.DataFrame
                   , common_cols:list[str]
                   , match_keys:list[str]):
    """
      Inner join on "Common_Cols"
      and also return a new key which is the merge of fields in "keys_merge"
    """
    # Merge the two dataframes based on SLK, Program, and client_type
    # TODO extract, "client_type" from SurveyData
    merged_df = pd.merge(assessment_df,\
                          episode_df, on=common_cols, how="inner")
    merged_df, unique_key = utdf.merge_keys_new_field( merged_df, match_keys)

    # print ("Merged", merged_df)
    return merged_df, unique_key


def perform_date_matches(merged_df: pd.DataFrame, match_key:str, slack_ndays:int):
    
    # include all the warnings in the good_Df using matching with increasing slack
    result_matched_df, dt_unmat_asmts, duplicate_rows_dfs = \
      mis.match_dates_increasing_slack (merged_df #,mergekeys_to_check
                                          , max_slack=slack_ndays)

    mask_isuetype_map = dtchk.date_boundary_validators(limit_days=slack_ndays)
    # validation_issues, matched_df, invalid_indices =
    ew_df = dtchk.get_assessment_boundary_issues(\
       dt_unmat_asmts, mask_isuetype_map, match_key)
    
    if not utdf.has_data(duplicate_rows_dfs):
       return result_matched_df, ew_df
    # exclude from error reporting if it is in the results:
    duplicate_rows_dfs = duplicate_rows_dfs[~duplicate_rows_dfs.PMSEpisodeID_SLK_RowKey
                                            .isin(result_matched_df.PMSEpisodeID_SLK_RowKey)]
    # in the errors, show ALL the episodes the assessment matches to
    multi_match_errors = merged_df[merged_df.SLK_RowKey.isin(duplicate_rows_dfs.SLK_RowKey)]
    multi_match_errors = multi_match_errors.assign(issue_type=IssueType.ASMT_MATCHED_MULTI.name
                            , issue_level=IssueLevel.ERROR.name)
    final_dates_ewdf = pd.concat([ew_df, multi_match_errors],ignore_index=True)

    # return validation_issues, good_df, ew_df
    return  result_matched_df, final_dates_ewdf


def filter_asmt_by_ep_programs(
        ep_df: pd.DataFrame, a_df:pd.DataFrame)\
            -> tuple[pd.DataFrame, pd.DataFrame]:
  """
    Only retain Assessments, if their Program is one of the Programs in the Episodes.
    For instance, the assessments may have BEGAPATH which is no longer in operation 
    and won't have episodes, so no point trying to match/report program-mismatch errors.
  """
  asm_invalid_progs, asm_epprog = utdf.get_delta_by_key(a_df
                                                      , ep_df
                                                      , key='Program'
                                                      , common=True)
  return asm_epprog, asm_invalid_progs
  # ep_programs = ep_df['Program'].unique()
  # aprog_in_any_eprog =a_df['Program'].isin(ep_programs)
  # a_df_epprog = a_df[aprog_in_any_eprog]
  # return a_df_epprog, a_df[~aprog_in_any_eprog]


def get_merged_for_matching(episode_df: pd.DataFrame
                            , assessment_df: pd.DataFrame
                            , mergekeys_to_check:list[str]
                            , match_keys:list[str]
                            ):

    #ew_df - Errors Warnings Dataframe
    # ewdf = pd.DataFrame()
    # 1. Remove records with keys not common to both assessments and episodes: 
    #   (so we report the correct mismatch type and don't try to date-match them)
    only_in_ep, only_in_as, ep_df_inboth, as_df_inboth, merge_key = merge_check_keys(
        episode_df, assessment_df, k_tup=mergekeys_to_check# SLK or SLK+Program
    )
    # if any(only_in_ep) or any(only_in_as):
    #   # if they are irrelevent programs (TSS/Coco when doing NADA), we don't want to report them as errors
    #   # only_in_as_ep_prog = filter_asmt_by_ep_programs(only_in_as, ep_df_inboth)
    #   ewdf = add_client_issues(only_in_ep, only_in_as)
      
    # 2. Match for assessment date within episodes dates
    merged_df, match_key = merge_datasets(ep_df_inboth
                                           , as_df_inboth
                                           , common_cols=mergekeys_to_check
                                           , match_keys=match_keys)
    return merged_df, merge_key, match_key, only_in_as, only_in_ep
                                                        


def do_matches_slkprog(a_ineprogs:pd.DataFrame, e_df:pd.DataFrame, slack_for_matching:int) \
           -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    mkeys = ['SLK', 'Program']
    merged_df, merge_key, match_key, slk_prog_onlyinass, slk_prog_onlyin_ep = \
        get_merged_for_matching(e_df, a_ineprogs, mergekeys_to_check=mkeys
                                , match_keys=[dk.episode_id.value, dk.assessment_id.value]
                                )
    good_df, dates_ewdf = perform_date_matches(
        merged_df, match_key, slack_ndays=slack_for_matching)
    # exclude already matched assessments
    # len(a_df) should be = len(merged_df) + len(slk_prog_onlyinass)
    return good_df, dates_ewdf, slk_prog_onlyinass, slk_prog_onlyin_ep 
    

def do_matches_slk(not_matched_asmts_slkprog:pd.DataFrame, e_df:pd.DataFrame, slack_for_matching:int) \
           -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
  
    # retry mismatching dates, with just SLK
    # (in case the assesssment was made in a program different to the episode program)
    mkeys = ['SLK']

    merged_df3, merge_key2 \
      , match_key3, slk_onlyinass, _ = get_merged_for_matching(
                      e_df, not_matched_asmts_slkprog
                      , mergekeys_to_check=mkeys
                      , match_keys=[dk.episode_id.value, dk.assessment_id.value])
    # try date-matching again, but only where the SLKs are same but the Programs are different
    merged_df4 = merged_df3[merged_df3['Program_x'] != merged_df3['Program_y']]
    good_df2, dates_ewdf2 = perform_date_matches(
        merged_df4, match_key3, slack_ndays=slack_for_matching)
    
    return good_df2, dates_ewdf2, slk_onlyinass, merge_key2



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
    print("fix_incorrect_program: moving rows from slk_datematched becauase they exist in slkprog_datematched: \n" \
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
    """
      Perform Date Matching  - Assessment has to fall within Episode Start and End dates
      Steps: 
      1. Date-Match by SLK+Program combination
      2. For the unmatched set, date-match by SLK only (ignore if the program mismatches)
      3. Produce Errors and Warnings:
          a. Error: SLK not in Episodes list
          b. Error: SLK not in Assessments list
          c. Error: Assessment date does not fall between the Episode boundaries
          d. Warning: Successfully date-matched, but - SLK+Program combination not in Episodes (only in Assessment)
          e. Warning: Successfully date-matched, but - SLK+Program combination not in Assessment (only in Episodes)
    """

    a_ineprogs , a_notin_eprogs = filter_asmt_by_ep_programs (e_df, a_df)
    # print(f"Assessments not in any of the programs of the episode {len(a_notin_eprogs)}")

    # XXA this assumes Assessment's program is always Correct 
    slkprog_datematched, dates_ewdf \
    , slk_prog_onlyinass, slk_prog_onlyin_ep  = do_matches_slkprog(
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
      , slk_onlyinass, merge_key2  = do_matches_slk(unmatched_asmt_by_slkprog 
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