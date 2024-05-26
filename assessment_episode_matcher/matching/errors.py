from datetime import date
import pandas as pd
from assessment_episode_matcher.exporters.main import DataExporter
import assessment_episode_matcher.utils.df_ops_base as utdf
from assessment_episode_matcher.mytypes import IssueType, IssueLevel, DataKeys as dk
from assessment_episode_matcher.configs import audit as audit_cfg

def process_errors_warnings(ew:dict, warning_asmt_ids, merge_key2
                            ,period_start:date, period_end:date
                            , audit_exporter:DataExporter):


    slkonlyin_amst_error, slkprogonlyin_amst_warn = key_matching_errwarn_names(
        merge_key2
        , ew['slk_prog_onlyinass'], IssueType.SLKPROG_ONLY_IN_ASSESSMENT  # warni
        , ew['slk_onlyinass'], IssueType.CLIENT_ONLYIN_ASMT)  # error

    slkonlyin_ep_error, slkprogonlyin_ep_warn = key_matching_errwarn_names(
        merge_key2, ew['slk_prog_onlyin_ep'], IssueType.SLKPROG_ONLY_IN_EPISODE
        , ew['slk_onlyin_ep'], IssueType.CLIENT_ONLYIN_EPISODE)

    # one Assessment matching to multiple episodes
    # duplicate_rows_df = get_dupes_by_key(matched_df, asmt_key)
    # eousides with no assessment in the reporting period
    # mask_matched_eps = ep_asmt_merged_df.PMSEpisodeID.isin(result_matched_df.PMSEpisodeID)

    # previously marked as errors, with the 2nd round of (relaxed i.e. SLK-only matching)
    # we mark them as warnings, as they matches were included in good_df2
    dates_ewdf = ew['dates_ewdf']
    dates_ewdf2 = ew['dates_ewdf2']
    
    dates_ewdf.loc[dates_ewdf.SLK_RowKey.isin(warning_asmt_ids) \
                   , 'issue_level'] = IssueLevel.WARNING.name
    final_dates_ew = pd.concat(
        [dates_ewdf, dates_ewdf2]).reset_index(drop=True)
    
    # date mismatch errors that are outside the reporting period dnt need to be reported.
    a_dt = dk.assessment_date.value
    final_dates_ew = utdf.in_period(final_dates_ew
                      ,a_dt,a_dt
                      ,period_start, period_end)

    # limit columns to write out
    final_dates_ew = final_dates_ew[audit_cfg.COLUMNS_AUDIT_DATES]
    slkonlyin_ep_error = slkonlyin_ep_error[audit_cfg.COLUMNS_AUDIT_EPKEY_CLIENT]
    slkprogonlyin_ep_warn = slkprogonlyin_ep_warn[audit_cfg.COLUMNS_AUDIT_EPKEY_CLIENTPROG]
    slkonlyin_amst_error = slkonlyin_amst_error[audit_cfg.COLUMNS_AUDIT_ASMTKEY]
    slkprogonlyin_amst_warn = slkprogonlyin_amst_warn[audit_cfg.COLUMNS_AUDIT_ASMTKEY]


    ew2 = {
       'dates_ew':final_dates_ew,
       'asmt_key_errors': slkonlyin_amst_error,
       'ep_key_errors': slkonlyin_ep_error,
       'asmt_key_warn': slkprogonlyin_amst_warn,
       'ep_key_warn': slkprogonlyin_ep_warn,
    }
    # write_validation_results(good_df, dates_ewdf, slk_program_keys_ewdf)
    write_validation_results(ew2, audit_exporter)        


"""
  #"Key" issues
    # Assessment issues: 
      # not matched to any episode, b/c:
         # assessment SLK not in any episode -> ERROR
         # SLK+Program not in any episode --> WARNING (keep in good dataset)
    # Episode issues:
      # zero assessments, b/c: (note: eps at end of reporting period may not have asmts)
        # episode SLK not in any assessment  -> ERROR 
        # SLK+Program not in Assessment-list -> WARNING (keep in good dataset)
"""

def key_matching_errwarn_names(merge_key: str, slk_prog_onlyin: pd.DataFrame, it1: IssueType,
                        slk_onlyin: pd.DataFrame,  it2: IssueType):
    # slk_prog_onlyin (SLK+Program) doesn't need to have anytihng that is also in slk_onlyin (SLK)
    # redundant
    slk_prog_onlyin1, _ = utdf.get_delta_by_key(
        slk_prog_onlyin, slk_onlyin, key=merge_key)
    # mask_common = slk_prog_onlyin[matchkey2].isin(slk_onlyin[matchkey2])
    slk_prog_warn = slk_prog_onlyin1.assign(
        issue_type=it1.name,
        issue_level=IssueLevel.WARNING.name)

    slk_onlyin_error = slk_onlyin.assign(issue_type=it2.name,
                                         issue_level=IssueLevel.ERROR.name)
    # only_in_errors = pd.concat([slk_prog_new, slk_onlyin_new])

    return slk_onlyin_error, slk_prog_warn



def write_validation_results(errors_warnings:dict[str, pd.DataFrame]
                             , audit_exporter: DataExporter):
    
    for ew_type_name, errs_warns in errors_warnings.items():
      if utdf.has_data(errs_warns):
        audit_exporter.export_data(f"{ew_type_name}.csv", errs_warns)       
       
    

    # if utdf.has_data(dates_ewdf):
    #   audit_exporter.export_data("dates_ew", dates_ewdf)
    #   # dates_ewdf.to_csv(f'{output_folder}dates_ewdf.csv'
    #   #                   , index=False)
    
    # if utdf.has_data(asmt_key_errors):
    #    audit_exporter.export_data("asmt_key_errors", asmt_key_errors)
    #   # asmt_key_errors.to_csv(f'{output_folder}asmt_key_errors.csv'
    #   #                       , index=False)

    # if utdf.has_data(ep_key_errors):
    #    audit_exporter.export_data("ep_key_errors", ep_key_errors)
    #   # ep_key_errors.to_csv(f'{output_folder}ep_key_errors.csv'
    #   #                      , index=False)

    # if utdf.has_data(asmt_key_warn):
    #   audit_exporter.export_data("asmt_key_warn", asmt_key_warn)

    # if utdf.has_data(ep_key_warn):
    #   audit_exporter.export_data("ep_key_warn", ep_key_warn)


    # if utdf.has_data(asmt_key_warn):
    #   asmt_key_warn.to_csv(f'{output_folder}asmt_key_warn.csv'
    #                        , index=False)

    # if utdf.has_data(ep_key_warn):
    #   ep_key_warn.to_csv(f'{output_folder}ep_key_warn.csv'
    #                      , index=False)
