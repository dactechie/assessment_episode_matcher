import logging
import pandas as pd
from mytypes import DataKeys as dk
from utils.df_ops_base import get_dupes_by_key, has_data

def get_mask_datefit(row, slack_days=7):
    # Create a Timedelta for slack days
    slack_td = pd.Timedelta(days=slack_days)
    # dk.assessment_date.value
    after_commencement = row['AssessmentDate'] >= (row['CommencementDate'] - slack_td)
    before_end_date = row['AssessmentDate'] <= (row['EndDate'] + slack_td)
    return after_commencement and before_end_date


def match_with_dates(ep_atom_df:pd.DataFrame, matching_ndays_slack: int):
    # Filter rows where AssessmentDate falls within CommencementDate and EndDate (or after CommencementDate if EndDate is NaN)
    filtered_series = ep_atom_df.apply(get_mask_datefit, slack_days=matching_ndays_slack, axis=1)

    filtered_df = ep_atom_df[filtered_series]
    
    return filtered_df



def match_dates_increasing_slack(
      ep_asmt_merged_df:pd.DataFrame
     # ,mergekeys_to_check
      , max_slack:int=7):
  matching_ndays_slack = 0 
  asmt_key = dk.assessment_id.value
  # ep_stdt = dk.episode_start_date.value
  # ep_eddt = dk.episode_end_date.value
  unmatched_asmt = ep_asmt_merged_df
  result_matched_dfs = []
  result_matched_df = pd.DataFrame()
  duplicate_rows_dfs = pd.DataFrame ()


  while len(unmatched_asmt) > 0  and matching_ndays_slack <= max_slack:
      # Get matched assessments with the current slack
      matched_df = match_with_dates(unmatched_asmt, matching_ndays_slack)
      
      #one Assessment matching to multiple episodes    
      duplicate_rows_df = get_dupes_by_key(matched_df, asmt_key)

                                          #  'SLK_Program_y')
      # y because during the merge , assmt df is on the right (so not x)
      # this checks if there ATOMs matching to multiple episodes

      if has_data(duplicate_rows_df):
        #logging.error("Duplicate rows", duplicate_rows_df)
        duplicate_rows_dfs = pd.concat([duplicate_rows_dfs, duplicate_rows_df] , ignore_index=True)
        #TODO: remove duplicates from from matched_df ? is that what the below is doing ? : result_matched_df.drop_duplicates(subset=[asmt_key])

      if len(matched_df) == 0: # no more SLK+Program matches between Episode and ATOM
         break
      
      # Add the matched DataFrame to the list
      result_matched_dfs.append(matched_df)
      # remove from unmatched, any that matched in this iteration.
      unmatched_asmt = unmatched_asmt[~unmatched_asmt[asmt_key].isin(matched_df[asmt_key])]

      ## there may be other assessments for this SLK that can match if the slack dways are increased
      ## don't exclude the SLK, but the SLK +RowKey

      # Increment the slack days for the next iteration
      matching_ndays_slack += 1

  if len(unmatched_asmt) > 0 :
     logging.info(f"There are still {len(unmatched_asmt)} unmatched ATOMs")
    #  logging.info(f"Unmatched by program: {len(unmatched_asmt.Program.value_counts())}")

    #  logger.info(f"There are still {len(unmatched_atoms)} unmatched ATOMs")
    #  logger.info(f"Unmatched by program: {len(unmatched_atoms.Program.value_counts())}")

  # Concatenate all matched DataFrames from the list
  if result_matched_dfs:
    result_matched_df = pd.concat(result_matched_dfs, ignore_index=True)
  
  # add_to_issue_report(unmatched_by_date, IssueType.DATE_MISMATCH, IssueLevel.ERROR)
  # mask_matched_eps = ep_asmt_merged_df.PMSEpisodeID.isin(result_matched_df.PMSEpisodeID)
  
  
  # if dk.client_id.value == mergekeys_to_check:
  #   unmatched_episodes = ep_asmt_merged_df[~mask_matched_eps] \
  #                       [['PMSEpisodeID'
  #                         , dk.client_id.value 
  #                         ,ep_stdt,ep_eddt]].drop_duplicates()
  # else:
  #   # in matching.main>merge_datasets, Episode is the 2nd param to pd.merge, so Program_y
  #   unmatched_episodes = ep_asmt_merged_df[~mask_matched_eps] \
  #                       [['PMSEpisodeID'
  #                         , dk.client_id.value,f'{mergekeys_to_check}_y'
  #                         ,ep_stdt,ep_eddt]].drop_duplicates()
  
  result_matched_df = result_matched_df.drop_duplicates(subset=[asmt_key])#overlapping episodes -e.g.same end date +start date
  
  
  # Can't do this because need all columns fro matching
  #unmatched_asmt = unmatched_asmt[['SLK','RowKey','AssessmentDate','Program','Staff','PDCSubstanceOfConcern']].drop_duplicates()

  #remove unmatched asessments, if the episode-id doesn;t have other asmt matches #TODO TEST ME 
  # unmatched_asmt = unmatched_asmt[unmatched_asmt.PMSEpisodeID.isin(result_matched_df.PMSEpisodeID)]

  return result_matched_df, unmatched_asmt, duplicate_rows_dfs #, unmatched_episodes


# def test_match_increasing_slack():
#     ep_asmt_merged_df = pd.read_csv("test_data/episode_assessments_merged.csv")
#     matched_df, unmatched_asmt, duplicate_rows_dfs, unmatched_episodes =\
#         match_dates_increasing_slack(ep_asmt_merged_df, max_slack=7)
#     print(matched_df)
#     print(unmatched_asmt)
#     print(duplicate_rows_dfs)
#     print(unmatched_episodes)

# if __name__ == "__main__":
#     test_match_increasing_slack()