import logging
import pandas as pd
import numpy as np
from assessment_episode_matcher.mytypes import DataKeys as dk, \
  IssueType, ValidationError, ValidationIssue, ValidationWarning,\
       ValidationMaskIssueTuple
import assessment_episode_matcher.utils.df_ops_base as ut


def date_boundary_validators(limit_days:int) -> list[ValidationMaskIssueTuple]:
  """
  Creates a list of dictionaries containing mask, message, and issue_level information.

  Args:
      limit_days (int): Limit for days before episode start date.

  Returns:
      list: A list of dictionaries representing the mask-message-level mapping.
  """  
  return [
    ValidationMaskIssueTuple(
   
      mask = lambda df: (df['days_from_start'] < 0) & (df['days_from_start'] >= -limit_days),   
      validation_issue = ValidationWarning(      
                msg = f"Assessment date is before episode start date by fewer than {limit_days}.",
                issue_type = IssueType.DATE_MISMATCH,
      )
    ),
    ValidationMaskIssueTuple(
       mask =  lambda df: (df['days_from_end'] > 0) &  (df['days_from_end'] <= limit_days),
      validation_issue = ValidationWarning(      
                msg = f"Assessment date is after episode end date by fewer than {limit_days}.",
                issue_type = IssueType.DATE_MISMATCH,
      )  
    ),
    ValidationMaskIssueTuple(
       mask =   lambda df: df['days_from_start'] < -limit_days ,
      validation_issue = ValidationError(      
                msg = f"Assessment date is before episode start date by more than {limit_days}.",
                issue_type = IssueType.DATE_MISMATCH,
      )
  
    ),
    ValidationMaskIssueTuple(
       mask =  lambda df: (df['days_from_end'] > limit_days) ,
      validation_issue = ValidationError(      
                msg =  f"Assessment date is after episode end date by more than {limit_days}.",
                issue_type = IssueType.DATE_MISMATCH,
      )
    ),
    
   
  ]


# Define matching functions
def assessment_date_validator(gaps_df, mit_dict:ValidationMaskIssueTuple) ->\
                                tuple[pd.DataFrame, pd.DataFrame]:
  # Check if assessment date falls between commencement and end dates
  invalid_mask_lambda = mit_dict.mask # ~((df["commencement_date"] <= df["assessment_date"]) & (df["assessment_date"] <= df["end_date"]))
  if not invalid_mask_lambda:
      return gaps_df, pd.DataFrame()
  
  ew_df = gaps_df[invalid_mask_lambda(gaps_df.copy())]

  if not ut.has_data(ew_df):
     return gaps_df, pd.DataFrame()

  vi:ValidationIssue = mit_dict.validation_issue
  ew_df = ew_df.assign(issue_type=vi.issue_type.name
                       , issue_level=vi.issue_level.name
                      )
  # ew_df['issue_msg']  = validation_issue.msg

  invalid_indices = ew_df.index.tolist()
  remaining = gaps_df.drop(invalid_indices)

  return remaining, ew_df


def gap_asmtdate_epsd_boundaries(merged_df1:pd.DataFrame):
  ad = dk.assessment_date.value
  merged_df = merged_df1.assign(
     days_from_start=(merged_df1[ad] - merged_df1[dk.episode_start_date.value]).apply(lambda x: x.days) 
    , days_from_end=( merged_df1[ad] - merged_df1[dk.episode_end_date.value]).apply(lambda x: x.days)
    )
  return merged_df


def keep_nearest_mismatching_episode(unmatched_asmt:pd.DataFrame) -> pd.DataFrame:
   unm = unmatched_asmt.copy()
   unm['min_days'] = np.minimum(np.abs(unm['days_from_start']), np.abs(unm['days_from_end']))
   ew_df = unm.sort_values(['SLK_RowKey', 'min_days'])
   ew_df = ew_df.drop_duplicates('SLK_RowKey', keep='first')
   return ew_df


def get_assessment_boundary_issues(dt_unmtch_asmt:pd.DataFrame, mask_isuetypes:list[ValidationMaskIssueTuple], ukey:str) \
                      -> pd.DataFrame:
    gaps_df = gap_asmtdate_epsd_boundaries(dt_unmtch_asmt)
    nearest_remaining_mismatch = keep_nearest_mismatching_episode(gaps_df)
    full_ew_df =  pd.DataFrame()
    for v in mask_isuetypes:
        nearest_remaining_mismatch, ew_df = assessment_date_validator(nearest_remaining_mismatch, v)        
        if ut.has_data(ew_df):
            full_ew_df = pd.concat([full_ew_df, ew_df], ignore_index=True)

    if len(nearest_remaining_mismatch) > 0:
      logging.warn("matched_df should not have anything remaining.")
    
    return full_ew_df
