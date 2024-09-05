
import logging
# from typing import Optional
import pandas as pd

from assessment_episode_matcher.data_config import keep_parent_fields, mulselect_option_to_nadafield
from assessment_episode_matcher.mytypes import Purpose
from assessment_episode_matcher.utils.base import check_for_string
from assessment_episode_matcher.utils.dtypes import fix_numerics
from assessment_episode_matcher.utils.df_ops_base import concat_drop_parent, \
                           drop_fields_by_regex \
                     ,   drop_fields,\
                         to_num_yn_none, to_num_bool_none,transform_multiple
from assessment_episode_matcher.utils.fromstr import clean_and_parse_json
from assessment_episode_matcher.importers.aod import expand_drug_info

# logger = mylogger.get(__name__)

def get_surveydata_expanded(df: pd.DataFrame, prep_type: Purpose) -> pd.DataFrame:
    df_surveydata = df['SurveyData'].apply(clean_and_parse_json)
    
    # Filter out invalid or missing JSON data
    valid_surveydata = df_surveydata.apply(lambda x: isinstance(x, dict))
    df_surveydata = df_surveydata[valid_surveydata]
    
    df_surveydata_expanded = pd.json_normalize(df_surveydata.tolist(), max_level=1)
    
    if prep_type == Purpose.MATCHING:
        df_surveydata_expanded = df_surveydata_expanded[['ClientType', 'PDC']]
    
    # Ensure df_surveydata_expanded has the same index as valid_surveydata
    df_surveydata_expanded.index = valid_surveydata[valid_surveydata].index
    
    # if 'keep_parent_fields' in locals():
    existing_columns_to_remove = [col for col in keep_parent_fields 
                                  if col in df_surveydata_expanded.columns]
    if existing_columns_to_remove:
        df_surveydata_expanded = drop_fields(df_surveydata_expanded, keep_parent_fields)
    
    df_final = concat_drop_parent(df[valid_surveydata],
                                  df_surveydata_expanded,
                                  drop_parent_name='SurveyData')
    
    return df_final


def nadafield_from_multiselect(df1:pd.DataFrame) -> pd.DataFrame:
  df= df1.copy()
  # no_answer_value = -1  # do this together later for all fields.
  for ATOMMultiSelectQuestion, nadafield_searchstr in \
      mulselect_option_to_nadafield.items():
    for nadafield, search_str in nadafield_searchstr.items():
      if ATOMMultiSelectQuestion not in df.columns:
        logging.warn(f"No column {ATOMMultiSelectQuestion} nadafield_from_multiselect")
        continue
      df[nadafield] = df[ATOMMultiSelectQuestion].apply(
                    lambda x: check_for_string(x, search_str))

  return df


def convert_yes_nofields(df1, field_names):
  return transform_multiple(df1, field_names,to_num_yn_none)

def convert_true_falsefields(df1, field_names):
  return transform_multiple(df1, field_names,to_num_bool_none)




def prep_nada_fields(df:pd.DataFrame, config:dict):

  logging.debug(f"prep_dataframe of length {len(df)} : ")
  df2 = get_surveydata_expanded(df.copy(), Purpose.NADA)
 
  df4 = drop_fields_by_regex(df2,regex='Comment|Note|ITSP') # remove *Goals notes, so do before PDC step (PDCGoals dropdown)

  df5, warnings_aod = expand_drug_info(df4, config)

  # df51 = expand_activities_info(df5)
  df51 = nadafield_from_multiselect(df5)
  # df6 = df5[df5.PDCSubstanceOrGambling.notna()]# removes rows without PDC
  
  yes_nofields = ['Past4WkBeenArrested', 'Past4WkHaveYouViolenceAbusive']

  df52 = convert_yes_nofields(df51, yes_nofields)
  bool_fields = ['ATOPHomeless',	'ATOPRiskEviction',	'PrimaryCaregiver_0-5',
                 	'PrimaryCaregiver_5-15',	'Past4Wk_ViolentToYou',]
  df6 = convert_true_falsefields(df52, bool_fields)
   
  df7 = fix_numerics(df6)  
  df7.rename(columns={'ESTABLISHMENT IDENTIFIER': 'AgencyCode'}, inplace=True)
  
  df9 = df7.sort_values(by=["SLK", "AssessmentDate"])
  
  logging.debug(f"Done Prepping for NADA. \n\t" +
                "  Dataset shape:{df9.shape}). Warnings shape {warnings_aod}")
  return df9 , warnings_aod
