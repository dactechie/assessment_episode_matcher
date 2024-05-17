import pandas as pd
from utils.dtypes import date_to_str
from utils.df_ops_base import safe_convert_to_int_strs, prescribe_fields
from .config.NADAbase import nada_final_fields, notanswered_defaults


def get_stage_per_episode(df:pd.DataFrame)-> pd.Series:  
  df = df.sort_values(by=["PMSEpisodeID", "AssessmentDate"])
  # Rank the assessments within each client
  return  df.groupby('PMSEpisodeID').cumcount()


def set_not_answered(df1:pd.DataFrame, notanswered_cols:list) -> pd.DataFrame:
  df = df1.copy()
  for col in notanswered_cols:
    df[col].replace('', -1, inplace=True)

  return df


def cols_prep(source_df, dest_columns, fill_new_cols) -> pd.DataFrame:
  
  df_final = source_df.reindex(columns=dest_columns, fill_value=fill_new_cols)
  # 'StandardDrinksPerDay' (_PerOccassionUse) -> Range/average calculation resutls in float
  float_cols = list(df_final.select_dtypes(include=['float']).columns )
  df_final = safe_convert_to_int_strs (df_final, float_cols)#.astype('Int64')
  df_final = set_not_answered(df_final, notanswered_cols=notanswered_defaults)

  return df_final


def generate_finaloutput_df(df1):

  df = df1.copy()
  df["Stage"] = get_stage_per_episode(df)

  df_final = prescribe_fields(df, nada_final_fields)
  df_final = cols_prep(df, nada_final_fields, fill_new_cols="")
  
  # TODO zfill PID
  cols_fill4 = ['PDCCode', 'PMSPersonID']
  df_final[cols_fill4] = df_final[cols_fill4].astype(str).apply(lambda x: x.str.zfill(4))

  df_final['AssessmentDate'] = date_to_str(df_final['AssessmentDate'],  str_fmt='ddmmyyyy')
  return df_final