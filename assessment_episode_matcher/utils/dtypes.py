
''' 
  The Survey JSON has various types of data : text/string, dates, numeric, number-ranges.
  The data types are defined in data_config.py and this module uses that information to 
  convert the data to the correct type.
  
'''
import logging
from datetime import datetime, date
import pandas as pd
from pandas.api.types import CategoricalDtype
# import mylogging
from assessment_episode_matcher.data_config import predef_categories,\
    question_list_for_categories, data_types, option_variants
from assessment_episode_matcher.utils.df_ops_base import has_data #, remove_if_under_threshold

# from utils.fromstr import range_average

# logging = mylogging.get(__name__)

    # - pandas.Categorical is used to create a variable that holds categorical data.

    # - pandas.api.types.CategoricalDtype is a type useful for specifying the categories and 
    # order when creating a pandas.Categorical variable, 
    # or when converting a pandas Series to categorical.
###############################################


def make_serializable(df1:pd.DataFrame, date_cols:list[str]) -> pd.DataFrame:
  if not has_data(df1):
    return pd.DataFrame()
  
  df = df1.copy()
  # df[date_cols] = df[date_cols].applymap(lambda x: x.strftime('%Y-%m-%d') 
  #                                         if isinstance(x, pd.Timestamp) else x)
  
  df[date_cols] = df[date_cols].apply(lambda col: col.map(lambda x: x.strftime('%Y-%m-%d') if isinstance(x, pd.Timestamp) else x))

  df = df.astype(str)
  return df


# if __name__ == "__main__":
#     # Create a DataFrame with some sample data
#   data = {
#       'ID': [1, 2, 3, 4],
#       'Name': ['Alice', 'Bob', 'Charlie', 'David'],
#       'Age': [25, 30, 35, 40],
#       'Start_Date': [date(2020, 1, 1), date(2020, 2, 15), date(2020, 3, 10), date(2020, 4, 5)],
#       'End_Date': [date(2022, 1, 1), date(2023, 2, 15), None, date(2024, 4, 5)]
#   }

#   # Create DataFrame
#   test_df = pd.DataFrame(data)
#   cols = ['Start_Date','End_Date' ]
#   s = make_serializable (test_df, cols)
#   print(test_df)
simple_date_formats = {
   'ddmmyyyy' : "%d%m%Y",
   'yyyymmdd' : "%Y%m%d",
  #  'yyyymm' : "%Y%m",
  #  'yyyy' : "%Y",
  #  'mm' : "%m",
  #  'dd' : "%d",
  #  'hhmmss' : "%H%M%S",
  #  'hhmm' : "%H%M",
  #  'hh' : "%H",
  #  'mm' : "%M",
  #  'ss' : "%S",
}

def date_to_str(date_obj: date|pd.Series, str_fmt='yyyymmdd') -> str|pd.Series:
    """
    Convert a single date object or a Series of dates to a formatted string or a Series of formatted strings.

    Args:
        date_obj (datetime.date, datetime.datetime, pd.Series): A single date object or a Series of dates.
        date_format (str, optional): The desired date format for the output string(s). Default is '%d%m%Y'.

    Returns:
        str or pd.Series: A formatted string or a Series of formatted strings representing the input date(s).
    """
    if isinstance(date_obj, pd.Series):
        dt_df = pd.to_datetime(date_obj, format='%d%m%Y', errors='coerce')
        return dt_df.dt.strftime(simple_date_formats[str_fmt])
    else:
        return date_obj.strftime(simple_date_formats[str_fmt])
      
  
# Define a function to convert blanks to today's date
def blank_to_today_str(x):
    if pd.isnull(x)  or x.strip() == 'NaN' or x.strip() == '':
        return datetime.now().strftime('%d%m%Y')
    return x


def define_category_with_uniqs(df:pd.DataFrame, question:str) -> CategoricalDtype:  
  unique_list = list(df[df[question].notna()][question].unique())
  return CategoricalDtype(unique_list)
  

def define_category_for_question(df:pd.DataFrame, field:str):
  # if not a predefined category, define it using the unique values
  category_type = predef_categories.get(field,
                         define_category_with_uniqs(df, field) )
  return category_type


def define_all_categories(df:pd.DataFrame):
  category_nametypes = [(field
                          , define_category_for_question(df, field)
                        )
                        for field in question_list_for_categories]
  
  logging.debug(f"category_nametypes: {category_nametypes}")

  df1 = df.copy()  
  # Field elements must be 2- or 3-tuples, got ''Yes - Completely safe''
  for category_name, category_type in category_nametypes:
    #if category_type is not None and category_type..check type:
    # (isinstance(series.dtype, pd.api.types.CategoricalDtype):):
    df1[category_name] = df[category_name].astype(category_type)
  
  return df1
  
###############################################

def convert_float_to_datetime(series:pd.Series, format:str='%Y%m%d') -> pd.Series:
  """
    float:20240101.0 -> pandas.datetime: 2024-01-01
    Don't expect blanks. 
  """
  datetime_series =  pd.to_datetime(series.astype(int).astype(str)
                                    , format=format, errors='coerce')#.dt.date

  date_series = datetime_series.dt.date

  return date_series


def convert_to_datetime(series:pd.Series, format:str='%Y%m%d', fill_blanks=True) -> pd.Series:
  """
  Blanks are filled with today/now datetime
  """
  # df [column_names] =
  datetime_series =  pd.to_datetime(series.astype(str).str.zfill(8) , format=format, errors='coerce')#.dt.date
  now = pd.Timestamp.now()

  # Replace NaT values with the current date and time
  if fill_blanks:
    datetime_series = datetime_series.fillna(now)
  date_series = datetime_series.dt.date

  return date_series

"""
      # fix_variants       
      # d1 = df.loc[df['PDCMethodOfUse'] =='Ingests'].copy()
      # d1.loc[:,'PDCMethodOfUse'] = 'Ingest'
      # df.update(d1)

"""
def fix_variants (df1):

  has_variant_column_types = [ov for ov in option_variants.keys() if ov in df1.columns ]
  if not any(has_variant_column_types):
    return df1
  
  df = df1.copy()
  for field, variant_dict in option_variants.items():  # type: ignore #PDCMethodOfUse
    for variant, original in variant_dict.items(): # type: ignore
      logging.info(f"fixing {field} {variant} to {original}")
      d1 = df.loc[df[field] == variant].copy()
      d1.loc[:,field] = original
      df.update(d1)
  return df



  # convert numeric types
def fix_numerics(df1: pd.DataFrame):

  df = df1.copy()
  
  numeric_fields = [k for k, v in data_types.items() if v == 'numeric' and k in df.columns]
  logging.debug(f"numeric_fields: {numeric_fields}")
  df[numeric_fields] = df[numeric_fields].apply(pd.to_numeric, errors='coerce') # ignore ?

  # range_fields = [k for k, v in data_types.items() if v == 'range']
  # range_fields = [ f for f in df.columns
  #                    for sx in fieldname_suffixes_range
  #                       if f"_{sx}" in f ]
  # logging.debug(f"range_fields: {range_fields}") 
  # df[range_fields] = df[range_fields].applymap(range_average)

  return df


def convert_dtypes(df1):
  logging.debug(f"convert_dtypes")
  df = df1.copy()
  
  # common to matchng and NADA, so moving from here
  # convert_to_datetime(df,'AssessmentDate') # TODO : DOB
  
  df1 = fix_variants(df) # Smokes -> Smoke   # NOT FOR NADA
  df2 = fix_numerics(df1)
  # df3 = define_all_categories(df2)  # not for NADA
  # return df3
  return df2
  