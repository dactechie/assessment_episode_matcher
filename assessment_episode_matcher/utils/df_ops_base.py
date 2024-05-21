
import logging
import datetime
import pandas as pd


def safe_convert_to_int_strs(df1: pd.DataFrame, float_columns):
    df2 = df1.copy()
    df = df1.copy()

    df2[float_columns] = df[float_columns].astype(str).replace('nan', '')
    # df2[float_columns] = df2[float_columns].replace('nan', '')

    for col in float_columns:
        mask = ~df[col].isna()
        df2.loc[mask, col] = df.loc[mask, col].astype(int).astype(str)

    return df2


def get_delta_by_key(df1: pd.DataFrame
                     , df2: pd.DataFrame
                     , key: str, common:bool=False) \
                      -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Identify rows in the first DataFrame that do not have matching keys in the second DataFrame.
    
    Parameters:
    df1 (pd.DataFrame): The first DataFrame to compare.
    df2 (pd.DataFrame): The second DataFrame to compare against.
    key (str): The column name to use as the key for comparison.
    common (bool): If True, also return the rows in df1 with keys that are common to both DataFrames. Defaults to False.
    
    Returns:
    tuple[pd.DataFrame, pd.DataFrame]: 
        - The first DataFrame contains rows from df1 that do not have matching keys in df2.
        - The second DataFrame (empty if common is False) contains rows from df1 with keys that are common to both DataFrames.
    """    
    df1_keys = set(df1[key])
    df2_keys = set(df2[key])
    common_keys = df1_keys.intersection(df2_keys)
    
    mask = df1[key].isin(common_keys)
    not_in_df2 = df1[~mask]
    
    if common:
        return not_in_df2, df1[mask]
        
    return not_in_df2, pd.DataFrame()


def filter_out_common(df1: pd.DataFrame, df2: pd.DataFrame, key: str) -> pd.DataFrame:
    df1_keys = set(df1[key])
    df2_keys = set(df2[key])
    common_keys = df1_keys.intersection(df2_keys)
    filtered_df = df1[~df1[key].isin(common_keys)]
    return filtered_df


def has_data(df: pd.DataFrame | None) -> bool:
    return not (df is None or df.empty)


def get_dupes_by_key(df: pd.DataFrame, key: str):

    counts = df.groupby(key)[key].value_counts()
    if counts.empty:
        return None

    duplicates = counts[counts > 1].index.tolist()
    if not duplicates:
        return None
    return df[df[key].isin(duplicates)]


from datetime import date
def in_period(df:pd.DataFrame
                     , startfield:str, endfield:str
                     , start_date:date, end_date:date
                    #  , clientid_field:Optional[str]=None
                     ) -> pd.DataFrame:

    in_period_df = df[(start_date <= df[endfield]) & (df[startfield] <= end_date)]
    
    return in_period_df#, unique_clients


def get_last_day_n_months_ago(n_months_ago) -> datetime.date:
    current_date = datetime.date.today()
    first_day_of_current_month = datetime.date(
        current_date.year, current_date.month, n_months_ago)
    last_day_of_previous_month = first_day_of_current_month \
        - datetime.timedelta(days=1)
    last_end = last_day_of_previous_month
    return last_end


"""
  NOTE: if there are open episodes, it returns last_end as 
        the last date of the previous month
"""


def get_firststart_lastend(first_dt_series: pd.Series, last_dt_series: pd.Series):
    # Get the earliest date from the first datetime series
    first_start = first_dt_series.min()

    # Check if there are any non-null values in the last datetime series
    if last_dt_series.notnull().any():
        # Get the latest date from the last datetime series
        last_end = last_dt_series.max()
    else:
        # If all values in the last datetime series are null,
        # find the last day of the previous month
        last_end = get_last_day_n_months_ago(1)

    return first_start, last_end


def to_num_yn_none(x) -> str | None:
    if x == 'No':
        return '0'
    elif not pd.isna(x):
        return '1'
    else:
        return None


def to_num_bool_none(x: bool | None) -> str | None:
    if pd.isna(x):
        return None
    if x == True:
        return '1'
    return '0'


def transform_multiple(df1: pd.DataFrame, fields: list[str], transformer_fn) -> pd.DataFrame:
    df = df1.copy()
    # "None of [Index(['Past4WkBeenArrested', 'Past4WkHaveYouViolenceAbusive'], dtype='object')] are in the [columns]"
    fields_indf = [f for f in fields if f in df.columns]
    if not fields_indf:
        logging.info(f"transform_multiple: fields {fields} not in df columns {df.columns}")
        return df
    # if len(fields_indf) !=  len(df.columns):
    #   fields_noindf = [f for f in fields if f not in df.columns]
    #   logging.error(f"fields {fields_noindf} not in df columns {df.columns}")

    df[fields_indf] = df[fields_indf].apply(
        lambda field_series: field_series.apply(transformer_fn))
    return df



def prescribe_fields(matched_df, final_fields):
  df_final = pd.DataFrame(columns=final_fields)

  for column in final_fields:
      if column in matched_df.columns:
          df_final[column] = matched_df[column]  # Or use another default value
      else:
          df_final[column] =""
  return df_final
  

def drop_fields(df: pd.DataFrame, fieldnames: list | str | tuple):
    if isinstance(fieldnames, str):
        to_remove = fieldnames
    else:
        to_remove = [col for col in fieldnames if col in df.columns]
    df2 = df.drop(to_remove, axis=1)
    return df2


def drop_fields_by_regex(df, regex: str):
    df2 = df.loc[:, ~df.columns.str.contains(regex, case=False)]
    return df2


def concat_drop_parent(df, df2, drop_parent_name: str) -> pd.DataFrame:
    return pd.concat([df.drop(drop_parent_name, axis=1), df2], axis=1)


def get_non_empty_list_items(df: pd.DataFrame, field_name: str) -> pd.DataFrame:
    # get only rows where the list is not empty
    df2 = df[df[field_name].apply(
        lambda x: isinstance(x, list) and len(x) > 0)]
    return df2


def update(df1: pd.DataFrame, df2: pd.DataFrame, on: list[str]) -> pd.DataFrame|None:
    """
        Merges two dataframes based on specified key columns, updating common rows and adding new rows from df2 to df1.

        Parameters:
        -----------
        df1 : pandas.DataFrame
            The first dataframe to be merged.
        df2 : pandas.DataFrame
            The second dataframe to be merged.
        key_columns : list, optional (default=['PartitionKey', 'RowKey'])
            A list of column names to be used as the merge key.

        Returns:
        --------
        pandas.DataFrame
            The merged dataframe with the following properties:
            - Rows that exist in both df1 and df2 (matching key_columns) will have their values updated with the
              corresponding values from df2.
            - Rows that exist only in df2 will be added to the merged dataframe, with their values populated from df2.
            - Rows that exist only in df1 will be retained in the merged dataframe, with their original values preserved.

        Notes:
        ------
        - Both df1 and df2 must have the columns specified in key_columns.
        - Columns in df2 that are not present in df1 will be added to the merged dataframe.
        - If there are conflicting column names between df1 and df2 (apart from key_columns), the columns from df2 will
          be suffixed with '_new' in the merged dataframe.

        Example:
        --------
         df1 = pd.DataFrame({'PartitionKey': ['A', 'B', 'C'],
                                'RowKey': [1, 2, 3],
                                'Value': [10, 20, 30]})
         df2 = pd.DataFrame({'PartitionKey': ['B', 'C', 'D'],
                                'RowKey': [2, 3, 4],
                                'Value': [40, 50, 60]})
         merged_df = merge_dataframes(df1, df2)
         merged_df
          PartitionKey  RowKey  Value
        0            A       1     10
        1            B       2     40
        2            C       3     50
        3            D       4     60
    """
    merged_df = df1.merge(df2, on=on, how='outer', suffixes=('', '_new'))

    columns_to_update = [col for col in df2.columns if col not in on]

    for col in columns_to_update:
        merged_df[col] = merged_df[col + '_new'].combine_first(merged_df[col])
        merged_df[col] = merged_df[col].fillna(merged_df[col + '_new'])

    if not merged_df[[c for c in merged_df.columns if '_new' in c]].isnull().all().all():
        return None
    
    merged_df = merged_df.drop(
        columns=[col + '_new' for col in columns_to_update])
    return merged_df

    # merged_df = df1.merge(df2, on=on, how='left', suffixes=('', '_new'))

    # columns_to_update = [col for col in df2.columns if col not in on]

    # for col in columns_to_update:
    #     merged_df[col] = merged_df[col + '_new'].combine_first(merged_df[col])

    # merged_df = merged_df.drop(columns=[col + '_new' for col in columns_to_update])
    # return merged_df


def merge_keys_new_field(df1: pd.DataFrame, merge_fields: list[str], separator: str = '_')\
        -> tuple[pd.DataFrame, str]:
    """
    Merges multiple columns from a DataFrame into a single column using '_'.

    Args:
        df (pandas.DataFrame): The DataFrame containing the columns to merge.
        cols (list): A list of column names to be merged.

    Returns:
        pandas.DataFrame: The original DataFrame with a new column containing the merged data.
    """
    df = df1.copy()
    new_field = separator.join(merge_fields)
    merged_col = df[merge_fields].apply(
        lambda x: separator.join(x.astype(str)), axis=1)
    df[new_field] = merged_col
    # df[f'{field1}_{field2}'] =  df[field1] + '_' + df[field2]
    return df, new_field


# get PDC - it is the first/only list item in the PDC list
# , support:Optional[dict]):
def normalize_first_element(l1: pd.DataFrame, dict_key: str):

    masked_rows = l1[(dict_key in l1) and l1[dict_key].apply(
        lambda x: isinstance(x, list) and len(x) > 0)]

    # first dict of the list of dicts
    pdcs_df = masked_rows[dict_key].map(lambda x: x[0])
    normd_pdc: pd.DataFrame = pd.json_normalize(
        pdcs_df.to_list())  # index lost

    # l1.loc[7537,'PDC'] == masked_rows['PDC'][7537] == normd_pdc.loc[7317,:]
    l2 = masked_rows.reset_index(drop=True)
    result = concat_drop_parent(l2, normd_pdc, dict_key)
    return result


# def get_right_only(matched_atoms: pd.DataFrame, atoms_df: pd.DataFrame, join_cols: list) -> pd.DataFrame:
#     # Perform an outer join
#     outer_merged_df = pd.merge(matched_atoms, atoms_df, how='outer',
#                                left_on=join_cols, right_on=join_cols, indicator=True)
#     # Filter rows that are only in atoms_df
#     only_in_atoms_df = outer_merged_df[outer_merged_df['_merge']
#                                        == 'right_only']
#     # Drop the indicator column and keep only columns from atoms_df
#     only_in_atoms_df = only_in_atoms_df.drop(columns=['_merge'])
#     cleaned_df = only_in_atoms_df.dropna(axis=1, how='all')
#     return cleaned_df


"""
  Mutually unmatched
  merge_cols = ['SLK', 'Program']
"""


def get_lr_mux_unmatched(left_df: pd.DataFrame, right_df: pd.DataFrame, merge_cols: list['str']) \
        -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    merged_df = pd.merge(left_df, right_df, on=merge_cols,
                         how='outer', indicator=True)
    # Get non-matching rows for df1
    left_non_matching = merged_df[merged_df['_merge'] == 'left_only']

    # Get non-matching rows for df2
    right_non_matching = merged_df[merged_df['_merge'] == 'right_only']
    # Left outer join and filter for non-matching records
    # left_non_matching = pd.merge(left_df, right_df, how='left', left_on=merge_cols, right_on=merge_cols, indicator=True)
    # left_non_matching = left_non_matching[left_non_matching['_merge'] == 'left_only']

    # Right outer join and filter for non-matching records
    # right_non_matching = pd.merge(left_df, right_df, how='right', left_on=merge_cols, right_on=merge_cols, indicator=True)
    # right_non_matching = right_non_matching[right_non_matching['_merge'] == 'right_only']

    # Optionally, you can drop the '_merge' column if it's no longer needed
    left_non_matching.drop(columns=['_merge'], inplace=True)
    right_non_matching.drop(columns=['_merge'], inplace=True)

    # rows with common SLK, PRogram (good rows)
    common_rows = pd.merge(left_df, right_df, on=merge_cols, how='inner')

    # Step 2: Filter the original DataFrames to keep only the common rows
    common_left = left_df[left_df[merge_cols].isin(
        common_rows[merge_cols]).all(axis=1)]
    common_right = right_df[right_df[merge_cols].isin(
        common_rows[merge_cols]).all(axis=1)]

    return left_non_matching, right_non_matching, common_left, common_right


# if __name__ == '__main__':
#     df1 = pd.DataFrame({'PartitionKey': ['A', 'B', 'C'],
#                         'RowKey': [1, 2, 3],
#                         'Value': [10, 20, 30]})
#     df2 = pd.DataFrame({'PartitionKey': ['B', 'C', 'D'],
#                         'RowKey': [2, 3, 4],
#                         'Value': [40, 50, 60]})
#     merged_df = update(df1, df2, on=['PartitionKey', 'RowKey'])
#     print(merged_df)
