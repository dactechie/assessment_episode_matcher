from datetime import date
from assessment_episode_matcher.utils.dtypes import date_to_str
"""
    Merge two dictionaries, excluding keys from the second dictionary.
    Returns a new dictionary with the contents of dict1 and dict2.
    If a key is in both dictionaries, the value from the second dictionary (dict2) is used.
"""
def merge_dicts_exclude_keys(dict1, dict2, exclude_keys):
    return {k: v for k, v in {**dict1, **dict2}.items() if k not in exclude_keys}

    
def check_for_string(row: list|dict, str_to_check: str) -> bool | None:
    if isinstance(row, list):
        return any(item == str_to_check for item in row)
    elif isinstance(row, dict):
        return row.get(str_to_check)
    return None

def check_if_exists_in_other(one: set, two: set) -> tuple[set, set, set]:
    only_in_one = one - two
    only_in_two = two - one
    in_both = one & two
    return only_in_one, only_in_two, in_both


def get_period_range(extract_start_date:date, extract_end_date:date) -> tuple[str,str]:
  xtr_start_str = str(date_to_str(extract_start_date, str_fmt='yyyymmdd'))
  xtr_end_str = str(date_to_str(extract_end_date, str_fmt='yyyymmdd'))
  # period_range = f"{xtr_start_str}-{xtr_end_str}"
  return xtr_start_str, xtr_end_str