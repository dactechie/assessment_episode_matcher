import logging
import json
from datetime import datetime, date
# import difflib
# from typing import Optional

from rapidfuzz import fuzz

def calculate_similarity(slk1, slk2):
    # Using ratio method for similarity score
    return fuzz.ratio(slk1, slk2) / 100

def find_nearest_matches(unmatched_slks, database_slks, threshold=0.5):
    results = []
    
    for u_slk in unmatched_slks:
        best_match = None
        best_similarity = -1
        
        for d_slk in database_slks:
            similarity = calculate_similarity(u_slk, d_slk)
            
            if similarity > best_similarity:
                best_match = d_slk
                best_similarity = similarity
        
        if best_similarity >= threshold:
            results.append((u_slk, best_match, best_similarity))
        else:
            results.append((u_slk, None, best_similarity))
    
    return results


# import numpy as np

# def convert_format_datestr(date_string:str, from_format:str, to_format:str) -> tuple[str, date]:
#   date_dt = datetime.strptime(date_string, from_format)
#   date1 = date_dt.strftime(to_format)
#   return date1, date_dt.date()


def get_date_from_str(date_string:str, from_format:str) -> date:
  date_dt = datetime.strptime(date_string, from_format)
  return  date_dt.date()

def is_numeric(value):
    try:
        float(value)
        return True
    except ValueError:
        return False
    
    
def range_average(range_str:str, separator:str='-'):
  
  if is_numeric(range_str):
    return float(range_str)  
  
  elif separator in range_str:
    two_ints = range_str.split(separator)
   
  else:
    two_ints = range_str.split(' ')
  
  return (int(two_ints[0])+int(two_ints[-1]))/2
    # return np.nan
  

# # Function to safely parse JSON and handle errors
# def clean_and_parse_json(s:str):
    
#     # import mylogging
#     # logging = mylogging.get(__name__)
#     try:
#         cleaned_string = s.replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
#         return json.loads(cleaned_string)
#     except json.JSONDecodeError as e:
#         logging.error(f"Error parsing JSON: {e}")
#         logging.error(f"Problematic data: {s}")
#         # Return None or some default value if JSON is invalid
#         return None
def clean_and_parse_json(s: str):
    try:
        replacements = {
            '\n': '\\n',
            '\r': '\\r',
            '\t': '\\t'
        }
        cleaned_string = s.translate(str.maketrans(replacements))
        return json.loads(cleaned_string)
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing JSON: {e}")
        logging.error(f"Problematic data: {s}")
        return {}  # Return an empty dictionary or any other default value