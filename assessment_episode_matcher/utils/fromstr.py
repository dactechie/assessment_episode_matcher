import logging
import json
from datetime import datetime, date
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