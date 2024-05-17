
import pandas as pd

def check_ep_length(ep_df:pd.DataFrame) -> pd.DataFrame:
  
  eps_exceeding_year = ep_df[(ep_df['EndDate'] - ep_df['CommencementDate']).dt.days.abs() > 365]
  return eps_exceeding_year

  # if eps_exceeding_year.empty:
  #   return True