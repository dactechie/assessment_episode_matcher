
import pandas as pd

def get_unmatched_stats(unmatched_atoms:pd.DataFrame, matched_df:pd.DataFrame, ep_df:pd.DataFrame):
  raise NotImplementedError()
    # unmatched having an SLK in ep_df:
  # unmatched_atoms_withep = unmatched_atoms[ unmatched_atoms.SLK.isin(ep_df.SLK.unique())]
  # # mask =  (unmatched_atoms.AssessmentDate>= active_clients_start_date ) & (unmatched_atoms.AssessmentDate <= active_clients_end_date)
  # # unmatched_atoms_inperiod =   unmatched_atoms.loc[mask]
  # unmatched_pc = round(len(unmatched_atoms_inperiod)*100/len(atom_df),1)

  # # episodes without ATOM match
  # matched_slks  = matched_df.SLK.unique()
  # eps_no_assessment = ep_df[ ~ep_df.SLK.isin(matched_slks)]
  
  # return {
  #   f"Unmatched Atoms": len(unmatched_atoms_inperiod),
  #   "Unmatched Percentage" : {unmatched_pc}% ,
  #   f"Without Episode (slack {matching_ndays_slack}": len(unmatched_atoms_withep),\
  #   f"SLKs unmatched : {len(unmatched_atoms_withep['SLK'].unique())}"
  # }