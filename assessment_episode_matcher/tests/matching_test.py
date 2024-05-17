import sys
import pandas as pd

# sys.path.append('~/dev/NADATools_AzFunc')
from . import matching

import pytest

t = {
  'atoms_df':pd.DataFrame({'SLK':[ '1MidOfMyEp'],
              'RowKey':[ 'rk1'],'Program':['TSS'  ],
              'AssessmentDate':pd.to_datetime([ '2023-07-01' ])    
               }), 
        'episodes_df': pd.DataFrame({
            'SLK':['1MidOfMyEp', '1MidOfMyEp',],
            'Program':[ 'TSS', 'TSS'],
            'CommencementDate': pd.to_datetime([
                                                '2023-06-01', '2023-07-01'
                                                ]),
            'EndDate': [
                        pd.to_datetime('2024-08-23'),
                          pd.to_datetime('2024-06-23')
                        ],
          })
}
@pytest.mark.parametrize("matching_ndays_slack", [7])
def test_match_with_dates(matching_ndays_slack):
  # matching_ndays_slack  = 7
  matching_keys=['SLK', 'Program']
  
  slk_program_merged = pd.merge(t['episodes_df'], t['atoms_df']
                                , how='inner'
                                , left_on=matching_keys
                                , right_on=matching_keys)
  
  
  
  matched_df = matching.match_with_dates(slk_program_merged
                                         , matching_ndays_slack)
  print(matched_df)