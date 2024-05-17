# C:\Users\aftab.jalal\Directions Health\Directions Health Intranet - Reporting\ATOM\Documentation\ATOM_MDS_MatchingAnalysis.docx
# Change cell A1 from ESTABLISHMENT IDENTIFIER to PartitionKey  
# from EPISODE ID to RowKey
# Replace All single space with a underscore

# MEDICARE_NUMBER	PROPERTY_NAME	UNIT_FLAT_NUMBER	STREET_NUMBER	STREET_NAME	SUBURB
import pandas as pd
from utils.df_ops_base import drop_fields

cols_to_remove =  ['FAMILY_NAME',	'GIVEN_NAME',	'MIDDLE_NAME',
                   	'TITLE',
                   'MEDICARE_NUMBER', 'PROPERTY_NAME',	'UNIT_FLAT_NUMBER',
                     	'STREET_NUMBER',	'STREET_NAME',
                        	'SUBURB',
                          'END_REASON',  'REFERRAL_TO_ANOTHER_SERVICE',
                          'COUNTRY_OF_BIRTH',
                          'PREFERRED_LANGUAGE',
                          'SOURCE_OF_INCOME','LIVING_ARRANGEMENT','USUAL_ACCOMMODATION',
                          'PRINCIPAL_DRUG_OF_CONCERN',
                          'POSTCODE',	'SOURCE_OF_REFERRAL',	'MAIN_SERVICE', 'SETTING',
                          'ILLICIT_USE', 'METHOD_OF_USE_PRINCIPAL_DRUG','INJECTING_DRUG_USE',
                          'INDIGENOUS_STATUS', 
                          'DOB',	'DOB_STATUS',	'SEX',	'CLIENT_TYPE'
                          ]

def adjust_ccare_csv_for_aztable(df:pd.DataFrame):

  df.columns = [col.replace(' ', '_') for col in df.columns]
  new_df = drop_fields(df, fieldnames=cols_to_remove)
  new_df2 = new_df.rename(columns={'ESTABLISHMENT_IDENTIFIER': 'PartitionKey', 
                            'EPISODE_ID': 'RowKey'})
  new_df2.fillna('', inplace=True)

  return new_df2
