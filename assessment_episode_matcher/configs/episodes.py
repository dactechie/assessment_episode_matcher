  # List of columns we care about
  
date_cols=['START DATE', 'END DATE']

columns_of_interest = ['ESTABLISHMENT IDENTIFIER', 'GEOGRAPHICAL LOCATION'
                         , 'EPISODE ID','PERSON ID', 'SPECIFY DRUG OF CONCERN'
                         , 'PRINCIPAL DRUG OF CONCERN'
                         , 'START DATE', 'END DATE', 'SLK']
rename_columns = {
    'SPECIFY DRUG OF CONCERN': 'PDCSubstanceOfConcern',
    'PRINCIPAL DRUG OF CONCERN': 'PDCCode',
    'START DATE': 'CommencementDate', 'END DATE': 'EndDate',
    'EPISODE ID': 'PMSEpisodeID', 'PERSON ID': 'PMSPersonID',    
}