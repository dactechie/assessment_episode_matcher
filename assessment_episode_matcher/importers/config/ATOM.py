
# Define the categories and their corresponding names in the data
MAP_AOD_GROUPINGS = {
    'Alcohol': ['Ethanol','Alcohols, n.e.c.'],
    'Heroin': ['Heroin'],
    'Other Opioids': ['Oxycodone','Pharmaceutical Opioids','Pharmaceutical Opioids, n.f.d.', 'Methadone', 'Opioid Antagonists, nec'],
    # Fentanyl, Tramadol, COdeine, Morphine

    'Cocaine': ['Cocaine'],
    'Cannabis': ['Cannabinoids and Related Drugs, n.f.d.', 'Cannabinoids and related drugs, n.f.d.', 'Cannabinoids'],
    'Amphetamines': ['Amphetamines, n.f.d.', 'Amphetamines, n.f.d', 'Methamphetamine','Dexamphetamine'],     
    'Benzodiazepines': ['Benzodiazepines, nec', 'Benzodiazepines, n.e.c.', 'Benzodiazepines, n.f.d', 'Benzodiazepines, n.f.d.','Diazepam' ],
    # 'Another Drug':  [
    #    'Opioid Antagonists, n.e.c.','Volatile Nitrates, n.e.c.', 'Lithium',
    #    'Other','Other Drugs of Concern', 'Psychostimulants, n.f.d.','Zolpidem', 'Caffeine',  'MDMA/Ecstasy',  
    #                     'Gamma-hydroxybutyrate','Dexamphetamine','GHB type Drugs and Analogues, n.e.c.',
    #                     'Psilocybin or Psilocin', 'Amyl nitrate', 'Other Volatile Solvents, n.e.c.' ],
    'Nicotine': ['Nicotine'],
    'Gambling':['Gambling'],
    'Another Drug1':[], # h ack for expand_drug_info to work ->  for drug_cat in nada_drug_days_categories.keys():
    'Another Drug2': [] # h ack for expand_drug_info to work ->  for drug_cat in nada_drug_days_categories.keys():
}

