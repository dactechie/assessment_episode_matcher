# import logging
import pandas as pd
from assessment_episode_matcher.data_config import PDC_ODC_ATOMfield_names as PDC_ODC_fields
from assessment_episode_matcher.utils.fromstr import range_average
from assessment_episode_matcher.utils.df_ops_base import drop_fields
from assessment_episode_matcher.mytypes import AODWarning

def get_drug_category(drug_name:str, aod_groupings:dict) -> tuple[str, int]:
  for category_name, substances in aod_groupings.items():
     if drug_name in substances:
        return category_name, 1
  return drug_name, 0

def get_typical_qty(item, field_names:dict[str, str], assessment)->  tuple[float, str, str, AODWarning|None]:
  field_perocc = field_names['per_occassion']
  field_units = field_names['units']

  typical_qty = item.get(field_perocc, 0.0)
  typical_unit = item.get(field_units,'')
 
  if not typical_qty:
     warning = AODWarning(assessment['SLK'],assessment['RowKey']
                          ,drug_name=field_names['drug_name']
                          , field_name=field_perocc)
     return typical_qty, "", "", warning
  if not pd.isna(typical_qty):
      if typical_qty == '0':
         return 0.0, "", "0", None
      if typical_qty == 'Other':
         warning = AODWarning(assessment['SLK'],assessment['RowKey']
                               , drug_name=field_names['drug_name']
                          , field_name=field_perocc
                          , field_value=typical_qty)
         return 0.0, "", "", warning
      typical_qty = range_average(typical_qty)
  if not typical_unit:
     warning = AODWarning(assessment['SLK'],assessment['RowKey']
                          ,drug_name=field_names['drug_name']
                          , field_name=field_units
                          , field_value=typical_unit)
     return typical_qty, "", f"{typical_qty}", warning

  return typical_qty, typical_unit, f"{typical_qty}; {typical_unit}", None

def process_drug_list_for_assessment(pdc_odc_colname:str, assessment, config:dict):
  row_data = {}
  warnings = []
  drug_categories= config["drug_categories"]
  for item in assessment[pdc_odc_colname]:
    field_names  = PDC_ODC_fields[pdc_odc_colname]
    field_drug_name = field_names['drug_name']
    field_use_ndays = field_names['used_in_last_4wks']

    if not item:
      continue 

    substance = item.get(field_drug_name, '')    
    if not substance:
      continue

    mapped_drug, found_category = get_drug_category(substance, aod_groupings=drug_categories)
    if not found_category:
      warning = AODWarning(assessment['SLK'],assessment['RowKey']
                           ,drug_name=substance, field_name=field_drug_name)
      warnings.append(warning)
      if not row_data or not( 'Another Drug1' in row_data) or pd.isna(row_data['Another Drug1']):
        row_data['Another Drug1'] = mapped_drug
        mapped_drug ='Another Drug1'
      else:
        row_data['Another Drug2'] = mapped_drug
        mapped_drug ='Another Drug2'
       
    row_data[ f"{mapped_drug}_DaysInLast28"] = item.get(field_use_ndays,'')     
    per_occassion , typical_unit_str, typical_use_str, warning =  get_typical_qty(item, field_names, assessment)

    if per_occassion:
      row_data [ f"{mapped_drug}_PerOccassionUse"] = str(int(per_occassion))
    row_data [ f"{mapped_drug}_Units"] = typical_unit_str
    row_data [ f"{mapped_drug}_TypicalQtyStr"] = typical_use_str
    if warning:
      warnings.append(warning)

  return row_data, warnings

def normalize_pdc_odc(df:pd.DataFrame, config:dict):
  new_data = []
  warnings = []
  for index, row in df.iterrows():
    row_data = {}
    pdc_row_data ={}
    odc_row_data ={}
    if 'PDC' in row and isinstance(row['PDC'], list):
      pdc_row_data, warnings1 = process_drug_list_for_assessment('PDC', row, config)
      if warnings1:
         warnings.extend(warnings1)
    if 'ODC' in row and isinstance(row['ODC'], list):
      odc_row_data, warnings2 = process_drug_list_for_assessment('ODC', row, config)
      if warnings2:
         warnings.extend(warnings2)
    
    row_data = pdc_row_data | odc_row_data
    if row_data:
        new_data.append(row_data)
    else:
        new_data.append({})
  expanded_data = pd.DataFrame(new_data, index=df.index)   
  return expanded_data, warnings

def create_structure_masks(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    Create boolean masks for new and old structure rows
    """
    new_mask = (
        df['DrugsOfConcernDetails'].notna() & 
        df['PDCSubstanceOrGambling'].notna()
    )
    
    old_mask = (
        (df['PDC'].notna()) | 
        (df['ODC'].notna())
    )
    
    return new_mask, old_mask

NEW_TO_OLD_MAPPING = {
    'DrugsOfConcern': 'PDCSubstanceOrGambling',  
    'MethodOfUse': 'PDCMethodOfUse',
    'DaysInLast28': 'PDCDaysInLast28',
    'Units': 'PDCUnits',
    'HowMuchPerOccasion': 'PDCHowMuchPerOccasion',
    'Goals': 'PDCGoals'
}

def convert_new_to_old_structure(df: pd.DataFrame) -> pd.DataFrame:
    """Convert new drug info structure to old structure for compatibility"""
    df = df.copy()
    
    # Initialize PDC with single-item list placeholder, ODC with empty list
    if 'PDC' not in df.columns:
        df['PDC'] = None # pd.Series([[None]] * len(df), index=df.index)  # Single-item list
    if 'ODC' not in df.columns:
        df['ODC'] = None #pd.Series([[] for _ in range(len(df))], index=df.index)  # Empty list
    
    # For each row, create PDC and ODC lists
    for idx in df.index:  # Use actual index values
        row = df.loc[idx]
        pdc_substance = row.get('PDCSubstanceOrGambling')
        drugs_list = row.get('DrugsOfConcernDetails', [])
        
        pdc_item = None
        odc_list = []
        
        for drug in drugs_list:
            if drug['DrugsOfConcern'] == pdc_substance:
                # PDC is always a single item
                pdc_item = {
                    'PDCSubstanceOrGambling': drug['DrugsOfConcern'],
                    'PDCMethodOfUse': drug['MethodOfUse'],
                    'PDCDaysInLast28': drug['DaysInLast28'],
                    'PDCUnits': drug.get('Units', ''),
                    'PDCHowMuchPerOccasion': drug.get('HowMuchPerOccasion', ''),
                    'PDCGoals': drug.get('Goals', '')
                }
            else:
                # ODC can have 0-5 items
                odc_item = {
                    'OtherSubstancesConcernGambling': drug['DrugsOfConcern'],
                    'MethodOfUse': drug['MethodOfUse'],
                    'DaysInLast28': drug['DaysInLast28'],
                    'Units': drug.get('Units', ''),
                    'HowMuchPerOccasion': drug.get('HowMuchPerOccasion', ''),
                    'Goals': drug.get('Goals', '')
                }
                odc_list.append(odc_item)
        
        # Set PDC (always single item)
        if pdc_item:
            df._set_value(idx, 'PDC', [pdc_item])
            
        # Set ODC (0-5 items)
        if odc_list:
            df._set_value(idx, 'ODC', odc_list)
    
    # Drop the new structure columns
    df = drop_fields(df, ['DrugsOfConcernDetails'])
    
    return df

def expand_drug_info(df1: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, list[AODWarning]]:
    """Expand drug information handling mixed structures efficiently"""
    new_mask, old_mask = create_structure_masks(df1)
    
    # Split DataFrame into new and old structures
    df_new = df1[new_mask].copy() if new_mask.any() else pd.DataFrame()
    df_old = df1[old_mask].copy() if old_mask.any() else pd.DataFrame()
    
    # Process each structure type
    results = []
    all_warnings = []
    
    if not df_old.empty:
        expanded_old, warnings_old = normalize_pdc_odc(df_old, config)
        results.append(expanded_old)
        all_warnings.extend(warnings_old)
    
    if not df_new.empty:
        df_new_converted = convert_new_to_old_structure(df_new)
        expanded_new, warnings_new = normalize_pdc_odc(df_new_converted, config)
        results.append(expanded_new)
        all_warnings.extend(warnings_new)
    
    # Handle invalid rows (neither new nor old structure)
    invalid_mask = ~(new_mask | old_mask)
    if invalid_mask.any():
        invalid_rows = df1[invalid_mask]
        for idx, row in invalid_rows.iterrows():
            warning = AODWarning(
                row.get('SLK', ''),
                row.get('RowKey', ''),
                drug_name='',
                field_name='structure',
                field_value='Invalid structure - missing required fields'
            )
            all_warnings.append(warning)
    
    # Combine results maintaining original index order
    if results:
        combined_df = pd.concat(results)
        combined_df = combined_df.reindex(df1.index)
    else:
        combined_df = pd.DataFrame(index=df1.index)
    
    # Create a copy of original df without PDC/ODC columns
    preserved_df = drop_fields(df1.copy(), ['PDC', 'ODC'])
    
    # Join the normalized drug data with preserved columns
    final_df = preserved_df.join(combined_df)
    
    return final_df, all_warnings

if __name__ == "__main__":
    # Sample config
    config = {
        "drug_categories": {
            "Cannabis": ["Cannabinoids", "Cannabis"],
            "Alcohol": ["Ethanol"],
            "Stimulants": ["Caffeine", "Psychostimulants, n.f.d.", "MDMA/Ecstasy"]
        }
    }

    # Test setting ODC list in NaN cell with non-contiguous indices
    df_nan_test = pd.DataFrame({
        'PartitionKey': ['ABC', 'DEF', 'GHI'],
        'RowKey': ['rk115', 'rk171', 'rk202'],
        'SLK': ['SLK1', 'SLK2', 'SLK3']
    }, index=[115, 171, 202])  # Non-contiguous indices
    
    # Initialize columns with NaN
    df_nan_test['PDC'] = pd.NA
    df_nan_test['ODC'] = pd.NA
    df_nan_test['DrugsOfConcernDetails'] = pd.NA
    df_nan_test['PDCSubstanceOrGambling'] = pd.NA

    # Test case that would have caught the bug:
    # Setting ODC list in NaN cell at non-contiguous index
    df_nan_test._set_value(171, 'DrugsOfConcernDetails', [
        {
            'DrugsOfConcern': 'Cannabis',
            'MethodOfUse': 'Smoke',
            'DaysInLast28': '5',
            'Units': 'cones / joints',
            'HowMuchPerOccasion': '3'
        },
        {
            'DrugsOfConcern': 'Nicotine',
            'MethodOfUse': 'Inhale',
            'DaysInLast28': '0',
            'Units': 'dosage',
            'HowMuchPerOccasion': '0'
        }
    ])
    df_nan_test.loc[171, 'PDCSubstanceOrGambling'] = 'Cannabis'  # Not a list, can use loc

    print("\nTesting NaN handling with non-contiguous indices:")
    print("\nInput DataFrame:")
    print(df_nan_test)
    print("\nODC column before conversion:")
    print(df_nan_test['ODC'])
    out_nan_test, warnings_nan_test = expand_drug_info(df_nan_test, config)
    print("\nOutput after conversion:")
    print(out_nan_test)
    print("\nWarnings:")
    for w in warnings_nan_test:
        print(w)

    # Test non-contiguous index handling
    df_noncontiguous = pd.DataFrame({
        'PartitionKey': ['ABC', 'DEF', 'GHI'],
        'RowKey': ['rk115', 'rk171', 'rk202'],
        'SLK': ['SLK1', 'SLK2', 'SLK3']
    }, index=[115, 171, 202])  # Non-contiguous indices
    
    # Initialize columns with None
    df_noncontiguous['PDC'] = None
    df_noncontiguous['ODC'] = None
    df_noncontiguous['DrugsOfConcernDetails'] = None
    df_noncontiguous['PDCSubstanceOrGambling'] = None

    # Set test data using _set_value for list values
    df_noncontiguous._set_value(115, 'PDC', [{
        'PDCSubstanceOrGambling': 'Cannabinoids',
        'PDCDaysInLast28': '20',
        'PDCHowMuchPerOccasion': '55.0',
        'PDCUnits': 'blunts'
    }])
    df_noncontiguous._set_value(171, 'DrugsOfConcernDetails', [{
        'DrugsOfConcern': 'Ethanol',
        'MethodOfUse': 'Ingest',
        'DaysInLast28': '20',
        'Units': 'standard drinks',
        'HowMuchPerOccasion': '6'
    }])
    df_noncontiguous.loc[171, 'PDCSubstanceOrGambling'] = 'Ethanol'  # Not a list, can use loc
    df_noncontiguous._set_value(202, 'ODC', [{
        'OtherSubstancesConcernGambling': 'MDMA/Ecstasy',
        'DaysInLast28': '4',
        'Units': 'pills'
    }])

    print("\nTesting non-contiguous indices:")
    print("\nInput DataFrame:")
    print(df_noncontiguous)
    out_noncontiguous, warnings_noncontiguous = expand_drug_info(df_noncontiguous, config)
    print("\nOutput for non-contiguous indices:")
    print(out_noncontiguous)
    print("\nWarnings for non-contiguous indices:")
    for w in warnings_noncontiguous:
        print(w)

    # Test column preservation and reindexing
    df_preservation = pd.DataFrame({
        'PartitionKey': ['ABC', 'DEF', 'GHI'],
        'RowKey': ['rk115', 'rk171', 'rk202'],
        'SLK': ['SLK1', 'SLK2', 'SLK3'],
        'Age': [25, 30, 35],  # Non-drug column
        'Gender': ['M', 'F', 'M'],  # Non-drug column
        'Location': ['Sydney', 'Melbourne', 'Brisbane']  # Non-drug column
    }, index=[115, 171, 202])  # Non-contiguous indices

    # Add drug data
    df_preservation['PDC'] = None
    df_preservation['ODC'] = None
    df_preservation['DrugsOfConcernDetails'] = None
    df_preservation['PDCSubstanceOrGambling'] = None

    # Row 1: Old structure
    df_preservation._set_value(115, 'PDC', [{
        'PDCSubstanceOrGambling': 'Cannabinoids',
        'PDCDaysInLast28': '20',
        'PDCHowMuchPerOccasion': '55.0',
        'PDCUnits': 'blunts'
    }])

    # Row 2: New structure
    df_preservation.loc[171, 'PDCSubstanceOrGambling'] = 'Ethanol'
    df_preservation._set_value(171, 'DrugsOfConcernDetails', [{
        'DrugsOfConcern': 'Ethanol',
        'MethodOfUse': 'Ingest',
        'DaysInLast28': '20',
        'Units': 'standard drinks',
        'HowMuchPerOccasion': '6'
    }])

    # Row 3: Mixed data
    df_preservation._set_value(202, 'ODC', [{
        'OtherSubstancesConcernGambling': 'MDMA/Ecstasy',
        'DaysInLast28': '4',
        'Units': 'pills'
    }])

    print("\nTesting column preservation and reindexing:")
    print("\nInput DataFrame with non-drug columns:")
    print(df_preservation)
    out_preservation, warnings_preservation = expand_drug_info(df_preservation, config)

    print("\nOutput DataFrame:")
    print(out_preservation)

    # Verify column preservation
    print("\nVerifying column preservation:")
    original_cols = ['PartitionKey', 'RowKey', 'SLK', 'Age', 'Gender', 'Location']
    for col in original_cols:
        if col not in out_preservation.columns:
            print(f"ERROR: Column {col} was lost")
        else:
            print(f"Column {col} preserved")

    # Verify row integrity
    print("\nVerifying row integrity:")
    for idx in df_preservation.index:
        original_row = df_preservation.loc[idx]
        output_row = out_preservation.loc[idx]
        
        # Check demographic data stayed with correct row
        for col in original_cols:
            if original_row[col] != output_row[col]:
                print(f"ERROR: Row {idx} value mismatch in {col}")
                print(f"Original: {original_row[col]}")
                print(f"Output: {output_row[col]}")

    print("\nWarnings for preservation test:")
    for w in warnings_preservation:
        print(w)

    # Test mixed structure DataFrame
    df_mixed = pd.DataFrame({
        'PartitionKey': ['ABC', 'DEF', 'GHI', 'JKL'],
        'RowKey': ['rk1', 'rk2', 'rk3', 'rk4'],
        'SLK': ['SLK1', 'SLK2', 'SLK3', 'SLK4'],
        'PDC': [None] * 4,
        'ODC': [None] * 4,
        'DrugsOfConcernDetails': [None] * 4,
        'PDCSubstanceOrGambling': [None] * 4
    })

    # Row 1: Old structure
    df_mixed._set_value(0, 'PDC', [{
        'PDCSubstanceOrGambling': 'Cannabinoids',
        'PDCDaysInLast28': '20',
        'PDCHowMuchPerOccasion': '55.0',
        'PDCUnits': 'blunts'
    }])
    df_mixed._set_value(0, 'ODC', [{
        'OtherSubstancesConcernGambling': 'Caffeine',
        'DaysInLast28': '10',
        'HowMuchPerOccasion': '50-59'
    }])

    # Row 2: New structure
    df_mixed.loc[1, 'PDCSubstanceOrGambling'] = 'Ethanol'  # Not a list, can use loc
    df_mixed._set_value(1, 'DrugsOfConcernDetails', [
        {
            'DrugsOfConcern': 'Ethanol',
            'MethodOfUse': 'Ingest',
            'DaysInLast28': '20',
            'Units': 'standard drinks',
            'HowMuchPerOccasion': '6'
        },
        {
            'DrugsOfConcern': 'Cannabis',
            'MethodOfUse': 'Smoke',
            'DaysInLast28': '5',
            'Units': 'cones / joints',
            'HowMuchPerOccasion': '3'
        }
    ])

    # Row 3: Old structure
    df_mixed._set_value(2, 'PDC', [{
        'PDCSubstanceOrGambling': 'MDMA/Ecstasy',
        'PDCDaysInLast28': '4',
        'PDCUnits': 'pills'
    }])

    # Row 4: Invalid structure (all None/empty)

    print("\nTesting mixed structures:")
    out_mixed, warnings_mixed = expand_drug_info(df_mixed, config)
    print("\nOutput for mixed structures:")
    print(out_mixed)
    print("\nWarnings for mixed structures:")
    for w in warnings_mixed:
        print(w)

    # Verify structure detection
    new_mask, old_mask = create_structure_masks(df_mixed)
    print("\nStructure detection results:")
    print("New structure rows:", df_mixed[new_mask].index.tolist())
    print("Old structure rows:", df_mixed[old_mask].index.tolist())
    print("Invalid rows:", df_mixed[~(new_mask | old_mask)].index.tolist())
