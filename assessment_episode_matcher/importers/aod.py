# import logging
import pandas as pd
from assessment_episode_matcher.data_config import PDC_ODC_ATOMfield_names as PDC_ODC_fields
from assessment_episode_matcher.utils.fromstr import range_average
from assessment_episode_matcher.utils.df_ops_base import drop_fields
from assessment_episode_matcher.mytypes import AODWarning

def get_drug_category(drug_name:str, aod_groupings:dict) -> tuple[str, int]:
  """
  Find a drug name in a dictionary of drug categories and return the category name and a flag.
  
  Args:
      drug_name: Name of the drug to categorize
      aod_groupings: Dictionary mapping category names to lists of substances
      
  Returns:
      Tuple of (category_name, found_flag) where found_flag is 1 if found, 0 if not
  """
  try:
    # Validate inputs
    if not drug_name or not isinstance(drug_name, str):
      return "", 0
      
    if not aod_groupings or not isinstance(aod_groupings, dict):
      return drug_name, 0
    
    # Search for drug in categories
    for category_name, substances in aod_groupings.items():
      if not isinstance(substances, list):
        continue
        
      if drug_name in substances:
        return category_name, 1
        
    # Drug not found in any category
    return drug_name, 0
    
  except Exception:
    # Return original drug name if any error occurs
    return drug_name, 0

def get_typical_qty(item, field_names:dict[str, str], assessment)->  tuple[float, str, str, AODWarning|None]:
  """
  Extract and process the typical quantity of a drug from an assessment item.
  
  Args:
      item: Dictionary containing drug information
      field_names: Dictionary mapping field types to actual field names
      assessment: Row from DataFrame containing assessment information
      
  Returns:
      Tuple of (quantity, unit, formatted_string, warning)
  """
  try:
    # Validate inputs
    if not isinstance(item, dict):
      return 0.0, "", "", AODWarning(
        assessment.get('SLK', ''),
        assessment.get('RowKey', ''),
        drug_name='',
        field_name='item',
        field_value="Item is not a dictionary"
      )
      
    if not isinstance(field_names, dict) or 'per_occassion' not in field_names or 'units' not in field_names:
      return 0.0, "", "", AODWarning(
        assessment.get('SLK', ''),
        assessment.get('RowKey', ''),
        drug_name='',
        field_name='field_names',
        field_value="Invalid field_names dictionary"
      )
    
    # Get field names
    field_perocc = field_names['per_occassion']
    field_units = field_names['units']
    field_drug_name = field_names.get('drug_name', '')
    
    # Get values from item
    typical_qty = item.get(field_perocc, 0.0)
    typical_unit = item.get(field_units, '')
    
    # Handle missing quantity
    if not typical_qty:
      warning = AODWarning(
        assessment.get('SLK', ''),
        assessment.get('RowKey', ''),
        drug_name=field_drug_name,
        field_name=field_perocc
      )
      return 0.0, "", "", warning
      
    # Process quantity value
    if not pd.isna(typical_qty):
      # Handle special values
      if typical_qty == '0':
        return 0.0, "", "0", None
        
      if typical_qty == 'Other':
        warning = AODWarning(
          assessment.get('SLK', ''),
          assessment.get('RowKey', ''),
          drug_name=field_drug_name,
          field_name=field_perocc,
          field_value=typical_qty
        )
        return 0.0, "", "", warning
        
      # Convert range to average
      try:
        typical_qty = range_average(typical_qty)
      except Exception as e:
        warning = AODWarning(
          assessment.get('SLK', ''),
          assessment.get('RowKey', ''),
          drug_name=field_drug_name,
          field_name=field_perocc,
          field_value=f"Error converting range: {str(e)}"
        )
        return 0.0, "", "", warning
    
    # Handle missing unit
    if not typical_unit:
      warning = AODWarning(
        assessment.get('SLK', ''),
        assessment.get('RowKey', ''),
        drug_name=field_drug_name,
        field_name=field_units,
        field_value=typical_unit
      )
      return typical_qty, "", f"{typical_qty}", warning
    
    # Return complete information
    return typical_qty, typical_unit, f"{typical_qty}; {typical_unit}", None
    
  except Exception as e:
    # Catch-all for any unexpected errors
    return 0.0, "", "", AODWarning(
      assessment.get('SLK', ''),
      assessment.get('RowKey', ''),
      drug_name=field_names.get('drug_name', ''),
      field_name='typical_qty',
      field_value=f"Unexpected error: {str(e)}"
    )

def process_drug_list_for_assessment(pdc_odc_colname:str, assessment, config:dict):
  """
  Process a list of drugs for an assessment, extracting information like drug names, 
  usage days, and typical quantities.
  
  Args:
      pdc_odc_colname: Column name ('PDC' or 'ODC')
      assessment: Row from DataFrame containing drug information
      config: Dictionary containing drug categories configuration
      
  Returns:
      Tuple of (row_data, warnings)
  """
  row_data = {}
  warnings = []
  
  # Validate inputs
  if pdc_odc_colname not in PDC_ODC_fields:
    warnings.append(AODWarning(
      assessment.get('SLK', ''),
      assessment.get('RowKey', ''),
      drug_name='',
      field_name='column_name',
      field_value=f"Invalid column name: {pdc_odc_colname}"
    ))
    return row_data, warnings
    
  if pdc_odc_colname not in assessment or not isinstance(assessment[pdc_odc_colname], list):
    return row_data, warnings
    
  if "drug_categories" not in config:
    warnings.append(AODWarning(
      assessment.get('SLK', ''),
      assessment.get('RowKey', ''),
      drug_name='',
      field_name='config',
      field_value="Missing drug_categories in config"
    ))
    return row_data, warnings
    
  drug_categories = config["drug_categories"]
  field_names = PDC_ODC_fields[pdc_odc_colname]
  field_drug_name = field_names['drug_name']
  field_use_ndays = field_names['used_in_last_4wks']
  
  # Process each item in the drug list
  for item in assessment[pdc_odc_colname]:
    # Skip empty or non-dict items
    if not item or not isinstance(item, dict):
      continue 

    # Get substance name
    substance = item.get(field_drug_name, '')    
    if not substance:
      continue

    try:
      # Map drug to category
      mapped_drug, found_category = get_drug_category(substance, aod_groupings=drug_categories)
      
      # Handle unmapped drugs
      if not found_category:
        warning = AODWarning(
          assessment.get('SLK', ''),
          assessment.get('RowKey', ''),
          drug_name=substance, 
          field_name=field_drug_name
        )
        warnings.append(warning)
        
        # Assign to "Another Drug" category
        if not row_data or not('Another Drug1' in row_data) or pd.isna(row_data.get('Another Drug1')):
          row_data['Another Drug1'] = mapped_drug
          mapped_drug = 'Another Drug1'
        else:
          row_data['Another Drug2'] = mapped_drug
          mapped_drug = 'Another Drug2'
      
      # Add days in last 28 days
      days_value = item.get(field_use_ndays, '')
      row_data[f"{mapped_drug}_DaysInLast28"] = days_value
      
      # Get typical quantity information
      try:
        per_occassion, typical_unit_str, typical_use_str, warning = get_typical_qty(item, field_names, assessment)
        
        if per_occassion:
          try:
            row_data[f"{mapped_drug}_PerOccassionUse"] = str(int(per_occassion))
          except (ValueError, TypeError):
            row_data[f"{mapped_drug}_PerOccassionUse"] = str(per_occassion)
            
        row_data[f"{mapped_drug}_Units"] = typical_unit_str
        row_data[f"{mapped_drug}_TypicalQtyStr"] = typical_use_str
        
        if warning:
          warnings.append(warning)
          
      except Exception as e:
        warnings.append(AODWarning(
          assessment.get('SLK', ''),
          assessment.get('RowKey', ''),
          drug_name=substance,
          field_name='typical_qty',
          field_value=f"Error processing typical quantity: {str(e)}"
        ))
        
    except Exception as e:
      warnings.append(AODWarning(
        assessment.get('SLK', ''),
        assessment.get('RowKey', ''),
        drug_name=substance if substance else '',
        field_name=pdc_odc_colname,
        field_value=f"Error processing drug: {str(e)}"
      ))

  return row_data, warnings

def normalize_pdc_odc(df:pd.DataFrame, config:dict):
  """
  Normalize PDC (Principal Drug of Concern) and ODC (Other Drugs of Concern) data from a DataFrame.
  
  Args:
      df: DataFrame containing PDC and/or ODC columns
      config: Dictionary containing drug categories configuration
      
  Returns:
      Tuple of (expanded_data, warnings)
  """
  if df.empty:
    return pd.DataFrame(index=df.index), []
    
  new_data = []
  warnings = []
  
  for index, row in df.iterrows():
    row_data = {}
    pdc_row_data = {}
    odc_row_data = {}
    
    # Process PDC (Principal Drug of Concern)
    if 'PDC' in row and isinstance(row['PDC'], list) and row['PDC']:
      try:
        pdc_row_data, warnings1 = process_drug_list_for_assessment('PDC', row, config)
        if warnings1:
          warnings.extend(warnings1)
      except Exception as e:
        warnings.append(AODWarning(
          row.get('SLK', ''),
          row.get('RowKey', ''),
          drug_name='',
          field_name='PDC',
          field_value=f"Error processing PDC: {str(e)}"
        ))
    
    # Process ODC (Other Drugs of Concern)
    if 'ODC' in row and isinstance(row['ODC'], list) and row['ODC']:
      try:
        odc_row_data, warnings2 = process_drug_list_for_assessment('ODC', row, config)
        if warnings2:
          warnings.extend(warnings2)
      except Exception as e:
        warnings.append(AODWarning(
          row.get('SLK', ''),
          row.get('RowKey', ''),
          drug_name='',
          field_name='ODC',
          field_value=f"Error processing ODC: {str(e)}"
        ))
    
    # Combine PDC and ODC data
    row_data = pdc_row_data | odc_row_data
    if row_data:
      new_data.append(row_data)
    else:
      new_data.append({})
      
  # Create DataFrame with same index as input
  expanded_data = pd.DataFrame(new_data, index=df.index)   
  return expanded_data, warnings

def create_structure_masks(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    Create boolean masks for new and old structure rows.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Tuple of (new_mask, old_mask) where each mask is a boolean Series
    """
    try:
        # Validate input
        if not isinstance(df, pd.DataFrame):
            return pd.Series(False), pd.Series(False)
            
        if df.empty:
            return pd.Series(False, index=df.index), pd.Series(False, index=df.index)
        
        # Check if required columns exist for new structure
        has_new_structure_cols = all(col in df.columns for col in ['DrugsOfConcernDetails', 'PDCSubstanceOrGambling'])
        
        # Check if required columns exist for old structure
        has_old_structure_cols = any(col in df.columns for col in ['PDC', 'ODC'])
        
        # Create mask for new structure
        if has_new_structure_cols:
            try:
                # Create mask for rows with both columns non-null
                new_mask = (
                    df['DrugsOfConcernDetails'].notna() & 
                    df['PDCSubstanceOrGambling'].notna()
                )
            except Exception:
                # Fallback if mask creation fails
                new_mask = pd.Series(False, index=df.index)
        else:
            new_mask = pd.Series(False, index=df.index)
        
        # Create mask for old structure
        if has_old_structure_cols:
            try:
                # Collect masks for each column
                old_mask_parts = []
                if 'PDC' in df.columns:
                    old_mask_parts.append(df['PDC'].notna())
                if 'ODC' in df.columns:
                    old_mask_parts.append(df['ODC'].notna())
                
                # Combine masks with OR operation
                if old_mask_parts:
                    old_mask = pd.concat(old_mask_parts, axis=1).any(axis=1)
                else:
                    old_mask = pd.Series(False, index=df.index)
            except Exception:
                # Fallback if mask creation fails
                old_mask = pd.Series(False, index=df.index)
        else:
            old_mask = pd.Series(False, index=df.index)
        
        return new_mask, old_mask
        
    except Exception:
        # Return empty masks if any error occurs
        if isinstance(df, pd.DataFrame) and not df.empty:
            return pd.Series(False, index=df.index), pd.Series(False, index=df.index)
        else:
            return pd.Series(False), pd.Series(False)

NEW_TO_OLD_MAPPING = {
    'DrugsOfConcern': 'PDCSubstanceOrGambling',  
    'MethodOfUse': 'PDCMethodOfUse',
    'DaysInLast28': 'PDCDaysInLast28',
    'Units': 'PDCUnits',
    'HowMuchPerOccasion': 'PDCHowMuchPerOccasion',
    'Goals': 'PDCGoals'
}

def convert_new_to_old_structure(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert new drug info structure to old structure for compatibility.
    
    Args:
        df: DataFrame containing drug information in new structure
        
    Returns:
        DataFrame with drug information converted to old structure
    """
    try:
        # Validate input
        if not isinstance(df, pd.DataFrame):
            return pd.DataFrame()
            
        if df.empty:
            return df.copy()
            
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Check if required columns exist
        has_new_structure = 'DrugsOfConcernDetails' in df.columns and 'PDCSubstanceOrGambling' in df.columns
        if not has_new_structure:
            # If no new structure columns, just return the DataFrame as is
            return df
        
        # Initialize PDC with single-item list placeholder, ODC with empty list
        if 'PDC' not in df.columns:
            df['PDC'] = pd.Series([[]] * len(df), index=df.index)  # Empty list for each row
        if 'ODC' not in df.columns:
            df['ODC'] = pd.Series([[] for _ in range(len(df))], index=df.index)  # Empty list for each row
        
        # For each row, create PDC and ODC lists
        for idx in df.index:  # Use actual index values
            try:
                row = df.loc[idx]
                pdc_substance = row.get('PDCSubstanceOrGambling')
                drugs_list = row.get('DrugsOfConcernDetails', [])
                
                # Skip if drugs_list is not a list
                if not isinstance(drugs_list, list):
                    continue
                    
                pdc_item = None
                odc_list = []
                
                # Process each drug in the list
                for drug in drugs_list:
                    # Skip if drug is not a dictionary
                    if not isinstance(drug, dict):
                        continue
                        
                    # Get drug name
                    drug_name = drug.get('DrugsOfConcern')
                    if not drug_name:
                        continue
                        
                    # Check if this is the PDC or an ODC
                    if drug_name == pdc_substance:
                        # PDC is always a single item
                        pdc_item = {
                            'PDCSubstanceOrGambling': drug_name,
                            'PDCMethodOfUse': drug.get('MethodOfUse', ''),
                            'PDCDaysInLast28': drug.get('DaysInLast28', 0),
                            'PDCUnits': drug.get('Units', ''),
                            'PDCHowMuchPerOccasion': drug.get('HowMuchPerOccasion', ''),
                            'PDCGoals': drug.get('Goals', '')
                        }
                    else:
                        # ODC can have 0-5 items
                        odc_item = {
                            'OtherSubstancesConcernGambling': drug_name,
                            'MethodOfUse': drug.get('MethodOfUse', ''),
                            'DaysInLast28': drug.get('DaysInLast28', 0),
                            'Units': drug.get('Units', ''),
                            'HowMuchPerOccasion': drug.get('HowMuchPerOccasion', ''),
                            'Goals': drug.get('Goals', '')
                        }
                        odc_list.append(odc_item)
                
                # Set PDC (always single item)
                if pdc_item:
                    try:
                        df.at[idx, 'PDC'] = [pdc_item]
                    except Exception:
                        # If at fails, try loc
                        df.loc[idx, 'PDC'] = [pdc_item]
                        
                # Set ODC (0-5 items)
                if odc_list:
                    try:
                        df.at[idx, 'ODC'] = odc_list
                    except Exception:
                        # If at fails, try loc
                        df.loc[idx, 'ODC'] = odc_list
                        
            except Exception:
                # Skip this row if any error occurs
                continue
        
        # Drop the new structure columns
        try:
            df = drop_fields(df, ['DrugsOfConcernDetails'])
        except Exception:
            # If drop_fields fails, try to drop manually
            if 'DrugsOfConcernDetails' in df.columns:
                df = df.drop('DrugsOfConcernDetails', axis=1)
        
        return df
        
    except Exception:
        # Return empty DataFrame if any error occurs
        return pd.DataFrame()

def expand_drug_info(df1: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, list[AODWarning]]:
    """
    Expand drug information handling mixed structures efficiently.
    
    Args:
        df1: DataFrame containing drug information in either new or old structure
        config: Dictionary containing drug categories configuration
        
    Returns:
        Tuple of (final_df, all_warnings)
    """
    # Handle empty DataFrame
    if df1.empty:
        return pd.DataFrame(), []
        
    try:
        # Create structure masks
        new_mask, old_mask = create_structure_masks(df1)
        
        # Split DataFrame into new and old structures
        df_new = df1[new_mask].copy() if new_mask.any() else pd.DataFrame()
        df_old = df1[old_mask].copy() if old_mask.any() else pd.DataFrame()
        
        # Process each structure type
        results = []
        all_warnings = []
        
        # Process old structure (PDC/ODC)
        if not df_old.empty:
            try:
                expanded_old, warnings_old = normalize_pdc_odc(df_old, config)
                results.append(expanded_old)
                all_warnings.extend(warnings_old)
            except Exception as e:
                # Add warning for processing error
                all_warnings.append(AODWarning(
                    '',  # No specific SLK
                    '',  # No specific RowKey
                    drug_name='',
                    field_name='old_structure',
                    field_value=f"Error processing old structure: {str(e)}"
                ))
        
        # Process new structure (DrugsOfConcernDetails)
        if not df_new.empty:
            try:
                df_new_converted = convert_new_to_old_structure(df_new)
                expanded_new, warnings_new = normalize_pdc_odc(df_new_converted, config)
                results.append(expanded_new)
                all_warnings.extend(warnings_new)
            except Exception as e:
                # Add warning for processing error
                all_warnings.append(AODWarning(
                    '',  # No specific SLK
                    '',  # No specific RowKey
                    drug_name='',
                    field_name='new_structure',
                    field_value=f"Error processing new structure: {str(e)}"
                ))
        
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
            # Use safe concatenation
            try:
                combined_df = pd.concat(results)
                combined_df = combined_df.reindex(df1.index)
            except Exception as e:
                # If concatenation fails, create empty DataFrame with same index
                combined_df = pd.DataFrame(index=df1.index)
                all_warnings.append(AODWarning(
                    '',  # No specific SLK
                    '',  # No specific RowKey
                    drug_name='',
                    field_name='concat',
                    field_value=f"Error combining results: {str(e)}"
                ))
        else:
            combined_df = pd.DataFrame(index=df1.index)
        
        # Create a copy of original df without PDC/ODC columns
        try:
            preserved_df = drop_fields(df1.copy(), ['PDC', 'ODC'])
            
            # Join the normalized drug data with preserved columns
            final_df = preserved_df.join(combined_df)
        except Exception as e:
            # If join fails, return original DataFrame without drug columns
            final_df = drop_fields(df1.copy(), ['PDC', 'ODC', 'DrugsOfConcernDetails', 'PDCSubstanceOrGambling'])
            all_warnings.append(AODWarning(
                '',  # No specific SLK
                '',  # No specific RowKey
                drug_name='',
                field_name='join',
                field_value=f"Error joining results: {str(e)}"
            ))
        
        return final_df, all_warnings
        
    except Exception as e:
        # Catch-all for any unexpected errors
        return df1.copy(), [AODWarning(
            '',  # No specific SLK
            '',  # No specific RowKey
            drug_name='',
            field_name='expand_drug_info',
            field_value=f"Unexpected error: {str(e)}"
        )]

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
    df_nan_test.at[171, 'DrugsOfConcernDetails'] = [
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
    ]
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

    # Set test data using at for list values
    df_noncontiguous.at[115, 'PDC'] = [{
        'PDCSubstanceOrGambling': 'Cannabinoids',
        'PDCDaysInLast28': '20',
        'PDCHowMuchPerOccasion': '55.0',
        'PDCUnits': 'blunts'
    }]
    df_noncontiguous.at[171, 'DrugsOfConcernDetails'] = [{
        'DrugsOfConcern': 'Ethanol',
        'MethodOfUse': 'Ingest',
        'DaysInLast28': '20',
        'Units': 'standard drinks',
        'HowMuchPerOccasion': '6'
    }]
    df_noncontiguous.loc[171, 'PDCSubstanceOrGambling'] = 'Ethanol'  # Not a list, can use loc
    df_noncontiguous.at[202, 'ODC'] = [{
        'OtherSubstancesConcernGambling': 'MDMA/Ecstasy',
        'DaysInLast28': '4',
        'Units': 'pills'
    }]

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
    df_preservation.at[115, 'PDC'] = [{
        'PDCSubstanceOrGambling': 'Cannabinoids',
        'PDCDaysInLast28': '20',
        'PDCHowMuchPerOccasion': '55.0',
        'PDCUnits': 'blunts'
    }]

    # Row 2: New structure
    df_preservation.loc[171, 'PDCSubstanceOrGambling'] = 'Ethanol'
    df_preservation.at[171, 'DrugsOfConcernDetails'] = [{
        'DrugsOfConcern': 'Ethanol',
        'MethodOfUse': 'Ingest',
        'DaysInLast28': '20',
        'Units': 'standard drinks',
        'HowMuchPerOccasion': '6'
    }]

    # Row 3: Mixed data
    df_preservation.at[202, 'ODC'] = [{
        'OtherSubstancesConcernGambling': 'MDMA/Ecstasy',
        'DaysInLast28': '4',
        'Units': 'pills'
    }]

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
    df_mixed.at[0, 'PDC'] = [{
        'PDCSubstanceOrGambling': 'Cannabinoids',
        'PDCDaysInLast28': '20',
        'PDCHowMuchPerOccasion': '55.0',
        'PDCUnits': 'blunts'
    }]
    df_mixed.at[0, 'ODC'] = [{
        'OtherSubstancesConcernGambling': 'Caffeine',
        'DaysInLast28': '10',
        'HowMuchPerOccasion': '50-59'
    }]

    # Row 2: New structure
    df_mixed.loc[1, 'PDCSubstanceOrGambling'] = 'Ethanol'  # Not a list, can use loc
    df_mixed.at[1, 'DrugsOfConcernDetails'] = [
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
    ]

    # Row 3: Old structure
    df_mixed.at[2, 'PDC'] = [{
        'PDCSubstanceOrGambling': 'MDMA/Ecstasy',
        'PDCDaysInLast28': '4',
        'PDCUnits': 'pills'
    }]

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
