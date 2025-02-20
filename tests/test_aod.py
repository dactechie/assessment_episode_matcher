import pytest
import pandas as pd
from assessment_episode_matcher.importers.aod import expand_drug_info
from assessment_episode_matcher.mytypes import AODWarning

@pytest.fixture
def config():
    """Fixture providing test drug category configuration"""
    return {
        "drug_categories": {
            "Cannabis": ["Cannabinoids", "Cannabis"],
            "Alcohol": ["Ethanol"],
            "Stimulants": ["Caffeine", "Psychostimulants, n.f.d.", "MDMA/Ecstasy"]
        }
    }

def test_nan_handling(config):
    """Test handling of NaN values with non-contiguous indices"""
    df_nan_test = pd.DataFrame({
        'PartitionKey': ['ABC', 'DEF', 'GHI'],
        'RowKey': ['rk115', 'rk171', 'rk202'],
        'SLK': ['SLK1', 'SLK2', 'SLK3']
    }, index=[115, 171, 202])
    
    # Initialize columns with NaN
    df_nan_test['PDC'] = pd.NA
    df_nan_test['ODC'] = pd.NA
    df_nan_test['DrugsOfConcernDetails'] = pd.NA
    df_nan_test['PDCSubstanceOrGambling'] = pd.NA

    # Set test data
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
    df_nan_test.loc[171, 'PDCSubstanceOrGambling'] = 'Cannabis'

    out_nan_test, warnings = expand_drug_info(df_nan_test, config)

    # Verify index preservation
    assert list(out_nan_test.index) == [115, 171, 202]
    
    # Verify original columns preserved
    assert all(col in out_nan_test.columns for col in ['PartitionKey', 'RowKey', 'SLK'])
    
    # Verify drug data for row 171
    assert out_nan_test.loc[171, 'Cannabis_DaysInLast28'] == '5'
    assert out_nan_test.loc[171, 'Cannabis_Units'] == 'cones / joints'
    assert out_nan_test.loc[171, 'Cannabis_PerOccassionUse'] == '3'

def test_noncontiguous_indices(config):
    """Test handling of non-contiguous indices with mixed drug data"""
    df_noncontiguous = pd.DataFrame({
        'PartitionKey': ['ABC', 'DEF', 'GHI'],
        'RowKey': ['rk115', 'rk171', 'rk202'],
        'SLK': ['SLK1', 'SLK2', 'SLK3']
    }, index=[115, 171, 202])
    
    # Initialize columns with None
    df_noncontiguous['PDC'] = None
    df_noncontiguous['ODC'] = None
    df_noncontiguous['DrugsOfConcernDetails'] = None
    df_noncontiguous['PDCSubstanceOrGambling'] = None

    # Set test data
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
    df_noncontiguous.loc[171, 'PDCSubstanceOrGambling'] = 'Ethanol'
    df_noncontiguous._set_value(202, 'ODC', [{
        'OtherSubstancesConcernGambling': 'MDMA/Ecstasy',
        'DaysInLast28': '4',
        'Units': 'pills'
    }])

    out_noncontiguous, warnings = expand_drug_info(df_noncontiguous, config)

    # Verify index preservation
    assert list(out_noncontiguous.index) == [115, 171, 202]
    
    # Verify original columns preserved
    assert all(col in out_noncontiguous.columns for col in ['PartitionKey', 'RowKey', 'SLK'])
    
    # Verify drug data
    assert out_noncontiguous.loc[115, 'Cannabis_DaysInLast28'] == '20'
    assert out_noncontiguous.loc[115, 'Cannabis_Units'] == 'blunts'
    assert out_noncontiguous.loc[115, 'Cannabis_PerOccassionUse'] == '55'
    
    assert out_noncontiguous.loc[171, 'Alcohol_DaysInLast28'] == '20'
    assert out_noncontiguous.loc[171, 'Alcohol_Units'] == 'standard drinks'
    assert out_noncontiguous.loc[171, 'Alcohol_PerOccassionUse'] == '6'
    
    assert out_noncontiguous.loc[202, 'Stimulants_DaysInLast28'] == '4'
    assert out_noncontiguous.loc[202, 'Stimulants_Units'] == 'pills'

def test_column_preservation_and_reindexing(config):
    """Test preservation of non-drug columns and correct reindexing"""
    df_preservation = pd.DataFrame({
        'PartitionKey': ['ABC', 'DEF', 'GHI'],
        'RowKey': ['rk115', 'rk171', 'rk202'],
        'SLK': ['SLK1', 'SLK2', 'SLK3'],
        'Age': [25, 30, 35],  # Non-drug column
        'Gender': ['M', 'F', 'M'],  # Non-drug column
        'Location': ['Sydney', 'Melbourne', 'Brisbane']  # Non-drug column
    }, index=[115, 171, 202])

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

    out_preservation, warnings = expand_drug_info(df_preservation, config)

    # Verify all non-drug columns are preserved
    original_cols = ['PartitionKey', 'RowKey', 'SLK', 'Age', 'Gender', 'Location']
    assert all(col in out_preservation.columns for col in original_cols)

    # Verify row integrity for each index
    for idx in df_preservation.index:
        original_row = df_preservation.loc[idx]
        output_row = out_preservation.loc[idx]
        
        # Check demographic data stayed with correct row
        for col in original_cols:
            assert original_row[col] == output_row[col], f"Row {idx} value mismatch in {col}"

    # Verify drug data
    assert out_preservation.loc[115, 'Cannabis_DaysInLast28'] == '20'
    assert out_preservation.loc[171, 'Alcohol_DaysInLast28'] == '20'
    assert out_preservation.loc[202, 'Stimulants_DaysInLast28'] == '4'

def test_mixed_structures(config):
    """Test handling of mixed old and new data structures"""
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
    df_mixed.loc[1, 'PDCSubstanceOrGambling'] = 'Ethanol'
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

    out_mixed, warnings = expand_drug_info(df_mixed, config)

    # Verify original columns preserved
    assert all(col in out_mixed.columns for col in ['PartitionKey', 'RowKey', 'SLK'])

    # Verify old structure data (Row 1)
    assert out_mixed.loc[0, 'Cannabis_DaysInLast28'] == '20'
    assert out_mixed.loc[0, 'Cannabis_Units'] == 'blunts'
    assert out_mixed.loc[0, 'Cannabis_PerOccassionUse'] == '55'
    assert out_mixed.loc[0, 'Stimulants_DaysInLast28'] == '10'

    # Verify new structure data (Row 2)
    assert out_mixed.loc[1, 'Alcohol_DaysInLast28'] == '20'
    assert out_mixed.loc[1, 'Alcohol_Units'] == 'standard drinks'
    assert out_mixed.loc[1, 'Alcohol_PerOccassionUse'] == '6'

    # Verify old structure data (Row 3)
    assert out_mixed.loc[2, 'Stimulants_DaysInLast28'] == '4'
    assert out_mixed.loc[2, 'Stimulants_Units'] == 'pills'

    # Verify invalid row handling (Row 4)
    assert any(isinstance(w, AODWarning) and w.field_name == 'structure' 
              for w in warnings)
