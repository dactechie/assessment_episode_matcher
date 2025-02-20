import pytest
from unittest.mock import patch, Mock
from assessment_episode_matcher.azutil.helper import get_results

@pytest.fixture
def mock_table_query():
  with patch('assessment_episode_matcher.azutil.helper.SampleTablesQuery') as mock:
    query_instance = Mock()
    mock.return_value = query_instance
    yield query_instance

@pytest.fixture
def sample_atom_data():
    return [
        {
            "PartitionKey": "ATOM",
            "RowKey": "1",
            "Program": "Program1",
            "AssessmentDate": 20240101,
            "AssessmentType": "ClinicalAssessment"
        },
        {
            "PartitionKey": "ATOM",
            "RowKey": "2",
            "Program": "Program2",
            "AssessmentDate": 20240102,
            "AssessmentType": "OutcomeAssessment"
        }
    ]

def test_get_results_empty(mock_table_query):
    mock_table_query.query_table.return_value = []
    
    results = get_results('ATOM', 20240101, 20240331)
    
    assert len(results) == 0
    assert mock_table_query.query_table.called




def test_get_results_multiple_records(mock_table_query):
    data = [
        {
            "PartitionKey": "ATOM",
            "RowKey": "1", 
            "Program": "Program1",
            "AssessmentDate": 20240101,
            "AssessmentType": "ClinicalAssessment"
        },
        {
            "PartitionKey": "ATOM", 
            "RowKey": "2",
            "Program": "Program2", 
            "AssessmentDate": 20240102,
            "AssessmentType": "OutcomeAssessment"
        }
    ]
    # mock_table_query.query_table.return_value = data
    
    results = get_results('ATOM', 20240101, 20240331)
    
    assert len(results) == 2
    assert results[0]['Program'] == 'Program1'
    assert results[1]['Program'] == 'Program2'


# def test_get_results_multiple_records(mock_table_query):
#     data = [
#         {
#             "PartitionKey": "ATOM",
#             "RowKey": "1", 
#             "Program": "Program1",
#             "AssessmentDate": 20240101,
#             "AssessmentType": "ClinicalAssessment"
#         },
#         {
#             "PartitionKey": "ATOM", 
#             "RowKey": "2",
#             "Program": "Program2", 
#             "AssessmentDate": 20240102,
#             "AssessmentType": "OutcomeAssessment"
#         }
#     ]
#     mock_table_query.query_table.return_value = data
    
#     results = get_results('ATOM', 20240101, 20240331)
    
#     assert len(results) == 2
#     assert results[0]['Program'] == 'Program1'
#     assert results[1]['Program'] == 'Program2'

def test_get_results_date_filter(mock_table_query, sample_atom_data):
    mock_table_query.query_table.return_value = sample_atom_data
    
    results = get_results('ATOM', 20240201, 20240228)
    
    filter_str = mock_table_query.query_table.call_args[1]['filter_template']
    assert 'AssessmentDate ge' in filter_str
    assert '20240201' in filter_str
    assert '20240228' in filter_str

def test_get_results_invalid_date_range(mock_table_query):
    with pytest.raises(ValueError):
        get_results('ATOM', 20240331, 20240101)  # end before start

def test_get_results_program_filter(mock_table_query, sample_atom_data):
    mock_table_query.query_table.return_value = sample_atom_data
    
    results = get_results('ATOM', 20240101, 20240331, 
                         filters={"Program": "Program1"})
    
    filter_str = mock_table_query.query_table.call_args[1]['filter_template']
    assert "Program eq" in filter_str

def test_get_clinical_assessment():
    # Create test data with multiple assessment types
    mock_data = [
        type('Assessment', (), {'AssessmentType': 'OtherAssessment', 'id': '1'}),
        type('Assessment', (), {'AssessmentType': 'ClinicalAssessment', 'id': '2'}),
        type('Assessment', (), {'AssessmentType': 'ThirdAssessment', 'id': '3'})
    ]
    
    # Mock get_assessments to return our test data
    with patch('get_results', return_value=mock_data):
        result = get_results('ATOM', 20240101, 20240331)
        
        assert result is not None
        assert result.AssessmentType == 'ClinicalAssessment'
        assert result.id == '2'