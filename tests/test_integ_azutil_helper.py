import os
import pytest
from dotenv import load_dotenv
from azure.data.tables import TableServiceClient
from azure.core.exceptions import AzureError
from assessment_episode_matcher.azutil.helper import get_results
from datetime import datetime

@pytest.fixture(scope="session")
def azure_connection():
    """Load environment variables and create Azure connection"""
    load_dotenv('.env.prod')
    conn_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    assert conn_str is not None, "AZURE_STORAGE_CONNECTION_STRING not found in .env.prod"
    return conn_str

@pytest.fixture(scope="session")
def table_client(azure_connection):
    """Create Azure Table client"""
    try:
        service_client = TableServiceClient.from_connection_string(azure_connection)
        return service_client
    except AzureError as e:
        pytest.fail(f"Failed to connect to Azure Table Storage: {str(e)}")

def test_env_file_exists():
    """Test if .env.prod file exists"""
    assert os.path.exists('.env.prod'), ".env.prod file not found"

def test_connection_string_format(azure_connection):
    """Test if connection string has required components"""
    assert "DefaultEndpointsProtocol=" in azure_connection
    assert "AccountName=" in azure_connection
    assert "AccountKey=" in azure_connection
    assert "TableEndpoint=" in azure_connection

def test_azure_table_connection(table_client):
    """Test if we can connect to Azure Table"""
    assert table_client is not None
    try:
        # List tables to verify connection
        list(table_client.list_tables())
    except AzureError as e:
        pytest.fail(f"Failed to list tables: {str(e)}")

def test_real_data_retrieval():
    """Test retrieving real data from Azure Table"""
    load_dotenv('.env.prod')
    
    # Use actual date range for testing
    start_date = 20241201  # Dec  1, 2024
    end_date = 20241231    # Dec 31, 2024
    
    results = get_results('ATOM', start_date, end_date)
    
    assert isinstance(results, list), "Results should be a list"
    if len(results) > 0:
        # Verify structure of returned data
        assert 'PartitionKey' in results[0]
        assert 'RowKey' in results[0]
        assert 'Program' in results[0]
        assert 'AssessmentDate' in results[0]

def test_date_range_filtering():
    """Test date range filtering with real data"""
    load_dotenv('.env.prod')
    
    # Test with a specific month
    start_date = 20240101
    end_date = 20240131
    
    results = get_results('ATOM', start_date, end_date)
    
    for result in results:
        assert result['AssessmentDate'] >= start_date
        assert result['AssessmentDate'] <= end_date

def test_program_filtering():
    """Test program filtering with real data"""
    load_dotenv('.env.prod')
    
    start_date = 20240101
    end_date = 20240331
    
    # Get all results first to find an existing program
    all_results = get_results('ATOM', start_date, end_date)
    
    if len(all_results) > 0:
        # Use the first program found in the data
        test_program = all_results[0]['Program']
        
        # Filter by this program
        filtered_results = get_results('ATOM', start_date, end_date, 
                                     filters={"Program": test_program})
        
        assert all(result['Program'] == test_program for result in filtered_results)

def test_matching_days_slack():
    """Test MATCHING_NDAYS_SLACK environment variable"""
    load_dotenv('.env.prod')
    slack_days = os.getenv('MATCHING_NDAYS_SLACK')
    
    assert slack_days is not None
    assert slack_days.isdigit()
    assert int(slack_days) >= 0

def test_invalid_date_range():
    """Test handling of invalid date ranges with real connection"""
    load_dotenv('.env.prod')
    
    with pytest.raises(ValueError):
        get_results('ATOM', 20240331, 20240101)  # end date before start date

def test_blob_connection_string():
    """Test AZURE_BLOB_CONNECTION_STRING environment variable"""
    load_dotenv('.env.prod')