import pytest
from unittest import mock
import json
import os
import pandas as pd
from google.cloud import storage

# Import the main GCF functions and constants
from main import xref_processor, LANDING_ZONE_BUCKET, EXTERNAL_TABLES_BUCKET, DEAD_LETTER_BUCKET

# --- Fixtures for Mock Data and Environment Setup ---

# Mock config for a single file structure (for the 'addcharge_mapping.csv' test file)
@pytest.fixture
def mock_config_data():
    """Returns a mock configuration JSON string representing a single, valid config file."""
    return json.dumps({
        "expected_columns": 3,
        "target_path": "shared_data/",
        "filename_pattern": "addcharge_mapping.csv" 
    })

@pytest.fixture
def gcf_event_success():
    """Event for a successful ingestion (3 columns, matching name 'addcharge_mapping.csv')."""
    mock_event = mock.Mock()
    mock_event.data = {
        'bucket': LANDING_ZONE_BUCKET,
        'name': 'addcharge_mapping.csv',
        'contentType': 'text/csv'
    }
    return mock_event

@pytest.fixture
def gcf_event_mismatch():
    """Event for a file that fails column count (name dictates config lookup, but columns are wrong)."""
    mock_event = mock.Mock()
    mock_event.data = {
        'bucket': LANDING_ZONE_BUCKET,
        'name': 'addcharge_mapping.csv', # Config lookup will succeed
        'contentType': 'text/csv'
    }
    return mock_event

@pytest.fixture
def gcf_event_unconfigured():
    """Event for a file that fails configuration lookup (config file not found)."""
    mock_event = mock.Mock()
    mock_event.data = {
        'bucket': LANDING_ZONE_BUCKET,
        'name': 'unknown_file.csv', # Config lookup will fail with FileNotFoundError
        'contentType': 'text/csv'
    }
    return mock_event

@pytest.fixture(autouse=True)
def mock_env_variables(monkeypatch):
    """Mocks necessary environment variables and file system interactions."""
    # 1. Mock module-level constants and env vars
    monkeypatch.setattr('main.CONFIG_BUCKET', 'xref-config')
    monkeypatch.setattr('main.DEAD_LETTER_BUCKET', 'xref-dead-letter')
    monkeypatch.setattr('main.EXTERNAL_TABLES_BUCKET', 'xref-ext-tables')

    # 2. Mock file system cleanup (Crucial to prevent errors)
    monkeypatch.setattr('main.os.path.exists', lambda x: True)
    monkeypatch.setattr('main.os.remove', lambda x: None)
    monkeypatch.setattr('main.os.path.isdir', lambda x: False)
    monkeypatch.setattr('main.os.makedirs', lambda x, exist_ok: None)


# --- Tests ---

@mock.patch('main.copy_blob') 
@mock.patch('main.STORAGE_CLIENT')
@mock.patch('main.pd.read_csv')
def test_successful_ingestion(mock_read_csv, mock_storage_client, mock_copy_blob, gcf_event_success, mock_config_data):
    """Test case where file name dictates config lookup, and column count is correct (Success)."""
    
    # Setup mocks:
    mock_read_csv.return_value = mock.Mock(columns=['A', 'B', 'C']) # 3 columns (matches config)
    mock_storage_client.bucket.return_value.blob.return_value.download_as_text.return_value = mock_config_data
    
    xref_processor(gcf_event_success)
    
    # Assertions
    # 1. Check that the *correct* dynamic config file was requested
    expected_config_blob = 'config/addcharge_mapping.json'
    mock_storage_client.bucket.return_value.blob.assert_any_call(expected_config_blob)
    
    # 2. Check that the successful copy utility was called exactly once
    mock_copy_blob.assert_called_once()
    
    # 3. Assert the destination argument passed was the external table bucket
    target_bucket_name_arg = mock_copy_blob.call_args[0][2] 
    assert target_bucket_name_arg == EXTERNAL_TABLES_BUCKET # Asserting against the imported constant is fine here
                                                           # because the success path uses the hardcoded value.


@mock.patch('main.copy_blob') 
@mock.patch('main.STORAGE_CLIENT')
@mock.patch('main.pd.read_csv')
def test_column_mismatch_to_dead_letter(mock_read_csv, mock_storage_client, mock_copy_blob, gcf_event_mismatch, mock_config_data):
    """Test case where file name dictates config lookup, but column count is wrong (Failure)."""
    
    # Setup mocks: 
    mock_read_csv.return_value = mock.Mock(columns=['A', 'B', 'C', 'D', 'E']) # 5 columns (config expects 3)
    mock_storage_client.bucket.return_value.blob.return_value.download_as_text.return_value = mock_config_data
    
    xref_processor(gcf_event_mismatch)
    
    # Assertions
    # 1. Config was read successfully
    expected_config_blob = 'config/addcharge_mapping.json'
    mock_storage_client.bucket.return_value.blob.assert_any_call(expected_config_blob)
    
    # 2. Check that the copy utility function was called exactly once (to DQL)
    mock_copy_blob.assert_called_once()
    
    # 3. Assert the destination argument passed was the Dead Letter Bucket
    target_bucket_name_arg = mock_copy_blob.call_args[0][2]
    # Assert against the known string literal value (or the correctly mocked constant)
    assert target_bucket_name_arg == 'xref-dead-letter' 


@mock.patch('main.copy_blob')
@mock.patch('main.STORAGE_CLIENT')
@mock.patch('main.pd.read_csv')
def test_config_not_found_to_dead_letter(mock_read_csv, mock_storage_client, mock_copy_blob, gcf_event_unconfigured):
    """Test case where no config file exists for the dataset (Failure)."""
    
    # Setup mocks: 
    mock_read_csv.return_value = mock.Mock(columns=['A', 'B', 'C']) 
    
    # Mock the dynamic config file download to raise FileNotFoundError 
    # (This simulates the 404 GCS error)
    mock_storage_client.bucket.return_value.blob.return_value.download_as_text.side_effect = FileNotFoundError 
    
    xref_processor(gcf_event_unconfigured)
    
    # Assertions
    # 1. Config was requested for 'unknown_file.json'
    expected_config_blob = 'config/unknown_file.json'
    mock_storage_client.bucket.return_value.blob.assert_any_call(expected_config_blob)
    
    # 2. Check that the copy utility function was called exactly once (to DQL)
    mock_copy_blob.assert_called_once()
    
    # 3. Assert the destination argument passed was the Dead Letter Bucket
    target_bucket_name_arg = mock_copy_blob.call_args[0][2]
    # Assert against the known string literal value
    assert target_bucket_name_arg == 'xref-dead-letter'