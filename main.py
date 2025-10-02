import functions_framework
from google.cloud import storage
import pandas as pd
import json
import fnmatch
import os
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize GCS client globally for efficiency
STORAGE_CLIENT = storage.Client()

# Configuration constants from environment variables
CONFIG_BUCKET = os.environ.get('CONFIG_BUCKET') 
DEAD_LETTER_BUCKET = os.environ.get('DEAD_LETTER_BUCKET') 

# Hardcoded bucket names from the project requirements
LANDING_ZONE_BUCKET = 'xref-landing-zone'
EXTERNAL_TABLES_BUCKET = 'xref-ext-tables'

# HARDCODED: The prefix where config files are stored in the GCS bucket
CONFIG_FOLDER = 'config/'

# --- Utility Functions ---

def load_file_config_dynamic(config_bucket: str, source_blob_name: str) -> Dict[str, Any]:
    """
    Loads configuration rules dynamically based on the source file's name.
    
    The expected config path is: gs://xref-config/config/[filename_without_extension].json
    """
    if not config_bucket:
        raise ValueError("CONFIG_BUCKET environment variable is not set.")
    
    # 1. Strip directories and get the filename without extension (e.g., 'raw_site_orders.json')
    base_name = os.path.basename(source_blob_name)
    file_name_without_ext, _ = os.path.splitext(base_name)
    
    # 2. Dynamically construct the specific config file name
    config_blob_name = f"{CONFIG_FOLDER}{file_name_without_ext}.json"
        
    try:
        logger.info(f"Attempting to load config from: {config_blob_name}")
        bucket = STORAGE_CLIENT.bucket(config_bucket)
        blob = bucket.blob(config_blob_name) 
        config_data = blob.download_as_text()
        return json.loads(config_data)
    except Exception as e:
        logger.warning(f"Configuration file not found or corrupted: {config_blob_name}. Error: {e}")
        # Raise a specific error type for easy handling in the main function
        raise FileNotFoundError(f"Config file not found: {config_blob_name}")


def copy_blob(source_bucket_name: str, source_blob_name: str, 
              target_bucket_name: str, target_blob_name: str) -> None:
    """Copies a blob from one bucket to another using the correct Bucket.copy_blob method."""
    
    source_bucket = STORAGE_CLIENT.bucket(source_bucket_name) 
    source_blob = source_bucket.blob(source_blob_name)
    destination_bucket = STORAGE_CLIENT.bucket(target_bucket_name)

    #  Call copy_blob on the source_bucket object
    source_bucket.copy_blob(
        source_blob, 
        destination_bucket, 
        new_name=target_blob_name 
    )
    
    logger.info(f"File copied to gs://{target_bucket_name}/{target_blob_name}")

def process_dead_letter(source_bucket: str, source_blob: str, reason: str) -> None:
    """Copies the file to the Dead Letter Bucket and logs the error."""
    logger.error(f"DEAD LETTER: File {source_blob} failed processing. Reason: {reason}")
    if not DEAD_LETTER_BUCKET:
        logger.critical("CRITICAL: DEAD_LETTER_BUCKET environment variable is not set. Cannot move file.")
        return

    # Target blob path in DQL: error / timestamp_filename
    timestamp_prefix = pd.Timestamp.now().strftime('%Y%m%d%H%M%S')
    target_blob_name = f"error/{timestamp_prefix}_{source_blob}"
    
    try:
        copy_blob(source_bucket, source_blob, DEAD_LETTER_BUCKET, target_blob_name)
        logger.info(f"File moved to Dead Letter: gs://{DEAD_LETTER_BUCKET}/{target_blob_name}")
    except Exception as e:
        logger.critical(f"CRITICAL: Failed to move file to DQL bucket {DEAD_LETTER_BUCKET}. Error: {e}")

def find_config(config_rules: Dict) -> Dict:
    """
    Validates that the required keys exist and extracts path/columns from the single config file.
    
    The config_rules dictionary must contain 'expected_columns', 'target_path', and 'filename_pattern'.
    """
    required_keys = ['expected_columns', 'target_path', 'filename_pattern']
    for key in required_keys:
        if key not in config_rules:
            raise ValueError(f"Configuration file is missing required key: '{key}'")
            
    return config_rules

# --- Main Entry Point ---

@functions_framework.cloud_event
def xref_processor(cloud_event: Dict[str, Any]):
    """
    GCF entry point triggered by GCS file upload to xref-landing-zone.
    Validates, timestamps, and moves the file to the external table bucket.
    """
    data = cloud_event.data
    source_bucket_name = data.get('bucket')
    source_blob_name = data.get('name')
    
    if source_bucket_name != LANDING_ZONE_BUCKET:
        logger.warning(f"Event from unexpected bucket: {source_bucket_name}. Ignoring.")
        return
    if not source_blob_name:
        logger.error(f"Missing file name in event data: {data}")
        return
    
    logger.info(f"Processing file: gs://{source_bucket_name}/{source_blob_name}")
    
    # Use only the filename for local storage to prevent FileNotFoundError due to nested folders
    file_name_only = os.path.basename(source_blob_name)
    temp_local_file = f'/tmp/{file_name_only}' 

    try:
        # 1. Load Configuration DYNAMICALLY based on filename (FIRST STEP)
        # This will raise FileNotFoundError if the config file doesn't exist
        config_rules = load_file_config_dynamic(CONFIG_BUCKET, source_blob_name)
        
        # 2. Extract Validation Rules and Check Pattern (Logic Match)
        validated_config = find_config(config_rules)
        expected_columns = validated_config['expected_columns']
        target_path = validated_config['target_path']
        expected_pattern = validated_config['filename_pattern']
        
        # Match the actual GCS path against the explicit pattern
        if not fnmatch.fnmatch(source_blob_name, expected_pattern):
            reason = f"Filename '{source_blob_name}' does not match the mandatory pattern '{expected_pattern}' defined in config file."
            return process_dead_letter(source_bucket_name, source_blob_name, reason)

        # 3. Download and COUNT Columns (Validation)
        bucket = STORAGE_CLIENT.bucket(source_bucket_name)
        blob = bucket.blob(source_blob_name)
        
        # Download the file to the simple /tmp/[filename] path
        blob.download_to_filename(temp_local_file)
        
        # Read and count columns (Using encoding='latin-1')
        df = pd.read_csv(temp_local_file, nrows=1, header=None, encoding='latin-1') 
        actual_columns = len(df.columns)
        
        if actual_columns != expected_columns:
            reason = f"Column count mismatch. Config expected {expected_columns}, but file has {actual_columns}."
            return process_dead_letter(source_bucket_name, source_blob_name, reason)

        # 4. Ingestion Timestamp & Target Copy
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        clean_target_path = target_path if target_path.endswith('/') else target_path + '/'
        
        # Target blob name includes the full original path (e.g., folder/file.csv)
        target_blob_name = f"{clean_target_path}ingestion_timestamp={timestamp}/{source_blob_name}"

        copy_blob(source_bucket_name, source_blob_name, EXTERNAL_TABLES_BUCKET, target_blob_name)
        
        logger.info(f"SUCCESS: File {source_blob_name} validated (Cols: {actual_columns}) and copied to gs://{EXTERNAL_TABLES_BUCKET}/{target_blob_name}")

    except FileNotFoundError as e:
        # Handles 404 error if the config file for the dataset is missing
        reason = f"Configuration file was not found for this dataset: {str(e)}"
        return process_dead_letter(source_bucket_name, source_blob_name, reason)
    except Exception as e:
        logger.exception(f"An unexpected error occurred during processing for {source_blob_name}.")
        process_dead_letter(source_bucket_name, source_blob_name, f"Unexpected processing error: {type(e).__name__} - {str(e)}")
    finally:
        # Final cleanup for the temp file
        if os.path.exists(temp_local_file):
            os.remove(temp_local_file)