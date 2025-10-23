#!/usr/bin/env python3
"""
BigQuery Dataset Transfer Service for Cloud Run
Handles dataset transfers between projects with environment-specific redaction
"""

import os
import json
import logging
from typing import Dict, List, Optional
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, BadRequest
import argparse
from flask import Flask, request, jsonify

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BigQueryTransferService:
    def __init__(self, environment: str):
        self.environment = environment
        self._setup_environment_config()
        # Initialize client with destination project (where jobs will run)
        self.client = bigquery.Client(project=self.dest_project)
    
    def _setup_environment_config(self):
        """Setup environment-specific configuration"""
        if self.environment == "dev":
            self.source_project = os.getenv("DEV_SOURCE_PROJECT", "sbox-rgodoy-001-20251124")
            self.dest_project = os.getenv("DEV_DEST_PROJECT", "sbox-rgodoy-002-20251008")
            self.source_dataset = "dts_01"
            self.dest_dataset = "dev_dts"
            # NOTE: sensitive_columns format is DATASET:TABLE.COLUMN.TACTIC, matching bq_transfer.sh
            self.sensitive_columns = [
                "dts_01:stg_business_licenses.account_number.redact",
                "dts_01:stg_business_licenses.business_address.redact",
                "dts_01:stg_business_licenses.community_area.redact",
                "dts_01:stg_business_licenses.payment_date.redact",
                "dts_01:stg_crimes.x_coordinate.redact",
                "dts_01:stg_crimes.y_coordinate.redact",
                "dts_01:stg_crimes.latitude.redact",
                "dts_01:stg_crimes.longitude.redact",
                "dts_01:stg_crimes.location_description.redact"
            ]
            logger.info("Using DEV environment configuration (full redaction)")
            
        elif self.environment == "uat":
            self.source_project = os.getenv("UAT_SOURCE_PROJECT", "sbox-rgodoy-001-20251124")
            self.dest_project = os.getenv("UAT_DEST_PROJECT", "sbox-rgodoy-002-20251008")
            self.source_dataset = "dts_01"
            self.dest_dataset = "uat_dts"
            self.sensitive_columns = []  # No redaction for UAT
            logger.info("Using UAT environment configuration (no redaction)")
            
        else:
            raise ValueError(f"Unknown environment: {self.environment}")
    
    def validate_authentication(self) -> bool:
        """Validate that we can access both projects"""
        try:
            # Test source project access
            source_client = bigquery.Client(project=self.source_project)
            list(source_client.list_datasets(max_results=1))
            logger.info(f"Source project access validated: {self.source_project}")
            
            # Test destination project access
            dest_client = bigquery.Client(project=self.dest_project)
            list(dest_client.list_datasets(max_results=1))
            logger.info(f"Destination project access validated: {self.dest_project}")
            
            return True
        except Exception as e:
            logger.error(f"Authentication validation failed: {e}")
            return False

    def ensure_dest_dataset_exists(self) -> bool:
        """
        FIX 1: Creates the destination dataset if it does not exist.
        This prevents the copy_table operation from failing on a NotFound error.
        It also attempts to infer the location from the source dataset for compatibility.
        """
        dest_dataset_id = f"{self.dest_project}.{self.dest_dataset}"
        dataset_ref = bigquery.DatasetReference(self.dest_project, self.dest_dataset)
        dataset = bigquery.Dataset(dataset_ref)
        
        try:
            # Check if dataset exists
            existing_dataset = self.client.get_dataset(dataset_ref)
            logger.info(f"Destination dataset {dest_dataset_id} already exists (Location: {existing_dataset.location}).")
            return True
        except NotFound:
            logger.info(f"Destination dataset {dest_dataset_id} not found. Creating it...")
            try:
                # Attempt to get source dataset location for multi-region compatibility
                source_dataset_ref = bigquery.DatasetReference(self.source_project, self.source_dataset)
                source_dataset = self.client.get_dataset(source_dataset_ref)
                dataset.location = source_dataset.location
                logger.info(f"Setting location to match source: {dataset.location}")
            except NotFound:
                logger.warning("Source dataset location not found. Using default project location.")
            except Exception as e:
                 logger.warning(f"Could not determine source location: {e}. Using default project location.")
                
            try:
                self.client.create_dataset(dataset)
                logger.info(f"Created destination dataset {dest_dataset_id}")
                return True
            except Exception as e:
                logger.error(f"Failed to create dataset {dest_dataset_id}: {e}")
                return False
        except Exception as e:
            logger.error(f"Error checking for dataset {dest_dataset_id}: {e}")
            return False
    
    def get_tables_to_copy(self) -> List[str]:
        """Get list of tables to copy from source dataset"""
        try:
            source_client = bigquery.Client(project=self.source_project)
            dataset_ref = source_client.dataset(self.source_dataset)
            
            tables = []
            for table in source_client.list_tables(dataset_ref):
                # Only copy native BigQuery tables (BASE TABLE, not EXTERNAL, VIEW, etc.)
                # This check ensures consistency with the bash script's INFORMATION_SCHEMA query.
                table_obj = source_client.get_table(table.reference)
                if table_obj.table_type == "TABLE":
                    tables.append(table.table_id)
            
            logger.info(f"Found {len(tables)} tables to copy: {tables}")
            return tables
            
        except Exception as e:
            logger.error(f"Failed to get tables: {e}")
            return []
    
    def copy_table(self, table_name: str) -> bool:
        """Copy a single table from source to destination"""
        try:
            source_table_id = f"{self.source_project}.{self.source_dataset}.{table_name}"
            dest_table_id = f"{self.dest_project}.{self.dest_dataset}.{table_name}"
            
            logger.info(f"Copying table: {source_table_id} → {dest_table_id}")
            
            # Create a job config with explicit write disposition
            job_config = bigquery.CopyJobConfig(
                write_disposition="WRITE_TRUNCATE"  # Overwrite existing table
            )
            
            # Copy the table using the destination project client
            # This ensures the job runs in the destination project with proper billing
            job = self.client.copy_table(
                source_table_id,
                dest_table_id,
                job_config=job_config
            )
            
            # Wait for the job to complete
            job.result()  # This blocks until the job completes
            
            # Get the destination table to report row count
            try:
                dest_table = self.client.get_table(dest_table_id)
                logger.info(f"Successfully copied table: {table_name} ({dest_table.num_rows} rows)")
            except Exception:
                # If we can't get the row count, still report success
                logger.info(f"Successfully copied table: {table_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to copy table {table_name}: {e}")
            logger.error(f"Details: {type(e).__name__}: {str(e)}")
            return False
    
    def apply_redaction(self, table_name: str) -> bool:
        """Apply redaction to sensitive columns in a table"""
        if not self.sensitive_columns:
            logger.info(f"No redaction needed for table: {table_name}")
            return True
        
        try:
            # DML updates run in the project where the destination table resides
            dest_table_id = f"{self.dest_project}.{self.dest_dataset}.{table_name}"
            
            for column_config in self.sensitive_columns:
                try:
                    # FIX 2: Correctly parse config: dataset:table.column.tactic
                    parts = column_config.split(":") 
                    if len(parts) != 2:
                        logger.warning(f"Skipping malformed config (expecting 1 ':'): {column_config}")
                        continue

                    # parts[0] is dataset_name (dts_01)
                    col_info = parts[1] # e.g., 'stg_business_licenses.account_number.redact'
                    
                    # Split col_info by '.' to get table, column, and tactic
                    col_parts = col_info.split(".")
                    if len(col_parts) != 3:
                        logger.warning(f"Skipping malformed config (expecting 2 '.'): {column_config}")
                        continue
                        
                    config_table_name = col_parts[0]  # e.g., 'stg_business_licenses'
                    column_name = col_parts[1]        # e.g., 'account_number'
                    tactic = col_parts[2]             # e.g., 'redact'
                    
                except Exception as e:
                    logger.error(f"Failed to parse sensitive column config '{column_config}': {e}. Skipping.")
                    continue
                
                # CORRECT MATCHING: Check if the current table matches the configured table name
                if table_name == config_table_name:
                    
                    logger.info(f"Applying {tactic} to {table_name}.{column_name}")
                    
                    if tactic == "redact":
                        sql = f"""
                        UPDATE `{dest_table_id}`
                        SET {column_name} = NULL
                        WHERE TRUE
                        """
                    elif tactic == "FF":
                        # Note: The original SQL was slightly cleaner, using T.
                        sql = f"""
                        UPDATE `{dest_table_id}` T
                        SET {column_name} = FARM_FINGERPRINT(CAST(T.{column_name} AS STRING))
                        WHERE TRUE
                        """
                    elif tactic == "mask":
                        sql = f"""
                        UPDATE `{dest_table_id}`
                        SET {column_name} = CASE 
                            WHEN LENGTH(CAST({column_name} AS STRING)) > 4 THEN 
                                CONCAT('****', SUBSTR(CAST({column_name} AS STRING), -4))
                            ELSE '****'
                        END
                        WHERE {column_name} IS NOT NULL
                        """
                    elif tactic == "hash":
                        sql = f"""
                        UPDATE `{dest_table_id}`
                        SET {column_name} = TO_HEX(SHA256(CAST({column_name} AS BYTES)))
                        WHERE {column_name} IS NOT NULL
                        """
                    else:
                        logger.warning(f"Unknown tactic: {tactic}. Skipping.")
                        continue
                    
                    # Execute the SQL (DML runs in the destination project)
                    job_config = bigquery.QueryJobConfig(
                        # This ensures the job runs in the destination project for DML
                        default_dataset=bigquery.DatasetReference(self.dest_project, self.dest_dataset)
                    )
                    job = self.client.query(sql, job_config=job_config)
                    job.result()
                    logger.info(f"Applied {tactic} to {table_name}.{column_name} successfully.")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to apply redaction to {table_name}: {e}")
            return False
    
    def transfer_dataset(self) -> bool:
        """Main method to transfer dataset with redaction"""
        logger.info(f"Starting dataset transfer: {self.environment}")
        
        # 1. Validate authentication
        if not self.validate_authentication():
            return False
        
        # 2. FIX: Ensure destination dataset exists
        if not self.ensure_dest_dataset_exists():
            return False
        
        # 3. Get tables to copy
        tables = self.get_tables_to_copy()
        if not tables:
            logger.error("No tables found to copy")
            return False
        
        # 4. Copy and Redact each table
        success_count = 0
        for table_name in tables:
            if self.copy_table(table_name):
                # Only apply redaction if copy was successful
                if self.apply_redaction(table_name):
                    success_count += 1
                else:
                    logger.warning(f"Redaction failed for {table_name}. Counting as failure.")
            else:
                 logger.error(f"Copy failed for {table_name}. Skipping redaction.")
        
        logger.info(f"Transfer completed: {success_count}/{len(tables)} tables successful")
        return success_count == len(tables)

def main():
    parser = argparse.ArgumentParser(description="BigQuery Dataset Transfer Service")
    parser.add_argument("environment", choices=["dev", "uat"], 
                       help="Target environment (dev or uat)")
    
    args = parser.parse_args()
    
    try:
        service = BigQueryTransferService(args.environment)
        success = service.transfer_dataset()
        
        if success:
            logger.info("Dataset transfer completed successfully!")
            return 0
        else:
            logger.error("Dataset transfer failed!")
            return 1
            
    except Exception as e:
        logger.error(f"Service error: {e}")
        return 1

# Flask web service for Cloud Run
app = Flask(__name__)

@app.route('/', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "service": "BigQuery Transfer Service"}), 200

@app.route('/transfer', methods=['POST'])
def transfer_dataset_endpoint():
    """Transfer dataset endpoint"""
    try:
        data = request.get_json()
        # Default to 'dev' if no environment is provided in the JSON payload or environment variables
        environment = data.get('environment') if data and 'environment' in data else os.getenv('ENVIRONMENT', 'dev')
        
        if environment not in ['dev', 'uat']:
            return jsonify({"error": "Invalid environment. Must be 'dev' or 'uat'"}), 400
        
        logger.info(f"Starting dataset transfer via web service: {environment}")
        
        service = BigQueryTransferService(environment)
        success = service.transfer_dataset()
        
        if success:
            logger.info("Dataset transfer completed successfully!")
            return jsonify({"status": "success", "message": "Dataset transfer completed successfully!"}), 200
        else:
            logger.error("Dataset transfer failed!")
            return jsonify({"status": "error", "message": "Dataset transfer failed!"}), 500
            
    except Exception as e:
        logger.error(f"Service error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/transfer/<environment>', methods=['POST'])
def transfer_dataset_by_env(environment):
    """Transfer dataset endpoint with environment in URL"""
    try:
        if environment not in ['dev', 'uat']:
            return jsonify({"error": "Invalid environment. Must be 'dev' or 'uat'"}), 400
        
        logger.info(f"Starting dataset transfer via web service: {environment}")
        
        service = BigQueryTransferService(environment)
        success = service.transfer_dataset()
        
        if success:
            logger.info("Dataset transfer completed successfully!")
            return jsonify({"status": "success", "message": "Dataset transfer completed successfully!"}), 200
        else:
            logger.error("Dataset transfer failed!")
            return jsonify({"status": "error", "message": "Dataset transfer failed!"}), 500
            
    except Exception as e:
        logger.error(f"Service error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    # Check if running as web service (Cloud Run) or CLI
    if os.getenv('PORT'):
        # Running as web service
        port = int(os.getenv('PORT', 8080))
        logger.info(f"Starting web service on port {port}")
        app.run(host='0.0.0.0', port=port, debug=False)
    else:
        # Running as CLI
        exit(main())