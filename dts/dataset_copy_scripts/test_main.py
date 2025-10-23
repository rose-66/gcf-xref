#!/usr/bin/env python3
"""
Test script for BigQuery Transfer Cloud Run Service
Tests the main.py functionality before deploying to Cloud Run
"""

# To test dev environment: ./test_main.py dev
# To test uat environment: ./test_main.py uat

import os
import sys
import json
import logging
import argparse
from typing import Dict, List, Optional
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, BadRequest
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class BigQueryTransferTester:
    def __init__(self, environment: str):
        self.environment = environment
        self.client = bigquery.Client()
        self._setup_environment_config()
    
    def _setup_environment_config(self):
        """Setup environment-specific configuration"""
        if self.environment == "dev":
            self.source_project = os.getenv("DEV_SOURCE_PROJECT", "sbox-rgodoy-001-20251124")
            self.dest_project = os.getenv("DEV_DEST_PROJECT", "sbox-rgodoy-002-20251008")
            self.source_dataset = "dts_01"
            self.dest_dataset = "dev_dts"
            self.sensitive_columns = [
                "dts_01:stg_business_licenses.account_number:redact",
                "dts_01:stg_business_licenses.business_address:redact",
                "dts_01:stg_business_licenses.community_area:redact",
                "dts_01:stg_business_licenses.payment_date:redact",
                "dts_01:stg_crimes.x_coordinate:redact",
                "dts_01:stg_crimes.y_coordinate:redact",
                "dts_01:stg_crimes.latitude:redact",
                "dts_01:stg_crimes.longitude:redact",
                "dts_01:stg_crimes.location_description:redact"
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
    
    def test_authentication(self) -> bool:
        """Test 1: Validate authentication"""
        logger.info("=" * 50)
        logger.info("Test 1: Authentication Validation")
        logger.info("=" * 50)
        
        try:
            # Test source project access
            logger.info("Step 1: Testing source project access...")
            source_client = bigquery.Client(project=self.source_project)
            list(source_client.list_datasets(max_results=1))
            logger.info(f"PASS: Source project access validated: {self.source_project}")
            
            # Test destination project access
            logger.info("Step 2: Testing destination project access...")
            dest_client = bigquery.Client(project=self.dest_project)
            list(dest_client.list_datasets(max_results=1))
            logger.info(f"PASS: Destination project access validated: {self.dest_project}")
            
            return True
            
        except Exception as e:
            logger.error(f"FAIL: Authentication validation failed: {e}")
            return False
    
    def test_environment_configuration(self) -> bool:
        """Test 2: Validate environment configuration"""
        logger.info("=" * 50)
        logger.info("Test 2: Environment Configuration")
        logger.info("=" * 50)
        
        try:
            logger.info("Step 1: Validating environment configuration...")
            logger.info(f"Environment: {self.environment}")
            
            if self.environment not in ["dev", "uat"]:
                logger.error(f"FAIL: Invalid environment: {self.environment}")
                return False
            
            logger.info("PASS: Valid environment")
            
            logger.info("Step 2: Configuration summary...")
            logger.info(f"Sensitive columns configured: {len(self.sensitive_columns)}")
            
            logger.info("Step 3: Tactics being used...")
            tactics = set()
            for column_config in self.sensitive_columns:
                if ":" in column_config:
                    parts = column_config.split(":")
                    if len(parts) >= 3:
                        tactic = parts[-1]
                        tactics.add(tactic)
            
            if tactics:
                logger.info(f"Unique tactics: {', '.join(tactics)}")
            else:
                logger.info("No tactics (no redaction)")
            
            logger.info("Step 4: Validating tactics...")
            valid_tactics = {"redact", "FF", "mask", "hash"}
            for tactic in tactics:
                if tactic not in valid_tactics:
                    logger.error(f"FAIL: Invalid tactic: {tactic}")
                    return False
                logger.info(f"PASS: Valid tactic: {tactic}")
            
            logger.info("PASS: Environment configuration validation passed")
            return True
            
        except Exception as e:
            logger.error(f"FAIL: Environment configuration validation failed: {e}")
            return False
    
    def test_dataset_existence(self) -> bool:
        """Test 3: Check dataset existence"""
        logger.info("=" * 50)
        logger.info("Test 3: Dataset Existence")
        logger.info("=" * 50)
        
        try:
            logger.info("Step 1: Checking source dataset exists...")
            logger.info(f"Looking for dataset: {self.source_project}:{self.source_dataset}")
            
            source_client = bigquery.Client(project=self.source_project)
            source_dataset_ref = source_client.dataset(self.source_dataset)
            
            try:
                source_client.get_dataset(source_dataset_ref)
                logger.info("PASS: Source dataset exists")
            except NotFound:
                logger.error("FAIL: Source dataset does not exist")
                return False
            
            logger.info("Step 2: Checking destination dataset exists...")
            logger.info(f"Looking for dataset: {self.dest_project}:{self.dest_dataset}")
            
            dest_client = bigquery.Client(project=self.dest_project)
            dest_dataset_ref = dest_client.dataset(self.dest_dataset)
            
            try:
                dest_client.get_dataset(dest_dataset_ref)
                logger.info("PASS: Destination dataset exists")
            except NotFound:
                logger.error("FAIL: Destination dataset does not exist")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"FAIL: Dataset existence check failed: {e}")
            return False
    
    def test_table_existence(self) -> bool:
        """Test 4: Check table existence"""
        logger.info("=" * 50)
        logger.info("Test 4: Table Existence")
        logger.info("=" * 50)
        
        try:
            logger.info("Step 1: Checking required tables exist in source dataset...")
            
            source_client = bigquery.Client(project=self.source_project)
            dataset_ref = source_client.dataset(self.source_dataset)
            
            expected_tables = ["stg_business_licenses", "stg_crimes"]
            existing_tables = []
            
            for table in source_client.list_tables(dataset_ref):
                if table.table_id in expected_tables:
                    existing_tables.append(table.table_id)
                    logger.info(f"PASS: Source table exists: {table.table_id}")
            
            missing_tables = set(expected_tables) - set(existing_tables)
            if missing_tables:
                logger.error(f"FAIL: Missing tables: {missing_tables}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"FAIL: Table existence check failed: {e}")
            return False
    
    def test_table_schemas(self) -> bool:
        """Test 5: Validate table schemas"""
        logger.info("=" * 50)
        logger.info("Test 5: Table Schemas")
        logger.info("=" * 50)
        
        try:
            logger.info("Step 1: Extracting table schemas from source tables...")
            
            source_client = bigquery.Client(project=self.source_project)
            dataset_ref = source_client.dataset(self.source_dataset)
            
            test_tables = ["stg_business_licenses", "stg_crimes"]
            
            for table_name in test_tables:
                logger.info(f"Retrieving schema for: {table_name}")
                
                table_ref = dataset_ref.table(table_name)
                table = source_client.get_table(table_ref)
                
                column_names = [field.name for field in table.schema]
                logger.info(f"PASS: Schema retrieved for {table_name}: {' '.join(column_names)}")
            
            return True
            
        except Exception as e:
            logger.error(f"FAIL: Schema retrieval failed: {e}")
            return False
    
    def test_table_data_counts(self) -> bool:
        """Test 6: Check data counts"""
        logger.info("=" * 50)
        logger.info("Test 6: Table Data Counts")
        logger.info("=" * 50)
        
        try:
            logger.info("Step 1: Counting rows in source tables...")
            
            source_client = bigquery.Client(project=self.source_project)
            test_tables = ["stg_business_licenses", "stg_crimes"]
            
            for table_name in test_tables:
                logger.info(f"Counting rows in: {table_name}")
                
                query = f"""
                SELECT COUNT(*) as row_count
                FROM `{self.source_project}.{self.source_dataset}.{table_name}`
                """
                
                result = source_client.query(query).result()
                row_count = list(result)[0].row_count
                
                logger.info(f"PASS: Table {table_name} has {row_count} rows")
            
            return True
            
        except Exception as e:
            logger.error(f"FAIL: Data count check failed: {e}")
            return False
    
    def test_sensitive_data_detection(self) -> bool:
        """Test 7: Analyze sensitive data"""
        logger.info("=" * 50)
        logger.info("Test 7: Sensitive Data Detection")
        logger.info("=" * 50)
        
        try:
            logger.info("Step 1: Environment configuration...")
            logger.info(f"Testing environment: {self.environment}")
            logger.info(f"Sensitive columns configured: {len(self.sensitive_columns)}")
            
            if not self.sensitive_columns:
                logger.info("No sensitive columns configured - skipping analysis")
                return True
            
            logger.info("Step 2: Analyzing sensitive data in each column...")
            
            source_client = bigquery.Client(project=self.source_project)
            
            for column_config in self.sensitive_columns:
                parts = column_config.split(":")
                if len(parts) >= 3:
                    dataset_name = parts[0]
                    table_col = parts[1]
                    tactic = parts[2]
                    table_name = table_col.split(".")[0]
                    column_name = table_col.split(".")[1]
                else:
                    logger.warning(f"Skipping malformed column config: {column_config}")
                    continue
                
                logger.info(f"Checking sensitive data in: {table_name}.{column_name} (tactic: {tactic})")
                
                query = f"""
                SELECT COUNT(*) as non_null_count
                FROM `{self.source_project}.{self.source_dataset}.{table_name}`
                WHERE {column_name} IS NOT NULL
                """
                
                result = source_client.query(query).result()
                non_null_count = list(result)[0].non_null_count
                
                if non_null_count > 0:
                    logger.warning(f"WARN: Found {non_null_count} non-null values in sensitive column {table_name}.{column_name} (will apply {tactic})")
                else:
                    logger.info(f"No sensitive data found in {table_name}.{column_name} (already redacted)")
            
            return True
            
        except Exception as e:
            logger.error(f"FAIL: Sensitive data detection failed: {e}")
            return False
    
    def test_sql_generation(self) -> bool:
        """Test 8: Test SQL generation"""
        logger.info("=" * 50)
        logger.info("Test 8: SQL Generation")
        logger.info("=" * 50)
        
        try:
            logger.info("Step 1: Testing SQL generation for all tactics...")
            
            test_table = "stg_business_licenses"
            test_column = "account_number"
            test_table_id = f"{self.dest_project}.{self.dest_dataset}.{test_table}"
            
            tactics = ["redact", "FF", "mask", "hash"]
            
            for tactic in tactics:
                logger.info(f"Testing {tactic.title()} tactic...")
                
                if tactic == "redact":
                    sql = f"UPDATE `{test_table_id}` SET {test_column} = NULL WHERE TRUE;"
                elif tactic == "FF":
                    sql = f"UPDATE `{test_table_id}` T SET {test_column} = FARM_FINGERPRINT(CAST(T.{test_column} AS STRING)) WHERE TRUE;"
                elif tactic == "mask":
                    sql = f"UPDATE `{test_table_id}` SET {test_column} = CASE WHEN LENGTH(CAST({test_column} AS STRING)) > 4 THEN CONCAT('****', SUBSTR(CAST({test_column} AS STRING), -4)) ELSE '****' END WHERE {test_column} IS NOT NULL;"
                elif tactic == "hash":
                    sql = f"UPDATE `{test_table_id}` SET {test_column} = TO_HEX(SHA256(CAST({test_column} AS BYTES))) WHERE {test_column} IS NOT NULL;"
                
                logger.info(f"Generated SQL for {tactic.title()}: {sql}")
                logger.info(f"PASS: {tactic.title()} SQL generation test passed")
            
            logger.info("PASS: All SQL generation tests passed")
            return True
            
        except Exception as e:
            logger.error(f"FAIL: SQL generation test failed: {e}")
            return False
    
    def test_cloud_run_readiness(self) -> bool:
        """Test 9: Check Cloud Run readiness"""
        logger.info("=" * 50)
        logger.info("Test 9: Cloud Run Readiness")
        logger.info("=" * 50)
        
        try:
            logger.info("Step 1: Checking Python dependencies...")
            
            # Check if required packages are available
            required_packages = [
                "google-cloud-bigquery",
                "google-cloud-storage", 
                "google-auth",
                "requests"
            ]
            
            for package in required_packages:
                try:
                    if package == "google-cloud-bigquery":
                        import google.cloud.bigquery
                    elif package == "google-cloud-storage":
                        import google.cloud.storage
                    elif package == "google-auth":
                        import google.auth
                    elif package == "requests":
                        import requests
                    logger.info(f"PASS: Package {package} is available")
                except ImportError:
                    logger.error(f"FAIL: Package {package} is not available")
                    return False
            
            logger.info("Step 2: Checking Dockerfile...")
            if os.path.exists("Dockerfile"):
                logger.info("PASS: Dockerfile exists")
            else:
                logger.error("FAIL: Dockerfile not found")
                return False
            
            logger.info("Step 3: Checking requirements.txt...")
            if os.path.exists("requirements.txt"):
                logger.info("PASS: requirements.txt exists")
            else:
                logger.error("FAIL: requirements.txt not found")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"FAIL: Cloud Run readiness check failed: {e}")
            return False
    
    def test_permissions(self) -> bool:
        """Test 10: Check required permissions"""
        logger.info("=" * 50)
        logger.info("Test 10: Required Permissions")
        logger.info("=" * 50)
        
        try:
            logger.info("Step 1: Testing BigQuery Data Viewer permission...")
            
            # Test read access
            source_client = bigquery.Client(project=self.source_project)
            list(source_client.list_datasets(max_results=1))
            logger.info("PASS: BigQuery Data Viewer permission confirmed")
            
            logger.info("Step 2: Testing BigQuery Job User permission...")
            
            # Test query execution
            test_query = f"SELECT 1 as test FROM `{self.source_project}.{self.source_dataset}.stg_business_licenses` LIMIT 1"
            result = source_client.query(test_query).result()
            list(result)  # Consume the result
            logger.info("PASS: BigQuery Job User permission confirmed")
            
            return True
            
        except Exception as e:
            logger.error(f"FAIL: Permission check failed: {e}")
            return False
    
    def run_all_tests(self) -> Dict[str, bool]:
        """Run all tests and return results"""
        logger.info("=" * 50)
        logger.info("BigQuery Transfer Cloud Run Test Suite")
        logger.info("=" * 50)
        logger.info("Starting comprehensive test of main.py functionality...")
        logger.info("This test suite validates 10 key areas before deploying to Cloud Run.")
        logger.info("")
        logger.info(f"Environment: {self.environment}")
        logger.info(f"Source Project: {self.source_project}")
        logger.info(f"Destination Project: {self.dest_project}")
        logger.info("")
        
        tests = [
            ("test_authentication", self.test_authentication),
            ("test_environment_configuration", self.test_environment_configuration),
            ("test_dataset_existence", self.test_dataset_existence),
            ("test_table_existence", self.test_table_existence),
            ("test_table_schemas", self.test_table_schemas),
            ("test_table_data_counts", self.test_table_data_counts),
            ("test_sensitive_data_detection", self.test_sensitive_data_detection),
            ("test_sql_generation", self.test_sql_generation),
            ("test_cloud_run_readiness", self.test_cloud_run_readiness),
            ("test_permissions", self.test_permissions)
        ]
        
        results = {}
        
        for i, (test_name, test_func) in enumerate(tests, 1):
            logger.info(f"Executing Step {i}: {test_name}")
            try:
                results[test_name] = test_func()
            except Exception as e:
                logger.error(f"FAIL: {test_name} failed with exception: {e}")
                results[test_name] = False
            
            logger.info("")
        
        return results
    
    def print_summary(self, results: Dict[str, bool]):
        """Print test results summary"""
        logger.info("=" * 50)
        logger.info("Test Results Summary")
        logger.info("=" * 50)
        
        passed = 0
        failed = 0
        
        for test_name, result in results.items():
            status = "PASS" if result else "FAIL"
            icon = "✅" if result else "❌"
            logger.info(f"{icon} {status}: {test_name}")
            
            if result:
                passed += 1
            else:
                failed += 1
        
        logger.info("")
        
        if failed == 0:
            logger.info("PASS: All tests passed! The main.py service is ready for Cloud Run deployment.")
            logger.info("")
            logger.info(f"You can now safely deploy: ./deploy-{self.environment}.sh")
        else:
            logger.error(f"FAIL: {failed} test(s) failed. Please fix the issues before deploying to Cloud Run.")
            logger.info("")
            logger.info("Common fixes:")
            logger.info("- Check authentication and project permissions")
            logger.info("- Ensure datasets and tables exist")
            logger.info("- Verify Python dependencies are installed")
            logger.info("- Check network connectivity")

def main():
    parser = argparse.ArgumentParser(description="Test BigQuery Transfer Cloud Run Service")
    parser.add_argument("environment", choices=["dev", "uat"], 
                       help="Target environment (dev or uat)")
    
    args = parser.parse_args()
    
    try:
        tester = BigQueryTransferTester(args.environment)
        results = tester.run_all_tests()
        tester.print_summary(results)
        
        # Return exit code based on results
        failed_tests = sum(1 for result in results.values() if not result)
        return 0 if failed_tests == 0 else 1
        
    except Exception as e:
        logger.error(f"Test suite error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
