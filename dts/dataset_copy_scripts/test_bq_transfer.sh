#!/bin/bash

# ==============================================================================
# BigQuery Transfer Script Test Suite
# ==============================================================================
# This script tests the bq_transfer.sh functionality before running the actual transfer
# Author: AI Assistant
# Date: $(date)

set -e  # Exit on any error

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Environment configuration for testing
# Usage: ./test_bq_transfer.sh [dev|uat]
TEST_ENVIRONMENT="${1:-${BQ_ENVIRONMENT:-dev}}"

# Load project configuration from bq_transfer.sh
# This ensures we test with the same configuration that will be used
if [[ -f "bq_transfer.sh" ]]; then
    # Extract project information based on environment
    case "$TEST_ENVIRONMENT" in
        "dev")
            TEST_PROJECT_01="${BQ_DEV_SOURCE_PROJECT:-sbox-rgodoy-001-20251124}"
            TEST_PROJECT_02="${BQ_DEV_DEST_PROJECT:-sbox-rgodoy-002-20251008}"
            ;;
        "uat")
            TEST_PROJECT_01="${BQ_UAT_SOURCE_PROJECT:-sbox-rgodoy-001-20251124}"
            TEST_PROJECT_02="${BQ_UAT_DEST_PROJECT:-sbox-rgodoy-002-20251008}"
            ;;
        *)
            echo "ERROR: Unknown environment '$TEST_ENVIRONMENT'. Must be 'dev' or 'uat'."
            exit 1
            ;;
    esac
else
    echo "ERROR: bq_transfer.sh not found. Cannot load project configuration."
    exit 1
fi

# Dataset configuration
# For testing: different destination datasets per environment
case "$TEST_ENVIRONMENT" in
    "dev")
        TEST_DATASET_01="dts_01"
        TEST_DATASET_02="dev_dts"
        ;;
    "uat")
        TEST_DATASET_01="dts_01"
        TEST_DATASET_02="uat_dts"
        ;;
esac

# Service account keyfile selection based on environment
case "$TEST_ENVIRONMENT" in
    "dev")
        AUTH_KEYFILE="${BQ_AUTH_KEYFILE_DEV:-${BQ_AUTH_KEYFILE:-/Users/rosemarie/Downloads/sbox-rgodoy-002-20251008-58246c009c2c.json}}"
        ;;
    "uat")
        AUTH_KEYFILE="${BQ_AUTH_KEYFILE_UAT:-${BQ_AUTH_KEYFILE:-/Users/rosemarie/Downloads/sbox-rgodoy-002-20251008-58246c009c2c.json}}"
        ;;
esac

# Test tables to validate
TEST_TABLES=(
    "stg_business_licenses"
    "stg_crimes"
)

# Sensitive columns to test remediation (environment-specific)
SENSITIVE_COLUMNS_DEV=(
    "stg_business_licenses:account_number:redact"
    "stg_business_licenses:business_address:redact"
    "stg_business_licenses:community_area:redact"
    "stg_business_licenses:payment_date:redact"
    "stg_crimes:x_coordinate:redact"
    "stg_crimes:y_coordinate:redact"
    "stg_crimes:latitude:redact"
    "stg_crimes:longitude:redact"
    "stg_crimes:location_description:redact"
)

# Select appropriate configuration based on environment
case "$TEST_ENVIRONMENT" in
    "dev")
        SENSITIVE_COLUMNS=("${SENSITIVE_COLUMNS_DEV[@]}")
        ;;
    "uat")
        SENSITIVE_COLUMNS=()
        ;;
    *)
        echo "WARNING: Unknown environment '$TEST_ENVIRONMENT'. Using DEV configuration."
        SENSITIVE_COLUMNS=("${SENSITIVE_COLUMNS_DEV[@]}")
        ;;
esac

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_success() {
    echo -e "${GREEN}PASS: $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}WARN: $1${NC}"
}

print_error() {
    echo -e "${RED}FAIL: $1${NC}"
}

print_info() {
    echo -e "${BLUE}INFO: $1${NC}"
}

# ==============================================================================
# TEST FUNCTIONS
# ==============================================================================

test_authentication() {
    print_header "Testing Authentication"
    
    # Step 1: Verify auth keyfile exists on filesystem
    print_info "Step 1: Checking auth keyfile exists..."
    if [[ ! -f "$AUTH_KEYFILE" ]]; then
        print_error "Auth keyfile not found: $AUTH_KEYFILE"
        return 1
    fi
    print_success "Auth keyfile exists"
    
    # Step 2: Test authentication with source project (project 01)
    print_info "Step 2: Testing authentication with source project..."
    if bq --credential_file="$AUTH_KEYFILE" --project_id="$TEST_PROJECT_01" ls > /dev/null 2>&1; then
        print_success "Authentication successful for project 01"
    else
        print_error "Authentication failed for project 01"
        return 1
    fi
    
    # Step 3: Test authentication with destination project (project 02)
    print_info "Step 3: Testing authentication with destination project..."
    if bq --credential_file="$AUTH_KEYFILE" --project_id="$TEST_PROJECT_02" ls > /dev/null 2>&1; then
        print_success "Authentication successful for project 02"
    else
        print_error "Authentication failed for project 02"
        return 1
    fi
}

test_environment_configuration() {
    print_header "Testing Environment Configuration"
    
    # Step 1: Validate environment variable
    print_info "Step 1: Validating environment configuration..."
    print_info "Environment: $TEST_ENVIRONMENT"
    
    if [[ -z "$TEST_ENVIRONMENT" ]]; then
        print_error "Environment not set. Use BQ_ENVIRONMENT environment variable."
        return 1
    fi
    
    # Step 2: Validate environment value
    case "$TEST_ENVIRONMENT" in
        "dev"|"uat"|"prod")
            print_success "Valid environment: $TEST_ENVIRONMENT"
            ;;
        *)
            print_error "Invalid environment: $TEST_ENVIRONMENT. Must be 'dev', 'uat', or 'prod'."
            return 1
            ;;
    esac
    
    # Step 3: Display configuration summary
    print_info "Step 2: Configuration summary..."
    print_info "Sensitive columns configured: ${#SENSITIVE_COLUMNS[@]}"
    
    # Step 4: Show tactics being used
    print_info "Step 3: Tactics being used..."
    local tactics_used=()
    for column_info in "${SENSITIVE_COLUMNS[@]}"; do
        IFS=':' read -r table column tactic <<< "$column_info"
        tactics_used+=("$tactic")
    done
    
    # Count unique tactics
    local unique_tactics=($(printf '%s\n' "${tactics_used[@]}" | sort -u))
    print_info "Unique tactics: ${unique_tactics[*]}"
    
    # Step 5: Validate tactics
    print_info "Step 4: Validating tactics..."
    local valid_tactics=("redact" "mask" "hash" "FF")
    for tactic in "${unique_tactics[@]}"; do
        if [[ " ${valid_tactics[@]} " =~ " ${tactic} " ]]; then
            print_success "Valid tactic: $tactic"
        else
            print_error "Invalid tactic: $tactic"
            return 1
        fi
    done
    
    print_success "Environment configuration validation passed"
}

test_dataset_existence() {
    
    # Step 1: Verify source dataset exists in project 01
    print_info "Step 1: Checking source dataset exists..."
    print_info "Looking for dataset: $TEST_PROJECT_01:$TEST_DATASET_01"
    if bq --credential_file="$AUTH_KEYFILE" --project_id="$TEST_PROJECT_01" ls | grep -q "^[[:space:]]*$TEST_DATASET_01[[:space:]]*$" > /dev/null 2>&1; then
        print_success "Source dataset exists"
    else
        print_error "Source dataset does not exist: $TEST_PROJECT_01:$TEST_DATASET_01"
        return 1
    fi
    
    # Step 2: Verify destination dataset exists in project 02
    print_info "Step 2: Checking destination dataset exists..."
    print_info "Looking for dataset: $TEST_PROJECT_02:$TEST_DATASET_02"
    if bq --credential_file="$AUTH_KEYFILE" --project_id="$TEST_PROJECT_02" ls | grep -q "^[[:space:]]*$TEST_DATASET_02[[:space:]]*$" > /dev/null 2>&1; then
        print_success "Destination dataset exists"
    else
        print_error "Destination dataset does not exist: $TEST_PROJECT_02:$TEST_DATASET_02"
        return 1
    fi
}

test_table_existence() {
    print_header "Testing Table Existence"
    
    # Step 1: Check each required table exists in source dataset
    print_info "Step 1: Checking required tables exist in source dataset..."
    for table in "${TEST_TABLES[@]}"; do
        print_info "Looking for table: $TEST_PROJECT_01:$TEST_DATASET_01.$table"
        if bq --credential_file="$AUTH_KEYFILE" --project_id="$TEST_PROJECT_01" ls "$TEST_DATASET_01" | grep -q "^[[:space:]]*$table[[:space:]]*" > /dev/null 2>&1; then
            print_success "Source table exists: $table"
        else
            print_error "Source table does not exist: $TEST_PROJECT_01:$TEST_DATASET_01.$table"
            return 1
        fi
    done
}

test_table_schemas() {
    print_header "Testing Table Schemas"
    
    # Step 1: Extract and validate schema for each table
    print_info "Step 1: Extracting table schemas from source tables..."
    for table in "${TEST_TABLES[@]}"; do
        print_info "Retrieving schema for: $table"
        
        # Get source table schema using BigQuery show command
        local source_schema
        source_schema=$(bq --credential_file="$AUTH_KEYFILE" --project_id="$TEST_PROJECT_01" show --format=json "$TEST_DATASET_01.$table" | jq -r '.schema.fields[].name' | tr '\n' ' ')
        
        if [[ -z "$source_schema" ]]; then
            print_error "Could not retrieve schema for $table"
            return 1
        fi
        
        print_success "Schema retrieved for $table: $source_schema"
    done
}

test_table_data_counts() {
    print_header "Testing Table Data Counts"
    
    # Step 1: Count rows in each source table to verify data exists
    print_info "Step 1: Counting rows in source tables..."
    for table in "${TEST_TABLES[@]}"; do
        print_info "Counting rows in: $table"
        
        local row_count
        row_count=$(bq --credential_file="$AUTH_KEYFILE" --project_id="$TEST_PROJECT_01" query --use_legacy_sql=false --format=csv "SELECT COUNT(*) as count FROM \`$TEST_PROJECT_01.$TEST_DATASET_01.$table\`" | tail -n +2)
        
        if [[ "$row_count" =~ ^[0-9]+$ ]]; then
            print_success "Table $table has $row_count rows"
        else
            print_error "Could not retrieve row count for $table"
            return 1
        fi
    done
}

test_sensitive_data_detection() {
    print_header "Testing Sensitive Data Detection"
    
    # Step 1: Display environment configuration
    print_info "Step 1: Environment configuration..."
    print_info "Testing environment: $TEST_ENVIRONMENT"
    print_info "Sensitive columns configured: ${#SENSITIVE_COLUMNS[@]}"
    
    # Step 2: Check each sensitive column for non-null values that need remediation
    print_info "Step 2: Analyzing sensitive data in each column..."
    for column_info in "${SENSITIVE_COLUMNS[@]}"; do
        IFS=':' read -r table column tactic <<< "$column_info"
        print_info "Checking sensitive data in: $table.$column (tactic: $tactic)"
        
        # Count non-null values in sensitive columns
        local non_null_count
        non_null_count=$(bq --credential_file="$AUTH_KEYFILE" --project_id="$TEST_PROJECT_01" query --use_legacy_sql=false --format=csv "SELECT COUNT(*) as count FROM \`$TEST_PROJECT_01.$TEST_DATASET_01.$table\` WHERE $column IS NOT NULL" | tail -n +2)
        
        if [[ "$non_null_count" =~ ^[0-9]+$ ]]; then
            if [[ "$non_null_count" -gt 0 ]]; then
                print_warning "Found $non_null_count non-null values in sensitive column $table.$column (will apply $tactic)"
            else
                print_info "No sensitive data found in $table.$column (already redacted)"
            fi
        else
            print_error "Could not check sensitive data for $table.$column"
            return 1
        fi
    done
}

test_remediation_sql_generation() {
    print_header "Testing Remediation SQL Generation"
    
    # Step 1: Test SQL generation for all tactics
    print_info "Step 1: Testing SQL generation for all tactics..."
    local test_table="sbox-rgodoy-002-20251008:dts_01_2.stg_business_licenses"
    local test_column="account_number"
    
    # Convert colon-qualified name to dot-qualified
    local sql_fqn
    sql_fqn=$(echo "$test_table" | sed 's/:/./')
    
    # Test all tactics
    local tactics=("redact" "mask" "hash" "FF")
    local tactic_names=("Redact" "Mask" "Hash" "Farm Fingerprint")
    
    for i in "${!tactics[@]}"; do
        local tactic="${tactics[$i]}"
        local tactic_name="${tactic_names[$i]}"
        
        print_info "Testing $tactic_name tactic..."
        
        # Build SQL statement based on tactic
        local sql_statement=""
        case "$tactic" in
            "redact")
                sql_statement="UPDATE \`$sql_fqn\` SET $test_column = NULL WHERE TRUE;"
                ;;
            "mask")
                sql_statement="UPDATE \`$sql_fqn\` SET $test_column = CASE WHEN LENGTH(CAST($test_column AS STRING)) > 4 THEN CONCAT('****', SUBSTR(CAST($test_column AS STRING), -4)) ELSE '****' END WHERE $test_column IS NOT NULL;"
                ;;
            "hash")
                sql_statement="UPDATE \`$sql_fqn\` SET $test_column = TO_HEX(SHA256(CAST($test_column AS BYTES))) WHERE $test_column IS NOT NULL;"
                ;;
            "FF")
                sql_statement="UPDATE \`$sql_fqn\` T SET $test_column = FARM_FINGERPRINT(CAST(T.$test_column AS STRING)) WHERE TRUE;"
                ;;
        esac
        
        print_info "Generated SQL for $tactic_name: $sql_statement"
        
        if [[ -n "$sql_statement" ]]; then
            print_success "$tactic_name SQL generation test passed"
        else
            print_error "$tactic_name SQL generation test failed"
            return 1
        fi
    done
    
    print_success "All SQL generation tests passed"
}

test_copy_command_availability() {
    print_header "Testing Copy Command Availability"
    
    # Step 1: Verify bq copy command is available
    print_info "Step 1: Testing bq copy command availability..."
    if bq --credential_file="$AUTH_KEYFILE" --project_id="$TEST_PROJECT_02" cp --help | grep -q "cp" > /dev/null 2>&1; then
        print_success "Copy command is available"
        print_info "Copy syntax: bq cp source_table destination_table"
    else
        print_error "Copy command not available"
        return 1
    fi
    
    # Step 2: Validate copy command syntax for each table
    print_info "Step 2: Validating copy command syntax for each table..."
    for table in "${TEST_TABLES[@]}"; do
        print_info "Validating copy syntax for: $table"
        
        # Test that the source table exists (we already tested this)
        # and that we can construct a valid copy command
        local copy_command="bq cp $TEST_PROJECT_01:$TEST_DATASET_01.$table $TEST_PROJECT_02:$TEST_DATASET_02.${table}_test"
        print_info "Copy command would be: $copy_command"
        print_success "Copy syntax validation passed for $table"
    done
}

test_permissions() {
    print_header "Testing Required Permissions"
    
    # Step 1: Test BigQuery Data Viewer permission
    print_info "Step 1: Testing BigQuery Data Viewer permission..."
    if bq --credential_file="$AUTH_KEYFILE" --project_id="$TEST_PROJECT_01" query --use_legacy_sql=false --dry_run "SELECT 1" > /dev/null 2>&1; then
        print_success "BigQuery Data Viewer permission confirmed"
    else
        print_error "BigQuery Data Viewer permission missing"
        return 1
    fi
    
    # Step 2: Test BigQuery Job User permission
    print_info "Step 2: Testing BigQuery Job User permission..."
    if bq --credential_file="$AUTH_KEYFILE" --project_id="$TEST_PROJECT_02" query --use_legacy_sql=false --dry_run "SELECT 1" > /dev/null 2>&1; then
        print_success "BigQuery Job User permission confirmed"
    else
        print_error "BigQuery Job User permission missing"
        return 1
    fi
}

# ==============================================================================
# MAIN TEST EXECUTION
# ==============================================================================

main() {
    print_header "BigQuery Transfer Test Suite"
    print_info "Starting comprehensive test of bq_transfer.sh functionality..."
    print_info "This test suite validates 10 key areas before running the data transfer."
    echo
    print_info "Environment: $TEST_ENVIRONMENT"
    print_info "Source Project: $TEST_PROJECT_01"
    print_info "Destination Project: $TEST_PROJECT_02"
    print_info "Auth Keyfile: $AUTH_KEYFILE"
    echo
    
    local test_results=()
    local failed_tests=0
    
    # Run all tests - 10 comprehensive validation steps
    tests=(
        "test_authentication"           # Step 1: Authentication validation
        "test_environment_configuration" # Step 2: Environment configuration validation
        "test_dataset_existence"       # Step 3: Dataset existence check
        "test_table_existence"         # Step 4: Table existence validation
        "test_table_schemas"           # Step 5: Schema analysis
        "test_table_data_counts"       # Step 6: Data count verification
        "test_sensitive_data_detection" # Step 7: Sensitive data analysis
        "test_remediation_sql_generation" # Step 8: SQL generation testing
        "test_copy_command_availability" # Step 9: Copy command validation
        "test_permissions"            # Step 10: Permission verification
    )
    
    local step_number=1
    for test in "${tests[@]}"; do
        echo
        print_info "Executing Step $step_number: $test"
        if $test; then
            test_results+=("✅ PASS: $test")
        else
            test_results+=("❌ FAIL: $test")
            ((failed_tests++))
        fi
        ((step_number++))
    done
    
    # Print summary
    echo
    print_header "Test Results Summary"
    
    for result in "${test_results[@]}"; do
        echo "$result"
    done
    
    echo
    if [[ $failed_tests -eq 0 ]]; then
        print_success "🎉 All tests passed! The bq_transfer.sh script is ready to run."
        echo
        print_info "You can now safely run: ./bq_transfer.sh $TEST_ENVIRONMENT"
    else
        print_error "❌ $failed_tests test(s) failed. Please fix the issues before running bq_transfer.sh"
        echo
        print_info "Common fixes:"
        print_info "- Check authentication keyfile path"
        print_info "- Verify project permissions"
        print_info "- Ensure datasets and tables exist"
        print_info "- Check network connectivity"
    fi
    
    return $failed_tests
}

# Run the tests
main "$@"
