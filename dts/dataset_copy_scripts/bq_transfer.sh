#!/bin/bash

# ============================================================================== 
# CONFIGURATION SECTION
# ============================================================================== 

# Environment Configuration
# Usage: ./bq_transfer.sh [dev|uat]
# Set to 'dev' or 'uat' to control project selection and redaction behavior
ENVIRONMENT="${1:-${BQ_ENVIRONMENT:-dev}}"

# Dynamic project selection based on environment
case "$ENVIRONMENT" in
    "dev")
        SOURCE_PROJECT="${BQ_DEV_SOURCE_PROJECT:-sbox-rgodoy-001-20251124}"
        DEST_PROJECT="${BQ_DEV_DEST_PROJECT:-sbox-rgodoy-002-20251008}"
        DATASET_MAPPING="dts_01:dev_dts"
        ;;
    "uat")
        SOURCE_PROJECT="${BQ_UAT_SOURCE_PROJECT:-sbox-rgodoy-001-20251124}"
        DEST_PROJECT="${BQ_UAT_DEST_PROJECT:-sbox-rgodoy-002-20251008}"
        DATASET_MAPPING="dts_01:uat_dts"
        ;;
    *)
        echo "ERROR: Unknown environment '$ENVIRONMENT'. Must be 'dev' or 'uat'."
        echo "Usage: ./bq_transfer.sh [dev|uat]"
        echo "Or set BQ_ENVIRONMENT environment variable"
        exit 1
        ;;
esac

# Build dataset configuration dynamically
IFS=':' read -r SRC_DATASET DEST_DATASET <<< "$DATASET_MAPPING"
DATASETS_TO_COPY=(
    "$SOURCE_PROJECT:$SRC_DATASET:$DEST_PROJECT:$DEST_DATASET"
)

# Service account keyfile selection based on environment
case "$ENVIRONMENT" in
    "dev")
        AUTH_KEYFILE="${BQ_AUTH_KEYFILE_DEV:-${BQ_AUTH_KEYFILE:-/Users/rosemarie/Downloads/sbox-rgodoy-002-20251008-58246c009c2c.json}}"
        ;;
    "uat")
        AUTH_KEYFILE="${BQ_AUTH_KEYFILE_UAT:-${BQ_AUTH_KEYFILE:-/Users/rosemarie/Downloads/sbox-rgodoy-002-20251008-58246c009c2c.json}}"
        ;;
esac

# Default BigQuery location for destination jobs/datasets
DEFAULT_LOCATION="us-central1"

# Define sensitive columns and remediation tactics by environment
# Format: DATASET:TABLE.COLUMN.TACTIC
# TACTICS: 'redact' (set to NULL), 'FF' (FARM_FINGERPRINT), 'mask' (partial masking), 'hash' (SHA256 hash)

# Development Environment - Full redaction for testing
SENSITIVE_TABLE_COLUMNS_DEV=(
    # Business Licenses - redact sensitive columns
    "dts_01:stg_business_licenses.account_number.redact"
    "dts_01:stg_business_licenses.business_address.redact"
    "dts_01:stg_business_licenses.community_area.redact"
    "dts_01:stg_business_licenses.payment_date.redact"
    
    # Crimes - redact location/coordinate columns
    "dts_01:stg_crimes.x_coordinate.redact"
    "dts_01:stg_crimes.y_coordinate.redact"
    "dts_01:stg_crimes.latitude.redact"
    "dts_01:stg_crimes.longitude.redact"
    "dts_01:stg_crimes.location_description.redact"
)

# Select appropriate configuration based on environment
case "$ENVIRONMENT" in
    "dev")
        SENSITIVE_TABLE_COLUMNS=("${SENSITIVE_TABLE_COLUMNS_DEV[@]}")
        echo "Using DEV environment configuration (full redaction)"
        ;;
    "uat")
        SENSITIVE_TABLE_COLUMNS=()
        echo "Using UAT environment configuration (no redaction)"
        ;;
    *)
        echo "WARNING: Unknown environment '$ENVIRONMENT'. Using DEV configuration."
        SENSITIVE_TABLE_COLUMNS=("${SENSITIVE_TABLE_COLUMNS_DEV[@]}")
        ;;
esac

# ==============================================================================
# TEMPLATED SQL STATEMENTS
# ==============================================================================

# SQL template for REDACT (set column value to NULL)
SQL_REDACT_TEMPLATE="
UPDATE \`{FULL_TABLE_NAME}\`
SET {COLUMN_NAME} = NULL
WHERE TRUE;
"

# SQL template for FARM_FINGERPRINT (FF)
SQL_FF_TEMPLATE="
UPDATE \`{FULL_TABLE_NAME}\` T
SET {COLUMN_NAME} = FARM_FINGERPRINT(CAST(T.{COLUMN_NAME} AS STRING))
WHERE TRUE;
"

# SQL template for MASK (partial masking)
SQL_MASK_TEMPLATE="
UPDATE \`{FULL_TABLE_NAME}\`
SET {COLUMN_NAME} = CASE 
    WHEN LENGTH(CAST({COLUMN_NAME} AS STRING)) > 4 THEN 
        CONCAT('****', SUBSTR(CAST({COLUMN_NAME} AS STRING), -4))
    ELSE '****'
END
WHERE {COLUMN_NAME} IS NOT NULL;
"

# SQL template for HASH (SHA256 hash)
SQL_HASH_TEMPLATE="
UPDATE \`{FULL_TABLE_NAME}\`
SET {COLUMN_NAME} = TO_HEX(SHA256(CAST({COLUMN_NAME} AS BYTES)))
WHERE {COLUMN_NAME} IS NOT NULL;
"

# ==============================================================================
# FUNCTIONS
# ==============================================================================

# Validate authentication keyfile
validate_auth_keyfile() {
    if [[ ! -f "$AUTH_KEYFILE" ]]; then
        echo "ERROR: Auth keyfile not found: $AUTH_KEYFILE"
        echo "Set BQ_AUTH_KEYFILE_${ENVIRONMENT^^} environment variable or check file path"
        exit 1
    fi
    
    # Test authentication with a simple query using the destination project
    if ! bq --credential_file="$AUTH_KEYFILE" --project_id="$DEST_PROJECT" query --use_legacy_sql=false --dry_run "SELECT 1" > /dev/null 2>&1; then
        echo "ERROR: Auth keyfile is invalid or expired for project $DEST_PROJECT"
        echo "Please check the keyfile path and permissions"
        exit 1
    fi
    
    echo "Authentication validated successfully for $ENVIRONMENT environment"
    echo "Source Project: $SOURCE_PROJECT"
    echo "Destination Project: $DEST_PROJECT"
}

# Function to execute a templated SQL statement
execute_sql() {
    local full_table_name="$1"
    local column_name="$2"
    local tactic="$3"

    echo "  -> Applying remediation for $column_name with TACTIC: $tactic"

    # Convert colon-qualified name (proj:ds.table) to dot-qualified (proj.ds.table) for SQL
    local sql_fqn
    sql_fqn=$(echo "$full_table_name" | sed 's/:/./')

    # Build SQL statement directly
    local sql_statement=""
    if [[ "$tactic" == "redact" ]]; then
        sql_statement="UPDATE \`$sql_fqn\` SET $column_name = NULL WHERE TRUE;"
    elif [[ "$tactic" == "FF" ]]; then
        sql_statement="UPDATE \`$sql_fqn\` T SET $column_name = FARM_FINGERPRINT(CAST(T.$column_name AS STRING)) WHERE TRUE;"
    elif [[ "$tactic" == "mask" ]]; then
        sql_statement="UPDATE \`$sql_fqn\` SET $column_name = CASE WHEN LENGTH(CAST($column_name AS STRING)) > 4 THEN CONCAT('****', SUBSTR(CAST($column_name AS STRING), -4)) ELSE '****' END WHERE $column_name IS NOT NULL;"
    elif [[ "$tactic" == "hash" ]]; then
        sql_statement="UPDATE \`$sql_fqn\` SET $column_name = TO_HEX(SHA256(CAST($column_name AS BYTES))) WHERE $column_name IS NOT NULL;"
    else
        echo "  -> ERROR: Unknown tactic '$tactic'. Skipping."
        return 1
    fi

    # Derive project from full_table_name (before ':') for job project
    local job_project
    job_project=${full_table_name%%:*}

    # Execute the query using bq command line tool with auth, project and location
    echo "$sql_statement"
    local job_location
    job_location="${DEST_LOCATION:-$DEFAULT_LOCATION}"
    # DML UPDATE runs in the destination project where the table resides
    bq "${BQ_AUTH_ARGS[@]}" --project_id="$job_project" --location="$job_location" query --nouse_legacy_sql --quiet "$sql_statement"
    if [ $? -ne 0 ]; then
        echo "  -> ERROR: BigQuery query failed for $full_table_name ($tactic)."
        return 1
    else
        echo "  -> Remediation completed successfully."
    fi
}

# ============================================================================== 
# MAIN LOGIC
# ============================================================================== 

echo "=========================================="
echo "BIGQUERY DATA TRANSFER AND REMEDIATION"
echo "=========================================="
echo "Environment: $ENVIRONMENT"
echo "Source Project: $SOURCE_PROJECT"
echo "Destination Project: $DEST_PROJECT"
echo "Dataset Mapping: $DATASET_MAPPING"
echo "Using service account credentials from $AUTH_KEYFILE"
echo "=========================================="

# Validate authentication before proceeding
validate_auth_keyfile

# Build optional bq auth args
BQ_AUTH_ARGS=()
if [ -n "$AUTH_KEYFILE" ]; then
    if [ ! -f "$AUTH_KEYFILE" ]; then
        echo "ERROR: AUTH_KEYFILE not found at $AUTH_KEYFILE"
        exit 1
    fi
    BQ_AUTH_ARGS=(--credential_file="$AUTH_KEYFILE")
    echo "Using service account credentials from $AUTH_KEYFILE"
fi

# 1. Iterate through datasets and copy tables
for dataset_config in "${DATASETS_TO_COPY[@]}"; do
    IFS=':' read -r SRC_PROJ SRC_DS DEST_PROJ DEST_DS <<< "$dataset_config"
    echo "--- Processing Dataset: $SRC_PROJ:$SRC_DS -> $DEST_PROJ:$DEST_DS ---"

    # Detect destination dataset location and create it if missing
    DEST_LOCATION=""
    if bq "${BQ_AUTH_ARGS[@]}" --project_id="$DEST_PROJ" show --format=json -d "$DEST_PROJ:$DEST_DS" >/tmp/.dest_ds.json 2>/dev/null; then
        DEST_LOCATION=$(cat /tmp/.dest_ds.json | sed -n 's/.*"location"\s*:\s*"\([^"]*\)".*/\1/p' | head -n1)
        [ -z "$DEST_LOCATION" ] && DEST_LOCATION="$DEFAULT_LOCATION"
        echo "  - Destination dataset exists at location: $DEST_LOCATION"
    else
        echo "  - Destination dataset $DEST_PROJ:$DEST_DS not found. Creating it in $DEFAULT_LOCATION..."
        if ! bq "${BQ_AUTH_ARGS[@]}" --project_id="$DEST_PROJ" --location="$DEFAULT_LOCATION" mk -d "$DEST_PROJ:$DEST_DS"; then
            echo "  - ERROR: Failed to create destination dataset $DEST_PROJ:$DEST_DS"
            continue
        fi
        DEST_LOCATION="$DEFAULT_LOCATION"
    fi

    # Detect source dataset location (needed for cross-project copy)
    SRC_LOCATION=""
    if bq "${BQ_AUTH_ARGS[@]}" --project_id="$SRC_PROJ" show --format=json -d "$SRC_PROJ:$SRC_DS" >/tmp/.src_ds.json 2>/dev/null; then
        SRC_LOCATION=$(sed -n 's/.*"location"\s*:\s*"\([^"]*\)".*/\1/p' </tmp/.src_ds.json | head -n1)
        [ -z "$SRC_LOCATION" ] && SRC_LOCATION="$DEFAULT_LOCATION"
        echo "  - Source dataset exists at location: $SRC_LOCATION"
    else
        echo "  - ERROR: Unable to read source dataset $SRC_PROJ:$SRC_DS. Ensure the credential has at least BigQuery Metadata Viewer on the SOURCE project."
        continue
    fi

    # Enforce location match between source and destination datasets for native copy
    if [ "$SRC_LOCATION" != "$DEST_LOCATION" ]; then
        echo "  - ERROR: Location mismatch. Source is $SRC_LOCATION, destination is $DEST_LOCATION. Native copy requires matching locations."
        echo "    Fix: Create destination dataset in $SRC_LOCATION, or perform extract+load. Skipping dataset."
        continue
    fi

    # List tables using INFORMATION_SCHEMA (BASE TABLE) with dataset scoping; avoids backtick quoting issues
    source_tables=$(bq "${BQ_AUTH_ARGS[@]}" --project_id="$SRC_PROJ" --location="$SRC_LOCATION" --dataset_id="$SRC_PROJ:$SRC_DS" \
        query --nouse_legacy_sql --format=csv --quiet \
        "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_type='BASE TABLE' AND NOT STARTS_WITH(table_name, 'ext_') ORDER BY table_name" 2>/dev/null | tail -n +2)

    # Debug print what we found (non-fatal)
    if [ -z "$source_tables" ]; then
        echo "  - Debug: No BASE TABLE entries returned by INFORMATION_SCHEMA."
    else
        echo "  - Debug: Found tables (BASE TABLE): $source_tables"
    fi

    if [ -z "$source_tables" ]; then
        echo "  - No **native** tables (Type=TABLE) found in $SRC_PROJ:$SRC_DS."
        echo "    Note: External tables (Type=EXTERNAL) and views are intentionally ignored."
        echo "    If you expected tables, verify credentials can list tables and that tables exist."
        continue
    fi

    for table_name in $source_tables; do
        # Skip suspicious/non-table entries defensively
        if [[ ! "$table_name" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
            echo "  - Skipping non-table entry: $table_name"
            continue
        fi

        SRC_FULL_TABLE="$SRC_PROJ:$SRC_DS.$table_name"
        DEST_FULL_TABLE="$DEST_PROJ:$DEST_DS.$table_name"

        
        echo "  - Copying native table: $SRC_FULL_TABLE to $DEST_FULL_TABLE"

        # Use bq cp for tables. The -f flag ensures overwrite.
        # Use the SOURCE dataset location for the copy job
        bq "${BQ_AUTH_ARGS[@]}" --project_id="$DEST_PROJ" --location="$SRC_LOCATION" cp -f "$SRC_FULL_TABLE" "$DEST_FULL_TABLE"
        
        if [ $? -ne 0 ]; then
            echo "  - ERROR: Table copy failed for $table_name. Skipping remediation."
            continue
        fi

        echo "  - Copy successful. Checking for remediation..."

        # 2. Check for remediation tasks for this table in the destination dataset
        for sensitive_config in "${SENSITIVE_TABLE_COLUMNS[@]}"; do
            IFS=':' read -r CFG_DS CFG_TC <<< "$sensitive_config"
            
            # Match config to source dataset name
            if [[ "$CFG_DS" == "$SRC_DS" ]]; then
                IFS='.' read -r CFG_T CFG_C CFG_TACTIC <<< "$CFG_TC"
                
                # Check if the table name matches (case insensitive check)
                cfg_t_lower=$(echo "$CFG_T" | tr '[:upper:]' '[:lower:]')
                table_name_lower=$(echo "$table_name" | tr '[:upper:]' '[:lower:]')
                if [[ "$cfg_t_lower" == "$table_name_lower" ]]; then
                    execute_sql "$DEST_FULL_TABLE" "$CFG_C" "$CFG_TACTIC"
                fi
            fi
        done
    done
done

echo "Script finished."