# BigQuery Dataset Transfer & Remediation Scripts

## **Overview**

Scripts for securely transferring BigQuery datasets between projects with automatic sensitive data remediation. Includes comprehensive testing to ensure successful transfers.

## **Files**

- **`bq_transfer.sh`** - Main transfer script with automatic remediation
- **`test_bq_transfer.sh`** - Comprehensive test suite (9 validation steps)
- **`README.md`** - This documentation

## **Infrastructure Requirements**

### **Minimal Setup:**
- **Local Machine/Server** - Where scripts run
- **Google Cloud SDK** - For `gcloud` and `bq` commands
- **Service Account Keyfile** - JSON authentication file
- **Network Access** - Internet connectivity to Google Cloud APIs

### **BigQuery Native Copy Technique:**
- **Direct project-to-project transfer** - No intermediate storage
- **Preserves all metadata** - Schema, partitioning, clustering
- **Atomic operation** - Complete success or clean failure
- **Location-aware** - Requires same region for source/destination

## **Pricing**

### **BigQuery Copy (`bq cp`) Pricing:**
- **✅ Copy Operation:** FREE (no charges)
- **📊 Storage Only:** $0.02/GB/month (active) or $0.01/GB/month (long-term)
- **🌐 Same Region:** FREE (no transfer fees)

### **Cost Examples:**
| Dataset Size | Copy Cost | Monthly Storage |
|--------------|-----------|----------------|
| **1 GB** | FREE | $0.02 |
| **100 GB** | FREE | $2.00 |
| **1 TB** | FREE | $20.00 |

## **Quick Start**

### **1. Setup Authentication**
```bash
export BQ_AUTH_KEYFILE="/path/to/your/service-account-key.json"
```

### **2. Configure Datasets**
Edit `DATASETS_TO_COPY` in `bq_transfer.sh`:
```bash
DATASETS_TO_COPY=(
    "SOURCE_PROJECT:SOURCE_DATASET:DEST_PROJECT:DEST_DATASET"
)
```

### **3. Configure Sensitive Columns**
Edit `SENSITIVE_TABLE_COLUMNS` in `bq_transfer.sh`

### **4. Run Tests & Transfer**
```bash
./test_bq_transfer.sh  # Validate everything first
./bq_transfer.sh       # Execute transfer
```

## **Test Suite (9 Steps)**

1. **Authentication Validation** - Verifies keyfile and project access
2. **Dataset Existence** - Confirms source and destination datasets
3. **Table Existence** - Validates required tables exist
4. **Schema Analysis** - Extracts and validates table schemas
5. **Data Counts** - Confirms data exists for transfer
6. **Sensitive Data Detection** - Analyzes columns requiring remediation
7. **SQL Generation** - Tests remediation query syntax
8. **Copy Command Validation** - Verifies bq copy availability
9. **Permission Verification** - Confirms required BigQuery roles

## **Transfer Process**

1. **Authentication Validation** - Tests keyfile before proceeding
2. **Dataset Processing** - Processes each configured dataset
3. **Table Copying** - Uses `bq cp` to copy tables with overwrite
4. **Automatic Remediation** - Applies redaction to sensitive columns

## **Workflow**

1. **Configure** datasets and sensitive columns
2. **Set** authentication (environment variable)
3. **Run** test suite: `./test_bq_transfer.sh`
4. **Review** test results and fix any issues
5. **Execute** transfer: `./bq_transfer.sh`
6. **Verify** data in destination project

## **Required Permissions**

### **Source Project Service Account:**
- BigQuery Data Viewer
- BigQuery Data Editor
- BigQuery Job User
- BigQuery Metadata Viewer

### **Destination Project Service Account in Source Project:**
- BigQuery Data Viewer 
- BigQuery Metadata Viewer

### **Destination Project Service Account:**
- BigQuery Data Editor
- BigQuery Job User
- BigQuery Metadata Viewer

## **Troubleshooting**

### **Common Issues:**

**Authentication Errors:**
- Check keyfile path and permissions
- Verify service account has required roles
- Test manually: `bq ls --project_id=PROJECT_ID`

**Dataset Not Found:**
- Verify dataset names and projects
- Check dataset locations match
- Confirm access permissions

**Table Copy Failures:**
- Check table exists in source
- Verify destination permissions
- Check for naming conflicts

**Remediation Failures:**
- Verify column names exist
- Check SQL syntax
- Confirm destination table permissions

