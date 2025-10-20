# BigQuery Dataset Transfer & Remediation Scripts

## **Overview**

Scripts for securely transferring BigQuery datasets between projects with automatic sensitive data remediation. Supports multiple environments (dev, uat) with environment-specific redaction levels.

## **Files**

- **`bq_transfer.sh`** - Main transfer script with automatic remediation
- **`test_bq_transfer.sh`** - Comprehensive test suite (10 validation steps)
- **`README.md`** - This documentation

## **Multi-Environment Support**

### **Environment Configuration**
The scripts support two environments with different redaction levels:

| Environment | Usage | Dataset | Redaction Level |
|-------------|-------|---------|-----------------|
| **DEV** | `./bq_transfer.sh dev` | `dev_dts` | Full redaction (all → NULL) |
| **UAT** | `./bq_transfer.sh uat` | `uat_dts` | Minimal redaction (FF/mask) |

### **Current Project Setup**
All environments currently use the same projects for testing:
- **Source:** `sbox-rgodoy-001-20251124`
- **Destination:** `sbox-rgodoy-002-20251008`

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

### **2. Run Tests & Transfer**

**Development Environment:**
```bash
./test_bq_transfer.sh dev
./bq_transfer.sh dev
```

**UAT Environment:**
```bash
./test_bq_transfer.sh uat
./bq_transfer.sh uat
```

### **3. Redaction Tactics**

**Available Tactics:**
- `redact` - Set column value to NULL (full redaction)
- `mask` - Partial masking (shows last 4 characters for strings)
- `FF` - FARM_FINGERPRINT hash (preserves uniqueness for numeric data)
- `hash` - SHA256 hash (preserves uniqueness for analysis)

**Environment-Specific Behavior:**
- **DEV:** Full redaction for testing (all sensitive data → NULL)
- **UAT:** Minimal redaction (numeric data → FF hash, strings → mask)

## **Test Suite (10 Steps)**

1. **Authentication Validation** - Verifies keyfile and project access
2. **Environment Configuration** - Validates environment and tactics
3. **Dataset Existence** - Confirms source and destination datasets
4. **Table Existence** - Validates required tables exist
5. **Schema Analysis** - Extracts and validates table schemas
6. **Data Counts** - Confirms data exists for transfer
7. **Sensitive Data Detection** - Analyzes columns requiring remediation
8. **SQL Generation** - Tests remediation query syntax for all tactics
9. **Copy Command Validation** - Verifies bq copy availability
10. **Permission Verification** - Confirms required BigQuery roles

## **Transfer Process**

1. **Authentication Validation** - Tests keyfile before proceeding
2. **Dataset Processing** - Processes each configured dataset
3. **Table Copying** - Uses `bq cp` to copy tables with overwrite
4. **Automatic Remediation** - Applies redaction to sensitive columns

## **Workflow**

1. **Choose Environment** - Select dev or uat
2. **Run Tests** - `./test_bq_transfer.sh [environment]`
3. **Review Results** - Fix any issues found
4. **Execute Transfer** - `./bq_transfer.sh [environment]`
5. **Verify Data** - Check destination dataset

**Example:**
```bash
# Test UAT environment
./test_bq_transfer.sh uat

# If tests pass, run transfer
./bq_transfer.sh uat
```

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