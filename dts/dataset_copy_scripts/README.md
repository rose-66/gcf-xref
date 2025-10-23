# BigQuery Dataset Transfer & Remediation Pipeline

## **Overview**

**Cloud Run-based serverless service** for securely transferring BigQuery datasets between projects with automatic sensitive data remediation. Deployed as a containerized Python/Flask application on Google Cloud Run, supporting multiple environments (dev, uat) with environment-specific redaction levels.

### **Architecture**
- **Deployment**: Google Cloud Run (Serverless containers)
- **Runtime**: Python 3.11 + Flask
- **Authentication**: Cloud Run service account (`dts-cloud-run-deploy-sa`)
- **Transfer Method**: BigQuery native cross-project copy (no intermediate storage)
- **Execution**: HTTP POST triggers via authenticated API endpoints

## **Files**

### **Cloud Run Deployment**
- **`main.py`** - Cloud Run service implementation (Python/Flask)
- **`Dockerfile`** - Container image definition for Cloud Run
- **`requirements.txt`** - Python dependencies
- **`deploy-dev.sh`** - Deploy & run script for dev environment 
- **`deploy-uat.sh`** - Deploy & run script for uat environment 

### **Testing & Validation**
- **`test_main.py`** - Python service test suite (10 validation steps)

## **Multi-Environment Support**

### **Environment Configuration**
The Cloud Run service supports two environments with different redaction levels:

| Environment | Cloud Run Service | Dataset | Redaction Level |
|-------------|-------------------|---------|-----------------|
| **DEV** | `bq-transfer-dev` | `dev_dts` | Full redaction (all → NULL) |
| **UAT** | `bq-transfer-uat` | `uat_dts` | No redaction (original data) |

### **Current Project Setup**
All environments currently use the same projects:
- **Source:** `sbox-rgodoy-001-20251124` (read-only access)
- **Destination:** `sbox-rgodoy-002-20251008` (write access)
- **Cloud Run Region:** `us-central1`

## **Cloud Run Infrastructure**

### **Components**
- **Cloud Run Service** - Serverless container hosting the Flask API
- **Google Container Registry** - Stores Docker images
- **Cloud Build** - Builds and pushes container images
- **BigQuery API** - Data transfer and transformation
- **Service Account** - `dts-cloud-run-deploy-sa@sbox-rgodoy-002-20251008.iam.gserviceaccount.com`

## **Pricing**

### **BigQuery Costs:**
- **Copy Operation:** FREE (no charges)
- **Storage Only:** $0.02/GB/month (active) or $0.01/GB/month (long-term)
- **Same Region:** FREE (no transfer fees)


## **Quick Start - Cloud Run Deployment**

```bash

# Deploy & execute transfer for DEV (with redaction)
./deploy-dev.sh

# Deploy & execute transfer for UAT (no redaction)
./deploy-uat.sh
```

**What happens:**
1. ✅ Builds Docker container image
2. ✅ Pushes to Google Container Registry  
3. ✅ Deploys to Cloud Run
4. ✅ Automatically triggers dataset transfer
5. ✅ Shows transfer results and logs

---

### **Alternative: Step-by-Step Testing**

If you want to test before deploying:

#### **1. Test Locally (Optional)**
```bash
# Install dependencies
pip install -r requirements.txt

# Run Python test suite
python3 test_main.py dev  # or uat
```

#### **2. Deploy to Cloud Run**
```bash
./deploy-dev.sh  # or ./deploy-uat.sh
```

#### **3. Manual Trigger (if needed)**
```bash
# Get service URL
SERVICE_URL=$(gcloud run services describe bq-transfer-dev \
    --region us-central1 \
    --project sbox-rgodoy-002-20251008 \
    --format="value(status.url)")

# Trigger transfer manually
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
     -H "Content-Type: application/json" \
     -X POST \
     -d '{"environment": "dev"}' \
     "$SERVICE_URL/transfer"
```

#### **4. View Logs**
```bash
gcloud logging read \
    "resource.type=cloud_run_revision AND resource.labels.service_name=bq-transfer-dev" \
    --project=sbox-rgodoy-002-20251008 \
    --limit=50
```

---

## **Required Permissions**

### **Cloud Run Service Account**
Service Account: `dts-cloud-run-deploy-sa@sbox-rgodoy-002-20251008.iam.gserviceaccount.com`

### **Summary of Required Roles:**
| Project | Role | Purpose |
|---------|------|---------|
| **Source (001)** | `bigquery.dataViewer` | Read tables and data |
| **Source (001)** | `bigquery.jobUser` | Run queries to check data |
| **Destination (002)** | `bigquery.dataEditor` | Create/update/copy tables |
| **Destination (002)** | `bigquery.jobUser` | Run copy and redaction jobs |

## **Troubleshooting**

### **Common Cloud Run Issues:**

**Permission Errors (403):**
```
ERROR: Access Denied: Dataset ... Permission bigquery.xxx.xxx denied
```
- Verify service account has required roles on BOTH projects
- Run the permission grant commands above
- Check that service account exists: `dts-cloud-run-deploy-sa@sbox-rgodoy-002-20251008.iam.gserviceaccount.com`

**Transfer Failed (500):**
```
{"status": "error", "message": "Dataset transfer failed!"}
```
- View logs: `gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=bq-transfer-dev" --project=sbox-rgodoy-002-20251008 --limit=50`
- Check if destination dataset exists
- Verify source and destination regions match

**Deployment Failures:**
```
Cloud Build failed
```
- Check Docker image build logs in Cloud Console
- Verify `main.py`, `Dockerfile`, and `requirements.txt` exist
- Ensure Cloud Build API is enabled

**Service Not Found:**
```
Could not get service URL
```
- Service may still be deploying (wait 1-2 minutes)
- Check if service exists: `gcloud run services list --project=sbox-rgodoy-002-20251008`
- Verify you're in the correct project
