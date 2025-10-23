#!/bin/bash

# BigQuery Dataset Transfer Service - Deploy and Run Script (DEV Environment)
# This script deploys the Cloud Run service and automatically triggers the transfer
#
# Usage:
#   ./deploy-dev.sh
#
# What this does:
#   1. Builds and deploys the Cloud Run service
#   2. Waits for deployment to complete
#   3. Automatically triggers the dataset transfer
#   4. Shows the results

echo "=========================================="
echo "BigQuery Transfer: Deploy & Run (DEV)"
echo "=========================================="
echo ""

# =============================================================================
# Configuration
# =============================================================================

# Environment identifier
ENVIRONMENT="dev"

# GCP Project Configuration
PROJECT_ID="sbox-rgodoy-002-20251008"
REGION="us-central1"

# Service Configuration
SERVICE_NAME="bq-transfer-dev"
SERVICE_ACCOUNT="dts-cloud-run-deploy-sa@${PROJECT_ID}.iam.gserviceaccount.com"

# Cloud Run Resource Allocation
MEMORY="4Gi"
CPU="2"
TIMEOUT="3600"
MIN_INSTANCES="0"
MAX_INSTANCES="10"

# Google Container Registry
IMAGE_NAME="gcr.io/${PROJECT_ID}/${SERVICE_NAME}"

# Application Settings
LOG_LEVEL="INFO"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# =============================================================================
# PHASE 1: DEPLOYMENT
# =============================================================================
echo -e "${BLUE}Phase 1: Deploying Cloud Run Service${NC}"
echo "========================================"
echo ""

# Pre-Deployment Checks
echo "Pre-Deployment Checks:"
if [ ! -f "main.py" ] || [ ! -f "Dockerfile" ]; then
    echo -e "${RED}  ✗ Required files not found${NC}"
    exit 1
fi
echo -e "${GREEN}  ✓ Required files found (main.py, Dockerfile)${NC}"
echo ""

# Set GCP Project
echo "Setting up GCP Project..."
gcloud config set project $PROJECT_ID > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo -e "${GREEN}  ✓ Project set: $PROJECT_ID${NC}"
else
    echo -e "${RED}  ✗ Failed to set project${NC}"
    exit 1
fi
echo ""

# Build and Push Docker Image
echo "Building Docker Image..."
echo -e "  Target: ${YELLOW}$IMAGE_NAME${NC}"
echo ""

gcloud builds submit --tag $IMAGE_NAME --region $REGION --project $PROJECT_ID --quiet

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}  ✓ Docker image built and pushed${NC}"
else
    echo ""
    echo -e "${RED}  ✗ Docker image build failed${NC}"
    exit 1
fi
echo ""

# Deploy to Cloud Run
echo "Deploying to Cloud Run..."
echo -e "  Service: ${YELLOW}$SERVICE_NAME${NC}"
echo -e "  Region: ${YELLOW}$REGION${NC}"
echo ""

gcloud run deploy $SERVICE_NAME \
  --image $IMAGE_NAME \
  --region $REGION \
  --platform managed \
  --memory $MEMORY \
  --cpu $CPU \
  --timeout ${TIMEOUT}s \
  --min-instances $MIN_INSTANCES \
  --max-instances $MAX_INSTANCES \
  --service-account $SERVICE_ACCOUNT \
  --set-env-vars ENVIRONMENT=$ENVIRONMENT,DEV_SOURCE_PROJECT=sbox-rgodoy-001-20251124,DEV_DEST_PROJECT=sbox-rgodoy-002-20251008,LOG_LEVEL=$LOG_LEVEL \
  --no-allow-unauthenticated \
  --project $PROJECT_ID \
  --quiet

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}  ✓ Cloud Run service deployed${NC}"
else
    echo ""
    echo -e "${RED}  ✗ Cloud Run deployment failed${NC}"
    exit 1
fi

# Get the service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region $REGION --project $PROJECT_ID --format="value(status.url)" 2>/dev/null)

if [ -z "$SERVICE_URL" ]; then
    echo -e "${RED}  ✗ Could not get service URL${NC}"
    exit 1
fi

echo -e "${GREEN}  ✓ Service URL: $SERVICE_URL${NC}"
echo ""

# =============================================================================
# PHASE 2: TRIGGER TRANSFER
# =============================================================================
echo -e "${BLUE}Phase 2: Triggering Dataset Transfer${NC}"
echo "========================================"
echo ""

# Wait for service to be fully ready
echo "Waiting for service to be ready..."
sleep 10
echo -e "${GREEN}  ✓ Service ready${NC}"
echo ""

# Trigger the transfer
echo "Executing transfer..."
echo -e "  POST ${YELLOW}$SERVICE_URL/transfer${NC}"
echo ""

TRANSFER_RESPONSE=$(curl -s -w "\n%{http_code}" \
    -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json" \
    -X POST \
    -d "{\"environment\": \"$ENVIRONMENT\"}" \
    "$SERVICE_URL/transfer")

HTTP_CODE=$(echo "$TRANSFER_RESPONSE" | tail -n 1)
RESPONSE_BODY=$(echo "$TRANSFER_RESPONSE" | sed '$d')

# =============================================================================
# RESULTS
# =============================================================================
echo ""
echo "=========================================="
if [ "$HTTP_CODE" == "200" ]; then
    echo -e "${GREEN}  ✓ TRANSFER COMPLETED SUCCESSFULLY${NC}"
    echo "=========================================="
    echo ""
    echo "Response:"
    echo "$RESPONSE_BODY" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE_BODY"
    echo ""
    
    # Show recent logs
    echo "Recent Transfer Logs:"
    echo "--------------------"
    gcloud logging read \
        "resource.type=cloud_run_revision AND resource.labels.service_name=$SERVICE_NAME AND severity>=INFO" \
        --project="$PROJECT_ID" \
        --limit=15 \
        --format="value(textPayload)" 2>/dev/null | grep -E "(Successfully|copied|redaction|INFO:|ERROR:)" | tail -10 || echo "Logs not available yet"
    
    echo ""
    echo -e "${GREEN}=========================================="
    echo -e "  SUCCESS!"
    echo -e "==========================================${NC}"
    echo ""
    echo "Summary:"
    echo "  • Environment: $ENVIRONMENT"
    echo "  • Service: $SERVICE_NAME"
    echo "  • Status: Transfer completed"
    echo ""
    
elif [ "$HTTP_CODE" == "500" ]; then
    echo -e "${RED}  ✗ TRANSFER FAILED${NC}"
    echo "=========================================="
    echo ""
    echo "Response:"
    echo "$RESPONSE_BODY" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE_BODY"
    echo ""
    
    # Show error logs
    echo "Error Logs:"
    echo "-----------"
    gcloud logging read \
        "resource.type=cloud_run_revision AND resource.labels.service_name=$SERVICE_NAME AND severity>=ERROR" \
        --project="$PROJECT_ID" \
        --limit=10 \
        --format="value(textPayload)" 2>/dev/null || echo "Logs not available yet"
    
    echo ""
    echo -e "${RED}=========================================="
    echo -e "  FAILED"
    echo -e "==========================================${NC}"
    exit 1
else
    echo -e "${RED}  ✗ TRANSFER FAILED (HTTP $HTTP_CODE)${NC}"
    echo "=========================================="
    echo ""
    echo "$RESPONSE_BODY"
    exit 1
fi
