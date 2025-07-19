#!/bin/bash

# Economic Intelligence Platform - Complete Setup and Deployment Script (API Version)
# This script builds all Docker images and deploys the complete platform using MinIO API

set -e

echo "🚀 Starting Economic Intelligence Platform Complete Setup (API Version)..."

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="economic-observatory"
KUBECTL_TIMEOUT="300s"
IMAGE_TAG="latest"

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
print_status "Checking prerequisites..."

if ! command_exists kubectl; then
    print_error "kubectl is not installed. Please install kubectl first."
    exit 1
fi

if ! command_exists docker; then
    print_error "Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command_exists minikube; then
    print_error "Minikube is not installed. Please install Minikube first."
    exit 1
fi

if ! command_exists python3; then
    print_error "Python 3 is not installed. Please install Python 3 first."
    exit 1
fi

# Check if Minikube is running and start if necessary
print_status "Checking Minikube status..."
if ! minikube status >/dev/null 2>&1; then
    print_warning "Minikube is not running. Starting Minikube..."
    minikube start
    if [ $? -eq 0 ]; then
        print_success "Minikube started successfully"
    else
        print_error "Failed to start Minikube. Please check your system configuration."
        exit 1
    fi
else
    print_success "Minikube is already running"
fi

print_success "Prerequisites check completed"

# Install Python dependencies for MinIO API
print_status "Installing Python dependencies..."
python3 -m pip install --quiet minio requests urllib3
print_success "Python dependencies installed"

# Configure Docker to use Minikube's Docker daemon
print_status "Configuring Docker environment for Minikube..."
eval $(minikube docker-env)
print_success "Docker environment configured for Minikube"

# Build Docker images
print_status "Building Docker images..."

# Build data producers image
print_status "Building data-producers image..."
cd producers
docker build -t economic-observatory/data-producers:${IMAGE_TAG} .
if [ $? -eq 0 ]; then
    print_success "data-producers image built successfully"
else
    print_error "Failed to build data-producers image"
    exit 1
fi
cd ..

# Build Spark streaming image
print_status "Building spark-streaming image..."
cd spark
docker build -t economic-observatory/spark-streaming:${IMAGE_TAG} .
if [ $? -eq 0 ]; then
    print_success "spark-streaming image built successfully"
else
    print_error "Failed to build spark-streaming image"
    exit 1
fi
cd ..

# Verify images are available
print_status "Verifying built images..."
docker images | grep economic-observatory

print_success "All Docker images built successfully!"

# Create namespace if it doesn't exist
print_status "Creating namespace $NAMESPACE..."
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
print_success "Namespace $NAMESPACE created/verified"

# Function to wait for deployment
wait_for_deployment() {
    local deployment_name=$1
    local namespace=$2
    
    print_status "Waiting for deployment $deployment_name to be ready..."
    kubectl wait --for=condition=available --timeout=$KUBECTL_TIMEOUT deployment/$deployment_name -n $namespace
    
    if [ $? -eq 0 ]; then
        print_success "Deployment $deployment_name is ready"
        # Additional verification - check if pods are actually running
        local running_pods=$(kubectl get pods -n $namespace -l app=$deployment_name --field-selector=status.phase=Running --no-headers | wc -l)
        if [ "$running_pods" -gt 0 ]; then
            print_success "Deployment $deployment_name has $running_pods running pod(s)"
        else
            print_warning "Deployment $deployment_name is available but no running pods found"
            kubectl get pods -n $namespace -l app=$deployment_name
        fi
    else
        print_error "Deployment $deployment_name failed to become ready"
        print_status "Checking deployment status..."
        kubectl describe deployment/$deployment_name -n $namespace
        print_status "Checking pod status..."
        kubectl get pods -n $namespace -l app=$deployment_name
        return 1
    fi
}

# Function to verify ConfigMap content
verify_configmap() {
    local configmap_name=$1
    local namespace=$2
    local expected_files=$3
    
    print_status "Verifying ConfigMap $configmap_name..."
    
    # Check if ConfigMap exists
    if ! kubectl get configmap $configmap_name -n $namespace >/dev/null 2>&1; then
        print_error "ConfigMap $configmap_name does not exist"
        return 1
    fi
    
    # Get raw data for debugging
    local raw_data=$(kubectl get configmap $configmap_name -n $namespace -o jsonpath='{.data}' 2>/dev/null || echo "")
    print_status "Raw ConfigMap data: $raw_data"
    
    # Check if ConfigMap has data
    local data_keys=$(echo "$raw_data" | grep -o '"[^"]*":' | sed 's/[":]*//g' | tr '\n' ' ' | xargs)
    
    if [ -z "$data_keys" ] || [ "$raw_data" = "{}" ] || [ "$raw_data" = "null" ]; then
        print_error "ConfigMap $configmap_name is empty or has no data"
        print_error "Describing ConfigMap for debugging:"
        kubectl describe configmap $configmap_name -n $namespace
        return 1
    fi
    
    print_success "ConfigMap $configmap_name contains: $data_keys"
    
    # Verify expected files if provided
    if [ ! -z "$expected_files" ]; then
        for file in $expected_files; do
            if echo "$data_keys" | grep -q "$file"; then
                print_success "✓ Found expected file: $file"
            else
                print_error "✗ Missing expected file: $file"
                print_error "Expected: $expected_files"
                print_error "Found: $data_keys"
                return 1
            fi
        done
    fi
    
    return 0
}

# Function to cleanup and retry ConfigMap creation
retry_configmap_creation() {
    local configmap_name=$1
    local namespace=$2
    local create_command=$3
    local max_retries=3
    local retry_count=0
    
    while [ $retry_count -lt $max_retries ]; do
        print_status "Attempt $((retry_count + 1)) to create ConfigMap $configmap_name..."
        
        # Delete existing ConfigMap if it exists
        kubectl delete configmap $configmap_name -n $namespace >/dev/null 2>&1 || true
        
        # Wait a moment for deletion to complete
        sleep 3
        
        # Execute the creation command
        print_status "Executing: $create_command"
        eval "$create_command"
        local exit_code=$?
        
        if [ $exit_code -eq 0 ]; then
            print_success "ConfigMap $configmap_name created successfully"
            # Verify it was actually created with content
            sleep 2
            local data_check=$(kubectl get configmap $configmap_name -n $namespace -o jsonpath='{.data}' 2>/dev/null || echo "")
            if [ -n "$data_check" ] && [ "$data_check" != "{}" ]; then
                print_success "ConfigMap $configmap_name verified with content"
                return 0
            else
                print_warning "ConfigMap $configmap_name created but appears empty, retrying..."
            fi
        else
            print_warning "Failed to create ConfigMap $configmap_name (attempt $((retry_count + 1)), exit code: $exit_code)"
        fi
        
        retry_count=$((retry_count + 1))
        if [ $retry_count -lt $max_retries ]; then
            sleep 5
        fi
    done
    
    print_error "Failed to create ConfigMap $configmap_name after $max_retries attempts"
    return 1
}

# Deploy core infrastructure (Kafka, MinIO)
print_status "Deploying core infrastructure..."

print_status "Deploying namespace configuration..."
kubectl apply -f k8s/namespace.yaml

print_status "Deploying MinIO storage..."
kubectl apply -f k8s/minio.yaml
wait_for_deployment "minio" $NAMESPACE

print_status "Deploying Kafka cluster..."
kubectl apply -f k8s/kafka.yaml
wait_for_deployment "kafka" $NAMESPACE

# Wait for Kafka to be fully ready
print_status "Waiting for Kafka to be fully operational..."
sleep 30

# Initialize MinIO buckets using API
print_status "Initializing MinIO buckets using API..."

# Get MinIO service endpoint
MINIO_NODEPORT=$(kubectl get service minio-nodeport -n $NAMESPACE -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo "")
MINIO_IP=$(minikube ip)

if [ ! -z "$MINIO_NODEPORT" ] && [ ! -z "$MINIO_IP" ]; then
    MINIO_ENDPOINT="$MINIO_IP:$MINIO_NODEPORT"
    print_status "Using MinIO endpoint: $MINIO_ENDPOINT"
else
    # Fallback to port-forward
    print_status "Setting up port-forward for MinIO API access..."
    kubectl port-forward -n $NAMESPACE svc/minio-service 9000:9000 &
    PORT_FORWARD_PID=$!
    sleep 5
    MINIO_ENDPOINT="localhost:9000"
fi

# Run MinIO bucket initialization
print_status "Creating MinIO buckets via API..."
python3 scripts/init_minio_buckets.py \
    --endpoint "$MINIO_ENDPOINT" \
    --access-key "admin" \
    --secret-key "password123" \
    --max-retries 20 \
    --retry-interval 15

if [ $? -eq 0 ]; then
    print_success "MinIO buckets created successfully via API"
else
    print_error "Failed to create MinIO buckets via API"
    # Kill port-forward if it was started
    if [ ! -z "$PORT_FORWARD_PID" ]; then
        kill $PORT_FORWARD_PID 2>/dev/null || true
    fi
    exit 1
fi

# Kill port-forward if it was started
if [ ! -z "$PORT_FORWARD_PID" ]; then
    kill $PORT_FORWARD_PID 2>/dev/null || true
fi

# Deploy data producers
print_status "Deploying data producers..."
kubectl apply -f k8s/producers.yaml
wait_for_deployment "data-producers" $NAMESPACE

print_success "Core infrastructure and data producers deployed successfully"

# Deploy Spark streaming infrastructure
print_status "Deploying Spark streaming infrastructure..."

# Verify Spark files exist before creating ConfigMaps
print_status "Verifying Spark application files..."
if [ ! -f "spark/spark_streaming_consumer.py" ]; then
    print_error "spark/spark_streaming_consumer.py not found!"
    exit 1
fi

if [ ! -f "spark/etl_bronze_to_silver.py" ]; then
    print_error "spark/etl_bronze_to_silver.py not found!"
    exit 1
fi

if [ ! -f "spark/requirements.txt" ]; then
    print_error "spark/requirements.txt not found!"
    exit 1
fi

print_success "All Spark application files verified"

# Create ConfigMap for Spark streaming consumer with retry logic
print_status "Creating spark-streaming-code ConfigMap..."
STREAMING_CMD="kubectl create configmap spark-streaming-code --from-file=spark/spark_streaming_consumer.py --from-file=spark/requirements.txt -n $NAMESPACE"
if retry_configmap_creation "spark-streaming-code" "$NAMESPACE" "$STREAMING_CMD"; then
    verify_configmap "spark-streaming-code" "$NAMESPACE" "spark_streaming_consumer.py requirements.txt"
    if [ $? -ne 0 ]; then
        print_error "spark-streaming-code ConfigMap verification failed"
        exit 1
    fi
else
    print_error "Failed to create spark-streaming-code ConfigMap after retries"
    exit 1
fi

# Create ConfigMap for Spark app code (streaming consumer deployment) with retry logic
print_status "Creating spark-app-code ConfigMap..."
APP_CODE_CMD="kubectl create configmap spark-app-code --from-file=spark_streaming_consumer.py=spark/spark_streaming_consumer.py -n $NAMESPACE"
if retry_configmap_creation "spark-app-code" "$NAMESPACE" "$APP_CODE_CMD"; then
    verify_configmap "spark-app-code" "$NAMESPACE" "spark_streaming_consumer.py"
    if [ $? -ne 0 ]; then
        print_error "spark-app-code ConfigMap verification failed"
        exit 1
    fi
else
    print_error "Failed to create spark-app-code ConfigMap after retries"
    exit 1
fi

# Create ConfigMap for ETL job with retry logic
print_status "Creating spark-etl-code ConfigMap..."
ETL_CODE_CMD="kubectl create configmap spark-etl-code --from-file=etl_bronze_to_silver.py=spark/etl_bronze_to_silver.py --from-file=requirements.txt=spark/requirements.txt -n $NAMESPACE"
if retry_configmap_creation "spark-etl-code" "$NAMESPACE" "$ETL_CODE_CMD"; then
    verify_configmap "spark-etl-code" "$NAMESPACE" "etl_bronze_to_silver.py requirements.txt"
    if [ $? -ne 0 ]; then
        print_error "spark-etl-code ConfigMap verification failed"
        exit 1
    fi
else
    print_error "Failed to create spark-etl-code ConfigMap after retries"
    exit 1
fi

# Wait a moment for ConfigMaps to be fully propagated
print_status "Waiting for ConfigMaps to propagate..."
sleep 5

# Final validation of all ConfigMaps
print_status "Performing final validation of all ConfigMaps..."
CONFIGMAPS_TO_VALIDATE=(
    "spark-streaming-code:spark_streaming_consumer.py requirements.txt"
    "spark-app-code:spark_streaming_consumer.py"
    "spark-etl-code:etl_bronze_to_silver.py requirements.txt"
)

for configmap_info in "${CONFIGMAPS_TO_VALIDATE[@]}"; do
    IFS=':' read -r configmap_name expected_files <<< "$configmap_info"
    print_status "Validating $configmap_name..."
    verify_configmap "$configmap_name" "$NAMESPACE" "$expected_files"
    if [ $? -ne 0 ]; then
        print_error "Final validation failed for $configmap_name"
        print_error "Listing all ConfigMaps for debugging:"
        kubectl get configmaps -n $NAMESPACE
        print_error "Describing problematic ConfigMap:"
        kubectl describe configmap "$configmap_name" -n $NAMESPACE
        exit 1
    fi
done
print_success "All ConfigMaps validated successfully"

# Deploy Spark streaming jobs
print_status "Deploying Spark streaming YAML configuration..."
kubectl apply -f k8s/spark-streaming.yaml

if [ $? -eq 0 ]; then
    print_success "Spark streaming configuration applied"
    
    # Wait for spark-streaming-consumer deployment to be ready
    print_status "Waiting for spark-streaming-consumer deployment..."
    wait_for_deployment "spark-streaming-consumer" "$NAMESPACE"
    
    # Verify the streaming consumer can find its Python file
    print_status "Verifying Spark streaming consumer startup..."
    sleep 10  # Allow time for container to start
    
    CONSUMER_POD=$(kubectl get pods -n $NAMESPACE -l app=spark-streaming-consumer --no-headers -o custom-columns=":metadata.name" | head -1)
    if [ -n "$CONSUMER_POD" ]; then
        print_status "Checking logs for $CONSUMER_POD..."
        # Check for the specific error we've been encountering
        ERROR_CHECK=$(kubectl logs "$CONSUMER_POD" -n $NAMESPACE 2>/dev/null | grep "can't open file '/app/spark_streaming_consumer.py'" || true)
        if [ -n "$ERROR_CHECK" ]; then
            print_error "Spark streaming consumer still cannot find Python file!"
            print_error "Pod logs:"
            kubectl logs "$CONSUMER_POD" -n $NAMESPACE --tail=20
            print_error "ConfigMap contents:"
            kubectl describe configmap spark-app-code -n $NAMESPACE
            exit 1
        else
            print_success "Spark streaming consumer appears to be starting correctly"
        fi
    else
        print_warning "No spark-streaming-consumer pod found yet"
    fi
else
    print_error "Failed to apply Spark streaming configuration"
    exit 1
fi

print_success "Spark streaming infrastructure deployed successfully"

# Deploy dbt analytics infrastructure
print_status "Deploying dbt analytics infrastructure..."

# Create ConfigMaps for dbt project
print_status "Creating dbt project ConfigMaps..."

# Create dbt models ConfigMap
kubectl create configmap dbt-models \
    --from-file=dbt/models/ \
    -n $NAMESPACE \
    --dry-run=client -o yaml | kubectl apply -f -

# Create dbt project files ConfigMap
kubectl create configmap dbt-project-config \
    --from-file=dbt/dbt_project.yml \
    --from-file=dbt/requirements.txt \
    -n $NAMESPACE \
    --dry-run=client -o yaml | kubectl apply -f -

# Deploy dbt analytics jobs
kubectl apply -f k8s/dbt-analytics.yaml
wait_for_deployment "dbt-docs-server" $NAMESPACE

print_success "dbt analytics infrastructure deployed successfully"

# Verify MinIO buckets one more time
print_status "Verifying MinIO bucket setup..."
MINIO_NODEPORT=$(kubectl get service minio-nodeport -n $NAMESPACE -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo "")
if [ ! -z "$MINIO_NODEPORT" ]; then
    MINIO_ENDPOINT="$(minikube ip):$MINIO_NODEPORT"
    python3 -c "
import sys
sys.path.append('scripts')
from init_minio_buckets import MinIOBucketInitializer
initializer = MinIOBucketInitializer(endpoint='$MINIO_ENDPOINT', access_key='admin', secret_key='password123')
initializer._create_client()
initializer.list_buckets()
initializer.verify_setup()
" || print_warning "Could not verify buckets via API"
fi

# Display service endpoints
print_status "Getting service endpoints..."
echo ""
echo "=== SERVICE ENDPOINTS ==="
kubectl get services -n $NAMESPACE

echo ""
echo "=== POD STATUS ==="
kubectl get pods -n $NAMESPACE

echo ""
echo "=== DOCKER IMAGES ==="
docker images | grep economic-observatory

# Create enhanced access script with API verification
print_status "Creating enhanced dashboard access script..."
cat > access_dashboards_api.sh << 'EOF'
#!/bin/bash

# Economic Intelligence Platform - Enhanced Dashboard Access Script with API Verification

echo "🚀 Starting enhanced dashboard access with API verification..."

NAMESPACE="economic-observatory"

# Function to check if port is in use
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo "Port $port is already in use"
        return 1
    fi
    return 0
}

# Function to verify MinIO API
verify_minio_api() {
    echo "🔍 Verifying MinIO API access..."
    python3 -c "
import sys, os
sys.path.append('scripts')
try:
    from init_minio_buckets import MinIOBucketInitializer
    initializer = MinIOBucketInitializer(endpoint='localhost:9001', access_key='admin', secret_key='password123')
    if initializer._create_client():
        print('✅ MinIO API accessible')
        initializer.list_buckets()
    else:
        print('❌ MinIO API not accessible')
except Exception as e:
    print(f'⚠️  MinIO API verification failed: {e}')
"
}

# Kill existing port forwards
echo "Stopping existing port forwards..."
pkill -f "kubectl port-forward" || true
sleep 2

# Start MinIO Console
if check_port 9001; then
    echo "Starting MinIO Console on port 9001..."
    kubectl port-forward -n $NAMESPACE svc/minio-service 9001:9001 &
    MINIO_PID=$!
fi

# Start MinIO API (for verification)
if check_port 9000; then
    echo "Starting MinIO API on port 9000..."
    kubectl port-forward -n $NAMESPACE svc/minio-service 9000:9000 &
    MINIO_API_PID=$!
fi

# Start Spark UI
if check_port 4040; then
    echo "Starting Spark UI on port 4040..."
    kubectl port-forward -n $NAMESPACE svc/spark-streaming-service 4040:4040 &
    SPARK_PID=$!
fi

# Start dbt Docs
if check_port 8080; then
    echo "Starting dbt Docs on port 8080..."
    kubectl port-forward -n $NAMESPACE svc/dbt-docs-service 8080:80 &
    DBT_PID=$!
fi

sleep 5

# Verify MinIO API
verify_minio_api

echo ""
echo "=== ENHANCED DASHBOARD ACCESS ==="
echo "📊 MinIO Console: http://localhost:9001"
echo "   Username: admin"
echo "   Password: password123"
echo "🔧 MinIO API: http://localhost:9000 (for programmatic access)"
echo "⚡ Spark UI: http://localhost:4040"
echo "📈 dbt Docs: http://localhost:8080"
echo ""
echo "🔍 API Features:"
echo "   - Bucket management via Python SDK"
echo "   - Programmatic data access"
echo "   - Automated bucket verification"
echo ""
echo "Press Ctrl+C to stop all port forwards"

# Wait for interrupt
trap 'echo "\nStopping port forwards..."; kill $MINIO_PID $MINIO_API_PID $SPARK_PID $DBT_PID 2>/dev/null; exit 0' INT
wait
EOF

chmod +x access_dashboards_api.sh
print_success "Enhanced dashboard access script created: ./access_dashboards_api.sh"

# Create bucket management utility
print_status "Creating bucket management utility..."
cat > manage_buckets.py << 'EOF'
#!/usr/bin/env python3
"""
MinIO Bucket Management Utility
Provides easy bucket management for the Economic Intelligence Platform
"""

import sys
import os
sys.path.append('scripts')

from init_minio_buckets import MinIOBucketInitializer
import argparse

def main():
    parser = argparse.ArgumentParser(description='Manage MinIO buckets')
    parser.add_argument('action', choices=['list', 'create', 'verify', 'recreate'], 
                       help='Action to perform')
    parser.add_argument('--endpoint', default='localhost:9000', help='MinIO endpoint')
    parser.add_argument('--access-key', default='admin', help='Access key')
    parser.add_argument('--secret-key', default='password123', help='Secret key')
    
    args = parser.parse_args()
    
    initializer = MinIOBucketInitializer(
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key
    )
    
    if not initializer._create_client():
        print("❌ Failed to connect to MinIO")
        sys.exit(1)
    
    if args.action == 'list':
        initializer.list_buckets()
    elif args.action == 'create':
        initializer.create_all_buckets()
    elif args.action == 'verify':
        if initializer.verify_setup():
            print("✅ All buckets verified")
        else:
            print("❌ Bucket verification failed")
            sys.exit(1)
    elif args.action == 'recreate':
        print("🔄 Recreating all buckets...")
        initializer.create_all_buckets()
        initializer.verify_setup()

if __name__ == '__main__':
    main()
EOF

chmod +x manage_buckets.py
print_success "Bucket management utility created: ./manage_buckets.py"

# Final status
print_success "🎉 Economic Intelligence Platform setup completed successfully with API integration!"

echo ""
echo "=== SETUP COMPLETION SUMMARY (API VERSION) ==="
echo "✅ Docker Images: Built and available in Minikube"
echo "✅ MinIO Buckets: Created via API (not CLI)"
echo "✅ Data Lakehouse Architecture: Bronze, Silver, Gold layers implemented"
echo "✅ Real-time Data Ingestion: Kafka → Spark Streaming → Delta Lake"
echo "✅ ETL Pipeline: Bronze → Silver data transformation"
echo "✅ Analytics Engineering: dbt models for business intelligence"
echo "✅ Data Sources: ACRA, SingStat, URA integrated"
echo "✅ Storage: MinIO with Delta Lake format"
echo "✅ Processing: Apache Spark for streaming and batch"
echo "✅ Orchestration: Kubernetes with automated scheduling"
echo "✅ API Integration: MinIO Python SDK for bucket management"
echo "✅ Monitoring: Health checks and performance monitoring"
echo ""
echo "🚀 Platform is ready for use with enhanced API capabilities!"
echo ""
echo "Next steps:"
echo "1. Run './access_dashboards_api.sh' to access all dashboards with API verification"
echo "2. Use './manage_buckets.py list' to list buckets via API"
echo "3. Use './manage_buckets.py verify' to verify bucket setup"
echo "4. Monitor data ingestion and processing"
echo "5. Explore analytics in dbt docs"
echo "6. Check data quality in MinIO buckets"

print_success "Setup script completed successfully with API integration!"