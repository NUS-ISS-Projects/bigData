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
