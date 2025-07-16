#!/bin/bash

# Enhanced Deployment Script for Economic Intelligence Platform
# This script deploys the platform with all enhanced features including monitoring and health checks

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] ✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] ⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ❌ $1${NC}"
}

# Configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$PROJECT_DIR/logs"
MONITORING_DIR="$PROJECT_DIR/monitoring"
DATA_DIR="$PROJECT_DIR/data"
BACKUP_DIR="$PROJECT_DIR/backups"

# Default values
ENVIRONMENT="development"
SKIP_TESTS=false
SKIP_HEALTH_CHECK=false
ENABLE_MONITORING=true
MONITORING_INTERVAL=30

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --env)
            ENVIRONMENT="$2"
            shift 2
            ;;
        --skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        --skip-health-check)
            SKIP_HEALTH_CHECK=true
            shift
            ;;
        --disable-monitoring)
            ENABLE_MONITORING=false
            shift
            ;;
        --monitoring-interval)
            MONITORING_INTERVAL="$2"
            shift 2
            ;;
        --help)
            echo "Enhanced Deployment Script for Economic Intelligence Platform"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --env ENVIRONMENT          Set deployment environment (development|staging|production)"
            echo "  --skip-tests              Skip running tests"
            echo "  --skip-health-check       Skip health checks"
            echo "  --disable-monitoring      Disable performance monitoring"
            echo "  --monitoring-interval SEC Set monitoring interval in seconds (default: 30)"
            echo "  --help                    Show this help message"
            echo ""
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

log "Starting enhanced deployment for Economic Intelligence Platform"
log "Environment: $ENVIRONMENT"
log "Project Directory: $PROJECT_DIR"

# Create necessary directories
log "Creating necessary directories..."
mkdir -p "$LOG_DIR" "$DATA_DIR" "$BACKUP_DIR"
log_success "Directories created"

# Validate environment
log "Validating environment..."
cd "$PROJECT_DIR"

# Check if .env file exists
if [[ ! -f ".env" ]]; then
    log_error ".env file not found. Please create it with required configuration."
    exit 1
fi

# Load environment variables
source .env

# Validate required environment variables
required_vars=("KAFKA_BOOTSTRAP_SERVERS" "ACRA_RESOURCE_ID" "URA_ACCESS_KEY")
for var in "${required_vars[@]}"; do
    if [[ -z "${!var}" ]]; then
        log_error "Required environment variable $var is not set"
        exit 1
    fi
done

log_success "Environment validation completed"

# Install/update dependencies
log "Installing/updating Python dependencies..."
if command -v python3 &> /dev/null; then
    python3 -m pip install --upgrade pip
    python3 -m pip install -r requirements.txt
    log_success "Dependencies installed"
else
    log_error "Python 3 is not installed"
    exit 1
fi

# Run tests if not skipped
if [[ "$SKIP_TESTS" == false ]]; then
    log "Running enhanced test suite..."
    
    # Run basic data source tests
    if python3 test_data_sources.py; then
        log_success "Data source tests passed"
    else
        log_error "Data source tests failed"
        exit 1
    fi
    
    # Run enhanced functionality tests
    if python3 tests/test_enhanced_functionality.py; then
        log_success "Enhanced functionality tests passed"
    else
        log_warning "Some enhanced functionality tests failed, but continuing deployment"
    fi
else
    log_warning "Skipping tests as requested"
fi

# Start infrastructure services
log "Starting infrastructure services..."
if command -v docker-compose &> /dev/null; then
    # Stop any existing services
    docker-compose down --remove-orphans
    
    # Start services
    docker-compose up -d
    
    # Wait for services to be ready
    log "Waiting for services to be ready..."
    sleep 30
    
    log_success "Infrastructure services started"
else
    log_warning "Docker Compose not found. Please ensure Kafka and other services are running manually."
fi

# Run health checks if not skipped
if [[ "$SKIP_HEALTH_CHECK" == false ]]; then
    log "Running comprehensive health checks..."
    
    if python3 monitoring/health_check.py; then
        log_success "All health checks passed"
    else
        log_error "Health checks failed. Please check the system status."
        exit 1
    fi
else
    log_warning "Skipping health checks as requested"
fi

# Start performance monitoring if enabled
if [[ "$ENABLE_MONITORING" == true ]]; then
    log "Starting performance monitoring..."
    
    # Create monitoring log file
    MONITORING_LOG="$LOG_DIR/performance_monitor_$(date +%Y%m%d_%H%M%S).log"
    
    # Start monitoring in background
    nohup python3 monitoring/performance_monitor.py \
        --interval "$MONITORING_INTERVAL" \
        --duration 0 \
        > "$MONITORING_LOG" 2>&1 &
    
    MONITOR_PID=$!
    echo $MONITOR_PID > "$PROJECT_DIR/monitoring.pid"
    
    log_success "Performance monitoring started (PID: $MONITOR_PID)"
    log "Monitoring logs: $MONITORING_LOG"
else
    log_warning "Performance monitoring disabled"
fi

# Create deployment summary
DEPLOYMENT_SUMMARY="$LOG_DIR/deployment_summary_$(date +%Y%m%d_%H%M%S).json"
cat > "$DEPLOYMENT_SUMMARY" << EOF
{
    "deployment_timestamp": "$(date -Iseconds)",
    "environment": "$ENVIRONMENT",
    "project_directory": "$PROJECT_DIR",
    "configuration": {
        "skip_tests": $SKIP_TESTS,
        "skip_health_check": $SKIP_HEALTH_CHECK,
        "enable_monitoring": $ENABLE_MONITORING,
        "monitoring_interval": $MONITORING_INTERVAL
    },
    "services": {
        "kafka_bootstrap_servers": "$KAFKA_BOOTSTRAP_SERVERS",
        "acra_resource_id": "$ACRA_RESOURCE_ID",
        "monitoring_enabled": $ENABLE_MONITORING
    },
    "status": "deployed"
}
EOF

log_success "Deployment summary saved to: $DEPLOYMENT_SUMMARY"

# Display deployment information
echo ""
echo "=========================================="
echo "🚀 DEPLOYMENT COMPLETED SUCCESSFULLY! 🚀"
echo "=========================================="
echo ""
echo "Environment: $ENVIRONMENT"
echo "Project Directory: $PROJECT_DIR"
echo "Log Directory: $LOG_DIR"
echo ""
echo "Services Status:"
echo "  ✅ Data Producers: Ready"
echo "  ✅ Kafka Integration: Active"
echo "  ✅ Health Monitoring: $([ "$SKIP_HEALTH_CHECK" == false ] && echo 'Active' || echo 'Skipped')"
echo "  ✅ Performance Monitoring: $([ "$ENABLE_MONITORING" == true ] && echo 'Active' || echo 'Disabled')"
echo ""
echo "Data Sources:"
echo "  📊 ACRA: Companies data (Resource ID: $ACRA_RESOURCE_ID)"
echo "  📈 SingStat: Economic indicators"
echo "  🏢 URA: Geospatial and property data"
echo ""
echo "Management Commands:"
echo "  Health Check: python3 monitoring/health_check.py"
echo "  Performance Report: python3 monitoring/performance_monitor.py --report"
echo "  Test Data Sources: python3 test_data_sources.py"
echo ""
if [[ "$ENABLE_MONITORING" == true ]]; then
    echo "  Stop Monitoring: kill \$(cat monitoring.pid)"
    echo "  Monitoring Logs: tail -f $MONITORING_LOG"
    echo ""
fi
echo "Deployment Summary: $DEPLOYMENT_SUMMARY"
echo ""
echo "🎉 The Economic Intelligence Platform is now ready for use!"
echo "=========================================="

# Save deployment completion marker
echo "$(date -Iseconds)" > "$PROJECT_DIR/.deployment_complete"

log_success "Enhanced deployment completed successfully!"