# Economic Intelligence Platform - Phase A Makefile
# Convenient commands for managing the infrastructure

.PHONY: help setup start stop restart status logs clean test build deploy-k8s

# Default target
help:
	@echo "Economic Intelligence Platform - Phase A Management"
	@echo ""
	@echo "Available commands:"
	@echo "  setup          - Initial setup of Phase A infrastructure"
	@echo "  start          - Start all services"
	@echo "  stop           - Stop all services"
	@echo "  restart        - Restart all services"
	@echo "  status         - Show status of all services"
	@echo "  logs           - Show logs from all services"
	@echo "  logs-follow    - Follow logs from all services"
	@echo "  logs-producers - Show logs from data producers only"
	@echo "  test           - Run Phase A validation tests"
	@echo "  clean          - Clean up all resources"
	@echo "  build          - Build Docker images"
	@echo "  deploy-k8s     - Deploy to Kubernetes"
	@echo "  topics         - List Kafka topics"
	@echo "  buckets        - List MinIO buckets"
	@echo "  health         - Check health of all services"
	@echo "  manual-extract - Run manual data extraction"
	@echo ""
	@echo "Quick start: make setup && make test"

# Setup Phase A infrastructure
setup:
	@echo "Setting up Phase A infrastructure..."
	./setup-phase-a.sh

# Start services
start:
	@echo "Starting services..."
	docker-compose up -d
	@echo "Waiting for services to be ready..."
	sleep 30
	@echo "Services started successfully!"

# Stop services
stop:
	@echo "Stopping services..."
	docker-compose down

# Restart services
restart: stop start

# Show service status
status:
	@echo "Service Status:"
	@echo "==============="
	docker-compose ps
	@echo ""
	@echo "Docker Images:"
	@echo "==============="
	docker images | grep -E "(economic-observatory|confluent|minio)"

# Show logs
logs:
	@echo "Recent logs from all services:"
	@echo "=============================="
	docker-compose logs --tail=50

# Follow logs
logs-follow:
	@echo "Following logs from all services (Ctrl+C to stop):"
	@echo "=================================================="
	docker-compose logs -f

# Show producer logs only
logs-producers:
	@echo "Data Producers Logs:"
	@echo "==================="
	docker-compose logs data-producers --tail=100

# Run tests
test:
	@echo "Running Phase A validation tests..."
	./test-phase-a.sh

# Clean up everything
clean:
	@echo "Cleaning up all resources..."
	docker-compose down --volumes --remove-orphans
	docker system prune -f
	@echo "Cleanup completed!"

# Build Docker images
build:
	@echo "Building Docker images..."
	cd producers && docker build -t economic-observatory/data-producers:latest .
	@echo "Images built successfully!"

# Deploy to Kubernetes
deploy-k8s:
	@echo "Deploying to Kubernetes..."
	./setup-phase-a.sh --kubernetes

# List Kafka topics
topics:
	@echo "Kafka Topics:"
	@echo "============="
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# List MinIO buckets
buckets:
	@echo "MinIO Buckets:"
	@echo "=============="
	docker exec minio mc ls minio/

# Health check
health:
	@echo "Health Check Results:"
	@echo "===================="
	@echo -n "MinIO: "
	@curl -f -s http://localhost:9000/minio/health/live > /dev/null && echo "✓ Healthy" || echo "✗ Unhealthy"
	@echo -n "Kafka UI: "
	@curl -f -s http://localhost:8080 > /dev/null && echo "✓ Healthy" || echo "✗ Unhealthy"
	@echo -n "Kafka Broker: "
	@docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 && echo "✓ Healthy" || echo "✗ Unhealthy"

# Manual data extraction
manual-extract:
	@echo "Running manual data extraction..."
	@echo "Select producer to run:"
	@echo "1) ACRA Producer"
	@echo "2) SingStat Producer"
	@echo "3) URA Producer"
	@echo "4) All Producers"
	@read -p "Enter choice (1-4): " choice; \
	case $$choice in \
		1) docker exec data-producers python acra_producer.py ;; \
		2) docker exec data-producers python singstat_producer.py ;; \
		3) docker exec data-producers python ura_producer.py ;; \
		4) docker exec data-producers python -c "from scheduler import DataIngestionScheduler; s = DataIngestionScheduler(); s.run_all_extractions()" ;; \
		*) echo "Invalid choice" ;; \
	esac

# Development helpers
dev-setup:
	@echo "Setting up development environment..."
	cp .env.example .env
	@echo "Environment file created. Please review and modify .env as needed."

# Monitor resources
monitor:
	@echo "Resource Usage:"
	@echo "==============="
	docker stats --no-stream

# Backup data
backup:
	@echo "Creating backup..."
	mkdir -p backups/$(shell date +%Y%m%d_%H%M%S)
	docker exec minio mc mirror minio/bronze backups/$(shell date +%Y%m%d_%H%M%S)/bronze
	docker exec minio mc mirror minio/silver backups/$(shell date +%Y%m%d_%H%M%S)/silver
	docker exec minio mc mirror minio/gold backups/$(shell date +%Y%m%d_%H%M%S)/gold
	@echo "Backup completed in backups/$(shell date +%Y%m%d_%H%M%S)/"

# Show service URLs
urls:
	@echo "Service URLs:"
	@echo "============="
	@echo "MinIO Console: http://localhost:9001 (admin/password123)"
	@echo "MinIO API: http://localhost:9000"
	@echo "Kafka UI: http://localhost:8080"
	@echo "Kafka Bootstrap: localhost:9092"

# Quick troubleshooting
troubleshoot:
	@echo "Troubleshooting Information:"
	@echo "==========================="
	@echo "Docker version:"
	docker --version
	@echo ""
	@echo "Docker Compose version:"
	docker-compose --version
	@echo ""
	@echo "Available disk space:"
	df -h .
	@echo ""
	@echo "Available memory:"
	free -h
	@echo ""
	@echo "Service status:"
	make status

# Performance test
perf-test:
	@echo "Running performance tests..."
	@echo "Testing Kafka throughput..."
	docker exec kafka kafka-producer-perf-test --topic test-perf --num-records 1000 --record-size 1024 --throughput 100 --producer-props bootstrap.servers=localhost:9092
	docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic test-perf

# Show project structure
structure:
	@echo "Project Structure:"
	@echo "=================="
	tree -I '__pycache__|*.pyc|.git|out' -L 3

# Validate configuration
validate:
	@echo "Validating configuration..."
	@echo "Checking Docker Compose file..."
	docker-compose config > /dev/null && echo "✓ docker-compose.yml is valid" || echo "✗ docker-compose.yml has errors"
	@echo "Checking Kubernetes manifests..."
	@for file in k8s/*.yaml; do \
		kubectl --dry-run=client apply -f $$file > /dev/null 2>&1 && echo "✓ $$file is valid" || echo "✗ $$file has errors"; \
	done