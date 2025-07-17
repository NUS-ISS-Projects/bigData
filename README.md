# Economic Intelligence Platform

A streamlined data platform for economic intelligence and business analytics, built with modern data engineering practices.

## 🏗️ Architecture

The platform implements a modern data lakehouse architecture:

### Data Sources
- **ACRA**: Company registration and business data
- **SingStat**: Economic indicators and statistics  
- **URA**: Property and geospatial data

### Technology Stack
- **Orchestration**: Kubernetes
- **Streaming**: Apache Kafka
- **Processing**: Apache Spark with Delta Lake
- **Storage**: MinIO (S3-compatible)
- **Analytics**: dbt (Data Build Tool)
- **Containerization**: Docker

## 🚀 Quick Start

### Prerequisites
- Docker and Kubernetes (Minikube)
- Python 3.9+

### Setup

1. **Clone and Setup**
   ```bash
   git clone <repository-url>
   cd bigData_project
   cp .env.example .env
   ```

2. **Deploy Platform**
   ```bash
   ./setup_and_deploy_api.sh
   ```

3. **Access Dashboards**
   ```bash
   ./access_dashboards_api.sh
   ```

### Access Points
- **MinIO Console**: http://localhost:9001 (admin/password123)
- **MinIO API**: http://localhost:9000
- **Spark UI**: http://localhost:4040
- **dbt Docs**: http://localhost:8080

## 📊 Data Pipeline

### Bronze Layer (Raw Data)
- Direct ingestion from APIs via Kafka
- Minimal transformation
- Delta Lake format

### Silver Layer (Cleaned Data)
- Data quality validation
- Schema standardization
- ETL transformations

### Gold Layer (Business Data)
- Aggregated metrics
- Business KPIs
- Analytics-ready datasets

## 🔧 Project Structure

```
bigData_project/
├── producers/          # Data ingestion services
├── spark/             # Spark streaming and ETL
├── dbt/               # Analytics engineering
├── k8s/               # Kubernetes manifests
├── monitoring/        # Health checks
├── scripts/           # Utility scripts
├── models/            # Data models
└── requirements.txt   # Consolidated dependencies
```

## 🛠️ Management

### Bucket Management
```bash
# List buckets
python manage_buckets.py list

# Verify setup
python manage_buckets.py verify

# Create buckets
python manage_buckets.py create
```

### Monitoring
```bash
# Check health
python monitoring/health_check.py

# Performance monitoring
python monitoring/performance_monitor.py
```

## 📈 Key Features

- **API-First Design**: MinIO integration via Python SDK
- **Streamlined Deployment**: Single script setup
- **Consolidated Dependencies**: Unified requirements.txt
- **Health Monitoring**: Built-in health checks
- **Bucket Management**: Easy bucket operations
- **Clean Architecture**: Minimal, focused codebase

## 🔧 Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Initialize MinIO buckets
python scripts/init_minio_buckets.py --endpoint localhost:9000

# Deploy to Kubernetes
kubectl apply -f k8s/
```

## 📋 Components

### Data Producers
- ACRA company data producer
- SingStat economics data producer
- URA geospatial data producer
- Scheduled data ingestion

### Spark Processing
- Structured streaming consumer
- Bronze to Silver ETL
- Delta Lake integration

### dbt Analytics
- Staging models
- Business intelligence marts
- Economic analysis models
- Geospatial analysis

### Monitoring
- Health check utilities
- Performance monitoring
- System metrics

---

**Built for economic intelligence and data-driven decision making**