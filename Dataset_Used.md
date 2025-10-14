# Dataset Used - Economic Intelligence Platform

## 📊 **Data Sources Overview**

This document provides a comprehensive overview of all datasets used in the Economic Intelligence Platform, including their sources, characteristics, and integration methods.

---

## 🗂️ **Primary Datasets**

| **Dataset** | **Source** | **Type** | **Update Frequency** | **API Endpoint** | **Data Points** | **Purpose** |
|-------------|------------|----------|---------------------|------------------|-----------------|-------------|
| **Company Registration Data** | ACRA (Accounting & Corporate Regulatory Authority) | Business Registry | Real-time | data.gov.sg API | 500K+ entities | Business intelligence, company analysis |
| **Economic Indicators** | SingStat (Singapore Department of Statistics) | Economic Statistics | Quarterly/Annual | TableBuilder API | 2000+ indicators | Macroeconomic analysis, forecasting |
| **Property Market Data** | URA (Urban Redevelopment Authority) | Geospatial/Property | Monthly | URA DataService API | Property transactions | Real estate analytics, market trends |
| **Government Expenditure** | Data.gov.sg | Financial | Annual | data.gov.sg API | Budget allocations | Public finance analysis |

---

## 📈 **Detailed Dataset Specifications**

### **1. ACRA Company Data**
- **Resource ID**: `d_3f960c10fed6145404ca7b821f263b87`
- **Format**: JSON via REST API + CSV bulk data
- **Key Fields**: Company name, registration number, status, incorporation date, business activity
- **Volume**: 500,000+ registered entities
- **Integration**: Hybrid approach (API for incremental updates, CSV for historical bulk loading)
- **Kafka Topic**: `acra-companies`

### **2. SingStat Economic Indicators**
- **API Base**: `https://tablebuilder.singstat.gov.sg/api/table`
- **Categories**:
  - **GDP & National Accounts** (7 resources): M014811, M014812, M014871, M014911, M014921, M015721, M015731
  - **Trade & External Relations** (5 resources): Import/export statistics, balance of payments
  - **Labor & Employment** (4 resources): Employment rates, wage indices
  - **Price Indices** (3 resources): CPI, PPI, housing price indices
  - **Financial Markets** (3 resources): Interest rates, exchange rates, stock indices
- **Total Data Points**: 2000+ economic indicators
- **Kafka Topic**: `singstat-economics`

### **3. URA Property Data**
- **Service**: `PMI_Resi_Rental_Median` (Private Residential Rental Median)
- **Authentication**: Token-based API access
- **Coverage**: Singapore property market transactions and rentals
- **Geospatial**: Includes location coordinates and district information
- **Kafka Topic**: `ura-geospatial`

### **4. Government Expenditure Data**
- **Dataset ID**: `d_6a804a6860b5c51af08df679a71bc190`
- **Source**: Data.gov.sg official portal
- **Content**: Government budget allocations and expenditure by ministry/department
- **Format**: Structured financial data with categorical breakdowns
- **Kafka Topic**: `government-expenditure`

---

## 🔄 **Data Pipeline Architecture**

### **Bronze Layer (Raw Data)**
- **Storage**: Delta Lake format in MinIO
- **Retention**: Full historical data
- **Processing**: Minimal transformation, schema validation
- **Quality**: Raw API responses with metadata

### **Silver Layer (Cleaned Data)**
- **Transformations**: Data standardization, quality validation
- **Schema**: Unified data models using Pydantic
- **Deduplication**: Record-level deduplication
- **Enrichment**: Calculated fields and derived metrics

### **Gold Layer (Analytics-Ready)**
- **Aggregations**: Business KPIs and summary metrics
- **Models**: dbt-powered analytics models
- **Marts**: Subject-specific data marts for different use cases
- **Performance**: Optimized for analytical queries

---

## 📊 **Data Quality & Governance**

| **Aspect** | **Implementation** | **Tools** |
|------------|-------------------|-----------|
| **Validation** | Pydantic models with field validation | Python/Pydantic |
| **Quality Scoring** | Automated quality score calculation (0.0-1.0) | Custom algorithms |
| **Error Handling** | Comprehensive error logging and retry mechanisms | Loguru, retrying |
| **Monitoring** | Real-time data pipeline health checks | Custom monitoring |
| **Lineage** | Source tracking through standardized DataRecord model | Metadata management |

---

## 🔗 **API Integration Details**

### **Authentication Methods**
- **ACRA/SingStat**: Public APIs via data.gov.sg (no authentication required)
- **URA**: Token-based authentication with access key
- **Government Data**: Public access via data.gov.sg portal

### **Rate Limiting & Batch Processing**
- **Batch Size**: 100-1000 records per API call
- **Retry Logic**: Exponential backoff with 3 retry attempts
- **Pagination**: Offset-based pagination for large datasets
- **Throttling**: Configurable delays between API calls

---

## 📋 **Data Usage & Applications**

### **Business Intelligence**
- Company registration trends and business activity analysis
- Economic indicator correlation and forecasting
- Property market trend analysis and valuation models

### **Machine Learning**
- Economic forecasting models using historical indicators
- Anomaly detection in business registration patterns
- Property price prediction using geospatial and economic data

### **Regulatory Compliance**
- Government expenditure transparency and analysis
- Business registration compliance monitoring
- Economic policy impact assessment

---

## 🔧 **Technical Implementation**

### **Data Models**
- **Standardized Schema**: All data sources use unified `DataRecord` model
- **Type Safety**: Pydantic-based validation and serialization
- **Metadata**: Rich metadata including quality scores and processing notes

### **Streaming Architecture**
- **Message Broker**: Apache Kafka for real-time data streaming
- **Processing**: Apache Spark for distributed data processing
- **Storage**: Delta Lake for ACID transactions and versioning

### **Scalability**
- **Horizontal Scaling**: Kubernetes-based container orchestration
- **Storage**: S3-compatible MinIO for scalable object storage
- **Processing**: Spark cluster for distributed computing

---

*Last Updated: December 2024*
*Platform Version: 1.0*