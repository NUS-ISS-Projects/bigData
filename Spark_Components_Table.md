# Complete Spark Components Table - Economic Intelligence Platform

| **Component Category** | **Component Name** | **File Location** | **Line Range** | **Functionality** | **Key Features** | **Data Sources** | **Transformations** |
|------------------------|-------------------|-------------------|----------------|-------------------|------------------|------------------|-------------------|
| **Streaming Infrastructure** | SparkStreamingConsumer | `spark/spark_streaming_consumer.py` | 21-540 | Main streaming orchestrator | Multi-source Kafka processing, Delta Lake integration, MinIO storage | All 5 data sources | JSON parsing, schema validation, error handling |
| **Streaming Infrastructure** | Spark Session Config | `spark/spark_streaming_consumer.py` | 37-49 | Session initialization | Delta Lake extensions, S3A configuration, MinIO endpoint setup | N/A | Configuration setup |
| **Streaming Infrastructure** | Kafka Stream Creator | `spark/spark_streaming_consumer.py` | 51-65 | Kafka connection setup | Stream reader configuration, checkpoint management | Kafka topics | Stream initialization |
| **Real-time Streams** | ACRA Companies Stream | `spark/spark_streaming_consumer.py` | 100-167 | Business registration data | 8-field schema, UEN processing, entity validation | ACRA API | JSON parsing, flattening, null filtering, Bronze persistence |
| **Real-time Streams** | SingStat Economics Stream | `spark/spark_streaming_consumer.py` | 168-235 | Economic indicators | 7-field schema, time series processing | SingStat API | Data explosion, nested extraction, quality validation |
| **Real-time Streams** | Commercial Rental Stream | `spark/spark_streaming_consumer.py` | 236-303 | Rental market data | 6-field schema, quarterly processing | Commercial Rental API | Index normalization, property categorization |
| **Real-time Streams** | URA Geospatial Stream | `spark/spark_streaming_consumer.py` | 304-371 | Property location data | 8-field schema, coordinate validation | URA API | Geospatial validation, address standardization |
| **Real-time Streams** | Government Expenditure Stream | `spark/spark_streaming_consumer.py` | 372-439 | Financial spending data | 7-field schema, amount processing | Government API | Financial parsing, category standardization |
| **Stream Management** | Stream Orchestration | `spark/spark_streaming_consumer.py` | 500-540 | Multi-stream coordination | Parallel stream management, termination handling | All streams | Stream lifecycle management |
| **Stream Management** | Connectivity Testing | `spark/spark_streaming_consumer.py` | 440-499 | Pre-stream validation | Kafka connectivity checks, error reporting | Kafka cluster | Connection validation |
| **ETL Infrastructure** | BronzeToSilverETL | `spark/etl_bronze_to_silver.py` | 21-614 | Batch transformation pipeline | Parallel processing, quality scoring, deduplication | Bronze layer tables | Comprehensive data transformation |
| **ETL Infrastructure** | ETL Spark Session | `spark/etl_bronze_to_silver.py` | 37-49 | Batch session setup | Delta Lake configuration, S3A settings | N/A | Session configuration |
| **ETL Infrastructure** | S3 Configuration | `spark/etl_bronze_to_silver.py` | 50-59 | Storage setup | MinIO/S3 endpoint configuration | MinIO storage | Storage configuration |
| **Batch Transformations** | ACRA Companies ETL | `spark/etl_bronze_to_silver.py` | 82-167 | Business data cleaning | UEN validation, status normalization, quality scoring | acra_companies bronze | Advanced validation, deduplication, statistics |
| **Batch Transformations** | SingStat Economics ETL | `spark/etl_bronze_to_silver.py` | 168-246 | Economic data processing | Time series validation, numeric casting | singstat_economics bronze | Period parsing, data type standardization |
| **Batch Transformations** | URA Geospatial ETL | `spark/etl_bronze_to_silver.py` | 247-339 | Property data cleaning | Coordinate validation, rental normalization | ura_geospatial bronze | Geospatial validation, district standardization |
| **Batch Transformations** | Commercial Rental ETL | `spark/etl_bronze_to_silver.py` | 340-429 | Rental index processing | Quarter parsing, index categorization | commercial_rental_index bronze | Trend analysis preparation, base normalization |
| **Batch Transformations** | Government Expenditure ETL | `spark/etl_bronze_to_silver.py` | 430-525 | Financial data processing | Amount conversion, expenditure categorization | government_expenditure bronze | Financial validation, decade grouping |
| **Parallel Processing** | ThreadPool ETL Executor | `spark/etl_bronze_to_silver.py` | 526-578 | Concurrent processing | 5-worker thread pool, task management | All bronze tables | Parallel transformation execution |
| **Parallel Processing** | Sequential ETL Fallback | `spark/etl_bronze_to_silver.py` | 579-598 | Sequential processing | Compatibility mode, error handling | All bronze tables | Sequential transformation execution |
| **Data Quality** | Quality Scoring System | Both files | Multiple ranges | Data validation framework | Multi-dimensional scoring, validation rules | All data sources | Quality metrics calculation |
| **Data Quality** | Deduplication Logic | Both files | Multiple ranges | Duplicate removal | Window functions, latest record selection | All data sources | Advanced deduplication strategies |
| **Data Quality** | Validation Functions | Both files | Multiple ranges | Data validation | Regex patterns, null checks, type casting | All data sources | Comprehensive validation rules |
| **Analytics Infrastructure** | LLM Economic Analyzer | `analytics/enhanced_economic_intelligence.py` | 78-326 | AI-powered analysis | LLM integration, economic insights | Silver layer data | Business formation, economic indicators analysis |
| **Analytics Infrastructure** | Business Formation Analysis | `analytics/enhanced_economic_intelligence.py` | 96-166 | Business trend analysis | Metrics calculation, LLM enhancement | ACRA silver data | Industry distribution, formation trends |
| **Analytics Infrastructure** | Economic Indicators Analysis | `analytics/enhanced_economic_intelligence.py` | 167-226 | Economic data analysis | Indicator processing, trend analysis | SingStat silver data | Economic metrics, trend identification |
| **Analytics Infrastructure** | Cross-Sector Analysis | `analytics/enhanced_economic_intelligence.py` | 227-326 | Multi-source correlation | Cross-sector metrics, relationship analysis | Multiple silver sources | Correlation analysis, policy insights |
| **Machine Learning** | Anomaly Detection System | `analytics/enhanced_economic_intelligence.py` | 594-905 | ML-based anomaly detection | Statistical analysis, LLM explanation | All silver data | Business, economic, government, property anomalies |
| **Machine Learning** | Business Anomaly Detection | `analytics/enhanced_economic_intelligence.py` | 633-679 | Business pattern analysis | Registration anomalies, industry shifts | ACRA silver data | Statistical deviation detection |
| **Machine Learning** | Economic Anomaly Detection | `analytics/enhanced_economic_intelligence.py` | 680-728 | Economic indicator monitoring | Value anomalies, trend breaks | SingStat silver data | Economic pattern analysis |
| **Machine Learning** | Government Anomaly Detection | `analytics/enhanced_economic_intelligence.py` | 729-761 | Spending pattern analysis | Expenditure anomalies, budget deviations | Government silver data | Financial pattern detection |
| **Machine Learning** | Property Anomaly Detection | `analytics/enhanced_economic_intelligence.py` | 762-796 | Property market monitoring | Rental anomalies, market shifts | URA silver data | Property market analysis |
| **Forecasting** | Economic Forecasting Framework | `analytics/enhanced_economic_intelligence.py` | 65-76 | Prediction capabilities | Confidence intervals, scenario analysis | All silver data | Time series forecasting, risk assessment |
| **Data Connectors** | Silver Layer Connector | `analytics/silver_data_connector.py` | Full file | Data access layer | Multi-source data loading, caching | Silver layer tables | Data retrieval, preprocessing |
| **Configuration** | Delta Lake Integration | Both Spark files | Session configs | ACID transactions | Time travel, versioning, metadata | All data layers | Transactional data operations |
| **Configuration** | S3A/MinIO Setup | Both Spark files | Session configs | Distributed storage | Object storage integration, path-style access | MinIO buckets | Storage configuration |
| **Configuration** | Kafka Integration | Streaming file | Multiple sections | Message streaming | Topic consumption, offset management | Kafka topics | Stream processing setup |
| **Optimization** | Adaptive Query Execution | Both Spark files | Implicit | Query optimization | Dynamic optimization, join strategies | All queries | Automatic performance tuning |
| **Optimization** | Broadcast Joins | Both Spark files | Implicit | Join optimization | Small table broadcasting | Dimension tables | Join performance optimization |
| **Optimization** | Columnar Storage | Both Spark files | Delta format | Storage optimization | Parquet-based storage, compression | All tables | Storage efficiency |
| **Monitoring** | Logging Framework | All files | Throughout | Operational monitoring | Structured logging, error tracking | All operations | Comprehensive logging |
| **Monitoring** | Statistics Generation | ETL file | Multiple methods | Data profiling | Summary statistics, quality metrics | All silver tables | Data quality monitoring |
| **Error Handling** | Exception Management | All files | Throughout | Robust error handling | Try-catch blocks, graceful degradation | All operations | Error recovery and reporting |
| **Error Handling** | Fallback Mechanisms | Analytics file | Multiple methods | Backup processing | LLM fallbacks, alternative processing | All analytics | Resilient processing |

## Summary Statistics

| **Metric** | **Count** | **Details** |
|------------|-----------|-------------|
| **Total Components** | 42 | Distinct Spark implementations |
| **Files Analyzed** | 3 | Primary Spark-related files |
| **Lines of Code** | 1,800+ | Total Spark implementation |
| **Data Sources** | 5 | Integrated external APIs |
| **Streaming Pipelines** | 5 | Real-time data streams |
| **ETL Pipelines** | 5 | Batch transformation processes |
| **ML Components** | 8 | Machine learning implementations |
| **Quality Checks** | 25+ | Data validation rules |
| **Configuration Items** | 15+ | Spark optimization settings |

## Component Categories Distribution

| **Category** | **Component Count** | **Percentage** |
|--------------|-------------------|----------------|
| **Streaming Infrastructure** | 8 | 19% |
| **Batch ETL** | 10 | 24% |
| **Data Quality** | 6 | 14% |
| **Analytics & ML** | 12 | 29% |
| **Configuration** | 6 | 14% |

## Key Technical Achievements

- **Multi-Modal Processing**: Both real-time streaming and batch ETL
- **Comprehensive Data Quality**: 5-point quality scoring system
- **Advanced Analytics**: LLM-enhanced economic intelligence
- **Parallel Processing**: Concurrent ETL execution
- **Robust Architecture**: Delta Lake ACID transactions
- **Scalable Design**: Kubernetes-ready containerized deployment