#!/usr/bin/env python3
"""
Spark Structured Streaming Consumer for Economic Intelligence Platform
Consumes data from Kafka topics and writes to Delta Lake Bronze layer
"""

import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.types as T
from delta import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkStreamingConsumer:
    """Spark Structured Streaming consumer for Kafka to Delta Lake"""
    
    def __init__(self):
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-service.economic-observatory.svc.cluster.local:9092')
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio-service.economic-observatory.svc.cluster.local:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'admin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'password123')
        self.delta_path = f"s3a://bronze/"
        
        self.spark = self._create_spark_session()
        
        logger.info(f"Spark Streaming Consumer initialized with Kafka: {self.kafka_bootstrap_servers}")
    
    def _create_spark_session(self):
        """Create Spark session with Delta Lake support"""
        # Get MinIO configuration
        minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio-service.economic-observatory.svc.cluster.local:9000')
        minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'admin')
        minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'password123')
        
        builder = SparkSession.builder \
            .appName("EconomicIntelligence-StreamingConsumer") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{self.minio_endpoint}") \
            .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "10000")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        logger.info(f"Spark session created with S3 endpoint: {self.minio_endpoint}")

        return spark

    def create_kafka_stream(self, topic: str):
        """Create Kafka streaming DataFrame with enhanced error handling"""
        logger.info(f"Creating Kafka stream for topic: {topic} with servers: {self.kafka_bootstrap_servers}")
        
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("kafka.consumer.timeout.ms", "10000") \
            .option("kafka.request.timeout.ms", "30000") \
            .option("kafka.session.timeout.ms", "30000") \
            .load()
    
    def process_acra_stream(self):
        """Process ACRA companies data stream"""
        logger.info("Starting ACRA stream processing...")
        
        # Use flexible MapType for processed_data to handle varying field sets
        processed_data_schema = T.MapType(T.StringType(), T.StringType(), True)
        
        # Define schema for the complete DataRecord
        acra_schema = T.StructType([
            T.StructField("source", T.StringType(), True),
            T.StructField("timestamp", T.StringType(), True),
            T.StructField("data_type", T.StringType(), True),
            T.StructField("raw_data", T.MapType(T.StringType(), T.StringType()), True),
            T.StructField("processed_data", processed_data_schema, True),
            T.StructField("record_id", T.StringType(), True),
            T.StructField("quality_score", T.DoubleType(), True),
            T.StructField("validation_errors", T.ArrayType(T.StringType()), True),
            T.StructField("processing_notes", T.StringType(), True)
        ])
        
        # Create stream
        kafka_stream = self.create_kafka_stream("acra-companies")
        
        # Parse JSON and flatten the processed_data
        parsed_stream = kafka_stream.select(
            from_json(col("value").cast("string"), acra_schema).alias("record"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset")
        ).select(
            # Extract fields from processed_data using map access - flexible for varying fields
            col("record.processed_data")["entity_name"].alias("entity_name"),
            col("record.processed_data")["uen"].alias("uen"),
            col("record.processed_data")["entity_type_desc"].alias("entity_type"),
            col("record.processed_data")["uen_issue_date"].alias("uen_issue_date"),
            col("record.processed_data")["uen_status_desc"].alias("entity_status"),
            col("record.processed_data")["reg_street_name"].alias("reg_street_name"),
            col("record.processed_data")["reg_postal_code"].alias("reg_postal_code"),
            # Metadata fields
            col("record.source").alias("source"),
            col("record.timestamp").alias("ingestion_timestamp"),
            col("kafka_timestamp"),
            col("partition"),
            col("offset"),
            current_timestamp().alias("bronze_ingestion_timestamp")
        )
        
        # Write to Delta Lake Bronze layer
        query = parsed_stream.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/spark-checkpoints/acra-bronze") \
            .option("path", f"{self.delta_path}acra_companies") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query
    
    def process_singstat_stream(self):
        """Process SingStat economics data stream with full time series data"""
        logger.info("Starting SingStat stream processing...")
        
        # Define schema for the complete DataRecord structure with proper time_series_data handling
        singstat_schema = T.StructType([
            T.StructField("source", T.StringType(), True),
            T.StructField("timestamp", T.StringType(), True),
            T.StructField("data_type", T.StringType(), True),
            T.StructField("raw_data", T.MapType(T.StringType(), T.StringType()), True),
            T.StructField("processed_data", T.StructType([
                T.StructField("resource_id", T.StringType(), True),
                T.StructField("dataset_title", T.StringType(), True),
                T.StructField("series_no", T.StringType(), True),
                T.StructField("indicator_name", T.StringType(), True),
                T.StructField("unit_of_measure", T.StringType(), True),
                T.StructField("frequency", T.StringType(), True),
                T.StructField("data_source", T.StringType(), True),
                T.StructField("time_series_data", T.MapType(T.StringType(), T.StringType()), True),
                T.StructField("latest_period", T.StringType(), True),
                T.StructField("latest_value", T.StringType(), True),
                T.StructField("total_periods", T.StringType(), True),
                T.StructField("data_last_updated", T.StringType(), True),
                T.StructField("table_title", T.StringType(), True)
            ]), True),
            T.StructField("record_id", T.StringType(), True),
            T.StructField("quality_score", T.DoubleType(), True),
            T.StructField("validation_errors", T.ArrayType(T.StringType()), True),
            T.StructField("processing_notes", T.StringType(), True)
        ])
        
        kafka_stream = self.create_kafka_stream("singstat-economics")
        
        # Parse the JSON and extract the time series data
        parsed_stream = kafka_stream.select(
            from_json(col("value").cast("string"), singstat_schema).alias("record"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset")
        )
        
        # Extract full time series data by exploding the time_series_data map
        # This preserves all historical data points for complete analysis
        exploded_stream = parsed_stream.select(
            col("record.processed_data.resource_id").alias("resource_id"),
            col("record.processed_data.dataset_title").alias("dataset_title"),
            col("record.processed_data.series_no").alias("series_no"),
            col("record.processed_data.indicator_name").alias("indicator_name"),
            col("record.processed_data.unit_of_measure").alias("unit"),
            col("record.processed_data.frequency").alias("frequency"),
            col("record.processed_data.data_source").alias("data_source"),
            col("record.processed_data.total_periods").alias("total_periods"),
            col("record.processed_data.data_last_updated").alias("data_last_updated"),
            col("record.processed_data.table_title").alias("table_title"),
            # Explode time_series_data to get all period-value pairs
            explode(col("record.processed_data.time_series_data")).alias("period", "value"),
            # Metadata fields
            col("record.source").alias("source"),
            col("record.timestamp").alias("ingestion_timestamp"),
            col("record.data_type").alias("data_type"),
            col("record.record_id").alias("record_id"),
            col("kafka_timestamp"),
            col("partition"),
            col("offset"),
            current_timestamp().alias("bronze_ingestion_timestamp")
        )
        
        # Filter out null or empty values to ensure data quality
        flattened_stream = exploded_stream.filter(
            col("period").isNotNull() & 
            col("value").isNotNull() & 
            (col("period") != "") & 
            (col("value") != "")
        )
        
        query = flattened_stream.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/spark-checkpoints/singstat-bronze") \
            .option("path", f"{self.delta_path}singstat_economics") \
            .option("mergeSchema", "true") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query
    
    def process_commercial_rental_stream(self):
        """Process Commercial Rental Index data stream"""
        logger.info("Starting Commercial Rental stream processing...")
        
        # Define schema for Commercial Rental data based on the Kafka message structure
        commercial_rental_schema = T.StructType([
            T.StructField("source", T.StringType(), True),
            T.StructField("timestamp", T.StringType(), True),
            T.StructField("data_type", T.StringType(), True),
            T.StructField("raw_data", T.StructType([
                T.StructField("_id", T.StringType(), True),
                T.StructField("quarter", T.StringType(), True),
                T.StructField("property_type", T.StringType(), True),
                T.StructField("index", T.StringType(), True)
            ]), True),
            T.StructField("processed_data", T.StructType([
                T.StructField("record_id", T.StringType(), True),
                T.StructField("quarter", T.StringType(), True),
                T.StructField("property_type", T.StringType(), True),
                T.StructField("rental_index", T.StringType(), True),
                T.StructField("base_period", T.StringType(), True),
                T.StructField("base_value", T.StringType(), True)
            ]), True),
            T.StructField("record_id", T.StringType(), True),
            T.StructField("quality_score", T.DoubleType(), True),
            T.StructField("validation_errors", T.ArrayType(T.StringType()), True),
            T.StructField("processing_notes", T.StringType(), True)
        ])
        
        kafka_stream = self.create_kafka_stream("commercial-rental-index")
        
        # Parse JSON and extract commercial rental data
        parsed_stream = kafka_stream.select(
            from_json(col("value").cast("string"), commercial_rental_schema).alias("record"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset")
        ).select(
            # Extract fields from processed_data
            col("record.processed_data.record_id").alias("record_id"),
            col("record.processed_data.quarter").alias("quarter"),
            col("record.processed_data.property_type").alias("property_type"),
            col("record.processed_data.rental_index").alias("rental_index"),
            col("record.processed_data.base_period").alias("base_period"),
            col("record.processed_data.base_value").alias("base_value"),
            # Metadata fields
            col("record.source").alias("source"),
            col("record.timestamp").alias("ingestion_timestamp"),
            col("record.data_type").alias("data_type"),
            col("record.quality_score").alias("quality_score"),
            col("kafka_timestamp"),
            col("partition"),
            col("offset"),
            current_timestamp().alias("bronze_ingestion_timestamp")
        )
        
        # Filter out null or empty values to ensure data quality
        filtered_stream = parsed_stream.filter(
            col("quarter").isNotNull() & 
            col("property_type").isNotNull() & 
            col("rental_index").isNotNull() &
            (col("quarter") != "") & 
            (col("property_type") != "") & 
            (col("rental_index") != "")
        )
        
        query = filtered_stream.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/spark-checkpoints/commercial-rental-bronze") \
            .option("path", f"{self.delta_path}commercial_rental_index") \
            .option("mergeSchema", "true") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query
    
    def process_ura_stream(self):
        """Process URA geospatial data stream"""
        logger.info("Starting URA stream processing...")
        
        # Define schema for URA data
        ura_schema = T.StructType([
            T.StructField("project", T.StringType(), True),
            T.StructField("street", T.StringType(), True),
            T.StructField("x", T.StringType(), True),
            T.StructField("y", T.StringType(), True),
            T.StructField("lease_commence_date", T.StringType(), True),
            T.StructField("property_type", T.StringType(), True),
            T.StructField("district", T.StringType(), True),
            T.StructField("tenure", T.StringType(), True),
            T.StructField("built_year", T.StringType(), True),
            T.StructField("source", T.StringType(), True),
            T.StructField("ingestion_timestamp", T.StringType(), True)
        ])
        
        kafka_stream = self.create_kafka_stream("ura-geospatial")
        
        parsed_stream = kafka_stream.select(
            from_json(col("value").cast("string"), ura_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset")
        ).select(
            col("data.*"),
            col("kafka_timestamp"),
            col("partition"),
            col("offset"),
            current_timestamp().alias("bronze_ingestion_timestamp")
        )
        
        query = parsed_stream.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/spark-checkpoints/ura-bronze") \
            .option("path", f"{self.delta_path}ura_geospatial") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query
    
    def test_kafka_connectivity(self):
        """Test Kafka connectivity before starting streams"""
        try:
            logger.info(f"Testing Kafka connectivity to: {self.kafka_bootstrap_servers}")
            
            # Create a simple test stream to verify Kafka connectivity
            test_stream = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", "acra-companies") \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Start a test query that will fail quickly if Kafka is not accessible
            test_query = test_stream.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("numRows", 1) \
                .option("truncate", "false") \
                .trigger(processingTime="5 seconds") \
                .start()
            
            # Wait briefly to see if connection works
            import time
            time.sleep(10)
            test_query.stop()
            
            logger.info("✅ Kafka connectivity test successful")
            return True
            
        except Exception as e:
            logger.error(f"❌ Kafka connectivity test failed: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def start_all_streams(self):
        """Start all streaming queries"""
        logger.info("Starting all Kafka to Delta Lake streams...")
        
        queries = [
            self.process_acra_stream(),
            self.process_singstat_stream(),
            self.process_commercial_rental_stream(),
            self.process_ura_stream()
        ]
        
        logger.info(f"Started {len(queries)} streaming queries")
        
        # Wait for all queries to terminate
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping all streams...")
            for query in queries:
                query.stop()
        except Exception as e:
            logger.error(f"Stream execution error: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            for query in queries:
                query.stop()
        
        logger.info("All streams stopped")

def main():
    consumer = SparkStreamingConsumer()
    consumer.start_all_streams()

if __name__ == "__main__":
    main()