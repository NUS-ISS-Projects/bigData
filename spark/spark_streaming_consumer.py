#!/usr/bin/env python3
"""
Spark Structured Streaming Consumer for Economic Intelligence Platform
Consumes data from Kafka topics and writes to Delta Lake Bronze layer
"""

import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
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
        processed_data_schema = MapType(StringType(), StringType(), True)
        
        # Define schema for the complete DataRecord
        acra_schema = StructType([
            StructField("source", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("raw_data", MapType(StringType(), StringType()), True),
            StructField("processed_data", processed_data_schema, True),
            StructField("record_id", StringType(), True),
            StructField("quality_score", DoubleType(), True),
            StructField("validation_errors", ArrayType(StringType()), True),
            StructField("processing_notes", StringType(), True)
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
        """Process SingStat economics data stream"""
        logger.info("Starting SingStat stream processing...")
        
        # Define schema for SingStat data
        singstat_schema = StructType([
            StructField("table_id", StringType(), True),
            StructField("series_id", StringType(), True),
            StructField("data_type", StringType(), True),
            StructField("period", StringType(), True),
            StructField("value", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("source", StringType(), True),
            StructField("ingestion_timestamp", StringType(), True)
        ])
        
        kafka_stream = self.create_kafka_stream("singstat-economics")
        
        parsed_stream = kafka_stream.select(
            from_json(col("value").cast("string"), singstat_schema).alias("data"),
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
            .option("checkpointLocation", "/tmp/spark-checkpoints/singstat-bronze") \
            .option("path", f"{self.delta_path}singstat_economics") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query
    
    def process_ura_stream(self):
        """Process URA geospatial data stream"""
        logger.info("Starting URA stream processing...")
        
        # Define schema for URA data
        ura_schema = StructType([
            StructField("project", StringType(), True),
            StructField("street", StringType(), True),
            StructField("x", StringType(), True),
            StructField("y", StringType(), True),
            StructField("lease_commence_date", StringType(), True),
            StructField("property_type", StringType(), True),
            StructField("district", StringType(), True),
            StructField("tenure", StringType(), True),
            StructField("built_year", StringType(), True),
            StructField("source", StringType(), True),
            StructField("ingestion_timestamp", StringType(), True)
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
        """Start all streaming queries with connectivity testing"""
        logger.info("Starting all Kafka to Delta Lake streams...")
        
        # Test Kafka connectivity first
        if not self.test_kafka_connectivity():
            logger.error("Kafka connectivity test failed. Aborting stream startup.")
            return
        
        queries = [
            self.process_acra_stream(),
            self.process_singstat_stream(),
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
        
        self.spark.stop()
        logger.info("All streams stopped")

def main():
    consumer = SparkStreamingConsumer()
    consumer.start_all_streams()

if __name__ == "__main__":
    main()