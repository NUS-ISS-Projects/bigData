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
        self.spark = self._create_spark_session()
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.delta_path = f"s3a://bronze/"
        
        # Configure S3/MinIO settings
        self._configure_s3_settings()
        
        logger.info("Spark Streaming Consumer initialized")
    
    def _create_spark_session(self):
        """Create Spark session with Delta Lake support"""
        builder = SparkSession.builder \
            .appName("EconomicIntelligence-StreamingConsumer") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        return configure_spark_with_delta_pip(builder).getOrCreate()
    
    def _configure_s3_settings(self):
        """Configure Spark for S3/MinIO access"""
        hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", f"http://{self.minio_endpoint}")
        hadoop_conf.set("fs.s3a.access.key", self.minio_access_key)
        hadoop_conf.set("fs.s3a.secret.key", self.minio_secret_key)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    
    def create_kafka_stream(self, topic: str):
        """Create Kafka streaming DataFrame"""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
    
    def process_acra_stream(self):
        """Process ACRA companies data stream"""
        logger.info("Starting ACRA stream processing...")
        
        # Define schema for ACRA data
        acra_schema = StructType([
            StructField("uen", StringType(), True),
            StructField("reg_street_name", StringType(), True),
            StructField("entity_name", StringType(), True),
            StructField("entity_type", StringType(), True),
            StructField("entity_status", StringType(), True),
            StructField("uen_issue_date", StringType(), True),
            StructField("reg_postal_code", StringType(), True),
            StructField("source", StringType(), True),
            StructField("ingestion_timestamp", StringType(), True)
        ])
        
        # Create stream
        kafka_stream = self.create_kafka_stream("acra-companies")
        
        # Parse JSON and add metadata
        parsed_stream = kafka_stream.select(
            from_json(col("value").cast("string"), acra_schema).alias("data"),
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
    
    def start_all_streams(self):
        """Start all streaming queries"""
        logger.info("Starting all Kafka to Delta Lake streams...")
        
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
        
        self.spark.stop()
        logger.info("All streams stopped")

def main():
    consumer = SparkStreamingConsumer()
    consumer.start_all_streams()

if __name__ == "__main__":
    main()