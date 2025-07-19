#!/usr/bin/env python3
"""
ETL Job: Bronze to Silver Layer Transformation
Cleans, validates, and standardizes data from Bronze to Silver layer
"""

import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeToSilverETL:
    """ETL processor for Bronze to Silver layer transformation"""
    
    def __init__(self):
        self.spark = self._create_spark_session()
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.bronze_path = "s3a://bronze/"
        self.silver_path = "s3a://silver/"
        
        # Configure S3/MinIO settings
        self._configure_s3_settings()
        
        logger.info("Bronze to Silver ETL initialized")
    
    def _create_spark_session(self):
        """Create Spark session with Delta Lake support"""
        builder = SparkSession.builder \
            .appName("EconomicIntelligence-BronzeToSilver") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
        
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
    
    def read_bronze_table(self, table_name: str):
        """Read data from Bronze layer Delta table"""
        try:
            return self.spark.read.format("delta").load(f"{self.bronze_path}{table_name}")
        except Exception as e:
            logger.error(f"Error reading bronze table {table_name}: {e}")
            return None
    
    def write_silver_table(self, df, table_name: str, mode: str = "overwrite"):
        """Write data to Silver layer Delta table"""
        try:
            df.write \
                .format("delta") \
                .mode(mode) \
                .option("mergeSchema", "true") \
                .save(f"{self.silver_path}{table_name}")
            
            logger.info(f"Successfully wrote {df.count()} records to silver.{table_name}")
        except Exception as e:
            logger.error(f"Error writing to silver table {table_name}: {e}")
            raise
    
    def transform_acra_companies(self):
        """Transform ACRA companies data from Bronze to Silver"""
        logger.info("Transforming ACRA companies data...")
        
        bronze_df = self.read_bronze_table("acra_companies")
        if bronze_df is None:
            logger.warning("No ACRA bronze data found")
            return
        
        # Data quality checks and transformations
        silver_df = bronze_df \
            .filter(col("uen").isNotNull()) \
            .filter(col("entity_name").isNotNull()) \
            .withColumn("uen_clean", upper(trim(col("uen")))) \
            .withColumn("entity_name_clean", trim(col("entity_name"))) \
            .withColumn("entity_type_clean", upper(trim(col("entity_type")))) \
            .withColumn("entity_status_clean", upper(trim(col("entity_status")))) \
            .withColumn("reg_postal_code_clean", 
                       when(col("reg_postal_code").rlike("^[0-9]{6}$"), 
                            col("reg_postal_code")).otherwise(None)) \
            .withColumn("uen_issue_date_parsed", 
                       to_date(col("uen_issue_date"), "yyyy-MM-dd")) \
            .withColumn("is_active", 
                       when(col("entity_status_clean") == "LIVE", True)
                       .when(col("entity_status_clean") == "ACTIVE", True)
                       .otherwise(False)) \
            .withColumn("data_quality_score", 
                       (when(col("uen").isNotNull(), 1).otherwise(0) +
                        when(col("entity_name").isNotNull(), 1).otherwise(0) +
                        when(col("entity_type").isNotNull(), 1).otherwise(0) +
                        when(col("entity_status").isNotNull(), 1).otherwise(0) +
                        when(col("reg_postal_code_clean").isNotNull(), 1).otherwise(0)) / 5.0) \
            .withColumn("silver_processed_timestamp", current_timestamp())
        
        # Remove duplicates based on UEN (keep latest)
        window_spec = Window.partitionBy("uen_clean").orderBy(desc("bronze_ingestion_timestamp"))
        silver_df = silver_df \
            .withColumn("row_number", row_number().over(window_spec)) \
            .filter(col("row_number") == 1) \
            .drop("row_number")
        
        # Select final columns
        final_df = silver_df.select(
            col("uen_clean").alias("uen"),
            col("entity_name_clean").alias("entity_name"),
            col("entity_type_clean").alias("entity_type"),
            col("entity_status_clean").alias("entity_status"),
            col("is_active"),
            col("uen_issue_date_parsed").alias("uen_issue_date"),
            col("reg_street_name"),
            col("reg_postal_code_clean").alias("reg_postal_code"),
            col("data_quality_score"),
            col("source"),
            col("ingestion_timestamp"),
            col("bronze_ingestion_timestamp"),
            col("silver_processed_timestamp")
        )
        
        self.write_silver_table(final_df, "acra_companies_clean")
        
        # Create summary statistics
        stats_df = final_df.agg(
            count("*").alias("total_records"),
            countDistinct("uen").alias("unique_companies"),
            sum(when(col("is_active"), 1).otherwise(0)).alias("active_companies"),
            avg("data_quality_score").alias("avg_quality_score"),
            min("uen_issue_date").alias("earliest_registration"),
            max("uen_issue_date").alias("latest_registration")
        ).withColumn("table_name", lit("acra_companies_clean")) \
         .withColumn("processed_timestamp", current_timestamp())
        
        self.write_silver_table(stats_df, "data_quality_stats", "append")
    
    def transform_singstat_economics(self):
        """Transform SingStat economics data from Bronze to Silver"""
        logger.info("Transforming SingStat economics data...")
        
        bronze_df = self.read_bronze_table("singstat_economics")
        if bronze_df is None:
            logger.warning("No SingStat bronze data found")
            return
        
        # Data quality checks and transformations
        silver_df = bronze_df \
            .filter(col("table_title").isNotNull()) \
            .filter(col("series_no").isNotNull()) \
            .filter(col("value").isNotNull()) \
            .withColumn("table_id_clean", upper(trim(col("table_title")))) \
            .withColumn("series_id_clean", upper(trim(col("series_no")))) \
            .withColumn("data_type_clean", upper(trim(col("data_type")))) \
            .withColumn("period_clean", trim(col("period"))) \
            .withColumn("value_numeric", 
                       when(col("value").rlike("^-?[0-9]+(\\.[0-9]+)?$"), 
                            col("value").cast("double")).otherwise(None)) \
            .withColumn("unit_clean", trim(col("unit"))) \
            .withColumn("period_year", 
                       when(col("period_clean").rlike("^[0-9]{4}"), 
                            substring(col("period_clean"), 1, 4).cast("int")).otherwise(None)) \
            .withColumn("period_quarter", 
                       when(col("period_clean").rlike("Q[1-4]"), 
                            regexp_extract(col("period_clean"), "Q([1-4])", 1).cast("int")).otherwise(None)) \
            .withColumn("is_valid_numeric", col("value_numeric").isNotNull()) \
            .withColumn("data_quality_score", 
                       (when(col("table_title").isNotNull(), 1).otherwise(0) +
                        when(col("series_no").isNotNull(), 1).otherwise(0) +
                        when(col("value_numeric").isNotNull(), 1).otherwise(0) +
                        when(col("period_year").isNotNull(), 1).otherwise(0)) / 4.0) \
            .withColumn("silver_processed_timestamp", current_timestamp())
        
        # Remove duplicates
        window_spec = Window.partitionBy("table_id_clean", "series_id_clean", "period_clean") \
                           .orderBy(desc("bronze_ingestion_timestamp"))
        silver_df = silver_df \
            .withColumn("row_number", row_number().over(window_spec)) \
            .filter(col("row_number") == 1) \
            .drop("row_number")
        
        # Select final columns
        final_df = silver_df.select(
            col("table_id_clean").alias("table_id"),
            col("series_id_clean").alias("series_id"),
            col("data_type_clean").alias("data_type"),
            col("period_clean").alias("period"),
            col("period_year"),
            col("period_quarter"),
            col("value").alias("value_original"),
            col("value_numeric"),
            col("is_valid_numeric"),
            col("unit_clean").alias("unit"),
            col("data_quality_score"),
            col("source"),
            col("ingestion_timestamp"),
            col("bronze_ingestion_timestamp"),
            col("silver_processed_timestamp")
        )
        
        self.write_silver_table(final_df, "singstat_economics_clean")
        
        # Create summary statistics
        stats_df = final_df.agg(
            count("*").alias("total_records"),
            countDistinct("table_id").alias("unique_tables"),
            countDistinct("series_id").alias("unique_series"),
            sum(when(col("is_valid_numeric"), 1).otherwise(0)).alias("valid_numeric_values"),
            avg("data_quality_score").alias("avg_quality_score"),
            min("period_year").alias("earliest_year"),
            max("period_year").alias("latest_year")
        ).withColumn("table_name", lit("singstat_economics_clean")) \
         .withColumn("processed_timestamp", current_timestamp())
        
        self.write_silver_table(stats_df, "data_quality_stats", "append")
    
    def transform_ura_geospatial(self):
        """Transform URA geospatial data from Bronze to Silver"""
        logger.info("Transforming URA geospatial data...")
        
        bronze_df = self.read_bronze_table("ura_geospatial")
        if bronze_df is None:
            logger.warning("No URA bronze data found")
            return
        
        # Data quality checks and transformations
        silver_df = bronze_df \
            .filter(col("project").isNotNull()) \
            .withColumn("project_clean", trim(col("project"))) \
            .withColumn("street_clean", trim(col("street"))) \
            .withColumn("x_coordinate", 
                       when(col("x").rlike("^-?[0-9]+(\\.[0-9]+)?$"), 
                            col("x").cast("double")).otherwise(None)) \
            .withColumn("y_coordinate", 
                       when(col("y").rlike("^-?[0-9]+(\\.[0-9]+)?$"), 
                            col("y").cast("double")).otherwise(None)) \
            .withColumn("lease_commence_date_parsed", 
                       to_date(col("lease_commence_date"), "yyyy")) \
            .withColumn("property_type_clean", upper(trim(col("property_type")))) \
            .withColumn("district_clean", trim(col("district"))) \
            .withColumn("tenure_clean", upper(trim(col("tenure")))) \
            .withColumn("built_year_parsed", 
                       when(col("built_year").rlike("^[0-9]{4}$"), 
                            col("built_year").cast("int")).otherwise(None)) \
            .withColumn("has_coordinates", 
                       col("x_coordinate").isNotNull() & col("y_coordinate").isNotNull()) \
            .withColumn("data_quality_score", 
                       (when(col("project").isNotNull(), 1).otherwise(0) +
                        when(col("has_coordinates"), 1).otherwise(0) +
                        when(col("property_type").isNotNull(), 1).otherwise(0) +
                        when(col("built_year_parsed").isNotNull(), 1).otherwise(0)) / 4.0) \
            .withColumn("silver_processed_timestamp", current_timestamp())
        
        # Remove duplicates
        window_spec = Window.partitionBy("project_clean", "street_clean") \
                           .orderBy(desc("bronze_ingestion_timestamp"))
        silver_df = silver_df \
            .withColumn("row_number", row_number().over(window_spec)) \
            .filter(col("row_number") == 1) \
            .drop("row_number")
        
        # Select final columns
        final_df = silver_df.select(
            col("project_clean").alias("project"),
            col("street_clean").alias("street"),
            col("x_coordinate"),
            col("y_coordinate"),
            col("has_coordinates"),
            col("lease_commence_date_parsed").alias("lease_commence_date"),
            col("property_type_clean").alias("property_type"),
            col("district_clean").alias("district"),
            col("tenure_clean").alias("tenure"),
            col("built_year_parsed").alias("built_year"),
            col("data_quality_score"),
            col("source"),
            col("ingestion_timestamp"),
            col("bronze_ingestion_timestamp"),
            col("silver_processed_timestamp")
        )
        
        self.write_silver_table(final_df, "ura_geospatial_clean")
        
        # Create summary statistics
        stats_df = final_df.agg(
            count("*").alias("total_records"),
            countDistinct("project").alias("unique_projects"),
            sum(when(col("has_coordinates"), 1).otherwise(0)).alias("records_with_coordinates"),
            avg("data_quality_score").alias("avg_quality_score"),
            min("built_year").alias("earliest_built_year"),
            max("built_year").alias("latest_built_year")
        ).withColumn("table_name", lit("ura_geospatial_clean")) \
         .withColumn("processed_timestamp", current_timestamp())
        
        self.write_silver_table(stats_df, "data_quality_stats", "append")
    
    def run_etl(self):
        """Run complete Bronze to Silver ETL process"""
        logger.info("Starting Bronze to Silver ETL process...")
        
        try:
            # Transform all data sources
            self.transform_acra_companies()
            self.transform_singstat_economics()
            self.transform_ura_geospatial()
            
            logger.info("Bronze to Silver ETL completed successfully")
            
        except Exception as e:
            logger.error(f"ETL process failed: {e}")
            raise
        finally:
            self.spark.stop()

def main():
    etl = BronzeToSilverETL()
    etl.run_etl()

if __name__ == "__main__":
    main()