#!/usr/bin/env python3
"""
ETL Job: Bronze to Silver Layer Transformation
Cleans, validates, and standardizes data from Bronze to Silver layer
"""

import os
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'admin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'password123')
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
        """Transform URA PMI_Resi_Rental_Median data from Bronze to Silver"""
        logger.info("Transforming URA PMI_Resi_Rental_Median data...")
        
        bronze_df = self.read_bronze_table("ura_geospatial")
        if bronze_df is None:
            logger.warning("No URA bronze data found")
            return
        
        # Data quality checks and transformations for PMI_Resi_Rental_Median structure
        silver_df = bronze_df \
            .filter(col("service_type").isNotNull()) \
            .filter(col("project_name").isNotNull()) \
            .withColumn("service_type_clean", trim(col("service_type"))) \
            .withColumn("street_name_clean", trim(col("street_name"))) \
            .withColumn("project_name_clean", trim(col("project_name"))) \
            .withColumn("latitude_numeric", 
                       when(col("latitude").rlike("^-?[0-9]+(\\.[0-9]+)?$"), 
                            col("latitude").cast("double")).otherwise(None)) \
            .withColumn("longitude_numeric", 
                       when(col("longitude").rlike("^-?[0-9]+(\\.[0-9]+)?$"), 
                            col("longitude").cast("double")).otherwise(None)) \
            .withColumn("district_clean", trim(col("district"))) \
            .withColumn("rental_median_numeric", 
                       when(col("rental_median").rlike("^-?[0-9]+(\\.[0-9]+)?$"), 
                            col("rental_median").cast("double")).otherwise(None)) \
            .withColumn("rental_psf25_numeric", 
                       when(col("rental_psf25").rlike("^-?[0-9]+(\\.[0-9]+)?$"), 
                            col("rental_psf25").cast("double")).otherwise(None)) \
            .withColumn("rental_psf75_numeric", 
                       when(col("rental_psf75").rlike("^-?[0-9]+(\\.[0-9]+)?$"), 
                            col("rental_psf75").cast("double")).otherwise(None)) \
            .withColumn("ref_period_clean", trim(col("ref_period"))) \
            .withColumn("has_coordinates", 
                       col("latitude_numeric").isNotNull() & col("longitude_numeric").isNotNull()) \
            .withColumn("has_rental_data", 
                       col("rental_median_numeric").isNotNull()) \
            .withColumn("data_quality_score", 
                       (when(col("service_type").isNotNull(), 1).otherwise(0) +
                        when(col("project_name").isNotNull(), 1).otherwise(0) +
                        when(col("has_coordinates"), 1).otherwise(0) +
                        when(col("has_rental_data"), 1).otherwise(0) +
                        when(col("district").isNotNull(), 1).otherwise(0)) / 5.0) \
            .withColumn("silver_processed_timestamp", current_timestamp())
        
        # Remove duplicates based on project and street
        window_spec = Window.partitionBy("project_name_clean", "street_name_clean") \
                           .orderBy(desc("bronze_ingestion_timestamp"))
        silver_df = silver_df \
            .withColumn("row_number", row_number().over(window_spec)) \
            .filter(col("row_number") == 1) \
            .drop("row_number")
        
        # Select final columns for PMI_Resi_Rental_Median
        final_df = silver_df.select(
            col("service_type_clean").alias("service_type"),
            col("street_name_clean").alias("street_name"),
            col("project_name_clean").alias("project_name"),
            col("latitude_numeric").alias("latitude"),
            col("longitude_numeric").alias("longitude"),
            col("district_clean").alias("district"),
            col("rental_median_numeric").alias("rental_median"),
            col("rental_psf25_numeric").alias("rental_psf25"),
            col("rental_psf75_numeric").alias("rental_psf75"),
            col("ref_period_clean").alias("ref_period"),
            col("has_coordinates"),
            col("has_rental_data"),
            col("data_quality_score"),
            col("source"),
            col("timestamp").alias("ingestion_timestamp"),
            col("data_type"),
            col("record_id"),
            col("bronze_ingestion_timestamp"),
            col("silver_processed_timestamp")
        )
        
        self.write_silver_table(final_df, "ura_geospatial_clean")
        
        # Create summary statistics
        stats_df = final_df.agg(
            count("*").alias("total_records"),
            countDistinct("project_name").alias("unique_projects"),
            sum(when(col("has_coordinates"), 1).otherwise(0)).alias("records_with_coordinates"),
            sum(when(col("has_rental_data"), 1).otherwise(0)).alias("records_with_rental_data"),
            avg("data_quality_score").alias("avg_quality_score"),
            avg("rental_median").alias("avg_rental_median"),
            min("ref_period").alias("earliest_ref_period"),
            max("ref_period").alias("latest_ref_period")
        ).withColumn("table_name", lit("ura_geospatial_clean")) \
         .withColumn("processed_timestamp", current_timestamp())
        
        self.write_silver_table(stats_df, "data_quality_stats", "append")
    
    def transform_commercial_rental_index(self):
        """Transform Commercial Rental Index data from Bronze to Silver"""
        logger.info("Transforming Commercial Rental Index data...")
        
        bronze_df = self.read_bronze_table("commercial_rental_index")
        if bronze_df is None:
            logger.warning("No Commercial Rental Index bronze data found")
            return
        
        # Data quality checks and transformations
        silver_df = bronze_df \
            .filter(col("quarter").isNotNull()) \
            .filter(col("property_type").isNotNull()) \
            .filter(col("rental_index").isNotNull()) \
            .withColumn("quarter_clean", upper(trim(col("quarter")))) \
            .withColumn("property_type_clean", upper(trim(col("property_type")))) \
            .withColumn("rental_index_numeric", 
                       when(col("rental_index").rlike("^-?[0-9]+(\\.[0-9]+)?$"), 
                            col("rental_index").cast("double")).otherwise(None)) \
            .withColumn("base_value_numeric", 
                       when(col("base_value").rlike("^-?[0-9]+(\\.[0-9]+)?$"), 
                            col("base_value").cast("double")).otherwise(None)) \
            .withColumn("base_period_clean", trim(col("base_period"))) \
            .withColumn("quarter_year", 
                       when(col("quarter_clean").rlike("^[0-9]{4}-Q[1-4]$"), 
                            substring(col("quarter_clean"), 1, 4).cast("int")).otherwise(None)) \
            .withColumn("quarter_number", 
                       when(col("quarter_clean").rlike("Q[1-4]$"), 
                            regexp_extract(col("quarter_clean"), "Q([1-4])$", 1).cast("int")).otherwise(None)) \
            .withColumn("is_valid_rental_index", col("rental_index_numeric").isNotNull()) \
            .withColumn("rental_index_category", 
                       when(col("rental_index_numeric") < 100, "Below Base")
                       .when(col("rental_index_numeric") == 100, "At Base")
                       .when(col("rental_index_numeric") > 100, "Above Base")
                       .otherwise("Unknown")) \
            .withColumn("data_quality_score", 
                       (when(col("quarter").isNotNull(), 1).otherwise(0) +
                        when(col("property_type").isNotNull(), 1).otherwise(0) +
                        when(col("rental_index_numeric").isNotNull(), 1).otherwise(0) +
                        when(col("quarter_year").isNotNull(), 1).otherwise(0)) / 4.0) \
            .withColumn("silver_processed_timestamp", current_timestamp())
        
        # Remove duplicates based on quarter and property type (keep latest)
        # Also remove duplicates from multiple source files
        window_spec = Window.partitionBy("quarter_clean", "property_type_clean") \
                           .orderBy(desc("bronze_ingestion_timestamp"), desc("silver_processed_timestamp"))
        silver_df = silver_df \
            .withColumn("row_number", row_number().over(window_spec)) \
            .filter(col("row_number") == 1) \
            .drop("row_number")
        
        # Select final columns
        final_df = silver_df.select(
            col("record_id"),
            col("quarter_clean").alias("quarter"),
            col("quarter_year"),
            col("quarter_number"),
            col("property_type_clean").alias("property_type"),
            col("rental_index").alias("rental_index_original"),
            col("rental_index_numeric"),
            col("rental_index_category"),
            col("is_valid_rental_index"),
            col("base_period_clean").alias("base_period"),
            col("base_value").alias("base_value_original"),
            col("base_value_numeric"),
            col("data_quality_score"),
            col("quality_score").alias("bronze_quality_score"),
            col("source"),
            col("ingestion_timestamp"),
            col("bronze_ingestion_timestamp"),
            col("silver_processed_timestamp")
        )
        
        self.write_silver_table(final_df, "commercial_rental_index_clean")
        
        # Create summary statistics
        stats_df = final_df.agg(
            count("*").alias("total_records"),
            countDistinct("quarter").alias("unique_quarters"),
            countDistinct("property_type").alias("unique_property_types"),
            sum(when(col("is_valid_rental_index"), 1).otherwise(0)).alias("valid_rental_indices"),
            avg("data_quality_score").alias("avg_quality_score"),
            avg("rental_index_numeric").alias("avg_rental_index"),
            min("quarter_year").alias("earliest_year"),
            max("quarter_year").alias("latest_year")
        ).withColumn("table_name", lit("commercial_rental_index_clean")) \
         .withColumn("processed_timestamp", current_timestamp())
        
        self.write_silver_table(stats_df, "data_quality_stats", "append")

    def transform_government_expenditure(self):
        """Transform Government Expenditure data from Bronze to Silver"""
        logger.info("Transforming Government Expenditure data...")
        
        bronze_df = self.read_bronze_table("government_expenditure")
        if bronze_df is None:
            logger.warning("No Government Expenditure bronze data found")
            return
        
        # Data quality checks and transformations
        silver_df = bronze_df \
            .filter(col("record_id").isNotNull()) \
            .filter(col("financial_year").isNotNull()) \
            .filter(col("amount_million_sgd").isNotNull()) \
            .withColumn("record_id_clean", trim(col("record_id"))) \
            .withColumn("financial_year_parsed", 
                       when(col("financial_year").rlike("^[0-9]{4}$"), 
                            col("financial_year").cast("int")).otherwise(None)) \
            .withColumn("status_clean", upper(trim(col("status")))) \
            .withColumn("expenditure_type_clean", upper(trim(col("expenditure_type")))) \
            .withColumn("category_clean", trim(col("category"))) \
            .withColumn("expenditure_class_clean", trim(col("expenditure_class"))) \
            .withColumn("amount_million_numeric", 
                       when(col("amount_million_sgd").rlike("^-?[0-9]+(\\.[0-9]+)?$"), 
                            col("amount_million_sgd").cast("double")).otherwise(None)) \
            .withColumn("amount_sgd_calculated", 
                       when(col("amount_million_numeric").isNotNull(), 
                            col("amount_million_numeric") * 1000000).otherwise(None)) \
            .withColumn("is_valid_amount", col("amount_million_numeric").isNotNull()) \
            .withColumn("is_positive_amount", col("amount_million_numeric") > 0) \
            .withColumn("expenditure_category", 
                       when(col("amount_million_numeric") < 100, "Small")
                       .when(col("amount_million_numeric") < 1000, "Medium")
                       .when(col("amount_million_numeric") < 10000, "Large")
                       .when(col("amount_million_numeric") >= 10000, "Very Large")
                       .otherwise("Unknown")) \
            .withColumn("decade", 
                       when(col("financial_year_parsed").isNotNull(), 
                            (floor(col("financial_year_parsed") / 10) * 10).cast("int")).otherwise(None)) \
            .withColumn("data_quality_score", 
                       (when(col("record_id").isNotNull(), 1).otherwise(0) +
                        when(col("financial_year_parsed").isNotNull(), 1).otherwise(0) +
                        when(col("amount_million_numeric").isNotNull(), 1).otherwise(0) +
                        when(col("status").isNotNull(), 1).otherwise(0) +
                        when(col("expenditure_type").isNotNull(), 1).otherwise(0)) / 5.0) \
            .withColumn("silver_processed_timestamp", current_timestamp())
        
        # Remove duplicates based on record_id (keep latest)
        window_spec = Window.partitionBy("record_id_clean") \
                           .orderBy(desc("bronze_ingestion_timestamp"))
        silver_df = silver_df \
            .withColumn("row_number", row_number().over(window_spec)) \
            .filter(col("row_number") == 1) \
            .drop("row_number")
        
        # Select final columns
        final_df = silver_df.select(
            col("record_id_clean").alias("record_id"),
            col("financial_year_parsed").alias("financial_year"),
            col("decade"),
            col("status_clean").alias("status"),
            col("expenditure_type_clean").alias("expenditure_type"),
            col("category_clean").alias("category"),
            col("expenditure_class_clean").alias("expenditure_class"),
            col("amount_million_sgd").alias("amount_million_original"),
            col("amount_million_numeric"),
            col("amount_sgd_calculated"),
            col("is_valid_amount"),
            col("is_positive_amount"),
            col("expenditure_category"),
            col("data_quality_score"),
            col("source"),
            col("ingestion_timestamp"),
            col("bronze_ingestion_timestamp"),
            col("silver_processed_timestamp")
        )
        
        self.write_silver_table(final_df, "government_expenditure_clean")
        
        # Create summary statistics
        stats_df = final_df.agg(
            count("*").alias("total_records"),
            countDistinct("financial_year").alias("unique_years"),
            countDistinct("expenditure_type").alias("unique_expenditure_types"),
            countDistinct("category").alias("unique_categories"),
            sum(when(col("is_valid_amount"), 1).otherwise(0)).alias("valid_amounts"),
            sum("amount_million_numeric").alias("total_expenditure_million"),
            avg("amount_million_numeric").alias("avg_expenditure_million"),
            avg("data_quality_score").alias("avg_quality_score"),
            min("financial_year").alias("earliest_year"),
            max("financial_year").alias("latest_year")
        ).withColumn("table_name", lit("government_expenditure_clean")) \
         .withColumn("processed_timestamp", current_timestamp())
        
        self.write_silver_table(stats_df, "data_quality_stats", "append")

    def run_etl_parallel(self):
        """Run complete Bronze to Silver ETL process with parallel execution"""
        logger.info("Starting Bronze to Silver ETL process with parallel execution...")
        
        # Define transformation tasks
        transformation_tasks = [
            ("ACRA Companies", self.transform_acra_companies),
            ("SingStat Economics", self.transform_singstat_economics),
            ("URA Geospatial", self.transform_ura_geospatial),
            ("Commercial Rental Index", self.transform_commercial_rental_index),
            ("Government Expenditure", self.transform_government_expenditure)
        ]
        
        try:
            # Execute transformations in parallel
            with ThreadPoolExecutor(max_workers=5) as executor:
                # Submit all tasks
                future_to_task = {
                    executor.submit(task_func): task_name 
                    for task_name, task_func in transformation_tasks
                }
                
                # Process completed tasks
                completed_tasks = []
                failed_tasks = []
                
                for future in as_completed(future_to_task):
                    task_name = future_to_task[future]
                    try:
                        future.result()  # This will raise an exception if the task failed
                        completed_tasks.append(task_name)
                        logger.info(f"✓ {task_name} transformation completed successfully")
                    except Exception as e:
                        failed_tasks.append((task_name, str(e)))
                        logger.error(f"✗ {task_name} transformation failed: {e}")
                
                # Report results
                logger.info(f"ETL Summary: {len(completed_tasks)} successful, {len(failed_tasks)} failed")
                
                if failed_tasks:
                    logger.error("Failed transformations:")
                    for task_name, error in failed_tasks:
                        logger.error(f"  - {task_name}: {error}")
                    raise Exception(f"ETL process completed with {len(failed_tasks)} failures")
                
                logger.info("Bronze to Silver ETL completed successfully with parallel execution")
                
        except Exception as e:
            logger.error(f"ETL process failed: {e}")
            raise
        finally:
            self.spark.stop()
    
    def run_etl(self):
        """Run complete Bronze to Silver ETL process (sequential - for compatibility)"""
        logger.info("Starting Bronze to Silver ETL process (sequential mode)...")
        
        try:
            # Transform all data sources sequentially
            self.transform_acra_companies()
            self.transform_singstat_economics()
            self.transform_ura_geospatial()
            self.transform_commercial_rental_index()
            self.transform_government_expenditure()
            
            logger.info("Bronze to Silver ETL completed successfully")
            
        except Exception as e:
            logger.error(f"ETL process failed: {e}")
            raise
        finally:
            self.spark.stop()

def main():
    """Main entry point with parallel execution option"""
    import sys
    
    etl = BronzeToSilverETL()
    
    # Check for parallel execution flag
    if len(sys.argv) > 1 and sys.argv[1] == "--parallel":
        logger.info("Running ETL in parallel mode")
        etl.run_etl_parallel()
    else:
        logger.info("Running ETL in sequential mode (use --parallel flag for parallel execution)")
        etl.run_etl()

if __name__ == "__main__":
    main()