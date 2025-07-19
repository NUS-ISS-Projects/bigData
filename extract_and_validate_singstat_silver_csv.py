#!/usr/bin/env python3
"""
Extract SingStat Silver Parquet Files to CSV and Validate
Extracts all parquet files from the SingStat silver Delta Lake,
converts them to CSV format, and validates data completeness.
"""

import sys
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
sys.path.append('scripts')

from init_minio_buckets import MinIOBucketInitializer
from loguru import logger

# Configure logger
logger.remove()
logger.add(sys.stdout, level="INFO")

def extract_parquets_to_csv():
    """Extract all SingStat silver parquet files from MinIO and convert to CSV"""
    
    # Expected resource IDs from SingStatProducer (should be same as bronze but with enhanced data quality)
    expected_resource_ids = [
        # GDP & National Accounts
        'M014811', 'M014812', 'M014871', 'M014911', 'M014921', 'M015721', 'M015731',
        # Services & Tourism
        'M602091', 'M601481', 'M602081',
        # Trade & External Relations
        '17947', '17949', '17951', 'M083811', 'M060261', 'M060271',
        # Employment & Labor
        'M601471',
        # Inflation & Prices
        'M213801', 'M213811', 'M213071',
        # Demographics & Population
        'M810381', 'M810011',
        # Government & Public Finance
        'M060291'
    ]
    
    # Expected silver data fields (enhanced from bronze with data quality metrics)
    expected_silver_fields = [
        'table_id', 'series_id', 'data_type', 'period', 'period_year', 'period_quarter',
        'value_original', 'value_numeric', 'is_valid_numeric', 'unit', 'data_quality_score',
        'source', 'ingestion_timestamp', 'bronze_ingestion_timestamp', 'silver_processed_timestamp'
    ]
    
    try:
        # Initialize MinIO client
        initializer = MinIOBucketInitializer()
        if not initializer.wait_for_minio():
            logger.error("Failed to connect to MinIO")
            return False
        
        logger.info("Extracting SingStat silver parquet files from MinIO silver bucket...")
        
        # Create output directory with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path(f"singstat_silver_csv_exports_{timestamp}")
        output_dir.mkdir(exist_ok=True)
        
        # List all parquet files in silver/singstat_economics_clean/
        objects = list(initializer.client.list_objects('silver', prefix='singstat_economics_clean/', recursive=True))
        
        # Filter to only actual data parquet files (exclude Delta Lake metadata)
        parquet_files = [
            obj for obj in objects 
            if obj.object_name.endswith('.parquet')
            and not obj.object_name.endswith('.checkpoint.parquet')
            and '_delta_log' not in obj.object_name
            and 'part-' in obj.object_name  # Only actual data files
        ]
        
        logger.info(f"Found {len(parquet_files)} silver parquet files to process")
        logger.info(f"Total objects in silver/singstat_economics_clean/: {len(objects)}")
        
        if not parquet_files:
            logger.warning("No silver data parquet files found in silver/singstat_economics_clean/")
            return False
        
        # Download and process each parquet file
        all_dataframes = []
        processed_files = []
        
        for i, obj in enumerate(parquet_files, 1):
            logger.info(f"Processing file {i}/{len(parquet_files)}: {obj.object_name}")
            
            try:
                # Download parquet file to temporary location
                temp_file = f"/tmp/{Path(obj.object_name).name}"
                initializer.client.fget_object('silver', obj.object_name, temp_file)
                
                # Read parquet file with pandas
                df = pd.read_parquet(temp_file)
                logger.info(f"  - Loaded {len(df):,} rows, {len(df.columns)} columns")
                
                # Filter out rows that are Delta Lake metadata (if any leaked through)
                if 'table_id' in df.columns:
                    # Keep only rows with valid table_id data
                    original_count = len(df)
                    df = df[df['table_id'].notna() & (df['table_id'] != '')]
                    if len(df) < original_count:
                        logger.info(f"  - Filtered out {original_count - len(df)} metadata rows")
                
                if len(df) == 0:
                    logger.warning(f"  - No valid data rows found, skipping file")
                    continue
                
                # Add file source information
                df['silver_source_file'] = Path(obj.object_name).name
                df['extraction_timestamp'] = datetime.now().isoformat()
                
                all_dataframes.append(df)
                processed_files.append(obj.object_name)
                
                # Save individual CSV file
                csv_filename = output_dir / f"{Path(obj.object_name).stem}.csv"
                df.to_csv(csv_filename, index=False)
                logger.info(f"  - Saved to {csv_filename}")
                
                # Clean up temp file
                os.remove(temp_file)
                
            except Exception as e:
                logger.error(f"  - Failed to process {obj.object_name}: {e}")
                continue
        
        if not all_dataframes:
            logger.error("No silver parquet files were successfully processed")
            return False
        
        # Combine all dataframes
        logger.info("Combining all silver data into master CSV...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Save combined CSV
        combined_csv = output_dir / "all_singstat_silver_data_combined.csv"
        combined_df.to_csv(combined_csv, index=False)
        logger.info(f"Saved combined silver data to {combined_csv}")
        
        # Perform validation
        validation_results = validate_extracted_data(combined_df, expected_resource_ids, expected_silver_fields, output_dir)
        
        logger.info(f"\n=== Silver Extraction Summary ===")
        logger.info(f"Processed files: {len(processed_files)}")
        logger.info(f"Total records: {len(combined_df):,}")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Combined CSV: {combined_csv}")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error during silver extraction: {e}")
        return False

def validate_extracted_data(df, expected_resource_ids, expected_silver_fields, output_dir):
    """Validate the extracted SingStat silver data for completeness and integrity"""
    
    logger.info("\n=== Silver Data Validation ===")
    
    validation_results = {
        'total_records': len(df),
        'columns': list(df.columns),
        'resource_ids_found': [],
        'missing_resource_ids': [],
        'extra_resource_ids': [],
        'data_quality_scores': {},
        'completeness_scores': {},
        'validation_status_counts': {},
        'data_quality_issues': [],
        'validation_passed': True
    }
    
    try:
        # Basic data info
        logger.info(f"Total silver records: {len(df):,}")
        logger.info(f"Columns ({len(df.columns)}): {', '.join(df.columns)}")
        
        # Check for table_id column (silver layer uses table_id instead of resource_id)
        if 'table_id' not in df.columns:
            logger.error("❌ Missing 'table_id' column")
            validation_results['data_quality_issues'].append("Missing table_id column")
            validation_results['validation_passed'] = False
            return validation_results
        
        # Analyze table IDs (silver layer equivalent of resource IDs)
        unique_resource_ids = df['table_id'].unique().tolist()
        validation_results['resource_ids_found'] = sorted(unique_resource_ids)
        
        logger.info(f"\nUnique resource IDs found: {len(unique_resource_ids)}")
        logger.info(f"Resource IDs: {sorted(unique_resource_ids)}")
        
        # Check coverage
        missing_ids = set(expected_resource_ids) - set(unique_resource_ids)
        extra_ids = set(unique_resource_ids) - set(expected_resource_ids)
        
        validation_results['missing_resource_ids'] = sorted(missing_ids)
        validation_results['extra_resource_ids'] = sorted(extra_ids)
        
        logger.info(f"\n=== Coverage Analysis ===")
        logger.info(f"Expected: {len(expected_resource_ids)} resource IDs")
        logger.info(f"Found: {len(unique_resource_ids)} resource IDs")
        logger.info(f"Coverage: {len(unique_resource_ids)/len(expected_resource_ids)*100:.1f}%")
        
        if missing_ids:
            logger.warning(f"⚠️  Missing resource IDs ({len(missing_ids)}): {sorted(missing_ids)}")
            validation_results['data_quality_issues'].append(f"Missing {len(missing_ids)} expected resource IDs")
        else:
            logger.info("✅ All expected resource IDs are present!")
        
        if extra_ids:
            logger.info(f"ℹ️  Extra resource IDs ({len(extra_ids)}): {sorted(extra_ids)}")
        
        # Silver-specific: Data Quality Score Analysis
        if 'data_quality_score' in df.columns:
            quality_scores = df['data_quality_score'].describe()
            validation_results['data_quality_scores'] = quality_scores.to_dict()
            
            logger.info(f"\n=== Data Quality Score Analysis ===")
            logger.info(f"Mean quality score: {quality_scores['mean']:.2f}")
            logger.info(f"Min quality score: {quality_scores['min']:.2f}")
            logger.info(f"Max quality score: {quality_scores['max']:.2f}")
            logger.info(f"Std deviation: {quality_scores['std']:.2f}")
            
            # Quality score distribution
            score_bins = pd.cut(df['data_quality_score'], bins=[0, 0.5, 0.7, 0.85, 1.0], 
                               labels=['Poor (0-0.5)', 'Fair (0.5-0.7)', 'Good (0.7-0.85)', 'Excellent (0.85-1.0)'])
            score_distribution = score_bins.value_counts()
            
            logger.info(f"\n=== Quality Score Distribution ===")
            for category, count in score_distribution.items():
                logger.info(f"  {category}: {count:,} records ({count/len(df)*100:.1f}%)")
        
        # Silver-specific: Completeness Score Analysis
        if 'completeness_score' in df.columns:
            completeness_scores = df['completeness_score'].describe()
            validation_results['completeness_scores'] = completeness_scores.to_dict()
            
            logger.info(f"\n=== Completeness Score Analysis ===")
            logger.info(f"Mean completeness score: {completeness_scores['mean']:.2f}")
            logger.info(f"Min completeness score: {completeness_scores['min']:.2f}")
            logger.info(f"Max completeness score: {completeness_scores['max']:.2f}")
            logger.info(f"Std deviation: {completeness_scores['std']:.2f}")
        
        # Silver-specific: Validation Status Analysis
        if 'validation_status' in df.columns:
            status_counts = df['validation_status'].value_counts()
            validation_results['validation_status_counts'] = status_counts.to_dict()
            
            logger.info(f"\n=== Validation Status Analysis ===")
            for status, count in status_counts.items():
                logger.info(f"  {status}: {count:,} records ({count/len(df)*100:.1f}%)")
        
        # Record counts per table ID
        resource_counts = df['table_id'].value_counts().sort_index()
        logger.info(f"\n=== Records per Table ID ===")
        for table_id, count in resource_counts.items():
            logger.info(f"  {table_id}: {count:,} records")
        
        # Data quality checks
        logger.info(f"\n=== Data Quality Checks ===")
        
        # Check for null values in key fields
        key_fields = ['table_id', 'title']
        for field in key_fields:
            if field in df.columns:
                null_count = df[field].isnull().sum()
                if null_count > 0:
                    logger.warning(f"⚠️  {field}: {null_count:,} nulls ({null_count/len(df)*100:.1f}%)")
                    validation_results['data_quality_issues'].append(f"Null values in {field}: {null_count}")
                else:
                    logger.info(f"✅ {field}: No null values")
        
        # Check for duplicate records
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            logger.warning(f"⚠️  Found {duplicates:,} duplicate records ({duplicates/len(df)*100:.1f}%)")
            validation_results['data_quality_issues'].append(f"Duplicate records: {duplicates}")
        else:
            logger.info("✅ No duplicate records found")
        
        # Silver-specific: Check for processing timestamp
        if 'processed_timestamp' in df.columns:
            logger.info(f"✅ Processing timestamps present")
            latest_processing = df['processed_timestamp'].max()
            earliest_processing = df['processed_timestamp'].min()
            logger.info(f"  Latest processing: {latest_processing}")
            logger.info(f"  Earliest processing: {earliest_processing}")
        else:
            logger.warning("⚠️  Missing processed_timestamp field")
            validation_results['data_quality_issues'].append("Missing processed_timestamp field")
        
        # Check data types
        logger.info(f"\n=== Data Types ===")
        for col, dtype in df.dtypes.items():
            if col not in ['silver_source_file', 'extraction_timestamp']:  # Skip metadata
                logger.info(f"  {col}: {dtype}")
        
        # Sample data preview
        logger.info(f"\n=== Sample Silver Data (first 3 rows) ===")
        sample_df = df.head(3)
        for i, row in sample_df.iterrows():
            logger.info(f"Row {i+1}:")
            # Show key fields only
            key_display_fields = ['table_id', 'title', 'data_quality_score', 'completeness_score', 'validation_status']
            for field in key_display_fields:
                if field in row:
                    logger.info(f"  {field}: {row[field]}")
            logger.info("")
        
        # Field coverage analysis
        actual_data_fields = [col for col in df.columns if col not in ['silver_source_file', 'extraction_timestamp']]
        
        logger.info(f"\n=== Field Coverage Analysis ===")
        logger.info(f"Expected silver fields: {len(expected_silver_fields)}")
        logger.info(f"Actual data fields: {len(actual_data_fields)}")
        logger.info(f"Actual fields: {sorted(actual_data_fields)}")
        
        missing_fields = set(expected_silver_fields) - set(actual_data_fields)
        extra_fields = set(actual_data_fields) - set(expected_silver_fields)
        
        if missing_fields:
            logger.warning(f"⚠️  Missing expected fields: {sorted(missing_fields)}")
            validation_results['data_quality_issues'].append(f"Missing fields: {sorted(missing_fields)}")
        
        if extra_fields:
            logger.info(f"ℹ️  Extra fields found: {sorted(extra_fields)}")
        
        # Save validation report
        validation_report = output_dir / "silver_validation_report.json"
        import json
        with open(validation_report, 'w') as f:
            json.dump(validation_results, f, indent=2, default=str)
        logger.info(f"Silver validation report saved to {validation_report}")
        
        # Final validation status
        if not validation_results['data_quality_issues']:
            logger.info("\n✅ Silver data validation PASSED - All checks successful!")
        else:
            logger.warning(f"\n⚠️  Silver data validation completed with {len(validation_results['data_quality_issues'])} issues")
            # Don't mark as failed for minor issues like duplicates or extra fields
            major_issues = [issue for issue in validation_results['data_quality_issues'] 
                          if 'Missing resource_id' in issue or 'Missing processed_timestamp' in issue]
            if major_issues:
                validation_results['validation_passed'] = False
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error during silver validation: {e}")
        validation_results['validation_passed'] = False
        validation_results['data_quality_issues'].append(f"Validation error: {str(e)}")
        return validation_results

def main():
    logger.info("=== SingStat Silver Data Extraction and Validation ===")
    
    success = extract_parquets_to_csv()
    
    if success:
        logger.info("\n✅ Silver extraction and validation completed successfully!")
        logger.info("📊 Silver CSV files are ready for analysis")
    else:
        logger.error("\n❌ Silver extraction and validation failed")
    
    return success

if __name__ == "__main__":
    main()