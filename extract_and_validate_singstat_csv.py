#!/usr/bin/env python3
"""
Extract SingStat Bronze Parquet Files to CSV and Validate
Extracts all parquet files from the SingStat bronze Delta Lake,
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
    """Extract all SingStat parquet files from MinIO and convert to CSV"""
    
    # Expected resource IDs from SingStatProducer
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
    
    try:
        # Initialize MinIO client
        initializer = MinIOBucketInitializer()
        if not initializer.wait_for_minio():
            logger.error("Failed to connect to MinIO")
            return False
        
        logger.info("Extracting SingStat parquet files from MinIO bronze bucket...")
        
        # Create output directory with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path(f"singstat_csv_exports_{timestamp}")
        output_dir.mkdir(exist_ok=True)
        
        # List all parquet files in bronze/singstat_economics/
        objects = list(initializer.client.list_objects('bronze', prefix='singstat_economics/', recursive=True))
        parquet_files = [obj for obj in objects if obj.object_name.endswith('.parquet')]
        
        logger.info(f"Found {len(parquet_files)} parquet files to process")
        
        if not parquet_files:
            logger.warning("No parquet files found in bronze/singstat_economics/")
            return False
        
        # Download and process each parquet file
        all_dataframes = []
        processed_files = []
        
        for i, obj in enumerate(parquet_files, 1):
            logger.info(f"Processing file {i}/{len(parquet_files)}: {obj.object_name}")
            
            try:
                # Download parquet file to temporary location
                temp_file = f"/tmp/{Path(obj.object_name).name}"
                initializer.client.fget_object('bronze', obj.object_name, temp_file)
                
                # Read parquet file with pandas
                df = pd.read_parquet(temp_file)
                logger.info(f"  - Loaded {len(df):,} rows, {len(df.columns)} columns")
                
                # Add file source information
                df['source_file'] = Path(obj.object_name).name
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
            logger.error("No parquet files were successfully processed")
            return False
        
        # Combine all dataframes
        logger.info("Combining all data into master CSV...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Save combined CSV
        combined_csv = output_dir / "all_singstat_data_combined.csv"
        combined_df.to_csv(combined_csv, index=False)
        logger.info(f"Saved combined data to {combined_csv}")
        
        # Perform validation
        validation_results = validate_extracted_data(combined_df, expected_resource_ids, output_dir)
        
        logger.info(f"\n=== Extraction Summary ===")
        logger.info(f"Processed files: {len(processed_files)}")
        logger.info(f"Total records: {len(combined_df):,}")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Combined CSV: {combined_csv}")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        return False

def validate_extracted_data(df, expected_resource_ids, output_dir):
    """Validate the extracted SingStat data for completeness and integrity"""
    
    logger.info("\n=== Data Validation ===")
    
    validation_results = {
        'total_records': len(df),
        'columns': list(df.columns),
        'resource_ids_found': [],
        'missing_resource_ids': [],
        'extra_resource_ids': [],
        'data_quality_issues': [],
        'validation_passed': True
    }
    
    try:
        # Basic data info
        logger.info(f"Total records: {len(df):,}")
        logger.info(f"Columns ({len(df.columns)}): {', '.join(df.columns)}")
        
        # Check for resource_id column
        if 'resource_id' not in df.columns:
            logger.error("❌ Missing 'resource_id' column")
            validation_results['data_quality_issues'].append("Missing resource_id column")
            validation_results['validation_passed'] = False
            return validation_results
        
        # Analyze resource IDs
        unique_resource_ids = df['resource_id'].unique().tolist()
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
        
        # Record counts per resource ID
        resource_counts = df['resource_id'].value_counts().sort_index()
        logger.info(f"\n=== Records per Resource ID ===")
        for resource_id, count in resource_counts.items():
            logger.info(f"  {resource_id}: {count:,} records")
        
        # Data quality checks
        logger.info(f"\n=== Data Quality Checks ===")
        
        # Check for null values
        null_counts = df.isnull().sum()
        if null_counts.sum() > 0:
            logger.warning(f"⚠️  Found null values:")
            for col, count in null_counts[null_counts > 0].items():
                logger.warning(f"    {col}: {count:,} nulls ({count/len(df)*100:.1f}%)")
                validation_results['data_quality_issues'].append(f"Null values in {col}: {count}")
        else:
            logger.info("✅ No null values found")
        
        # Check for duplicate records
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            logger.warning(f"⚠️  Found {duplicates:,} duplicate records ({duplicates/len(df)*100:.1f}%)")
            validation_results['data_quality_issues'].append(f"Duplicate records: {duplicates}")
        else:
            logger.info("✅ No duplicate records found")
        
        # Check data types
        logger.info(f"\n=== Data Types ===")
        for col, dtype in df.dtypes.items():
            logger.info(f"  {col}: {dtype}")
        
        # Sample data preview
        logger.info(f"\n=== Sample Data (first 3 rows) ===")
        sample_df = df.head(3)
        for i, row in sample_df.iterrows():
            logger.info(f"Row {i+1}:")
            for col, val in row.items():
                logger.info(f"  {col}: {val}")
            logger.info("")
        
        # Save validation report
        validation_report = output_dir / "validation_report.json"
        import json
        with open(validation_report, 'w') as f:
            json.dump(validation_results, f, indent=2, default=str)
        logger.info(f"Validation report saved to {validation_report}")
        
        # Final validation status
        if not validation_results['data_quality_issues']:
            logger.info("\n✅ Data validation PASSED - All checks successful!")
        else:
            logger.warning(f"\n⚠️  Data validation completed with {len(validation_results['data_quality_issues'])} issues")
            validation_results['validation_passed'] = False
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error during validation: {e}")
        validation_results['validation_passed'] = False
        validation_results['data_quality_issues'].append(f"Validation error: {str(e)}")
        return validation_results

def main():
    logger.info("=== SingStat Bronze Data Extraction and Validation ===")
    
    success = extract_parquets_to_csv()
    
    if success:
        logger.info("\n✅ Extraction and validation completed successfully!")
        logger.info("📊 CSV files are ready for analysis")
    else:
        logger.error("\n❌ Extraction and validation failed")
    
    return success

if __name__ == "__main__":
    main()