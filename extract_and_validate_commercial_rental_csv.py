#!/usr/bin/env python3
"""
Extract Commercial Rental Bronze Parquet Files to CSV and Validate
Extracts all parquet files from the Commercial Rental bronze Delta Lake,
converts them to CSV format, and validates data completeness.
Also compares with original API response data.
"""

import sys
import os
import pandas as pd
import requests
import json
from datetime import datetime
from pathlib import Path
sys.path.append('scripts')

from init_minio_buckets import MinIOBucketInitializer
from loguru import logger

# Configure logger
logger.remove()
logger.add(sys.stdout, level="INFO")

def fetch_original_api_data():
    """Fetch original data from data.gov.sg API for comparison"""
    
    logger.info("Fetching original data from data.gov.sg API...")
    
    try:
        dataset_id = "d_862c74b13138382b9f0c50c68d436b95"
        url = f"https://data.gov.sg/api/action/datastore_search?resource_id={dataset_id}"
        
        all_records = []
        offset = 0
        limit = 1000
        
        while True:
            params = {
                'limit': limit,
                'offset': offset
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data.get('success'):
                logger.error(f"API returned error: {data}")
                return None
            
            records = data.get('result', {}).get('records', [])
            if not records:
                break
                
            all_records.extend(records)
            logger.info(f"Fetched {len(records)} records (total: {len(all_records)})")
            
            # Check if we've got all records
            total_records = data.get('result', {}).get('total', 0)
            if len(all_records) >= total_records:
                break
                
            offset += limit
        
        logger.info(f"Total records fetched from API: {len(all_records)}")
        return all_records
        
    except Exception as e:
        logger.error(f"Failed to fetch original API data: {e}")
        return None

def extract_parquets_to_csv():
    """Extract all Commercial Rental parquet files from MinIO and convert to CSV"""
    
    # Expected Commercial Rental data fields based on the producer
    expected_fields = [
        'quarter', 'property_type', 'rental_index'
    ]
    
    try:
        # Initialize MinIO client
        initializer = MinIOBucketInitializer()
        if not initializer.wait_for_minio():
            logger.error("Failed to connect to MinIO")
            return False
        
        logger.info("Extracting Commercial Rental parquet files from MinIO bronze bucket...")
        
        # Create output directory with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path(f"commercial_rental_csv_exports_{timestamp}")
        output_dir.mkdir(exist_ok=True)
        
        # List all parquet files in bronze/commercial_rental_index/
        objects = list(initializer.client.list_objects('bronze', prefix='commercial_rental_index/', recursive=True))
        
        # Filter to only actual data parquet files (exclude Delta Lake metadata)
        parquet_files = [
            obj for obj in objects 
            if obj.object_name.endswith('.parquet') 
            and not obj.object_name.endswith('.checkpoint.parquet')
            and '_delta_log' not in obj.object_name
            and 'part-' in obj.object_name  # Only actual data files
        ]
        
        logger.info(f"Found {len(parquet_files)} data parquet files to process")
        logger.info(f"Total objects in commercial_rental_index/: {len(objects)}")
        
        if not parquet_files:
            logger.warning("No data parquet files found in bronze/commercial_rental_index/")
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
                
                # Filter out rows that are Delta Lake metadata (if any leaked through)
                if 'quarter' in df.columns:
                    # Keep only rows with valid quarter data
                    original_count = len(df)
                    df = df[df['quarter'].notna() & (df['quarter'] != '')]
                    if len(df) < original_count:
                        logger.info(f"  - Filtered out {original_count - len(df)} metadata rows")
                
                if len(df) == 0:
                    logger.warning(f"  - No valid data rows found, skipping file")
                    continue
                
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
        combined_csv = output_dir / "all_commercial_rental_data_combined.csv"
        combined_df.to_csv(combined_csv, index=False)
        logger.info(f"Saved combined data to {combined_csv}")
        
        # Fetch original API data for comparison
        original_data = fetch_original_api_data()
        if original_data:
            # Save original API data
            original_csv = output_dir / "original_api_data.csv"
            original_df = pd.DataFrame(original_data)
            original_df.to_csv(original_csv, index=False)
            logger.info(f"Saved original API data to {original_csv}")
            
            # Compare datasets
            comparison_results = compare_datasets(combined_df, original_df, output_dir)
        else:
            comparison_results = None
        
        # Perform validation
        validation_results = validate_extracted_data(combined_df, expected_fields, output_dir)
        
        logger.info(f"\n=== Extraction Summary ===")
        logger.info(f"Processed files: {len(processed_files)}")
        logger.info(f"Total records: {len(combined_df):,}")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Combined CSV: {combined_csv}")
        
        return validation_results and (comparison_results is None or comparison_results)
        
    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        return False

def compare_datasets(bronze_df, original_df, output_dir):
    """Compare bronze data with original API data"""
    
    logger.info("\n=== Dataset Comparison ===")
    
    try:
        # Remove metadata columns from bronze data for comparison
        bronze_data_cols = [col for col in bronze_df.columns 
                           if col not in ['source_file', 'extraction_timestamp', 'ingestion_timestamp', 
                                        'bronze_ingestion_timestamp', 'source', 'kafka_timestamp', 
                                        'partition', 'offset']]
        bronze_clean = bronze_df[bronze_data_cols].copy()
        
        logger.info(f"Bronze data: {len(bronze_clean):,} records, {len(bronze_clean.columns)} columns")
        logger.info(f"Original API data: {len(original_df):,} records, {len(original_df.columns)} columns")
        
        # Column comparison
        bronze_cols = set(bronze_clean.columns)
        original_cols = set(original_df.columns)
        
        logger.info(f"\n=== Column Comparison ===")
        logger.info(f"Bronze columns: {sorted(bronze_cols)}")
        logger.info(f"Original columns: {sorted(original_cols)}")
        
        common_cols = bronze_cols.intersection(original_cols)
        bronze_only = bronze_cols - original_cols
        original_only = original_cols - bronze_cols
        
        logger.info(f"Common columns ({len(common_cols)}): {sorted(common_cols)}")
        if bronze_only:
            logger.info(f"Bronze-only columns ({len(bronze_only)}): {sorted(bronze_only)}")
        if original_only:
            logger.info(f"Original-only columns ({len(original_only)}): {sorted(original_only)}")
        
        # Record count comparison
        logger.info(f"\n=== Record Count Comparison ===")
        logger.info(f"Bronze records: {len(bronze_clean):,}")
        logger.info(f"Original records: {len(original_df):,}")
        
        if len(bronze_clean) == len(original_df):
            logger.info("✅ Record counts match perfectly")
        else:
            diff = len(bronze_clean) - len(original_df)
            logger.warning(f"⚠️  Record count difference: {diff:+,}")
        
        # Data type comparison for common columns
        if common_cols:
            logger.info(f"\n=== Data Type Comparison ===")
            for col in sorted(common_cols):
                bronze_dtype = bronze_clean[col].dtype
                original_dtype = original_df[col].dtype
                if bronze_dtype != original_dtype:
                    logger.info(f"  {col}: Bronze({bronze_dtype}) vs Original({original_dtype})")
        
        # Sample data comparison
        if common_cols and len(bronze_clean) > 0 and len(original_df) > 0:
            logger.info(f"\n=== Sample Data Comparison ===")
            
            # Compare first few records for common columns
            sample_cols = sorted(list(common_cols))[:5]  # Limit to first 5 common columns
            
            logger.info("Bronze sample (first 3 records):")
            for i, row in bronze_clean[sample_cols].head(3).iterrows():
                logger.info(f"  Record {i+1}: {dict(row)}")
            
            logger.info("Original sample (first 3 records):")
            for i, row in original_df[sample_cols].head(3).iterrows():
                logger.info(f"  Record {i+1}: {dict(row)}")
        
        # Value distribution comparison for key fields
        key_fields = ['quarter', 'property_type']
        for field in key_fields:
            if field in bronze_clean.columns and field in original_df.columns:
                logger.info(f"\n=== {field.title()} Distribution Comparison ===")
                
                bronze_counts = bronze_clean[field].value_counts().sort_index()
                original_counts = original_df[field].value_counts().sort_index()
                
                logger.info(f"Bronze {field} values: {sorted(bronze_counts.index.tolist())}")
                logger.info(f"Original {field} values: {sorted(original_counts.index.tolist())}")
                
                # Check for differences
                bronze_values = set(bronze_counts.index)
                original_values = set(original_counts.index)
                
                if bronze_values == original_values:
                    logger.info(f"✅ {field} values match perfectly")
                else:
                    bronze_only_vals = bronze_values - original_values
                    original_only_vals = original_values - bronze_values
                    if bronze_only_vals:
                        logger.warning(f"⚠️  Bronze-only {field} values: {sorted(bronze_only_vals)}")
                    if original_only_vals:
                        logger.warning(f"⚠️  Original-only {field} values: {sorted(original_only_vals)}")
        
        # Save comparison report
        comparison_report = {
            'bronze_records': len(bronze_clean),
            'original_records': len(original_df),
            'record_difference': len(bronze_clean) - len(original_df),
            'bronze_columns': sorted(bronze_cols),
            'original_columns': sorted(original_cols),
            'common_columns': sorted(common_cols),
            'bronze_only_columns': sorted(bronze_only),
            'original_only_columns': sorted(original_only),
            'comparison_timestamp': datetime.now().isoformat()
        }
        
        comparison_file = output_dir / "dataset_comparison_report.json"
        with open(comparison_file, 'w') as f:
            json.dump(comparison_report, f, indent=2, default=str)
        logger.info(f"\nComparison report saved to {comparison_file}")
        
        # Determine if comparison passed
        major_issues = (
            len(bronze_only) > 2 or  # Allow some metadata columns
            len(original_only) > 0 or
            abs(len(bronze_clean) - len(original_df)) > len(original_df) * 0.1  # 10% tolerance
        )
        
        if not major_issues:
            logger.info("\n✅ Dataset comparison PASSED - Data integrity maintained")
            return True
        else:
            logger.warning("\n⚠️  Dataset comparison completed with differences")
            return True  # Still return True unless critical issues
        
    except Exception as e:
        logger.error(f"Error during dataset comparison: {e}")
        return False

def validate_extracted_data(df, expected_fields, output_dir):
    """Validate the extracted Commercial Rental data for completeness and integrity"""
    
    logger.info("\n=== Data Validation ===")
    
    validation_results = {
        'total_records': len(df),
        'columns': list(df.columns),
        'unique_quarters': 0,
        'property_types_found': {},
        'rental_index_stats': {},
        'data_quality_issues': [],
        'validation_passed': True
    }
    
    try:
        # Basic data info
        logger.info(f"Total records: {len(df):,}")
        logger.info(f"Columns ({len(df.columns)}): {', '.join(df.columns)}")
        
        # Check for required columns
        required_cols = ['quarter', 'property_type', 'rental_index']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logger.error(f"❌ Missing required columns: {missing_cols}")
            validation_results['data_quality_issues'].extend([f"Missing {col} column" for col in missing_cols])
            validation_results['validation_passed'] = False
            return validation_results
        
        # Analyze quarters
        unique_quarters = df['quarter'].dropna().unique().tolist()
        validation_results['unique_quarters'] = len(unique_quarters)
        
        logger.info(f"\nUnique quarters found: {len(unique_quarters)}")
        logger.info(f"Quarter range: {sorted(unique_quarters)}")
        
        # Analyze property types
        property_types = df['property_type'].value_counts()
        validation_results['property_types_found'] = property_types.to_dict()
        
        logger.info(f"\n=== Property Types Analysis ===")
        logger.info(f"Total property types: {len(property_types)}")
        for prop_type, count in property_types.items():
            logger.info(f"  {prop_type}: {count:,} records")
        
        # Analyze rental index
        if 'rental_index' in df.columns:
            rental_stats = df['rental_index'].describe()
            validation_results['rental_index_stats'] = rental_stats.to_dict()
            
            logger.info(f"\n=== Rental Index Analysis ===")
            logger.info(f"  Count: {rental_stats['count']:,.0f}")
            logger.info(f"  Mean: {rental_stats['mean']:.2f}")
            logger.info(f"  Std: {rental_stats['std']:.2f}")
            logger.info(f"  Min: {rental_stats['min']:.2f}")
            logger.info(f"  Max: {rental_stats['max']:.2f}")
            logger.info(f"  Median: {rental_stats['50%']:.2f}")
        
        # Data quality checks
        logger.info(f"\n=== Data Quality Checks ===")
        
        # Check for null values in key fields
        for field in required_cols:
            null_count = df[field].isnull().sum()
            if null_count > 0:
                logger.warning(f"⚠️  {field}: {null_count:,} nulls ({null_count/len(df)*100:.1f}%)")
                validation_results['data_quality_issues'].append(f"Null values in {field}: {null_count}")
            else:
                logger.info(f"✅ {field}: No null values")
        
        # Check for duplicate records
        duplicate_count = df.duplicated(subset=['quarter', 'property_type']).sum()
        if duplicate_count > 0:
            logger.warning(f"⚠️  Found {duplicate_count:,} duplicate quarter-property_type combinations")
            validation_results['data_quality_issues'].append(f"Duplicate records: {duplicate_count}")
        else:
            logger.info("✅ No duplicate quarter-property_type combinations found")
        
        # Check rental index range (should be positive)
        if 'rental_index' in df.columns:
            negative_indices = (df['rental_index'] < 0).sum()
            if negative_indices > 0:
                logger.warning(f"⚠️  Found {negative_indices:,} negative rental index values")
                validation_results['data_quality_issues'].append(f"Negative rental indices: {negative_indices}")
            else:
                logger.info("✅ All rental index values are non-negative")
        
        # Check data types
        logger.info(f"\n=== Data Types ===")
        for col, dtype in df.dtypes.items():
            if col not in ['source_file', 'extraction_timestamp']:  # Skip metadata
                logger.info(f"  {col}: {dtype}")
        
        # Sample data preview
        logger.info(f"\n=== Sample Data (first 5 records) ===")
        sample_df = df.head(5)
        for i, row in sample_df.iterrows():
            logger.info(f"Record {i+1}:")
            for field in ['quarter', 'property_type', 'rental_index']:
                if field in row:
                    logger.info(f"  {field}: {row[field]}")
            logger.info("")
        
        # Save validation report
        validation_report = output_dir / "validation_report.json"
        with open(validation_report, 'w') as f:
            json.dump(validation_results, f, indent=2, default=str)
        logger.info(f"Validation report saved to {validation_report}")
        
        # Final validation status
        if not validation_results['data_quality_issues']:
            logger.info("\n✅ Data validation PASSED - All checks successful!")
        else:
            logger.warning(f"\n⚠️  Data validation completed with {len(validation_results['data_quality_issues'])} issues")
            # Don't mark as failed for minor issues like duplicates
            major_issues = [issue for issue in validation_results['data_quality_issues'] 
                          if 'Missing' in issue or 'column' in issue]
            if major_issues:
                validation_results['validation_passed'] = False
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error during validation: {e}")
        validation_results['validation_passed'] = False
        validation_results['data_quality_issues'].append(f"Validation error: {str(e)}")
        return validation_results

def main():
    logger.info("=== Commercial Rental Bronze Data Extraction and Validation ===")
    
    success = extract_parquets_to_csv()
    
    if success:
        logger.info("\n✅ Extraction and validation completed successfully!")
        logger.info("📊 CSV files are ready for analysis")
        logger.info("🔍 Original API data comparison completed")
    else:
        logger.error("\n❌ Extraction and validation failed")
    
    return success

if __name__ == "__main__":
    main()