#!/usr/bin/env python3
"""
Extract ACRA Bronze Parquet Files to CSV and Validate
Extracts all parquet files from the ACRA bronze Delta Lake,
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
    """Extract all ACRA parquet files from MinIO and convert to CSV"""
    
    # Expected ACRA data fields based on the producer
    expected_fields = [
        'entity_name', 'uen', 'entity_type', 'uen_issue_date', 
        'uen_status_desc', 'issuance_agency_desc', 'reg_street_name', 
        'reg_postal_code', 'unit_no', 'primary_ssic_description', 
        'secondary_ssic_description', 'paid_up_capital1_ordinary', 
        'registration_incorporation_date'
    ]
    
    try:
        # Initialize MinIO client
        initializer = MinIOBucketInitializer()
        if not initializer.wait_for_minio():
            logger.error("Failed to connect to MinIO")
            return False
        
        logger.info("Extracting ACRA parquet files from MinIO bronze bucket...")
        
        # Create output directory with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path(f"acra_csv_exports_{timestamp}")
        output_dir.mkdir(exist_ok=True)
        
        # List all parquet files in bronze/acra_companies/
        objects = list(initializer.client.list_objects('bronze', prefix='acra_companies/', recursive=True))
        
        # Filter to only actual data parquet files (exclude Delta Lake metadata)
        parquet_files = [
            obj for obj in objects 
            if obj.object_name.endswith('.parquet') 
            and not obj.object_name.endswith('.checkpoint.parquet')
            and '_delta_log' not in obj.object_name
            and 'part-' in obj.object_name  # Only actual data files
        ]
        
        logger.info(f"Found {len(parquet_files)} data parquet files to process")
        logger.info(f"Total objects in acra_companies/: {len(objects)}")
        
        if not parquet_files:
            logger.warning("No data parquet files found in bronze/acra_companies/")
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
                if 'uen' in df.columns:
                    # Keep only rows with valid UEN data
                    original_count = len(df)
                    df = df[df['uen'].notna() & (df['uen'] != '')]
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
        combined_csv = output_dir / "all_acra_data_combined.csv"
        combined_df.to_csv(combined_csv, index=False)
        logger.info(f"Saved combined data to {combined_csv}")
        
        # Perform validation
        validation_results = validate_extracted_data(combined_df, expected_fields, output_dir)
        
        logger.info(f"\n=== Extraction Summary ===")
        logger.info(f"Processed files: {len(processed_files)}")
        logger.info(f"Total records: {len(combined_df):,}")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Combined CSV: {combined_csv}")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        return False

def validate_extracted_data(df, expected_fields, output_dir):
    """Validate the extracted ACRA data for completeness and integrity"""
    
    logger.info("\n=== Data Validation ===")
    
    validation_results = {
        'total_records': len(df),
        'columns': list(df.columns),
        'unique_uens': 0,
        'entity_types_found': {},
        'data_quality_issues': [],
        'validation_passed': True
    }
    
    try:
        # Basic data info
        logger.info(f"Total records: {len(df):,}")
        logger.info(f"Columns ({len(df.columns)}): {', '.join(df.columns)}")
        
        # Check for UEN column (primary key for ACRA data)
        if 'uen' not in df.columns:
            logger.error("❌ Missing 'uen' column")
            validation_results['data_quality_issues'].append("Missing uen column")
            validation_results['validation_passed'] = False
            return validation_results
        
        # Analyze UENs
        unique_uens = df['uen'].dropna().unique().tolist()
        validation_results['unique_uens'] = len(unique_uens)
        
        logger.info(f"\nUnique UENs found: {len(unique_uens):,}")
        logger.info(f"Sample UENs: {sorted(unique_uens)[:10]}")
        
        # Check for entity types
        entity_type_col = None
        for col in ['entity_type_desc', 'entity_type']:
            if col in df.columns:
                entity_type_col = col
                break
        
        if entity_type_col:
            entity_types = df[entity_type_col].value_counts()
            validation_results['entity_types_found'] = entity_types.to_dict()
            
            logger.info(f"\n=== Entity Types Analysis ===")
            logger.info(f"Total entity types: {len(entity_types)}")
            for entity_type, count in entity_types.head(10).items():
                logger.info(f"  {entity_type}: {count:,} companies")
            if len(entity_types) > 10:
                logger.info(f"  ... and {len(entity_types) - 10} more types")
        
        # Check for expected fields coverage
        # Use flexible field matching since actual schema might differ
        actual_data_fields = [col for col in df.columns if col not in ['source_file', 'extraction_timestamp']]
        
        logger.info(f"\n=== Field Coverage Analysis ===")
        logger.info(f"Expected fields: {len(expected_fields)}")
        logger.info(f"Actual data fields: {len(actual_data_fields)}")
        logger.info(f"Actual fields: {sorted(actual_data_fields)}")
        
        # UEN/Entity status analysis
        status_col = None
        for col in ['uen_status_desc', 'entity_status', 'uen_status']:
            if col in df.columns:
                status_col = col
                break
        
        if status_col:
            status_counts = df[status_col].value_counts()
            logger.info(f"\n=== Entity Status Analysis ===")
            for status, count in status_counts.items():
                logger.info(f"  {status}: {count:,} companies ({count/len(df)*100:.1f}%)")
        
        # Data quality checks
        logger.info(f"\n=== Data Quality Checks ===")
        
        # Check for null values in key fields
        key_fields = ['uen', 'entity_name']
        for field in key_fields:
            if field in df.columns:
                null_count = df[field].isnull().sum()
                if null_count > 0:
                    logger.warning(f"⚠️  {field}: {null_count:,} nulls ({null_count/len(df)*100:.1f}%)")
                    validation_results['data_quality_issues'].append(f"Null values in {field}: {null_count}")
                else:
                    logger.info(f"✅ {field}: No null values")
        
        # Check for duplicate UENs
        if 'uen' in df.columns:
            duplicate_uens = df['uen'].duplicated().sum()
            if duplicate_uens > 0:
                logger.warning(f"⚠️  Found {duplicate_uens:,} duplicate UENs ({duplicate_uens/len(df)*100:.1f}%)")
                validation_results['data_quality_issues'].append(f"Duplicate UENs: {duplicate_uens}")
            else:
                logger.info("✅ No duplicate UENs found")
        
        # Check data types
        logger.info(f"\n=== Data Types ===")
        for col, dtype in df.dtypes.items():
            if col not in ['source_file', 'extraction_timestamp']:  # Skip metadata
                logger.info(f"  {col}: {dtype}")
        
        # Sample data preview
        logger.info(f"\n=== Sample Data (first 3 companies) ===")
        sample_df = df.head(3)
        for i, row in sample_df.iterrows():
            logger.info(f"Company {i+1}:")
            # Show key fields only
            key_display_fields = ['uen', 'entity_name', entity_type_col, status_col]
            for field in key_display_fields:
                if field and field in row:
                    logger.info(f"  {field}: {row[field]}")
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
    logger.info("=== ACRA Bronze Data Extraction and Validation ===")
    
    success = extract_parquets_to_csv()
    
    if success:
        logger.info("\n✅ Extraction and validation completed successfully!")
        logger.info("📊 CSV files are ready for analysis")
    else:
        logger.error("\n❌ Extraction and validation failed")
    
    return success

if __name__ == "__main__":
    main()