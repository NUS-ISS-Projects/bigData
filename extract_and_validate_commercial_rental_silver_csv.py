#!/usr/bin/env python3
"""
Extract Commercial Rental Silver Parquet Files to CSV and Validate
Extracts all parquet files from the Commercial Rental silver Delta Lake,
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

def extract_parquets_to_csv():
    """Extract all Commercial Rental silver parquet files from MinIO and convert to CSV"""
    
    # Expected Commercial Rental silver data fields (cleaned and standardized from bronze)
    expected_fields = [
        'quarter', 'property_type', 'rental_index', 'base_value', 'base_period',
        'quarter_clean', 'property_type_clean', 'rental_index_numeric', 
        'base_value_numeric', 'base_period_clean', 'quarter_year',
        'data_quality_score', 'processed_timestamp', 'bronze_source_file'
    ]
    
    try:
        # Initialize MinIO client
        initializer = MinIOBucketInitializer()
        if not initializer.wait_for_minio():
            logger.error("Failed to connect to MinIO")
            return False
        
        logger.info("Extracting Commercial Rental silver parquet files from MinIO silver bucket...")
        
        # Create output directory with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path(f"commercial_rental_silver_csv_exports_{timestamp}")
        output_dir.mkdir(exist_ok=True)
        
        # List all parquet files in silver/commercial_rental_index_clean/
        objects = list(initializer.client.list_objects('silver', prefix='commercial_rental_index_clean/', recursive=True))
        
        # Filter to only actual data parquet files (exclude Delta Lake metadata)
        parquet_files = [
            obj for obj in objects 
            if obj.object_name.endswith('.parquet') 
            and not obj.object_name.endswith('.checkpoint.parquet')
            and '_delta_log' not in obj.object_name
            and 'part-' in obj.object_name  # Only actual data files
        ]
        
        logger.info(f"Found {len(parquet_files)} silver data parquet files to process")
        logger.info(f"Total objects in silver/commercial_rental_index_clean/: {len(objects)}")
        
        if not parquet_files:
            logger.warning("No silver data parquet files found in silver/commercial_rental_index_clean/")
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
        combined_csv = output_dir / "all_commercial_rental_silver_data_combined.csv"
        combined_df.to_csv(combined_csv, index=False)
        logger.info(f"Saved combined silver data to {combined_csv}")
        
        # Fetch original API data for comparison
        original_data = fetch_original_api_data(output_dir)
        
        # Perform validation
        validation_results = validate_extracted_data(combined_df, expected_fields, output_dir, original_data)
        
        logger.info(f"\n=== Silver Extraction Summary ===")
        logger.info(f"Processed files: {len(processed_files)}")
        logger.info(f"Total records: {len(combined_df):,}")
        logger.info(f"Output directory: {output_dir}")
        logger.info(f"Combined CSV: {combined_csv}")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error during silver extraction: {e}")
        return False

def fetch_original_api_data(output_dir):
    """Fetch original Commercial Rental API data for comparison"""
    
    logger.info("\n=== Fetching Original API Data for Comparison ===")
    
    try:
        # Commercial Rental Index API endpoint
        api_url = "https://www.ura.gov.sg/uraDataService/invokeUraDS"
        params = {
            'service': 'PMI_Resi_Rental',
            'batch': '1'
        }
        
        headers = {
            'AccessKey': 'e5ca5a65-1f4b-4f8c-9c5e-2c5c5c5c5c5c',  # Demo key
            'User-Agent': 'Economic Intelligence Platform'
        }
        
        logger.info(f"Fetching data from: {api_url}")
        response = requests.get(api_url, params=params, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # Extract rental data
            rental_data = []
            if 'Result' in data:
                for item in data['Result']:
                    rental_data.append({
                        'quarter': item.get('quarter', ''),
                        'property_type': item.get('propertyType', ''),
                        'rental_index': item.get('rentalIndex', ''),
                        'base_value': item.get('baseValue', ''),
                        'base_period': item.get('basePeriod', '')
                    })
            
            # Save original API data
            original_df = pd.DataFrame(rental_data)
            original_csv = output_dir / "original_api_data.csv"
            original_df.to_csv(original_csv, index=False)
            
            logger.info(f"Original API data saved to {original_csv}")
            logger.info(f"Original API records: {len(original_df):,}")
            
            return original_df
            
        else:
            logger.warning(f"Failed to fetch original API data: HTTP {response.status_code}")
            return None
            
    except Exception as e:
        logger.warning(f"Could not fetch original API data: {e}")
        return None

def validate_extracted_data(df, expected_fields, output_dir, original_data=None):
    """Validate the extracted Commercial Rental silver data for completeness and integrity"""
    
    logger.info("\n=== Silver Data Validation ===")
    
    validation_results = {
        'total_records': len(df),
        'columns': list(df.columns),
        'unique_quarters': 0,
        'property_types_found': {},
        'data_quality_scores': {},
        'rental_index_stats': {},
        'data_quality_issues': [],
        'validation_passed': True
    }
    
    try:
        # Basic data info
        logger.info(f"Total silver records: {len(df):,}")
        logger.info(f"Columns ({len(df.columns)}): {', '.join(df.columns)}")
        
        # Check for quarter column (primary dimension for Commercial Rental data)
        if 'quarter' not in df.columns:
            logger.error("❌ Missing 'quarter' column")
            validation_results['data_quality_issues'].append("Missing quarter column")
            validation_results['validation_passed'] = False
            return validation_results
        
        # Analyze quarters
        unique_quarters = df['quarter'].dropna().unique().tolist()
        validation_results['unique_quarters'] = len(unique_quarters)
        
        logger.info(f"\nUnique quarters found: {len(unique_quarters):,}")
        logger.info(f"Sample quarters: {sorted(unique_quarters)[:10]}")
        
        # Check for property types
        property_type_col = None
        for col in ['property_type', 'property_type_clean']:
            if col in df.columns:
                property_type_col = col
                break
        
        if property_type_col:
            property_types = df[property_type_col].value_counts()
            validation_results['property_types_found'] = property_types.to_dict()
            
            logger.info(f"\n=== Property Types Analysis ===")
            logger.info(f"Total property types: {len(property_types)}")
            for prop_type, count in property_types.items():
                logger.info(f"  {prop_type}: {count:,} records")
        
        # Silver-specific: Data Quality Score Analysis
        if 'data_quality_score' in df.columns:
            quality_scores = df['data_quality_score'].describe()
            validation_results['data_quality_scores'] = quality_scores.to_dict()
            
            logger.info(f"\n=== Data Quality Score Analysis ===")
            logger.info(f"Mean quality score: {quality_scores['mean']:.2f}")
            logger.info(f"Min quality score: {quality_scores['min']:.2f}")
            logger.info(f"Max quality score: {quality_scores['max']:.2f}")
        
        # Rental Index Analysis
        rental_index_col = None
        for col in ['rental_index', 'rental_index_numeric']:
            if col in df.columns:
                rental_index_col = col
                break
        
        if rental_index_col:
            # Convert to numeric if needed
            if df[rental_index_col].dtype == 'object':
                numeric_values = pd.to_numeric(df[rental_index_col], errors='coerce')
            else:
                numeric_values = df[rental_index_col]
            
            rental_stats = numeric_values.describe()
            validation_results['rental_index_stats'] = rental_stats.to_dict()
            
            logger.info(f"\n=== Rental Index Analysis ===")
            logger.info(f"Mean rental index: {rental_stats['mean']:.2f}")
            logger.info(f"Min rental index: {rental_stats['min']:.2f}")
            logger.info(f"Max rental index: {rental_stats['max']:.2f}")
            logger.info(f"Null values: {numeric_values.isna().sum():,}")
        
        # Compare with original API data if available
        if original_data is not None and len(original_data) > 0:
            logger.info(f"\n=== Comparison with Original API Data ===")
            
            comparison_report = {
                'silver_records': len(df),
                'original_records': len(original_data),
                'record_difference': len(df) - len(original_data),
                'silver_columns': list(df.columns),
                'original_columns': list(original_data.columns),
                'common_columns': list(set(df.columns) & set(original_data.columns)),
                'silver_only_columns': list(set(df.columns) - set(original_data.columns)),
                'original_only_columns': list(set(original_data.columns) - set(df.columns))
            }
            
            logger.info(f"Silver records: {comparison_report['silver_records']:,}")
            logger.info(f"Original records: {comparison_report['original_records']:,}")
            logger.info(f"Difference: {comparison_report['record_difference']:+,}")
            
            logger.info(f"\nColumn comparison:")
            logger.info(f"  Common columns: {len(comparison_report['common_columns'])}")
            logger.info(f"  Silver-only columns: {len(comparison_report['silver_only_columns'])}")
            logger.info(f"  Original-only columns: {len(comparison_report['original_only_columns'])}")
            
            if comparison_report['silver_only_columns']:
                logger.info(f"  Silver enriched fields: {', '.join(comparison_report['silver_only_columns'])}")
            
            # Save comparison report
            comparison_file = output_dir / "dataset_comparison_report.json"
            with open(comparison_file, 'w') as f:
                json.dump(comparison_report, f, indent=2, default=str)
            logger.info(f"Comparison report saved to {comparison_file}")
        
        # Check for missing expected fields
        missing_fields = [field for field in expected_fields if field not in df.columns]
        if missing_fields:
            logger.warning(f"Missing expected silver fields: {', '.join(missing_fields)}")
            validation_results['data_quality_issues'].extend(missing_fields)
        
        # Final validation status
        if len(validation_results['data_quality_issues']) == 0:
            logger.info("\n✅ Silver data validation passed!")
        else:
            logger.warning(f"\n⚠️  Silver data validation completed with {len(validation_results['data_quality_issues'])} issues")
            validation_results['validation_passed'] = False
        
        # Save validation results
        validation_file = output_dir / "silver_validation_results.json"
        with open(validation_file, 'w') as f:
            json.dump(validation_results, f, indent=2, default=str)
        logger.info(f"Validation results saved to {validation_file}")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error during silver validation: {e}")
        validation_results['validation_passed'] = False
        validation_results['data_quality_issues'].append(f"Validation error: {str(e)}")
        return validation_results

def main():
    logger.info("=== Commercial Rental Silver Data Extraction and Validation ===")
    
    success = extract_parquets_to_csv()
    
    if success:
        logger.info("\n✅ Silver extraction and validation completed successfully!")
        logger.info("📊 CSV files are ready for analysis")
    else:
        logger.error("\n❌ Silver extraction and validation failed")
    
    return success

if __name__ == "__main__":
    main()