#!/usr/bin/env python3
"""
Extract URA Bronze Parquet Files to CSV and Validate
Extracts all parquet files from the URA bronze Delta Lake,
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
    """Extract all URA parquet files from MinIO and convert to CSV"""
    
    # Expected URA data fields based on PMI_Resi_Rental_Median source structure
    expected_fields = [
        # Fields from original URA API PMI_Resi_Rental_Median service
        'service_type', 'street_name', 'project_name', 'latitude', 'longitude', 'district',
        # Critical rental fields from rentalMedian array
        'rental_median', 'rental_psf25', 'rental_psf75', 'ref_period',
        # Metadata fields
        'source', 'timestamp', 'data_type', 'record_id', 'source_file', 'extraction_timestamp'
    ]
    
    try:
        # Initialize MinIO client
        initializer = MinIOBucketInitializer()
        if not initializer.wait_for_minio():
            logger.error("Failed to connect to MinIO")
            return False
        
        logger.info("Extracting URA parquet files from MinIO bronze bucket...")
        
        # Create output directory with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path(f"ura_csv_exports_{timestamp}")
        output_dir.mkdir(exist_ok=True)
        
        # List all parquet files in bronze/ura_geospatial/
        objects = list(initializer.client.list_objects('bronze', prefix='ura_geospatial/', recursive=True))
        
        # Filter to only actual data parquet files (exclude Delta Lake metadata)
        parquet_files = [
            obj for obj in objects 
            if obj.object_name.endswith('.parquet') 
            and not obj.object_name.endswith('.checkpoint.parquet')
            and '_delta_log' not in obj.object_name
            and 'part-' in obj.object_name  # Only actual data files
        ]
        
        logger.info(f"Found {len(parquet_files)} data parquet files to process")
        logger.info(f"Total objects in ura_geospatial/: {len(objects)}")
        
        if not parquet_files:
            logger.warning("No data parquet files found in bronze/ura_geospatial/")
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
                
                # Log data sample for inspection
                logger.info(f"  - Sample columns: {list(df.columns)[:10]}")
                if len(df) > 0:
                    logger.info(f"  - Sample data types: {dict(df.dtypes.head())}")
                
                if len(df) == 0:
                    logger.warning(f"  - No data rows found, skipping file")
                    continue
                
                # Extract URA data - data is already flattened in bronze layer
                flattened_rows = []
                for _, row in df.iterrows():
                    try:
                        # Create flattened record from direct columns (new bronze structure)
                        flattened_record = {
                            # Core URA fields - directly from columns
                            'service_type': row.get('service_type', ''),
                            'street_name': row.get('street_name', ''),
                            'project_name': row.get('project_name', ''),
                            'latitude': row.get('latitude', ''),
                            'longitude': row.get('longitude', ''),
                            'district': row.get('district', ''),
                            # Rental fields
                            'rental_median': row.get('rental_median', ''),
                            'rental_psf25': row.get('rental_psf25', ''),
                            'rental_psf75': row.get('rental_psf75', ''),
                            'ref_period': row.get('ref_period', ''),
                            # Metadata fields
                            'source': row.get('source', ''),
                            'timestamp': row.get('timestamp', ''),
                            'data_type': row.get('data_type', ''),
                            'record_id': row.get('record_id', ''),
                            # Additional metadata
                            'source_file': Path(obj.object_name).name,
                            'extraction_timestamp': datetime.now().isoformat()
                        }
                        flattened_rows.append(flattened_record)
                    except Exception as e:
                        logger.warning(f"  - Error processing row: {e}")
                        continue
                
                # Convert to DataFrame
                if flattened_rows:
                    df = pd.DataFrame(flattened_rows)
                    logger.info(f"  - Flattened to {len(df):,} rows with URA-specific fields")
                else:
                    logger.warning(f"  - No valid URA data found in file")
                    continue
                
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
        combined_csv = output_dir / "all_ura_data_combined.csv"
        combined_df.to_csv(combined_csv, index=False)
        logger.info(f"Saved combined data to {combined_csv}")
        
        # Generate validation report
        validation_results = validate_extracted_data(combined_df, expected_fields, output_dir)
        
        # Save validation report
        validation_file = output_dir / "bronze_validation_report.json"
        import json
        with open(validation_file, 'w') as f:
            json.dump(validation_results, f, indent=2)
        logger.info(f"Validation report saved to {validation_file}")
        
        # Print summary
        logger.info("\n=== URA Bronze Data Extraction Summary ===")
        logger.info(f"Files processed: {len(processed_files)}")
        logger.info(f"Total records: {len(combined_df):,}")
        logger.info(f"Total columns: {len(combined_df.columns)}")
        logger.info(f"Output directory: {output_dir}")
        
        if validation_results['validation_passed']:
            logger.info("✅ Data validation PASSED")
        else:
            logger.warning("⚠️  Data validation found issues")
            for issue in validation_results['issues']:
                logger.warning(f"  - {issue}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in extract_parquets_to_csv: {e}")
        return False

def validate_extracted_data(df, expected_fields, output_dir):
    """Validate the extracted URA bronze data"""
    
    validation_results = {
        'timestamp': datetime.now().isoformat(),
        'total_records': len(df),
        'validation_passed': True,
        'issues': []
    }
    
    try:
        # Check if dataframe is empty
        if df.empty:
            validation_results['validation_passed'] = False
            validation_results['issues'].append("No data found")
            return validation_results
        
        # Check columns
        actual_columns = set(df.columns.tolist())
        expected_columns = set(expected_fields)
        
        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns
        
        if missing_columns:
            validation_results['issues'].append(f"Missing columns: {list(missing_columns)}")
        
        if extra_columns:
            validation_results['issues'].append(f"Extra columns: {list(extra_columns)}")
        
        validation_results['columns_found'] = list(actual_columns)
        validation_results['columns_expected'] = list(expected_columns)
        
        # Check for null values in key fields - only fields that exist in PMI_Resi_Rental_Median
        key_fields = ['project_name', 'street_name']
        for field in key_fields:
            if field in df.columns:
                null_count = df[field].isnull().sum()
                if null_count > 0:
                    null_percentage = (null_count / len(df)) * 100
                    validation_results['issues'].append(
                        f"Field '{field}' has {null_count} null values ({null_percentage:.1f}%)"
                    )
        
        # Check for rental data completeness (critical for URA PMI_Resi_Rental_Median service)
        rental_fields = ['rental_median', 'rental_psf25', 'rental_psf75', 'ref_period']
        rental_data_found = False
        for field in rental_fields:
            if field in df.columns:
                non_empty_count = df[field].notna().sum()
                if non_empty_count > 0:
                    rental_data_found = True
                    break
        
        if not rental_data_found:
            validation_results['validation_passed'] = False
            validation_results['issues'].append(
                "CRITICAL: No rental data found in any rental fields. This indicates the rentalMedian array is not being processed correctly in the data pipeline."
            )
        
        # Validate rental data distribution if present
        if rental_data_found:
            for field in rental_fields:
                if field in df.columns:
                    non_empty_count = df[field].notna().sum()
                    if non_empty_count > 0:
                        coverage_percentage = (non_empty_count / len(df)) * 100
                        validation_results[f'{field}_coverage_percentage'] = coverage_percentage
                        logger.info(f"Rental field '{field}' coverage: {coverage_percentage:.1f}%")
        
        # Check property type distribution
        if 'property_type' in df.columns:
            property_counts = df['property_type'].value_counts()
            validation_results['property_type_distribution'] = property_counts.to_dict()
            logger.info(f"Property type distribution: {dict(property_counts)}")
        
        # Check coordinate data quality
        coord_fields = [('latitude', 'longitude'), ('x', 'y')]
        for lat_field, lon_field in coord_fields:
            if lat_field in df.columns and lon_field in df.columns:
                valid_coords = df[(df[lat_field].notna()) & (df[lon_field].notna()) & 
                                (df[lat_field] != '') & (df[lon_field] != '')]
                coord_percentage = (len(valid_coords) / len(df)) * 100
                validation_results[f'records_with_{lat_field}_{lon_field}'] = len(valid_coords)
                validation_results[f'{lat_field}_{lon_field}_coverage_percentage'] = coord_percentage
                
                if coord_percentage < 50:
                    validation_results['issues'].append(
                        f"Low coordinate coverage for {lat_field}/{lon_field}: only {coord_percentage:.1f}% of records have coordinates"
                    )
                break
        
        # Check date fields
        date_fields = ['lease_commence_date', 'built_year']
        for field in date_fields:
            if field in df.columns:
                valid_dates = df[df[field].notna() & (df[field] != '')]
                if len(valid_dates) > 0:
                    validation_results[f'{field}_records'] = len(valid_dates)
        
        # Generate data quality summary
        validation_results['data_quality_summary'] = {
            'total_records': len(df),
            'columns_count': len(df.columns),
            'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
        }
        
        # Save sample data for inspection
        if len(df) > 0:
            sample_size = min(100, len(df))
            sample_df = df.head(sample_size)
            sample_file = output_dir / "sample_data.csv"
            sample_df.to_csv(sample_file, index=False)
            logger.info(f"Sample data ({sample_size} records) saved to {sample_file}")
        
        # Generate comprehensive data pipeline validation report
        pipeline_report = {
            'data_pipeline_status': 'CRITICAL_ISSUES_DETECTED' if not rental_data_found else 'HEALTHY',
            'rental_data_extraction': {
                'status': 'FAILED' if not rental_data_found else 'SUCCESS',
                'expected_fields': rental_fields,
                'fields_found_in_bronze': [f for f in rental_fields if f in df.columns],
                'fields_missing_from_bronze': [f for f in rental_fields if f not in df.columns],
                'issue_description': 'The URA API rentalMedian array is not being properly processed in the bronze layer transformation. This suggests an issue in the ura_producer.py transform_data method.' if not rental_data_found else 'Rental data extraction is working correctly.'
            },
            'coordinate_mapping': {
                'latitude_longitude_available': 'latitude' in df.columns and 'longitude' in df.columns,
                'x_y_available': 'x' in df.columns and 'y' in df.columns,
                'mapping_status': 'SUCCESS' if ('latitude' in df.columns and 'longitude' in df.columns) else 'PARTIAL'
            },
            'field_mapping_status': {
                'project_to_project_name': 'project_name' in df.columns,
                'street_to_street_name': 'street_name' in df.columns,
                'basic_fields_mapped': True
            },
            'recommendations': []
        }
        
        if not rental_data_found:
            pipeline_report['recommendations'].extend([
                '1. Check ura_producer.py transform_data method for rentalMedian array processing',
                '2. Verify that the URA API response contains rentalMedian data',
                '3. Ensure the bronze layer ETL is extracting all fields from the rentalMedian array',
                '4. Re-run the URA data ingestion pipeline after fixing the transformation logic'
            ])
        
        validation_results['pipeline_validation_report'] = pipeline_report
        
        # Final validation check
        if len(validation_results['issues']) == 0:
            validation_results['validation_passed'] = True
        else:
            validation_results['validation_passed'] = False
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error in validation: {e}")
        validation_results['validation_passed'] = False
        validation_results['issues'].append(f"Validation error: {str(e)}")
        return validation_results

def main():
    logger.info("=== URA Bronze Data Extraction and Validation ===")
    
    success = extract_parquets_to_csv()
    
    if success:
        logger.info("\n✅ Extraction and validation completed successfully!")
        logger.info("📊 CSV files are ready for analysis")
    else:
        logger.error("\n❌ Extraction and validation failed")
    
    return success

if __name__ == "__main__":
    main()