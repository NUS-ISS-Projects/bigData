#!/usr/bin/env python3
"""
Extract URA Silver Parquet Files to CSV and Validate
Extracts all parquet files from the URA silver Delta Lake,
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
    """Extract all URA silver parquet files from MinIO and convert to CSV"""
    
    # Expected silver data fields based on ETL transformation
    expected_silver_fields = [
        'project', 'street', 'x_coordinate', 'y_coordinate', 'has_coordinates',
        'lease_commence_date', 'property_type', 'district', 'tenure', 'built_year',
        # Critical rental fields that should be propagated from bronze
        'rental_median', 'rental_psf25', 'rental_psf75', 'ref_period',
        'data_quality_score', 'source', 'ingestion_timestamp', 'bronze_ingestion_timestamp',
        'silver_processed_timestamp'
    ]
    
    try:
        # Initialize MinIO client
        logger.info("Initializing MinIO client...")
        bucket_init = MinIOBucketInitializer()
        bucket_init.wait_for_minio()
        client = bucket_init.client
        
        # List all objects in the URA silver bucket
        logger.info("Listing URA silver parquet files...")
        objects = list(client.list_objects('silver', prefix='ura_geospatial_clean/', recursive=True))
        parquet_files = [obj for obj in objects if obj.object_name.endswith('.parquet')]
        
        if not parquet_files:
            logger.warning("No URA silver parquet files found!")
            logger.info("\n=== URA Silver Layer Status ===")
            logger.info("❌ No URA data found in silver layer")
            logger.info("📋 This indicates that the bronze-to-silver ETL pipeline has not been executed for URA data")
            logger.info("\n🔧 Required Actions:")
            logger.info("1. Ensure URA bronze data is available and valid")
            logger.info("2. Run the bronze-to-silver ETL transformation pipeline")
            logger.info("3. Verify the ETL pipeline includes rental data propagation")
            logger.info("4. Re-run this validation script after ETL completion")
            
            # Create a validation report for missing silver data
            validation_report = {
                'timestamp': datetime.now().isoformat(),
                'silver_data_status': 'MISSING',
                'validation_passed': False,
                'critical_issues': [
                    'No URA silver parquet files found',
                    'Bronze-to-silver ETL pipeline not executed',
                    'Cannot validate rental data propagation without silver data'
                ],
                'pipeline_status': 'INCOMPLETE',
                'recommendations': [
                    'Execute bronze-to-silver ETL pipeline for URA data',
                    'Ensure rental fields are included in the transformation',
                    'Verify coordinate transformation from lat/lng to x_coordinate/y_coordinate',
                    'Add data quality scoring in the silver transformation'
                ]
            }
            
            # Save the validation report
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_dir = f"ura_silver_validation_{timestamp}"
            os.makedirs(output_dir, exist_ok=True)
            
            report_file = os.path.join(output_dir, 'silver_missing_data_report.json')
            import json
            with open(report_file, 'w') as f:
                json.dump(validation_report, f, indent=2)
            
            logger.info(f"\n📄 Missing data report saved to: {report_file}")
            return None, None
        
        logger.info(f"Found {len(parquet_files)} URA silver parquet files")
        
        # Create output directory
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = f"ura_silver_csv_exports_{timestamp}"
        os.makedirs(output_dir, exist_ok=True)
        
        # Download and process each parquet file
        all_dataframes = []
        processed_files = 0
        
        for obj in parquet_files:
            try:
                logger.info(f"Processing: {obj.object_name}")
                
                # Download parquet file
                local_path = os.path.join(output_dir, os.path.basename(obj.object_name))
                client.fget_object('silver', obj.object_name, local_path)
                
                # Read parquet file
                df = pd.read_parquet(local_path)
                
                if not df.empty:
                    all_dataframes.append(df)
                    processed_files += 1
                    
                    # Convert to CSV
                    csv_path = local_path.replace('.parquet', '.csv')
                    df.to_csv(csv_path, index=False)
                    logger.info(f"Converted to CSV: {csv_path} ({len(df)} records)")
                
                # Clean up parquet file
                os.remove(local_path)
                
            except Exception as e:
                logger.error(f"Error processing {obj.object_name}: {e}")
                continue
        
        if not all_dataframes:
            logger.warning("No valid data found in URA silver files")
            return None, None
        
        # Combine all dataframes
        logger.info("Combining all URA silver data...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Save combined CSV
        combined_csv_path = os.path.join(output_dir, 'ura_silver_combined.csv')
        combined_df.to_csv(combined_csv_path, index=False)
        
        logger.info(f"Successfully processed {processed_files} files")
        logger.info(f"Total records: {len(combined_df)}")
        logger.info(f"Combined CSV saved: {combined_csv_path}")
        
        return combined_df, output_dir
        
    except Exception as e:
        logger.error(f"Error in extract_parquets_to_csv: {e}")
        return None, None

def validate_extracted_data(df, expected_silver_fields, output_dir):
    """Validate the extracted URA silver data"""
    
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
        expected_columns = set(expected_silver_fields)
        
        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns
        
        if missing_columns:
            validation_results['issues'].append(f"Missing columns: {list(missing_columns)}")
        
        if extra_columns:
            validation_results['issues'].append(f"Extra columns: {list(extra_columns)}")
        
        validation_results['columns_found'] = list(actual_columns)
        validation_results['columns_expected'] = list(expected_columns)
        
        # Check for null values in key fields
        key_fields = ['project', 'district', 'property_type']
        for field in key_fields:
            if field in df.columns:
                null_count = df[field].isnull().sum()
                if null_count > 0:
                    validation_results['issues'].append(f"Field '{field}' has {null_count} null values")
        
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
                "CRITICAL: No rental data found in silver layer. This indicates the rental data was not properly propagated from bronze to silver."
            )
        
        # Validate rental data distribution if present
        if rental_data_found:
            for field in rental_fields:
                if field in df.columns:
                    non_empty_count = df[field].notna().sum()
                    if non_empty_count > 0:
                        coverage_percentage = (non_empty_count / len(df)) * 100
                        validation_results[f'{field}_coverage_percentage'] = coverage_percentage
                        logger.info(f"Silver rental field '{field}' coverage: {coverage_percentage:.1f}%")
        
        # Check data types
        if 'x_coordinate' in df.columns:
            non_numeric_x = df[df['x_coordinate'].notna()]['x_coordinate'].apply(lambda x: not isinstance(x, (int, float))).sum()
            if non_numeric_x > 0:
                validation_results['issues'].append(f"x_coordinate has {non_numeric_x} non-numeric values")
        
        if 'y_coordinate' in df.columns:
            non_numeric_y = df[df['y_coordinate'].notna()]['y_coordinate'].apply(lambda x: not isinstance(x, (int, float))).sum()
            if non_numeric_y > 0:
                validation_results['issues'].append(f"y_coordinate has {non_numeric_y} non-numeric values")
        
        # Check coordinate availability
        if 'has_coordinates' in df.columns:
            coord_stats = df['has_coordinates'].value_counts().to_dict()
            validation_results['coordinate_stats'] = coord_stats
        
        # Check property types
        if 'property_type' in df.columns:
            property_types = df['property_type'].value_counts().to_dict()
            validation_results['property_types'] = property_types
        
        # Check districts
        if 'district' in df.columns:
            districts = df['district'].value_counts().to_dict()
            validation_results['districts'] = districts
        
        # Check data quality scores
        if 'data_quality_score' in df.columns:
            quality_stats = df['data_quality_score'].describe().to_dict()
            validation_results['data_quality_stats'] = quality_stats
        
        # Generate comprehensive silver layer pipeline validation report
        pipeline_report = {
            'silver_pipeline_status': 'CRITICAL_ISSUES_DETECTED' if not rental_data_found else 'HEALTHY',
            'rental_data_propagation': {
                'status': 'FAILED' if not rental_data_found else 'SUCCESS',
                'expected_fields': rental_fields,
                'fields_found_in_silver': [f for f in rental_fields if f in df.columns],
                'fields_missing_from_silver': [f for f in rental_fields if f not in df.columns],
                'issue_description': 'Rental data from bronze layer is not being properly propagated to silver layer. Check the bronze-to-silver ETL transformation.' if not rental_data_found else 'Rental data propagation from bronze to silver is working correctly.'
            },
            'coordinate_transformation': {
                'x_y_to_coordinate_mapping': 'x_coordinate' in df.columns and 'y_coordinate' in df.columns,
                'has_coordinates_flag': 'has_coordinates' in df.columns,
                'transformation_status': 'SUCCESS' if ('x_coordinate' in df.columns and 'y_coordinate' in df.columns) else 'FAILED'
            },
            'data_quality_enhancement': {
                'quality_score_available': 'data_quality_score' in df.columns,
                'enhancement_status': 'SUCCESS' if 'data_quality_score' in df.columns else 'PARTIAL'
            },
            'recommendations': []
        }
        
        if not rental_data_found:
            pipeline_report['recommendations'].extend([
                '1. Check bronze-to-silver ETL transformation for rental field propagation',
                '2. Verify that bronze layer contains rental data before silver processing',
                '3. Ensure silver layer transformation includes all rental fields from bronze',
                '4. Re-run the bronze-to-silver ETL pipeline after fixing the transformation'
            ])
        
        validation_results['silver_pipeline_validation_report'] = pipeline_report
        
        # Set validation status
        if len(validation_results['issues']) > 2:  # Allow for minor issues
            validation_results['validation_passed'] = False
        
        logger.info(f"Validation completed with {len(validation_results['issues'])} issues")
        
    except Exception as e:
        logger.error(f"Error during validation: {e}")
        validation_results['validation_passed'] = False
        validation_results['issues'].append(f"Validation error: {str(e)}")
    
    # Save validation report
    import json
    report_path = os.path.join(output_dir, 'silver_validation_report.json')
    with open(report_path, 'w') as f:
        json.dump(validation_results, f, indent=2, default=str)
    
    logger.info(f"Validation report saved: {report_path}")
    return validation_results

def main():
    """Main function to extract and validate URA silver data"""
    logger.info("Starting URA Silver Data Extraction and Validation")
    
    # Extract data
    df, output_dir = extract_parquets_to_csv()
    
    if df is None:
        logger.error("Failed to extract URA silver data")
        sys.exit(1)
    
    # Expected silver fields
    expected_silver_fields = [
        'project', 'street', 'x_coordinate', 'y_coordinate', 'has_coordinates',
        'lease_commence_date', 'property_type', 'district', 'tenure', 'built_year',
        'data_quality_score', 'source', 'ingestion_timestamp', 'bronze_ingestion_timestamp',
        'silver_processed_timestamp'
    ]
    
    # Validate data
    validation_results = validate_extracted_data(df, expected_silver_fields, output_dir)
    
    # Print summary
    logger.info("=== URA Silver Data Validation Summary ===")
    logger.info(f"Total records: {validation_results['total_records']}")
    logger.info(f"Validation passed: {validation_results['validation_passed']}")
    logger.info(f"Issues found: {len(validation_results['issues'])}")
    
    if validation_results['issues']:
        logger.info("Issues:")
        for issue in validation_results['issues']:
            logger.info(f"  - {issue}")
    
    logger.info(f"Results saved in: {output_dir}")
    logger.info("URA Silver Data Extraction and Validation completed")

if __name__ == "__main__":
    main()