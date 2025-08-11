#!/usr/bin/env python3
"""
Enhanced Extract and Validate Gold Layer Data (Mart Models)
Extracts all parquet files from the Gold layer Delta Lake tables,
converts them to CSV format, and validates mart model completeness.
Validates the three mart models: business_intelligence, economic_analysis, and geospatial_analysis.
Includes enhanced data quality analysis and relationship validation.
"""

import sys
import os
import pandas as pd
import numpy as np
import json
from datetime import datetime
from pathlib import Path
sys.path.append('scripts')

from init_minio_buckets import MinIOBucketInitializer
from loguru import logger

# Configure logger
logger.remove()
logger.add(sys.stdout, level="INFO")

def extract_gold_parquets_to_csv():
    """Extract all Gold layer parquet files from MinIO and convert to CSV with enhanced validation"""
    
    # Expected mart models in gold layer
    expected_marts = [
        'mart_business_intelligence',
        'mart_economic_analysis', 
        'mart_geospatial_analysis'
    ]
    
    try:
        # Initialize MinIO client
        initializer = MinIOBucketInitializer()
        if not initializer.wait_for_minio():
            logger.error("Failed to connect to MinIO")
            return False
        
        logger.info("Extracting Gold layer parquet files from MinIO gold bucket and converting to CSV...")
        
        # Create output directory with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path(f"gold_layer_exports_enhanced_{timestamp}")
        output_dir.mkdir(exist_ok=True)
        
        # Create subdirectories for better organization
        parquet_dir = output_dir / "parquet_files"
        csv_dir = output_dir / "csv_files"
        validation_dir = output_dir / "validation_reports"
        
        parquet_dir.mkdir(exist_ok=True)
        csv_dir.mkdir(exist_ok=True)
        validation_dir.mkdir(exist_ok=True)
        
        # Check if gold bucket exists
        try:
            buckets = [bucket.name for bucket in initializer.client.list_buckets()]
            if 'gold' not in buckets:
                logger.warning("Gold bucket does not exist in MinIO")
                logger.info(f"Available buckets: {buckets}")
                return False
        except Exception as e:
            logger.error(f"Failed to list buckets: {e}")
            return False
        
        # List all objects in gold bucket
        try:
            objects = list(initializer.client.list_objects('gold', recursive=True))
            logger.info(f"Found {len(objects)} total objects in gold bucket")
            
            if not objects:
                logger.warning("Gold bucket is empty")
                return False
                
        except Exception as e:
            logger.error(f"Failed to list objects in gold bucket: {e}")
            return False
        
        # Group objects by mart model
        mart_files = {mart: [] for mart in expected_marts}
        other_files = []
        
        for obj in objects:
            logger.debug(f"Found object: {obj.object_name}")
            
            # Check which mart this file belongs to
            assigned = False
            for mart in expected_marts:
                # Check both directory structure and filename patterns
                mart_short_name = mart.replace('mart_', '')
                if ((mart in obj.object_name or mart_short_name in obj.object_name) and 
                    obj.object_name.endswith('.parquet')):
                        # Filter out Delta Lake metadata files
                        if ('_delta_log' not in obj.object_name and 
                            not obj.object_name.endswith('.checkpoint.parquet')):
                            mart_files[mart].append(obj)
                            assigned = True
                            break
            
            if not assigned:
                other_files.append(obj)
        
        # Report findings
        logger.info("\n=== Gold Layer Structure Analysis ===")
        for mart in expected_marts:
            count = len(mart_files[mart])
            logger.info(f"{mart}: {count} data files")
        
        if other_files:
            logger.info(f"Other files: {len(other_files)}")
            for obj in other_files[:5]:  # Show first 5 other files
                logger.info(f"  - {obj.object_name}")
        
        # Extract and process each mart
        extraction_results = {}
        
        for mart_name in expected_marts:
            logger.info(f"\n=== Processing {mart_name} ===")
            
            if not mart_files[mart_name]:
                logger.warning(f"No data files found for {mart_name}")
                extraction_results[mart_name] = {
                    'status': 'no_data',
                    'files_processed': 0,
                    'total_records': 0
                }
                continue
            
            # Download and process all parquet files for this mart
            combined_df = pd.DataFrame()
            files_processed = 0
            parquet_files_info = []
            
            for obj in mart_files[mart_name]:
                try:
                    logger.info(f"  Downloading {obj.object_name}...")
                    
                    # Download parquet file to permanent location for validation
                    safe_filename = obj.object_name.replace('/', '_').replace('\\', '_')
                    parquet_file = parquet_dir / f"{mart_name}_{safe_filename}"
                    initializer.client.fget_object('gold', obj.object_name, str(parquet_file))
                    
                    # Read parquet file
                    df = pd.read_parquet(parquet_file)
                    logger.info(f"    - Loaded {len(df)} records from {obj.object_name}")
                    
                    # Store parquet file info for validation
                    parquet_files_info.append({
                        'original_path': obj.object_name,
                        'local_parquet_path': str(parquet_file),
                        'record_count': len(df),
                        'columns': list(df.columns),
                        'file_size_bytes': obj.size
                    })
                    
                    # Add source file information
                    df['source_file'] = obj.object_name
                    df['extraction_timestamp'] = datetime.now().isoformat()
                    
                    # Combine with existing data
                    if combined_df.empty:
                        combined_df = df
                    else:
                        combined_df = pd.concat([combined_df, df], ignore_index=True)
                    
                    files_processed += 1
                    
                except Exception as e:
                    logger.error(f"    - Failed to process {obj.object_name}: {e}")
                    continue
            
            if combined_df.empty:
                logger.warning(f"No data extracted for {mart_name}")
                extraction_results[mart_name] = {
                    'status': 'extraction_failed',
                    'files_processed': files_processed,
                    'total_records': 0
                }
                continue
            
            # Save combined CSV
            csv_file = csv_dir / f"{mart_name}_combined.csv"
            combined_df.to_csv(csv_file, index=False)
            logger.info(f"  ✅ Saved {len(combined_df)} records to {csv_file}")
            
            # Save individual source files as CSV for reference and validation
            individual_csv_files = []
            for obj in mart_files[mart_name]:
                safe_filename = obj.object_name.replace('/', '_').replace('\\', '_').replace('.parquet', '.csv')
                source_csv = csv_dir / f"{mart_name}_{safe_filename}"
                source_data = combined_df[combined_df['source_file'] == obj.object_name].copy()
                if not source_data.empty:
                    # Remove metadata columns for clean CSV
                    clean_data = source_data.drop(columns=['source_file', 'extraction_timestamp'], errors='ignore')
                    clean_data.to_csv(source_csv, index=False)
                    individual_csv_files.append({
                        'csv_path': str(source_csv),
                        'record_count': len(clean_data),
                        'original_parquet': obj.object_name
                    })
            
            # Enhanced validation: compare parquet and CSV data integrity
            validation_results = validate_mart_data_enhanced(
                combined_df, mart_name, validation_dir, 
                parquet_files_info, individual_csv_files, csv_file
            )
            
            extraction_results[mart_name] = {
                'status': 'success',
                'files_processed': files_processed,
                'total_records': len(combined_df),
                'columns': list(combined_df.columns),
                'combined_csv': str(csv_file),
                'validation': validation_results
            }
        
        # Generate comprehensive summary report
        generate_enhanced_summary_report(extraction_results, output_dir, parquet_dir, csv_dir, validation_dir)
        
        logger.info(f"\n✅ Enhanced gold layer extraction and validation completed successfully!")
        logger.info(f"📊 Files organized in {output_dir}:")
        logger.info(f"   - Parquet files: {parquet_dir}")
        logger.info(f"   - CSV files: {csv_dir}")
        logger.info(f"   - Validation reports: {validation_dir}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to extract gold layer data: {e}")
        return False

def validate_mart_data_enhanced(df, mart_name, validation_dir, parquet_files_info=None, csv_files_info=None, combined_csv_path=None):
    """Enhanced validation of mart model data quality, structure, and format consistency"""
    
    logger.info(f"  Validating {mart_name} data quality and format consistency...")
    
    validation_results = {
        'mart_name': mart_name,
        'total_records': len(df),
        'total_columns': len(df.columns),
        'validation_timestamp': datetime.now().isoformat(),
        'format_validation': {},
        'data_quality_issues': [],
        'recommendations': []
    }
    
    # Validate parquet vs CSV consistency if both are provided
    if parquet_files_info and csv_files_info:
        validation_results['format_validation'] = validate_parquet_csv_consistency(
            parquet_files_info, csv_files_info, combined_csv_path
        )
    
    # Actual column schemas based on dbt models
    expected_columns = {
        'mart_business_intelligence': {
            'core': ['entity_category', 'postal_region', 'business_age_category', 
                    'total_companies', 'active_companies', 'activity_rate'],
            'metrics': ['avg_data_quality', 'avg_economic_value', 'indicator_count',
                       'total_expenditure', 'avg_rental_price', 'business_intelligence_score'],
            'metadata': ['analysis_timestamp']
        },
        'mart_economic_analysis': {
            'core': ['year', 'indicator_category', 'economic_indicator_value'],
            'metrics': ['total_new_businesses', 'total_active_new_businesses', 
                       'total_government_investment', 'economic_health_score'],
            'metadata': ['analysis_timestamp']
        },
        'mart_geospatial_analysis': {
            'core': ['postal_region', 'entity_category', 'business_count', 'active_business_count'],
            'metrics': ['regional_activity_rate', 'avg_regional_rental_price', 
                       'region_latitude', 'region_longitude', 'regional_economic_strength'],
            'metadata': ['analysis_timestamp']
        }
    }
    
    expected_schema = expected_columns.get(mart_name, {'core': [], 'metrics': [], 'metadata': []})
    all_expected = expected_schema['core'] + expected_schema['metrics'] + expected_schema['metadata']
    
    # Column analysis
    present_cols = [col for col in all_expected if col in df.columns]
    missing_cols = [col for col in all_expected if col not in df.columns]
    extra_cols = [col for col in df.columns if col not in all_expected and 
                 col not in ['source_file', 'extraction_timestamp']]
    
    validation_results['schema_analysis'] = {
        'expected_columns': all_expected,
        'present_columns': present_cols,
        'missing_columns': missing_cols,
        'extra_columns': extra_cols,
        'schema_completeness_pct': round(len(present_cols) / len(all_expected) * 100, 2) if all_expected else 100
    }
    
    # Data quality analysis
    validation_results['data_quality'] = {
        'null_counts': df.isnull().sum().to_dict(),
        'duplicate_records': int(df.duplicated().sum()),
        'completeness_by_column': {}
    }
    
    # Calculate completeness percentage for each column
    for col in df.columns:
        non_null_count = df[col].notna().sum()
        completeness_pct = round(non_null_count / len(df) * 100, 2)
        validation_results['data_quality']['completeness_by_column'][col] = completeness_pct
    
    # Mart-specific enhanced validations
    if mart_name == 'mart_business_intelligence':
        validation_results['business_intelligence_analysis'] = analyze_business_intelligence(df)
    elif mart_name == 'mart_economic_analysis':
        validation_results['economic_analysis'] = analyze_economic_trends(df)
    elif mart_name == 'mart_geospatial_analysis':
        validation_results['geospatial_analysis'] = analyze_geospatial_data(df)
    
    # Data relationships and consistency checks
    validation_results['relationship_analysis'] = analyze_data_relationships(df, mart_name)
    
    # Save enhanced validation report
    validation_file = validation_dir / f"{mart_name}_enhanced_validation_report.json"
    with open(validation_file, 'w') as f:
        json.dump(validation_results, f, indent=2, default=str)
    
    logger.info(f"    - Enhanced validation report saved to {validation_file}")
    
    # Log format validation results if available
    if validation_results['format_validation']:
        format_val = validation_results['format_validation']
        if format_val.get('consistency_check', {}).get('all_files_consistent', False):
            logger.info(f"    - ✅ Parquet and CSV formats are consistent")
        else:
            logger.warning(f"    - ⚠️ Format inconsistencies detected between parquet and CSV")
            if format_val.get('consistency_check', {}).get('inconsistent_files'):
                logger.warning(f"    - Inconsistent files: {len(format_val['consistency_check']['inconsistent_files'])}")
    
    # Enhanced data quality analysis and recommendations
    data_quality_issues = []
    recommendations = []
    
    # Check for empty critical columns
    critical_columns = {
        'mart_business_intelligence': ['rental_transactions', 'avg_rental_price'],
        'mart_economic_analysis': ['economic_indicator_value', 'total_new_businesses'],
        'mart_geospatial_analysis': ['total_property_transactions', 'avg_regional_rental_price']
    }
    
    if mart_name in critical_columns:
        for col in critical_columns[mart_name]:
            if col in df.columns:
                null_pct = (df[col].isnull().sum() / len(df)) * 100
                if null_pct > 50:
                    issue = f"Column '{col}' has {null_pct:.1f}% null values"
                    data_quality_issues.append(issue)
                    if col in ['rental_transactions', 'avg_rental_price']:
                        recommendations.append("Fix year mismatch between company registration data and rental data in dbt models")
                    elif col in ['total_property_transactions']:
                        recommendations.append("Verify property transaction data aggregation in geospatial mart")
    
    # Check for suspicious data patterns
    if mart_name == 'mart_business_intelligence':
        if 'rental_transactions' in df.columns:
            non_null_rentals = df['rental_transactions'].notna().sum()
            if non_null_rentals == 0:
                data_quality_issues.append("No rental transaction data available - likely due to year mismatch in joins")
                recommendations.append("Update mart_business_intelligence.sql to use broader year ranges or latest available rental data")
    
    elif mart_name == 'mart_geospatial_analysis':
        if 'postal_region' in df.columns:
            unknown_regions = (df['postal_region'] == 'Unknown').sum()
            if unknown_regions > len(df) * 0.5:
                data_quality_issues.append(f"High percentage of 'Unknown' postal regions: {unknown_regions/len(df)*100:.1f}%")
                recommendations.append("Improve postal code mapping logic in staging models")
    
    validation_results['data_quality_issues'] = data_quality_issues
    validation_results['recommendations'] = recommendations
    
    # Log key findings
    if missing_cols:
        logger.warning(f"    - Missing expected columns: {missing_cols}")
    else:
        logger.info(f"    - ✅ All expected columns present")
    
    if validation_results['data_quality']['duplicate_records'] > 0:
        logger.warning(f"    - Found {validation_results['data_quality']['duplicate_records']} duplicate records")
    else:
        logger.info(f"    - ✅ No duplicate records found")
    
    # Data quality summary
    avg_completeness = np.mean(list(validation_results['data_quality']['completeness_by_column'].values()))
    logger.info(f"    - Average data completeness: {avg_completeness:.1f}%")
    
    # Log data quality issues and recommendations
    if data_quality_issues:
        logger.warning(f"    - ⚠️ Found {len(data_quality_issues)} data quality issues:")
        for issue in data_quality_issues:
            logger.warning(f"      • {issue}")
    
    if recommendations:
        logger.info(f"    - 💡 Recommendations:")
        for rec in recommendations:
            logger.info(f"      • {rec}")
    
    return validation_results

def validate_parquet_csv_consistency(parquet_files_info, csv_files_info, combined_csv_path):
    """Validate consistency between parquet and CSV formats"""
    
    logger.info("    - Validating parquet vs CSV consistency...")
    
    validation_results = {
        'parquet_files_count': len(parquet_files_info),
        'csv_files_count': len(csv_files_info),
        'consistency_check': {
            'all_files_consistent': True,
            'inconsistent_files': [],
            'total_records_match': True,
            'column_structure_match': True
        },
        'data_integrity_check': {},
        'combined_csv_validation': {}
    }
    
    # Check if we have matching parquet and CSV files
    parquet_by_original = {info['original_path']: info for info in parquet_files_info}
    csv_by_original = {info['original_parquet']: info for info in csv_files_info}
    
    # Validate each file pair
    for original_path in parquet_by_original.keys():
        if original_path in csv_by_original:
            parquet_info = parquet_by_original[original_path]
            csv_info = csv_by_original[original_path]
            
            # Compare record counts (accounting for metadata columns in parquet)
            parquet_records = parquet_info['record_count']
            csv_records = csv_info['record_count']
            
            file_validation = {
                'parquet_records': parquet_records,
                'csv_records': csv_records,
                'records_match': parquet_records == csv_records,
                'parquet_path': parquet_info['local_parquet_path'],
                'csv_path': csv_info['csv_path']
            }
            
            # Detailed data comparison
            try:
                # Read both files for comparison
                parquet_df = pd.read_parquet(parquet_info['local_parquet_path'])
                csv_df = pd.read_csv(csv_info['csv_path'])
                
                # Remove metadata columns from both dataframes for fair comparison
                metadata_cols = ['source_file', 'extraction_timestamp']
                parquet_clean = parquet_df.drop(columns=metadata_cols, errors='ignore')
                csv_clean = csv_df.drop(columns=metadata_cols, errors='ignore')
                
                # Compare column names
                parquet_cols = set(parquet_clean.columns)
                csv_cols = set(csv_clean.columns)
                
                file_validation['column_comparison'] = {
                    'parquet_columns': sorted(list(parquet_cols)),
                    'csv_columns': sorted(list(csv_cols)),
                    'columns_match': parquet_cols == csv_cols,
                    'missing_in_csv': sorted(list(parquet_cols - csv_cols)),
                    'extra_in_csv': sorted(list(csv_cols - parquet_cols))
                }
                
                # Compare data content if columns match
                if parquet_cols == csv_cols and len(parquet_clean) == len(csv_clean):
                    # Sort both dataframes for comparison
                    parquet_sorted = parquet_clean.sort_values(by=list(parquet_clean.columns)).reset_index(drop=True)
                    csv_sorted = csv_clean.sort_values(by=list(csv_clean.columns)).reset_index(drop=True)
                    
                    # Compare values (handling potential float precision issues)
                    data_matches = True
                    mismatched_columns = []
                    
                    for col in parquet_cols:
                        if parquet_sorted[col].dtype in ['float64', 'float32'] and csv_sorted[col].dtype in ['float64', 'float32']:
                            # Use approximate comparison for floats
                            if not np.allclose(parquet_sorted[col].fillna(0), csv_sorted[col].fillna(0), rtol=1e-10, equal_nan=True):
                                data_matches = False
                                mismatched_columns.append(col)
                        else:
                            # Exact comparison for other types, handling NaN values properly
                            try:
                                # For numeric columns with NaN, use pandas comparison with check_exact=False
                                if parquet_sorted[col].dtype in ['int64', 'Int64', 'float64', 'float32'] or csv_sorted[col].dtype in ['int64', 'Int64', 'float64', 'float32']:
                                    pd.testing.assert_series_equal(parquet_sorted[col], csv_sorted[col], check_exact=False, check_names=False, check_dtype=False)
                                else:
                                    # For non-numeric columns, use direct comparison
                                    if not parquet_sorted[col].equals(csv_sorted[col]):
                                        data_matches = False
                                        mismatched_columns.append(col)
                            except AssertionError:
                                data_matches = False
                                mismatched_columns.append(col)
                            except Exception as e:
                                # Fallback to basic comparison if pandas testing fails
                                if not parquet_sorted[col].equals(csv_sorted[col]):
                                    data_matches = False
                                    mismatched_columns.append(col)
                    
                    file_validation['data_content_match'] = data_matches
                    file_validation['mismatched_columns'] = mismatched_columns
                else:
                    file_validation['data_content_match'] = False
                    file_validation['mismatched_columns'] = ['Structure mismatch prevents content comparison']
                
            except Exception as e:
                file_validation['validation_error'] = str(e)
                file_validation['data_content_match'] = False
            
            # Check if this file pair is consistent
            file_consistent = (
                file_validation['records_match'] and 
                file_validation.get('column_comparison', {}).get('columns_match', False) and
                file_validation.get('data_content_match', False)
            )
            
            if not file_consistent:
                validation_results['consistency_check']['all_files_consistent'] = False
                validation_results['consistency_check']['inconsistent_files'].append({
                    'original_path': original_path,
                    'issues': file_validation
                })
            
            validation_results['data_integrity_check'][original_path] = file_validation
    
    # Validate combined CSV if provided
    if combined_csv_path and Path(combined_csv_path).exists():
        try:
            combined_csv_df = pd.read_csv(combined_csv_path)
            total_expected_records = sum(info['record_count'] for info in csv_files_info)
            
            validation_results['combined_csv_validation'] = {
                'file_exists': True,
                'total_records': len(combined_csv_df),
                'expected_records': total_expected_records,
                'records_match': len(combined_csv_df) == total_expected_records,
                'columns': list(combined_csv_df.columns)
            }
        except Exception as e:
            validation_results['combined_csv_validation'] = {
                'file_exists': True,
                'validation_error': str(e)
            }
    else:
        validation_results['combined_csv_validation'] = {
            'file_exists': False
        }
    
    return validation_results

def analyze_business_intelligence(df):
    """Analyze business intelligence mart for insights and data quality"""
    analysis = {}
    
    if 'entity_category' in df.columns:
        analysis['entity_distribution'] = df['entity_category'].value_counts().to_dict()
    
    if 'postal_region' in df.columns:
        analysis['regional_distribution'] = df['postal_region'].value_counts().to_dict()
    
    if 'total_companies' in df.columns and 'active_companies' in df.columns:
        analysis['company_metrics'] = {
            'total_companies_sum': int(df['total_companies'].sum()),
            'active_companies_sum': int(df['active_companies'].sum()),
            'overall_activity_rate': round(df['active_companies'].sum() / df['total_companies'].sum() * 100, 2) if df['total_companies'].sum() > 0 else 0
        }
    
    if 'business_intelligence_score' in df.columns:
        analysis['intelligence_score_stats'] = {
            'min_score': float(df['business_intelligence_score'].min()),
            'max_score': float(df['business_intelligence_score'].max()),
            'avg_score': float(df['business_intelligence_score'].mean()),
            'median_score': float(df['business_intelligence_score'].median())
        }
    
    return analysis

def analyze_economic_trends(df):
    """Analyze economic analysis mart for trends and insights"""
    analysis = {}
    
    if 'year' in df.columns:
        analysis['temporal_coverage'] = {
            'year_range': [int(df['year'].min()), int(df['year'].max())],
            'years_covered': int(df['year'].nunique()),
            'records_per_year': df['year'].value_counts().to_dict()
        }
    
    if 'economic_health_score' in df.columns:
        analysis['health_score_analysis'] = {
            'min_score': float(df['economic_health_score'].min()),
            'max_score': float(df['economic_health_score'].max()),
            'avg_score': float(df['economic_health_score'].mean()),
            'trend_direction': 'improving' if df['economic_health_score'].corr(df['year']) > 0 else 'declining' if 'year' in df.columns else 'unknown'
        }
    
    if 'total_new_businesses' in df.columns:
        analysis['business_formation_trends'] = {
            'total_new_businesses': int(df['total_new_businesses'].sum()),
            'avg_new_businesses_per_year': float(df['total_new_businesses'].mean()),
            'peak_formation_year': int(df.loc[df['total_new_businesses'].idxmax(), 'year']) if 'year' in df.columns else None
        }
    
    return analysis

def analyze_geospatial_data(df):
    """Analyze geospatial analysis mart for location insights"""
    analysis = {}
    
    if 'postal_region' in df.columns:
        analysis['regional_coverage'] = {
            'regions_covered': int(df['postal_region'].nunique()),
            'region_distribution': df['postal_region'].value_counts().to_dict()
        }
    
    if 'business_count' in df.columns and 'active_business_count' in df.columns:
        analysis['regional_business_metrics'] = {
            'total_businesses': int(df['business_count'].sum()),
            'total_active_businesses': int(df['active_business_count'].sum()),
            'regional_activity_rate': round(df['active_business_count'].sum() / df['business_count'].sum() * 100, 2) if df['business_count'].sum() > 0 else 0
        }
    
    if 'region_latitude' in df.columns and 'region_longitude' in df.columns:
        # Filter out null coordinates
        coord_data = df.dropna(subset=['region_latitude', 'region_longitude'])
        if not coord_data.empty:
            analysis['coordinate_analysis'] = {
                'records_with_coordinates': len(coord_data),
                'coordinate_coverage_pct': round(len(coord_data) / len(df) * 100, 2),
                'latitude_range': [float(coord_data['region_latitude'].min()), float(coord_data['region_latitude'].max())],
                'longitude_range': [float(coord_data['region_longitude'].min()), float(coord_data['region_longitude'].max())]
            }
    
    if 'regional_economic_strength' in df.columns:
        analysis['economic_strength_analysis'] = {
            'min_strength': float(df['regional_economic_strength'].min()),
            'max_strength': float(df['regional_economic_strength'].max()),
            'avg_strength': float(df['regional_economic_strength'].mean()),
            'strongest_region': df.loc[df['regional_economic_strength'].idxmax(), 'postal_region'] if 'postal_region' in df.columns else None
        }
    
    return analysis

def analyze_data_relationships(df, mart_name):
    """Analyze data relationships and consistency within the mart"""
    analysis = {
        'record_consistency': {},
        'value_distributions': {},
        'potential_issues': []
    }
    
    # Check for logical consistency
    if mart_name == 'mart_business_intelligence':
        if 'total_companies' in df.columns and 'active_companies' in df.columns:
            inconsistent = df[df['active_companies'] > df['total_companies']]
            if not inconsistent.empty:
                analysis['potential_issues'].append(f"Found {len(inconsistent)} records where active_companies > total_companies")
    
    elif mart_name == 'mart_geospatial_analysis':
        if 'business_count' in df.columns and 'active_business_count' in df.columns:
            inconsistent = df[df['active_business_count'] > df['business_count']]
            if not inconsistent.empty:
                analysis['potential_issues'].append(f"Found {len(inconsistent)} records where active_business_count > business_count")
    
    # Check for unusual value distributions
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    for col in numeric_columns:
        if col not in ['source_file', 'extraction_timestamp']:
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            outliers = df[(df[col] < q1 - 1.5 * iqr) | (df[col] > q3 + 1.5 * iqr)]
            
            analysis['value_distributions'][col] = {
                'outlier_count': len(outliers),
                'outlier_percentage': round(len(outliers) / len(df) * 100, 2)
            }
    
    return analysis

def generate_data_quality_recommendations(mart_validations):
    """Generate comprehensive data quality recommendations based on validation results"""
    
    recommendations = {
        'data_completeness': [],
        'schema_issues': [],
        'data_consistency': [],
        'performance_optimization': [],
        'business_logic': []
    }
    
    for mart_name, validation in mart_validations.items():
        # Data completeness recommendations
        if 'data_quality' in validation:
            completeness = validation['data_quality'].get('completeness_by_column', {})
            for col, pct in completeness.items():
                if pct < 50:
                    recommendations['data_completeness'].append(
                        f"{mart_name}: Column '{col}' has only {pct}% completeness - investigate data source"
                    )
                elif pct < 80:
                    recommendations['data_completeness'].append(
                        f"{mart_name}: Column '{col}' has {pct}% completeness - consider data quality improvements"
                    )
        
        # Schema issues
        if 'schema_analysis' in validation:
            schema = validation['schema_analysis']
            if schema.get('missing_columns'):
                recommendations['schema_issues'].append(
                    f"{mart_name}: Missing expected columns: {', '.join(schema['missing_columns'])}"
                )
            if schema.get('schema_completeness_pct', 100) < 90:
                recommendations['schema_issues'].append(
                    f"{mart_name}: Schema completeness is {schema['schema_completeness_pct']}% - review dbt model"
                )
        
        # Data consistency issues
        if 'relationship_analysis' in validation:
            issues = validation['relationship_analysis'].get('potential_issues', [])
            for issue in issues:
                recommendations['data_consistency'].append(f"{mart_name}: {issue}")
        
        # Business logic recommendations
        if 'data_quality_issues' in validation:
            for issue in validation['data_quality_issues']:
                recommendations['business_logic'].append(f"{mart_name}: {issue}")
        
        if 'recommendations' in validation:
            for rec in validation['recommendations']:
                recommendations['business_logic'].append(f"{mart_name}: {rec}")
        
        # Performance optimization
        if validation.get('total_records', 0) == 0:
            recommendations['performance_optimization'].append(
                f"{mart_name}: No data found - check data pipeline and source connections"
            )
        elif 'data_quality' in validation:
            duplicates = validation['data_quality'].get('duplicate_records', 0)
            if duplicates > 0:
                recommendations['performance_optimization'].append(
                    f"{mart_name}: {duplicates} duplicate records found - add deduplication logic"
                )
    
    return recommendations

def generate_enhanced_summary_report(extraction_results, output_dir, parquet_dir, csv_dir, validation_dir):
    """Generate a comprehensive enhanced summary report with format validation insights"""
    
    logger.info("\n=== Generating Enhanced Summary Report ===")
    
    # Generate data quality recommendations from validation results
    mart_validations = {}
    for mart_name, result in extraction_results.items():
        if result['status'] == 'success' and 'validation' in result:
            mart_validations[mart_name] = result['validation']
    
    data_quality_recommendations = generate_data_quality_recommendations(mart_validations)
    
    summary = {
        'extraction_timestamp': datetime.now().isoformat(),
        'total_marts_expected': 3,
        'marts_with_data': sum(1 for result in extraction_results.values() if result['status'] == 'success'),
        'total_records_extracted': sum(result.get('total_records', 0) for result in extraction_results.values()),
        'mart_details': extraction_results,
        'data_quality_summary': {},
        'format_validation_summary': {},
        'file_organization': {
            'parquet_directory': str(parquet_dir),
            'csv_directory': str(csv_dir),
            'validation_directory': str(validation_dir)
        },
        'insights_and_recommendations': [],
        'data_quality_recommendations': data_quality_recommendations
    }
    
    # Aggregate data quality and format validation metrics
    total_completeness = []
    total_issues = []
    format_validation_stats = {
        'total_marts_with_format_validation': 0,
        'marts_with_consistent_formats': 0,
        'total_file_pairs_validated': 0,
        'consistent_file_pairs': 0,
        'format_issues': []
    }
    
    for mart_name, result in extraction_results.items():
        if result['status'] == 'success' and 'validation' in result:
            validation = result['validation']
            
            # Collect completeness metrics
            if 'data_quality' in validation and 'completeness_by_column' in validation['data_quality']:
                completeness_values = list(validation['data_quality']['completeness_by_column'].values())
                total_completeness.extend(completeness_values)
            
            # Collect potential issues
            if 'relationship_analysis' in validation and 'potential_issues' in validation['relationship_analysis']:
                total_issues.extend(validation['relationship_analysis']['potential_issues'])
            
            # Collect format validation metrics
            if 'format_validation' in validation and validation['format_validation']:
                format_val = validation['format_validation']
                format_validation_stats['total_marts_with_format_validation'] += 1
                
                if format_val.get('consistency_check', {}).get('all_files_consistent', False):
                    format_validation_stats['marts_with_consistent_formats'] += 1
                
                # Count file pairs
                data_integrity = format_val.get('data_integrity_check', {})
                format_validation_stats['total_file_pairs_validated'] += len(data_integrity)
                
                for file_path, file_validation in data_integrity.items():
                    if (file_validation.get('records_match', False) and 
                        file_validation.get('column_comparison', {}).get('columns_match', False) and
                        file_validation.get('data_content_match', False)):
                        format_validation_stats['consistent_file_pairs'] += 1
                    else:
                        # Collect specific format issues
                        issues = []
                        if not file_validation.get('records_match', False):
                            issues.append('Record count mismatch')
                        if not file_validation.get('column_comparison', {}).get('columns_match', False):
                            issues.append('Column structure mismatch')
                        if not file_validation.get('data_content_match', False):
                            issues.append('Data content mismatch')
                        
                        format_validation_stats['format_issues'].append({
                            'mart': mart_name,
                            'file': file_path,
                            'issues': issues
                        })
    
    summary['data_quality_summary'] = {
        'overall_completeness_avg': round(np.mean(total_completeness), 2) if total_completeness else 0,
        'total_data_issues_found': len(total_issues),
        'data_issues': total_issues
    }
    
    # Add format validation summary
    summary['format_validation_summary'] = format_validation_stats
    if format_validation_stats['total_file_pairs_validated'] > 0:
        consistency_rate = round(
            format_validation_stats['consistent_file_pairs'] / format_validation_stats['total_file_pairs_validated'] * 100, 2
        )
        summary['format_validation_summary']['consistency_rate_percent'] = consistency_rate
    
    # Generate insights and recommendations
    insights = []
    
    # Check data coverage
    successful_marts = [name for name, result in extraction_results.items() if result['status'] == 'success']
    if len(successful_marts) == 3:
        insights.append("✅ All three mart models are successfully populated with data")
    else:
        missing_marts = [name for name, result in extraction_results.items() if result['status'] != 'success']
        insights.append(f"⚠️ Missing or failed marts: {missing_marts}")
    
    # Data quality insights
    if summary['data_quality_summary']['overall_completeness_avg'] >= 95:
        insights.append("✅ Excellent data completeness across all marts")
    elif summary['data_quality_summary']['overall_completeness_avg'] >= 80:
        insights.append("⚠️ Good data completeness, but some fields may need attention")
    else:
        insights.append("❌ Poor data completeness detected - investigate data pipeline")
    
    # Format validation insights
    format_stats = summary['format_validation_summary']
    if format_stats['total_marts_with_format_validation'] > 0:
        consistency_rate = format_stats.get('consistency_rate_percent', 0)
        if consistency_rate == 100:
            insights.append("✅ Perfect consistency between parquet and CSV formats")
        elif consistency_rate >= 90:
            insights.append(f"✅ High format consistency: {consistency_rate}% of file pairs match")
        elif consistency_rate >= 70:
            insights.append(f"⚠️ Moderate format consistency: {consistency_rate}% of file pairs match")
        else:
            insights.append(f"❌ Poor format consistency: {consistency_rate}% of file pairs match")
        
        if format_stats['format_issues']:
            insights.append(f"⚠️ {len(format_stats['format_issues'])} format validation issues detected")
    else:
        insights.append("ℹ️ No format validation performed (missing parquet or CSV files)")
    
    # Business insights
    for mart_name, result in extraction_results.items():
        if result['status'] == 'success' and 'validation' in result:
            validation = result['validation']
            
            if mart_name == 'mart_business_intelligence' and 'business_intelligence_analysis' in validation:
                bi_analysis = validation['business_intelligence_analysis']
                if 'company_metrics' in bi_analysis:
                    total_companies = bi_analysis['company_metrics'].get('total_companies_sum', 0)
                    activity_rate = bi_analysis['company_metrics'].get('overall_activity_rate', 0)
                    insights.append(f"📊 Business Intelligence: {total_companies:,} total companies with {activity_rate}% activity rate")
            
            elif mart_name == 'mart_economic_analysis' and 'economic_analysis' in validation:
                econ_analysis = validation['economic_analysis']
                if 'health_score_analysis' in econ_analysis:
                    avg_health = econ_analysis['health_score_analysis'].get('avg_score', 0)
                    trend = econ_analysis['health_score_analysis'].get('trend_direction', 'unknown')
                    insights.append(f"📈 Economic Analysis: Average health score {avg_health:.1f}, trend is {trend}")
            
            elif mart_name == 'mart_geospatial_analysis' and 'geospatial_analysis' in validation:
                geo_analysis = validation['geospatial_analysis']
                if 'regional_coverage' in geo_analysis:
                    regions = geo_analysis['regional_coverage'].get('regions_covered', 0)
                    insights.append(f"🗺️ Geospatial Analysis: Coverage across {regions} postal regions")
    
    summary['insights_and_recommendations'] = insights
    
    # Save summary report
    summary_file = output_dir / "enhanced_gold_layer_summary_report.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    
    logger.info(f"Enhanced summary report saved to {summary_file}")
    
    # Log summary
    logger.info("\n=== ENHANCED EXTRACTION SUMMARY ===")
    logger.info(f"Expected mart models: {summary['total_marts_expected']}")
    logger.info(f"Marts with data: {summary['marts_with_data']}")
    logger.info(f"Total records extracted: {summary['total_records_extracted']:,}")
    logger.info(f"Overall data completeness: {summary['data_quality_summary']['overall_completeness_avg']}%")
    
    # Log format validation summary
    format_stats = summary['format_validation_summary']
    if format_stats['total_file_pairs_validated'] > 0:
        logger.info(f"Format validation: {format_stats['consistent_file_pairs']}/{format_stats['total_file_pairs_validated']} file pairs consistent")
        if 'consistency_rate_percent' in format_stats:
            logger.info(f"Format consistency rate: {format_stats['consistency_rate_percent']}%")
    else:
        logger.info("Format validation: Not performed")
    
    for mart_name, result in extraction_results.items():
        if result['status'] == 'success':
            logger.info(f"✅ {mart_name}: {result['total_records']:,} records from {result['files_processed']} files")
        else:
            logger.info(f"❌ {mart_name}: {result['status']}")
    
    logger.info("\n=== KEY INSIGHTS ===")
    for insight in insights:
        logger.info(insight)
    
    if summary['data_quality_summary']['total_data_issues_found'] > 0:
        logger.info("\n=== DATA QUALITY ISSUES ===")
        for issue in summary['data_quality_summary']['data_issues']:
            logger.warning(issue)
    
    # Log data quality recommendations
    if summary['data_quality_recommendations']:
        logger.info("\n=== DATA QUALITY RECOMMENDATIONS ===")
        for category, recs in summary['data_quality_recommendations'].items():
            if recs:
                logger.info(f"\n{category.replace('_', ' ').title()}:")
                for rec in recs[:3]:  # Show top 3 recommendations per category
                    logger.info(f"  • {rec}")
                if len(recs) > 3:
                    logger.info(f"  ... and {len(recs) - 3} more recommendations")
    
    return summary

def main():
    """Main execution function"""
    logger.info("Starting enhanced Gold Layer data extraction and validation...")
    
    success = extract_gold_parquets_to_csv()
    
    if success:
        logger.info("\n🎉 Enhanced gold layer analysis completed successfully!")
        logger.info("📋 Check the generated reports for detailed insights and recommendations")
    else:
        logger.error("❌ Enhanced gold layer analysis failed")
        sys.exit(1)

if __name__ == "__main__":
    main()