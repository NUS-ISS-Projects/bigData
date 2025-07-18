#!/usr/bin/env python3
"""
Verify that Delta Lake data matches the original CSV source
"""

import boto3
from botocore.client import Config
import pandas as pd
import pyarrow.parquet as pq
import io
import json
from datetime import datetime

def load_original_csv():
    """Load the original CSV data"""
    print("📄 Loading original CSV data...")
    try:
        df = pd.read_csv('/home/ngtianxun/bigData_project/data/EntitiesRegisteredwithACRA.csv')
        print(f"   ✅ Loaded {len(df):,} rows from CSV")
        print(f"   📊 Columns: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"   ❌ Error loading CSV: {e}")
        return None

def load_delta_lake_data():
    """Load data from Delta Lake bronze layer"""
    print("\n🏗️  Loading Delta Lake data...")
    
    # Configure S3 client for# MinIO configuration
    s3_client = boto3.client(
        's3',
        endpoint_url='http://192.168.49.2:30900',
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    try:
        # List all objects in acra_companies
        response = s3_client.list_objects_v2(
            Bucket='bronze',
            Prefix='acra_companies/'
        )
        
        all_files = [obj['Key'] for obj in response.get('Contents', [])]
        # Filter for actual data parquet files, excluding Delta Lake metadata
        parquet_files = [f for f in all_files if f.endswith('.parquet') and not f.startswith('acra_companies/_delta_log/')]
        
        print(f"   📁 Found {len(all_files)} total files")
        print(f"   📄 Found {len(parquet_files)} parquet files")
        
        if not parquet_files:
            print("   ❌ No parquet files found")
            return None
            
        # Load all parquet files and combine
        all_dataframes = []
        total_size = 0
        
        for file_key in parquet_files:
            print(f"   📖 Reading: {file_key}")
            obj = s3_client.get_object(Bucket='bronze', Key=file_key)
            parquet_data = obj['Body'].read()
            total_size += len(parquet_data)
            
            df = pd.read_parquet(io.BytesIO(parquet_data))
            all_dataframes.append(df)
            print(f"      - {len(df):,} rows, {len(parquet_data):,} bytes")
        
        # Combine all dataframes
        if all_dataframes:
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            print(f"   ✅ Combined: {len(combined_df):,} rows, {total_size:,} bytes total")
            print(f"   📊 Columns: {list(combined_df.columns)}")
            return combined_df
        else:
            return None
            
    except Exception as e:
        print(f"   ❌ Error loading Delta Lake data: {e}")
        return None

def compare_datasets(csv_df, delta_df):
    """Compare CSV and Delta Lake datasets"""
    print("\n🔍 Data Comparison Analysis")
    print("=" * 50)
    
    if csv_df is None or delta_df is None:
        print("❌ Cannot compare - one or both datasets failed to load")
        return
    
    # Basic statistics
    print(f"📊 Row Count Comparison:")
    print(f"   CSV:        {len(csv_df):,} rows")
    print(f"   Delta Lake: {len(delta_df):,} rows")
    print(f"   Difference: {len(delta_df) - len(csv_df):,} rows")
    
    # Column comparison
    csv_cols = set(csv_df.columns)
    delta_cols = set(delta_df.columns)
    
    print(f"\n📋 Column Comparison:")
    print(f"   CSV columns:        {len(csv_cols)}")
    print(f"   Delta Lake columns: {len(delta_cols)}")
    
    # Define field mappings between CSV and Delta Lake
    field_mappings = {
        'entity_type_desc': 'entity_type',
        'uen_status_desc': 'entity_status'
    }
    
    # Find common columns accounting for field mappings
    mapped_delta_cols = set()
    for col in delta_cols:
        mapped_delta_cols.add(col)
        # Add reverse mappings
        for csv_field, delta_field in field_mappings.items():
            if col == delta_field:
                mapped_delta_cols.add(csv_field)
    
    common_cols = csv_cols.intersection(mapped_delta_cols)
    csv_only = csv_cols - mapped_delta_cols
    delta_only = delta_cols - csv_cols
    
    print(f"   Common columns:     {len(common_cols)}")
    if csv_only:
        print(f"   CSV only:           {list(csv_only)}")
    if delta_only:
        print(f"   Delta Lake only:    {list(delta_only)}")
    
    # Sample data comparison
    if common_cols:
        print(f"\n📝 Sample Data Comparison (first 3 rows):")
        common_cols_list = sorted(list(common_cols))
        
        print("\n   📄 Original CSV:")
        csv_sample = csv_df[common_cols_list].head(3)
        print(csv_sample.to_string(index=False))
        
        print("\n   🏗️  Delta Lake:")
        # Create mapped column list for Delta Lake
        delta_cols_mapped = []
        for col in common_cols_list:
            delta_field = field_mappings.get(col, col)
            if delta_field in delta_df.columns:
                delta_cols_mapped.append(delta_field)
            elif col in delta_df.columns:
                delta_cols_mapped.append(col)
        
        if delta_cols_mapped:
            delta_sample = delta_df[delta_cols_mapped].head(3)
            # Rename columns back to CSV names for comparison
            rename_dict = {v: k for k, v in field_mappings.items() if v in delta_cols_mapped}
            if rename_dict:
                delta_sample = delta_sample.rename(columns=rename_dict)
            print(delta_sample.to_string(index=False))
        else:
            print("   No matching columns found in Delta Lake")
        
        # Check if data matches
        if len(common_cols_list) > 0:
            # Compare a few key fields, handling field mappings
            key_field = common_cols_list[0]  # Use first common column
            csv_values = set(csv_df[key_field].dropna().astype(str))
            
            # Handle field mapping for Delta Lake
            delta_field = field_mappings.get(key_field, key_field)
            if delta_field in delta_df.columns:
                delta_values = set(delta_df[delta_field].dropna().astype(str))
            else:
                delta_values = set(delta_df[key_field].dropna().astype(str))
            
            overlap = len(csv_values.intersection(delta_values))
            total_unique_csv = len(csv_values)
            total_unique_delta = len(delta_values)
            
            print(f"\n🎯 Data Overlap Analysis (using '{key_field}'):")
            print(f"   Unique values in CSV:        {total_unique_csv:,}")
            print(f"   Unique values in Delta Lake: {total_unique_delta:,}")
            print(f"   Overlapping values:          {overlap:,}")
            
            if total_unique_csv > 0:
                overlap_pct = (overlap / total_unique_csv) * 100
                print(f"   Overlap percentage:          {overlap_pct:.1f}%")
                
                if overlap_pct > 95:
                    print("   ✅ HIGH CONFIDENCE: Data appears to match original source")
                elif overlap_pct > 80:
                    print("   ⚠️  MEDIUM CONFIDENCE: Most data matches, some differences")
                else:
                    print("   ❌ LOW CONFIDENCE: Significant differences detected")
    
    # Check for ingestion metadata
    metadata_cols = [col for col in delta_df.columns if 'ingestion' in col.lower() or 'timestamp' in col.lower()]
    if metadata_cols:
        print(f"\n🏷️  Ingestion Metadata Detected:")
        for col in metadata_cols:
            sample_values = delta_df[col].dropna().head(3).tolist()
            print(f"   - {col}: {sample_values}")

def main():
    print("🔍 Data Source Verification")
    print("=" * 60)
    print("Comparing original CSV with Delta Lake bronze layer data")
    
    # Load both datasets
    csv_df = load_original_csv()
    delta_df = load_delta_lake_data()
    
    # Compare them
    compare_datasets(csv_df, delta_df)
    
    print("\n✨ Verification Complete!")
    print("   This analysis helps confirm data lineage from source to Delta Lake")

if __name__ == "__main__":
    main()