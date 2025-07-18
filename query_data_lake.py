#!/usr/bin/env python3
"""
Comprehensive tool for querying Delta Lake data without UI or manual Parquet decoding
Provides programmatic access to Delta Lake with filtering, search, and export capabilities
"""

import boto3
from botocore.client import Config
import pandas as pd
import pyarrow.parquet as pq
import io
import argparse
import sys
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

class DeltaLakeQueryTool:
    """Tool for querying Delta Lake data stored in MinIO"""
    
    def __init__(self, endpoint_url='http://192.168.49.2:30900'):
        """Initialize the query tool with MinIO configuration"""
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id='admin',
            aws_secret_access_key='password123',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        self.bucket = 'bronze'
        self.prefix = 'acra_companies/'
        
    def load_data(self, max_files: Optional[int] = None) -> pd.DataFrame:
        """Load all Delta Lake data into a pandas DataFrame"""
        print("🔍 Loading Delta Lake data...")
        
        try:
            # List all parquet files
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.prefix
            )
            
            parquet_files = [
                obj['Key'] for obj in response.get('Contents', [])
                if obj['Key'].endswith('.parquet') and not obj['Key'].startswith(f'{self.prefix}_delta_log/')
            ]
            
            if not parquet_files:
                print("❌ No parquet files found")
                return pd.DataFrame()
            
            # Limit files if specified
            if max_files:
                parquet_files = parquet_files[:max_files]
                print(f"📄 Loading {len(parquet_files)} files (limited)")
            else:
                print(f"📄 Loading {len(parquet_files)} files")
            
            # Load and combine all files
            dataframes = []
            for file_key in parquet_files:
                obj = self.s3_client.get_object(Bucket=self.bucket, Key=file_key)
                parquet_data = obj['Body'].read()
                df = pd.read_parquet(io.BytesIO(parquet_data))
                dataframes.append(df)
            
            if dataframes:
                combined_df = pd.concat(dataframes, ignore_index=True)
                print(f"✅ Loaded {len(combined_df):,} total rows")
                return combined_df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return pd.DataFrame()
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of the data"""
        df = self.load_data(max_files=5)  # Sample for summary
        
        if df.empty:
            return {"error": "No data available"}
        
        summary = {
            "total_rows_sampled": len(df),
            "columns": list(df.columns),
            "data_types": df.dtypes.to_dict(),
            "non_null_counts": df.count().to_dict(),
            "sample_data": df.head(3).to_dict('records')
        }
        
        # Add business insights
        if 'entity_type' in df.columns:
            summary['entity_types'] = df['entity_type'].value_counts().head(10).to_dict()
        
        if 'reg_postal_code' in df.columns:
            summary['top_postal_codes'] = df['reg_postal_code'].value_counts().head(10).to_dict()
        
        if 'bronze_ingestion_timestamp' in df.columns:
            df['bronze_ingestion_timestamp'] = pd.to_datetime(df['bronze_ingestion_timestamp'])
            summary['ingestion_date_range'] = {
                'earliest': df['bronze_ingestion_timestamp'].min().isoformat(),
                'latest': df['bronze_ingestion_timestamp'].max().isoformat()
            }
        
        return summary
    
    def search_entities(self, search_term: str, column: str = 'entity_name') -> pd.DataFrame:
        """Search for entities by name or other fields"""
        print(f"🔍 Searching for '{search_term}' in {column}...")
        
        df = self.load_data()
        if df.empty:
            return df
        
        if column not in df.columns:
            print(f"❌ Column '{column}' not found. Available columns: {list(df.columns)}")
            return pd.DataFrame()
        
        # Case-insensitive search
        mask = df[column].astype(str).str.contains(search_term, case=False, na=False)
        results = df[mask]
        
        print(f"✅ Found {len(results)} matches")
        return results
    
    def filter_data(self, filters: Dict[str, Any]) -> pd.DataFrame:
        """Filter data based on multiple criteria"""
        print(f"🔍 Applying filters: {filters}")
        
        df = self.load_data()
        if df.empty:
            return df
        
        filtered_df = df.copy()
        
        for column, value in filters.items():
            if column not in df.columns:
                print(f"⚠️ Column '{column}' not found, skipping")
                continue
            
            if isinstance(value, str):
                # String contains search
                mask = filtered_df[column].astype(str).str.contains(value, case=False, na=False)
            elif isinstance(value, list):
                # Value in list
                mask = filtered_df[column].isin(value)
            else:
                # Exact match
                mask = filtered_df[column] == value
            
            filtered_df = filtered_df[mask]
        
        print(f"✅ Filtered to {len(filtered_df)} rows")
        return filtered_df
    
    def get_recent_data(self, hours: int = 24) -> pd.DataFrame:
        """Get data ingested in the last N hours"""
        print(f"🔍 Getting data from last {hours} hours...")
        
        df = self.load_data()
        if df.empty:
            return df
        
        if 'bronze_ingestion_timestamp' not in df.columns:
            print("❌ No ingestion timestamp column found")
            return df
        
        # Convert to datetime
        df['bronze_ingestion_timestamp'] = pd.to_datetime(df['bronze_ingestion_timestamp'])
        
        # Filter by time
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_data = df[df['bronze_ingestion_timestamp'] >= cutoff_time]
        
        print(f"✅ Found {len(recent_data)} recent records")
        return recent_data
    
    def export_to_csv(self, df: pd.DataFrame, filename: str) -> bool:
        """Export DataFrame to CSV"""
        try:
            df.to_csv(filename, index=False)
            print(f"✅ Exported {len(df)} rows to {filename}")
            return True
        except Exception as e:
            print(f"❌ Export failed: {e}")
            return False
    
    def run_custom_query(self, query_func) -> Any:
        """Run a custom query function on the data"""
        df = self.load_data()
        if df.empty:
            return None
        
        return query_func(df)

def main():
    """Command-line interface for the query tool"""
    parser = argparse.ArgumentParser(description='Query Delta Lake data')
    parser.add_argument('action', choices=['summary', 'search', 'recent', 'filter', 'export'],
                       help='Action to perform')
    parser.add_argument('--search-term', help='Term to search for')
    parser.add_argument('--search-column', default='entity_name', help='Column to search in')
    parser.add_argument('--hours', type=int, default=24, help='Hours for recent data')
    parser.add_argument('--filter', help='JSON string of filters')
    parser.add_argument('--output', help='Output CSV filename')
    parser.add_argument('--endpoint', default='http://192.168.49.2:30900', help='MinIO endpoint')
    
    args = parser.parse_args()
    
    # Initialize query tool
    query_tool = DeltaLakeQueryTool(endpoint_url=args.endpoint)
    
    if args.action == 'summary':
        summary = query_tool.get_summary()
        print("\n📊 Data Summary:")
        print("=" * 50)
        for key, value in summary.items():
            if key == 'sample_data':
                print(f"{key}: {len(value)} sample records")
            elif isinstance(value, dict) and len(str(value)) > 100:
                print(f"{key}: {len(value)} items")
            else:
                print(f"{key}: {value}")
    
    elif args.action == 'search':
        if not args.search_term:
            print("❌ --search-term required for search action")
            sys.exit(1)
        
        results = query_tool.search_entities(args.search_term, args.search_column)
        if not results.empty:
            print(f"\n📋 Search Results ({len(results)} found):")
            print("=" * 50)
            print(results.head(10).to_string(index=False))
            
            if args.output:
                query_tool.export_to_csv(results, args.output)
    
    elif args.action == 'recent':
        recent_data = query_tool.get_recent_data(args.hours)
        if not recent_data.empty:
            print(f"\n📅 Recent Data ({len(recent_data)} records):")
            print("=" * 50)
            print(recent_data.head(10).to_string(index=False))
            
            if args.output:
                query_tool.export_to_csv(recent_data, args.output)
    
    elif args.action == 'filter':
        if not args.filter:
            print("❌ --filter required for filter action")
            sys.exit(1)
        
        try:
            import json
            filters = json.loads(args.filter)
            filtered_data = query_tool.filter_data(filters)
            
            if not filtered_data.empty:
                print(f"\n🔍 Filtered Results ({len(filtered_data)} found):")
                print("=" * 50)
                print(filtered_data.head(10).to_string(index=False))
                
                if args.output:
                    query_tool.export_to_csv(filtered_data, args.output)
        except json.JSONDecodeError:
            print("❌ Invalid JSON in --filter argument")
            sys.exit(1)
    
    elif args.action == 'export':
        if not args.output:
            print("❌ --output required for export action")
            sys.exit(1)
        
        df = query_tool.load_data()
        query_tool.export_to_csv(df, args.output)

if __name__ == "__main__":
    main()