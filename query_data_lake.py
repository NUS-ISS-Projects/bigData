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
    
    def __init__(self, endpoint_url='http://192.168.49.2:30900', dataset='acra'):
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
        self.dataset = dataset
        
        # Set dataset-specific configuration
        if dataset == 'acra':
            self.prefix = 'acra_companies/'
            self.default_search_column = 'entity_name'
        elif dataset == 'singstat':
            self.prefix = 'singstat_economics/'
            self.default_search_column = 'data_series_title'
        elif dataset == 'ura':
            self.prefix = 'ura_geospatial/'
            self.default_search_column = 'project'
        else:
            raise ValueError(f"Unsupported dataset: {dataset}. Use 'acra', 'singstat', or 'ura'")
        
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
    
    def get_summary(self, max_files: Optional[int] = None) -> Dict[str, Any]:
        """Get a summary of the data"""
        df = self.load_data(max_files=max_files)  # Load all data by default
        
        if df.empty:
            return {"error": "No data available"}
        
        summary = {
            "total_rows_sampled": len(df),
            "columns": list(df.columns),
            "data_types": df.dtypes.to_dict(),
            "non_null_counts": df.count().to_dict(),
            "sample_data": df.head(3).to_dict('records')
        }
        
        # Add dataset-specific business insights
        if self.dataset == 'acra':
            if 'entity_type' in df.columns:
                summary['entity_types'] = df['entity_type'].value_counts().head(10).to_dict()
            
            if 'reg_postal_code' in df.columns:
                summary['top_postal_codes'] = df['reg_postal_code'].value_counts().head(10).to_dict()
                
        elif self.dataset == 'singstat':
            if 'level_1' in df.columns:
                summary['top_categories'] = df['level_1'].value_counts().head(10).to_dict()
            
            if 'unit' in df.columns:
                summary['measurement_units'] = df['unit'].value_counts().head(10).to_dict()
                
            if 'source' in df.columns:
                summary['data_sources'] = df['source'].value_counts().head(10).to_dict()
                
            if 'period' in df.columns:
                summary['latest_periods'] = df['period'].value_counts().head(10).to_dict()
                
        elif self.dataset == 'ura':
            if 'type_of_area' in df.columns:
                summary['area_types'] = df['type_of_area'].value_counts().head(10).to_dict()
            
            if 'tenure' in df.columns:
                summary['tenure_types'] = df['tenure'].value_counts().head(10).to_dict()
        
        if 'bronze_ingestion_timestamp' in df.columns:
            df['bronze_ingestion_timestamp'] = pd.to_datetime(df['bronze_ingestion_timestamp'])
            summary['ingestion_date_range'] = {
                'earliest': df['bronze_ingestion_timestamp'].min().isoformat(),
                'latest': df['bronze_ingestion_timestamp'].max().isoformat()
            }
        
        return summary
    
    def search_entities(self, search_term: str, column: str = None) -> pd.DataFrame:
        """Search for entities by name or other fields"""
        if column is None:
            column = self.default_search_column
            
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
    
    def analyze_economic_indicators(self) -> Dict[str, Any]:
        """Analyze SingStat economic indicators (SingStat dataset only)"""
        if self.dataset != 'singstat':
            print("❌ Economic indicators analysis only available for SingStat dataset")
            return {}
        
        df = self.load_data()
        if df.empty:
            return {}
        
        analysis = {}
        
        # Analyze by categories
        if 'level_1' in df.columns:
            analysis['categories'] = df['level_1'].value_counts().to_dict()
        
        # Analyze data frequency
        if 'period' in df.columns:
            # Extract year from period for trend analysis
            df['year'] = df['period'].astype(str).str[:4]
            analysis['data_by_year'] = df['year'].value_counts().sort_index().to_dict()
        
        # Analyze value ranges
        if 'value' in df.columns:
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            analysis['value_statistics'] = {
                'mean': df['value'].mean(),
                'median': df['value'].median(),
                'min': df['value'].min(),
                'max': df['value'].max(),
                'std': df['value'].std()
            }
        
        return analysis
    
    def get_time_series_data(self, series_title: str) -> pd.DataFrame:
        """Get time series data for a specific indicator (SingStat dataset only)"""
        if self.dataset != 'singstat':
            print("❌ Time series analysis only available for SingStat dataset")
            return pd.DataFrame()
        
        df = self.load_data()
        if df.empty:
            return df
        
        # Filter by series title
        if 'data_series_title' in df.columns:
            mask = df['data_series_title'].str.contains(series_title, case=False, na=False)
            series_data = df[mask].copy()
            
            if not series_data.empty and 'period' in series_data.columns and 'value' in series_data.columns:
                # Sort by period for time series
                series_data = series_data.sort_values('period')
                series_data['value'] = pd.to_numeric(series_data['value'], errors='coerce')
                
            return series_data
        
        return pd.DataFrame()

def demo():
    """Example usage of the Delta Lake query tool"""
    print("=== Delta Lake Query Tool Demo ===")
    
    # Demo with ACRA dataset
    print("\n🏢 ACRA Dataset Analysis:")
    acra_tool = DeltaLakeQueryTool(dataset='acra')
    
    acra_summary = acra_tool.get_summary()
    print(f"Total ACRA records: {acra_summary.get('total_records', 0)}")
    
    # Search for companies
    acra_results = acra_tool.search_entities('PTE LTD')
    if not acra_results.empty:
        print(f"Found {len(acra_results)} companies with 'PTE LTD'")
    
    # Demo with SingStat dataset
    print("\n📊 SingStat Dataset Analysis:")
    singstat_tool = DeltaLakeQueryTool(dataset='singstat')
    
    singstat_summary = singstat_tool.get_summary()
    print(f"Total SingStat records: {singstat_summary.get('total_records', 0)}")
    
    # Analyze economic indicators
    econ_analysis = singstat_tool.analyze_economic_indicators()
    if econ_analysis:
        print("Economic indicators analysis:")
        if 'categories' in econ_analysis:
            print(f"Top categories: {list(econ_analysis['categories'].keys())[:5]}")
    
    # Search for GDP data
    gdp_data = singstat_tool.search_entities('GDP')
    if not gdp_data.empty:
        print(f"Found {len(gdp_data)} GDP-related records")
    
    # Get time series for a specific indicator
    gdp_series = singstat_tool.get_time_series_data('Gross Domestic Product')
    if not gdp_series.empty:
        print(f"GDP time series data: {len(gdp_series)} records")
    
    # Demo with URA dataset
    print("\n🏘️ URA Dataset Analysis:")
    ura_tool = DeltaLakeQueryTool(dataset='ura')
    
    ura_summary = ura_tool.get_summary()
    print(f"Total URA records: {ura_summary.get('total_records', 0)}")
    
    # Search for property data
    ura_results = ura_tool.search_entities('Condo')
    if not ura_results.empty:
        print(f"Found {len(ura_results)} condo-related records")

def main():
    """Command-line interface for the query tool"""
    parser = argparse.ArgumentParser(description='Query Delta Lake data')
    parser.add_argument('action', choices=['summary', 'search', 'recent', 'filter', 'export', 'demo'],
                       help='Action to perform')
    parser.add_argument('--dataset', choices=['acra', 'singstat', 'ura'], default='acra',
                       help='Dataset to query')
    parser.add_argument('--search-term', help='Term to search for')
    parser.add_argument('--search-column', help='Column to search in')
    parser.add_argument('--hours', type=int, default=24, help='Hours for recent data')
    parser.add_argument('--filter', help='JSON string of filters')
    parser.add_argument('--output', help='Output CSV filename')
    parser.add_argument('--endpoint', default='http://192.168.49.2:30900', help='MinIO endpoint')
    
    args = parser.parse_args()
    
    if args.action == 'demo':
        demo()
        return
    
    # Initialize query tool
    query_tool = DeltaLakeQueryTool(endpoint_url=args.endpoint, dataset=args.dataset)
    
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