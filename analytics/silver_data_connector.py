#!/usr/bin/env python3
"""
Silver Layer Data Connector
Connects to actual silver layer data sources (MinIO/S3) using DuckDB
Integrates with the LLM Economic Intelligence system
"""

import os
import sys
import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import duckdb
import json
from dataclasses import dataclass

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class DataSourceConfig:
    """Configuration for data source connections"""
    s3_endpoint: str = "localhost:9000"  # MinIO endpoint (no protocol for DuckDB)
    s3_access_key: str = "admin"
    s3_secret_key: str = "password123"
    s3_bucket: str = "silver"
    s3_region: str = "us-east-1"
    use_ssl: bool = False

class SilverLayerConnector:
    """Connects to silver layer data using DuckDB"""
    
    def __init__(self, config: DataSourceConfig = None):
        self.config = config or DataSourceConfig()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.conn = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize DuckDB connection with S3 configuration"""
        try:
            self.conn = duckdb.connect(':memory:')
            
            # Install and load required extensions
            self.conn.execute("INSTALL httpfs;")
            self.conn.execute("LOAD httpfs;")
            
            # Configure S3 settings for MinIO
            self.conn.execute(f"""
                SET s3_endpoint='{self.config.s3_endpoint}';
                SET s3_access_key_id='{self.config.s3_access_key}';
                SET s3_secret_access_key='{self.config.s3_secret_key}';
                SET s3_use_ssl={'true' if self.config.use_ssl else 'false'};
                SET s3_url_style='path';
            """)
            
            self.logger.info("DuckDB connection initialized with S3/MinIO configuration")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize DuckDB connection: {e}")
            self.conn = None
    
    def is_connected(self) -> bool:
        """Check if connection is available"""
        return self.conn is not None
    
    def load_acra_companies(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Load ACRA companies data from silver layer"""
        if not self.is_connected():
            self.logger.error("No database connection available")
            return pd.DataFrame()
        
        try:
            # Construct S3 path for ACRA data (Delta Lake format)
            s3_path = f"s3://{self.config.s3_bucket}/acra_companies_clean/"
            
            query = f"""
            SELECT *
            FROM read_parquet('{s3_path}*.parquet')
            ORDER BY uen_issue_date DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            df = self.conn.execute(query).df()
            self.logger.info(f"Loaded {len(df)} ACRA company records from silver layer")
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading ACRA data: {e}")
            # Return sample data as fallback
            return self._get_sample_acra_data()
    
    def load_economic_indicators(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Load economic indicators data from silver layer"""
        if not self.is_connected():
            self.logger.error("No database connection available")
            return pd.DataFrame()
        
        try:
            # Construct S3 path for economic indicators
            s3_path = f"s3://{self.config.s3_bucket}/singstat_economics_clean/"
            
            query = f"""
            SELECT *
            FROM read_parquet('{s3_path}*.parquet')
            ORDER BY period DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            df = self.conn.execute(query).df()
            self.logger.info(f"Loaded {len(df)} economic indicator records from silver layer")
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading economic indicators: {e}")
            # Return sample data as fallback
            return self._get_sample_economic_data()
    
    def load_government_expenditure(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Load government expenditure data from silver layer"""
        if not self.is_connected():
            self.logger.error("No database connection available")
            return pd.DataFrame()
        
        try:
            # Construct S3 path for government expenditure
            s3_path = f"s3://{self.config.s3_bucket}/government_expenditure_clean/"
            
            query = f"""
            SELECT *
            FROM read_parquet('{s3_path}*.parquet')
            ORDER BY financial_year DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            df = self.conn.execute(query).df()
            self.logger.info(f"Loaded {len(df)} government expenditure records from silver layer")
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading government expenditure: {e}")
            # Return sample data as fallback
            return self._get_sample_government_data()
    
    def load_property_market(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Load URA property market data from silver layer"""
        if not self.is_connected():
            self.logger.error("No database connection available")
            return pd.DataFrame()
        
        try:
            s3_path = f"s3://{self.config.s3_bucket}/ura_geospatial_clean/"
            
            query = f"""
            SELECT *
            FROM read_parquet('{s3_path}*.parquet')
            ORDER BY ref_period DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            df = self.conn.execute(query).df()
            self.logger.info(f"Loaded {len(df)} property market records from silver layer")
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading property market data: {e}")
            return self._get_sample_property_data()
    
    def load_commercial_rental(self, limit: Optional[int] = None) -> pd.DataFrame:
        """Load commercial rental data from silver layer"""
        if not self.is_connected():
            self.logger.error("No database connection available")
            return pd.DataFrame()
        
        try:
            # Construct S3 path for commercial rental data
            s3_path = f"s3://{self.config.s3_bucket}/commercial_rental_index_clean/"
            
            query = f"""
            SELECT *
            FROM read_parquet('{s3_path}*.parquet')
            ORDER BY quarter DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            df = self.conn.execute(query).df()
            self.logger.info(f"Loaded {len(df)} commercial rental records from silver layer")
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading commercial rental data: {e}")
            # Return sample data as fallback
            return self._get_sample_commercial_data()
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get summary statistics for all silver layer datasets"""
        summary = {
            "timestamp": datetime.now().isoformat(),
            "data_sources": {},
            "total_records": 0,
            "data_quality": {}
        }
        
        try:
            # ACRA Companies
            acra_df = self.load_acra_companies(limit=1000)
            if not acra_df.empty:
                summary["data_sources"]["acra_companies"] = {
                    "record_count": len(acra_df),
                    "date_range": {
                        "earliest": acra_df['registration_date'].min().isoformat() if 'registration_date' in acra_df.columns else None,
                        "latest": acra_df['registration_date'].max().isoformat() if 'registration_date' in acra_df.columns else None
                    },
                    "avg_quality_score": acra_df['data_quality_score'].mean() if 'data_quality_score' in acra_df.columns else None,
                    "industry_categories": acra_df['industry_category'].nunique() if 'industry_category' in acra_df.columns else 0
                }
            
            # Economic Indicators
            econ_df = self.load_economic_indicators(limit=1000)
            if not econ_df.empty:
                summary["data_sources"]["economic_indicators"] = {
                    "record_count": len(econ_df),
                    "indicators_count": econ_df['indicator_name'].nunique() if 'indicator_name' in econ_df.columns else 0,
                    "avg_quality_score": econ_df['data_quality_score'].mean() if 'data_quality_score' in econ_df.columns else None,
                    "latest_period": econ_df['period'].max() if 'period' in econ_df.columns else None
                }
            
            # Government Expenditure
            gov_df = self.load_government_expenditure(limit=1000)
            if not gov_df.empty:
                summary["data_sources"]["government_expenditure"] = {
                    "record_count": len(gov_df),
                    "total_amount_million": gov_df['amount_million_sgd'].sum() if 'amount_million_sgd' in gov_df.columns else 0,
                    "categories_count": gov_df['category'].nunique() if 'category' in gov_df.columns else 0,
                    "avg_quality_score": gov_df['data_quality_score'].mean() if 'data_quality_score' in gov_df.columns else None
                }
            
            # Property Market
            prop_df = self.load_property_market(limit=1000)
            if not prop_df.empty:
                summary["data_sources"]["property_market"] = {
                    "record_count": len(prop_df),
                    "districts_count": prop_df['district'].nunique() if 'district' in prop_df.columns else 0,
                    "avg_rental_psf": prop_df['median_rental_psf'].mean() if 'median_rental_psf' in prop_df.columns else None,
                    "avg_quality_score": prop_df['data_quality_score'].mean() if 'data_quality_score' in prop_df.columns else None
                }
            
            # Calculate totals
            summary["total_records"] = sum(
                source.get("record_count", 0) 
                for source in summary["data_sources"].values()
            )
            
            # Overall data quality
            quality_scores = []
            for source in summary["data_sources"].values():
                if source.get("avg_quality_score"):
                    quality_scores.append(source["avg_quality_score"])
            
            if quality_scores:
                summary["data_quality"] = {
                    "overall_avg_score": sum(quality_scores) / len(quality_scores),
                    "min_score": min(quality_scores),
                    "max_score": max(quality_scores)
                }
            
        except Exception as e:
            self.logger.error(f"Error generating data summary: {e}")
            summary["error"] = str(e)
        
        return summary
    
    def execute_custom_query(self, query: str) -> pd.DataFrame:
        """Execute custom SQL query on silver layer data"""
        if not self.is_connected():
            self.logger.error("No database connection available")
            return pd.DataFrame()
        
        try:
            df = self.conn.execute(query).df()
            self.logger.info(f"Custom query executed, returned {len(df)} records")
            return df
        except Exception as e:
            self.logger.error(f"Error executing custom query: {e}")
            return pd.DataFrame()
    
    def close_connection(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.info("Database connection closed")
    
    # Fallback sample data methods
    def _get_sample_acra_data(self) -> pd.DataFrame:
        """Get sample ACRA data when real data is not available"""
        sample_data = {
            'company_id': ['C001', 'C002', 'C003', 'C004', 'C005'],
            'company_name': [
                'Tech Innovations Pte Ltd', 
                'Green Energy Solutions', 
                'Financial Services Co',
                'Digital Marketing Hub',
                'Sustainable Foods Pte Ltd'
            ],
            'industry_category': ['Technology', 'Energy', 'Finance', 'Marketing', 'Food & Beverage'],
            'registration_date': [
                '2023-01-15', '2023-02-20', '2023-03-10', '2023-04-05', '2023-05-12'
            ],
            'status': ['Active', 'Active', 'Ceased', 'Active', 'Active'],
            'data_quality_score': [0.95, 0.88, 0.92, 0.89, 0.94]
        }
        df = pd.DataFrame(sample_data)
        df['registration_date'] = pd.to_datetime(df['registration_date'])
        return df
    
    def _get_sample_economic_data(self) -> pd.DataFrame:
        """Get sample economic data when real data is not available"""
        sample_data = {
            'indicator_name': ['GDP Growth Rate', 'Inflation Rate', 'Unemployment Rate', 'Export Growth', 'Manufacturing Index'],
            'value': [3.2, 2.1, 2.8, 4.5, 102.3],
            'period': ['2023-Q4', '2023-Q4', '2023-Q4', '2023-Q4', '2023-Q4'],
            'unit': ['%', '%', '%', '%', 'Index'],
            'data_quality_score': [0.98, 0.95, 0.93, 0.91, 0.89]
        }
        return pd.DataFrame(sample_data)
    
    def _get_sample_government_data(self) -> pd.DataFrame:
        """Get sample government data when real data is not available"""
        sample_data = {
            'category': ['Infrastructure', 'Education', 'Healthcare', 'Defense', 'Social Services'],
            'amount_million_sgd': [1500.0, 2200.0, 1800.0, 1200.0, 900.0],
            'fiscal_year': ['2023', '2023', '2023', '2023', '2023'],
            'expenditure_type': ['Development', 'Operating', 'Operating', 'Operating', 'Operating'],
            'data_quality_score': [0.96, 0.94, 0.97, 0.92, 0.88]
        }
        return pd.DataFrame(sample_data)
    
    def _get_sample_property_data(self) -> pd.DataFrame:
        """Get sample property data when real data is not available"""
        sample_data = {
            'district': ['Central', 'East', 'West', 'North', 'Northeast'],
            'property_type': ['Condo', 'HDB', 'Landed', 'Condo', 'HDB'],
            'median_rental_psf': [4.2, 2.8, 3.5, 3.8, 2.9],
            'quarter': ['2023-Q4', '2023-Q4', '2023-Q4', '2023-Q4', '2023-Q4'],
            'data_quality_score': [0.91, 0.89, 0.93, 0.87, 0.85]
        }
        return pd.DataFrame(sample_data)
    
    def _get_sample_commercial_data(self) -> pd.DataFrame:
        """Get sample commercial data when real data is not available"""
        sample_data = {
            'district': ['CBD', 'Orchard', 'Jurong', 'Tampines', 'Woodlands'],
            'property_type': ['Office', 'Retail', 'Industrial', 'Office', 'Industrial'],
            'rental_price_psf': [8.5, 12.0, 3.2, 6.8, 2.9],
            'quarter': ['2023-Q4', '2023-Q4', '2023-Q4', '2023-Q4', '2023-Q4'],
            'data_quality_score': [0.94, 0.91, 0.88, 0.86, 0.83]
        }
        return pd.DataFrame(sample_data)

def test_connection():
    """Test the silver layer connection"""
    print("🔗 Testing Silver Layer Connection")
    print("=" * 40)
    
    connector = SilverLayerConnector()
    
    if connector.is_connected():
        print("✅ Database connection established")
        
        # Test data loading
        print("\n📊 Testing data loading...")
        
        acra_df = connector.load_acra_companies(limit=5)
        print(f"   ACRA Companies: {len(acra_df)} records")
        
        econ_df = connector.load_economic_indicators(limit=5)
        print(f"   Economic Indicators: {len(econ_df)} records")
        
        gov_df = connector.load_government_expenditure(limit=5)
        print(f"   Government Expenditure: {len(gov_df)} records")
        
        prop_df = connector.load_property_market(limit=5)
        print(f"   Property Market: {len(prop_df)} records")
        
        # Get summary
        print("\n📈 Data Summary:")
        summary = connector.get_data_summary()
        print(f"   Total Records: {summary.get('total_records', 0)}")
        print(f"   Data Sources: {len(summary.get('data_sources', {}))}")
        
        if summary.get('data_quality'):
            print(f"   Avg Quality Score: {summary['data_quality'].get('overall_avg_score', 0):.2f}")
        
    else:
        print("❌ Database connection failed")
        print("   Using sample data fallback")
    
    connector.close_connection()
    print("\n🔚 Connection test completed")

if __name__ == "__main__":
    test_connection()