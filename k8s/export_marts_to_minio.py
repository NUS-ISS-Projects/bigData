#!/usr/bin/env python3
import duckdb
import pandas as pd
from minio import Minio
from datetime import datetime
import os

# Initialize MinIO client
client = Minio(
    'minio-service.economic-observatory.svc.cluster.local:9000',
    access_key='admin',
    secret_key='password123',
    secure=False
)

# Connect to DuckDB
conn = duckdb.connect('/data/economic_intelligence_prod.duckdb')

# Get timestamp for file naming
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

# Export each mart model
marts = ['mart_business_intelligence', 'mart_economic_analysis', 'mart_geospatial_analysis']

for mart in marts:
    try:
        print(f'Exporting {mart}...')
        
        # Use schema-qualified table name
        full_table_name = f'main_gold.{mart}'
        
        # Check if table exists
        try:
            test_query = conn.execute(f'SELECT COUNT(*) FROM {full_table_name}').fetchone()
            print(f'Table {full_table_name} exists with {test_query[0]} rows')
        except Exception as e:
            print(f'Table {full_table_name} not found: {e}')
            continue
        
        # Extract data
        df = conn.execute(f'SELECT * FROM {full_table_name}').df()
        print(f'Extracted {len(df)} rows from {mart}')
        
        if len(df) == 0:
            print(f'No data in {mart}, skipping export')
            continue
        
        # Save to parquet
        parquet_path = f'/tmp/{mart}_{timestamp}.parquet'
        df.to_parquet(parquet_path, index=False)
        
        # Upload to MinIO
        mart_short = mart.replace('mart_', '')
        object_name = f'{mart_short}/{mart}_{timestamp}.parquet'
        client.fput_object('gold', object_name, parquet_path)
        print(f'Uploaded {object_name} to MinIO gold bucket')
        
        # Clean up temp file
        os.remove(parquet_path)
        
    except Exception as e:
        print(f'Error exporting {mart}: {e}')

conn.close()
print('MinIO export completed successfully')