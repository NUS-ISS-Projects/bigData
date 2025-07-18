import os
import json
import time
import csv
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

import requests
import pandas as pd
from loguru import logger
from dotenv import load_dotenv

from base_producer import BaseProducer
from models.data_record import DataRecord


class ACRAProducer(BaseProducer):
    """Producer for ACRA (Accounting and Corporate Regulatory Authority) data"""
    
    def __init__(self, kafka_config: Dict[str, Any]):
        super().__init__(
            kafka_config=kafka_config,
            topic='acra-companies',
            source_name='ACRA'
        )
        # ACRA data is typically available through data.gov.sg
        self.base_url = "https://data.gov.sg/api/action/datastore_search"
        # This should be the actual ACRA dataset resource ID from data.gov.sg
        # The user provided sample data suggests this is the correct structure
        self.resource_id = os.getenv('ACRA_RESOURCE_ID', 'd_3f960c10fed6145404ca7b821f263b87')
        
        # CSV file path for bulk loading historical data
        self.csv_file_path = os.getenv('ACRA_CSV_PATH', '/home/ngtianxun/bigData_project/data/EntitiesRegisteredwithACRA.csv')
        
    def extract_data(self) -> None:
        """Extract company data from ACRA API (incremental updates)"""
        try:
            # Fetch data in batches
            offset = 0
            limit = 1000
            total_records = 0
            
            while True:
                params = {
                    'resource_id': self.resource_id,
                    'limit': limit,
                    'offset': offset
                }
                
                logger.info(f"Fetching ACRA data: offset={offset}, limit={limit}")
                raw_data = self.fetch_data(self.base_url, params=params)
                
                if not raw_data.get('result', {}).get('records'):
                    logger.info("No more records to fetch")
                    break
                
                records = raw_data['result']['records']
                batch_count = len(records)
                total_records += batch_count
                
                # Process each record
                for record in records:
                    try:
                        transformed_record = self.transform_data(record)
                        # Use company UEN as key for partitioning
                        key = record.get('uen', '')
                        self.send_to_kafka(transformed_record, key=key)
                    except Exception as e:
                        logger.error(f"Failed to process record {record.get('uen', 'unknown')}: {e}")
                        continue
                
                logger.info(f"Processed batch of {batch_count} records")
                
                # Check if we've reached the end
                if batch_count < limit:
                    break
                    
                offset += limit
                
                # Add small delay to avoid overwhelming the API
                import time
                time.sleep(0.5)
            
            logger.info(f"Total ACRA records processed: {total_records}")
            
        except Exception as e:
            logger.error(f"Error extracting ACRA data: {e}")
            raise
    
    def extract_csv_data(self, batch_size: int = 10000, max_records: int = None) -> None:
        """Extract historical company data from CSV file (bulk loading)"""
        try:
            if not os.path.exists(self.csv_file_path):
                logger.error(f"CSV file not found: {self.csv_file_path}")
                raise FileNotFoundError(f"CSV file not found: {self.csv_file_path}")
            
            logger.info(f"Starting CSV bulk loading from: {self.csv_file_path}")
            logger.info(f"Batch size: {batch_size}, Max records: {max_records or 'unlimited'}")
            
            total_records = 0
            processed_records = 0
            error_count = 0
            
            with open(self.csv_file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                # Log CSV headers for verification
                logger.info(f"CSV headers: {reader.fieldnames}")
                
                batch = []
                
                for row in reader:
                    total_records += 1
                    
                    # Check max records limit
                    if max_records and total_records > max_records:
                        logger.info(f"Reached max records limit: {max_records}")
                        break
                    
                    batch.append(row)
                    
                    # Process batch when it reaches batch_size
                    if len(batch) >= batch_size:
                        batch_processed, batch_errors = self._process_csv_batch(batch, total_records - len(batch) + 1)
                        processed_records += batch_processed
                        error_count += batch_errors
                        batch = []
                        
                        # Log progress
                        if total_records % (batch_size * 10) == 0:
                            logger.info(f"Progress: {total_records:,} records read, {processed_records:,} processed, {error_count} errors")
                
                # Process remaining records in the last batch
                if batch:
                    batch_processed, batch_errors = self._process_csv_batch(batch, total_records - len(batch) + 1)
                    processed_records += batch_processed
                    error_count += batch_errors
            
            logger.info(f"CSV bulk loading completed:")
            logger.info(f"  Total records read: {total_records:,}")
            logger.info(f"  Successfully processed: {processed_records:,}")
            logger.info(f"  Errors: {error_count}")
            logger.info(f"  Success rate: {(processed_records/total_records*100):.2f}%" if total_records > 0 else "  Success rate: 0%")
            
        except Exception as e:
            logger.error(f"Error during CSV bulk loading: {e}")
            raise
    
    def _process_csv_batch(self, batch: List[Dict[str, Any]], start_record_num: int) -> tuple[int, int]:
        """Process a batch of CSV records"""
        processed_count = 0
        error_count = 0
        
        for i, record in enumerate(batch):
            try:
                # Transform and send to Kafka
                transformed_record = self.transform_data(record, source_type='csv')
                key = record.get('uen', f'csv_{start_record_num + i}')
                self.send_to_kafka(transformed_record, key=key)
                processed_count += 1
                
            except Exception as e:
                error_count += 1
                logger.error(f"Failed to process CSV record {start_record_num + i} (UEN: {record.get('uen', 'unknown')}): {e}")
                continue
        
        return processed_count, error_count
    
    def transform_data(self, raw_data: Dict[str, Any], source_type: str = 'api') -> DataRecord:
        """Transform ACRA raw data into standardized format"""
        try:
            # Extract key fields based on CSV structure
            # CSV headers: uen,issuance_agency_desc,uen_status_desc,entity_name,entity_type_desc,uen_issue_date,reg_street_name,reg_postal_code
            processed_data = {
                'entity_name': raw_data.get('entity_name', ''),
                'uen': raw_data.get('uen', ''),
                'entity_type_desc': raw_data.get('entity_type_desc', ''),
                'uen_issue_date': raw_data.get('uen_issue_date', ''),
                'uen_status_desc': raw_data.get('uen_status_desc', ''),
                'issuance_agency_desc': raw_data.get('issuance_agency_desc', ''),
                'reg_street_name': raw_data.get('reg_street_name', ''),
                'reg_postal_code': raw_data.get('reg_postal_code', ''),
                'unit_no': raw_data.get('unit_no', ''),
                'primary_ssic_description': raw_data.get('primary_ssic_description', ''),
                'secondary_ssic_description': raw_data.get('secondary_ssic_description', ''),
                'paid_up_capital1_ordinary': raw_data.get('paid_up_capital1_ordinary', ''),
                'registration_incorporation_date': raw_data.get('registration_incorporation_date', ''),
                'last_updated': datetime.now().isoformat()
            }
            
            record = DataRecord(
                source=self.source_name,
                timestamp=datetime.now(),
                data_type="company",
                raw_data=raw_data,
                processed_data=processed_data,
                record_id=raw_data.get('uen'),
                processing_notes=f'Extracted via {source_type}'
            )
            
            # Calculate quality score
            record.validate_quality()
            
            return record
            
        except Exception as e:
            logger.error(f"Error transforming ACRA data: {e}")
            return DataRecord(
                source=self.source_name,
                timestamp=datetime.now(),
                data_type="company",
                raw_data=raw_data,
                processed_data={},
                validation_errors=[str(e)]
            )


    def run_hybrid_extraction(self, csv_batch_size: int = 10000, csv_max_records: int = None):
        """Run hybrid extraction: CSV bulk load first, then API incremental updates"""
        try:
            logger.info("Starting ACRA hybrid extraction process")
            
            # Step 1: Bulk load historical data from CSV
            logger.info("Phase 1: Bulk loading historical data from CSV")
            self.extract_csv_data(batch_size=csv_batch_size, max_records=csv_max_records)
            
            # Step 2: Incremental updates from API
            logger.info("Phase 2: Fetching incremental updates from API")
            self.extract_data()
            
            logger.info("ACRA hybrid extraction completed successfully")
            
        except Exception as e:
            logger.error(f"Error during hybrid extraction: {e}")
            raise


if __name__ == "__main__":
    import sys
    
    # Configuration
    kafka_config = {
        'bootstrap_servers': [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')]
    }
    
    # Initialize producer
    producer = ACRAProducer(kafka_config)
    
    try:
        # Check command line arguments for mode selection
        if len(sys.argv) > 1:
            mode = sys.argv[1].lower()
            
            if mode == 'csv':
                # CSV bulk loading only
                max_records = int(sys.argv[2]) if len(sys.argv) > 2 else None
                logger.info(f"Running CSV bulk loading mode (max_records: {max_records})")
                producer.extract_csv_data(max_records=max_records)
                
            elif mode == 'api':
                # API incremental updates only
                logger.info("Running API incremental mode")
                producer.run_extraction()
                
            elif mode == 'hybrid':
                # Full hybrid approach
                max_records = int(sys.argv[2]) if len(sys.argv) > 2 else None
                logger.info(f"Running hybrid mode (CSV max_records: {max_records})")
                producer.run_hybrid_extraction(csv_max_records=max_records)
                
            else:
                logger.error(f"Unknown mode: {mode}. Use 'csv', 'api', or 'hybrid'")
                sys.exit(1)
        else:
            # Default: API mode for backward compatibility
            logger.info("Running default API mode")
            producer.run_extraction()
            
    finally:
        producer.close()