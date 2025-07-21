#!/usr/bin/env python3
"""
Government Expenditure Data Producer
Fetches government expenditure data from data.gov.sg API and streams to Kafka
"""

import json
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

from loguru import logger
from retrying import retry

from base_producer import BaseProducer
from models.data_record import DataRecord


class GovernmentExpenditureProducer(BaseProducer):
    """Producer for Government Expenditure data from data.gov.sg"""
    
    def __init__(self, kafka_config: Dict[str, Any]):
        super().__init__(
            kafka_config=kafka_config,
            topic="government-expenditure",
            source_name="DataGovSG-Expenditure"
        )
        
        # API configuration
        self.base_url = "https://data.gov.sg/api/action/datastore_search"
        self.dataset_id = "d_6a804a6860b5c51af08df679a71bc190"
        self.batch_size = 100  # API default limit
        
        logger.info(f"Government Expenditure Producer initialized for dataset: {self.dataset_id}")
    
    def _build_api_url(self, offset: int = 0, limit: int = 100) -> str:
        """Build API URL with pagination parameters"""
        return f"{self.base_url}?resource_id={self.dataset_id}&offset={offset}&limit={limit}"
    
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def _fetch_batch(self, offset: int = 0) -> Dict[str, Any]:
        """Fetch a batch of data from the API"""
        url = self._build_api_url(offset=offset, limit=self.batch_size)
        
        try:
            logger.info(f"Fetching batch from offset {offset}")
            response_data = self.fetch_data(url)
            
            if not response_data.get('success', False):
                raise Exception(f"API returned unsuccessful response: {response_data}")
            
            return response_data.get('result', {})
            
        except Exception as e:
            logger.error(f"Failed to fetch batch at offset {offset}: {e}")
            raise
    
    def _validate_record(self, record: Dict[str, Any]) -> List[str]:
        """Validate a single expenditure record"""
        errors = []
        
        # Required fields validation
        required_fields = ['financial_year', 'actual_revised_estimated', 'type', 'category', 'class', 'amount_in_million']
        for field in required_fields:
            if field not in record or record[field] is None or str(record[field]).strip() == '':
                errors.append(f"Missing or empty required field: {field}")
        
        # Data type validations
        if 'financial_year' in record:
            try:
                year = int(record['financial_year'])
                if year < 1900 or year > 2100:
                    errors.append(f"Invalid financial year: {year}")
            except (ValueError, TypeError):
                errors.append(f"Invalid financial year format: {record['financial_year']}")
        
        if 'amount_in_million' in record:
            try:
                amount = float(record['amount_in_million'])
                if amount < 0:
                    errors.append(f"Negative amount not expected: {amount}")
            except (ValueError, TypeError):
                errors.append(f"Invalid amount format: {record['amount_in_million']}")
        
        # Enum validations
        valid_types = ['Operating', 'Development']
        if 'type' in record and record['type'] not in valid_types:
            errors.append(f"Invalid expenditure type: {record['type']}. Expected one of {valid_types}")
        
        valid_statuses = ['Actual', 'Revised', 'Estimated']
        if 'actual_revised_estimated' in record and record['actual_revised_estimated'] not in valid_statuses:
            errors.append(f"Invalid status: {record['actual_revised_estimated']}. Expected one of {valid_statuses}")
        
        return errors
    
    def transform_data(self, raw_record: Dict[str, Any]) -> DataRecord:
        """Transform raw expenditure data into standardized DataRecord format"""
        
        # Validate the record
        validation_errors = self._validate_record(raw_record)
        
        # Generate unique record ID
        record_id = f"gov_exp_{raw_record.get('_id', 'unknown')}_{int(time.time())}"
        
        # Process the data
        processed_data = {
            'record_id': record_id,
            'financial_year': raw_record.get('financial_year'),
            'status': raw_record.get('actual_revised_estimated'),
            'expenditure_type': raw_record.get('type'),
            'category': raw_record.get('category'),
            'expenditure_class': raw_record.get('class'),
            'amount_million_sgd': raw_record.get('amount_in_million'),
            'data_source': self.source_name,
            'extraction_timestamp': datetime.now().isoformat()
        }
        
        # Convert amount to float if possible
        if processed_data['amount_million_sgd']:
            try:
                processed_data['amount_million_sgd'] = float(processed_data['amount_million_sgd'])
                processed_data['amount_sgd'] = processed_data['amount_million_sgd'] * 1_000_000
            except (ValueError, TypeError):
                processed_data['amount_sgd'] = None
        
        # Convert year to int if possible
        if processed_data['financial_year']:
            try:
                processed_data['financial_year'] = int(processed_data['financial_year'])
            except (ValueError, TypeError):
                pass
        
        # Create DataRecord
        data_record = DataRecord(
            source=self.source_name,
            timestamp=datetime.now(),
            data_type="government_expenditure",
            raw_data=raw_record,
            processed_data=processed_data,
            record_id=record_id,
            validation_errors=validation_errors if validation_errors else None,
            processing_notes=f"Processed government expenditure record for FY {processed_data['financial_year']}"
        )
        
        # Calculate quality score
        data_record.validate_quality()
        
        return data_record
    
    def extract_data(self) -> None:
        """Extract all government expenditure data with pagination"""
        logger.info("Starting government expenditure data extraction")
        
        offset = 0
        total_records = 0
        processed_records = 0
        failed_records = 0
        
        try:
            while True:
                # Fetch batch
                batch_data = self._fetch_batch(offset)
                records = batch_data.get('records', [])
                
                if not records:
                    logger.info("No more records to process")
                    break
                
                # Get total count from first batch
                if offset == 0:
                    total_records = batch_data.get('total', len(records))
                    logger.info(f"Total records to process: {total_records}")
                
                # Process each record in the batch
                for record in records:
                    try:
                        # Transform data
                        data_record = self.transform_data(record)
                        
                        # Generate key for Kafka partitioning
                        key = f"{data_record.processed_data.get('financial_year', 'unknown')}"
                        
                        # Send to Kafka
                        if self.send_to_kafka(data_record, key=key):
                            processed_records += 1
                            if processed_records % 50 == 0:
                                logger.info(f"Processed {processed_records}/{total_records} records")
                        else:
                            failed_records += 1
                            logger.warning(f"Failed to send record {record.get('_id', 'unknown')} to Kafka")
                    
                    except Exception as e:
                        failed_records += 1
                        logger.error(f"Error processing record {record.get('_id', 'unknown')}: {e}")
                
                # Move to next batch
                offset += len(records)
                
                # Check if we've processed all records
                if offset >= total_records:
                    break
                
                # Rate limiting - be respectful to the API
                time.sleep(0.1)
            
            logger.info(f"Extraction completed. Processed: {processed_records}, Failed: {failed_records}, Total: {total_records}")
            
        except Exception as e:
            logger.error(f"Critical error during data extraction: {e}")
            raise
        
        finally:
            # Ensure producer is properly closed
            if self.producer:
                self.producer.flush()


def main():
    """Main function for testing the producer"""
    
    # Kafka configuration
    kafka_config = {
        'bootstrap_servers': ['localhost:9092']
    }
    
    # Create and run producer
    producer = GovernmentExpenditureProducer(kafka_config)
    
    try:
        producer.run_extraction()
    except KeyboardInterrupt:
        logger.info("Extraction interrupted by user")
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()