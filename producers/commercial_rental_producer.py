import os
import json
from datetime import datetime
from typing import Dict, Any, List, Optional

import requests
from loguru import logger
from dotenv import load_dotenv

from base_producer import BaseProducer
from models.data_record import DataRecord


class CommercialRentalProducer(BaseProducer):
    """Producer for Commercial Rental Index data from data.gov.sg"""
    
    def __init__(self, kafka_config: Dict[str, Any]):
        super().__init__(
            kafka_config=kafka_config,
            topic='commercial-rental-index',
            source_name='DATA_GOV_SG_COMMERCIAL_RENTAL'
        )
        # Commercial Rental Index dataset from data.gov.sg
        self.base_url = "https://data.gov.sg/api/action/datastore_search"
        self.resource_id = "d_862c74b13138382b9f0c50c68d436b95"
        
    def extract_data(self) -> None:
        """Extract commercial rental index data from data.gov.sg API"""
        try:
            # Fetch data in batches
            offset = 0
            limit = 100
            total_records = 0
            
            while True:
                params = {
                    'resource_id': self.resource_id,
                    'limit': limit,
                    'offset': offset
                }
                
                logger.info(f"Fetching Commercial Rental Index data: offset={offset}, limit={limit}")
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
                        # Use quarter and property_type as key for partitioning
                        key = f"{record.get('quarter', '')}_{record.get('property_type', '')}"
                        self.send_to_kafka(transformed_record, key=key)
                    except Exception as e:
                        logger.error(f"Failed to process record {record.get('_id', 'unknown')}: {e}")
                
                logger.info(f"Processed batch: {batch_count} records")
                
                # Check if we have more data
                total_available = raw_data.get('result', {}).get('total', 0)
                if offset + limit >= total_available:
                    logger.info(f"Reached end of data. Total available: {total_available}")
                    break
                
                offset += limit
            
            logger.info(f"Commercial Rental Index extraction completed. Total records processed: {total_records}")
            
        except Exception as e:
            logger.error(f"Error in Commercial Rental Index extraction: {e}")
            raise
    
    def transform_data(self, raw_record: Dict[str, Any]) -> DataRecord:
        """Transform raw commercial rental index data into standardized format"""
        try:
            # Extract and clean the data
            processed_data = {
                'record_id': str(raw_record.get('_id', '')),
                'quarter': raw_record.get('quarter', ''),
                'property_type': raw_record.get('property_type', ''),
                'rental_index': raw_record.get('index', ''),
                'base_period': '1998-Q4',  # Base quarter for index calculation (reference point)
                'base_value': '100'  # Base value for index calculation (reference point)
            }
            
            # Data quality validation
            quality_score = 1.0
            validation_errors = []
            processing_notes = []
            
            # Validate required fields
            if not processed_data['record_id']:
                validation_errors.append("Missing record ID")
                quality_score -= 0.2
            
            if not processed_data['quarter']:
                validation_errors.append("Missing quarter information")
                quality_score -= 0.3
            
            if not processed_data['property_type']:
                validation_errors.append("Missing property type")
                quality_score -= 0.3
            
            if not processed_data['rental_index']:
                validation_errors.append("Missing rental index value")
                quality_score -= 0.4
            else:
                # Validate index is numeric
                try:
                    float(processed_data['rental_index'])
                except (ValueError, TypeError):
                    validation_errors.append("Invalid rental index value (not numeric)")
                    quality_score -= 0.3
            
            # Validate quarter format (should be YYYY-QX)
            quarter = processed_data['quarter']
            if quarter and not (len(quarter) == 7 and quarter[4] == '-' and quarter[5] == 'Q'):
                validation_errors.append("Invalid quarter format")
                quality_score -= 0.2
            
            # Validate property type (should be 'office' or 'retail' based on sample data)
            property_type = processed_data['property_type'].lower()
            if property_type and property_type not in ['office', 'retail']:
                processing_notes.append(f"Unexpected property type: {property_type}")
            
            # Ensure quality score doesn't go below 0
            quality_score = max(0.0, quality_score)
            
            # Create standardized data record
            data_record = DataRecord(
                source=self.source_name,
                timestamp=datetime.now().isoformat(),
                data_type="commercial_rental_index",
                raw_data=raw_record,
                processed_data=processed_data,
                record_id=f"commercial_rental_{processed_data['record_id']}",
                quality_score=quality_score,
                validation_errors=validation_errors,
                processing_notes="; ".join(processing_notes) if processing_notes else None
            )
            
            return data_record
            
        except Exception as e:
            logger.error(f"Error transforming commercial rental record: {e}")
            # Return a minimal record with error information
            return DataRecord(
                source=self.source_name,
                timestamp=datetime.now().isoformat(),
                data_type="commercial_rental_index",
                raw_data=raw_record,
                processed_data={},
                record_id=f"commercial_rental_error_{raw_record.get('_id', 'unknown')}",
                quality_score=0.0,
                validation_errors=[f"Transformation error: {str(e)}"],
                processing_notes="Failed to transform record"
            )
    
    def run_extraction(self) -> None:
        """Run the complete extraction process"""
        logger.info("Starting Commercial Rental Index data extraction...")
        start_time = datetime.now()
        
        try:
            self.extract_data()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"Commercial Rental Index extraction completed successfully in {duration:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Commercial Rental Index extraction failed: {e}")
            raise


if __name__ == "__main__":
    # Configuration
    kafka_config = {
        'bootstrap_servers': [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:30092')]
    }
    
    # Initialize and run producer
    producer = CommercialRentalProducer(kafka_config)
    try:
        producer.run_extraction()
    finally:
        producer.close()