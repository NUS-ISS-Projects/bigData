import json
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime

import requests
from kafka import KafkaProducer
from loguru import logger
from retrying import retry

# Import the standardized DataRecord from models
from models.data_record import DataRecord


class BaseProducer(ABC):
    """Base class for all data producers"""
    
    def __init__(self, kafka_config: Dict[str, Any], topic: str, source_name: str):
        self.kafka_config = kafka_config
        self.topic = topic
        self.source_name = source_name
        self.producer = None
        self._validate_configuration()
        self._setup_kafka()
        
    def _validate_configuration(self):
        """Validate essential configuration parameters."""
        required_kafka_configs = ['bootstrap_servers']
        for config in required_kafka_configs:
            if config not in self.kafka_config:
                raise ValueError(f"Missing required Kafka configuration: {config}")
        
        if not self.topic:
            raise ValueError("Topic name cannot be empty")
        
        if not self.source_name:
            raise ValueError("Source name cannot be empty")
        
        logger.info(f"Configuration validated for {self.source_name}")
        
    def _setup_kafka(self):
        """Setup Kafka producer"""
        self._initialize_producer()
    
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def _initialize_producer(self):
        """Initialize Kafka producer with retry logic"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Kafka producer initialized for {self.source_name}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def fetch_data(self, url: str, params: Optional[Dict] = None, headers: Optional[Dict] = None) -> Dict[str, Any]:
        """Fetch data from API with retry logic"""
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data from {url}: {e}")
            raise
    
    def send_to_kafka(self, data: DataRecord, key: Optional[str] = None, max_retries: int = 3) -> bool:
        """Send data to Kafka topic with retry logic"""
        for attempt in range(max_retries):
            try:
                if not self.producer:
                    self._initialize_producer()
                
                # Validate data before sending
                if not data:
                    logger.warning("Attempted to send empty data to Kafka")
                    return False
                
                record_dict = data.model_dump()
                future = self.producer.send(
                    self.topic,
                    value=record_dict,
                    key=key
                )
                
                # Wait for the message to be sent
                record_metadata = future.get(timeout=10)
                logger.debug(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
                return True
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed to send message to Kafka: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to send message to Kafka after {max_retries} attempts")
                    return False
                # Reset producer on failure for retry
                self.producer = None
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return False
    
    @abstractmethod
    def extract_data(self) -> None:
        """Extract data from source - to be implemented by subclasses"""
        pass
    
    @abstractmethod
    def transform_data(self, raw_data: Dict[str, Any]) -> DataRecord:
        """Transform raw data into standardized format - to be implemented by subclasses"""
        pass
    
    def run_extraction(self):
        """Run the complete extraction process"""
        try:
            logger.info(f"Starting data extraction for {self.source_name}")
            self.extract_data()
            logger.info(f"Completed data extraction for {self.source_name}")
        except Exception as e:
            logger.error(f"Error during extraction for {self.source_name}: {e}")
            raise
    
    def close(self):
        """Close Kafka producer"""
        if self.producer:
            self.producer.close()
            logger.info(f"Kafka producer closed for {self.source_name}")