#!/usr/bin/env python3
"""
Enhanced Test Suite for Economic Intelligence Platform
Tests all enhanced functionality including error handling, validation, and monitoring.
"""

import unittest
import json
import os
import tempfile
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import sys

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'producers'))
sys.path.insert(0, os.path.join(project_root, 'monitoring'))
sys.path.insert(0, os.path.join(project_root, 'models'))

from base_producer import BaseProducer
from acra_producer import ACRAProducer
from singstat_producer import SingStatProducer
from ura_producer import URAProducer
from health_check import HealthChecker, HealthStatus
from data_record import DataRecord

class TestConfigurationValidation(unittest.TestCase):
    """Test configuration validation functionality"""
    
    def setUp(self):
        self.valid_kafka_config = {
            'bootstrap_servers': 'localhost:9092',
            'value_serializer': lambda v: json.dumps(v).encode('utf-8')
        }
    
    def test_valid_configuration(self):
        """Test that valid configuration passes validation"""
        # This should not raise an exception
        try:
            producer = ACRAProducer(
                kafka_config=self.valid_kafka_config,
                topic='test-topic',
                source_name='test-source'
            )
            self.assertIsNotNone(producer)
        except Exception as e:
            self.fail(f"Valid configuration should not raise exception: {e}")
    
    def test_missing_bootstrap_servers(self):
        """Test that missing bootstrap_servers raises ValueError"""
        invalid_config = {}
        
        with self.assertRaises(ValueError) as context:
            ACRAProducer(
                kafka_config=invalid_config,
                topic='test-topic',
                source_name='test-source'
            )
        
        self.assertIn('bootstrap_servers', str(context.exception))
    
    def test_empty_topic_name(self):
        """Test that empty topic name raises ValueError"""
        with self.assertRaises(ValueError) as context:
            ACRAProducer(
                kafka_config=self.valid_kafka_config,
                topic='',
                source_name='test-source'
            )
        
        self.assertIn('Topic name cannot be empty', str(context.exception))
    
    def test_empty_source_name(self):
        """Test that empty source name raises ValueError"""
        with self.assertRaises(ValueError) as context:
            ACRAProducer(
                kafka_config=self.valid_kafka_config,
                topic='test-topic',
                source_name=''
            )
        
        self.assertIn('Source name cannot be empty', str(context.exception))

class TestEnhancedErrorHandling(unittest.TestCase):
    """Test enhanced error handling and retry logic"""
    
    def setUp(self):
        self.kafka_config = {
            'bootstrap_servers': 'localhost:9092',
            'value_serializer': lambda v: json.dumps(v).encode('utf-8')
        }
    
    @patch('producers.base_producer.KafkaProducer')
    def test_kafka_retry_logic(self, mock_kafka_producer):
        """Test that Kafka send operations retry on failure"""
        # Mock producer that fails twice then succeeds
        mock_producer_instance = Mock()
        mock_future = Mock()
        mock_metadata = Mock()
        mock_metadata.topic = 'test-topic'
        mock_metadata.partition = 0
        mock_metadata.offset = 123
        mock_future.get.return_value = mock_metadata
        
        # First two calls fail, third succeeds
        mock_producer_instance.send.side_effect = [
            Exception("Network error"),
            Exception("Timeout error"),
            mock_future
        ]
        
        mock_kafka_producer.return_value = mock_producer_instance
        
        producer = ACRAProducer(
            kafka_config=self.kafka_config,
            topic='test-topic',
            source_name='test-source'
        )
        
        # Create test data record
        test_data = DataRecord(
            source='test',
            timestamp=datetime.now(),
            data_type='test',
            raw_data={'test': 'data'},
            processed_data={'test': 'processed'}
        )
        
        # This should eventually succeed after retries
        result = producer.send_to_kafka(test_data, max_retries=3)
        self.assertTrue(result)
    
    @patch('producers.base_producer.KafkaProducer')
    def test_kafka_max_retries_exceeded(self, mock_kafka_producer):
        """Test that max retries are respected"""
        # Mock producer that always fails
        mock_producer_instance = Mock()
        mock_producer_instance.send.side_effect = Exception("Persistent error")
        mock_kafka_producer.return_value = mock_producer_instance
        
        producer = ACRAProducer(
            kafka_config=self.kafka_config,
            topic='test-topic',
            source_name='test-source'
        )
        
        test_data = DataRecord(
            source='test',
            timestamp=datetime.now(),
            data_type='test',
            raw_data={'test': 'data'},
            processed_data={'test': 'processed'}
        )
        
        # This should fail after max retries
        result = producer.send_to_kafka(test_data, max_retries=2)
        self.assertFalse(result)
        
        # Verify that send was called the expected number of times
        self.assertEqual(mock_producer_instance.send.call_count, 2)

class TestURAEnhancements(unittest.TestCase):
    """Test URA producer enhancements"""
    
    def setUp(self):
        self.kafka_config = {
            'bootstrap_servers': 'localhost:9092',
            'value_serializer': lambda v: json.dumps(v).encode('utf-8')
        }
        
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {
            'URA_ACCESS_KEY': 'test-access-key',
            'URA_TOKEN_URL': 'https://test.ura.gov.sg/token',
            'URA_DATA_URL': 'https://test.ura.gov.sg/data'
        })
        self.env_patcher.start()
    
    def tearDown(self):
        self.env_patcher.stop()
    
    def test_ura_services_include_car_park(self):
        """Test that URA services include car park services"""
        producer = URAProducer(
            kafka_config=self.kafka_config,
            topic='test-topic',
            source_name='test-source'
        )
        
        # Check that car park services are included
        self.assertIn('Car_Park_Availability', producer.services)
        self.assertIn('Car_Park_Details', producer.services)
    
    def test_car_park_parameter_handling(self):
        """Test that car park services don't get refPeriod parameter"""
        producer = URAProducer(
            kafka_config=self.kafka_config,
            topic='test-topic',
            source_name='test-source'
        )
        
        # Mock the _extract_service_data method to capture parameters
        with patch.object(producer, '_extract_service_data') as mock_extract:
            mock_extract.return_value = []
            
            # Test car park service
            producer.fetch_and_produce('Car_Park_Availability')
            
            # Verify that refPeriod was not added
            call_args = mock_extract.call_args
            params = call_args[0][1]  # Second argument should be params
            self.assertNotIn('refPeriod', params)
    
    def test_car_park_data_transformation(self):
        """Test car park data transformation"""
        producer = URAProducer(
            kafka_config=self.kafka_config,
            topic='test-topic',
            source_name='test-source'
        )
        
        # Mock car park data
        mock_car_park_data = {
            'carParkNo': 'CP001',
            'area': 'Central',
            'development': 'Test Building',
            'location': 'Test Street',
            'availableLots': '50',
            'lotType': 'C',
            'agency': 'URA'
        }
        
        transformed = producer.transform_data(mock_car_park_data, 'Car_Park_Availability')
        
        # Check that car park specific fields are included
        self.assertEqual(transformed.processed_data['car_park_no'], 'CP001')
        self.assertEqual(transformed.processed_data['area'], 'Central')
        self.assertEqual(transformed.processed_data['available_lots'], '50')

class TestHealthChecking(unittest.TestCase):
    """Test health checking functionality"""
    
    def setUp(self):
        self.health_checker = HealthChecker()
    
    @patch('monitoring.health_check.KafkaProducer')
    def test_kafka_health_check_success(self, mock_kafka_producer):
        """Test successful Kafka health check"""
        # Mock successful Kafka connection
        mock_producer_instance = Mock()
        mock_kafka_producer.return_value = mock_producer_instance
        
        result = self.health_checker.check_kafka_connectivity()
        
        self.assertEqual(result.component, 'kafka')
        self.assertEqual(result.status, 'healthy')
        self.assertIsNotNone(result.response_time_ms)
    
    @patch('monitoring.health_check.KafkaProducer')
    def test_kafka_health_check_failure(self, mock_kafka_producer):
        """Test failed Kafka health check"""
        # Mock failed Kafka connection
        mock_kafka_producer.side_effect = Exception("Connection refused")
        
        result = self.health_checker.check_kafka_connectivity()
        
        self.assertEqual(result.component, 'kafka')
        self.assertEqual(result.status, 'unhealthy')
        self.assertIn('Connection refused', result.message)
    
    @patch('monitoring.health_check.requests.get')
    def test_acra_api_health_check(self, mock_get):
        """Test ACRA API health check"""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'success': True,
            'result': {'records': [{'test': 'data'}]}
        }
        mock_get.return_value = mock_response
        
        result = self.health_checker.check_acra_api()
        
        self.assertEqual(result.component, 'acra-api')
        self.assertEqual(result.status, 'healthy')
        self.assertIsNotNone(result.response_time_ms)
    
    def test_comprehensive_health_check_structure(self):
        """Test that comprehensive health check returns proper structure"""
        with patch.object(self.health_checker, 'check_kafka_connectivity') as mock_kafka, \
             patch.object(self.health_checker, 'check_kafka_topics') as mock_topics, \
             patch.object(self.health_checker, 'check_acra_api') as mock_acra, \
             patch.object(self.health_checker, 'check_singstat_api') as mock_singstat, \
             patch.object(self.health_checker, 'check_ura_api') as mock_ura:
            
            # Mock all checks as healthy
            mock_kafka.return_value = HealthStatus('kafka', 'healthy', 'OK', datetime.now())
            mock_topics.return_value = [HealthStatus('topic1', 'healthy', 'OK', datetime.now())]
            mock_acra.return_value = HealthStatus('acra-api', 'healthy', 'OK', datetime.now())
            mock_singstat.return_value = HealthStatus('singstat-api', 'healthy', 'OK', datetime.now())
            mock_ura.return_value = HealthStatus('ura-api', 'healthy', 'OK', datetime.now())
            
            result = self.health_checker.run_comprehensive_health_check()
            
            # Check structure
            self.assertIn('overall_status', result)
            self.assertIn('timestamp', result)
            self.assertIn('summary', result)
            self.assertIn('checks', result)
            
            # Check summary structure
            summary = result['summary']
            self.assertIn('total_checks', summary)
            self.assertIn('healthy', summary)
            self.assertIn('degraded', summary)
            self.assertIn('unhealthy', summary)

class TestDataQualityValidation(unittest.TestCase):
    """Test data quality validation"""
    
    def setUp(self):
        self.kafka_config = {
            'bootstrap_servers': 'localhost:9092',
            'value_serializer': lambda v: json.dumps(v).encode('utf-8')
        }
    
    def test_empty_data_validation(self):
        """Test that empty data is properly handled"""
        producer = ACRAProducer(
            kafka_config=self.kafka_config,
            topic='test-topic',
            source_name='test-source'
        )
        
        # Test with None data
        with patch.object(producer, 'send_to_kafka') as mock_send:
            mock_send.return_value = False
            result = producer.send_to_kafka(None)
            self.assertFalse(result)
    
    def test_data_record_validation(self):
        """Test DataRecord validation"""
        # Test valid data record
        valid_record = DataRecord(
            source='test',
            timestamp=datetime.now(),
            data_type='test',
            raw_data={'test': 'data'},
            processed_data={'test': 'processed'}
        )
        
        self.assertEqual(valid_record.source, 'test')
        self.assertEqual(valid_record.data_type, 'test')
        self.assertIsInstance(valid_record.timestamp, datetime)

class TestIntegrationScenarios(unittest.TestCase):
    """Test integration scenarios and edge cases"""
    
    def setUp(self):
        self.kafka_config = {
            'bootstrap_servers': 'localhost:9092',
            'value_serializer': lambda v: json.dumps(v).encode('utf-8')
        }
    
    @patch('requests.get')
    def test_api_timeout_handling(self, mock_get):
        """Test API timeout handling"""
        # Mock timeout exception
        mock_get.side_effect = Exception("Request timeout")
        
        producer = ACRAProducer(
            kafka_config=self.kafka_config,
            topic='test-topic',
            source_name='test-source'
        )
        
        # This should handle the timeout gracefully
        result = producer.fetch_data('http://test.url', {}, {})
        self.assertIsNone(result)
    
    def test_malformed_json_handling(self):
        """Test handling of malformed JSON responses"""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
            mock_response.text = "Invalid JSON response"
            mock_get.return_value = mock_response
            
            producer = ACRAProducer(
                kafka_config=self.kafka_config,
                topic='test-topic',
                source_name='test-source'
            )
            
            result = producer.fetch_data('http://test.url', {}, {})
            self.assertIsNone(result)

def run_enhanced_tests():
    """Run all enhanced functionality tests"""
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestConfigurationValidation,
        TestEnhancedErrorHandling,
        TestURAEnhancements,
        TestHealthChecking,
        TestDataQualityValidation,
        TestIntegrationScenarios
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Return success status
    return result.wasSuccessful()

if __name__ == '__main__':
    success = run_enhanced_tests()
    exit(0 if success else 1)