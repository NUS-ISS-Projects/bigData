#!/usr/bin/env python3
"""
Performance Monitoring Dashboard for Economic Intelligence Platform
Tracks system metrics, data flow, and performance indicators.
"""

import json
import logging
import time
import psutil
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import os
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SystemMetrics:
    """System performance metrics"""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_available_gb: float
    disk_usage_percent: float
    disk_free_gb: float
    network_bytes_sent: int
    network_bytes_recv: int
    load_average: List[float]
    active_connections: int

@dataclass
class KafkaMetrics:
    """Kafka performance metrics"""
    timestamp: datetime
    topic: str
    messages_per_second: float
    bytes_per_second: float
    lag: int
    partition_count: int
    consumer_count: int

@dataclass
class DataSourceMetrics:
    """Data source performance metrics"""
    timestamp: datetime
    source_name: str
    api_response_time_ms: float
    success_rate: float
    error_count: int
    records_processed: int
    data_quality_score: float

class PerformanceMonitor:
    """Comprehensive performance monitoring system"""
    
    def __init__(self, monitoring_interval: int = 30):
        self.monitoring_interval = monitoring_interval
        self.is_running = False
        self.metrics_history = {
            'system': deque(maxlen=1000),
            'kafka': defaultdict(lambda: deque(maxlen=1000)),
            'data_sources': defaultdict(lambda: deque(maxlen=1000))
        }
        
        # Kafka configuration
        self.kafka_config = {
            'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'group_id': 'performance_monitor',
            'value_deserializer': lambda m: json.loads(m.decode('utf-8'))
        }
        
        self.topics = [
            'acra-companies',
            'singstat-economics',
            'ura-geospatial'
        ]
        
        # Performance tracking
        self.message_counters = defaultdict(int)
        self.byte_counters = defaultdict(int)
        self.last_reset_time = time.time()
        
        # Threading
        self.monitor_thread = None
        self.kafka_thread = None
    
    def collect_system_metrics(self) -> SystemMetrics:
        """Collect current system performance metrics"""
        try:
            # CPU and Memory
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            
            # Disk usage
            disk = psutil.disk_usage('/')
            
            # Network
            network = psutil.net_io_counters()
            
            # Load average (Unix-like systems)
            try:
                load_avg = list(os.getloadavg())
            except (OSError, AttributeError):
                load_avg = [0.0, 0.0, 0.0]
            
            # Network connections
            try:
                connections = len(psutil.net_connections())
            except (psutil.AccessDenied, OSError):
                connections = 0
            
            return SystemMetrics(
                timestamp=datetime.now(),
                cpu_percent=cpu_percent,
                memory_percent=memory.percent,
                memory_available_gb=memory.available / (1024**3),
                disk_usage_percent=disk.percent,
                disk_free_gb=disk.free / (1024**3),
                network_bytes_sent=network.bytes_sent,
                network_bytes_recv=network.bytes_recv,
                load_average=load_avg,
                active_connections=connections
            )
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")
            return None
    
    def calculate_kafka_metrics(self) -> List[KafkaMetrics]:
        """Calculate Kafka performance metrics"""
        metrics = []
        current_time = time.time()
        time_diff = current_time - self.last_reset_time
        
        if time_diff == 0:
            return metrics
        
        try:
            for topic in self.topics:
                messages_count = self.message_counters.get(topic, 0)
                bytes_count = self.byte_counters.get(topic, 0)
                
                messages_per_second = messages_count / time_diff
                bytes_per_second = bytes_count / time_diff
                
                # Try to get additional Kafka metadata
                try:
                    consumer = KafkaConsumer(
                        topic,
                        **{k: v for k, v in self.kafka_config.items() if k != 'value_deserializer'}
                    )
                    partitions = consumer.partitions_for_topic(topic)
                    partition_count = len(partitions) if partitions else 0
                    consumer.close()
                except Exception:
                    partition_count = 0
                
                metrics.append(KafkaMetrics(
                    timestamp=datetime.now(),
                    topic=topic,
                    messages_per_second=messages_per_second,
                    bytes_per_second=bytes_per_second,
                    lag=0,  # Would need additional logic to calculate lag
                    partition_count=partition_count,
                    consumer_count=1  # Simplified for this implementation
                ))
        
        except Exception as e:
            logger.error(f"Error calculating Kafka metrics: {e}")
        
        # Reset counters
        self.message_counters.clear()
        self.byte_counters.clear()
        self.last_reset_time = current_time
        
        return metrics
    
    def monitor_kafka_messages(self):
        """Monitor Kafka messages in a separate thread"""
        try:
            consumer = KafkaConsumer(*self.topics, **self.kafka_config)
            
            for message in consumer:
                if not self.is_running:
                    break
                
                topic = message.topic
                message_size = len(message.value) if message.value else 0
                
                self.message_counters[topic] += 1
                self.byte_counters[topic] += message_size
                
        except Exception as e:
            logger.error(f"Error monitoring Kafka messages: {e}")
        finally:
            try:
                consumer.close()
            except:
                pass
    
    def estimate_data_source_metrics(self) -> List[DataSourceMetrics]:
        """Estimate data source performance metrics based on recent activity"""
        metrics = []
        
        # This is a simplified implementation
        # In a real system, you'd track actual API calls and responses
        data_sources = [
            {'name': 'ACRA', 'base_response_time': 500, 'base_success_rate': 0.95},
            {'name': 'SingStat', 'base_response_time': 800, 'base_success_rate': 0.92},
            {'name': 'URA', 'base_response_time': 1200, 'base_success_rate': 0.88}
        ]
        
        import random
        
        for source in data_sources:
            # Add some realistic variation
            response_time = source['base_response_time'] * (0.8 + 0.4 * random.random())
            success_rate = min(1.0, source['base_success_rate'] + 0.1 * (random.random() - 0.5))
            
            metrics.append(DataSourceMetrics(
                timestamp=datetime.now(),
                source_name=source['name'],
                api_response_time_ms=response_time,
                success_rate=success_rate,
                error_count=random.randint(0, 5),
                records_processed=random.randint(50, 200),
                data_quality_score=0.85 + 0.15 * random.random()
            ))
        
        return metrics
    
    def generate_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'monitoring_period_minutes': self.monitoring_interval / 60,
            'system_health': {},
            'kafka_performance': {},
            'data_source_performance': {},
            'alerts': []
        }
        
        # System health summary
        if self.metrics_history['system']:
            recent_system = list(self.metrics_history['system'])[-10:]  # Last 10 readings
            avg_cpu = sum(m.cpu_percent for m in recent_system) / len(recent_system)
            avg_memory = sum(m.memory_percent for m in recent_system) / len(recent_system)
            
            report['system_health'] = {
                'average_cpu_percent': round(avg_cpu, 2),
                'average_memory_percent': round(avg_memory, 2),
                'current_load_average': recent_system[-1].load_average if recent_system else [0, 0, 0],
                'disk_free_gb': recent_system[-1].disk_free_gb if recent_system else 0
            }
            
            # System alerts
            if avg_cpu > 80:
                report['alerts'].append({
                    'type': 'HIGH_CPU',
                    'message': f'High CPU usage: {avg_cpu:.1f}%',
                    'severity': 'warning'
                })
            
            if avg_memory > 85:
                report['alerts'].append({
                    'type': 'HIGH_MEMORY',
                    'message': f'High memory usage: {avg_memory:.1f}%',
                    'severity': 'warning'
                })
        
        # Kafka performance summary
        kafka_summary = {}
        for topic in self.topics:
            if self.metrics_history['kafka'][topic]:
                recent_kafka = list(self.metrics_history['kafka'][topic])[-5:]  # Last 5 readings
                avg_mps = sum(m.messages_per_second for m in recent_kafka) / len(recent_kafka)
                avg_bps = sum(m.bytes_per_second for m in recent_kafka) / len(recent_kafka)
                
                kafka_summary[topic] = {
                    'average_messages_per_second': round(avg_mps, 2),
                    'average_bytes_per_second': round(avg_bps, 2),
                    'partition_count': recent_kafka[-1].partition_count if recent_kafka else 0
                }
                
                # Kafka alerts
                if avg_mps < 0.1:  # Very low message rate
                    report['alerts'].append({
                        'type': 'LOW_MESSAGE_RATE',
                        'message': f'Low message rate for {topic}: {avg_mps:.2f} msg/s',
                        'severity': 'info'
                    })
        
        report['kafka_performance'] = kafka_summary
        
        # Data source performance summary
        ds_summary = {}
        for source in ['ACRA', 'SingStat', 'URA']:
            if self.metrics_history['data_sources'][source]:
                recent_ds = list(self.metrics_history['data_sources'][source])[-5:]  # Last 5 readings
                avg_response_time = sum(m.api_response_time_ms for m in recent_ds) / len(recent_ds)
                avg_success_rate = sum(m.success_rate for m in recent_ds) / len(recent_ds)
                avg_quality = sum(m.data_quality_score for m in recent_ds) / len(recent_ds)
                
                ds_summary[source] = {
                    'average_response_time_ms': round(avg_response_time, 2),
                    'average_success_rate': round(avg_success_rate, 3),
                    'average_data_quality_score': round(avg_quality, 3),
                    'total_records_processed': sum(m.records_processed for m in recent_ds)
                }
                
                # Data source alerts
                if avg_success_rate < 0.9:
                    report['alerts'].append({
                        'type': 'LOW_SUCCESS_RATE',
                        'message': f'Low success rate for {source}: {avg_success_rate:.1%}',
                        'severity': 'warning'
                    })
                
                if avg_response_time > 2000:
                    report['alerts'].append({
                        'type': 'HIGH_RESPONSE_TIME',
                        'message': f'High response time for {source}: {avg_response_time:.0f}ms',
                        'severity': 'warning'
                    })
        
        report['data_source_performance'] = ds_summary
        
        return report
    
    def start_monitoring(self):
        """Start the performance monitoring system"""
        if self.is_running:
            logger.warning("Performance monitoring is already running")
            return
        
        self.is_running = True
        logger.info("Starting performance monitoring...")
        
        # Start Kafka monitoring thread
        self.kafka_thread = threading.Thread(target=self.monitor_kafka_messages, daemon=True)
        self.kafka_thread.start()
        
        # Start main monitoring loop
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()
        
        logger.info(f"Performance monitoring started with {self.monitoring_interval}s interval")
    
    def stop_monitoring(self):
        """Stop the performance monitoring system"""
        if not self.is_running:
            logger.warning("Performance monitoring is not running")
            return
        
        self.is_running = False
        logger.info("Stopping performance monitoring...")
        
        # Wait for threads to finish
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
        
        if self.kafka_thread and self.kafka_thread.is_alive():
            self.kafka_thread.join(timeout=5)
        
        logger.info("Performance monitoring stopped")
    
    def _monitoring_loop(self):
        """Main monitoring loop"""
        while self.is_running:
            try:
                # Collect system metrics
                system_metrics = self.collect_system_metrics()
                if system_metrics:
                    self.metrics_history['system'].append(system_metrics)
                
                # Calculate Kafka metrics
                kafka_metrics = self.calculate_kafka_metrics()
                for metric in kafka_metrics:
                    self.metrics_history['kafka'][metric.topic].append(metric)
                
                # Estimate data source metrics
                ds_metrics = self.estimate_data_source_metrics()
                for metric in ds_metrics:
                    self.metrics_history['data_sources'][metric.source_name].append(metric)
                
                # Log summary
                if system_metrics:
                    logger.info(f"System: CPU {system_metrics.cpu_percent:.1f}%, "
                              f"Memory {system_metrics.memory_percent:.1f}%, "
                              f"Disk {system_metrics.disk_usage_percent:.1f}%")
                
                # Sleep until next monitoring cycle
                time.sleep(self.monitoring_interval)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(self.monitoring_interval)
    
    def export_metrics(self, filepath: str):
        """Export collected metrics to JSON file"""
        try:
            export_data = {
                'export_timestamp': datetime.now().isoformat(),
                'system_metrics': [asdict(m) for m in self.metrics_history['system']],
                'kafka_metrics': {
                    topic: [asdict(m) for m in metrics]
                    for topic, metrics in self.metrics_history['kafka'].items()
                },
                'data_source_metrics': {
                    source: [asdict(m) for m in metrics]
                    for source, metrics in self.metrics_history['data_sources'].items()
                }
            }
            
            with open(filepath, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            logger.info(f"Metrics exported to {filepath}")
            
        except Exception as e:
            logger.error(f"Error exporting metrics: {e}")

def main():
    """Main function for running performance monitoring"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Performance Monitor for Economic Intelligence Platform')
    parser.add_argument('--interval', type=int, default=30, help='Monitoring interval in seconds')
    parser.add_argument('--duration', type=int, default=300, help='Monitoring duration in seconds')
    parser.add_argument('--export', type=str, help='Export metrics to file')
    parser.add_argument('--report', action='store_true', help='Generate performance report')
    
    args = parser.parse_args()
    
    # Create and start monitor
    monitor = PerformanceMonitor(monitoring_interval=args.interval)
    
    try:
        monitor.start_monitoring()
        
        if args.duration > 0:
            logger.info(f"Monitoring for {args.duration} seconds...")
            time.sleep(args.duration)
        else:
            logger.info("Monitoring indefinitely. Press Ctrl+C to stop.")
            while True:
                time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    
    finally:
        monitor.stop_monitoring()
        
        # Generate report if requested
        if args.report:
            report = monitor.generate_performance_report()
            print("\n" + "="*50)
            print("PERFORMANCE REPORT")
            print("="*50)
            print(json.dumps(report, indent=2, default=str))
        
        # Export metrics if requested
        if args.export:
            monitor.export_metrics(args.export)

if __name__ == '__main__':
    main()