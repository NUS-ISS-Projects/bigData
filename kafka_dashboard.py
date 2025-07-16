#!/usr/bin/env python3
"""
Real-time Kafka Data Ingestion Dashboard
Provides live monitoring of data flow across all topics
"""

import json
import time
from datetime import datetime
from typing import Dict, List
from collections import defaultdict, deque
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from loguru import logger
import threading
import os

class KafkaDashboard:
    """Real-time dashboard for monitoring Kafka data ingestion"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.topics = ['acra-companies', 'singstat-economics', 'ura-geospatial']
        
        # Metrics storage (keep last 100 readings per topic)
        self.message_counts = defaultdict(lambda: deque(maxlen=100))
        self.message_rates = defaultdict(lambda: deque(maxlen=100))
        self.error_counts = defaultdict(int)
        self.last_message_time = defaultdict(lambda: None)
        
        # Control flags
        self.running = False
        self.consumers = {}
        
        logger.info(f"Kafka Dashboard initialized for topics: {', '.join(self.topics)}")
    
    def get_topic_message_count(self, topic: str) -> int:
        """Get current message count for a topic"""
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                consumer_timeout_ms=1000
            )
            
            # Get partition info and calculate total messages
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                consumer.close()
                return 0
            
            total_messages = 0
            for partition in partitions:
                tp = consumer.assignment()
                if tp:
                    end_offset = consumer.end_offsets(tp)
                    total_messages += sum(end_offset.values())
            
            consumer.close()
            return total_messages
            
        except Exception as e:
            logger.error(f"Error getting message count for {topic}: {e}")
            return 0
    
    def monitor_topic(self, topic: str):
        """Monitor a specific topic in real-time"""
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='latest',  # Only new messages
                consumer_timeout_ms=5000,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            self.consumers[topic] = consumer
            message_count = 0
            start_time = time.time()
            
            logger.info(f"🔍 Started monitoring {topic}")
            
            while self.running:
                try:
                    for message in consumer:
                        if not self.running:
                            break
                        
                        message_count += 1
                        self.last_message_time[topic] = datetime.now()
                        
                        # Calculate rate every 10 seconds
                        current_time = time.time()
                        if current_time - start_time >= 10:
                            rate = message_count / (current_time - start_time)
                            self.message_rates[topic].append(rate)
                            self.message_counts[topic].append(message_count)
                            
                            # Reset counters
                            message_count = 0
                            start_time = current_time
                        
                        # Brief pause to prevent overwhelming
                        time.sleep(0.01)
                        
                except Exception as e:
                    self.error_counts[topic] += 1
                    logger.warning(f"Error processing message in {topic}: {e}")
                    time.sleep(1)
            
            consumer.close()
            logger.info(f"🛑 Stopped monitoring {topic}")
            
        except Exception as e:
            logger.error(f"Failed to monitor {topic}: {e}")
            self.error_counts[topic] += 1
    
    def display_dashboard(self):
        """Display real-time dashboard"""
        while self.running:
            try:
                # Clear screen (works on most terminals)
                os.system('clear' if os.name == 'posix' else 'cls')
                
                print("="*80)
                print("🚀 KAFKA DATA INGESTION DASHBOARD")
                print("Economic Intelligence Platform")
                print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("="*80)
                
                # Topic status
                print("\n📊 TOPIC STATUS:")
                print("-" * 60)
                
                for topic in self.topics:
                    # Get current metrics
                    total_messages = self.get_topic_message_count(topic)
                    current_rate = list(self.message_rates[topic])[-1] if self.message_rates[topic] else 0
                    last_msg = self.last_message_time[topic]
                    errors = self.error_counts[topic]
                    
                    # Status indicator
                    if last_msg and (datetime.now() - last_msg).seconds < 30:
                        status = "🟢 ACTIVE"
                    elif total_messages > 0:
                        status = "🟡 IDLE"
                    else:
                        status = "🔴 NO DATA"
                    
                    print(f"\n{topic.upper()}:")
                    print(f"  Status: {status}")
                    print(f"  Total Messages: {total_messages:,}")
                    print(f"  Current Rate: {current_rate:.2f} msg/sec")
                    print(f"  Errors: {errors}")
                    
                    if last_msg:
                        print(f"  Last Message: {last_msg.strftime('%H:%M:%S')}")
                
                # Overall statistics
                total_all = sum(self.get_topic_message_count(topic) for topic in self.topics)
                total_errors = sum(self.error_counts.values())
                
                print("\n" + "="*60)
                print("📈 OVERALL STATISTICS:")
                print(f"  Total Messages (All Topics): {total_all:,}")
                print(f"  Total Errors: {total_errors}")
                print(f"  Active Topics: {len([t for t in self.topics if self.get_topic_message_count(t) > 0])}")
                
                # Instructions
                print("\n" + "="*60)
                print("⌨️  CONTROLS:")
                print("  Press Ctrl+C to stop monitoring")
                print("  Dashboard updates every 10 seconds")
                
                time.sleep(10)  # Update every 10 seconds
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Dashboard display error: {e}")
                time.sleep(5)
    
    def start_monitoring(self):
        """Start real-time monitoring of all topics"""
        self.running = True
        
        # Start monitoring threads for each topic
        monitor_threads = []
        for topic in self.topics:
            thread = threading.Thread(target=self.monitor_topic, args=(topic,))
            thread.daemon = True
            thread.start()
            monitor_threads.append(thread)
        
        # Start dashboard display
        try:
            self.display_dashboard()
        except KeyboardInterrupt:
            logger.info("\n🛑 Stopping dashboard...")
        finally:
            self.stop_monitoring()
    
    def stop_monitoring(self):
        """Stop all monitoring"""
        self.running = False
        
        # Close all consumers
        for topic, consumer in self.consumers.items():
            try:
                consumer.close()
            except:
                pass
        
        logger.info("✅ Dashboard stopped")
    
    def quick_status(self):
        """Get quick status without starting full monitoring"""
        print("🔍 KAFKA QUICK STATUS CHECK")
        print("=" * 40)
        
        for topic in self.topics:
            try:
                count = self.get_topic_message_count(topic)
                status = "✅" if count > 0 else "❌"
                print(f"{status} {topic}: {count:,} messages")
            except Exception as e:
                print(f"❌ {topic}: Error - {e}")
        
        print("\n✅ Quick status check completed")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Kafka Data Dashboard')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                       help='Kafka bootstrap servers')
    parser.add_argument('--quick', action='store_true',
                       help='Quick status check only')
    
    args = parser.parse_args()
    
    dashboard = KafkaDashboard(args.bootstrap_servers)
    
    if args.quick:
        dashboard.quick_status()
    else:
        try:
            dashboard.start_monitoring()
        except KeyboardInterrupt:
            print("\n👋 Dashboard stopped by user")

if __name__ == "__main__":
    main()