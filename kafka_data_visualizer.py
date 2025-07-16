#!/usr/bin/env python3
"""
Kafka Data Visualizer for Economic Intelligence Platform
Provides comprehensive visualization and monitoring of Kafka data ingestion
"""

import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import defaultdict, Counter
import matplotlib.pyplot as plt
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import seaborn as sns
from loguru import logger
import argparse
import os

# Set up plotting style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

class KafkaDataVisualizer:
    """Comprehensive Kafka data visualization and monitoring"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.topics = ['acra-companies', 'singstat-economics', 'ura-geospatial']
        self.data_cache = defaultdict(list)
        
        # Create output directory for visualizations
        self.output_dir = 'kafka_visualizations'
        os.makedirs(self.output_dir, exist_ok=True)
        
        logger.info(f"Kafka Data Visualizer initialized")
        logger.info(f"Target topics: {', '.join(self.topics)}")
        logger.info(f"Output directory: {self.output_dir}")
    
    def get_topic_info(self) -> Dict[str, Dict[str, Any]]:
        """Get comprehensive information about all topics"""
        topic_info = {}
        
        for topic in self.topics:
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.bootstrap_servers,
                    auto_offset_reset='earliest',
                    consumer_timeout_ms=5000,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                )
                
                messages = []
                message_timestamps = []
                message_sizes = []
                
                for message in consumer:
                    try:
                        msg_data = message.value
                        messages.append(msg_data)
                        message_timestamps.append(message.timestamp)
                        message_sizes.append(len(json.dumps(msg_data)))
                        
                        # Limit to prevent memory issues
                        if len(messages) >= 1000:
                            break
                    except Exception as e:
                        logger.warning(f"Failed to parse message in {topic}: {e}")
                        continue
                
                consumer.close()
                
                # Calculate statistics
                total_messages = len(messages)
                avg_size = sum(message_sizes) / len(message_sizes) if message_sizes else 0
                
                # Time analysis
                if message_timestamps:
                    timestamps_dt = [datetime.fromtimestamp(ts/1000) for ts in message_timestamps]
                    time_span = max(timestamps_dt) - min(timestamps_dt) if len(timestamps_dt) > 1 else timedelta(0)
                    messages_per_minute = total_messages / max(time_span.total_seconds() / 60, 1)
                else:
                    time_span = timedelta(0)
                    messages_per_minute = 0
                
                topic_info[topic] = {
                    'total_messages': total_messages,
                    'average_message_size': round(avg_size, 2),
                    'time_span_minutes': round(time_span.total_seconds() / 60, 2),
                    'messages_per_minute': round(messages_per_minute, 2),
                    'sample_messages': messages[:5],  # First 5 messages for inspection
                    'message_timestamps': message_timestamps,
                    'message_sizes': message_sizes
                }
                
                # Cache data for visualization
                self.data_cache[topic] = messages
                
                logger.info(f"✅ {topic}: {total_messages} messages, avg size: {avg_size:.1f} bytes")
                
            except Exception as e:
                logger.error(f"❌ Failed to analyze topic {topic}: {e}")
                topic_info[topic] = {
                    'error': str(e),
                    'total_messages': 0
                }
        
        return topic_info
    
    def create_message_volume_chart(self, topic_info: Dict[str, Dict[str, Any]]):
        """Create message volume comparison chart"""
        topics = []
        message_counts = []
        
        for topic, info in topic_info.items():
            if 'total_messages' in info:
                topics.append(topic.replace('-', '\n'))
                message_counts.append(info['total_messages'])
        
        plt.figure(figsize=(12, 6))
        bars = plt.bar(topics, message_counts, color=['#FF6B6B', '#4ECDC4', '#45B7D1'])
        plt.title('Kafka Topic Message Volumes', fontsize=16, fontweight='bold')
        plt.xlabel('Topics', fontsize=12)
        plt.ylabel('Number of Messages', fontsize=12)
        
        # Add value labels on bars
        for bar, count in zip(bars, message_counts):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(message_counts)*0.01,
                    str(count), ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(f'{self.output_dir}/message_volumes.png', dpi=300, bbox_inches='tight')
        plt.show()
        logger.info(f"📊 Message volume chart saved to {self.output_dir}/message_volumes.png")
    
    def create_message_size_analysis(self, topic_info: Dict[str, Dict[str, Any]]):
        """Create message size analysis charts"""
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('Message Size Analysis Across Topics', fontsize=16, fontweight='bold')
        
        # Average message sizes
        topics = []
        avg_sizes = []
        for topic, info in topic_info.items():
            if 'average_message_size' in info:
                topics.append(topic.replace('-', '\n'))
                avg_sizes.append(info['average_message_size'])
        
        axes[0, 0].bar(topics, avg_sizes, color=['#FF6B6B', '#4ECDC4', '#45B7D1'])
        axes[0, 0].set_title('Average Message Size by Topic')
        axes[0, 0].set_ylabel('Bytes')
        
        # Message size distributions
        for i, (topic, info) in enumerate(topic_info.items()):
            if 'message_sizes' in info and info['message_sizes']:
                ax_idx = (0, 1) if i == 0 else (1, i-1) if i < 3 else (1, 1)
                axes[ax_idx].hist(info['message_sizes'], bins=20, alpha=0.7, 
                                label=topic, color=['#FF6B6B', '#4ECDC4', '#45B7D1'][i])
                axes[ax_idx].set_title(f'{topic} Message Size Distribution')
                axes[ax_idx].set_xlabel('Message Size (bytes)')
                axes[ax_idx].set_ylabel('Frequency')
        
        plt.tight_layout()
        plt.savefig(f'{self.output_dir}/message_size_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        logger.info(f"📊 Message size analysis saved to {self.output_dir}/message_size_analysis.png")
    
    def analyze_data_content(self, topic_info: Dict[str, Dict[str, Any]]):
        """Analyze and visualize data content patterns"""
        content_analysis = {}
        
        for topic, info in topic_info.items():
            if topic not in self.data_cache or not self.data_cache[topic]:
                continue
            
            messages = self.data_cache[topic]
            analysis = {
                'field_frequency': Counter(),
                'source_distribution': Counter(),
                'data_types': Counter()
            }
            
            for msg in messages:
                # Analyze field frequency
                if isinstance(msg, dict):
                    for key in msg.keys():
                        analysis['field_frequency'][key] += 1
                    
                    # Source analysis
                    if 'source' in msg:
                        analysis['source_distribution'][msg['source']] += 1
                    
                    # Data type analysis
                    if 'data_type' in msg:
                        analysis['data_types'][msg['data_type']] += 1
            
            content_analysis[topic] = analysis
        
        # Create content analysis visualization
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Kafka Data Content Analysis', fontsize=16, fontweight='bold')
        
        # Field frequency analysis
        all_fields = set()
        for topic_analysis in content_analysis.values():
            all_fields.update(topic_analysis['field_frequency'].keys())
        
        field_data = []
        for topic, analysis in content_analysis.items():
            for field in all_fields:
                field_data.append({
                    'topic': topic,
                    'field': field,
                    'frequency': analysis['field_frequency'].get(field, 0)
                })
        
        if field_data:
            df_fields = pd.DataFrame(field_data)
            pivot_fields = df_fields.pivot(index='field', columns='topic', values='frequency').fillna(0)
            
            sns.heatmap(pivot_fields, annot=True, fmt='g', cmap='YlOrRd', ax=axes[0, 0])
            axes[0, 0].set_title('Field Frequency Heatmap')
            axes[0, 0].set_xlabel('Topics')
            axes[0, 0].set_ylabel('Fields')
        
        # Source distribution pie charts
        for i, (topic, analysis) in enumerate(content_analysis.items()):
            if i >= 3:  # Limit to 3 topics
                break
            
            ax_idx = [(0, 1), (1, 0), (1, 1)][i]
            
            if analysis['source_distribution']:
                sources = list(analysis['source_distribution'].keys())
                counts = list(analysis['source_distribution'].values())
                
                axes[ax_idx].pie(counts, labels=sources, autopct='%1.1f%%', startangle=90)
                axes[ax_idx].set_title(f'{topic} - Source Distribution')
        
        plt.tight_layout()
        plt.savefig(f'{self.output_dir}/content_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()
        logger.info(f"📊 Content analysis saved to {self.output_dir}/content_analysis.png")
        
        return content_analysis
    
    def generate_comprehensive_report(self, topic_info: Dict[str, Dict[str, Any]], 
                                    content_analysis: Dict[str, Any]):
        """Generate a comprehensive text report"""
        report_file = f'{self.output_dir}/kafka_data_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
        
        with open(report_file, 'w') as f:
            f.write("="*80 + "\n")
            f.write("KAFKA DATA INGESTION REPORT\n")
            f.write("Economic Intelligence Platform\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*80 + "\n\n")
            
            # Summary
            total_messages = sum(info.get('total_messages', 0) for info in topic_info.values())
            f.write(f"SUMMARY\n")
            f.write(f"Total Messages Across All Topics: {total_messages:,}\n")
            f.write(f"Active Topics: {len([t for t in topic_info if topic_info[t].get('total_messages', 0) > 0])}\n\n")
            
            # Topic details
            for topic, info in topic_info.items():
                f.write(f"TOPIC: {topic.upper()}\n")
                f.write("-" * 40 + "\n")
                
                if 'error' in info:
                    f.write(f"❌ Error: {info['error']}\n\n")
                    continue
                
                f.write(f"Messages: {info.get('total_messages', 0):,}\n")
                f.write(f"Average Message Size: {info.get('average_message_size', 0):.2f} bytes\n")
                f.write(f"Time Span: {info.get('time_span_minutes', 0):.2f} minutes\n")
                f.write(f"Messages per Minute: {info.get('messages_per_minute', 0):.2f}\n")
                
                # Sample message
                if 'sample_messages' in info and info['sample_messages']:
                    f.write(f"\nSample Message:\n")
                    f.write(json.dumps(info['sample_messages'][0], indent=2)[:500] + "...\n")
                
                f.write("\n")
            
            # Content analysis
            f.write("CONTENT ANALYSIS\n")
            f.write("=" * 40 + "\n")
            
            for topic, analysis in content_analysis.items():
                f.write(f"\n{topic.upper()}:\n")
                f.write(f"  Most Common Fields: {', '.join(list(analysis['field_frequency'].keys())[:5])}\n")
                f.write(f"  Sources: {', '.join(analysis['source_distribution'].keys())}\n")
                f.write(f"  Data Types: {', '.join(analysis['data_types'].keys())}\n")
        
        logger.info(f"📄 Comprehensive report saved to {report_file}")
        return report_file
    
    def run_full_analysis(self):
        """Run complete Kafka data analysis and visualization"""
        logger.info("🚀 Starting comprehensive Kafka data analysis...")
        
        # Get topic information
        topic_info = self.get_topic_info()
        
        # Create visualizations
        self.create_message_volume_chart(topic_info)
        self.create_message_size_analysis(topic_info)
        
        # Analyze content
        content_analysis = self.analyze_data_content(topic_info)
        
        # Generate report
        report_file = self.generate_comprehensive_report(topic_info, content_analysis)
        
        logger.info("✅ Kafka data analysis completed!")
        logger.info(f"📁 All outputs saved to: {self.output_dir}/")
        
        return {
            'topic_info': topic_info,
            'content_analysis': content_analysis,
            'report_file': report_file
        }

def main():
    parser = argparse.ArgumentParser(description='Kafka Data Visualizer')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                       help='Kafka bootstrap servers')
    parser.add_argument('--quick', action='store_true',
                       help='Run quick analysis (fewer visualizations)')
    
    args = parser.parse_args()
    
    visualizer = KafkaDataVisualizer(args.bootstrap_servers)
    
    if args.quick:
        topic_info = visualizer.get_topic_info()
        visualizer.create_message_volume_chart(topic_info)
    else:
        visualizer.run_full_analysis()

if __name__ == "__main__":
    main()