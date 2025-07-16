import os
import time
import threading
from datetime import datetime
from typing import Dict, Any

import schedule
from loguru import logger
from dotenv import load_dotenv

from acra_producer import ACRAProducer
from singstat_producer import SingStatProducer
from ura_producer import URAProducer

# Load environment variables
load_dotenv()


class DataIngestionScheduler:
    """Scheduler for coordinating all data producers"""
    
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:30092')]
        }
        
        # Initialize producers
        self.producers = {
            'acra': ACRAProducer(self.kafka_config),
            'singstat': SingStatProducer(self.kafka_config),
            'ura': URAProducer(self.kafka_config)
        }
        
        self.running = False
        
    def run_acra_extraction(self):
        """Run ACRA data extraction"""
        try:
            logger.info("Starting scheduled ACRA extraction")
            self.producers['acra'].run_extraction()
            logger.info("Completed scheduled ACRA extraction")
        except Exception as e:
            logger.error(f"Error in scheduled ACRA extraction: {e}")
    
    def run_singstat_extraction(self):
        """Run SingStat data extraction"""
        try:
            logger.info("Starting scheduled SingStat extraction")
            self.producers['singstat'].run_extraction()
            logger.info("Completed scheduled SingStat extraction")
        except Exception as e:
            logger.error(f"Error in scheduled SingStat extraction: {e}")
    
    def run_ura_extraction(self):
        """Run URA data extraction"""
        try:
            logger.info("Starting scheduled URA extraction")
            self.producers['ura'].run_extraction()
            logger.info("Completed scheduled URA extraction")
        except Exception as e:
            logger.error(f"Error in scheduled URA extraction: {e}")
    
    def run_all_extractions(self):
        """Run all data extractions sequentially"""
        try:
            logger.info("Starting full data extraction cycle")
            
            # Run extractions in sequence to avoid overwhelming APIs
            self.run_acra_extraction()
            time.sleep(60)  # 1 minute delay between extractions
            
            self.run_singstat_extraction()
            time.sleep(60)
            
            self.run_ura_extraction()
            
            logger.info("Completed full data extraction cycle")
        except Exception as e:
            logger.error(f"Error in full extraction cycle: {e}")
    
    def setup_schedules(self):
        """Setup extraction schedules"""
        # ACRA data - daily at 2 AM (company registrations don't change frequently)
        schedule.every().day.at("02:00").do(self.run_acra_extraction)
        
        # SingStat data - daily at 3 AM (economic indicators updated regularly)
        schedule.every().day.at("03:00").do(self.run_singstat_extraction)
        
        # URA data - daily at 4 AM (property transactions updated regularly)
        schedule.every().day.at("04:00").do(self.run_ura_extraction)
        
        # Full extraction cycle - weekly on Sundays at 1 AM
        schedule.every().sunday.at("01:00").do(self.run_all_extractions)
        
        # For testing - run every 30 minutes (comment out for production)
        # schedule.every(30).minutes.do(self.run_singstat_extraction)
        
        logger.info("Data extraction schedules configured")
    
    def start(self):
        """Start the scheduler"""
        self.running = True
        self.setup_schedules()
        
        logger.info("Data ingestion scheduler started")
        logger.info("Scheduled jobs:")
        for job in schedule.jobs:
            logger.info(f"  - {job}")
        
        # Run initial extraction on startup
        logger.info("Running initial data extraction...")
        threading.Thread(target=self.run_all_extractions, daemon=True).start()
        
        # Main scheduler loop
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                self.stop()
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                time.sleep(60)
    
    def stop(self):
        """Stop the scheduler and close all producers"""
        self.running = False
        
        logger.info("Stopping data ingestion scheduler")
        
        # Close all producers
        for name, producer in self.producers.items():
            try:
                producer.close()
                logger.info(f"Closed {name} producer")
            except Exception as e:
                logger.error(f"Error closing {name} producer: {e}")
        
        logger.info("Data ingestion scheduler stopped")
    
    def run_manual_extraction(self, producer_name: str = None):
        """Run manual extraction for testing"""
        if producer_name:
            if producer_name in self.producers:
                logger.info(f"Running manual {producer_name} extraction")
                getattr(self, f'run_{producer_name}_extraction')()
            else:
                logger.error(f"Unknown producer: {producer_name}")
        else:
            logger.info("Running manual full extraction")
            self.run_all_extractions()


def main():
    """Main function"""
    # Configure logging
    logger.add(
        "logs/data_ingestion_{time:YYYY-MM-DD}.log",
        rotation="1 day",
        retention="30 days",
        level="INFO"
    )
    
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    logger.info("Starting Economic Intelligence Platform - Data Ingestion Service")
    
    # Initialize and start scheduler
    scheduler = DataIngestionScheduler()
    
    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        scheduler.stop()


if __name__ == "__main__":
    main()