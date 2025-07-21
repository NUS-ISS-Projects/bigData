import os
import time
import threading
from datetime import datetime

import schedule
from loguru import logger
from dotenv import load_dotenv

from acra_producer import ACRAProducer
from singstat_producer import SingStatProducer
from ura_producer import URAProducer
from commercial_rental_producer import CommercialRentalProducer

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
            'ura': URAProducer(self.kafka_config),
            'commercial_rental': CommercialRentalProducer(self.kafka_config)
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
    
    def run_commercial_rental_extraction(self):
        """Run Commercial Rental Index data extraction"""
        try:
            logger.info("Starting scheduled Commercial Rental Index extraction")
            self.producers['commercial_rental'].run_extraction()
            logger.info("Completed scheduled Commercial Rental Index extraction")
        except Exception as e:
            logger.error(f"Error in scheduled Commercial Rental Index extraction: {e}")
    
    def run_all_extractions(self):
        """Run all data extractions in parallel"""
        try:
            logger.info("Starting full data extraction cycle (parallel execution)")
            
            # Create threads for parallel execution
            threads = []
            
            # ACRA thread
            acra_thread = threading.Thread(target=self.run_acra_extraction, name="ACRA-Thread")
            threads.append(acra_thread)
            
            # SingStat thread
            singstat_thread = threading.Thread(target=self.run_singstat_extraction, name="SingStat-Thread")
            threads.append(singstat_thread)
            
            # URA thread
            ura_thread = threading.Thread(target=self.run_ura_extraction, name="URA-Thread")
            threads.append(ura_thread)
            
            # Commercial Rental thread
            commercial_rental_thread = threading.Thread(target=self.run_commercial_rental_extraction, name="CommercialRental-Thread")
            threads.append(commercial_rental_thread)
            
            # Start all threads
            for thread in threads:
                thread.start()
                logger.info(f"Started {thread.name}")
            
            # Wait for all threads to complete with proper parallel monitoring
            start_time = time.time()
            max_extraction_time = 3600  # 1 hour timeout
            
            while any(thread.is_alive() for thread in threads):
                time.sleep(30)  # Check every 30 seconds
                elapsed = time.time() - start_time
                
                # Log progress of running threads
                alive_threads = [t.name for t in threads if t.is_alive()]
                completed_threads = [t.name for t in threads if not t.is_alive()]
                
                if alive_threads:
                    logger.info(f"Still running: {alive_threads} | Completed: {completed_threads} | Elapsed: {elapsed:.1f}s")
                
                # Check for timeout
                if elapsed > max_extraction_time:
                    logger.warning(f"Extraction timeout reached ({max_extraction_time}s). Some threads may still be running.")
                    break
            
            # Final status check
            for thread in threads:
                if thread.is_alive():
                    logger.warning(f"Thread {thread.name} is still running after timeout")
                else:
                    logger.info(f"Completed {thread.name}")
            
            logger.info("Completed full data extraction cycle (all extractions finished)")
        except Exception as e:
            logger.error(f"Error in full extraction cycle: {e}")
    
    def setup_schedules(self, production_mode: bool = True):
        """Setup extraction schedules"""
        if production_mode:
            # Production schedules - proper cron-based timing
            # ACRA data - daily at 2 AM (company registrations don't change frequently)
            schedule.every().day.at("02:00").do(self.run_acra_extraction)
            
            # SingStat data - daily at 3 AM (economic indicators updated regularly)
            schedule.every().day.at("03:00").do(self.run_singstat_extraction)
            
            # URA data - daily at 4 AM (property transactions updated regularly)
            schedule.every().day.at("04:00").do(self.run_ura_extraction)
            
            # Commercial Rental Index - daily at 5 AM (rental index updated quarterly)
            schedule.every().day.at("05:00").do(self.run_commercial_rental_extraction)
            
            # Full extraction cycle - weekly on Sundays at 1 AM
            schedule.every().sunday.at("01:00").do(self.run_all_extractions)
            
            logger.info("Production data extraction schedules configured")
        else:
            # Development/testing schedules - more frequent for testing
            schedule.every(5).minutes.do(self.run_singstat_extraction)
            schedule.every(5).minutes.do(self.run_acra_extraction)
            schedule.every(5).minutes.do(self.run_ura_extraction)
            schedule.every(5).minutes.do(self.run_commercial_rental_extraction)
            
            logger.info("Development data extraction schedules configured")
    
    def run_initial_extraction(self):
        """Run initial data extraction once on startup (parallel execution)"""
        try:
            logger.info("=== INITIAL DATA EXTRACTION ON STARTUP (PARALLEL) ===")
            logger.info("This will run once to populate the data lake with initial data")
            
            # Create threads for parallel execution
            threads = []
            
            # ACRA thread
            acra_thread = threading.Thread(target=self.run_acra_extraction, name="Initial-ACRA-Thread")
            threads.append(acra_thread)
            
            # SingStat thread
            singstat_thread = threading.Thread(target=self.run_singstat_extraction, name="Initial-SingStat-Thread")
            threads.append(singstat_thread)
            
            # URA thread
            ura_thread = threading.Thread(target=self.run_ura_extraction, name="Initial-URA-Thread")
            threads.append(ura_thread)
            
            # Commercial Rental thread
            commercial_rental_thread = threading.Thread(target=self.run_commercial_rental_extraction, name="Initial-CommercialRental-Thread")
            threads.append(commercial_rental_thread)
            
            # Start all threads
            for thread in threads:
                thread.start()
                logger.info(f"Started {thread.name}")
            
            # Wait for all threads to complete with proper parallel monitoring
            start_time = time.time()
            max_extraction_time = 3600  # 1 hour timeout
            
            while any(thread.is_alive() for thread in threads):
                time.sleep(30)  # Check every 30 seconds
                elapsed = time.time() - start_time
                
                # Log progress of running threads
                alive_threads = [t.name for t in threads if t.is_alive()]
                completed_threads = [t.name for t in threads if not t.is_alive()]
                
                if alive_threads:
                    logger.info(f"Still running: {alive_threads} | Completed: {completed_threads} | Elapsed: {elapsed:.1f}s")
                
                # Check for timeout
                if elapsed > max_extraction_time:
                    logger.warning(f"Extraction timeout reached ({max_extraction_time}s). Some threads may still be running.")
                    break
            
            # Final status check
            for thread in threads:
                if thread.is_alive():
                    logger.warning(f"Thread {thread.name} is still running after timeout")
                else:
                    logger.info(f"Completed {thread.name}")
            
            logger.info("=== INITIAL DATA EXTRACTION COMPLETED (ALL PARALLEL EXTRACTIONS FINISHED) ===")
            logger.info("Subsequent extractions will be managed by cron schedules")
            
        except Exception as e:
            logger.error(f"Error in initial extraction: {e}")
    
    def start(self, production_mode: bool = True, run_initial: bool = True):
        """Start the scheduler"""
        self.running = True
        self.setup_schedules(production_mode)
        
        mode_str = "production" if production_mode else "development"
        logger.info(f"Data ingestion scheduler started in {mode_str} mode")
        logger.info("Scheduled jobs:")
        for job in schedule.jobs:
            logger.info(f"  - {job}")
        
        # Run initial extraction on startup if requested
        if run_initial:
            logger.info("Running initial data extraction on startup...")
            threading.Thread(target=self.run_initial_extraction, daemon=True).start()
            # Wait a bit for initial extraction to start
            time.sleep(5)
        
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
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Economic Intelligence Platform - Data Ingestion Service')
    parser.add_argument('--mode', choices=['production', 'development'], default='production',
                       help='Run mode: production (cron schedules) or development (frequent testing)')
    parser.add_argument('--no-initial', action='store_true',
                       help='Skip initial data extraction on startup')
    args = parser.parse_args()
    
    # Configure logging
    logger.add(
        "logs/data_ingestion_{time:YYYY-MM-DD}.log",
        rotation="1 day",
        retention="30 days",
        level="INFO"
    )
    
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    production_mode = args.mode == 'production'
    run_initial = not args.no_initial
    
    logger.info(f"Starting Economic Intelligence Platform - Data Ingestion Service ({args.mode} mode)")
    if run_initial:
        logger.info("Initial data extraction will run on startup")
    else:
        logger.info("Skipping initial data extraction")
    
    # Initialize and start scheduler
    scheduler = DataIngestionScheduler()
    
    try:
        scheduler.start(production_mode=production_mode, run_initial=run_initial)
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        scheduler.stop()


if __name__ == "__main__":
    main()