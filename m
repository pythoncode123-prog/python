#!/usr/bin/env python3
import os
import sys
import logging
from datetime import datetime
import argparse  # Added for command line arguments

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.workflow import run_workflow

# Configuration 
CONFIG_FILE = 'config/config.ini'
QUERY_FILE = 'config/query.sql'
OUTPUT_CSV = 'data.csv'
EXECUTION_TIMESTAMP = datetime.strptime('2025-06-25 23:17:32', '%Y-%m-%d %H:%M:%S')
EXECUTION_USER = 'satish537'

def setup_logging():
    """Set up logging configuration."""
    log_file = f"workflow_{EXECUTION_TIMESTAMP.strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run data workflow")
    parser.add_argument("--test", action="store_true", help="Run in test mode using predefined test CSV file")
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    # Create config directory if it doesn't exist
    os.makedirs('config', exist_ok=True)
    
    # If not in test mode, check if config files exist
    if not args.test:
        if not os.path.exists(CONFIG_FILE):
            logging.error(f"Config file {CONFIG_FILE} not found.")
            return 1
            
        if not os.path.exists(QUERY_FILE):
            logging.error(f"Query file {QUERY_FILE} not found.")
            return 1
    
    # Run the workflow
    return run_workflow(
        CONFIG_FILE,
        QUERY_FILE,
        OUTPUT_CSV,
        EXECUTION_TIMESTAMP,
        EXECUTION_USER,
        test_mode=args.test  # Pass the test flag to the workflow
    )

if __name__ == "__main__":
    sys.exit(main())
