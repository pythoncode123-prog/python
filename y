#!/usr/bin/env python3
import os
import sys
import json
import logging
import re
import calendar
from datetime import datetime, timedelta
import argparse

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.workflow import run_workflow
from lib.secure_config import SecureConfig

# Configuration
CONFIG_FILE = 'config/config.ini'
QUERY_FILE = 'config/query.sql'
OUTPUT_CSV = 'data.csv'
EXECUTION_TIMESTAMP = datetime.strptime('2025-07-06 21:14:17', '%Y-%m-%d %H:%M:%S')  # Updated timestamp
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

def check_password_available():
    """Check if password is available either in environment or as encrypted in config."""
    # Check environment variable first
    if os.environ.get('CONFLUENCE_PASSWORD'):
        return True

    # Check for encrypted password in config
    if os.path.exists("config.json"):
        try:
            with open("config.json", 'r') as f:
                config = json.load(f)
            if 'PASSWORD_ENCRYPTED' in config:
                # Check if key file exists
                if os.path.exists(SecureConfig.KEY_FILE):
                    return True
        except Exception:
            pass
    return False

def get_month_date_range():
    """Get the start and end date of the current month."""
    today = datetime.now()
    start_of_month = datetime(today.year, today.month, 1)
    
    # Calculate the end of the month
    if today.month == 12:
        end_of_month = datetime(today.year + 1, 1, 1) - timedelta(days=1)
    else:
        end_of_month = datetime(today.year, today.month + 1, 1) - timedelta(days=1)
    
    start_date_str = start_of_month.strftime('%Y-%m-%d 00:00:00')
    end_date_str = end_of_month.strftime('%Y-%m-%d 23:59:00')
    
    # Get month name for Confluence page
    month_name = calendar.month_name[today.month]
    
    return start_date_str, end_date_str, month_name

def get_ytd_date_range():
    """Get the date range from start of current year to today."""
    today = datetime.now()
    start_of_year = datetime(today.year, 1, 1)
    
    start_date_str = start_of_year.strftime('%Y-%m-%d 00:00:00')
    today_str = today.strftime('%Y-%m-%d 23:59:00')
    
    return start_date_str, today_str

def update_query_with_date_range(query_path, start_date, end_date):
    """Update the query.sql file with specified date range."""
    try:
        # Read the existing query
        with open(query_path, 'r') as f:
            query = f.read()
        
        # Replace the date range in the query
        # Find the BETWEEN clause and update the dates
        pattern = r"BETWEEN\s+TO_DATE\('[^']+', 'YYYY-MM-DD HH24:MI:SS'\)\s+and\s+TO_DATE\('[^']+', 'YYYY-MM-DD HH24:MI:SS'\)"
        replacement = f"BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD HH24:MI:SS') and TO_DATE('{end_date}', 'YYYY-MM-DD HH24:MI:SS')"
        
        updated_query = re.sub(pattern, replacement, query)
        
        # Write back the updated query
        with open(query_path, 'w') as f:
            f.write(updated_query)
        
        logging.info(f"Updated query with date range: {start_date} to {end_date}")
        return True
    except Exception as e:
        logging.error(f"Error updating query file: {str(e)}")
        return False

def update_confluence_page_title(config_path, suffix):
    """Update the Confluence page title in the config file."""
    try:
        # Read the config file
        with open(config_path, 'r') as f:
            config_content = f.read()
        
        # Look for PAGE_TITLE in the config
        pattern = r'(PAGE_TITLE\s*=\s*[\'"])(.*?)([\'"])'
        
        # Check if there's already a suffix
        match = re.search(pattern, config_content)
        if match:
            base_title = match.group(2)
            # Remove any existing suffix (month name or YTD)
            base_title = re.sub(r'\s+-\s+(January|February|March|April|May|June|July|August|September|October|November|December|YTD)$', '', base_title)
            # Add the new suffix
            new_title = f"{base_title} - {suffix}"
            updated_config = re.sub(pattern, f"\\1{new_title}\\3", config_content)
            
            # Write back the updated config
            with open(config_path, 'w') as f:
                f.write(updated_config)
            
            logging.info(f"Updated Confluence page title to include suffix: {suffix}")
            return new_title
        else:
            logging.warning(f"Could not find PAGE_TITLE in config file: {config_path}")
            return None
    except Exception as e:
        logging.error(f"Error updating Confluence page title: {str(e)}")
        return None

def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run data workflow")
    parser.add_argument("--test", action="store_true", help="Run in test mode using predefined test CSV")
    parser.add_argument("--no-publish", action="store_true", help="Skip actual publishing to Confluence")
    parser.add_argument("--monthly", action="store_true", help="Use current month date range in SQL query")
    parser.add_argument("--daily", action="store_true", help="Use year-to-date range in SQL query")
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
    
    # Handle date range flags (monthly and daily)
    # Ensure only one flag is used
    if args.monthly and args.daily:
        logging.error("Cannot use both --monthly and --daily flags together. Please choose one.")
        return 1
    
    # Update query and Confluence page title based on flags
    if args.monthly:
        logging.info("Running with monthly date range flag")
        start_date, end_date, month_name = get_month_date_range()
        if not update_query_with_date_range(QUERY_FILE, start_date, end_date):
            logging.error("Failed to update query with monthly dates")
            return 1
        
        # Update Confluence page title with month name
        update_confluence_page_title(CONFIG_FILE, month_name)
        logging.info(f"Query updated to use {month_name} date range by {EXECUTION_USER}")
    
    elif args.daily:
        logging.info("Running with year-to-date range flag")
        start_date, end_date = get_ytd_date_range()
        if not update_query_with_date_range(QUERY_FILE, start_date, end_date):
            logging.error("Failed to update query with year-to-date range")
            return 1
        
        # Update Confluence page title with YTD
        update_confluence_page_title(CONFIG_FILE, "YTD")
        logging.info(f"Query updated to use year-to-date range by {EXECUTION_USER}")

    # Check for password availability if publishing
    if not args.no_publish and not check_password_available():
        print("No Confluence authentication credentials found.")
        print("Please either:")
        print("1. Set CONFLUENCE_PASSWORD environment variable")
        print("2. Run setup_secure_config.py to encrypt your password in config.json")
        print("3. Use --no-publish flag to skip publishing")
        return 1

    # Run the workflow
    run_workflow(
        CONFIG_FILE,
        QUERY_FILE,
        OUTPUT_CSV,
        EXECUTION_TIMESTAMP,
        EXECUTION_USER,
        test_mode=args.test,
        publish_test=not args.no_publish  # Whether to publish in test mode
    )

if __name__ == "__main__":
    sys.exit(main())
