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
from src.publish_confluence import publish_to_confluence  # Import the new function

# Configuration
CONFIG_FILE = 'config/config.ini'
QUERY_FILE = 'config/query.sql'
OUTPUT_CSV = 'data.csv'
EXECUTION_TIMESTAMP = datetime.strptime('2025-07-06 23:06:46', '%Y-%m-%d %H:%M:%S')  # Updated timestamp
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

def get_previous_month_date_range():
    """Get the start and end date of the previous month."""
    today = datetime.now()
    
    # Calculate first day of current month
    first_day_of_current_month = datetime(today.year, today.month, 1)
    
    # Calculate last day of previous month (one day before first day of current month)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    
    # Calculate first day of previous month
    first_day_of_previous_month = datetime(last_day_of_previous_month.year, last_day_of_previous_month.month, 1)
    
    start_date_str = first_day_of_previous_month.strftime('%Y-%m-%d 00:00:00')
    end_date_str = last_day_of_previous_month.strftime('%Y-%m-%d 23:59:59')
    
    # Get month name for Confluence page
    month_name = calendar.month_name[last_day_of_previous_month.month]
    
    logging.info(f"Previous month date range: {start_date_str} to {end_date_str} ({month_name})")
    return start_date_str, end_date_str, month_name

def get_ytd_date_range():
    """Get the date range from start of current year to today."""
    today = datetime.now()
    start_of_year = datetime(today.year, 1, 1)
    
    start_date_str = start_of_year.strftime('%Y-%m-%d 00:00:00')
    today_str = today.strftime('%Y-%m-%d 23:59:59')
    
    logging.info(f"Year-to-date range: {start_date_str} to {today_str}")
    return start_date_str, today_str

def update_query_with_date_range(query_path, start_date, end_date):
    """Update the query.sql file with specified date range."""
    try:
        # Create a new SQL query with proper date range (without country field)
        new_query = f"""-- Query updated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} by {EXECUTION_USER}
-- Date range: {start_date} to {end_date}

SELECT 
    net_report.net_date as DATE,
    net_report.ctm_host_name as REGION,
    net_report_data.fvalue as ENV,
    net_report_data.jobs as TOTAL_JOBS
FROM 
    CTMHK0068.net_report,
    CTMHK0068.net_report_data
WHERE 
    net_report.report_id=net_report_data.report_id 
    and net_report_data.fname='NODE_ID' 
    and net_report.net_date 
    BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD HH24:MI:SS') 
    and TO_DATE('{end_date}', 'YYYY-MM-DD HH24:MI:SS')
ORDER BY
    net_report.net_date ASC;
"""
        # Create a backup of the original query first
        backup_path = f"{query_path}.bak"
        if os.path.exists(query_path):
            with open(query_path, 'r') as f:
                original_query = f.read()
            with open(backup_path, 'w') as f:
                f.write(original_query)
            logging.info(f"Created backup of original query at {backup_path}")

        # Write the new query
        with open(query_path, 'w') as f:
            f.write(new_query)
            
        logging.info(f"Successfully created new query with date range: {start_date} to {end_date}")
        
        return True
    except Exception as e:
        logging.error(f"Error updating query file: {str(e)}")
        return False

def update_confluence_page_title(config_path, suffix):
    """Update the Confluence page title in the config file to include the suffix."""
    try:
        # Check if file exists
        if not os.path.exists(config_path):
            logging.error(f"Config file not found: {config_path}")
            return None
            
        # Read the config file
        with open(config_path, 'r') as f:
            config_content = f.readlines()
        
        # Find the PAGE_TITLE line and update it
        page_title_found = False
        updated_content = []
        
        for line in config_content:
            if 'PAGE_TITLE' in line and '=' in line:
                page_title_found = True
                # Extract the base title (remove existing suffix if present)
                parts = line.split('=')
                if len(parts) >= 2:
                    base_title = parts[1].strip().strip('"\'')
                    # Remove any existing suffix
                    base_title = re.sub(r'\s+-\s+(January|February|March|April|May|June|July|August|September|October|November|December|YTD)$', '', base_title)
                    # Add the new suffix and update the line
                    new_title = f"{base_title} - {suffix}"
                    variable_name = parts[0].strip()
                    updated_line = f"{variable_name} = \"{new_title}\"\n"
                    updated_content.append(updated_line)
                    logging.info(f"Updated Confluence page title to: {new_title}")
                else:
                    updated_content.append(line)
            else:
                updated_content.append(line)
        
        if not page_title_found:
            logging.warning("PAGE_TITLE not found in config file. The Confluence page title will not be updated.")
            return None
            
        # Write back the updated config
        with open(config_path, 'w') as f:
            f.writelines(updated_content)
            
        return True
    except Exception as e:
        logging.error(f"Error updating Confluence page title: {str(e)}")
        return None

def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run data workflow")
    parser.add_argument("--test", action="store_true", help="Run in test mode using predefined test CSV")
    parser.add_argument("--no-publish", action="store_true", help="Skip actual publishing to Confluence")
    parser.add_argument("--monthly", action="store_true", help="Use previous month date range in SQL query")
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
        logging.info("Running with monthly date range flag (for PREVIOUS month)")
        start_date, end_date, month_name = get_previous_month_date_range()
        
        # Update SQL query with date range (without country field)
        if not update_query_with_date_range(QUERY_FILE, start_date, end_date):
            logging.error("Failed to update query with previous month dates")
            return 1
            
        # Update Confluence page title with month name
        update_result = update_confluence_page_title(CONFIG_FILE, month_name)
        if update_result:
            logging.info(f"Confluence page title updated to include: {month_name}")
        else:
            logging.warning("Failed to update Confluence page title. Will continue with existing title.")
        
        logging.info(f"Query updated to use {month_name} date range by {EXECUTION_USER}")
    
    elif args.daily:
        logging.info("Running with year-to-date range flag")
        start_date, end_date = get_ytd_date_range()
        
        # Update SQL query with date range (without country field)
        if not update_query_with_date_range(QUERY_FILE, start_date, end_date):
            logging.error("Failed to update query with year-to-date range")
            return 1
            
        # Update Confluence page title with YTD
        update_result = update_confluence_page_title(CONFIG_FILE, "YTD")
        if update_result:
            logging.info("Confluence page title updated to include: YTD")
        else:
            logging.warning("Failed to update Confluence page title. Will continue with existing title.")
            
        logging.info(f"Query updated to use year-to-date range by {EXECUTION_USER}")

    # Check for password availability if publishing
    if not args.no_publish and not check_password_available():
        print("No Confluence authentication credentials found.")
        print("Please either:")
        print("1. Set CONFLUENCE_PASSWORD environment variable")
        print("2. Run setup_secure_config.py to encrypt your password in config.json")
        print("3. Use --no-publish flag to skip publishing")
        return 1

    # Run the workflow to export data to CSV
    workflow_result = run_workflow(
        CONFIG_FILE,
        QUERY_FILE,
        OUTPUT_CSV,
        EXECUTION_TIMESTAMP,
        EXECUTION_USER,
        test_mode=args.test,
        publish_test=False  # Skip the default publishing mechanism
    )
    
    # If workflow completed and publishing is enabled, use our custom publish function
    if workflow_result and not args.no_publish and os.path.exists(OUTPUT_CSV):
        logging.info("Publishing data to Confluence with embedded task usage graph...")
        publish_result = publish_to_confluence(
            OUTPUT_CSV,
            CONFIG_FILE,
            EXECUTION_TIMESTAMP,
            EXECUTION_USER
        )
        
        if publish_result:
            logging.info("Successfully published data with graph to Confluence")
        else:
            logging.error("Failed to publish data to Confluence")
            return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
