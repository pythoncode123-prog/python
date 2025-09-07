#!/usr/bin/env python3
import os
import sys
import json
import logging
import re
import calendar
from datetime import datetime, timedelta
import argparse
import tempfile

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.workflow import run_workflow
from lib.secure_config import SecureConfig

# Configuration
CONFIG_FILE = 'config/config.ini'
QUERY_FILE = 'config/query.sql'
OUTPUT_CSV = 'data.csv'
EXECUTION_TIMESTAMP = datetime.strptime('2025-07-07 00:13:56', '%Y-%m-%d %H:%M:%S')  # Updated timestamp
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
    """Update the query.sql file with specified date range and REMOVE country field."""
    try:
        # Create a new SQL query with proper date range WITHOUT country field
        new_query = f"""-- Query updated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} by {EXECUTION_USER}
-- Date range: {start_date} to {end_date}

SELECT 
    TO_CHAR(net_report.net_date, 'YYYY-MM-DD') AS NET_DATE,
    COUNT(*) AS TOTAL_JOBS
FROM
    CTMHK0068.net_report,
    CTMHK0068.net_report_data
WHERE
    net_report.report_id = net_report_data.report_id
    AND net_report_data.fname = 'NODE_ID'
    AND net_report.net_date BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD HH24:MI:SS')
    AND TO_DATE('{end_date}', 'YYYY-MM-DD HH24:MI:SS')
GROUP BY TO_CHAR(net_report.net_date, 'YYYY-MM-DD')
ORDER BY TO_CHAR(net_report.net_date, 'YYYY-MM-DD');
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
        logging.info("COUNTRY field removed from query")
        
        return True
    except Exception as e:
        logging.error(f"Error updating query file: {str(e)}")
        return False

def update_confluence_page_title(config_path, suffix, is_daily=False):
    """
    Update the Confluence page title in the config file.
    
    Args:
        config_path: Path to the config file
        suffix: Suffix to add to the page title
        is_daily: If True, append suffix as "_daily", otherwise use " - suffix"
    """
    try:
        # Check if file exists
        if not os.path.exists(config_path):
            logging.error(f"Config file not found: {config_path}")
            return False
        
        # Read the entire file content
        with open(config_path, 'r') as f:
            content = f.read()
        
        # Use regex to find and update the PAGE_TITLE parameter
        pattern = r'(PAGE_TITLE\s*=\s*["\'])([^"\']*?)(["\'])'
        
        match = re.search(pattern, content)
        if match:
            # Get the current title and strip any existing suffix
            current_title = match.group(2)
            
            # Remove any existing suffix pattern (both formats)
            base_title = re.sub(r'_daily$', '', current_title)  # Remove _daily if exists
            base_title = re.sub(r'\s+-\s+(January|February|March|April|May|June|July|August|September|October|November|December|YTD)$', '', base_title)  # Remove month/YTD suffix
            
            # Create new title with appropriate suffix format
            if is_daily:
                new_title = f"{base_title}_daily"
            else:
                new_title = f"{base_title} - {suffix}"
            
            # Replace in the content
            updated_content = re.sub(pattern, r'\1' + new_title + r'\3', content)
            
            # Write back to file
            with open(config_path, 'w') as f:
                f.write(updated_content)
                
            logging.info(f"Successfully updated Confluence page title to: {new_title}")
            return True
        else:
            logging.error(f"Could not find PAGE_TITLE parameter in {config_path}")
            return False
            
    except Exception as e:
        logging.error(f"Error updating Confluence page title: {str(e)}")
        return False

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run data workflow")
    parser.add_argument("--test", action="store_true", help="Run in test mode using predefined test CSV")
    parser.add_argument("--no-publish", action="store_true", help="Skip actual publishing to Confluence")
    parser.add_argument("--monthly", action="store_true", help="Use previous month date range in SQL query")
    parser.add_argument("--daily", action="store_true", help="Use year-to-date range in SQL query")
    parser.add_argument("--countries-config", help="Path to countries JSON to run multi-country workflow")
    args = parser.parse_args()

    setup_logging()
    os.makedirs('config', exist_ok=True)

    # If not in test mode, check if config files exist
    if not args.test and not args.countries_config:
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
    
    date_range = None
    month_name = None
    if args.monthly:
        start_date, end_date, month_name = get_previous_month_date_range()
        date_range = (start_date, end_date, month_name)
    elif args.daily:
        start_date, end_date = get_ytd_date_range()
        date_range = (start_date, end_date, None)

    # MULTI-COUNTRY LOGIC (countries.json)
    if args.countries_config:
        try:
            with open(args.countries_config, "r", encoding='utf-8') as f:
                countries_cfg = json.load(f)
            countries = countries_cfg.get("countries", [])
            if not countries:
                logging.error(f"No countries defined in {args.countries_config}")
                return 1
        except Exception as e:
            logging.error(f"Failed to read countries config: {e}")
            return 1

        for c in countries:
            name = c.get("name")
            cfg_path = c.get("config_file") or CONFIG_FILE
            qry_path = c.get("query_file") or QUERY_FILE
            out_csv = f"data_{name.lower()}.csv"

            if not name or not cfg_path:
                logging.error("Each country must have 'name' and 'config_file'")
                continue
            if not args.test and not os.path.exists(cfg_path):
                logging.error(f"Config file for {name} not found: {cfg_path}")
                continue
            if not args.test and not os.path.exists(qry_path):
                logging.error(f"Query file for {name} not found: {qry_path}")
                continue

            # Per-country date range update (if needed)
            if date_range:
                sd, ed, _ = date_range
                tmp_qry = os.path.join(tempfile.gettempdir(), f"query_{name}.sql")
                with open(qry_path, 'r', encoding='utf-8') as src, open(tmp_qry, 'w', encoding='utf-8') as dst:
                    dst.write(src.read())
                if not update_query_with_date_range(tmp_qry, sd, ed):
                    logging.error(f"Failed to update query dates for {name}")
                    continue
                qry_path_use = tmp_qry
            else:
                qry_path_use = qry_path

            logging.info(f"Running workflow for country: {name}")
            run_workflow(cfg_path, qry_path_use, out_csv, EXECUTION_TIMESTAMP, EXECUTION_USER,
                         test_mode=args.test, publish_test=False)  # Only produce CSVs, don't publish yet

        # --- AGGREGATION STEP ---
        logging.info("Aggregating all country CSVs...")
        from lib.csv_processor import CSVProcessor
        processor = CSVProcessor(EXECUTION_TIMESTAMP, EXECUTION_USER)
        processor.process_all_files()  # This processes all data_*.csv files and creates region reports

        # --- PUBLISHING STEP ---
        try:
            from lib.confluence_publisher import publish_to_confluence
            publish_success = publish_to_confluence(
                report_file="task_usage_report_by_region.csv",
                test_mode=args.test,
                skip_actual_upload=args.no_publish
            )
            if not publish_success:
                logging.error("Failed to publish to Confluence.")
                return 3
        except ImportError:
            logging.warning("Confluence publisher not found or not implemented, skipping publish.")

        logging.info("Workflow completed for all countries and published to Confluence.")
        return 0

    # SINGLE-COUNTRY LOGIC (legacy, unchanged)
    if args.monthly:
        start_date, end_date, month_name = date_range
        if not update_query_with_date_range(QUERY_FILE, start_date, end_date):
            logging.error("Failed to update query with previous month dates")
            return 1
        if not update_confluence_page_title(CONFIG_FILE, month_name, is_daily=False):
            logging.warning("Failed to update Confluence page title. Will continue with existing title.")
        else:
            logging.info(f"Confluence page title updated to include: {month_name}")
        logging.info(f"Query updated to use {month_name} date range by {EXECUTION_USER}")
    
    elif args.daily:
        start_date, end_date, _ = date_range
        if not update_query_with_date_range(QUERY_FILE, start_date, end_date):
            logging.error("Failed to update query with year-to-date range")
            return 1
        if not update_confluence_page_title(CONFIG_FILE, "", is_daily=True):
            logging.warning("Failed to update Confluence page title. Will continue with existing title.")
        else:
            logging.info("Confluence page title updated to append: _daily")
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
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
