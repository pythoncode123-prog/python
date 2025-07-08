#!/usr/bin/env python3
import os
import sys
import json
import logging
import re
import calendar
from datetime import datetime, timedelta
import argparse
import shutil
from concurrent.futures import ThreadPoolExecutor

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.workflow import run_workflow
from lib.secure_config import SecureConfig

# Configuration
BASE_CONFIG_DIR = 'config'
OUTPUT_CSV_TEMPLATE = 'data_{region}.csv'
EXECUTION_TIMESTAMP = datetime.strptime('2025-07-08 00:10:23', '%Y-%m-%d %H:%M:%S')  # Updated timestamp
EXECUTION_USER = 'satish537'

# Define supported regions
REGIONS = ['HK', 'UK', 'MEXICO', 'US']

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

def get_region_config_path(region):
    """Get the configuration path for a specific region."""
    return os.path.join(BASE_CONFIG_DIR, region, 'config.ini')

def get_region_query_path(region):
    """Get the query path for a specific region."""
    return os.path.join(BASE_CONFIG_DIR, region, 'query.sql')

def ensure_region_directories():
    """Ensure all region directories exist with config templates."""
    for region in REGIONS:
        region_dir = os.path.join(BASE_CONFIG_DIR, region)
        os.makedirs(region_dir, exist_ok=True)
        
        # Create config.ini template if it doesn't exist
        config_path = get_region_config_path(region)
        if not os.path.exists(config_path):
            create_region_config_template(region, config_path)
            
        # Create query.sql template if it doesn't exist
        query_path = get_region_query_path(region)
        if not os.path.exists(query_path):
            create_region_query_template(region, query_path)

def create_region_config_template(region, config_path):
    """Create a config.ini template for a specific region."""
    # Default config templates for each region
    templates = {
        'HK': f"""[DATABASE]
DBType = ORACLE
DBHost=hkl25051992n2-scan-sm.hk.hsbc
Port=2001
ServiceName=DHK_00068_01.hk.hsbc
SchemaName=CTMHK0068
Username=DBAWPHKU1
Password=CTMPortalL%%67!

[CONFLUENCE]
URL = https://alm-confluence.systems.uk.hsbc/confluence/rest/api/content/
USERNAME = 45292857
AUTH_TYPE = basic
SPACE_KEY = DIGIBAP
PAGE_TITLE = CIReleaseNo919

[GENERAL]
REGION = HK
BASELINE = 1899206
""",
        'UK': f"""[DATABASE]
DBType = ORACLE
DBHost=ukl25051992n2-scan-sm.uk.hsbc
Port=2001
ServiceName=DUK_00068_01.uk.hsbc
SchemaName=CTMUK0068
Username=DBAWPUKU1
Password=CTMPortalL%%67!

[CONFLUENCE]
URL = https://alm-confluence.systems.uk.hsbc/confluence/rest/api/content/
USERNAME = 45292857
AUTH_TYPE = basic
SPACE_KEY = DIGIBAP
PAGE_TITLE = CIReleaseNo919

[GENERAL]
REGION = UK
BASELINE = 1899206
""",
        'MEXICO': f"""[DATABASE]
DBType = ORACLE
DBHost=mxl25051992n2-scan-sm.mx.hsbc
Port=2001
ServiceName=DMX_00068_01.mx.hsbc
SchemaName=CTMMX0068
Username=DBAWPMXU1
Password=CTMPortalL%%67!

[CONFLUENCE]
URL = https://alm-confluence.systems.uk.hsbc/confluence/rest/api/content/
USERNAME = 45292857
AUTH_TYPE = basic
SPACE_KEY = DIGIBAP
PAGE_TITLE = CIReleaseNo919

[GENERAL]
REGION = MEXICO
BASELINE = 1899206
""",
        'US': f"""[DATABASE]
DBType = ORACLE
DBHost=usl25051992n2-scan-sm.us.hsbc
Port=2001
ServiceName=DUS_00068_01.us.hsbc
SchemaName=CTMUS0068
Username=DBAWPUSU1
Password=CTMPortalL%%67!

[CONFLUENCE]
URL = https://alm-confluence.systems.uk.hsbc/confluence/rest/api/content/
USERNAME = 45292857
AUTH_TYPE = basic
SPACE_KEY = DIGIBAP
PAGE_TITLE = CIReleaseNo919

[GENERAL]
REGION = US
BASELINE = 1899206
"""
    }
    
    # Create the config file using the template
    with open(config_path, 'w') as f:
        f.write(templates[region])
    
    logging.info(f"Created config template for {region} at {config_path}")

def create_region_query_template(region, query_path):
    """Create a query.sql template for a specific region."""
    # Create a basic query template (will be updated by update_query_with_date_range)
    schema_name = f"CTM{region}0068" if region != "MEXICO" else "CTMMX0068"
    
    query_template = f"""-- Query template for {region} region
-- This will be updated when the script runs

SELECT 
    net_report.net_date as DATE,
    net_report.ctm_host_name as REGION,
    net_report_data.fvalue as ENV,
    net_report_data.jobs as TOTAL_JOBS
FROM 
    {schema_name}.net_report,
    {schema_name}.net_report_data
WHERE 
    net_report.report_id=net_report_data.report_id 
    AND net_report_data.fname='NODE_ID' 
    AND net_report.net_date 
    BETWEEN TO_DATE('2025-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') 
    AND TO_DATE('2025-01-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
ORDER BY
    net_report.net_date ASC;
"""
    
    with open(query_path, 'w') as f:
        f.write(query_template)
    
    logging.info(f"Created query template for {region} at {query_path}")

def update_query_with_date_range(query_path, start_date, end_date):
    """Update the query.sql file with specified date range (keeping original query structure)."""
    try:
        # Create a new SQL query with proper date range but keep the structure the same
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
    AND net_report_data.fname='NODE_ID' 
    AND net_report.net_date 
    BETWEEN TO_DATE('{start_date}', 'YYYY-MM-DD HH24:MI:SS') 
    AND TO_DATE('{end_date}', 'YYYY-MM-DD HH24:MI:SS')
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

def update_confluence_page_title(config_path, suffix, is_daily=False):
    """
    Update the Confluence page title in the config file and set environment variable.
    
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
        pattern = r'(PAGE_TITLE\s*=\s*["\']?)([^"\'\n]*?)(["\']?\s*$)'
        
        match = re.search(pattern, content, re.MULTILINE)
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
            updated_content = re.sub(pattern, r'\1' + new_title + r'\3', content, flags=re.MULTILINE)
            
            # Write back to file
            with open(config_path, 'w') as f:
                f.write(updated_content)
            
            # Set environment variable for conf_publish.py to use
            os.environ["CONFIG_TITLE"] = new_title
                
            logging.info(f"Successfully updated Confluence page title to: {new_title}")
            return True
        else:
            logging.error(f"Could not find PAGE_TITLE parameter in {config_path}")
            return False
            
    except Exception as e:
        logging.error(f"Error updating Confluence page title: {str(e)}")
        return False

def process_region(region, args, start_date, end_date, suffix=None, is_daily=False):
    """Process a single region with the given parameters."""
    try:
        logging.info(f"Processing region: {region}")
        
        # Get paths for this region
        config_path = get_region_config_path(region)
        query_path = get_region_query_path(region)
        output_csv = OUTPUT_CSV_TEMPLATE.format(region=region.lower())
        
        # Update query with date range
        if not update_query_with_date_range(query_path, start_date, end_date):
            logging.error(f"Failed to update query for region {region}")
            return False
        
        # Update Confluence page title if needed
        if suffix:
            if not update_confluence_page_title(config_path, suffix, is_daily):
                logging.warning(f"Failed to update Confluence page title for {region}. Will continue with existing title.")
        
        # Run the workflow for this region
        result = run_workflow(
            config_path,
            query_path,
            output_csv,
            EXECUTION_TIMESTAMP,
            EXECUTION_USER,
            test_mode=args.test,
            publish_test=not args.no_publish,
            region=region
        )
        
        logging.info(f"Completed processing for region {region} with result: {result}")
        return result
        
    except Exception as e:
        logging.error(f"Error processing region {region}: {str(e)}")
        return False

def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run data workflow for multiple regions")
    parser.add_argument("--test", action="store_true", help="Run in test mode using predefined test CSV")
    parser.add_argument("--no-publish", action="store_true", help="Skip actual publishing to Confluence")
    parser.add_argument("--monthly", action="store_true", help="Use previous month date range in SQL query")
    parser.add_argument("--daily", action="store_true", help="Use year-to-date range in SQL query")
    parser.add_argument("--regions", type=str, help="Comma-separated list of regions to process (default: all)")
    parser.add_argument("--parallel", action="store_true", help="Process regions in parallel")
    args = parser.parse_args()

    # Setup logging
    setup_logging()

    # Create region directories and config templates
    ensure_region_directories()

    # Determine which regions to process
    regions_to_process = REGIONS
    if args.regions:
        regions_list = [r.strip().upper() for r in args.regions.split(',')]
        regions_to_process = [r for r in regions_list if r in REGIONS]
        if not regions_to_process:
            logging.error(f"No valid regions specified. Valid regions are: {', '.join(REGIONS)}")
            return 1
    
    logging.info(f"Processing regions: {', '.join(regions_to_process)}")
    
    # Handle date range flags (monthly and daily)
    # Ensure only one flag is used
    if args.monthly and args.daily:
        logging.error("Cannot use both --monthly and --daily flags together. Please choose one.")
        return 1
    
    # Determine date range and suffix for Confluence page title
    start_date = end_date = suffix = None
    is_daily = False
    
    if args.monthly:
        logging.info("Running with monthly date range flag (for PREVIOUS month)")
        start_date, end_date, month_name = get_previous_month_date_range()
        suffix = month_name
        is_daily = False
    elif args.daily:
        logging.info("Running with year-to-date range flag")
        start_date, end_date = get_ytd_date_range()
        is_daily = True
    else:
        logging.error("Must specify either --monthly or --daily flag")
        return 1

    # Check for password availability if publishing
    if not args.no_publish and not check_password_available():
        print("No Confluence authentication credentials found.")
        print("Please either:")
        print("1. Set CONFLUENCE_PASSWORD environment variable")
        print("2. Run setup_secure_config.py to encrypt your password in config.json")
        print("3. Use --no-publish flag to skip publishing")
        return 1

    # Process each region
    results = {}
    
    if args.parallel and len(regions_to_process) > 1:
        # Process regions in parallel
        with ThreadPoolExecutor(max_workers=min(4, len(regions_to_process))) as executor:
            futures = {
                executor.submit(process_region, region, args, start_date, end_date, suffix, is_daily): region
                for region in regions_to_process
            }
            
            for future in futures:
                region = futures[future]
                try:
                    results[region] = future.result()
                except Exception as e:
                    logging.error(f"Error processing region {region}: {str(e)}")
                    results[region] = False
    else:
        # Process regions sequentially
        for region in regions_to_process:
            results[region] = process_region(region, args, start_date, end_date, suffix, is_daily)
    
    # Log summary of results
    logging.info("=== Processing Summary ===")
    all_success = True
    for region, success in results.items():
        logging.info(f"Region {region}: {'SUCCESS' if success else 'FAILED'}")
        if not success:
            all_success = False
    
    return 0 if all_success else 1

if __name__ == "__main__":
    sys.exit(main())
