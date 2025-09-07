#!/usr/bin/env python3
"""
Main workflow launcher.

Key features:
- Single or multi-country execution
- Date range modes: --monthly (previous month), --daily (YTD)
- Page title updates now target config.json (single source of truth) (Option 2)
- Query date range updates preserve the original query formatting (no column tampering)
- Test mode (--test) uses a predefined test CSV (handled inside workflow)
- --no-publish skips Confluence upload
- Multi-country support with date-based folder organization
- Temporary directory management for config files and outputs
"""

import os
import sys
import json
import logging
import re
import calendar
from datetime import datetime, timedelta
import argparse
import tempfile
import shutil

# Make project root importable
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PROJECT_ROOT)

from src.workflow import run_workflow  # noqa: E402
from lib.secure_config import SecureConfig  # noqa: E402
from lib.csv_processor import CSVProcessor  # noqa: E402
from lib.confluence_publisher import publish_to_confluence  # noqa: E402

# ---------------------------------------------------------------------------
# Constants / Configuration Paths
# ---------------------------------------------------------------------------

# Database / single-country INI (used ONLY for DB connection parameters)
CONFIG_FILE = 'config/config.ini'

# Authoritative Confluence configuration (page title lives here) - FIXED PATH
TITLE_CONFIG_FILE = 'config/config.json'

# Base SQL and default output
QUERY_FILE = 'config/query.sql'
OUTPUT_CSV = 'data.csv'

# Current execution metadata - UPDATED TO CURRENT TIME
EXECUTION_TIMESTAMP = datetime.strptime('2025-09-07 23:55:27', '%Y-%m-%d %H:%M:%S')
EXECUTION_USER = 'satish537'


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging():
    log_file = f"workflow_{EXECUTION_TIMESTAMP.strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


# ---------------------------------------------------------------------------
# Credential Checks
# ---------------------------------------------------------------------------

def check_password_available():
    """
    Returns True if we can authenticate to Confluence (env password or encrypted JSON),
    otherwise False.
    """
    if os.environ.get('CONFLUENCE_PASSWORD'):
        return True
    
    # Check multiple possible locations for config.json
    config_paths = [
        "config/config.json",  # Most likely location based on your structure
        "config.json",         # Root directory fallback
        "config/config_test.json"  # Test config alternative
    ]
    
    for config_path in config_paths:
        abs_path = os.path.abspath(config_path)
        if os.path.exists(abs_path):
            try:
                with open(abs_path, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
                if 'PASSWORD_ENCRYPTED' in cfg and os.path.exists(SecureConfig.KEY_FILE):
                    return True
            except Exception:
                continue
    return False


# ---------------------------------------------------------------------------
# Date Range Helpers
# ---------------------------------------------------------------------------

def get_previous_month_date_range():
    """
    Return: (start_str, end_str, month_name) for previous calendar month.
    """
    today = datetime.now()
    first_day_current = datetime(today.year, today.month, 1)
    last_day_prev = first_day_current - timedelta(days=1)
    first_day_prev = datetime(last_day_prev.year, last_day_prev.month, 1)
    start_date_str = first_day_prev.strftime('%Y-%m-%d 00:00:00')
    end_date_str = last_day_prev.strftime('%Y-%m-%d 23:59:59')
    month_name = calendar.month_name[last_day_prev.month]
    logging.info(f"Previous month date range: {start_date_str} to {end_date_str} ({month_name})")
    return start_date_str, end_date_str, month_name


def get_ytd_date_range():
    """
    Year-to-date (Jan 1 .. today 23:59:59).
    """
    today = datetime.now()
    start_of_year = datetime(today.year, 1, 1)
    start_date_str = start_of_year.strftime('%Y-%m-%d 00:00:00')
    end_date_str = today.strftime('%Y-%m-%d 23:59:59')
    logging.info(f"YTD date range: {start_date_str} to {end_date_str}")
    return start_date_str, end_date_str


# ---------------------------------------------------------------------------
# Query Date Range Updater (Preserve Original Formatting)
# ---------------------------------------------------------------------------

def update_query_dates_preserve(query_path, start_date, end_date):
    """
    Preserve the original query.sql text; only update the BETWEEN date range.

    We look for the first occurrence of:
      BETWEEN TO_DATE('....', 'YYYY-MM-DD HH24:MI:SS') and TO_DATE('....', 'YYYY-MM-DD HH24:MI:SS')

    Replace only the two datetime literals. If pattern not found, log and return False.
    """
    import re as _re
    try:
        if not os.path.exists(query_path):
            logging.error("Query file not found: %s", query_path)
            return False

        with open(query_path, 'r', encoding='utf-8') as f:
            original = f.read()

        pattern = _re.compile(
            r"(BETWEEN\s+TO_DATE\(')"
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
            r"('\s*,\s*'YYYY-MM-DD HH24:MI:SS'\)\s+and\s+TO_DATE\(')"
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
            r"('\s*,\s*'YYYY-MM-DD HH24:MI:SS'\))",
            _re.IGNORECASE | _re.MULTILINE
        )

        def repl(m):
            return f"{m.group(1)}{start_date}{m.group(2)}{end_date}{m.group(3)}"

        new_query, count = pattern.subn(repl, original, count=1)
        if count == 0:
            logging.error("Could not locate BETWEEN date range to replace in %s", query_path)
            # Optional trace comment
            with open(query_path, 'a', encoding='utf-8') as f:
                f.write(f"\n-- WARNING: Date range replacement failed at {datetime.now()}\n")
            return False

        backup_path = f"{query_path}.bak"
        if not os.path.exists(backup_path):
            with open(backup_path, 'w', encoding='utf-8') as f:
                f.write(original)
            logging.info("Created backup at %s", backup_path)

        with open(query_path, 'w', encoding='utf-8') as f:
            f.write(new_query)

        logging.info(
            "Updated date range in %s to %s -> %s (preserved original formatting)",
            query_path, start_date, end_date
        )
        return True
    except Exception as e:
        logging.error("Error preserving query dates: %s", e)
        return False


# ---------------------------------------------------------------------------
# Page Title Updater (JSON Single Source)
# ---------------------------------------------------------------------------

def update_confluence_page_title(config_path, suffix, is_daily=False):
    """
    Update PAGE_TITLE in JSON (preferred) or INI fallback.

    Steps:
    - Strip trailing _daily
    - Strip trailing " - <MonthName>" or " - YTD"
    - Append _daily (if daily) otherwise " - <suffix>"
    """
    import configparser
    try:
        # Use absolute path to ensure we can find the config file
        abs_config_path = os.path.abspath(config_path)
        if not os.path.exists(abs_config_path):
            logging.error(f"Config file not found: {abs_config_path}")
            return False

        # JSON path (primary)
        if abs_config_path.endswith('.json'):
            try:
                with open(abs_config_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except Exception as e:
                logging.error(f"Failed reading JSON config {abs_config_path}: {e}")
                return False

            if 'PAGE_TITLE' not in data:
                logging.error(f"PAGE_TITLE key not found in JSON {abs_config_path}")
                return False

            base = data['PAGE_TITLE']
            base = re.sub(r'_daily$', '', base)
            base = re.sub(
                r'\s+-\s+(January|February|March|April|May|June|July|August|September|October|November|December|YTD)$',
                '', base
            )
            if is_daily:
                new_title = f"{base}_daily"
            else:
                new_title = f"{base} - {suffix}"

            data['PAGE_TITLE'] = new_title
            try:
                with open(abs_config_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4)
            except Exception as e:
                logging.error(f"Failed writing JSON config {abs_config_path}: {e}")
                return False

            logging.info(f"Successfully updated Confluence PAGE_TITLE in JSON to: {new_title}")
            return True

        # INI fallback (not typically used now)
        cp = configparser.ConfigParser()
        cp.optionxform = str
        with open(abs_config_path, 'r', encoding='utf-8') as f:
            raw = f.read()

        if '[' not in raw.splitlines()[0]:
            temp_raw = "[DEFAULT]\n" + raw
        else:
            temp_raw = raw
        cp.read_string(temp_raw)

        target_sections = cp.sections() if cp.sections() else ['DEFAULT']
        updated = False
        for sec in target_sections:
            if 'PAGE_TITLE' in cp[sec]:
                current = cp[sec]['PAGE_TITLE'].strip().strip('"\'')
                base = re.sub(r'_daily$', '', current)
                base = re.sub(
                    r'\s+-\s+(January|February|March|April|May|June|July|August|September|October|November|December|YTD)$',
                    '', base
                )
                new_title = f"{base}_daily" if is_daily else f"{base} - {suffix}"
                cp[sec]['PAGE_TITLE'] = new_title
                updated = True
                break

        if updated:
            with open(abs_config_path, 'w', encoding='utf-8') as f:
                if target_sections == ['DEFAULT']:
                    for k, v in cp['DEFAULT'].items():
                        if k == 'PAGE_TITLE':
                            f.write(f'PAGE_TITLE = "{v}"\n')
                        else:
                            f.write(f'{k} = {v}\n')
                else:
                    cp.write(f)
            logging.info(f"Successfully updated Confluence PAGE_TITLE in INI to: {new_title}")
            return True

        logging.error(f"Could not find PAGE_TITLE in {abs_config_path}")
        return False

    except Exception as e:
        logging.error(f"Error updating Confluence page title: {str(e)}")
        return False


# ---------------------------------------------------------------------------
# Find Config File Helper
# ---------------------------------------------------------------------------

def find_config_file(preferred_name="config.json"):
    """
    Find the config file in multiple possible locations.
    """
    possible_paths = [
        f"config/{preferred_name}",
        preferred_name,
        f"config/{preferred_name.replace('.json', '_test.json')}",
        f"{preferred_name.replace('.json', '_test.json')}"
    ]
    
    for path in possible_paths:
        abs_path = os.path.abspath(path)
        if os.path.exists(abs_path):
            logging.info(f"Found config file: {abs_path}")
            return abs_path
    
    logging.error(f"Could not find config file. Searched: {possible_paths}")
    return None


# ---------------------------------------------------------------------------
# Confluence Publisher Wrapper
# ---------------------------------------------------------------------------

def publish_with_correct_config(report_file, test_mode=False, skip_actual_upload=False, original_cwd=None):
    """
    Wrapper for publish_to_confluence that ensures config.json is found correctly.
    """
    if original_cwd is None:
        original_cwd = os.getcwd()
    
    # Save current directory
    current_dir = os.getcwd()
    
    try:
        # Change to original directory for publishing
        os.chdir(original_cwd)
        
        # Find the correct config file
        config_path = find_config_file("config_test.json" if test_mode else "config.json")
        if not config_path:
            # Fallback to any config.json
            config_path = find_config_file("config.json")
        
        if not config_path:
            logging.error("Cannot find any config file for Confluence publishing")
            return False
        
        # Copy config to current directory with expected name
        expected_name = "config_test.json" if test_mode else "config.json"
        temp_config = os.path.join(current_dir, expected_name)
        if not os.path.exists(temp_config):
            shutil.copy2(config_path, temp_config)
            logging.info(f"Copied config for publishing: {temp_config}")
        
        # Change back to where the report file is
        os.chdir(current_dir)
        
        # Now publish
        return publish_to_confluence(
            report_file=report_file,
            test_mode=test_mode,
            skip_actual_upload=skip_actual_upload
        )
        
    except Exception as e:
        logging.error(f"Error in publish_with_correct_config: {e}")
        return False
    finally:
        # Always return to current directory
        os.chdir(current_dir)


# ---------------------------------------------------------------------------
# Temporary Directory Management
# ---------------------------------------------------------------------------

def create_temp_workspace(countries, original_cwd):
    """
    Create a temporary workspace with all necessary config files and directories.
    
    Args:
        countries: List of country configurations
        original_cwd: Original working directory
        
    Returns:
        (temp_dir_path, updated_countries_list)
    """
    # Create date-based temporary directory
    date_str = EXECUTION_TIMESTAMP.strftime('%Y-%m-%d_%H%M%S')
    temp_base = os.path.join(original_cwd, 'temp_workspace', date_str)
    os.makedirs(temp_base, exist_ok=True)
    
    logging.info(f"Created temporary workspace: {temp_base}")
    
    # Copy main config files - SEARCH IN MULTIPLE LOCATIONS
    main_config = find_config_file("config.json")
    test_config = find_config_file("config_test.json")
    
    if main_config:
        dst = os.path.join(temp_base, "config.json")
        shutil.copy2(main_config, dst)
        logging.info(f"Copied main config to temp workspace: {dst}")
    
    if test_config:
        dst = os.path.join(temp_base, "config_test.json")
        shutil.copy2(test_config, dst)
        logging.info(f"Copied test config to temp workspace: {dst}")
    
    # Create subdirectories for each country
    updated_countries = []
    for country in countries:
        country_name = country['name'].lower()
        country_dir = os.path.join(temp_base, f"country_{country_name}")
        os.makedirs(country_dir, exist_ok=True)
        
        # Copy country-specific config files
        original_config = os.path.join(original_cwd, country['config_file'])
        original_query = country['query_file']
        
        # Create new paths in temp directory
        temp_config = os.path.join(country_dir, f"config_{country_name}.ini")
        temp_query = os.path.join(country_dir, f"query_{country_name}.sql")
        
        # Copy files
        if os.path.exists(original_config):
            shutil.copy2(original_config, temp_config)
            logging.info(f"Copied config for {country_name}: {temp_config}")
        
        if os.path.exists(original_query):
            shutil.copy2(original_query, temp_query)
            logging.info(f"Copied query for {country_name}: {temp_query}")
        
        # Apply date range if specified
        if 'date_range' in country:
            sd, ed = country['date_range']
            if update_query_dates_preserve(temp_query, sd, ed):
                logging.info(f"Applied date range to {country_name} query")
            else:
                logging.error(f"Failed to apply date range to {country_name} query")
        
        # Update country configuration with new paths
        updated_country = country.copy()
        updated_country['config_file'] = temp_config
        updated_country['query_file'] = temp_query
        updated_country['country_dir'] = country_dir
        updated_country['output_csv'] = os.path.join(country_dir, f"data_{country_name}.csv")
        
        updated_countries.append(updated_country)
    
    # Create reports directory in temp workspace
    reports_dir = os.path.join(temp_base, 'reports')
    os.makedirs(reports_dir, exist_ok=True)
    
    return temp_base, updated_countries


# ---------------------------------------------------------------------------
# Multi-Country Workflow Function
# ---------------------------------------------------------------------------

def run_workflow_multi(countries, default_output_csv, execution_timestamp, execution_user, test_mode=False, publish_test=True):
    """
    Run workflow for multiple countries and aggregate results.
    
    Args:
        countries: List of country configs with name, config_file, query_file
        default_output_csv: Base output CSV name
        execution_timestamp: Timestamp for execution
        execution_user: Username of executing user
        test_mode: Whether to run in test mode
        publish_test: Whether to publish to Confluence in test mode
    """
    # Store original working directory
    original_cwd = os.getcwd()
    
    try:
        # Create temporary workspace with all config files
        temp_workspace, updated_countries = create_temp_workspace(countries, original_cwd)
        
        # Track generated CSV files and their country info
        country_results = []
        
        # Process each country
        for country in updated_countries:
            country_name = country['name']
            config_file = country['config_file']
            query_file = country['query_file']
            output_csv = country['output_csv']
            country_dir = country['country_dir']
            
            logging.info(f"Processing country: {country_name}")
            
            # Change to country directory for processing
            os.chdir(country_dir)
            
            # Run workflow for this country
            result = run_workflow(
                config_file,
                query_file,
                os.path.basename(output_csv),  # Use basename since we're in country dir
                execution_timestamp,
                execution_user,
                test_mode=test_mode,
                publish_test=False  # Don't publish individual countries
            )
            
            if result == 0:
                country_results.append({
                    'name': country_name,
                    'csv_file': output_csv,
                    'success': True
                })
                logging.info(f"Successfully processed {country_name}")
            else:
                country_results.append({
                    'name': country_name,
                    'csv_file': output_csv,
                    'success': False
                })
                logging.error(f"Failed to process {country_name}")
        
        # Change back to temp workspace root for aggregation
        os.chdir(temp_workspace)
        
        # Check if any countries were processed successfully
        successful_countries = [r for r in country_results if r['success']]
        if not successful_countries:
            logging.error("No countries processed successfully")
            return False
        
        # Copy all successful CSV files to reports directory for aggregation
        reports_dir = os.path.join(temp_workspace, 'reports')
        for result in successful_countries:
            if os.path.exists(result['csv_file']):
                dst = os.path.join(reports_dir, f"data_{result['name'].lower()}.csv")
                shutil.copy2(result['csv_file'], dst)
                logging.info(f"Copied {result['name']} data to reports: {dst}")
        
        # Change to reports directory for aggregation
        os.chdir(reports_dir)
        
        # Aggregate all country CSVs
        logging.info("Aggregating all country data...")
        processor = CSVProcessor(execution_timestamp, execution_user)
        
        # Process all country CSV files to create aggregated reports
        success = processor.process_all_files()
        if not success:
            logging.error("Failed to aggregate country data")
            return False
        
        # Copy aggregated reports to main directory for publishing
        aggregated_report = "task_usage_report_by_region.csv"
        detailed_report = "task_usage_report.csv"
        
        main_aggregated_path = os.path.join(original_cwd, aggregated_report)
        main_detailed_path = os.path.join(original_cwd, detailed_report)
        
        if os.path.exists(aggregated_report):
            shutil.copy2(aggregated_report, main_aggregated_path)
            logging.info(f"Copied aggregated report to {main_aggregated_path}")
        
        if os.path.exists(detailed_report):
            shutil.copy2(detailed_report, main_detailed_path)
            logging.info(f"Copied detailed report to {main_detailed_path}")
        
        # Publish aggregated report if not skipping
        if publish_test:
            logging.info("Publishing aggregated report to Confluence...")
            
            # Use the wrapper function that handles config file paths correctly
            publish_success = publish_with_correct_config(
                report_file=aggregated_report,
                test_mode=test_mode,
                skip_actual_upload=False,
                original_cwd=original_cwd
            )
            if not publish_success:
                logging.error("Failed to publish to Confluence")
                return False
        
        # Change back to original directory
        os.chdir(original_cwd)
        
        # Log summary of results
        logging.info("="*60)
        logging.info("MULTI-COUNTRY WORKFLOW SUMMARY")
        logging.info("="*60)
        logging.info(f"Workspace Location: {temp_workspace}")
        logging.info(f"Total Countries: {len(country_results)}")
        logging.info(f"Successful: {len(successful_countries)}")
        logging.info(f"Failed: {len(country_results) - len(successful_countries)}")
        
        for result in country_results:
            status = "✓ SUCCESS" if result['success'] else "✗ FAILED"
            logging.info(f"  {result['name']}: {status}")
        
        if successful_countries:
            logging.info(f"Aggregated reports created and {'published' if publish_test else 'ready for publishing'}")
        
        logging.info("="*60)
        
        return len(successful_countries) > 0
        
    except Exception as e:
        logging.error(f"Error in multi-country workflow: {e}")
        return False
    finally:
        # Ensure we're back in the original directory
        os.chdir(original_cwd)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Run data workflow")
    parser.add_argument("--test", action="store_true", help="Run in test mode using predefined test CSV")
    parser.add_argument("--no-publish", action="store_true", help="Skip actual publishing to Confluence")
    parser.add_argument("--monthly", action="store_true", help="Use previous month date range in SQL query")
    parser.add_argument("--daily", action="store_true", help="Use year-to-date range in SQL query")
    parser.add_argument("--countries-config", help="Path to countries JSON to run multi-country workflow")
    args = parser.parse_args()

    setup_logging()
    os.makedirs('config', exist_ok=True)

    # Basic presence checks for single-country DB files if not test & not multi
    if not args.test and not args.countries_config:
        if not os.path.exists(CONFIG_FILE):
            logging.error(f"Config file {CONFIG_FILE} not found.")
            return 1
        if not os.path.exists(QUERY_FILE):
            logging.error(f"Query file {QUERY_FILE} not found.")
            return 1

    if args.monthly and args.daily:
        logging.error("Cannot use both --monthly and --daily flags together.")
        return 1

    date_range = None
    month_name = None
    if args.monthly:
        sd, ed, month_name = get_previous_month_date_range()
        date_range = (sd, ed, month_name)
    elif args.daily:
        sd, ed = get_ytd_date_range()
        date_range = (sd, ed, None)

    # Authentication check (unless skipping publish)
    if not args.no_publish and not check_password_available():
        print("No Confluence authentication credentials found.")
        print("1. Set CONFLUENCE_PASSWORD env var")
        print("2. Or run setup_secure_config.py to create encrypted config.json")
        print("3. Or use --no-publish to skip publishing")
        return 1

    # -----------------------------------------------------------------------
    # Multi-country Flow
    # -----------------------------------------------------------------------
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

        prepared = []
        for c in countries:
            name = c.get("name")
            cfg_path = c.get("config_file") or CONFIG_FILE
            qry_path = c.get("query_file") or QUERY_FILE
            if not name or not cfg_path:
                logging.error("Each country must have 'name' and 'config_file'")
                return 1
            if not args.test and not os.path.exists(cfg_path):
                logging.error(f"Config file for {name} not found: {cfg_path}")
                return 1
            if not args.test and not os.path.exists(qry_path):
                logging.error(f"Query file for {name} not found: {qry_path}")
                return 1

            prepared.append({"name": name, "config_file": cfg_path, "query_file": qry_path})

        # Update title ONCE (global JSON) not per country
        if args.monthly and month_name:
            update_confluence_page_title(TITLE_CONFIG_FILE, month_name, is_daily=False)
        elif args.daily:
            update_confluence_page_title(TITLE_CONFIG_FILE, "", is_daily=True)

        # Apply date range updates to prepared countries in temp workspace
        if date_range:
            sd, ed, _ = date_range
            for country in prepared:
                # The temp workspace will handle copying and updating query files
                country['date_range'] = (sd, ed)

        result = run_workflow_multi(
            countries=prepared,
            default_output_csv=OUTPUT_CSV,
            execution_timestamp=EXECUTION_TIMESTAMP,
            execution_user=EXECUTION_USER,
            test_mode=args.test,
            publish_test=not args.no_publish
        )
        return 0 if result else 1

    # -----------------------------------------------------------------------
    # Single-country Flow
    # -----------------------------------------------------------------------
    if args.monthly:
        sd, ed, month_name = date_range
        if not update_query_dates_preserve(QUERY_FILE, sd, ed):
            logging.error("Failed to update query dates for previous month")
            return 1
        if not update_confluence_page_title(TITLE_CONFIG_FILE, month_name, is_daily=False):
            logging.warning("Failed to update Confluence page title (JSON).")
        else:
            logging.info(f"Confluence page title updated to include: {month_name}")
        logging.info(f"Query date range updated (monthly) by {EXECUTION_USER}")

    elif args.daily:
        sd, ed, _ = date_range
        if not update_query_dates_preserve(QUERY_FILE, sd, ed):
            logging.error("Failed to update query dates for YTD")
            return 1
        if not update_confluence_page_title(TITLE_CONFIG_FILE, "", is_daily=True):
            logging.warning("Failed to update Confluence page title (JSON).")
        else:
            logging.info("Confluence page title updated to append: _daily")
        logging.info(f"Query date range updated (daily/YTD) by {EXECUTION_USER}")

    # Run single workflow
    result = run_workflow(
        CONFIG_FILE,
        QUERY_FILE,
        OUTPUT_CSV,
        EXECUTION_TIMESTAMP,
        EXECUTION_USER,
        test_mode=args.test,
        publish_test=not args.no_publish
    )
    return result


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sys.exit(main())
