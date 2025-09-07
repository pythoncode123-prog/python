#!/usr/bin/env python3
"""
Main workflow launcher.

Key features:
- Single or multi-country execution (via --countries-config)
- Date range modes: --monthly (previous month), --daily (YTD)
- Page title updates now target config.json (single source of truth)
- Query date range updates preserve the original query formatting
- Test mode (--test) uses a predefined test CSV
- --no-publish skips Confluence upload
"""

import os
import sys
import json
import logging
import calendar
from datetime import datetime, timedelta
import argparse
import tempfile

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PROJECT_ROOT)

from workflow import run_workflow
from lib.secure_config import SecureConfig  # noqa: E402

# ---------------------------------------------------------------------------
# Constants / Configuration Paths
# ---------------------------------------------------------------------------

CONFIG_FILE = 'config/config.ini'
TITLE_CONFIG_FILE = 'config.json'
QUERY_FILE = 'config/query.sql'
OUTPUT_CSV = 'data.csv'
EXECUTION_TIMESTAMP = datetime.strptime('2025-07-07 00:13:56', '%Y-%m-%d %H:%M:%S')
EXECUTION_USER = 'satish537'

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

def get_previous_month_date_range():
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
    today = datetime.now()
    start_of_year = datetime(today.year, 1, 1)
    start_date_str = start_of_year.strftime('%Y-%m-%d 00:00:00')
    end_date_str = today.strftime('%Y-%m-%d 23:59:59')
    logging.info(f"YTD date range: {start_date_str} to {end_date_str}")
    return start_date_str, end_date_str

def update_query_dates_preserve(query_path, start_date, end_date):
    import re
    try:
        if not os.path.exists(query_path):
            logging.error("Query file not found: %s", query_path)
            return False
        with open(query_path, 'r', encoding='utf-8') as f:
            original = f.read()
        pattern = re.compile(
            r"(BETWEEN\s+TO_DATE\(')"
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
            r"('\s*,\s*'YYYY-MM-DD HH24:MI:SS'\)\s+and\s+TO_DATE\(')"
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
            r"('\s*,\s*'YYYY-MM-DD HH24:MI:SS'\))",
            re.IGNORECASE | re.MULTILINE
        )
        def repl(m):
            return f"{m.group(1)}{start_date}{m.group(2)}{end_date}{m.group(3)}"
        new_query, count = pattern.subn(repl, original, count=1)
        if count == 0:
            logging.error("Could not locate BETWEEN date range to replace in %s", query_path)
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

    # Multi-country support via countries.json
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

        # Run workflow for each country in list
        for c in countries:
            name = c.get("name")
            cfg_path = c.get("config_file") or CONFIG_FILE
            qry_path = c.get("query_file") or QUERY_FILE
            out_csv = f"data_{name.lower()}.csv"  # Output file per country

            if not name or not cfg_path:
                logging.error("Each country must have 'name' and 'config_file'")
                continue
            if not args.test and not os.path.exists(cfg_path):
                logging.error(f"Config file for {name} not found: {cfg_path}")
                continue
            if not args.test and not os.path.exists(qry_path):
                logging.error(f"Query file for {name} not found: {qry_path}")
                continue

            # Optionally update query date range for each country
            if date_range:
                sd, ed, _ = date_range
                tmp_qry = os.path.join(tempfile.gettempdir(), f"query_{name}.sql")
                with open(qry_path, 'r', encoding='utf-8') as src, open(tmp_qry, 'w', encoding='utf-8') as dst:
                    dst.write(src.read())
                if not update_query_dates_preserve(tmp_qry, sd, ed):
                    logging.error(f"Failed to update date range for {name}")
                    continue
                qry_path_use = tmp_qry
            else:
                qry_path_use = qry_path

            logging.info(f"Running workflow for country: {name}")
            run_workflow(cfg_path, qry_path_use, out_csv, EXECUTION_TIMESTAMP, EXECUTION_USER,
                         test_mode=args.test, publish_test=not args.no_publish)

        return 0

    # Single-country workflow (default)
    run_workflow(
        CONFIG_FILE,
        QUERY_FILE,
        OUTPUT_CSV,
        EXECUTION_TIMESTAMP,
        EXECUTION_USER,
        test_mode=args.test,
        publish_test=not args.no_publish
    )
    return 0

if __name__ == "__main__":
    sys.exit(main())
