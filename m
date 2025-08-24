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

# Make project root importable
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PROJECT_ROOT)

from src.workflow import run_workflow, run_workflow_multi  # noqa: E402
from lib.secure_config import SecureConfig  # noqa: E402

# ---------------------------------------------------------------------------
# Constants / Configuration Paths
# ---------------------------------------------------------------------------

# Database / single-country INI (used ONLY for DB connection parameters)
CONFIG_FILE = 'config/config.ini'

# Authoritative Confluence configuration (page title lives here)
TITLE_CONFIG_FILE = 'config.json'

# Base SQL and default output
QUERY_FILE = 'config/query.sql'
OUTPUT_CSV = 'data.csv'

# Fixed execution metadata (kept as in your previous versions)
EXECUTION_TIMESTAMP = datetime.strptime('2025-07-07 00:13:56', '%Y-%m-%d %H:%M:%S')
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
    if os.path.exists("config.json"):
        try:
            with open("config.json", 'r', encoding='utf-8') as f:
                cfg = json.load(f)
            if 'PASSWORD_ENCRYPTED' in cfg and os.path.exists(SecureConfig.KEY_FILE):
                return True
        except Exception:
            pass
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
        if not os.path.exists(config_path):
            logging.error(f"Config file not found: {config_path}")
            return False

        # JSON path (primary)
        if config_path.endswith('.json'):
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except Exception as e:
                logging.error(f"Failed reading JSON config {config_path}: {e}")
                return False

            if 'PAGE_TITLE' not in data:
                logging.error(f"PAGE_TITLE key not found in JSON {config_path}")
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
                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4)
            except Exception as e:
                logging.error(f"Failed writing JSON config {config_path}: {e}")
                return False

            logging.info(f"Successfully updated Confluence PAGE_TITLE in JSON to: {new_title}")
            return True

        # INI fallback (not typically used now)
        cp = configparser.ConfigParser()
        cp.optionxform = str
        with open(config_path, 'r', encoding='utf-8') as f:
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
            with open(config_path, 'w', encoding='utf-8') as f:
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

        logging.error(f"Could not find PAGE_TITLE in {config_path}")
        return False

    except Exception as e:
        logging.error(f"Error updating Confluence page title: {str(e)}")
        return False


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

            if date_range:
                sd, ed, _ = date_range
                tmp_qry = os.path.join(tempfile.gettempdir(), f"query_{name}.sql")
                # Copy original base query EXACTLY
                with open(qry_path, 'r', encoding='utf-8') as src, open(tmp_qry, 'w', encoding='utf-8') as dst:
                    dst.write(src.read())
                if not update_query_dates_preserve(tmp_qry, sd, ed):
                    logging.error(f"Failed to update date range for {name}")
                    return 1
                prepared.append({"name": name, "config_file": cfg_path, "query_file": tmp_qry})
            else:
                prepared.append({"name": name, "config_file": cfg_path, "query_file": qry_path})

        # Update title ONCE (global JSON) not per country
        if args.monthly and month_name:
            update_confluence_page_title(TITLE_CONFIG_FILE, month_name, is_daily=False)
        elif args.daily:
            update_confluence_page_title(TITLE_CONFIG_FILE, "", is_daily=True)

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


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sys.exit(main())
