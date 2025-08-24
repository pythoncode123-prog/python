import logging
from datetime import datetime
import os
import shutil
from typing import List, Dict, Tuple

from lib.db_utils import sql_to_csv
from lib.csv_processor import CSVProcessor
from lib.confluence_publisher import publish_to_confluence

# Try to import multi-country publisher (added alongside single publisher)
try:
    from lib.confluence_publisher import publish_to_confluence_multi
except Exception:
    publish_to_confluence_multi = None


def run_sql_and_process(config_file: str,
                        query_file: str,
                        output_csv: str,
                        execution_timestamp: datetime,
                        execution_user: str,
                        test_mode: bool = False,
                        output_prefix: str = "") -> Tuple[bool, str]:
    """
    Run SQL->CSV (or copy test CSV) and process outputs for a single dataset.
    Returns (success, report_file_path).
    """
    # Step 1: SQL to CSV (or test copy)
    if test_mode:
        test_file = "test_task_usage_report_by_region.csv"
        if not os.path.exists(test_file):
            logging.error(f"Test file {test_file} not found.")
            return False, ""
        shutil.copy(test_file, output_csv)
        logging.info(f"Using test file: {test_file}")
    else:
        if not sql_to_csv(config_file, query_file, output_csv):
            logging.error("SQL to CSV failed.")
            return False, ""
    logging.info("SQL to CSV completed successfully.")

    # Step 2: CSV Processing
    processor = CSVProcessor(execution_timestamp, execution_user)
    eff_prefix = ("test_" if test_mode else "") + output_prefix
    ok = processor.process_all_files(output_csv, output_prefix=eff_prefix)
    if not ok:
        logging.error("CSV processing failed.")
        return False, ""
    logging.info("CSV Processing completed successfully.")

    report_file = f"{eff_prefix}task_usage_report_by_region.csv"
    return True, report_file


def run_workflow(config_file,
                 query_file,
                 output_csv,
                 execution_timestamp,
                 execution_user,
                 test_mode: bool = False,
                 publish_test: bool = True):
    """
    Existing single-country workflow (kept compatible).
    Returns exit code: 0 on success, non-zero on error.
    """
    # Step 1 + 2
    success, report_file = run_sql_and_process(
        config_file=config_file,
        query_file=query_file,
        output_csv=output_csv,
        execution_timestamp=execution_timestamp,
        execution_user=execution_user,
        test_mode=test_mode,
        output_prefix=""
    )
    if not success:
        print("ERROR: Workflow failed before publishing.")
        return 2

    # Step 3: Confluence Publishing
    print("\nStep 3: Confluence Publishing...")
    # In test mode, if publish_test is False, skip actual upload (simulate only)
    skip_actual_upload = test_mode and not publish_test
    confluence_result = publish_to_confluence(
        report_file=report_file,
        test_mode=test_mode,
        skip_actual_upload=skip_actual_upload
    )
    if not confluence_result:
        print("ERROR: Confluence publishing failed. Check log for details.")
        return 3

    print("\nComplete workflow executed successfully!")
    return 0


def run_workflow_multi(countries: List[Dict],
                       default_output_csv: str,
                       execution_timestamp: datetime,
                       execution_user: str,
                       test_mode: bool = False,
                       publish_test: bool = True) -> bool:
    """
    Multi-country workflow:
    - For each country: extract, process, and gather report file.
    - Publish one combined Confluence page with all countries.

    Args:
        countries: list of dicts with keys {name, config_file, query_file}
        default_output_csv: base name used to write dataset CSVs (per-country suffix will be added)
        execution_timestamp: timestamp to include in processed outputs
        execution_user: user to include in processed outputs
        test_mode: if True, use test CSV instead of running real SQL
        publish_test: if True in test mode, allow actual upload; if False, simulate upload in test mode

    Returns:
        True on success, False otherwise.
    """
    report_files: List[Tuple[str, str]] = []

    # Step 1 + 2 for each country
    for c in countries:
        name = c["name"]
        cfg = c["config_file"]
        qry = c["query_file"]

        out_csv = f"data_{name}.csv"
        prefix = f"{name.lower()}_"

        print(f"\n=== Country: {name} ===")
        success, report = run_sql_and_process(
            config_file=cfg,
            query_file=qry,
            output_csv=out_csv,
            execution_timestamp=execution_timestamp,
            execution_user=execution_user,
            test_mode=test_mode,
            output_prefix=prefix
        )
        if not success:
            print(f"ERROR: Workflow failed for {name}")
            return False
        report_files.append((name, report))

    # Step 3: Combined Confluence Publishing
    print("\nStep 3: Confluence Publishing (combined)...")

    if publish_to_confluence_multi is None:
        print("ERROR: Multi-country publisher not available. Please update lib/confluence_publisher.py")
        return False

    # In test mode, if publish_test is False, skip actual upload (simulate only)
    skip_actual_upload = test_mode and not publish_test

    ok = publish_to_confluence_multi(
        report_files=report_files,
        test_mode=test_mode,
        skip_actual_upload=skip_actual_upload
    )
    if not ok:
        print("ERROR: Confluence publishing failed.")
        return False

    print("\nMulti-country workflow executed successfully!")
    return True
