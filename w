import logging
from datetime import datetime
import os
import shutil
from typing import List, Dict

from lib.db_utils import sql_to_csv
from lib.csv_processor import CSVProcessor
from lib.confluence_publisher import publish_to_confluence


def run_workflow(config_file,
                 query_file,
                 output_csv,
                 execution_timestamp,
                 execution_user,
                 test_mode: bool = False,
                 publish_test: bool = True):
    """
    Single-country workflow.
    Returns exit code: 0 on success, non-zero on error.
    """
    logging.info(f"Starting single-country workflow")
    logging.info(f"Config: {config_file}, Query: {query_file}, Output: {output_csv}")
    
    # Step 1: SQL to CSV
    if test_mode:
        test_file = "test_data.csv"
        if not os.path.exists(test_file):
            logging.error(f"Test file {test_file} not found.")
            return 2
        shutil.copy(test_file, output_csv)
        logging.info(f"Using test file: {test_file}")
    else:
        if not sql_to_csv(config_file, query_file, output_csv, execution_timestamp):
            logging.error("SQL to CSV failed.")
            return 2
    
    logging.info("SQL to CSV completed successfully.")

    # Step 2: Rename file to match expected pattern
    if not os.path.basename(output_csv).startswith("data_"):
        new_filename = f"data_default.csv"
        output_dir = os.path.dirname(output_csv) or "."
        new_path = os.path.join(output_dir, new_filename)
        
        if os.path.exists(output_csv):
            shutil.move(output_csv, new_path)
            logging.info(f"Renamed {output_csv} to {new_path}")
            output_csv = new_path
    
    # Step 3: Process CSV files
    original_dir = os.getcwd()
    csv_dir = os.path.dirname(output_csv) or "."
    os.chdir(csv_dir)
    
    try:
        processor = CSVProcessor(execution_timestamp, execution_user)
        
        # Call process_all_files with NO parameters
        ok = processor.process_all_files()
        
        if not ok:
            logging.error("CSV processing failed.")
            return 2
        
        logging.info("CSV Processing completed successfully.")
        
        # Step 4: Find the generated report file
        report_file = "task_usage_report_by_region.csv"
        if not os.path.exists(report_file):
            logging.error(f"Report file {report_file} not found")
            return 3
        
        # Copy report back to original directory if needed
        if csv_dir != original_dir:
            src = os.path.join(csv_dir, report_file)
            dst = os.path.join(original_dir, report_file)
            if os.path.exists(src):
                shutil.copy2(src, dst)
                report_file = os.path.basename(dst)
        
        # Step 5: Publish to Confluence
        logging.info("Step 5: Confluence Publishing...")
        skip_actual_upload = test_mode and not publish_test
        confluence_result = publish_to_confluence(
            report_file=report_file,
            test_mode=test_mode,
            skip_actual_upload=skip_actual_upload
        )
        
        if not confluence_result:
            logging.error("Confluence publishing failed.")
            return 3
        
        logging.info("Complete workflow executed successfully!")
        return 0
        
    finally:
        os.chdir(original_dir)


def run_workflow_multi(countries: List[Dict],
                       default_output_csv: str,
                       execution_timestamp: datetime,
                       execution_user: str,
                       test_mode: bool = False,
                       publish_test: bool = True) -> bool:
    """
    Multi-country workflow - simplified version.
    """
    logging.info("="*60)
    logging.info("Starting multi-country workflow")
    logging.info(f"Countries to process: {[c['name'] for c in countries]}")
    logging.info("="*60)
    
    original_cwd = os.getcwd()
    country_data_files = []
    
    try:
        # Step 1: Extract data for each country
        for country in countries:
            name = country["name"]
            cfg = country["config_file"]
            qry = country["query_file"]
            
            country_csv = f"data_{name.lower()}.csv"
            
            logging.info(f"\n--- Processing Country: {name} ---")
            logging.info(f"Config: {cfg}")
            logging.info(f"Query: {qry}")
            logging.info(f"Output: {country_csv}")
            
            if test_mode:
                test_file = f"test_data_{name.lower()}.csv"
                if os.path.exists(test_file):
                    shutil.copy(test_file, country_csv)
                    logging.info(f"Using test file: {test_file}")
                else:
                    # Create dummy test data
                    with open(country_csv, 'w') as f:
                        f.write("NET_DATE,TOTAL_JOBS\n")
                        f.write("2024-09-01,5000\n")
                        f.write("2024-09-02,5500\n")
                    logging.info(f"Created dummy test data for {name}")
            else:
                if not sql_to_csv(cfg, qry, country_csv, execution_timestamp):
                    logging.error(f"SQL to CSV failed for {name}")
                    continue
            
            if os.path.exists(country_csv):
                file_size = os.path.getsize(country_csv)
                with open(country_csv, 'r') as f:
                    line_count = len(f.readlines())
                logging.info(f"Created {country_csv}: {file_size} bytes, {line_count} lines")
                country_data_files.append(country_csv)
            else:
                logging.error(f"Failed to create {country_csv}")
        
        if not country_data_files:
            logging.error("No country data files were created successfully")
            return False
        
        # Step 2: Process all country CSVs together
        logging.info("\n--- Aggregating Country Data ---")
        logging.info(f"Processing files: {country_data_files}")
        logging.info(f"Current directory: {os.getcwd()}")
        logging.info(f"Files in current directory: {os.listdir('.')}")
        
        processor = CSVProcessor(execution_timestamp, execution_user)
        
        # Call process_all_files with NO parameters
        ok = processor.process_all_files()
        
        if not ok:
            logging.error("CSV aggregation failed")
            return False
        
        logging.info("CSV aggregation completed successfully")
        
        # Step 3: Verify output files exist
        report_file = "task_usage_report_by_region.csv"
        detailed_file = "task_usage_report.csv"
        
        if os.path.exists(report_file):
            with open(report_file, 'r') as f:
                lines = f.readlines()
            logging.info(f"Generated {report_file}: {len(lines)} lines")
            if len(lines) > 1:
                logging.info(f"Sample data: {lines[1].strip()}")
        else:
            logging.error(f"Report file {report_file} was not generated")
            return False
        
        # Step 4: Publish to Confluence
        if publish_test:
            logging.info("\n--- Publishing to Confluence ---")
            skip_actual_upload = test_mode and not publish_test
            
            confluence_result = publish_to_confluence(
                report_file=report_file,
                test_mode=test_mode,
                skip_actual_upload=skip_actual_upload
            )
            
            if not confluence_result:
                logging.error("Confluence publishing failed")
                return False
            
            logging.info("Confluence publishing completed successfully")
        else:
            logging.info("Skipping Confluence publishing (--no-publish flag)")
        
        logging.info("\n" + "="*60)
        logging.info("Multi-country workflow completed successfully!")
        logging.info("="*60)
        return True
        
    except Exception as e:
        logging.error(f"Error in multi-country workflow: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False
    finally:
        os.chdir(original_cwd)
