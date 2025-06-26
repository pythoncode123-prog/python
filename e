import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import json
import urllib3
import os
import csv
from datetime import datetime
import logging
from typing import Tuple, Optional, Dict, List
import base64

# Disable insecure connection warnings for Confluence API
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def load_config(config_path: str = "config.json") -> Dict:
    """Load Confluence configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        required_keys = ['CONFLUENCE_URL', 'USERNAME', 'API_TOKEN', 'SPACE_KEY', 'PAGE_TITLE', 'CSV_FILE']
        if not all(key in config for key in required_keys):
            logging.error(f"Missing required configuration keys. Required: {required_keys}")
            return None
            
        # Check if BASELINE exists, if not add default value
        if 'BASELINE' not in config:
            config['BASELINE'] = 1899206
            
        return config
    except Exception as e:
        logging.error(f"Error loading Confluence config: {str(e)}")
        return None

def create_session(config: Dict) -> requests.Session:
    """Create a configured requests session for Confluence API."""
    session = requests.Session()
    
    # Try different authentication methods
    auth_type = config.get('AUTH_TYPE', 'basic')
    
    if auth_type == 'basic':
        # Basic authentication with username and token/password
        session.auth = HTTPBasicAuth(config['USERNAME'], config['API_TOKEN'])
    elif auth_type == 'jwt':
        # JWT token authentication
        session.headers.update({
            "Authorization": f"Bearer {config['API_TOKEN']}"
        })
    elif auth_type == 'cookie':
        # Cookie-based authentication (for some older Confluence instances)
        session.headers.update({
            "Cookie": f"JSESSIONID={config['API_TOKEN']}"
        })
    elif auth_type == 'header':
        # Custom header authentication
        session.headers.update({
            "X-Auth-Token": config['API_TOKEN']
        })
    
    # Common headers
    session.headers.update({
        "Content-Type": "application/json",
        "X-Atlassian-Token": "no-check"
    })
    
    # Handle proxy settings if provided
    if 'PROXY' in config:
        session.proxies = {
            "http": config['PROXY'],
            "https": config['PROXY']
        }
    
    session.verify = False
    return session

def load_csv_data(csv_file: str) -> Tuple[pd.DataFrame, bool]:
    """Load CSV file into a pandas DataFrame."""
    try:
        df = pd.read_csv(csv_file)
        
        # Convert DATE column to datetime if it exists
        if 'DATE' in df.columns:
            df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
        
        # Convert TOTAL_JOBS to numeric if it exists
        if 'TOTAL_JOBS' in df.columns:
            df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
            
        return df, True
    except Exception as e:
        logging.error(f"Error loading CSV data: {str(e)}")
        return pd.DataFrame(), False

def generate_table_and_chart(df: pd.DataFrame, baseline: int = 1899206) -> str:
    """Generate the main table and chart for the Confluence page."""
    try:
        # Ensure TOTAL_JOBS is numeric
        df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        
        # Create a copy of the dataframe to avoid modifying the original
        chart_df = df.copy()
        chart_df = chart_df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum()
        
        # Get top 4 dates by TOTAL_JOBS
        try:
            chart_df = chart_df.nlargest(4, 'TOTAL_JOBS')
        except Exception as e:
            logging.warning(f"Could not use nlargest: {e}")
            chart_df = chart_df.sort_values('TOTAL_JOBS', ascending=False).head(4)
        
        chart_df['Base Line'] = baseline
        
        # Generate table rows
        table_rows = ["<tr><th>DATE</th><th>Baseline</th><th>Total Jobs</th></tr>"]
        for _, row in chart_df.iterrows():
            table_rows.append(
                f"<tr><td>{row['DATE'].strftime('%Y-%m-%d')}</td><td>{row['Base Line']}</td><td>{int(row['TOTAL_JOBS'])}</td></tr>"
            )
        
        return f"""
<ac:structured-macro ac:name="table-chart">
    <ac:parameter ac:name="type">column</ac:parameter>
    <ac:parameter ac:name="is3d">true</ac:parameter>
    <ac:parameter ac:name="title">Daily Job Volume Trend</ac:parameter>
    <ac:parameter ac:name="legend">false</ac:parameter>
    <ac:parameter ac:name="dataorientation">vertical</ac:parameter>
    <ac:parameter ac:name="columns">Baseline, Total Jobs</ac:parameter>
    <ac:rich-text-body>
        <table class="wrapped">
            <colgroup><col /><col /></colgroup>
            <tbody>{''.join(table_rows)}</tbody>
        </table>
    </ac:rich-text-body>
</ac:structured-macro>
"""
    except Exception as e:
        logging.error(f"Error generating table and chart: {str(e)}")
        return f"<p>Error generating table and chart: {str(e)}</p>"

def generate_region_chart(df: pd.DataFrame) -> str:
    """Generate the region chart for the Confluence page."""
    try:
        if not {'DATE', 'REGION', 'TOTAL_JOBS'}.issubset(df.columns):
            return "<p>Missing required columns for region chart: DATE, REGION, TOTAL_JOBS</p>"

        # Ensure TOTAL_JOBS is numeric
        df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        
        # Group by date and region
        df_region = df.groupby(['DATE', 'REGION'], as_index=False)['TOTAL_JOBS'].sum()
        pivot_df = df_region.pivot_table(index='DATE', columns='REGION', values='TOTAL_JOBS', aggfunc='sum').fillna(0)
        
        # Generate table rows
        table_rows = [f"<tr><th>DATE</th>" + "".join([f"<th>{col}</th>" for col in pivot_df.columns]) + "</tr>"]
        for date, row in pivot_df.iterrows():
            cells = [f"<td>{date.strftime('%Y-%m-%d')}</td>"] + [f"<td>{int(row[col])}</td>" for col in pivot_df.columns]
            table_rows.append(f"<tr>{''.join(cells)}</tr>")
        
        return f"""
<ac:structured-macro ac:name="table-chart">
    <ac:parameter ac:name="type">column</ac:parameter>
    <ac:parameter ac:name="is3d">true</ac:parameter>
    <ac:parameter ac:name="title">Total Jobs by Region per Date</ac:parameter>
    <ac:parameter ac:name="legend">true</ac:parameter>
    <ac:parameter ac:name="dataorientation">vertical</ac:parameter>
    <ac:rich-text-body>
        <table class="wrapped">
            <colgroup>{''.join([f"<col />" for _ in pivot_df.columns])}</colgroup>
            <tbody>{''.join(table_rows)}</tbody>
        </table>
    </ac:rich-text-body>
</ac:structured-macro>
"""
    except Exception as e:
        logging.error(f"Error generating region chart: {str(e)}")
        return f"<p>Error generating region chart: {str(e)}</p>"

def generate_daily_summary_table(df: pd.DataFrame) -> str:
    """Generate the daily summary table for the Confluence page."""
    try:
        # Ensure TOTAL_JOBS is numeric
        df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        
        df_summary = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum()
        df_summary['DATE'] = df_summary['DATE'].dt.strftime('%Y-%m-%d')
        
        rows = ["<tr><th>Date</th><th>Total Jobs</th></tr>"]
        for _, row in df_summary.iterrows():
            rows.append(f"<tr><td>{row['DATE']}</td><td>{int(row['TOTAL_JOBS'])}</td></tr>")
        
        return f"""
<h2>Total Jobs Per Day</h2><table class="wrapped"><tbody>{''.join(rows)}</tbody></table>
"""
    except Exception as e:
        logging.error(f"Error generating daily summary table: {str(e)}")
        return f"<p>Error generating daily summary table: {str(e)}</p>"

def generate_daily_trend_chart(df: pd.DataFrame) -> str:
    """Generate the daily trend chart for the Confluence page."""
    try:
        # Ensure TOTAL_JOBS is numeric
        df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        
        df_summary = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum()
        df_summary['DATE'] = df_summary['DATE'].dt.strftime('%Y-%m-%d')
        
        rows = ["<tr><th>Date</th><th>Peaks</th></tr>"]
        for _, row in df_summary.iterrows():
            rows.append(f"<tr><td>{row['DATE']}</td><td>{int(row['TOTAL_JOBS'])}</td></tr>")
        
        return f"""
<ac:structured-macro ac:name="table-chart">
    <ac:parameter ac:name="type">timeline</ac:parameter>
    <ac:parameter ac:name="is3d">false</ac:parameter>
    <ac:parameter ac:name="title">Daily Job Volume Trend</ac:parameter>
    <ac:parameter ac:name="legend">false</ac:parameter>
    <ac:parameter ac:name="dataorientation">vertical</ac:parameter>
    <ac:parameter ac:name="columns">Peaks</ac:parameter>
    <ac:rich-text-body>
        <table class="wrapped">
            <colgroup><col /><col /></colgroup>
            <tbody>{''.join(rows)}</tbody>
        </table>
    </ac:rich-text-body>
</ac:structured-macro>
"""
    except Exception as e:
        logging.error(f"Error generating daily trend chart: {str(e)}")
        return f"<p>Error generating daily trend chart: {str(e)}</p>"

def generate_peaks_variation_table(df: pd.DataFrame, baseline: int = 1899206) -> str:
    """Generate the peaks variation table for the Confluence page."""
    try:
        # Ensure TOTAL_JOBS is numeric
        df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        
        max_range = 2000000 
        
        df_summary = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum()
        df_summary['DATE'] = df_summary['DATE'].dt.strftime('%Y-%m-%d')
        df_summary['Baseline'] = baseline
        df_summary['Variation'] = df_summary['Baseline'] - df_summary['TOTAL_JOBS']
        
        rows = ["<tr><th>Date</th><th>Peaks</th><th>Variation with Baseline</th><th>Baseline</th><th>Max Range</th></tr>"]
        for _, row in df_summary.iterrows():
            rows.append(f"<tr><td>{row['DATE']}</td><td>{int(row['TOTAL_JOBS'])}</td><td>{int(row['Variation'])}</td><td>{int(row['Baseline'])}</td><td>{max_range}</td></tr>")
        
        return f"""
<h2>Daily Peaks vs Baseline</h2><table class="wrapped"><tbody>{''.join(rows)}</tbody></table>
"""
    except Exception as e:
        logging.error(f"Error generating peaks variation table: {str(e)}")
        return f"<p>Error generating peaks variation table: {str(e)}</p>"

def get_page_info(session: requests.Session, config: Dict) -> Tuple[Optional[str], Optional[int]]:
    """Get the Confluence page ID and current version number."""
    try:
        base_url = config['CONFLUENCE_URL'].rstrip('/')
        
        # If URL ends with '/content', use it as is, otherwise adjust
        if not base_url.endswith('/content'):
            if base_url.endswith('/rest/api'):
                base_url = f"{base_url}/content"
            else:
                base_url = f"{base_url}/rest/api/content"
        
        # Make sure URL matches Confluence Cloud vs Server format
        search_url = base_url
        if '?' not in search_url:
            search_url += '?'
        
        params = {
            'title': config['PAGE_TITLE'],
            'spaceKey': config['SPACE_KEY'],
            'expand': 'version'
        }
        
        logging.info(f"Getting page info from URL: {search_url}")
        
        # Try using URL params
        response = session.get(search_url, params=params)
        if response.status_code != 200:
            # Try again with path format for older Confluence
            alt_url = f"{base_url}/search?cql=space={config['SPACE_KEY']} AND title=\"{config['PAGE_TITLE']}\""
            logging.info(f"First attempt failed, trying: {alt_url}")
            response = session.get(alt_url)
        
        if response.status_code != 200:
            logging.error(f"Get page info failed: {response.status_code} - {response.text}")
            return None, None
            
        data = response.json()
        
        # Handle different response formats for different Confluence versions
        if 'results' in data:
            if data.get('size', 0) > 0:  # Confluence Cloud format
                return data['results'][0]['id'], data['results'][0]['version']['number']
            else:
                return None, None
        elif isinstance(data, list) and len(data) > 0:  # Some Confluence Server versions
            return data[0]['id'], data[0]['version']['number'] 
        else:
            logging.error("Unexpected response format from Confluence API")
            logging.debug(f"Response: {data}")
            return None, None
            
    except Exception as e:
        logging.error(f"Error getting page info: {str(e)}")
        return None, None

def create_or_update_page(session: requests.Session, config: Dict, content: str, page_id: Optional[str] = None, version: Optional[int] = None) -> bool:
    """Create a new page or update an existing page in Confluence."""
    try:
        base_url = config['CONFLUENCE_URL'].rstrip('/')
        
        # If URL ends with '/content', use it as is, otherwise adjust
        if not base_url.endswith('/content'):
            if base_url.endswith('/rest/api'):
                base_url = f"{base_url}/content"
            else:
                base_url = f"{base_url}/rest/api/content"
        
        payload = {
            "type": "page",
            "title": config['PAGE_TITLE'],
            "space": {"key": config['SPACE_KEY']},
            "body": {"storage": {"value": content, "representation": "storage"}}
        }

        if page_id and version is not None:
            payload["id"] = page_id
            payload["version"] = {"number": version + 1}
            
            logging.info(f"Updating page ID {page_id} at URL: {base_url}/{page_id}")
            response = session.put(f"{base_url}/{page_id}", json=payload)
        else:
            logging.info(f"Creating new page at URL: {base_url}")
            response = session.post(base_url, json=payload)
        
        if response.status_code >= 400:
            logging.error(f"API request failed: {response.status_code} - {response.text}")
            return False
            
        logging.info(f"Page '{config['PAGE_TITLE']}' {'updated' if page_id else 'created'} successfully.")
        return True
    except Exception as e:
        logging.error(f"Error creating/updating page: {str(e)}")
        return False

def test_connection(config: Dict) -> bool:
    """Test the connection to Confluence API."""
    try:
        session = create_session(config)
        
        base_url = config['CONFLUENCE_URL'].rstrip('/')
        if not base_url.endswith('/content'):
            if base_url.endswith('/rest/api'):
                base_url = f"{base_url}/content"
            else:
                base_url = f"{base_url}/rest/api/content"
        
        # Try to get spaces (lightweight API call)
        test_url = base_url.replace('/content', '/space')
        
        logging.info(f"Testing connection to: {test_url}")
        response = session.get(test_url, params={"limit": 1})
        
        if response.status_code == 200:
            logging.info("Connection test successful")
            return True
        else:
            logging.error(f"Connection test failed: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logging.error(f"Connection test failed with exception: {str(e)}")
        return False

def publish_to_confluence(report_file='task_usage_report_by_region.csv', test_mode=False):
    """
    Publish report data to Confluence.
    
    Args:
        report_file: CSV file containing report data
        test_mode: Boolean indicating if running in test mode
    """
    if test_mode:
        logging.info("Starting Confluence publishing process in TEST MODE")
        print(f"[{datetime.now()}] TEST MODE: Publishing to test page in Confluence")
    else:
        logging.info("Starting Confluence publishing process")
    
    try:
        if not os.path.exists(report_file):
            logging.error(f"File {report_file} not found!")
            return False
        
        print(f"[{datetime.now()}] Starting Confluence publishing process...")
        print(f"[{datetime.now()}] Loading data from {report_file}")
        
        # Load configuration
        config_path = "config.json"
        if test_mode:
            # Use test configuration if available, otherwise use regular config
            test_config_path = "config_test.json"
            if os.path.exists(test_config_path):
                config_path = test_config_path
                print(f"[{datetime.now()}] Using test configuration: {test_config_path}")
            else:
                print(f"[{datetime.now()}] Test configuration not found, using regular config with test mode")
        
        # Check if config file exists
        if not os.path.exists(config_path):
            # Create a default config file for demonstration
            default_config = {
                "CONFLUENCE_URL": "https://alm-confluence.systems.uk.hsbc/confluence/rest/api/content/",
                "USERNAME": "45292857",
                "API_TOKEN": "SASU143sasu*",
                "AUTH_TYPE": "basic",  # Added auth type
                "SPACE_KEY": "DIGIBAP",
                "PAGE_TITLE": "CIReleaseNote",
                "CSV_FILE": report_file,
                "BASELINE": 1899206
            }
            
            with open(config_path, 'w') as f:
                json.dump(default_config, f, indent=4)
            
            print(f"[{datetime.now()}] Created default config file at {config_path}")
            
            if test_mode:
                print(f"[{datetime.now()}] TEST MODE: Simulating report generation without uploading")
                return True
        
        # Load the configuration
        config = load_config(config_path)
        if not config:
            if test_mode:
                print(f"[{datetime.now()}] TEST MODE: Simulating report generation without uploading")
                return True
            else:
                return False
        
        # Override CSV_FILE with the report_file parameter
        config['CSV_FILE'] = report_file
        
        # If in test mode, modify the page title
        if test_mode and not config['PAGE_TITLE'].endswith("-TEST"):
            config['PAGE_TITLE'] += "-TEST"
        
        # Test connection to Confluence before proceeding
        print(f"[{datetime.now()}] Testing connection to Confluence...")
        if not test_connection(config):
            print(f"[{datetime.now()}] ERROR: Could not connect to Confluence API")
            print(f"[{datetime.now()}] Please verify your credentials and URL")
            
            if test_mode:
                print(f"[{datetime.now()}] TEST MODE: Continuing with simulation only")
            else:
                return False
        else:
            print(f"[{datetime.now()}] Successfully connected to Confluence")
            
        # Load and process the CSV data
        df, success = load_csv_data(report_file)
        if not success:
            if test_mode:
                print(f"[{datetime.now()}] TEST MODE: Simulating with sample data")
                # Create sample data for testing
                data = {
                    'DATE': pd.date_range(start='2025-06-01', periods=5),
                    'REGION': ['NA', 'EMEA', 'APAC', 'LATAM', 'NA'],
                    'ENV': ['PROD', 'PROD', 'PROD', 'PROD', 'PROD'],
                    'TOTAL_JOBS': [1500000, 1600000, 1700000, 1800000, 1900000]
                }
                df = pd.DataFrame(data)
            else:
                return False
        
        # Ensure TOTAL_JOBS is numeric
        if 'TOTAL_JOBS' in df.columns:
            df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        
        # Display some sample data
        print(f"[{datetime.now()}] Successfully loaded data")
        print(f"[{datetime.now()}] File headers: {list(df.columns)}")
        
        print("\n=== Sample Data ===")
        for i, row in df.head().iterrows():
            print(f"Row {i+1}: {row.to_dict()}")
        
        if len(df) > 5:
            print(f"... and {len(df)-5} more rows")
            
        # Get baseline from config
        baseline = config.get('BASELINE', 1899206)
        
        # Generate the report content
        execution_timestamp = datetime.now()
        execution_user = 'satish537'
        
        content = f"""
<h1>Monthly Task Usage Report{' - TEST DATA' if test_mode else ''}</h1>
<p><strong>Last updated:</strong> {execution_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
<p><strong>Generated by:</strong> {execution_user}</p>
{generate_table_and_chart(df, baseline)}
{generate_region_chart(df)}
{generate_daily_summary_table(df)}
{generate_daily_trend_chart(df)}
{generate_peaks_variation_table(df, baseline)}
<hr />
<p><em>Note: This report shows the task usage data{' (TEST MODE)' if test_mode else ''}</em></p>
"""
        
        # Create a session for Confluence API
        session = create_session(config)
        
        # If in test mode, we can either skip the actual upload or use a test page
        if test_mode:
            print(f"[{datetime.now()}] TEST MODE: Creating test page content...")
            print(f"[{datetime.now()}] TEST MODE: Would publish to page '{config['PAGE_TITLE']}' in space '{config['SPACE_KEY']}'")
            
            # Optionally, we can actually create/update a test page
            should_upload = os.environ.get('TEST_MODE_UPLOAD', 'false').lower() in ('true', '1', 't')
            if should_upload:
                print(f"[{datetime.now()}] TEST MODE: Uploading to test page...")
                page_id, version = get_page_info(session, config)
                success = create_or_update_page(session, config, content, page_id, version)
                if success:
                    print(f"[{datetime.now()}] SUCCESS: Test data published to Confluence test page!")
                else:
                    print(f"[{datetime.now()}] TEST MODE: Simulated publishing only (no actual upload)")
            else:
                print(f"[{datetime.now()}] TEST MODE: Simulated publishing only (no actual upload)")
                
            logging.info("Test Confluence publishing completed successfully")
            return True
        else:
            # Regular mode - actually upload to Confluence
            print(f"[{datetime.now()}] Connecting to Confluence...")
            print(f"[{datetime.now()}] Creating page content...")
            print(f"[{datetime.now()}] Publishing to Confluence page '{config['PAGE_TITLE']}' in space '{config['SPACE_KEY']}'...")
            
            page_id, version = get_page_info(session, config)
            success = create_or_update_page(session, config, content, page_id, version)
            
            if success:
                print(f"[{datetime.now()}] SUCCESS: Data published to Confluence!")
                logging.info("Confluence publishing completed successfully")
                return True
            else:
                print(f"[{datetime.now()}] ERROR: Failed to publish to Confluence")
                return False
                
    except Exception as e:
        logging.error(f"Error in Confluence publishing: {str(e)}")
        print(f"Error publishing to Confluence: {str(e)}")
        return False
