import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import json
import urllib3
import os
from datetime import datetime
import logging
from typing import Tuple, Optional, Dict, List

# -------------------------------------------------------------------------------------------------
# ORIGINAL (LEGACY) SINGLE-DATASET CONFLUENCE PUBLISHER + ENHANCEMENTS
# Added multi-country support (generate_daily_by_country_chart + publish_to_confluence_multi)
# No Python 3.8-only syntax (removed walrus operator etc.)
# -------------------------------------------------------------------------------------------------

# Disable insecure connection warnings for Confluence API (because verify=False is used)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -------------------------------------------------------------------------------------------------
# Configuration / Auth Helpers
# -------------------------------------------------------------------------------------------------

def load_config(config_path: str = "config.json") -> Optional[Dict]:
    """Load Confluence configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)

        required_keys = ['CONFLUENCE_URL', 'USERNAME', 'SPACE_KEY', 'PAGE_TITLE']
        if not all(key in config for key in required_keys):
            logging.error(f"Missing required configuration keys. Required: {required_keys}")
            return None

        # Ensure baseline default
        if 'BASELINE' not in config:
            config['BASELINE'] = 1899206

        return config
    except Exception as e:
        logging.error(f"Error loading Confluence config: {str(e)}")
        return None


def get_password_from_config(config: Dict) -> Optional[str]:
    """Get decrypted password from encrypted config value if available."""
    if 'PASSWORD_ENCRYPTED' in config:
        try:
            from lib.secure_config import SecureConfig
            return SecureConfig.decrypt_password(config['PASSWORD_ENCRYPTED'])
        except Exception as e:
            logging.error(f"Error decrypting password: {str(e)}")
            return None
    return None


def create_session(config: Dict) -> Optional[requests.Session]:
    """
    Create configured requests session for Confluence API.
    Supports: basic (password or API token), jwt (Bearer), cookie auth.
    """
    session = requests.Session()
    auth_type = config.get('AUTH_TYPE', 'basic').lower()

    if auth_type == 'basic':
        password = os.environ.get('CONFLUENCE_PASSWORD')
        if not password:
            password = get_password_from_config(config)
        if password:
            session.auth = HTTPBasicAuth(config['USERNAME'], password)
        elif 'API_TOKEN' in config:
            session.auth = HTTPBasicAuth(config['USERNAME'], config['API_TOKEN'])
        else:
            logging.error("No authentication credentials found for basic auth.")
            return None
    elif auth_type == 'jwt':
        token = os.environ.get('CONFLUENCE_TOKEN') or config.get('API_TOKEN')
        if not token:
            logging.error("No token available for JWT authentication")
            return None
        session.headers.update({"Authorization": "Bearer {}".format(token)})
    elif auth_type == 'cookie':
        cookie = os.environ.get('CONFLUENCE_COOKIE') or config.get('SESSION_COOKIE')
        if not cookie:
            logging.error("No cookie available for cookie authentication")
            return None
        session.headers.update({"Cookie": cookie})
    else:
        logging.error(f"Unsupported AUTH_TYPE: {auth_type}")
        return None

    # Common headers
    session.headers.update({
        "Content-Type": "application/json",
        "X-Atlassian-Token": "no-check"
    })

    # Optional proxy usage
    if 'PROXY' in config:
        session.proxies = {
            "http": config['PROXY'],
            "https": config['PROXY']
        }

    session.verify = False  # Intentionally disabled verification (as in legacy code)
    return session

# -------------------------------------------------------------------------------------------------
# Data Loading Helpers
# -------------------------------------------------------------------------------------------------

def load_csv_data(csv_file: str) -> Tuple[pd.DataFrame, bool]:
    """Load CSV file into a pandas DataFrame."""
    try:
        df = pd.read_csv(csv_file)

        if 'DATE' in df.columns:
            df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')

        if 'TOTAL_JOBS' in df.columns:
            df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')

        return df, True
    except Exception as e:
        logging.error(f"Error loading CSV data: {str(e)}")
        return pd.DataFrame(), False

# -------------------------------------------------------------------------------------------------
# Content Generators (Legacy + Kept)
# -------------------------------------------------------------------------------------------------

def generate_table_and_chart(df: pd.DataFrame, baseline: int = 1899206) -> str:
    """Generate the main table and chart (top 4 peaks) for the Confluence page."""
    try:
        df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        chart_df = df.copy()
        chart_df = chart_df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum()

        try:
            chart_df = chart_df.nlargest(4, 'TOTAL_JOBS')
        except Exception as e:
            logging.warning(f"Could not use nlargest: {e}")
            chart_df = chart_df.sort_values('TOTAL_JOBS', ascending=False).head(4)

        chart_df['Baseline'] = baseline
        chart_df['Variation'] = baseline - chart_df['TOTAL_JOBS']
        chart_df['FormattedDate'] = chart_df['DATE'].dt.strftime('%m-%d')
        chart_df['FullDate'] = chart_df['DATE'].dt.strftime('%m/%d/%Y')

        table_rows = ["<tr><th>Date</th><th>Baseline</th><th>Total Jobs</th><th>Variation</th></tr>"]
        for _, row in chart_df.iterrows():
            variation_style = 'style="background-color: #90EE90;"' if row['Variation'] > 0 else 'style="background-color: #FFB6C1;"'
            table_rows.append(
                f"<tr><td>{row['FullDate']}</td><td>{int(row['Baseline'])}</td>"
                f"<td>{int(row['TOTAL_JOBS'])}</td><td {variation_style}>{int(row['Variation'])}</td></tr>"
            )

        month_name = chart_df['DATE'].dt.strftime('%b').iloc[0] if not chart_df.empty else "Month"
        chart_title = f"4th Peak of {month_name}:"

        chart_table_rows = ["<tr><th>Metric</th>"]
        for _, row in chart_df.iterrows():
            chart_table_rows[0] += f"<th>{row['FormattedDate']}</th>"
        chart_table_rows[0] += "</tr>"

        baseline_row = "<tr><td>Baseline</td>"
        for _, row in chart_df.iterrows():
            baseline_row += f"<td>{int(row['Baseline'])}</td>"
        baseline_row += "</tr>"
        chart_table_rows.append(baseline_row)

        jobs_row = "<tr><td>Total Jobs</td>"
        for _, row in chart_df.iterrows():
            jobs_row += f"<td>{int(row['TOTAL_JOBS'])}</td>"
        jobs_row += "</tr>"
        chart_table_rows.append(jobs_row)

        return f"""
<ac:structured-macro ac:name="chart">
    <ac:parameter ac:name="title">{chart_title}</ac:parameter>
    <ac:parameter ac:name="type">bar</ac:parameter>
    <ac:parameter ac:name="orientation">vertical</ac:parameter>
    <ac:parameter ac:name="width">600</ac:parameter>
    <ac:parameter ac:name="height">400</ac:parameter>
    <ac:parameter ac:name="3D">true</ac:parameter>
    <ac:parameter ac:name="legend">true</ac:parameter>
    <ac:parameter ac:name="dataDisplay">true</ac:parameter>
    <ac:parameter ac:name="stacked">false</ac:parameter>
    <ac:parameter ac:name="showValues">true</ac:parameter>
    <ac:parameter ac:name="valuePosition">inside</ac:parameter>
    <ac:parameter ac:name="displayValuesOnBars">true</ac:parameter>
    <ac:parameter ac:name="color">green</ac:parameter>
    <ac:parameter ac:name="labelAngle">45</ac:parameter>
    <ac:parameter ac:name="labelSpacing">50</ac:parameter>
    <ac:parameter ac:name="xLabel">Date</ac:parameter>
    <ac:parameter ac:name="yLabel">Total Jobs</ac:parameter>
    <ac:rich-text-body>
        <table><tbody>{"".join(chart_table_rows)}</tbody></table>
    </ac:rich-text-body>
</ac:structured-macro>

<h3>Daily Peaks vs Baseline</h3>
<table class="wrapped"><tbody>{"".join(table_rows)}</tbody></table>
"""
    except Exception as e:
        logging.error(f"Error generating table and chart: {str(e)}")
        return f"<p>Error generating table and chart: {str(e)}</p>"


def generate_region_chart(df: pd.DataFrame) -> str:
    """Aggregate jobs by region (pie chart + table)."""
    try:
        needed = {'DATE', 'REGION', 'TOTAL_JOBS'}
        if not needed.issubset(df.columns):
            return "<p>Missing required columns for region chart: DATE, REGION, TOTAL_JOBS</p>"

        df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        region_summary = df.groupby('REGION', as_index=False)['TOTAL_JOBS'].sum()

        table_rows = []
        for _, row in region_summary.iterrows():
            table_rows.append(f"<tr><td>{row['REGION']}</td><td>{int(row['TOTAL_JOBS'])}</td></tr>")

        return f"""
<ac:structured-macro ac:name="chart">
    <ac:parameter ac:name="title">Jobs by Region</ac:parameter>
    <ac:parameter ac:name="type">pie</ac:parameter>
    <ac:parameter ac:name="width">400</ac:parameter>
    <ac:parameter ac:name="height">300</ac:parameter>
    <ac:parameter ac:name="dataDisplay">true</ac:parameter>
    <ac:parameter ac:name="color">green</ac:parameter>
    <ac:rich-text-body>
        <table>
            <tbody>
                <tr><th>Region</th><th>Total Jobs</th></tr>
                {chr(10).join(table_rows)}
            </tbody>
        </table>
    </ac:rich-text-body>
</ac:structured-macro>

<h3>Total Jobs by Region</h3>
<table class="wrapped"><tbody>
<tr><th>Region</th><th>Total Jobs</th></tr>
{"".join(table_rows)}
</tbody></table>
"""
    except Exception as e:
        logging.error(f"Error generating region chart: {str(e)}")
        return f"<p>Error generating region chart: {str(e)}</p>"


def generate_daily_usage_by_region_chart(df: pd.DataFrame) -> str:
    """Daily usage by region (line chart)."""
    try:
        needed = {'DATE', 'REGION', 'TOTAL_JOBS'}
        if not needed.issubset(df.columns):
            return "<p>Missing required columns for daily usage by region chart: DATE, REGION, TOTAL_JOBS</p>"

        df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        df_sorted = df.sort_values('DATE')
        regions = sorted(df_sorted['REGION'].unique())
        dates = sorted(df_sorted['DATE'].unique())

        if len(dates) > 10:
            sampled_dates = dates[::3]
        else:
            sampled_dates = dates

        formatted_dates = [pd.to_datetime(date).strftime('%m-%d') for date in sampled_dates]
        chart_table_rows = ["<tr><th>Region</th>" + "".join(f"<th>{d}</th>" for d in formatted_dates) + "</tr>"]

        for region in regions:
            row_html = f"<tr><td>{region}</td>"
            for date_val in sampled_dates:
                rd = df_sorted[(df_sorted['REGION'] == region) & (df_sorted['DATE'] == date_val)]
                val = int(rd['TOTAL_JOBS'].sum()) if not rd.empty else 0
                row_html += f"<td>{val}</td>"
            row_html += "</tr>"
            chart_table_rows.append(row_html)

        return f"""
<h3>Daily Task Usage Report by Region</h3>
<ac:structured-macro ac:name="chart">
    <ac:parameter ac:name="title">Daily Task Usage Report</ac:parameter>
    <ac:parameter ac:name="type">line</ac:parameter>
    <ac:parameter ac:name="width">1000</ac:parameter>
    <ac:parameter ac:name="height">400</ac:parameter>
    <ac:parameter ac:name="legend">true</ac:parameter>
    <ac:parameter ac:name="dataDisplay">false</ac:parameter>
    <ac:parameter ac:name="xLabel">Date</ac:parameter>
    <ac:parameter ac:name="yLabel">Total Jobs</ac:parameter>
    <ac:parameter ac:name="color">green</ac:parameter>
    <ac:parameter ac:name="showShapes">true</ac:parameter>
    <ac:parameter ac:name="opacity">80</ac:parameter>
    <ac:parameter ac:name="labelAngle">45</ac:parameter>
    <ac:parameter ac:name="labelSpacing">20</ac:parameter>
    <ac:parameter ac:name="lineStyle">solid</ac:parameter>
    <ac:parameter ac:name="fontColor">black</ac:parameter>
    <ac:parameter ac:name="fontSize">12</ac:parameter>
    <ac:parameter ac:name="labelFontSize">12</ac:parameter>
    <ac:parameter ac:name="backgroundColor">white</ac:parameter>
    <ac:rich-text-body>
        <table><tbody>{"".join(chart_table_rows)}</tbody></table>
    </ac:rich-text-body>
</ac:structured-macro>

<h4>Transposed Data Structure (for debugging):</h4>
<table class="wrapped"><tbody>
{chart_table_rows[0]}
{chart_table_rows[1] if len(chart_table_rows) > 1 else '<tr><td colspan="9">No data</td></tr>'}
</tbody></table>
"""
    except Exception as e:
        logging.error(f"Error generating daily usage by region chart: {str(e)}")
        return f"<p>Error generating daily usage by region chart: {str(e)}</p>"


def generate_baseline_variation_chart(df: pd.DataFrame, baseline: int = 1899206) -> str:
    """Variation with baseline line chart."""
    try:
        df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        df_summary = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')
        df_summary['Day'] = df_summary['DATE'].dt.strftime('%m-%d')

        header_row = "<tr><th>Date</th>" + "".join(f"<th>{d}</th>" for d in df_summary['Day']) + "</tr>"
        baseline_row = "<tr><td>Baseline</td>" + "".join(f"<td>{baseline}</td>" for _ in df_summary['Day']) + "</tr>"
        peaks_row = "<tr><td>Peaks</td>" + "".join(f"<td>{int(v)}</td>" for v in df_summary['TOTAL_JOBS']) + "</tr>"
        max_range = 2000000
        max_row = "<tr><td>Max Range</td>" + "".join(f"<td>{max_range}</td>" for _ in df_summary['Day']) + "</tr>"

        chart_rows = [header_row, baseline_row, peaks_row, max_row]

        return f"""
<h3>Variation with Baseline Data:</h3>
<ac:structured-macro ac:name="chart">
    <ac:parameter ac:name="title">Variation with Baseline Data:</ac:parameter>
    <ac:parameter ac:name="type">line</ac:parameter>
    <ac:parameter ac:name="width">1000</ac:parameter>
    <ac:parameter ac:name="height">500</ac:parameter>
    <ac:parameter ac:name="legend">true</ac:parameter>
    <ac:parameter ac:name="dataDisplay">false</ac:parameter>
    <ac:parameter ac:name="xLabel">Date</ac:parameter>
    <ac:parameter ac:name="yLabel">Total Jobs</ac:parameter>
    <ac:parameter ac:name="colors">#6B8E23,#0047AB,#DAA520</ac:parameter>
    <ac:parameter ac:name="seriesColors">#6B8E23,#0047AB,#DAA520</ac:parameter>
    <ac:parameter ac:name="showShapes">true</ac:parameter>
    <ac:parameter ac:name="thickness">2</ac:parameter>
    <ac:parameter ac:name="labelAngle">45</ac:parameter>
    <ac:rich-text-body>
        <table><tbody>{chr(10).join(chart_rows)}</tbody></table>
    </ac:rich-text-body>
</ac:structured-macro>
"""
    except Exception as e:
        logging.error(f"Error generating baseline variation chart: {str(e)}")
        return f"<p>Error generating baseline variation chart: {str(e)}</p>"


def generate_monthly_task_usage_chart(df: pd.DataFrame, baseline: int = 1899206) -> str:
    """Overall monthly task usage chart (daily aggregated)."""
    try:
        df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        df_monthly = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')
        df_monthly['Day'] = df_monthly['DATE'].dt.strftime('%m-%d')

        header_row = "<tr><th>Series</th>" + "".join(f"<th>{d}</th>" for d in df_monthly['Day']) + "</tr>"
        baseline_row = "<tr><td>Baseline</td>" + "".join(f"<td>{baseline}</td>" for _ in df_monthly['Day']) + "</tr>"
        jobs_row = "<tr><td>Sum of TOTAL_JOBS</td>" + "".join(f"<td>{int(v)}</td>" for v in df_monthly['TOTAL_JOBS']) + "</tr>"

        return f"""
<h2>Overall Monthly Task Usage Report</h2>
<ac:structured-macro ac:name="chart">
    <ac:parameter ac:name="title">Monthly Task Usage Report</ac:parameter>
    <ac:parameter ac:name="type">line</ac:parameter>
    <ac:parameter ac:name="width">1200</ac:parameter>
    <ac:parameter ac:name="height">600</ac:parameter>
    <ac:parameter ac:name="legend">true</ac:parameter>
    <ac:parameter ac:name="dataDisplay">false</ac:parameter>
    <ac:parameter ac:name="stacked">false</ac:parameter>
    <ac:parameter ac:name="opacity">70</ac:parameter>
    <ac:parameter ac:name="showShapes">false</ac:parameter>
    <ac:parameter ac:name="xLabel">Date</ac:parameter>
    <ac:parameter ac:name="yLabel">Task Count</ac:parameter>
    <ac:parameter ac:name="colors">#6B8E23,#B22222</ac:parameter>
    <ac:parameter ac:name="seriesColors">#6B8E23,#B22222</ac:parameter>
    <ac:parameter ac:name="thickness">2</ac:parameter>
    <ac:parameter ac:name="labelAngle">45</ac:parameter>
    <ac:parameter ac:name="yAxisLowerBound">0</ac:parameter>
    <ac:rich-text-body>
        <table><tbody>
            {header_row}
            {baseline_row}
            {jobs_row}
        </tbody></table>
    </ac:rich-text-body>
</ac:structured-macro>
"""
    except Exception as e:
        logging.error(f"Error generating monthly task usage chart: {str(e)}")
        return f"<p>Error generating monthly task usage chart: {str(e)}</p>"


def generate_daily_summary_table(df: pd.DataFrame) -> str:
    """Daily TOTAL_JOBS table."""
    try:
        df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        df_summary = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum()
        df_summary['FormattedDate'] = df_summary['DATE'].dt.strftime('%m/%d/%Y')

        rows = ["<tr><th>Date</th><th>Total Jobs</th></tr>"]
        for _, row in df_summary.iterrows():
            rows.append(f"<tr><td>{row['FormattedDate']}</td><td>{int(row['TOTAL_JOBS'])}</td></tr>")

        return f"""
<h3>Total Jobs Per Day</h3>
<table class="wrapped"><tbody>{"".join(rows)}</tbody></table>
"""
    except Exception as e:
        logging.error(f"Error generating daily summary table: {str(e)}")
        return f"<p>Error generating daily summary table: {str(e)}</p>"


def generate_peaks_variation_table(df: pd.DataFrame, baseline: int = 1899206) -> str:
    """Peaks variation table (entire period)."""
    try:
        df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        max_range = 2000000
        df_summary = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum()
        df_summary['FormattedDate'] = df_summary['DATE'].dt.strftime('%m/%d/%Y')
        df_summary['Baseline'] = baseline
        df_summary['Variation'] = df_summary['Baseline'] - df_summary['TOTAL_JOBS']

        rows = ["<tr><th>Date</th><th>Peaks</th><th>Variation with Baseline</th><th>Baseline</th><th>Max Range</th></tr>"]
        for _, row in df_summary.iterrows():
            rows.append(
                f"<tr><td>{row['FormattedDate']}</td><td>{int(row['TOTAL_JOBS'])}</td>"
                f"<td>{int(row['Variation'])}</td><td>{int(row['Baseline'])}</td><td>{max_range}</td></tr>"
            )

        return f"""
<h3>Daily Peaks vs Baseline</h3>
<table class="wrapped"><tbody>{"".join(rows)}</tbody></table>
"""
    except Exception as e:
        logging.error(f"Error generating peaks variation table: {str(e)}")
        return f"<p>Error generating peaks variation table: {str(e)}</p>"

# -------------------------------------------------------------------------------------------------
# NEW: Multi-Country Content Generators
# -------------------------------------------------------------------------------------------------

def generate_daily_by_country_chart(df: pd.DataFrame) -> str:
    """
    Combined line chart: one series per COUNTRY (sum TOTAL_JOBS by DATE).
    Requires columns: DATE, COUNTRY, TOTAL_JOBS
    """
    try:
        needed = {'DATE', 'COUNTRY', 'TOTAL_JOBS'}
        if not needed.issubset(df.columns):
            return "<p>Missing required columns for country chart: DATE, COUNTRY, TOTAL_JOBS</p>"

        df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        if not pd.api.types.is_datetime64_any_dtype(df['DATE']):
            df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')

        agg = df.groupby(['DATE', 'COUNTRY'], as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')
        agg['Day'] = agg['DATE'].dt.strftime('%m-%d')

        dates = agg['Day'].dropna().unique().tolist()
        countries = sorted(agg['COUNTRY'].dropna().astype(str).unique().tolist())

        header = "<tr><th>Series</th>" + "".join(f"<th>{d}</th>" for d in dates) + "</tr>"
        rows = [header]

        for country in countries:
            slice_df = agg[agg['COUNTRY'].astype(str) == country]
            row_vals = []
            for d in dates:
                val = slice_df.loc[slice_df['Day'] == d, 'TOTAL_JOBS'].sum()
                val = int(val) if pd.notna(val) else 0
                row_vals.append(f"<td>{val}</td>")
            rows.append(f"<tr><td>{country}</td>{''.join(row_vals)}</tr>")

        return f"""
<h2>Daily Total Jobs by Country</h2>
<ac:structured-macro ac:name="chart">
    <ac:parameter ac:name="title">Daily Total Jobs by Country</ac:parameter>
    <ac:parameter ac:name="type">line</ac:parameter>
    <ac:parameter ac:name="width">1200</ac:parameter>
    <ac:parameter ac:name="height">600</ac:parameter>
    <ac:parameter ac:name="legend">true</ac:parameter>
    <ac:parameter ac:name="dataDisplay">false</ac:parameter>
    <ac:parameter ac:name="xLabel">Date</ac:parameter>
    <ac:parameter ac:name="yLabel">Total Jobs</ac:parameter>
    <ac:rich-text-body>
        <table><tbody>{''.join(rows)}</tbody></table>
    </ac:rich-text-body>
</ac:structured-macro>
"""
    except Exception as e:
        logging.error(f"Error generating daily by country chart: {str(e)}")
        return f"<p>Error generating daily by country chart: {str(e)}</p>"

# -------------------------------------------------------------------------------------------------
# Confluence Page Operations
# -------------------------------------------------------------------------------------------------

def get_page_info(session: requests.Session, config: Dict) -> Tuple[Optional[str], Optional[int]]:
    """Get Confluence page ID & version number (supports slight variation in REST endpoints)."""
    try:
        base_url = config['CONFLUENCE_URL'].rstrip('/')
        if not base_url.endswith('/content'):
            if base_url.endswith('/rest/api'):
                base_url = f"{base_url}/content"
            else:
                base_url = f"{base_url}/rest/api/content"

        search_url = base_url
        if '?' not in search_url:
            search_url += '?'

        params = {
            'title': config['PAGE_TITLE'],
            'spaceKey': config['SPACE_KEY'],
            'expand': 'version'
        }
        logging.info(f"Getting page info from URL: {search_url}")
        response = session.get(search_url, params=params)
        if response.status_code != 200:
            alt_url = f"{base_url}/search?cql=space={config['SPACE_KEY']} AND title=\"{config['PAGE_TITLE']}\""
            logging.info(f"First attempt failed, trying: {alt_url}")
            response = session.get(alt_url)

        if response.status_code != 200:
            logging.error(f"Get page info failed: {response.status_code} - {response.text}")
            return None, None

        data = response.json()

        if 'results' in data:
            if data.get('size', 0) > 0:
                return data['results'][0]['id'], data['results'][0]['version']['number']
            return None, None
        elif isinstance(data, list) and len(data) > 0:
            return data[0]['id'], data[0]['version']['number']
        else:
            logging.error("Unexpected response format from Confluence API")
            logging.debug(f"Response: {data}")
            return None, None
    except Exception as e:
        logging.error(f"Error getting page info: {str(e)}")
        return None, None


def create_or_update_page(session: requests.Session,
                          config: Dict,
                          content: str,
                          page_id: Optional[str] = None,
                          version: Optional[int] = None) -> bool:
    """Create or update Confluence page."""
    try:
        base_url = config['CONFLUENCE_URL'].rstrip('/')
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
            logging.info(f"Updating page ID {page_id} at {base_url}/{page_id}")
            resp = session.put(f"{base_url}/{page_id}", json=payload)
        else:
            logging.info(f"Creating new page at {base_url}")
            resp = session.post(base_url, json=payload)

        if resp.status_code >= 400:
            logging.error(f"API request failed: {resp.status_code} - {resp.text}")
            return False

        logging.info(f"Page '{config['PAGE_TITLE']}' {'updated' if page_id else 'created'} successfully.")
        return True
    except Exception as e:
        logging.error(f"Error creating/updating page: {str(e)}")
        return False


def test_connection(session: requests.Session, config: Dict) -> bool:
    """Lightweight Confluence connection test."""
    try:
        base_url = config['CONFLUENCE_URL'].rstrip('/')
        if base_url.endswith('/content'):
            base_url = base_url[:-8]
        elif not base_url.endswith('/rest/api'):
            base_url = f"{base_url}/rest/api"

        test_url = f"{base_url}/space"
        logging.info(f"Testing connection to: {test_url}")
        resp = session.get(test_url, params={"limit": 1})
        if resp.status_code == 200:
            logging.info("Connection test successful")
            return True
        logging.error(f"Connection test failed: {resp.status_code} - {resp.text}")
        return False
    except Exception as e:
        logging.error(f"Connection test failed with exception: {str(e)}")
        return False

# -------------------------------------------------------------------------------------------------
# Single Dataset Publish
# -------------------------------------------------------------------------------------------------

def publish_to_confluence(report_file='task_usage_report_by_region.csv',
                          test_mode=False,
                          skip_actual_upload=False):
    """
    Publish single report data to Confluence.
    """
    if test_mode:
        logging.info("Starting Confluence publishing process in TEST MODE")
        print(f"[{datetime.now()}] TEST MODE: Publishing to test page in Confluence")
        if not skip_actual_upload:
            print(f"[{datetime.now()}] TEST MODE: Will attempt actual upload to test page")
    else:
        logging.info("Starting Confluence publishing process")

    try:
        if not os.path.exists(report_file):
            logging.error(f"File {report_file} not found!")
            return False

        print(f"[{datetime.now()}] Starting Confluence publishing process...")
        print(f"[{datetime.now()}] Loading data from {report_file}")

        config_path = "config.json"
        if test_mode:
            test_config_path = "config_test.json"
            if os.path.exists(test_config_path):
                config_path = test_config_path
                print(f"[{datetime.now()}] Using test configuration: {test_config_path}")
            else:
                print(f"[{datetime.now()}] Test configuration not found, using regular config with test mode")

        if not os.path.exists(config_path):
            default_config = {
                "CONFLUENCE_URL": "https://alm-confluence.systems.uk.hsbc/confluence/rest/api/content/",
                "USERNAME": "45292857",
                "AUTH_TYPE": "basic",
                "SPACE_KEY": "DIGIBAP",
                "PAGE_TITLE": "CIReleaseNo99",
                "CSV_FILE": report_file,
                "BASELINE": 1899206
            }
            with open(config_path, 'w') as f:
                json.dump(default_config, f, indent=4)
            print(f"[{datetime.now()}] Created default config file at {config_path}")
            if skip_actual_upload:
                print(f"[{datetime.now()}] Skipping actual upload (simulation only)")
                return True

        config = load_config(config_path)
        if not config:
            if skip_actual_upload:
                print(f"[{datetime.now()}] Simulating report generation without uploading")
                return True
            return False

        config['CSV_FILE'] = report_file

        if test_mode and not config['PAGE_TITLE'].endswith("-TEST"):
            config['PAGE_TITLE'] += "-TEST"

        if 'AUTH_TYPE' not in config:
            print(f"[{datetime.now()}] No AUTH_TYPE specified, defaulting to basic")
            config['AUTH_TYPE'] = 'basic'

        if skip_actual_upload:
            session = None
            print(f"[{datetime.now()}] Skipping actual API connection (simulation only)")
        else:
            session = create_session(config)
            if not session:
                print(f"[{datetime.now()}] ERROR: Failed to create session")
                if test_mode:
                    print(f"[{datetime.now()}] TEST MODE: Continuing with simulation only")
                    skip_actual_upload = True
                else:
                    return False

        if not skip_actual_upload:
            print(f"[{datetime.now()}] Testing connection to Confluence...")
            if not test_connection(session, config):
                print(f"[{datetime.now()}] ERROR: Could not connect to Confluence API")
                if test_mode:
                    print(f"[{datetime.now()}] TEST MODE: Continuing with simulation only")
                    skip_actual_upload = True
                else:
                    return False
            else:
                print(f"[{datetime.now()}] Successfully connected to Confluence")

        df, success = load_csv_data(report_file)
        if not success:
            if skip_actual_upload:
                print(f"[{datetime.now()}] Simulating with sample data")
                sample = {
                    'DATE': pd.date_range(start='2025-06-01', periods=5),
                    'REGION': ['NA', 'EMEA', 'APAC', 'LATAM', 'NA'],
                    'ENV': ['PROD'] * 5,
                    'TOTAL_JOBS': [1500000, 1600000, 1700000, 1800000, 1900000]
                }
                df = pd.DataFrame(sample)
            else:
                return False

        if 'TOTAL_JOBS' in df.columns:
            df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')

        print(f"[{datetime.now()}] Successfully loaded data")
        print(f"[{datetime.now()}] File headers: {list(df.columns)}")
        print("\n=== Sample Data ===")
        for i, row in df.head().iterrows():
            print(f"Row {i+1}: {row.to_dict()}")
        if len(df) > 5:
            print(f"... and {len(df)-5} more rows")

        baseline = config.get('BASELINE', 1899206)
        execution_timestamp = datetime.strptime('2025-07-06 23:45:29', '%Y-%m-%d %H:%M:%S')
        execution_user = 'satish537'

        content = f"""
<h1>Monthly Task Usage Report{' - TEST DATA' if test_mode else ''}</h1>
<p><strong>Last updated:</strong> {execution_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
<p><strong>Generated by:</strong> {execution_user}</p>

{generate_table_and_chart(df, baseline)}
{generate_daily_usage_by_region_chart(df)}
{generate_monthly_task_usage_chart(df, baseline)}
{generate_baseline_variation_chart(df, baseline)}
{generate_daily_summary_table(df)}
<hr />
<p><em>Note: This report shows the task usage data{' (TEST MODE)' if test_mode else ''}</em></p>
"""

        if skip_actual_upload:
            print(f"[{datetime.now()}] TEST MODE: Creating test page content...")
            print(f"[{datetime.now()}] TEST MODE: Would publish to page '{config['PAGE_TITLE']}' in space '{config['SPACE_KEY']}'")
            print(f"[{datetime.now()}] TEST MODE: Simulated publishing only (no actual upload)")
            logging.info("Test Confluence publishing simulated successfully")
            return True
        else:
            print(f"[{datetime.now()}] Creating page content...")
            page_id, version = get_page_info(session, config)
            success = create_or_update_page(session, config, content, page_id, version)
            if success:
                print(f"[{datetime.now()}] SUCCESS: Data published to Confluence!")
                logging.info("Confluence publishing completed successfully")
                return True
            print(f"[{datetime.now()}] ERROR: Failed to publish to Confluence")
            return False

    except Exception as e:
        logging.error(f"Error in Confluence publishing: {str(e)}")
        print(f"Error publishing to Confluence: {str(e)}")
        return False

# -------------------------------------------------------------------------------------------------
# Multi-Country Publish
# -------------------------------------------------------------------------------------------------

def publish_to_confluence_multi(report_files: List[Tuple[str, str]],
                                test_mode: bool = False,
                                skip_actual_upload: bool = False) -> bool:
    """
    Publish multiple country datasets to a single Confluence page.
    Args:
        report_files: list of (country_name, csv_path)
        test_mode: mark content as TEST DATA
        skip_actual_upload: simulation only
    """
    try:
        print(f"[{datetime.now()}] Starting multi-country Confluence publishing process...")
        config_path = "config.json"
        if test_mode:
            test_config_path = "config_test.json"
            if os.path.exists(test_config_path):
                config_path = test_config_path
                print(f"[{datetime.now()}] Using test configuration: {test_config_path}")

        if not os.path.exists(config_path):
            logging.error("Config file not found for multi-country publish.")
            if skip_actual_upload:
                print(f"[{datetime.now()}] Simulation only; continuing without config.")
                config = {
                    "CONFLUENCE_URL": "",
                    "USERNAME": "",
                    "SPACE_KEY": "",
                    "PAGE_TITLE": "Multi Country Task Usage (Simulated)",
                    "BASELINE": 1899206
                }
            else:
                return False
        else:
            config = load_config(config_path)
            if not config:
                return False

        if test_mode and not config['PAGE_TITLE'].endswith("-TEST"):
            config['PAGE_TITLE'] += "-TEST"

        baseline = config.get('BASELINE', 1899206)

        # Load each country's CSV
        frames = []
        per_country = []  # (name, df)
        for country, path in report_files:
            if not os.path.exists(path):
                logging.warning(f"[{datetime.now()}] Missing report for {country}: {path}")
                continue
            df = pd.read_csv(path)
            if 'DATE' in df.columns:
                df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
            if 'TOTAL_JOBS' in df.columns:
                df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
            df['COUNTRY'] = country
            frames.append(df)
            per_country.append((country, df))

        if not frames:
            logging.error("No valid country data to publish.")
            return False

        all_df = pd.concat(frames, ignore_index=True)

        execution_timestamp = datetime.strptime('2025-07-06 23:45:29', '%Y-%m-%d %H:%M:%S')
        execution_user = 'satish537'

        sections = []
        sections.append(generate_daily_by_country_chart(all_df))
        sections.append("<hr /><h2>Per-Country Sections</h2>")

        # For each country add same set of charts/tables as single mode (order adjustable)
        for country, df_country in per_country:
            sections.append(f"<hr /><h1>{country}</h1>")
            try:
                sections.append(generate_monthly_task_usage_chart(df_country, baseline))
                sections.append(generate_baseline_variation_chart(df_country, baseline))
                sections.append(generate_daily_usage_by_region_chart(df_country))
                sections.append(generate_daily_summary_table(df_country))
            except Exception as e:
                logging.error(f"Error generating section for {country}: {e}")
                sections.append(f"<p>Error building charts for {country}: {e}</p>")

        content = f"""
<h1>Multi-Country Task Usage Report{' - TEST DATA' if test_mode else ''}</h1>
<p><strong>Last updated:</strong> {execution_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
<p><strong>Generated by:</strong> {execution_user}</p>
{''.join(sections)}
<hr />
<p><em>Note: This report shows the task usage data{' (TEST MODE)' if test_mode else ''}</em></p>
"""

        # Simulation path
        if skip_actual_upload:
            print(f"[{datetime.now()}] TEST MODE: Simulated combined publish to '{config['PAGE_TITLE']}'")
            logging.info("Simulated multi-country publish OK")
            return True

        # Real upload
        session = create_session(config)
        if not session:
            print(f"[{datetime.now()}] ERROR: Failed to create session for multi-country publish")
            return False

        print(f"[{datetime.now()}] Testing connection to Confluence...")
        if not test_connection(session, config):
            print(f"[{datetime.now()}] ERROR: Unable to connect to Confluence")
            return False

        print(f"[{datetime.now()}] Uploading combined multi-country page...")
        page_id, version = get_page_info(session, config)
        ok = create_or_update_page(session, config, content, page_id, version)
        if ok:
            print(f"[{datetime.now()}] SUCCESS: Multi-country data published!")
            return True
        print(f"[{datetime.now()}] ERROR: Failed publishing multi-country data")
        return False

    except Exception as e:
        logging.error(f"Error in multi-country publishing: {str(e)}")
        print(f"Error in multi-country publishing: {str(e)}")
        return False

# -------------------------------------------------------------------------------------------------
# If needed, add a __main__ harness for ad hoc testing
# -------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    parser = argparse.ArgumentParser(description="Confluence Publisher (Single / Multi)")
    parser.add_argument("--file", help="Single report CSV to publish")
    parser.add_argument("--multi", nargs="*", help="List of COUNTRY=path/to/file.csv for multi-country publish")
    parser.add_argument("--simulate", action="store_true", help="Skip actual upload (simulation)")
    parser.add_argument("--test", action="store_true", help="Test mode (marks page title)")
    args = parser.parse_args()

    if args.file:
        ok = publish_to_confluence(
            report_file=args.file,
            test_mode=args.test,
            skip_actual_upload=args.simulate
        )
        print("Single publish:", "OK" if ok else "FAILED")
    elif args.multi:
        pairs = []
        for item in args.multi:
            if "=" not in item:
                print(f"Ignoring invalid multi argument (expected COUNTRY=path): {item}")
                continue
            country, path = item.split("=", 1)
            pairs.append((country.strip(), path.strip()))
        ok = publish_to_confluence_multi(
            report_files=pairs,
            test_mode=args.test,
            skip_actual_upload=args.simulate
        )
        print("Multi publish:", "OK" if ok else "FAILED")
    else:
        parser.print_help()
