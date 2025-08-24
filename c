#!/usr/bin/env python3
"""
Confluence Publisher

Builds HTML content (Confluence storage format) from processed CSV reports and publishes
to a Confluence page via the Confluence REST API.

This module supports:
- Single-dataset publishing (backward compatible with existing workflow)
- Multi-country publishing: combines per-country datasets into one page with a cross-country chart
  and per-country sections.

Expected input CSV for single dataset:
- Columns (typical): DATE, REGION, ENV, TOTAL_JOBS
- DATE should be parseable to a date/datetime; TOTAL_JOBS numeric.

For multi-country:
- Each per-country CSV should be in the same output schema; the publisher will add a COUNTRY column
  when combining for the cross-country chart.

Note:
- Some timestamps and usernames are printed in the content. By design (matching the existing repo), these
  values are currently set to fixed values in this module. If desired, refactor to accept these from callers.

"""
from __future__ import annotations

import os
import json
import logging
from typing import List, Tuple, Dict, Optional, Any
from datetime import datetime

import pandas as pd
import requests

# --------------------------------------------------------------------------------------
# Configuration helpers
# --------------------------------------------------------------------------------------

def load_config_json(path: str = "config.json") -> Dict[str, Any]:
    """
    Load the JSON configuration used for Confluence credentials and page metadata.
    The following keys are relevant:
      - CONFLUENCE_URL: e.g., https://<host>/confluence/rest/api/content/
      - USERNAME: Confluence username (or account ID) for basic auth
      - AUTH_TYPE: 'basic' (default)
      - SPACE_KEY: Confluence space key
      - PAGE_TITLE: Title of the target page
      - PASSWORD_ENCRYPTED: Encrypted password, decryptable via lib.secure_config.SecureConfig
      - BASELINE: integer baseline value (defaults to 1899206)

    If the file doesn't exist, a minimal default dict is returned.
    """
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                cfg = json.load(f)
            return cfg
        except Exception as e:
            logging.error(f"Error loading {path}: {e}")
    # Fallback defaults (minimal)
    return {
        "CONFLUENCE_URL": "",
        "USERNAME": "",
        "AUTH_TYPE": "basic",
        "SPACE_KEY": "",
        "PAGE_TITLE": "Task Usage",
        "BASELINE": 1899206,
    }


# --------------------------------------------------------------------------------------
# Confluence REST helpers
# --------------------------------------------------------------------------------------

def _ensure_api_base_url(base: str) -> str:
    """
    Given a Confluence content REST base, ensure it ends with a trailing slash.
    Expected to be .../rest/api/content/
    """
    if not base:
        return ""
    if not base.endswith("/"):
        base += "/"
    return base


def _confluence_auth_session(config: Dict[str, Any]) -> requests.Session:
    """
    Create an authenticated requests session for Confluence using basic auth.

    Password resolution order:
    1. CONFLUENCE_PASSWORD environment variable
    2. config['PASSWORD_ENCRYPTED'] decrypted via lib.secure_config.SecureConfig

    Returns:
        Authenticated requests.Session

    Raises:
        RuntimeError if password is not available.
    """
    session = requests.Session()
    auth_type = (config.get("AUTH_TYPE") or "basic").lower()
    if auth_type == "basic":
        password = os.environ.get("CONFLUENCE_PASSWORD")
        if not password and "PASSWORD_ENCRYPTED" in config:
            try:
                from lib.secure_config import SecureConfig
                password = SecureConfig.decrypt_password(config["PASSWORD_ENCRYPTED"])
            except Exception as e:
                logging.error(f"Failed decrypting PASSWORD_ENCRYPTED: {e}")
                password = None
        if not password:
            raise RuntimeError("No Confluence password available (env or encrypted).")
        session.auth = (config.get("USERNAME", "") or "", password)
    else:
        # Extend for PAT/bearer if you support it
        raise NotImplementedError("Only basic auth is implemented.")
    return session


def get_page_info(session: requests.Session, config: Dict[str, Any]) -> Tuple[Optional[str], Optional[int]]:
    """
    Get the Confluence page ID and current version number for the configured title/space.

    Returns:
        (page_id, version_number) or (None, None) if not found.
    """
    try:
        base_url = _ensure_api_base_url(config.get("CONFLUENCE_URL") or "")
        if not base_url:
            logging.error("CONFLUENCE_URL is not configured.")
            return None, None

        title = config.get("PAGE_TITLE", "")
        space_key = config.get("SPACE_KEY", "")

        # Confluence REST: GET content by title and space
        # e.g. GET /rest/api/content?title=MyTitle&spaceKey=KEY&expand=version.number
        url = f"{base_url}?title={requests.utils.quote(title)}&spaceKey={requests.utils.quote(space_key)}&expand=version.number"
        resp = session.get(url, headers={"Accept": "application/json"})
        if resp.status_code != 200:
            logging.error(f"Failed fetching page info: {resp.status_code} {resp.text}")
            return None, None

        data = resp.json()
        results = data.get("results", [])
        if not results:
            # Not found
            return None, None

        page = results[0]
        page_id = page.get("id")
        version = page.get("version", {}).get("number")
        return page_id, version
    except Exception as e:
        logging.error(f"Error in get_page_info: {e}")
        return None, None


def create_or_update_page(session: requests.Session,
                          config: Dict[str, Any],
                          html_content: str,
                          page_id: Optional[str] = None,
                          version: Optional[int] = None) -> bool:
    """
    Create or update a Confluence page using 'storage' representation.

    - If page_id is provided, updates the page with version+1.
    - Otherwise, creates a new page.

    Returns:
        True on success, False otherwise.
    """
    base_url = _ensure_api_base_url(config.get("CONFLUENCE_URL") or "")
    if not base_url:
        logging.error("CONFLUENCE_URL is not configured.")
        return False

    headers = {"Content-Type": "application/json"}
    title = config.get("PAGE_TITLE", "Task Usage")
    space_key = config.get("SPACE_KEY", "")

    try:
        if page_id:
            # Update existing page
            url = f"{base_url}{page_id}"
            payload = {
                "id": page_id,
                "type": "page",
                "title": title,
                "space": {"key": space_key},
                "body": {
                    "storage": {
                        "value": html_content,
                        "representation": "storage",
                    }
                },
                "version": {"number": int(version or 1) + 1},
            }
            resp = session.put(url, headers=headers, data=json.dumps(payload))
            if resp.status_code not in (200, 202):
                logging.error(f"Failed to update page {page_id}: {resp.status_code} {resp.text}")
                return False
            return True
        else:
            # Create new page
            url = base_url
            payload = {
                "type": "page",
                "title": title,
                "space": {"key": space_key},
                "body": {
                    "storage": {
                        "value": html_content,
                        "representation": "storage",
                    }
                },
            }
            resp = session.post(url, headers=headers, data=json.dumps(payload))
            if resp.status_code not in (200, 201):
                logging.error(f"Failed to create page: {resp.status_code} {resp.text}")
                return False
            return True
    except Exception as e:
        logging.error(f"Error creating/updating Confluence page: {e}")
        return False


# --------------------------------------------------------------------------------------
# Content generation helpers (Confluence storage-format HTML)
# --------------------------------------------------------------------------------------

def _ensure_date_col(df: pd.DataFrame, col: str = "DATE") -> pd.DataFrame:
    """
    Ensure DATE column is datetime.
    """
    if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
        try:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        except Exception:
            pass
    return df


def _ensure_numeric(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Ensure a numeric column (coerce errors).
    """
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def generate_table_and_chart(df: pd.DataFrame, baseline: int = 1899206) -> str:
    """
    Generate a combined section with:
    - A line chart of total jobs per day (with transposed structure for Confluence macro)
    - A table showing daily peaks vs baseline

    Requires columns: DATE, TOTAL_JOBS
    """
    try:
        if not {'DATE', 'TOTAL_JOBS'}.issubset(df.columns):
            return "<p>Missing required columns for table and chart: DATE, TOTAL_JOBS</p>"

        df = _ensure_date_col(df, "DATE")
        df = _ensure_numeric(df, "TOTAL_JOBS")

        # Aggregate by date and sort
        df_summary = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')

        # For chart: transposed structure
        # Header row: Series name + each date (MM-DD)
        chart_header = "<tr><th>Series</th>"
        day_labels = df_summary['DATE'].dt.strftime('%m-%d').tolist()
        chart_header += "".join(f"<th>{d}</th>" for d in day_labels)
        chart_header += "</tr>"

        # Series rows
        # Baseline series
        baseline_row = "<tr><td>Baseline</td>" + "".join(f"<td>{baseline}</td>" for _ in day_labels) + "</tr>"

        # Total Jobs series
        jobs_row = "<tr><td>Sum of TOTAL_JOBS</td>" + "".join(f"<td>{int(v)}</td>" for v in df_summary['TOTAL_JOBS']) + "</tr>"

        # For the "Daily Peaks vs Baseline" table
        table_rows = ["<tr><th>Date</th><th>Peaks</th><th>Variation with Baseline</th><th>Baseline</th><th>Max Range</th></tr>"]
        max_range = 2000000
        df_tab = df_summary.copy()
        df_tab["FormattedDate"] = df_tab["DATE"].dt.strftime("%m/%d/%Y")
        df_tab["Baseline"] = baseline
        df_tab["Variation"] = df_tab["Baseline"] - df_tab["TOTAL_JOBS"]
        for _, row in df_tab.iterrows():
            table_rows.append(
                f"<tr><td>{row['FormattedDate']}</td><td>{int(row['TOTAL_JOBS'])}</td><td>{int(row['Variation'])}</td>"
                f"<td>{int(row['Baseline'])}</td><td>{max_range}</td></tr>"
            )

        return f"""
<ac:structured-macro ac:name="chart">
    <ac:parameter ac:name="title">Daily Peaks vs Baseline</ac:parameter>
    <ac:parameter ac:name="type">line</ac:parameter>
    <ac:parameter ac:name="width">1000</ac:parameter>
    <ac:parameter ac:name="height">450</ac:parameter>
    <ac:parameter ac:name="legend">true</ac:parameter>
    <ac:parameter ac:name="dataDisplay">false</ac:parameter>
    <ac:parameter ac:name="xLabel">Date</ac:parameter>
    <ac:parameter ac:name="yLabel">Total Jobs</ac:parameter>
    <ac:rich-text-body>
        <table>
            <tbody>
                {chart_header}
                {baseline_row}
                {jobs_row}
            </tbody>
        </table>
    </ac:rich-text-body>
</ac:structured-macro>

<h3>Daily Peaks vs Baseline</h3>
<table class="wrapped">
    <tbody>
        {"".join(table_rows)}
    </tbody>
</table>
"""
    except Exception as e:
        logging.error(f"Error generating table and chart: {str(e)}")
        return f"<p>Error generating table and chart: {str(e)}</p>"


def generate_region_pie_and_table(df: pd.DataFrame) -> str:
    """
    Retained for backward compatibility if needed:
    Generates a pie chart and table for total jobs by REGION aggregated over the whole dataset.
    Requires columns: REGION, TOTAL_JOBS
    """
    try:
        if not {'REGION', 'TOTAL_JOBS'}.issubset(df.columns):
            return "<p>Missing required columns for region pie: REGION, TOTAL_JOBS</p>"

        df = _ensure_numeric(df, "TOTAL_JOBS")
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
                <tr>
                    <th>Region</th>
                    <th>Total Jobs</th>
                </tr>
                {chr(10).join(table_rows)}
            </tbody>
        </table>
    </ac:rich-text-body>
</ac:structured-macro>

<h3>Total Jobs by Region</h3>
<table class="wrapped">
    <tbody>
        <tr>
            <th>Region</th>
            <th>Total Jobs</th>
        </tr>
        {"".join(table_rows)}
    </tbody>
</table>
"""
    except Exception as e:
        logging.error(f"Error generating region pie and table: {str(e)}")
        return f"<p>Error generating region pie and table: {str(e)}</p>"


def generate_daily_usage_by_region_chart(df: pd.DataFrame) -> str:
    """
    Generate the daily usage chart by region (line chart with dates on x-axis).
    Transposes the table so that regions are the series (rows) and dates are columns.

    Requires columns: DATE, REGION, TOTAL_JOBS
    """
    try:
        if not {'DATE', 'REGION', 'TOTAL_JOBS'}.issubset(df.columns):
            return "<p>Missing required columns for daily usage by region chart: DATE, REGION, TOTAL_JOBS</p>"

        df = _ensure_numeric(df, "TOTAL_JOBS")
        df = _ensure_date_col(df, "DATE")

        # Sort by date to ensure proper timeline
        df_sorted = df.sort_values('DATE')

        # Unique regions and dates
        regions = sorted(df_sorted['REGION'].dropna().astype(str).unique())
        dates = sorted(df_sorted['DATE'].dropna().unique())

        # Sample dates to reduce x-axis crowding
        if len(dates) > 10:
            sampled_dates = dates[::3]
        else:
            sampled_dates = dates

        # Format dates for display - use MM-DD format
        formatted_dates = [pd.to_datetime(date).strftime('%m-%d') for date in sampled_dates]

        # Create header row: Region + dates
        chart_table_rows = ["<tr><th>Region</th>"]
        for date_str in formatted_dates:
            chart_table_rows[0] += f"<th>{date_str}</th>"
        chart_table_rows[0] += "</tr>"

        # Add data rows - each row is a region with values for each date
        for region in regions:
            data_row = f"<tr><td>{region}</td>"
            for date in sampled_dates:
                region_date_data = df_sorted[(df_sorted['REGION'].astype(str) == region) & (df_sorted['DATE'] == date)]
                value = int(region_date_data['TOTAL_JOBS'].sum()) if not region_date_data.empty else 0
                data_row += f"<td>{value}</td>"
            data_row += "</tr>"
            chart_table_rows.append(data_row)

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
        <table>
            <tbody>
                {"".join(chart_table_rows)}
            </tbody>
        </table>
    </ac:rich-text-body>
</ac:structured-macro>

<h4>Transposed Data Structure (for debugging):</h4>
<table class="wrapped">
    <tbody>
        {chart_table_rows[0]}
        {chart_table_rows[1] if len(chart_table_rows) > 1 else '<tr><td colspan="9">No data</td></tr>'}
    </tbody>
</table>
"""
    except Exception as e:
        logging.error(f"Error generating daily usage by region chart: {str(e)}")
        return f"<p>Error generating daily usage by region chart: {str(e)}</p>"


def generate_baseline_variation_chart(df: pd.DataFrame, baseline: int = 1899206) -> str:
    """
    Generate the variation with baseline chart with dates on x-axis.

    Requires columns: DATE, TOTAL_JOBS
    """
    try:
        df = _ensure_numeric(df, "TOTAL_JOBS")
        df = _ensure_date_col(df, "DATE")

        df_summary = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')
        df_summary['Day'] = df_summary['DATE'].dt.strftime('%m-%d')

        # Header row with dates
        chart_rows = []
        header_row = "<tr><th>Date</th>" + "".join(f"<th>{row['Day']}</th>" for _, row in df_summary.iterrows()) + "</tr>"
        chart_rows.append(header_row)

        # Baseline series
        baseline_row = "<tr><td>Baseline</td>" + "".join("<td>{}</td>".format(baseline) for _ in range(len(df_summary))) + "</tr>"
        chart_rows.append(baseline_row)

        # Peaks (actual)
        peaks_row = "<tr><td>Peaks</td>" + "".join(f"<td>{int(row['TOTAL_JOBS'])}</td>" for _, row in df_summary.iterrows()) + "</tr>"
        chart_rows.append(peaks_row)

        # Max Range constant series
        max_range = 2000000
        max_row = "<tr><td>Max Range</td>" + "".join(f"<td>{max_range}</td>" for _ in range(len(df_summary))) + "</tr>"
        chart_rows.append(max_row)

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
        <table>
            <tbody>
                {chr(10).join(chart_rows)}
            </tbody>
        </table>
    </ac:rich-text-body>
</ac:structured-macro>
"""
    except Exception as e:
        logging.error(f"Error generating baseline variation chart: {str(e)}")
        return f"<p>Error generating baseline variation chart: {str(e)}</p>"


def generate_monthly_task_usage_chart(df: pd.DataFrame, baseline: int = 1899206) -> str:
    """
    Generate the overall monthly task usage chart.
    (In practice this is a daily aggregation across the month in the input data.)

    Requires columns: DATE, TOTAL_JOBS
    """
    try:
        df = _ensure_numeric(df, "TOTAL_JOBS")
        df = _ensure_date_col(df, "DATE")

        df_monthly = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')
        df_monthly['Day'] = df_monthly['DATE'].dt.strftime('%m-%d')

        # Create header row with dates
        header_row = "<tr><th>Series</th>" + "".join(f"<th>{row['Day']}</th>" for _, row in df_monthly.iterrows()) + "</tr>"

        # Baseline series
        baseline_row = "<tr><td>Baseline</td>" + "".join(f"<td>{baseline}</td>" for _ in range(len(df_monthly))) + "</tr>"

        # Sum of TOTAL_JOBS series
        jobs_row = "<tr><td>Sum of TOTAL_JOBS</td>" + "".join(f"<td>{int(row['TOTAL_JOBS'])}</td>" for _, row in df_monthly.iterrows()) + "</tr>"

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
        <table>
            <tbody>
                {header_row}
                {baseline_row}
                {jobs_row}
            </tbody>
        </table>
    </ac:rich-text-body>
</ac:structured-macro>
"""
    except Exception as e:
        logging.error(f"Error generating monthly task usage chart: {str(e)}")
        return f"<p>Error generating monthly task usage chart: {str(e)}</p>"


def generate_daily_summary_table(df: pd.DataFrame) -> str:
    """
    Generate the daily summary table of TOTAL_JOBS per DATE.

    Requires columns: DATE, TOTAL_JOBS
    """
    try:
        df = _ensure_numeric(df, "TOTAL_JOBS")
        df = _ensure_date_col(df, "DATE")

        df_summary = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')
        df_summary['FormattedDate'] = df_summary['DATE'].dt.strftime('%m/%d/%Y')

        rows = ["<tr><th>Date</th><th>Total Jobs</th></tr>"]
        for _, row in df_summary.iterrows():
            rows.append(f"<tr><td>{row['FormattedDate']}</td><td>{int(row['TOTAL_JOBS'])}</td></tr>")

        return f"""
<h3>Total Jobs Per Day</h3>
<table class="wrapped">
    <tbody>
        {"".join(rows)}
    </tbody>
</table>
"""
    except Exception as e:
        logging.error(f"Error generating daily summary table: {str(e)}")
        return f"<p>Error generating daily summary table: {str(e)}</p>"


def generate_peaks_variation_table(df: pd.DataFrame, baseline: int = 1899206) -> str:
    """
    Generate the peaks variation table (Peaks, Variation, Baseline, Max Range) by DATE.

    Requires columns: DATE, TOTAL_JOBS
    """
    try:
        df = _ensure_numeric(df, "TOTAL_JOBS")
        df = _ensure_date_col(df, "DATE")

        max_range = 2000000
        df_summary = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')

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
<table class="wrapped">
    <tbody>
        {"".join(rows)}
    </tbody>
</table>
"""
    except Exception as e:
        logging.error(f"Error generating peaks variation table: {str(e)}")
        return f"<p>Error generating peaks variation table: {str(e)}</p>"


def generate_daily_by_country_chart(df: pd.DataFrame) -> str:
    """
    Combined line chart: dates on X-axis, one series per COUNTRY showing sum of TOTAL_JOBS.

    Requires columns: DATE, COUNTRY, TOTAL_JOBS
    """
    try:
        if not {'DATE', 'COUNTRY', 'TOTAL_JOBS'}.issubset(df.columns):
            return "<p>Missing required columns for country chart: DATE, COUNTRY, TOTAL_JOBS</p>"

        df = _ensure_numeric(df, "TOTAL_JOBS")
        df = _ensure_date_col(df, "DATE")

        df = df.groupby(['DATE', 'COUNTRY'], as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')
        df['Day'] = df['DATE'].dt.strftime('%m-%d')

        dates = df['Day'].dropna().unique().tolist()
        countries = sorted(df['COUNTRY'].dropna().astype(str).unique().tolist())

        # Header row
        header = "<tr><th>Series</th>" + "".join(f"<th>{d}</th>" for d in dates) + "</tr>"

        rows = [header]
        for country in countries:
            series = df[df['COUNTRY'].astype(str) == country]
            values = []
            for d in dates:
                v = series.loc[series['Day'] == d, 'TOTAL_JOBS'].sum()
                v = int(v) if pd.notna(v) else 0
                values.append(f"<td>{v}</td>")
            rows.append(f"<tr><td>{country}</td>{''.join(values)}</tr>")

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
        <table><tbody>
            {''.join(rows)}
        </tbody></table>
    </ac:rich-text-body>
</ac:structured-macro>
"""
    except Exception as e:
        logging.error(f"Error generating daily by country chart: {str(e)}")
        return f"<p>Error generating daily by country chart: {str(e)}</p>"


# --------------------------------------------------------------------------------------
# Single-dataset publishing
# --------------------------------------------------------------------------------------

def publish_to_confluence(report_file: str,
                          test_mode: bool = False,
                          skip_actual_upload: bool = False) -> bool:
    """
    Publish a single dataset report to Confluence.

    Args:
        report_file: Path to the processed CSV (e.g., task_usage_report_by_region.csv)
        test_mode: When True, marks content as TEST DATA and allows simulated publishing
        skip_actual_upload: When True, will not perform HTTP upload (simulated only in test flows)

    Returns:
        True on success, False on failure.
    """
    try:
        if not os.path.exists(report_file):
            logging.error(f"Report file not found: {report_file}")
            print(f"Report file not found: {report_file}")
            return False

        # Read config
        config = load_config_json()
        baseline = config.get('BASELINE', 1899206)

        # Load CSV
        df = pd.read_csv(report_file)
        if 'DATE' in df.columns:
            df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')

        # Optional console preview
        try:
            preview = df.head(5)
            print("Preview of report data (first 5 rows):")
            print(preview.to_string(index=False))
            if len(df) > 5:
                print(f"... and {len(df) - 5} more rows")
        except Exception:
            pass

        # Execution metadata (kept consistent with existing module practice)
        execution_timestamp = datetime.strptime('2025-07-06 23:45:29', '%Y-%m-%d %H:%M:%S')
        execution_user = 'satish537'

        # Build page content (order aligned with the existing repo’s layout)
        content_parts = [
            f"<h1>Monthly Task Usage Report{' - TEST DATA' if test_mode else ''}</h1>",
            f"<p><strong>Last updated:</strong> {execution_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}</p>",
            f"<p><strong>Generated by:</strong> {execution_user}</p>",
            generate_table_and_chart(df, baseline),
            generate_daily_usage_by_region_chart(df),
            generate_monthly_task_usage_chart(df, baseline),
            generate_baseline_variation_chart(df, baseline),
            generate_daily_summary_table(df),
            "<hr />",
            f"<p><em>Note: This report shows the task usage data{' (TEST MODE)' if test_mode else ''}</em></p>",
        ]
        content = "\n".join(content_parts)

        # Simulated upload path
        if skip_actual_upload:
            print(f"[{datetime.now()}] TEST MODE: Creating page content...")
            print(f"[{datetime.now()}] TEST MODE: Would publish to page '{config.get('PAGE_TITLE')}' in space '{config.get('SPACE_KEY')}'")
            print(f"[{datetime.now()}] TEST MODE: Simulated publishing only (no actual upload)")
            logging.info("Test Confluence publishing simulated successfully")
            return True

        # Real upload path
        print(f"[{datetime.now()}] Connecting to Confluence...")
        try:
            session = _confluence_auth_session(config)
        except Exception as e:
            logging.error(f"Failed to set up Confluence auth: {e}")
            print("No Confluence password available for upload.")
            return False

        print(f"[{datetime.now()}] Creating page content...")
        page_id, version = get_page_info(session, config)
        ok = create_or_update_page(session, config, html_content=content, page_id=page_id, version=version)
        if not ok:
            logging.error("Failed to create/update the Confluence page.")
            return False

        print(f"[{datetime.now()}] Publish complete.")
        logging.info("Confluence publishing completed successfully")
        return True

    except Exception as e:
        logging.error(f"Error in publish_to_confluence: {str(e)}")
        return False


# --------------------------------------------------------------------------------------
# Multi-country publishing
# --------------------------------------------------------------------------------------

def publish_to_confluence_multi(report_files: List[Tuple[str, str]],
                                test_mode: bool = False,
                                skip_actual_upload: bool = False) -> bool:
    """
    Publish a combined page for multiple countries.

    Args:
        report_files: list of (country_name, report_csv_path)
        test_mode: mark content as TEST DATA
        skip_actual_upload: simulate only

    Returns:
        True on success, False otherwise
    """
    try:
        # Load config for Confluence info
        config = load_config_json()
        baseline = config.get('BASELINE', 1899206)

        # Read all CSVs and tag with COUNTRY
        frames: List[pd.DataFrame] = []
        details: List[Tuple[str, pd.DataFrame]] = []

        for country, path in report_files:
            if not os.path.exists(path):
                logging.warning(f"Report file for {country} not found: {path}")
                continue
            df = pd.read_csv(path)

            # Parse DATE if present
            if 'DATE' in df.columns:
                df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
            # Ensure numeric total jobs
            if 'TOTAL_JOBS' in df.columns:
                df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')

            df['COUNTRY'] = country
            frames.append(df)
            details.append((country, df))

        if not frames:
            logging.error("No report data available to publish.")
            print("No report data available to publish.")
            return False

        all_df = pd.concat(frames, ignore_index=True)

        # Metadata (kept consistent with existing module practice)
        execution_timestamp = datetime.strptime('2025-07-06 23:45:29', '%Y-%m-%d %H:%M:%S')
        execution_user = 'satish537'

        # Build combined content
        sections: List[str] = []

        # Combined cross-country chart
        sections.append(generate_daily_by_country_chart(all_df))

        # Per-country sections (reuse existing components expecting DATE, REGION, TOTAL_JOBS)
        sections.append("<hr />")
        sections.append("<h2>Per-Country Sections</h2>")
        for country, dfc in details:
            sections.append(f"<hr /><h1>{country}</h1>")
            try:
                sections.append(generate_monthly_task_usage_chart(dfc, baseline))
                sections.append(generate_baseline_variation_chart(dfc, baseline))
                sections.append(generate_daily_usage_by_region_chart(df=dcf := dfc))  # assign to keep function call readable
                sections.append(generate_daily_summary_table(dcf))
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

        # Simulated upload path
        if skip_actual_upload:
            print(f"[{datetime.now()}] TEST MODE: Creating combined page content...")
            print(f"[{datetime.now()}] TEST MODE: Would publish to page '{config.get('PAGE_TITLE')}' in space '{config.get('SPACE_KEY')}'")
            print(f"[{datetime.now()}] TEST MODE: Simulated publishing only (no actual upload)")
            logging.info("Test Confluence multi-country publishing simulated successfully")
            return True

        # Real upload path
        print(f"[{datetime.now()}] Connecting to Confluence...")
        try:
            session = _confluence_auth_session(config)
        except Exception as e:
            logging.error(f"Failed to set up Confluence auth: {e}")
            print("No Confluence password available for upload.")
            return False

        page_id, version = get_page_info(session, config)
        ok = create_or_update_page(session, config, html_content=content, page_id=page_id, version=version)
        if not ok:
            logging.error("Failed to create/update the Confluence page.")
            return False

        print(f"[{datetime.now()}] Publish complete.")
        logging.info("Confluence multi-country publishing completed successfully")
        return True

    except Exception as e:
        logging.error(f"Error in publish_to_confluence_multi: {str(e)}")
        return False


# --------------------------------------------------------------------------------------
# Module entry (manual test helper)
# --------------------------------------------------------------------------------------

if __name__ == "__main__":
    # Basic manual test scaffold (no network I/O unless files exist and credentials provided).
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    import argparse
    p = argparse.ArgumentParser(description="Confluence Publisher Tester")
    p.add_argument("--file", help="Single report CSV to publish (single mode)")
    p.add_argument("--multi", nargs="*", help="Space-separated list of 'COUNTRY=path/to/report.csv'")
    p.add_argument("--simulate", action="store_true", help="Do not upload; simulate only")
    p.add_argument("--test", action="store_true", help="Mark content as TEST DATA")
    args = p.parse_args()

    if args.file:
        ok = publish_to_confluence(args.file, test_mode=args.test, skip_actual_upload=args.simulate)
        print("Single publish:", "OK" if ok else "FAILED")
    elif args.multi:
        pairs: List[Tuple[str, str]] = []
        for item in args.multi:
            if "=" not in item:
                print(f"Invalid item (expected COUNTRY=path): {item}")
                continue
            country, path = item.split("=", 1)
            pairs.append((country.strip(), path.strip()))
        ok = publish_to_confluence_multi(pairs, test_mode=args.test, skip_actual_upload=args.simulate)
        print("Multi publish:", "OK" if ok else "FAILED")
    else:
        print("No input specified. Use --file or --multi.")
