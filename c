# Append the following new functions to this module

import os
import json
import logging
from typing import List, Tuple, Dict
from datetime import datetime

import pandas as pd
import requests

def generate_daily_by_country_chart(df: pd.DataFrame) -> str:
    """
    Combined line chart: dates on X-axis, one series per COUNTRY showing sum of TOTAL_JOBS.
    Requires columns: DATE, COUNTRY, TOTAL_JOBS
    """
    try:
        if not {'DATE', 'COUNTRY', 'TOTAL_JOBS'}.issubset(df.columns):
            return "<p>Missing required columns for country chart: DATE, COUNTRY, TOTAL_JOBS</p>"

        # Ensure types
        df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        if not pd.api.types.is_datetime64_any_dtype(df['DATE']):
            df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')

        # Aggregate by date and country
        df = df.groupby(['DATE', 'COUNTRY'], as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')
        df['Day'] = df['DATE'].dt.strftime('%m-%d')

        dates = df['Day'].unique().tolist()
        countries = sorted(df['COUNTRY'].unique().tolist())

        # Header row
        header = "<tr><th>Series</th>" + "".join(f"<th>{d}</th>" for d in dates) + "</tr>"

        rows = [header]
        for country in countries:
            series = df[df['COUNTRY'] == country]
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

def _confluence_auth_session(config: Dict) -> requests.Session:
    """
    Create an authenticated requests session for Confluence using basic auth.
    Tries CONFLUENCE_PASSWORD env, falls back to PASSWORD_ENCRYPTED in config with SecureConfig.
    """
    session = requests.Session()
    auth_type = (config.get("AUTH_TYPE") or "basic").lower()
    if auth_type == "basic":
        password = os.environ.get("CONFLUENCE_PASSWORD")
        if not password and 'PASSWORD_ENCRYPTED' in config:
            try:
                from lib.secure_config import SecureConfig
                password = SecureConfig.decrypt_password(config['PASSWORD_ENCRYPTED'])
            except Exception:
                password = None
        if not password:
            raise RuntimeError("No Confluence password available (env or encrypted).")
        session.auth = (config.get("USERNAME", ""), password)
    else:
        # Extend for PAT/bearer if supported by the repo
        raise NotImplementedError("Only basic auth is implemented in this helper.")
    return session

def _create_or_update_page(session: requests.Session, config: Dict, html_content: str, page_id: str = None, version: int = None) -> bool:
    """
    Create or update a Confluence page using 'storage' representation.
    If page_id is provided, updates the page with version+1. Otherwise creates a new page.
    """
    base_url = config.get("CONFLUENCE_URL") or ""
    # Ensure base url ends with .../rest/api/content/
    if not base_url.endswith("/"):
        base_url += "/"

    headers = {"Content-Type": "application/json"}

    title = config.get("PAGE_TITLE", "Task Usage (Multi-Country)")
    space_key = config.get("SPACE_KEY", "")

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
                    "representation": "storage"
                }
            },
            "version": {"number": (version or 1) + 1}
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
                    "representation": "storage"
                }
            }
        }
        resp = session.post(url, headers=headers, data=json.dumps(payload))
        if resp.status_code not in (200, 201):
            logging.error(f"Failed to create page: {resp.status_code} {resp.text}")
            return False
        return True

def publish_to_confluence_multi(report_files: List[Tuple[str, str]],
                                test_mode: bool = False,
                                skip_actual_upload: bool = False) -> bool:
    """
    Publish a combined page for multiple countries.
    report_files: list of (country_name, report_csv_path)
    """
    try:
        # Load config.json for Confluence info (this module in the repo already relies on config.json)
        config_path = "config.json"
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                config = json.load(f)
        else:
            config = {
                "CONFLUENCE_URL": "",
                "USERNAME": "",
                "AUTH_TYPE": "basic",
                "SPACE_KEY": "",
                "PAGE_TITLE": "Task Usage (Multi-Country)",
                "BASELINE": 1899206
            }

        baseline = config.get('BASELINE', 1899206)

        # Read all CSVs and tag with COUNTRY
        frames = []
        details = []
        for country, path in report_files:
            if not os.path.exists(path):
                logging.warning(f"Report file for {country} not found: {path}")
                continue
            df = pd.read_csv(path)
            # Parse DATE if present
            if 'DATE' in df.columns:
                df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
            # Ensure TOTAL_JOBS numeric if present
            if 'TOTAL_JOBS' in df.columns:
                df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
            df['COUNTRY'] = country
            frames.append(df)
            details.append((country, df))

        if not frames:
            logging.error("No report data available to publish.")
            return False

        all_df = pd.concat(frames, ignore_index=True)

        # Metadata (kept consistent with existing module practice)
        execution_timestamp = datetime.strptime('2025-07-06 23:45:29', '%Y-%m-%d %H:%M:%S')
        execution_user = 'satish537'

        # Build combined content
        sections = []

        # Combined cross-country chart
        sections.append(generate_daily_by_country_chart(all_df))

        # Per-country sections
        from .confluence_publisher import (
            generate_monthly_task_usage_chart,
            generate_baseline_variation_chart,
            generate_daily_usage_by_region_chart,
            generate_daily_summary_table
        )

        for country, df in details:
            sections.append(f"<hr /><h1>{country}</h1>")
            try:
                sections.append(generate_monthly_task_usage_chart(df, baseline))
                sections.append(generate_baseline_variation_chart(df, baseline))
                sections.append(generate_daily_usage_by_region_chart(df))
                sections.append(generate_daily_summary_table(df))
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

        if skip_actual_upload:
            print(f"[{datetime.now()}] TEST MODE: Creating combined page content...")
            print(f"[{datetime.now()}] TEST MODE: Would publish to page '{config.get('PAGE_TITLE')}' in space '{config.get('SPACE_KEY')}'")
            print(f"[{datetime.now()}] TEST MODE: Simulated publishing only (no actual upload)")
            logging.info("Test Confluence multi-country publishing simulated successfully")
            return True

        # Real upload path
        try:
            session = _confluence_auth_session(config)
        except Exception as e:
            logging.error(f"Failed to set up Confluence auth: {e}")
            print("No Confluence password available for upload.")
            return False

        # Use existing helper to get page info, then create or update
        try:
            page_id, version = get_page_info(session, config)  # existing function in this module
        except Exception as e:
            logging.error(f"Failed to get page info: {e}")
            page_id, version = None, None

        ok = _create_or_update_page(session, config, html_content=content, page_id=page_id, version=version)
        return bool(ok)

    except Exception as e:
        logging.error(f"Error in publish_to_confluence_multi: {str(e)}")
        return False
