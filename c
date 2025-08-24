import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import json
import urllib3
import os
from datetime import datetime
import logging
import re
from typing import Tuple, Optional, Dict, List

# -------------------------------------------------------------------------------------------------
# CONFLUENCE PUBLISHER (Refactored)
# - Monthly vs Daily detection
# - Correct Top 4 Peaks (per month)
# - Baseline vs Total Jobs bar charts
# - Daily totals (all countries combined)
# - Region, Country, and Variation visuals
# -------------------------------------------------------------------------------------------------

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MONTH_NAMES = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
)

def _is_monthly_run(config: Dict) -> bool:
    title = config.get('PAGE_TITLE', '')
    if title.endswith('_daily'):
        return False
    if re.search(r'\s-\s(' + '|'.join(MONTH_NAMES) + r')$', title):
        return True
    return True  # default to monthly style (legacy behavior)

# -------------------------------------------------------------------------------------------------
# Config / Auth
# -------------------------------------------------------------------------------------------------

def load_config(config_path: str = "config.json") -> Optional[Dict]:
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        required = ['CONFLUENCE_URL', 'USERNAME', 'SPACE_KEY', 'PAGE_TITLE']
        if not all(k in config for k in required):
            logging.error(f"Missing required configuration keys. Need: {required}")
            return None
        if 'BASELINE' not in config:
            config['BASELINE'] = 1899206
        return config
    except Exception as e:
        logging.error(f"Error loading config: {e}")
        return None

def get_password_from_config(config: Dict) -> Optional[str]:
    if 'PASSWORD_ENCRYPTED' in config:
        try:
            from lib.secure_config import SecureConfig
            return SecureConfig.decrypt_password(config['PASSWORD_ENCRYPTED'])
        except Exception as e:
            logging.error(f"Error decrypting password: {e}")
            return None
    return None

def create_session(config: Dict) -> Optional[requests.Session]:
    session = requests.Session()
    auth_type = config.get('AUTH_TYPE', 'basic').lower()
    if auth_type == 'basic':
        password = (os.environ.get('CONFLUENCE_PASSWORD') or
                    get_password_from_config(config) or
                    config.get('API_TOKEN'))
        if not password:
            logging.error("Missing credentials for basic auth.")
            return None
        session.auth = HTTPBasicAuth(config['USERNAME'], password)
    elif auth_type == 'jwt':
        token = os.environ.get('CONFLUENCE_TOKEN') or config.get('API_TOKEN')
        if not token:
            logging.error("Missing JWT token.")
            return None
        session.headers.update({"Authorization": f"Bearer {token}"})
    elif auth_type == 'cookie':
        cookie = os.environ.get('CONFLUENCE_COOKIE') or config.get('SESSION_COOKIE')
        if not cookie:
            logging.error("Missing cookie token.")
            return None
        session.headers.update({"Cookie": cookie})
    else:
        logging.error(f"Unsupported AUTH_TYPE {auth_type}")
        return None

    session.headers.update({
        "Content-Type": "application/json",
        "X-Atlassian-Token": "no-check"
    })
    if 'PROXY' in config:
        session.proxies = {"http": config['PROXY'], "https": config['PROXY']}
    session.verify = False
    return session

# -------------------------------------------------------------------------------------------------
# Data
# -------------------------------------------------------------------------------------------------

def load_csv_data(csv_file: str) -> Tuple[pd.DataFrame, bool]:
    try:
        df = pd.read_csv(csv_file)
        if 'DATE' in df.columns:
            df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
        if 'TOTAL_JOBS' in df.columns:
            df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
        return df, True
    except Exception as e:
        logging.error(f"Error loading CSV {csv_file}: {e}")
        return pd.DataFrame(), False

# -------------------------------------------------------------------------------------------------
# Peak Helpers
# -------------------------------------------------------------------------------------------------

def _latest_year_month(daily_df: pd.DataFrame) -> Tuple[int, int]:
    latest = daily_df['DATE'].max()
    return latest.year, latest.month

def generate_monthly_top4_peaks_section(df: pd.DataFrame,
                                        baseline: int,
                                        year: Optional[int] = None,
                                        month: Optional[int] = None,
                                        top_n: int = 4) -> str:
    try:
        if 'DATE' not in df.columns or 'TOTAL_JOBS' not in df.columns:
            return "<p>Missing DATE or TOTAL_JOBS columns for peak computation.</p>"

        daily = (df[['DATE', 'TOTAL_JOBS']]
                 .dropna()
                 .groupby('DATE', as_index=False)['TOTAL_JOBS'].sum())
        if daily.empty:
            return "<p>No data for peaks.</p>"

        if year is None or month is None:
            year, month = _latest_year_month(daily)

        month_df = daily[(daily.DATE.dt.year == year) & (daily.DATE.dt.month == month)].copy()
        if month_df.empty:
            return f"<p>No data for {year}-{month:02d}.</p>"

        try:
            peaks = month_df.nlargest(top_n, 'TOTAL_JOBS')
        except Exception:
            peaks = month_df.sort_values('TOTAL_JOBS', ascending=False).head(top_n)

        peaks_rank_order = peaks.sort_values('TOTAL_JOBS', ascending=False).reset_index(drop=True)
        peaks_display = peaks.sort_values('DATE').copy()
        peaks_display['Baseline'] = baseline
        peaks_display['Variation'] = baseline - peaks_display['TOTAL_JOBS']
        peaks_display['DateStr'] = peaks_display['DATE'].dt.strftime('%Y-%m-%d')
        rank_map = {row.DATE: idx + 1 for idx, row in peaks_rank_order.iterrows()}

        month_label = peaks_display['DATE'].dt.strftime('%b').iloc[0]
        chart_title = f"Top {len(peaks_display)} Peaks of {month_label}"

        # Chart: Date | Baseline | Total Jobs
        chart_header = "<tr><th>Date</th><th>Baseline</th><th>Total Jobs</th></tr>"
        chart_rows = [
            f"<tr><td>{r.DateStr}</td><td>{int(r.Baseline)}</td><td>{int(r.TOTAL_JOBS)}</td></tr>"
            for r in peaks_display.itertuples()
        ]
        chart_table = "<table><tbody>" + chart_header + "".join(chart_rows) + "</tbody></table>"

        # Detail table with rank + variation
        detail_header = "<tr><th>Rank</th><th>Date</th><th>Total Jobs</th><th>Baseline</th><th>Variation (Baseline - Total)</th></tr>"
        detail_rows = []
        for r in peaks_display.itertuples():
            variation = int(r.Variation)
            style = 'style="background-color:#90EE90;"' if variation > 0 else 'style="background-color:#FFB6C1;"'
            detail_rows.append(
                f"<tr><td>{rank_map[r.DATE]}</td><td>{r.DateStr}</td><td>{int(r.TOTAL_JOBS)}</td>"
                f"<td>{int(r.Baseline)}</td><td {style}>{variation}</td></tr>"
            )
        detail_table = "<table class='wrapped'><tbody>" + detail_header + "".join(detail_rows) + "</tbody></table>"

        return f"""
<h3>{chart_title}</h3>
<ac:structured-macro ac:name="chart">
  <ac:parameter ac:name="title">{chart_title}</ac:parameter>
  <ac:parameter ac:name="type">bar</ac:parameter>
  <ac:parameter ac:name="orientation">vertical</ac:parameter>
  <ac:parameter ac:name="width">800</ac:parameter>
  <ac:parameter ac:name="height">450</ac:parameter>
  <ac:parameter ac:name="legend">true</ac:parameter>
  <ac:parameter ac:name="dataDisplay">true</ac:parameter>
  <ac:parameter ac:name="stacked">false</ac:parameter>
  <ac:parameter ac:name="showValues">true</ac:parameter>
  <ac:parameter ac:name="valuePosition">inside</ac:parameter>
  <ac:parameter ac:name="displayValuesOnBars">true</ac:parameter>
  <ac:parameter ac:name="labelAngle">45</ac:parameter>
  <ac:parameter ac:name="xLabel">Date</ac:parameter>
  <ac:parameter ac:name="yLabel">Jobs</ac:parameter>
  <ac:parameter ac:name="colors">#B22222,#6B8E23</ac:parameter>
  <ac:parameter ac:name="seriesColors">#B22222,#6B8E23</ac:parameter>
  <ac:rich-text-body>
    {chart_table}
  </ac:rich-text-body>
</ac:structured-macro>

<h4>Peak Day Details (Variation vs Baseline)</h4>
{detail_table}
"""
    except Exception as e:
        logging.error(f"Error generating monthly top peaks: {e}")
        return f"<p>Error generating monthly top peaks: {e}</p>"

# -------------------------------------------------------------------------------------------------
# Additional Charts
# -------------------------------------------------------------------------------------------------

def generate_all_countries_daily_totals_chart(df: pd.DataFrame, baseline: int) -> str:
    try:
        if 'DATE' not in df.columns or 'TOTAL_JOBS' not in df.columns:
            return "<p>Missing DATE or TOTAL_JOBS for daily total chart.</p>"
        daily = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')
        if daily.empty:
            return "<p>No data for daily totals.</p>"
        daily['DS'] = daily['DATE'].dt.strftime('%Y-%m-%d')
        header = "<tr><th>Date</th><th>Baseline</th><th>Total Jobs</th></tr>"
        rows = [f"<tr><td>{r.DS}</td><td>{baseline}</td><td>{int(r.TOTAL_JOBS)}</td></tr>" for r in daily.itertuples()]
        table_html = "<table><tbody>" + header + "".join(rows) + "</tbody></table>"
        return f"""
<h3>Daily Total Jobs (All Countries)</h3>
<ac:structured-macro ac:name="chart">
  <ac:parameter ac:name="title">Daily Total Jobs (All Countries)</ac:parameter>
  <ac:parameter ac:name="type">bar</ac:parameter>
  <ac:parameter ac:name="width">1100</ac:parameter>
  <ac:parameter ac:name="height">500</ac:parameter>
  <ac:parameter ac:name="legend">true</ac:parameter>
  <ac:parameter ac:name="dataDisplay">false</ac:parameter>
  <ac:parameter ac:name="showValues">false</ac:parameter>
  <ac:parameter ac:name="labelAngle">45</ac:parameter>
  <ac:parameter ac:name="xLabel">Date</ac:parameter>
  <ac:parameter ac:name="yLabel">Jobs</ac:parameter>
  <ac:rich-text-body>
    {table_html}
  </ac:rich-text-body>
</ac:structured-macro>
"""
    except Exception as e:
        logging.error(f"Error generating daily total jobs chart: {e}")
        return f"<p>Error generating daily total jobs chart: {e}</p>"

def generate_variation_with_baseline_chart(df: pd.DataFrame, baseline: int) -> str:
    try:
        if 'DATE' not in df.columns or 'TOTAL_JOBS' not in df.columns:
            return "<p>Missing DATE or TOTAL_JOBS for variation chart.</p>"
        daily = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')
        if daily.empty:
            return "<p>No data for variation chart.</p>"
        daily['DS'] = daily['DATE'].dt.strftime('%Y-%m-%d')
        header = "<tr><th>Date</th><th>Baseline</th><th>Total Jobs</th></tr>"
        rows = [f"<tr><td>{r.DS}</td><td>{baseline}</td><td>{int(r.TOTAL_JOBS)}</td></tr>" for r in daily.itertuples()]
        table_html = "<table><tbody>" + header + "".join(rows) + "</tbody></table>"
        return f"""
<h3>Baseline vs Daily Totals</h3>
<ac:structured-macro ac:name="chart">
  <ac:parameter ac:name="title">Baseline vs Daily Totals</ac:parameter>
  <ac:parameter ac:name="type">line</ac:parameter>
  <ac:parameter ac:name="width">1100</ac:parameter>
  <ac:parameter ac:name="height">450</ac:parameter>
  <ac:parameter ac:name="legend">true</ac:parameter>
  <ac:parameter ac:name="dataDisplay">false</ac:parameter>
  <ac:parameter ac:name="xLabel">Date</ac:parameter>
  <ac:parameter ac:name="yLabel">Jobs</ac:parameter>
  <ac:parameter ac:name="labelAngle">45</ac:parameter>
  <ac:rich-text-body>
    {table_html}
  </ac:rich-text-body>
</ac:structured-macro>
"""
    except Exception as e:
        logging.error(f"Error generating variation chart: {e}")
        return f"<p>Error generating variation chart: {e}</p>"

def generate_daily_usage_by_region_chart(df: pd.DataFrame) -> str:
    try:
        needed = {'DATE', 'REGION', 'TOTAL_JOBS'}
        if not needed.issubset(df.columns):
            return "<p>Missing columns for region line chart (DATE, REGION, TOTAL_JOBS).</p>"
        df2 = df.copy()
        df2['TOTAL_JOBS'] = pd.to_numeric(df2['TOTAL_JOBS'], errors='coerce')
        agg = df2.groupby(['DATE', 'REGION'], as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')
        agg['DS'] = agg['DATE'].dt.strftime('%Y-%m-%d')
        dates = agg['DS'].unique().tolist()
        regions = sorted(agg['REGION'].unique().tolist())
        header = "<tr><th>Date</th>" + "".join(f"<th>{r}</th>" for r in regions) + "</tr>"
        rows = [header]
        for d in dates:
            slice_d = agg[agg['DS'] == d]
            row = f"<tr><td>{d}</td>"
            for r in regions:
                val = slice_d.loc[slice_d['REGION'] == r, 'TOTAL_JOBS'].sum()
                row += f"<td>{int(val) if pd.notna(val) else 0}</td>"
            row += "</tr>"
            rows.append(row)
        table_html = "<table><tbody>" + "".join(rows) + "</tbody></table>"
        return f"""
<h3>Daily Task Usage by Region</h3>
<ac:structured-macro ac:name="chart">
  <ac:parameter ac:name="title">Daily Task Usage by Region</ac:parameter>
  <ac:parameter ac:name="type">line</ac:parameter>
  <ac:parameter ac:name="width">1100</ac:parameter>
  <ac:parameter ac:name="height">450</ac:parameter>
  <ac:parameter ac:name="legend">true</ac:parameter>
  <ac:parameter ac:name="dataDisplay">false</ac:parameter>
  <ac:parameter ac:name="xLabel">Date</ac:parameter>
  <ac:parameter ac:name="yLabel">Jobs</ac:parameter>
  <ac:parameter ac:name="labelAngle">45</ac:parameter>
  <ac:rich-text-body>
    {table_html}
  </ac:rich-text-body>
</ac:structured-macro>
"""
    except Exception as e:
        logging.error(f"Error generating region usage chart: {e}")
        return f"<p>Error generating region usage chart: {e}</p>"

def generate_region_chart(df: pd.DataFrame) -> str:
    """Pie chart by region (optional legacy)."""
    try:
        needed = {'DATE', 'REGION', 'TOTAL_JOBS'}
        if not needed.issubset(df.columns):
            return "<p>Missing columns for region pie chart (DATE, REGION, TOTAL_JOBS).</p>"
        summary = (df.groupby('REGION', as_index=False)['TOTAL_JOBS']
                     .sum()
                     .sort_values('TOTAL_JOBS', ascending=False))
        header = "<tr><th>Region</th><th>Total Jobs</th></tr>"
        rows = [f"<tr><td>{r.REGION}</td><td>{int(r.TOTAL_JOBS)}</td></tr>" for r in summary.itertuples()]
        table_html = "<table><tbody>" + header + "".join(rows) + "</tbody></table>"
        return f"""
<h3>Jobs by Region (Pie)</h3>
<ac:structured-macro ac:name="chart">
  <ac:parameter ac:name="title">Jobs by Region</ac:parameter>
  <ac:parameter ac:name="type">pie</ac:parameter>
  <ac:parameter ac:name="width">500</ac:parameter>
  <ac:parameter ac:name="height">350</ac:parameter>
  <ac:parameter ac:name="legend">true</ac:parameter>
  <ac:parameter ac:name="dataDisplay">true</ac:parameter>
  <ac:rich-text-body>
    {table_html}
  </ac:rich-text-body>
</ac:structured-macro>
"""
    except Exception as e:
        logging.error(f"Error generating region pie chart: {e}")
        return f"<p>Error generating region pie chart: {e}</p>"

def generate_daily_summary_table(df: pd.DataFrame) -> str:
    try:
        if 'DATE' not in df.columns or 'TOTAL_JOBS' not in df.columns:
            return "<p>Missing DATE or TOTAL_JOBS for daily summary.</p>"
        daily = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum()
        daily['DS'] = daily['DATE'].dt.strftime('%Y-%m-%d')
        header = "<tr><th>Date</th><th>Total Jobs</th></tr>"
        rows = [f"<tr><td>{r.DS}</td><td>{int(r.TOTAL_JOBS)}</td></tr>" for r in daily.itertuples()]
        return "<h3>Daily Totals Table</h3><table class='wrapped'><tbody>" + header + "".join(rows) + "</tbody></table>"
    except Exception as e:
        logging.error(f"Error generating daily summary: {e}")
        return f"<p>Error generating daily summary: {e}</p>"

def generate_daily_by_country_chart(df: pd.DataFrame) -> str:
    try:
        needed = {'DATE', 'COUNTRY', 'TOTAL_JOBS'}
        if not needed.issubset(df.columns):
            return "<p>Missing required columns for country chart (DATE, COUNTRY, TOTAL_JOBS).</p>"
        agg = (df.groupby(['DATE', 'COUNTRY'], as_index=False)['TOTAL_JOBS']
                 .sum()
                 .sort_values('DATE'))
        agg['DS'] = agg['DATE'].dt.strftime('%Y-%m-%d')
        dates = agg['DS'].unique().tolist()
        countries = sorted(agg['COUNTRY'].unique().tolist())
        header = "<tr><th>Date</th>" + "".join(f"<th>{c}</th>" for c in countries) + "</tr>"
        rows = [header]
        for d in dates:
            slice_d = agg[agg['DS'] == d]
            row = f"<tr><td>{d}</td>"
            for c in countries:
                val = slice_d.loc[slice_d['COUNTRY'] == c, 'TOTAL_JOBS'].sum()
                row += f"<td>{int(val) if pd.notna(val) else 0}</td>"
            row += "</tr>"
            rows.append(row)
        table_html = "<table><tbody>" + "".join(rows) + "</tbody></table>"
        return f"""
<h3>Daily Total Jobs by Country</h3>
<ac:structured-macro ac:name="chart">
  <ac:parameter ac:name="title">Daily Total Jobs by Country</ac:parameter>
  <ac:parameter ac:name="type">line</ac:parameter>
  <ac:parameter ac:name="width">1100</ac:parameter>
  <ac:parameter ac:name="height">500</ac:parameter>
  <ac:parameter ac:name="legend">true</ac:parameter>
  <ac:parameter ac:name="dataDisplay">false</ac:parameter>
  <ac:parameter ac:name="xLabel">Date</ac:parameter>
  <ac:parameter ac:name="yLabel">Jobs</ac:parameter>
  <ac:parameter ac:name="labelAngle">45</ac:parameter>
  <ac:rich-text-body>
    {table_html}
  </ac:rich-text-body>
</ac:structured-macro>
"""
    except Exception as e:
        logging.error(f"Error generating daily by country chart: {e}")
        return f"<p>Error generating daily by country chart: {e}</p>"

# -------------------------------------------------------------------------------------------------
# Confluence API
# -------------------------------------------------------------------------------------------------

def get_page_info(session: requests.Session, config: Dict) -> Tuple[Optional[str], Optional[int]]:
    try:
        base_url = config['CONFLUENCE_URL'].rstrip('/')
        if not base_url.endswith('/content'):
            if base_url.endswith('/rest/api'):
                base_url = base_url + "/content"
            else:
                base_url = base_url + "/rest/api/content"
        params = {
            'title': config['PAGE_TITLE'],
            'spaceKey': config['SPACE_KEY'],
            'expand': 'version'
        }
        resp = session.get(base_url, params=params)
        if resp.status_code != 200:
            alt = f"{base_url}/search?cql=space={config['SPACE_KEY']} AND title=\"{config['PAGE_TITLE']}\""
            resp = session.get(alt)
        if resp.status_code != 200:
            logging.error(f"Page lookup failed: {resp.status_code} - {resp.text}")
            return None, None
        data = resp.json()
        if 'results' in data and data.get('size', 0) > 0:
            return data['results'][0]['id'], data['results'][0]['version']['number']
        if isinstance(data, list) and data:
            return data[0]['id'], data[0]['version']['number']
        return None, None
    except Exception as e:
        logging.error(f"Error getting page info: {e}")
        return None, None

def create_or_update_page(session: requests.Session,
                          config: Dict,
                          content: str,
                          page_id: Optional[str] = None,
                          version: Optional[int] = None) -> bool:
    try:
        base_url = config['CONFLUENCE_URL'].rstrip('/')
        if not base_url.endswith('/content'):
            if base_url.endswith('/rest/api'):
                base_url = base_url + "/content"
            else:
                base_url = base_url + "/rest/api/content"
        payload = {
            "type": "page",
            "title": config['PAGE_TITLE'],
            "space": {"key": config['SPACE_KEY']},
            "body": {"storage": {"value": content, "representation": "storage"}}
        }
        if page_id and version is not None:
            payload['id'] = page_id
            payload['version'] = {"number": version + 1}
            resp = session.put(f"{base_url}/{page_id}", json=payload)
        else:
            resp = session.post(base_url, json=payload)
        if resp.status_code >= 400:
            logging.error(f"Create/Update failed: {resp.status_code} - {resp.text}")
            return False
        logging.info(f"Page '{config['PAGE_TITLE']}' {'updated' if page_id else 'created'} successfully.")
        return True
    except Exception as e:
        logging.error(f"Error creating/updating page: {e}")
        return False

def test_connection(session: requests.Session, config: Dict) -> bool:
    try:
        base = config['CONFLUENCE_URL'].rstrip('/')
        if base.endswith('/content'):
            base = base[:-8]
        elif not base.endswith('/rest/api'):
            base = base + '/rest/api'
        test_url = f"{base}/space"
        resp = session.get(test_url, params={'limit': 1})
        if resp.status_code == 200:
            return True
        logging.error(f"Connection test failed: {resp.status_code} - {resp.text}")
        return False
    except Exception as e:
        logging.error(f"Connection test exception: {e}")
        return False

# -------------------------------------------------------------------------------------------------
# Single Publish
# -------------------------------------------------------------------------------------------------

def publish_to_confluence(report_file='task_usage_report_by_region.csv',
                          test_mode=False,
                          skip_actual_upload=False):
    try:
        if not os.path.exists(report_file):
            logging.error(f"File {report_file} not found.")
            return False

        cfg_path = "config_test.json" if (test_mode and os.path.exists("config_test.json")) else "config.json"
        if not os.path.exists(cfg_path):
            logging.error("Config file missing.")
            return False

        config = load_config(cfg_path)
        if not config:
            return False
        if test_mode and not config['PAGE_TITLE'].endswith("-TEST"):
            config['PAGE_TITLE'] += "-TEST"

        monthly = _is_monthly_run(config)
        baseline = config.get('BASELINE', 1899206)

        if skip_actual_upload:
            session = None
        else:
            session = create_session(config)
            if not session:
                if not test_mode:
                    return False
                skip_actual_upload = True

        if session and not skip_actual_upload and not test_connection(session, config):
            if not test_mode:
                return False
            skip_actual_upload = True

        df, ok = load_csv_data(report_file)
        if not ok:
            return False

        peaks_section = generate_monthly_top4_peaks_section(df, baseline) if monthly else "<!-- Monthly peaks suppressed (daily run) -->"
        daily_total_all = generate_all_countries_daily_totals_chart(df, baseline)
        variation_chart = generate_variation_with_baseline_chart(df, baseline)
        region_line = generate_daily_usage_by_region_chart(df) if 'REGION' in df.columns else "<!-- Region line suppressed: no REGION col -->"
        region_pie = generate_region_chart(df) if 'REGION' in df.columns else ""
        daily_summary = generate_daily_summary_table(df)

        execution_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        execution_user = 'satish537'

        content = f"""
<h1>Task Usage Report{' - TEST DATA' if test_mode else ''}</h1>
<p><strong>Last updated:</strong> {execution_timestamp}</p>
<p><strong>Generated by:</strong> {execution_user}</p>

{peaks_section}
{daily_total_all}
{variation_chart}
{region_line}
{region_pie}
{daily_summary}
<hr />
<p><em>Note: This report shows the task usage data{' (TEST MODE)' if test_mode else ''}.</em></p>
"""

        if skip_actual_upload:
            logging.info("Simulation only: not uploading.")
            return True

        page_id, version = get_page_info(session, config)
        return create_or_update_page(session, config, content, page_id, version)
    except Exception as e:
        logging.error(f"Error in single publish: {e}")
        return False

# -------------------------------------------------------------------------------------------------
# Multi-Country Publish
# -------------------------------------------------------------------------------------------------

def publish_to_confluence_multi(report_files: List[Tuple[str, str]],
                                test_mode: bool = False,
                                skip_actual_upload: bool = False) -> bool:
    try:
        cfg_path = "config_test.json" if (test_mode and os.path.exists("config_test.json")) else "config.json"
        if not os.path.exists(cfg_path):
            logging.error("Config file missing for multi-country.")
            return False
        config = load_config(cfg_path)
        if not config:
            return False
        if test_mode and not config['PAGE_TITLE'].endswith("-TEST"):
            config['PAGE_TITLE'] += "-TEST"

        monthly = _is_monthly_run(config)
        baseline = config.get('BASELINE', 1899206)

        frames = []
        per_country = []
        for ctry, path in report_files:
            if not os.path.exists(path):
                logging.warning(f"Missing report for {ctry}: {path}")
                continue
            df = pd.read_csv(path)
            if 'DATE' in df.columns:
                df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
            if 'TOTAL_JOBS' in df.columns:
                df['TOTAL_JOBS'] = pd.to_numeric(df['TOTAL_JOBS'], errors='coerce')
            df['COUNTRY'] = ctry
            frames.append(df)
            per_country.append((ctry, df))

        if not frames:
            logging.error("No valid country data.")
            return False

        all_df = pd.concat(frames, ignore_index=True)

        global_peaks = generate_monthly_top4_peaks_section(all_df, baseline) if monthly else "<!-- Monthly peaks suppressed (daily run) -->"
        global_daily = generate_all_countries_daily_totals_chart(all_df, baseline)
        global_variation = generate_variation_with_baseline_chart(all_df, baseline)
        global_country_line = generate_daily_by_country_chart(all_df)
        global_region_line = generate_daily_usage_by_region_chart(all_df) if 'REGION' in all_df.columns else ""
        global_region_pie = generate_region_chart(all_df) if 'REGION' in all_df.columns else ""

        sections = [
            global_peaks,
            global_daily,
            global_variation,
            global_country_line,
            global_region_line,
            global_region_pie,
            "<hr /><h2>Per-Country Sections</h2>"
        ]

        for ctry, df_ctry in per_country:
            sections.append(f"<h3>{ctry}</h3>")
            if monthly:
                sections.append(generate_monthly_top4_peaks_section(df_ctry, baseline))
            sections.append(generate_all_countries_daily_totals_chart(df_ctry, baseline))
            sections.append(generate_variation_with_baseline_chart(df_ctry, baseline))
            sections.append(generate_daily_summary_table(df_ctry))

        execution_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        execution_user = 'satish537'
        content = f"""
<h1>Multi-Country Task Usage Report{' - TEST DATA' if test_mode else ''}</h1>
<p><strong>Last updated:</strong> {execution_timestamp}</p>
<p><strong>Generated by:</strong> {execution_user}</p>
{''.join(sections)}
<hr />
<p><em>Note: This report shows the task usage data{' (TEST MODE)' if test_mode else ''}.</em></p>
"""

        if skip_actual_upload:
            logging.info("Simulation only: not uploading multi-country page.")
            return True

        session = create_session(config)
        if not session:
            return False
        if not test_connection(session, config):
            return False
        page_id, version = get_page_info(session, config)
        return create_or_update_page(session, config, content, page_id, version)
    except Exception as e:
        logging.error(f"Error in multi-country publish: {e}")
        return False

# -------------------------------------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    p = argparse.ArgumentParser(description="Confluence Publisher (Single / Multi)")
    p.add_argument("--file", help="Single report CSV to publish")
    p.add_argument("--multi", nargs="*", help="List of COUNTRY=path/to/file.csv")
    p.add_argument("--simulate", action="store_true", help="Skip actual upload (simulation)")
    p.add_argument("--test", action="store_true", help="Test mode (marks page title)")
    args = p.parse_args()

    if args.file:
        ok = publish_to_confluence(report_file=args.file,
                                   test_mode=args.test,
                                   skip_actual_upload=args.simulate)
        print("Single publish:", "OK" if ok else "FAILED")
    elif args.multi:
        pairs = []
        for item in args.multi:
            if "=" not in item:
                print(f"Ignoring invalid multi argument (expected COUNTRY=path): {item}")
                continue
            ctry, path = item.split("=", 1)
            pairs.append((ctry.strip(), path.strip()))
        ok = publish_to_confluence_multi(report_files=pairs,
                                         test_mode=args.test,
                                         skip_actual_upload=args.simulate)
        print("Multi publish:", "OK" if ok else "FAILED")
    else:
        p.print_help()
