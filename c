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

# -------------------------------------------------------------
# Confluence Publisher (Peaks + Daily Charts) - Improved
# - Robust TOTAL_JOBS parsing (commas, spaces)
# - Optional column override via JOB_COLUMN
# - Month selection + debug diagnostics
# - Transposed Top 4 Peaks (Baseline vs Total Jobs)
# - Single-series Daily Total Jobs (green bars)
# - FIXED: Proper display of both Baseline and Total Jobs bars
# -------------------------------------------------------------

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MONTH_NAMES = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
)

# =============================================================
# Configuration / Auth
# =============================================================

def _is_monthly_run(config: Dict) -> bool:
    title = config.get('PAGE_TITLE', '')
    if title.endswith('_daily'):
        return False
    if re.search(r'\s-\s(' + '|'.join(MONTH_NAMES) + r')$', title):
        return True
    return True  # default behave as monthly

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
            logging.error("Missing cookie value for cookie auth.")
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

# =============================================================
# Data Loading & Sanitization
# =============================================================

def load_csv_data(csv_file: str) -> Tuple[pd.DataFrame, bool]:
    try:
        df = pd.read_csv(csv_file)
        if 'DATE' in df.columns:
            df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
        return df, True
    except Exception as e:
        logging.error(f"Error loading CSV {csv_file}: {e}")
        return pd.DataFrame(), False

def sanitize_total_jobs(df: pd.DataFrame,
                        preferred_col: Optional[str] = None) -> pd.DataFrame:
    """
    Ensure we have a numeric TOTAL_JOBS column.
    - If preferred_col provided and exists, use it as source.
    - Else if TOTAL_JOBS exists, clean it.
    - Removes commas, spaces; coerces to numeric.
    """
    candidate = None
    if preferred_col and preferred_col in df.columns:
        candidate = preferred_col
    elif 'TOTAL_JOBS' in df.columns:
        candidate = 'TOTAL_JOBS'
    else:
        # Try to guess a column with large ints
        for c in df.columns:
            if re.search(r'jobs|count|total', c.lower()):
                candidate = c
                break

    if candidate is None:
        logging.error("No suitable column found for TOTAL_JOBS.")
        df['TOTAL_JOBS'] = pd.Series(dtype='float')
        return df

    # Create/overwrite TOTAL_JOBS from candidate
    cleaned = (
        df[candidate]
        .astype(str)
        .str.replace(',', '', regex=False)
        .str.replace(' ', '', regex=False)
        .str.strip()
    )
    df['TOTAL_JOBS'] = pd.to_numeric(cleaned, errors='coerce')

    return df

# =============================================================
# Peak Calculation
# =============================================================

def latest_year_month(daily_df: pd.DataFrame) -> Tuple[int, int]:
    latest = daily_df['DATE'].max()
    return latest.year, latest.month

def filter_to_month(daily_df: pd.DataFrame,
                    year: Optional[int],
                    month: Optional[int]) -> pd.DataFrame:
    if year is None or month is None:
        year, month = latest_year_month(daily_df)
    return daily_df[(daily_df.DATE.dt.year == year) & (daily_df.DATE.dt.month == month)].copy(), year, month

def compute_top_peaks(daily_df: pd.DataFrame,
                      top_n: int,
                      year: Optional[int],
                      month: Optional[int]) -> Tuple[pd.DataFrame, int, int]:
    month_df, y, m = filter_to_month(daily_df, year, month)
    if month_df.empty:
        return month_df, y, m
    try:
        peaks = month_df.nlargest(top_n, 'TOTAL_JOBS')
    except Exception:
        peaks = month_df.sort_values('TOTAL_JOBS', ascending=False).head(top_n)
    return peaks[['DATE', 'TOTAL_JOBS']], y, m

# =============================================================
# Debug Helpers
# =============================================================

def dump_debug_sample(daily_df: pd.DataFrame, peaks_df: pd.DataFrame, label: str):
    if os.environ.get('DEBUG_PEAKS') == '1':
        print(f"\n[DEBUG] ---- {label} DAILY TOP 10 (by TOTAL_JOBS) ----")
        print(daily_df.sort_values('TOTAL_JOBS', ascending=False).head(10))
        print(f"[DEBUG] ---- {label} SELECTED PEAKS ----")
        print(peaks_df.sort_values('TOTAL_JOBS', ascending=False))
        print("[DEBUG] ------------------------------------\n")

def debug_chart_data(peaks_df, baseline):
    """Print details about chart data to help with debugging"""
    print("===== DEBUG CHART DATA =====")
    print(f"Dates: {[d.strftime('%Y-%m-%d') for d in peaks_df['DATE']]}")
    print(f"Baseline values: {[baseline] * len(peaks_df)}")
    print(f"Total Jobs values: {[int(v) for v in peaks_df['TOTAL_JOBS']]}")
    print("============================")

# =============================================================
# Chart Builders (Transposed Peaks) - FIXED
# =============================================================

def build_transposed_table(dates: List[str],
                           series_rows: Dict[str, List[int]]) -> str:
    """
    Create table with dates as columns and each series as rows
    Fixed implementation to ensure Confluence properly displays all data series
    """
    # Create header row with dates
    header = "".join(f"<th>{d}</th>" for d in dates)
    
    # Build body rows - each series becomes a row
    body_rows = []
    for series_name, values in series_rows.items():
        cells = "".join(f"<td>{v}</td>" for v in values)
        body_rows.append(f"<tr><th>{series_name}</th>{cells}</tr>")
    
    # Complete table structure
    return f"<table><tbody><tr><th>Date</th>{header}</tr>{''.join(body_rows)}</tbody></table>"

def peaks_transposed_chart(peaks_df: pd.DataFrame,
                           baseline: int,
                           chart_title: str) -> str:
    """
    Create chart with transposed data (dates as columns, series as rows)
    Fixed to properly display both baseline and total jobs bars
    """
    if peaks_df.empty:
        return f"<p>No data for {chart_title}.</p>"
    
    # Sort by date to ensure consistent ordering
    peaks_df = peaks_df.sort_values('DATE')
    
    # Format dates and prepare values
    date_cols = [d.strftime('%Y-%m-%d') for d in peaks_df['DATE']]
    
    # Ensure all values are properly formatted as integers
    series_rows = {
        'Baseline': [int(baseline) for _ in range(len(peaks_df))],
        'Total Jobs': [int(float(v)) for v in peaks_df['TOTAL_JOBS']]
    }
    
    # Debug output if environment variable is set
    if os.environ.get('DEBUG_CHARTS') == '1':
        debug_chart_data(peaks_df, baseline)
    
    # Build the table HTML for the chart
    table_html = build_transposed_table(date_cols, series_rows)
    
    # Return complete chart macro with correct colors and settings
    return f"""
<ac:structured-macro ac:name="chart">
  <ac:parameter ac:name="title">{chart_title}</ac:parameter>
  <ac:parameter ac:name="type">bar</ac:parameter>
  <ac:parameter ac:name="orientation">vertical</ac:parameter>
  <ac:parameter ac:name="3D">true</ac:parameter>
  <ac:parameter ac:name="width">900</ac:parameter>
  <ac:parameter ac:name="height">480</ac:parameter>
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
    {table_html}
  </ac:rich-text-body>
</ac:structured-macro>
"""

def peaks_detail_table(peaks_df: pd.DataFrame,
                       baseline: int,
                       label: str) -> str:
    if peaks_df.empty:
        return ""
    ranked = peaks_df.sort_values('TOTAL_JOBS', ascending=False).reset_index(drop=True)
    rank_map = {row.DATE: idx + 1 for idx, row in ranked.iterrows()}
    display = peaks_df.sort_values('DATE').copy()
    display['Variation'] = baseline - display['TOTAL_JOBS']
    rows = ["<tr><th>Rank</th><th>Date</th><th>Total Jobs</th><th>Baseline</th><th>Variation</th></tr>"]
    for _, r in display.iterrows():
        variation = int(r['Variation'])
        style = 'style="background-color:#90EE90;"' if variation > 0 else 'style="background-color:#FFB6C1;"'
        rows.append(
            f"<tr><td>{rank_map[r['DATE']]}</td>"
            f"<td>{r['DATE'].strftime('%Y-%m-%d')}</td>"
            f"<td>{int(r['TOTAL_JOBS'])}</td>"
            f"<td>{baseline}</td>"
            f"<td {style}>{variation}</td></tr>"
        )
    return f"<h4>Top 4 Peaks Detail ({label})</h4><table class='wrapped'><tbody>{''.join(rows)}</tbody></table>"

# =============================================================
# Section Generators
# =============================================================

def generate_global_peaks_section(df: pd.DataFrame,
                                  baseline: int,
                                  top_n: int = 4) -> str:
    daily = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum()
    # Choose month (allow override)
    forced = os.environ.get('FORCE_MONTH')  # format YYYY-MM
    year = month = None
    if forced and re.match(r'^\d{4}-\d{2}$', forced):
        year, month = map(int, forced.split('-'))
    peaks, y, m = compute_top_peaks(daily, top_n, year, month)
    dump_debug_sample(daily, peaks, 'GLOBAL')
    if peaks.empty:
        return "<p>No peak data for current month.</p>"
    month_label = peaks['DATE'].dt.strftime('%b').iloc[0]
    chart = peaks_transposed_chart(peaks, baseline, f"Top {len(peaks)} Peaks (All Countries) of {month_label}")
    details = peaks_detail_table(peaks, baseline, f"All Countries {month_label}")
    return chart + details

def generate_country_peaks_section(df_country: pd.DataFrame,
                                   country: str,
                                   baseline: int,
                                   top_n: int = 4) -> str:
    daily = df_country.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum()
    peaks, y, m = compute_top_peaks(daily, top_n, None, None)
    dump_debug_sample(daily, peaks, country)
    if peaks.empty:
        return f"<p>No peak data for {country}.</p>"
    month_label = peaks['DATE'].dt.strftime('%b').iloc[0]
    chart = peaks_transposed_chart(peaks, baseline, f"{country}: Top {len(peaks)} Peaks of {month_label}")
    details = peaks_detail_table(peaks, baseline, f"{country} {month_label}")
    return chart + details

def generate_daily_total_single_series(df: pd.DataFrame,
                                       baseline: int,
                                       title: str = "Daily Total Jobs (All Countries)",
                                       show_percent: bool = False) -> str:
    """
    Single green bar per date (like original). Optionally show % labels.
    If DAILY_PERCENT=1 env var set, adds a second 'Percent' series (NOT bars) or uses
    value labels inside bars (simpler: embed text).
    For now we only display numeric values inside bars (not percentages) unless requested.
    """
    daily = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')
    if daily.empty:
        return "<p>No daily data.</p>"
    dates = [d.strftime('%Y-%m-%d') for d in daily['DATE']]
    totals = [int(v) for v in daily['TOTAL_JOBS']]
    percent_labels = []
    if show_percent:
        for v in totals:
            pct = (v / baseline) * 100 if baseline else 0
            percent_labels.append(f"{pct:.1f}%")
    # Build table (Date first col, only one series)
    rows = ["<tr><th>Date</th><th>Total Jobs</th></tr>"]
    for i, d in enumerate(dates):
        val = totals[i]
        rows.append(f"<tr><td>{d}</td><td>{val}</td></tr>")
    table_html = "<table><tbody>" + "".join(rows) + "</tbody></table>"
    macro = f"""
<h3>{title}</h3>
<ac:structured-macro ac:name="chart">
  <ac:parameter ac:name="title">{title}</ac:parameter>
  <ac:parameter ac:name="type">bar</ac:parameter>
  <ac:parameter ac:name="orientation">vertical</ac:parameter>
  <ac:parameter ac:name="3D">true</ac:parameter>
  <ac:parameter ac:name="width">1200</ac:parameter>
  <ac:parameter ac:name="height">520</ac:parameter>
  <ac:parameter ac:name="legend">false</ac:parameter>
  <ac:parameter ac:name="dataDisplay">false</ac:parameter>
  <ac:parameter ac:name="showValues">true</ac:parameter>
  <ac:parameter ac:name="valuePosition">inside</ac:parameter>
  <ac:parameter ac:name="displayValuesOnBars">true</ac:parameter>
  <ac:parameter ac:name="labelAngle">45</ac:parameter>
  <ac:parameter ac:name="xLabel">Date</ac:parameter>
  <ac:parameter ac:name="yLabel">Jobs</ac:parameter>
  <ac:parameter ac:name="colors">#6B8E23</ac:parameter>
  <ac:parameter ac:name="seriesColors">#6B8E23</ac:parameter>
  <ac:rich-text-body>
    {table_html}
  </ac:rich-text-body>
</ac:structured-macro>
"""
    return macro

def generate_variation_line(df: pd.DataFrame, baseline: int) -> str:
    daily = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')
    if daily.empty:
        return "<p>No variation data.</p>"
    rows = ["<tr><th>Date</th><th>Baseline</th><th>Total Jobs</th></tr>"]
    for _, r in daily.iterrows():
        ds = r['DATE'].strftime('%Y-%m-%d')
        rows.append(f"<tr><td>{ds}</td><td>{baseline}</td><td>{int(r['TOTAL_JOBS'])}</td></tr>")
    table_html = "<table><tbody>" + "".join(rows) + "</tbody></table>"
    return f"""
<h3>Baseline vs Daily Totals (Line)</h3>
<ac:structured-macro ac:name="chart">
  <ac:parameter ac:name="title">Baseline vs Daily Totals</ac:parameter>
  <ac:parameter ac:name="type">line</ac:parameter>
  <ac:parameter ac:name="width">1100</ac:parameter>
  <ac:parameter ac:name="height">450</ac:parameter>
  <ac:parameter ac:name="legend">true</ac:parameter>
  <ac:parameter ac:name="xLabel">Date</ac:parameter>
  <ac:parameter ac:name="yLabel">Jobs</ac:parameter>
  <ac:parameter ac:name="dataDisplay">false</ac:parameter>
  <ac:parameter ac:name="labelAngle">45</ac:parameter>
  <ac:rich-text-body>
    {table_html}
  </ac:rich-text-body>
</ac:structured-macro>
"""

def generate_daily_summary(df: pd.DataFrame) -> str:
    daily = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum()
    if daily.empty:
        return ""
    rows = ["<tr><th>Date</th><th>Total Jobs</th></tr>"]
    for _, r in daily.iterrows():
        rows.append(f"<tr><td>{r['DATE'].strftime('%Y-%m-%d')}</td><td>{int(r['TOTAL_JOBS'])}</td></tr>")
    return "<h3>Daily Totals Table</h3><table class='wrapped'><tbody>" + "".join(rows) + "</tbody></table>"

# =============================================================
# Confluence REST Helpers
# =============================================================

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
            payload["id"] = page_id
            payload["version"] = {"number": version + 1}
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
        return resp.status_code == 200
    except Exception as e:
        logging.error(f"Connection test exception: {e}")
        return False

# =============================================================
# Publish (Single)
# =============================================================

def publish_to_confluence(report_file='task_usage_report_by_region.csv',
                          test_mode=False,
                          skip_actual_upload: bool = False) -> bool:
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

        # Load + sanitize
        df, ok = load_csv_data(report_file)
        if not ok:
            return False
        df = sanitize_total_jobs(df, preferred_col=config.get('JOB_COLUMN') or os.environ.get('JOB_COLUMN'))

        session = None
        if not skip_actual_upload:
            session = create_session(config)
            if not session:
                return False
            if not test_connection(session, config):
                logging.error("Connection test failed.")
                return False

        peaks_section = generate_global_peaks_section(df, baseline) if monthly else "<!-- Monthly peaks suppressed (daily run) -->"
        daily_single = generate_daily_total_single_series(
            df,
            baseline,
            title="Daily Total Jobs (All Countries)",
            show_percent=os.environ.get('DAILY_PERCENT') == '1'
        )
        variation_line = generate_variation_line(df, baseline)
        summary = generate_daily_summary(df)

        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        user = 'satish537'
        content = f"""
<h1>Task Usage Report{' - TEST DATA' if test_mode else ''}</h1>
<p><strong>Last updated:</strong> {timestamp}</p>
<p><strong>Generated by:</strong> {user}</p>
{peaks_section}
{daily_single}
{variation_line}
{summary}
<hr />
<p><em>Note: This report shows the task usage data{' (TEST MODE)' if test_mode else ''}.</em></p>
"""

        if skip_actual_upload:
            logging.info("Simulation only (single).")
            return True

        page_id, version = get_page_info(session, config)
        return create_or_update_page(session, config, content, page_id, version)

    except Exception as e:
        logging.error(f"Error in single publish: {e}")
        return False

# =============================================================
# Publish (Multi-Country)
# =============================================================

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
        pref_col = config.get('JOB_COLUMN') or os.environ.get('JOB_COLUMN')
        for ctry, path in report_files:
            if not os.path.exists(path):
                logging.warning(f"Missing report for {ctry}: {path}")
                continue
            df = pd.read_csv(path)
            if 'DATE' in df.columns:
                df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
            df = sanitize_total_jobs(df, preferred_col=pref_col)
            df['COUNTRY'] = ctry
            frames.append(df)
            per_country.append((ctry, df))

        if not frames:
            logging.error("No country data to publish.")
            return False

        all_df = pd.concat(frames, ignore_index=True)

        session = None
        if not skip_actual_upload:
            session = create_session(config)
            if not session:
                return False
            if not test_connection(session, config):
                logging.error("Connection test failed.")
                return False

        global_peaks = generate_global_peaks_section(all_df, baseline) if monthly else "<!-- Monthly peaks suppressed (daily run) -->"
        global_daily_single = generate_daily_total_single_series(
            all_df,
            baseline,
            title="Daily Total Jobs (All Countries)",
            show_percent=os.environ.get('DAILY_PERCENT') == '1'
        )
        global_variation = generate_variation_line(all_df, baseline)

        per_country_sections = []
        if monthly:
            for ctry, df_ctry in per_country:
                per_country_sections.append(f"<h3>{ctry}</h3>")
                per_country_sections.append(generate_country_peaks_section(df_ctry, ctry, baseline))

        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        user = 'satish537'
        content = f"""
<h1>Multi-Country Task Usage Report{' - TEST DATA' if test_mode else ''}</h1>
<p><strong>Last updated:</strong> {timestamp}</p>
<p><strong>Generated by:</strong> {user}</p>
{global_peaks}
{global_daily_single}
{global_variation}
{'<hr /><h2>Per-Country Peaks</h2>' + ''.join(per_country_sections) if per_country_sections else ''}
<hr />
<p><em>Note: This report shows the task usage data{' (TEST MODE)' if test_mode else ''}.</em></p>
"""

        if skip_actual_upload:
            logging.info("Simulation only (multi-country).")
            return True

        page_id, version = get_page_info(session, config)
        return create_or_update_page(session, config, content, page_id, version)

    except Exception as e:
        logging.error(f"Error in multi-country publish: {e}")
        return False

# =============================================================
# CLI Harness
# =============================================================

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    p = argparse.ArgumentParser(description="Confluence Publisher (Single / Multi)")
    p.add_argument("--file", help="Single report CSV to publish")
    p.add_argument("--multi", nargs="*", help="List of COUNTRY=path/to/file.csv")
    p.add_argument("--simulate", action="store_true", help="Skip actual upload (simulation)")
    p.add_argument("--test", action="store_true", help="Test mode (marks page title)")
    p.add_argument("--debug", action="store_true", help="Enable debug output for charts")
    args = p.parse_args()

    if args.debug:
        os.environ['DEBUG_CHARTS'] = '1'
    
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
