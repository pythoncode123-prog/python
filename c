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

# -----------------------------------------------
# Confluence Publisher (Refined)
# - Correct table orientation for Confluence Chart macro
# - Global (all countries) monthly Top 4 Peaks
# - Per-country optional peaks
# - Daily Totals (all countries) bar chart (2 series only)
# -----------------------------------------------

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MONTH_NAMES = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
)

# ------------- Run Mode Detection ---------------

def _is_monthly_run(config: Dict) -> bool:
    title = config.get('PAGE_TITLE', '')
    if title.endswith('_daily'):
        return False
    if re.search(r'\s-\s(' + '|'.join(MONTH_NAMES) + r')$', title):
        return True
    return True  # default to monthly behavior

# ------------- Config / Auth ---------------------

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

# ------------- Data Loading ----------------------

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

# ------------- Peak Computation Utilities --------

def _latest_year_month(daily_df: pd.DataFrame) -> Tuple[int, int]:
    latest = daily_df['DATE'].max()
    return latest.year, latest.month

def compute_monthly_top_peaks(df: pd.DataFrame,
                              top_n: int,
                              year: Optional[int],
                              month: Optional[int]) -> pd.DataFrame:
    """
    Returns a DataFrame with columns: DATE, TOTAL_JOBS (aggregated),
    limited to the specified month & top_n days by TOTAL_JOBS.
    """
    subset = df[['DATE', 'TOTAL_JOBS']].dropna()
    daily = subset.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum()
    if year is None or month is None:
        year, month = _latest_year_month(daily)
    month_df = daily[(daily.DATE.dt.year == year) & (daily.DATE.dt.month == month)]
    if month_df.empty:
        return pd.DataFrame(columns=['DATE', 'TOTAL_JOBS'])
    try:
        peaks = month_df.nlargest(top_n, 'TOTAL_JOBS')
    except Exception:
        peaks = month_df.sort_values('TOTAL_JOBS', ascending=False).head(top_n)
    return peaks

# ------------- Chart Generators ------------------

def generate_top4_peaks_chart(peaks_df: pd.DataFrame,
                              baseline: int,
                              title: str) -> str:
    """
    Build the actual bar chart macro for Top 4 peaks.
    Table structure EXACTLY:
      Date | Baseline | Total Jobs
      (one row per date)
    ONLY the table in the rich-text-body to avoid orientation issues.
    """
    if peaks_df.empty:
        return f"<p>No peak data for {title}.</p>"
    peaks_df = peaks_df.sort_values('DATE')
    rows = ["<tr><th>Date</th><th>Baseline</th><th>Total Jobs</th></tr>"]
    for _, r in peaks_df.iterrows():
        d = r['DATE'].strftime('%Y-%m-%d')
        rows.append(f"<tr><td>{d}</td><td>{baseline}</td><td>{int(r['TOTAL_JOBS'])}</td></tr>")
    table_html = "<table><tbody>" + "".join(rows) + "</tbody></table>"
    return f"""
<ac:structured-macro ac:name="chart">
  <ac:parameter ac:name="title">{title}</ac:parameter>
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
  <ac:parameter ac:name="colors">#B22222,#6B8E23</ac:parameter>
  <ac:parameter ac:name="seriesColors">#B22222,#6B8E23</ac:parameter>
  <ac:parameter ac:name="labelAngle">45</ac:parameter>
  <ac:parameter ac:name="xLabel">Date</ac:parameter>
  <ac:parameter ac:name="yLabel">Jobs</ac:parameter>
  <ac:rich-text-body>
    {table_html}
  </ac:rich-text-body>
</ac:structured-macro>
"""

def generate_peaks_detail_table(peaks_df: pd.DataFrame,
                                baseline: int,
                                month_label: str) -> str:
    if peaks_df.empty:
        return ""
    # Rank by TOTAL_JOBS desc
    ranked = peaks_df.sort_values('TOTAL_JOBS', ascending=False).reset_index(drop=True)
    rank_map = {row.DATE: idx + 1 for idx, row in ranked.iterrows()}
    display = peaks_df.sort_values('DATE').copy()
    display['Variation'] = baseline - display['TOTAL_JOBS']
    rows = ["<tr><th>Rank</th><th>Date</th><th>Total Jobs</th><th>Baseline</th><th>Variation</th></tr>"]
    for _, r in display.iterrows():
        d = r['DATE'].strftime('%Y-%m-%d')
        variation = int(r['Variation'])
        style = 'style="background-color:#90EE90;"' if variation > 0 else 'style="background-color:#FFB6C1;"'
        rows.append(
            f"<tr><td>{rank_map[r['DATE']]}</td><td>{d}</td>"
            f"<td>{int(r['TOTAL_JOBS'])}</td><td>{baseline}</td>"
            f"<td {style}>{variation}</td></tr>"
        )
    table_html = "<table class='wrapped'><tbody>" + "".join(rows) + "</tbody></table>"
    return f"<h4>Top 4 Peaks Detail ({month_label})</h4>{table_html}"

def generate_global_monthly_top4_peaks(df: pd.DataFrame,
                                       baseline: int,
                                       top_n: int = 4) -> str:
    if 'DATE' not in df.columns or 'TOTAL_JOBS' not in df.columns:
        return "<p>No columns for global peaks.</p>"
    daily = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum()
    if daily.empty:
        return "<p>No data for global peaks.</p>"
    peaks = compute_monthly_top_peaks(daily, top_n=top_n, year=None, month=None)
    if peaks.empty:
        return "<p>No peak data for month.</p>"
    month_label = peaks['DATE'].dt.strftime('%b').iloc[0]
    chart = generate_top4_peaks_chart(peaks, baseline, f"Top {len(peaks)} Peaks (All Countries) of {month_label}")
    details = generate_peaks_detail_table(peaks, baseline, f"{month_label} (All Countries)")
    return chart + details

def generate_country_monthly_top4_peaks(df_country: pd.DataFrame,
                                        country: str,
                                        baseline: int,
                                        top_n: int = 4) -> str:
    peaks = compute_monthly_top_peaks(df_country, top_n=top_n, year=None, month=None)
    if peaks.empty:
        return f"<p>No peak data for {country}.</p>"
    month_label = peaks['DATE'].dt.strftime('%b').iloc[0]
    chart = generate_top4_peaks_chart(peaks, baseline, f"{country}: Top {len(peaks)} Peaks of {month_label}")
    details = generate_peaks_detail_table(peaks, baseline, f"{country} {month_label}")
    return chart + details

def generate_daily_total_all_countries_bar(df: pd.DataFrame, baseline: int) -> str:
    """
    Corrected daily total chart: categories=dates, TWO series (Baseline, Total Jobs).
    """
    if 'DATE' not in df.columns or 'TOTAL_JOBS' not in df.columns:
        return "<p>Missing DATE or TOTAL_JOBS for daily totals.</p>"
    daily = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')
    if daily.empty:
        return "<p>No daily totals data.</p>"
    rows = ["<tr><th>Date</th><th>Baseline</th><th>Total Jobs</th></tr>"]
    for _, r in daily.iterrows():
        ds = r['DATE'].strftime('%Y-%m-%d')
        rows.append(f"<tr><td>{ds}</td><td>{baseline}</td><td>{int(r['TOTAL_JOBS'])}</td></tr>")
    table_html = "<table><tbody>" + "".join(rows) + "</tbody></table>"
    return f"""
<h3>Daily Total Jobs (All Countries Combined)</h3>
<ac:structured-macro ac:name="chart">
  <ac:parameter ac:name="title">Daily Total Jobs (All Countries)</ac:parameter>
  <ac:parameter ac:name="type">bar</ac:parameter>
  <ac:parameter ac:name="width">1100</ac:parameter>
  <ac:parameter ac:name="height">500</ac:parameter>
  <ac:parameter ac:name="legend">true</ac:parameter>
  <ac:parameter ac:name="dataDisplay">false</ac:parameter>
  <ac:parameter ac:name="showValues">false</ac:parameter>
  <ac:parameter ac:name="labelAngle">45</ac:parameter>
  <ac:parameter ac:name="colors">#B22222,#2E8B57</ac:parameter>
  <ac:parameter ac:name="seriesColors">#B22222,#2E8B57</ac:parameter>
  <ac:parameter ac:name="xLabel">Date</ac:parameter>
  <ac:parameter ac:name="yLabel">Jobs</ac:parameter>
  <ac:rich-text-body>
    {table_html}
  </ac:rich-text-body>
</ac:structured-macro>
"""

def generate_variation_with_baseline_line(df: pd.DataFrame, baseline: int) -> str:
    if 'DATE' not in df.columns or 'TOTAL_JOBS' not in df.columns:
        return "<p>Missing DATE or TOTAL_JOBS for variation chart.</p>"
    daily = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum().sort_values('DATE')
    if daily.empty:
        return "<p>No data for variation.</p>"
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
  <ac:parameter ac:name="dataDisplay">false</ac:parameter>
  <ac:parameter ac:name="xLabel">Date</ac:parameter>
  <ac:parameter ac:name="yLabel">Jobs</ac:parameter>
  <ac:parameter ac:name="labelAngle">45</ac:parameter>
  <ac:rich-text-body>
    {table_html}
  </ac:rich-text-body>
</ac:structured-macro>
"""

def generate_daily_summary_table(df: pd.DataFrame) -> str:
    if 'DATE' not in df.columns or 'TOTAL_JOBS' not in df.columns:
        return ""
    daily = df.groupby('DATE', as_index=False)['TOTAL_JOBS'].sum()
    daily['DS'] = daily['DATE'].dt.strftime('%Y-%m-%d')
    rows = ["<tr><th>Date</th><th>Total Jobs</th></tr>"]
    for _, r in daily.iterrows():
        rows.append(f"<tr><td>{r['DS']}</td><td>{int(r['TOTAL_JOBS'])}</td></tr>")
    return "<h3>Daily Totals Table</h3><table class='wrapped'><tbody>" + "".join(rows) + "</tbody></table>"

# (Optional) Region / Country detail charts if needed later (omitted for brevity)
# Provide them again if you need the line-by-region or pie charts.

# ------------- Confluence API --------------------

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

# ------------- Single Publish --------------------

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

        session = None
        if not skip_actual_upload:
            session = create_session(config)
            if not session:
                return False
            if not test_connection(session, config):
                logging.error("Connection test failed.")
                return False

        df, ok = load_csv_data(report_file)
        if not ok:
            return False

        # Sections
        global_peaks = generate_global_monthly_top4_peaks(df, baseline) if monthly else "<!-- Monthly peaks suppressed (daily run) -->"
        daily_totals = generate_daily_total_all_countries_bar(df, baseline)
        variation_line = generate_variation_with_baseline_line(df, baseline)
        daily_table = generate_daily_summary_table(df)

        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        user = 'satish537'
        content = f"""
<h1>Task Usage Report{' - TEST DATA' if test_mode else ''}</h1>
<p><strong>Last updated:</strong> {timestamp}</p>
<p><strong>Generated by:</strong> {user}</p>
{global_peaks}
{daily_totals}
{variation_line}
{daily_table}
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

# ------------- Multi-Country Publish --------------

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

        # Global sections
        global_peaks = generate_global_monthly_top4_peaks(all_df, baseline) if monthly else "<!-- Monthly peaks suppressed (daily run) -->"
        global_daily_totals = generate_daily_total_all_countries_bar(all_df, baseline)
        global_variation_line = generate_variation_with_baseline_line(all_df, baseline)

        # Per-country peaks (only if monthly)
        per_country_sections = []
        if monthly:
            for ctry, df_ctry in per_country:
                per_country_sections.append(f"<h3>{ctry}</h3>")
                per_country_sections.append(generate_country_monthly_top4_peaks(df_ctry, ctry, baseline))

        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        user = 'satish537'
        content = f"""
<h1>Multi-Country Task Usage Report{' - TEST DATA' if test_mode else ''}</h1>
<p><strong>Last updated:</strong> {timestamp}</p>
<p><strong>Generated by:</strong> {user}</p>
{global_peaks}
{global_daily_totals}
{global_variation_line}
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

# ------------- CLI Harness -----------------------

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
