import pyodbc
import csv
import configparser
import logging
from datetime import datetime
import os
import oracledb

def load_config(config_path):
    """Load database configuration from file."""
    config = configparser.ConfigParser()
    config.read(config_path)
    db_config = config['DATABASE']
    return {
        'host': db_config['DBHost'],
        'port': db_config['Port'],
        'service': db_config['ServiceName'],
        'schema': db_config['SchemaName'],
        'user': db_config['Username'],
        'password': db_config['Password']
    }

def connect_oracle(cfg):
    """Connect to Oracle database using configuration."""
    dsn = oracledb.makedsn(
        cfg['host'],
        int(cfg['port']),
        service_name=cfg['service']
    )
    return oracledb.connect(
        user=cfg['user'],
        password=cfg['password'],
        dsn=dsn
    )

def execute_query_and_write_csv(conn, query, csv_path):
    """Execute query and write results to CSV file."""
    cursor = conn.cursor()
    # Split the query on ';' and remove empty statements
    statements = [stmt.strip() for stmt in query.split(';') if stmt.strip()]
    # Execute all but the last statement (setup statements)
    for stmt in statements[:-1]:
        cursor.execute(stmt)
    # Execute the last statement (the SELECT)
    cursor.execute(statements[-1])
    columns = [column[0] for column in cursor.description]
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        for row in cursor:
            writer.writerow(row)
    cursor.close()
    logging.info(f"SQL query results exported to {csv_path}")

def sql_to_csv(config_file, query_file, output_csv, execution_timestamp=None):
    """Execute SQL and export to CSV."""
    logging.info("Starting SQL to CSV process")
    try:
        # Check query file exists
        if not os.path.exists(query_file):
            logging.error(f"Query file '{query_file}' not found")
            return False

        cfg = load_config(config_file)
        with open(query_file, 'r', encoding='utf-8') as f:
            query = f.read()
        query = query.replace('<EM_schema>', cfg['schema'])
        
        conn = connect_oracle(cfg)
        execute_query_and_write_csv(conn, query, output_csv)
        conn.close()
        logging.info("SQL to CSV process completed successfully")
        return True
    except Exception as e:
        logging.error(f"Error in SQL to CSV process: {str(e)}")
        return False

SELECT TO_CHAR(net_report.net_date, 'YYYY-MM-DD HH24:MI:SS') AS NET_DATE,
       net_report.ctm_host_name,
       net_report_data.fvalue,
       net_report_data.job_id
FROM <EM_schema>.net_report,
     <EM_schema>.net_report_data
WHERE net_report.report_id=net_report_data.report_id
  AND net_report_data.fname='NODE_ID'
  AND net_report.net_date BETWEEN '2024-09-01 00:00:00' AND '2024-09-30 23:59:00'
ORDER BY net_report.net_date;
