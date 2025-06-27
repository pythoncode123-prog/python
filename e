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
