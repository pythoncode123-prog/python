SELECT TO_CHAR(net_report.net_date, 'YYYY-MM-DD') AS NET_DATE,
       SUM(net_report_data.jobs) AS TOTAL_JOBS  -- FIXED!
FROM <EM_schema>.net_report, <EM_schema>.net_report_data
WHERE net_report.report_id=net_report_data.report_id
  AND net_report_data.fname='NODE_ID'
  AND net_report.net_date BETWEEN TO_DATE('2024-09-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
                               AND TO_DATE('2024-09-30 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
GROUP BY TO_CHAR(net_report.net_date, 'YYYY-MM-DD')
ORDER BY NET_DATE;
