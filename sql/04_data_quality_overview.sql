-- Quick look at why events were rejected and which orders carry DQ flags.
SELECT 'rejected: ' || reason AS check_name, COUNT(*) AS n
FROM rejected_events GROUP BY reason
UNION ALL
SELECT 'event: ' || timestamp_issue, COUNT(*)
FROM fct_order_events WHERE timestamp_issue IS NOT NULL GROUP BY timestamp_issue
UNION ALL
SELECT 'line: ' || line_issue, COUNT(*)
FROM fct_order_items WHERE line_issue IS NOT NULL GROUP BY line_issue
UNION ALL
SELECT 'order: missing_created_event', COUNT(*) FROM dim_orders WHERE missing_created_event
ORDER BY check_name;
