SELECT
    net,
    COUNT(DISTINCT id) AS event_count
FROM hive.default.launch_events
GROUP BY net;