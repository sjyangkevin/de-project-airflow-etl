SELECT
    net,
    COUNT(DISTINCT id) AS event_count
FROM minio.default.launch_events
GROUP BY net;