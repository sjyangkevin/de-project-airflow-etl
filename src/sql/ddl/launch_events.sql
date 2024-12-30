CREATE TABLE IF NOT EXISTS hive.default.launch_events (
    id VARCHAR COMMENT 'Unique identifier for the event',
    url VARCHAR COMMENT 'URL associated with the event',
    name VARCHAR COMMENT 'Name of the event',
    status VARCHAR COMMENT 'Current status of the event',
    image_url VARCHAR COMMENT 'URL of the event image',
    license VARCHAR COMMENT 'License information',
    net DATE COMMENT 'Net date for the event'
)
WITH (
    external_location = 's3://datalake/reports/launch',
    format = 'PARQUET',
    partitioned_by = ARRAY['net']
);