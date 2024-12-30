USE hive.default;
CALL system.sync_partition_metadata('default', 'launch_events', 'ADD');