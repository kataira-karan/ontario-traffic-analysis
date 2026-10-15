-- Ontario Traffic Analysis - Snowflake data-quality validation

-- 1. Current staging row count
SELECT COUNT(*) AS staging_row_count
FROM PUBLIC.TRAFFIC_EVENTS_STAGING;

-- 2. Master table row count
SELECT COUNT(*) AS master_row_count
FROM PUBLIC.TRAFFIC_EVENTS;

-- 3. Null event IDs: expected result = 0
SELECT COUNT(*) AS null_event_ids
FROM PUBLIC.TRAFFIC_EVENTS_STAGING
WHERE EVENT_ID IS NULL;

-- 4. Duplicate event IDs: expected result = no rows
SELECT EVENT_ID, COUNT(*) AS duplicate_count
FROM PUBLIC.TRAFFIC_EVENTS_STAGING
GROUP BY EVENT_ID
HAVING COUNT(*) > 1;

-- 5. New events in the latest staging snapshot
SELECT COUNT(*) AS new_events
FROM PUBLIC.TRAFFIC_EVENTS_STAGING s
WHERE NOT EXISTS (
    SELECT 1
    FROM PUBLIC.TRAFFIC_EVENTS t
    WHERE t.EVENT_ID = s.EVENT_ID
);

-- 6. Events in master that disappeared from the latest snapshot
SELECT COUNT(*) AS disappeared_events
FROM PUBLIC.TRAFFIC_EVENTS t
WHERE NOT EXISTS (
    SELECT 1
    FROM PUBLIC.TRAFFIC_EVENTS_STAGING s
    WHERE s.EVENT_ID = t.EVENT_ID
);

-- 7. Inspect disappeared events
SELECT t.*
FROM PUBLIC.TRAFFIC_EVENTS t
WHERE NOT EXISTS (
    SELECT 1
    FROM PUBLIC.TRAFFIC_EVENTS_STAGING s
    WHERE s.EVENT_ID = t.EVENT_ID
)
ORDER BY t.LAST_UPDATED DESC;

-- 8. Active vs inactive event counts (run after IS_ACTIVE is available)
SELECT IS_ACTIVE, COUNT(*) AS event_count
FROM PUBLIC.TRAFFIC_EVENTS
GROUP BY IS_ACTIVE
ORDER BY IS_ACTIVE DESC;
