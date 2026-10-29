INSERT INTO ONTARIO_TRAFFIC.PUBLIC.PIPELINE_VALIDATION
(
    RUN_ID,
    RUN_TIMESTAMP,
    STAGGING_ROW_COUNT,
    NULL_COUNT,
    DUPLICATE_EVENT_COUNT,
    NEW_EVENT_COUNT,
    CLOSED_EVENT_COUNT,
    IS_VALID
)

WITH VALIDATION_METRICS AS
(
    SELECT

        /* Total staging rows */
        (
            SELECT COUNT(*)
            FROM ONTARIO_TRAFFIC.PUBLIC.TRAFFIC_EVENTS_STAGING
        ) AS ROW_COUNT,


        /* NULL EVENT_IDs */
        (
            SELECT COUNT(*)
            FROM ONTARIO_TRAFFIC.PUBLIC.TRAFFIC_EVENTS_STAGING
            WHERE EVENT_ID IS NULL
        ) AS NULL_COUNT,


        /* Number of duplicated EVENT_IDs */
        (
            SELECT COUNT(*)
            FROM
            (
                SELECT EVENT_ID
                FROM ONTARIO_TRAFFIC.PUBLIC.TRAFFIC_EVENTS_STAGING
                WHERE EVENT_ID IS NOT NULL
                GROUP BY EVENT_ID
                HAVING COUNT(*) > 1
            )
        ) AS DUPLICATE_COUNT,


        /* New events */
        (
            SELECT COUNT(*)
            FROM ONTARIO_TRAFFIC.PUBLIC.TRAFFIC_EVENTS_STAGING ST
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM ONTARIO_TRAFFIC.PUBLIC.TRAFFIC_EVENTS T
                WHERE T.EVENT_ID = ST.EVENT_ID
            )
        ) AS NEW_EVENTS,


        /* Currently active events missing from staging */
        (
            SELECT COUNT(*)
            FROM ONTARIO_TRAFFIC.PUBLIC.TRAFFIC_EVENTS T
            WHERE T.IS_ACTIVE = TRUE
              AND NOT EXISTS
              (
                  SELECT 1
                  FROM ONTARIO_TRAFFIC.PUBLIC.TRAFFIC_EVENTS_STAGING ST
                  WHERE ST.EVENT_ID = T.EVENT_ID
              )
        ) AS CLOSED_EVENTS
)

SELECT
    UUID_STRING(),
    CURRENT_TIMESTAMP(),
    ROW_COUNT,
    NULL_COUNT,
    DUPLICATE_COUNT,
    NEW_EVENTS,
    CLOSED_EVENTS,

    CASE
        WHEN ROW_COUNT > 0
         AND NULL_COUNT = 0
         AND DUPLICATE_COUNT = 0
        THEN TRUE
        ELSE FALSE
    END

FROM VALIDATION_METRICS;