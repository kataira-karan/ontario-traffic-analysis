import os

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

cursor = None

try:
    cursor = conn.cursor()

    # -------------------------------------------------
    # 1. Get latest validation result
    # -------------------------------------------------

    cursor.execute("""
        SELECT IS_VALID
        FROM ONTARIO_TRAFFIC.PUBLIC.PIPELINE_VALIDATION
        ORDER BY RUN_TIMESTAMP DESC
        LIMIT 1;
    """)

    result = cursor.fetchone()

    if result is None:
        raise Exception(
            "No validation record found. MERGE cancelled."
        )

    is_valid = result[0]

    print("Latest validation result:", is_valid)

    # -------------------------------------------------
    # 2. Stop pipeline if validation failed
    # -------------------------------------------------

    if not is_valid:
        raise Exception(
            "Validation failed. MERGE cancelled."
        )

    # -------------------------------------------------
    # 3. Validation passed - run MERGE
    # -------------------------------------------------

    print("Validation passed. Starting MERGE...")

    cursor.execute("""
        CALL ONTARIO_TRAFFIC.PUBLIC.MERGE_TRAFFIC_EVENTS();
    """)

    merge_result = cursor.fetchone()

    print("Snowflake MERGE completed successfully.")

    if merge_result:
        print("Procedure result:", merge_result[0])

finally:
    if cursor is not None:
        cursor.close()

    conn.close()