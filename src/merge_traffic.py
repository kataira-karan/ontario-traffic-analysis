import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv


load_dotenv()


# -------------------------------------------------
# Snowflake connection
# -------------------------------------------------

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


    # =================================================
    # 1. READ VALIDATION SQL FILE
    # =================================================

    project_root = Path(__file__).resolve().parent.parent
    validation_file = project_root / "sql" / "validation.sql"

    validation_sql = validation_file.read_text(encoding="utf-8")


    # =================================================
    # 2. RUN VALIDATION
    # =================================================

    print("Running data validation...")

    cursor.execute(validation_sql)

    print("Validation checks completed.")


    # =================================================
    # 3. GET VALIDATION RESULT FROM THIS RUN
    # =================================================

    cursor.execute("""
        SELECT
            RUN_ID,
            STAGGING_ROW_COUNT,
            NULL_COUNT,
            DUPLICATE_EVENT_COUNT,
            NEW_EVENT_COUNT,
            CLOSED_EVENT_COUNT,
            IS_VALID
        FROM ONTARIO_TRAFFIC.PUBLIC.PIPELINE_VALIDATION
        WHERE RUN_ID = (
            SELECT RUN_ID
            FROM ONTARIO_TRAFFIC.PUBLIC.PIPELINE_VALIDATION
            ORDER BY RUN_TIMESTAMP DESC
            LIMIT 1
        );
    """)

    result = cursor.fetchone()


    if result is None:
        raise Exception(
            "Validation record was not created. Pipeline cancelled."
        )


    (
        run_id,
        row_count,
        null_count,
        duplicate_count,
        new_events,
        closed_events,
        is_valid,
    ) = result


    # =================================================
    # 4. PRINT VALIDATION REPORT
    # =================================================

    print("\n--- VALIDATION REPORT ---")
    print("Run ID:", run_id)
    print("Staging rows:", row_count)
    print("Null EVENT_IDs:", null_count)
    print("Duplicate EVENT_IDs:", duplicate_count)
    print("New events:", new_events)
    print("Closed events:", closed_events)
    print("Valid:", is_valid)
    print("-------------------------\n")


    # =================================================
    # 5. VALIDATION GATE
    # =================================================

    if not is_valid:

        raise Exception(
            "Data validation FAILED. "
            "MERGE_TRAFFIC_EVENTS will not run."
        )


    # =================================================
    # 6. VALIDATION PASSED
    # =================================================

    print("Validation PASSED.")
    print("Starting Snowflake MERGE...")


    # =================================================
    # 7. RUN MERGE PROCEDURE
    # =================================================

    cursor.execute("""
        CALL ONTARIO_TRAFFIC.PUBLIC.MERGE_TRAFFIC_EVENTS();
    """)

    merge_result = cursor.fetchone()


    print("Snowflake MERGE completed successfully.")

    if merge_result:
        print("Procedure result:", merge_result[0])


except Exception as e:

    print("\nPIPELINE FAILED")
    print("Reason:", e)

    raise


finally:

    if cursor is not None:
        cursor.close()

    conn.close()

    print("Snowflake connection closed.")