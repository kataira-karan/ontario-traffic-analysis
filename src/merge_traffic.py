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
    cursor.execute("CALL ONTARIO_TRAFFIC.PUBLIC.MERGE_TRAFFIC_EVENTS();")
    result = cursor.fetchone()

    print("Snowflake MERGE completed successfully.")
    if result:
        print("Procedure result:", result[0])
finally:
    if cursor is not None:
        cursor.close()
    conn.close()
