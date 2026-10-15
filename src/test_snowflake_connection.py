import os

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
conn = None
try:
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_USER()")
    print("Connected successfully!")
    print(cursor.fetchone())
    cursor.close()
except Exception as error:
    print("Connection failed:", error)
finally:
    if conn is not None:
        conn.close()
