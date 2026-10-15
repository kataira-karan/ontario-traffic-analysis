import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()
files = sorted(Path("data/cleaned").glob("ontario_*.csv"))
if not files:
    raise FileNotFoundError("No traffic CSV found")

df = pd.read_csv(files[-1])

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

try:
    columns = ["ID", "SourceId", "Organization", "RoadwayName", "DirectionOfTravel", "Description", "Reported", "LastUpdated", "StartDate", "PlannedEndDate", "LanesAffected", "Latitude", "Longitude", "LatitudeSecondary", "LongitudeSecondary", "EventType", "IsFullClosure", "Severity", "EncodedPolyline", "LinkId", "Impact", "ReportedLocal", "ReportedDate", "ReportedHour", "ReportedDay", "ReportedMonth", "ReportedYear"]
    upload_df = df[columns].copy()
    upload_df.columns = ["EVENT_ID", "SOURCE_ID", "ORGANIZATION", "ROADWAY_NAME", "DIRECTION_OF_TRAVEL", "DESCRIPTION", "REPORTED", "LAST_UPDATED", "START_DATE", "PLANNED_END_DATE", "LANES_AFFECTED", "LATITUDE", "LONGITUDE", "LATITUDE_SECONDARY", "LONGITUDE_SECONDARY", "EVENT_TYPE", "IS_FULL_CLOSURE", "SEVERITY", "ENCODED_POLYLINE", "LINK_ID", "IMPACT", "REPORTED_LOCAL", "REPORTED_DATE", "REPORTED_HOUR", "REPORTED_DAY", "REPORTED_MONTH", "REPORTED_YEAR"]

    success, num_chunks, num_rows, output = write_pandas(
        conn, upload_df,
        table_name="TRAFFIC_EVENTS_STAGING",
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        auto_create_table=False,
        overwrite=True,
        quote_identifiers=False,
    )
    print("Upload successful:", success)
    print("Rows uploaded:", num_rows)
    print("Chunks uploaded:", num_chunks)
finally:
    conn.close()
