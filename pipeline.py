import os
from datetime import datetime, timezone

import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()
API_URL = "https://511on.ca/api/v2/get/event"

RAW_COLUMNS = ["ID", "SourceId", "Organization", "RoadwayName", "DirectionOfTravel", "Description", "Reported", "LastUpdated", "StartDate", "PlannedEndDate", "LanesAffected", "Latitude", "Longitude", "LatitudeSecondary", "LongitudeSecondary", "EventType", "IsFullClosure", "Severity", "EncodedPolyline", "LinkId", "Impact"]
STAGING_COLUMNS = ["EVENT_ID", "SOURCE_ID", "ORGANIZATION", "ROADWAY_NAME", "DIRECTION_OF_TRAVEL", "DESCRIPTION", "REPORTED", "LAST_UPDATED", "START_DATE", "PLANNED_END_DATE", "LANES_AFFECTED", "LATITUDE", "LONGITUDE", "LATITUDE_SECONDARY", "LONGITUDE_SECONDARY", "EVENT_TYPE", "IS_FULL_CLOSURE", "SEVERITY", "ENCODED_POLYLINE", "LINK_ID", "IMPACT", "REPORTED_LOCAL", "REPORTED_DATE", "REPORTED_HOUR", "REPORTED_DAY", "REPORTED_MONTH", "REPORTED_YEAR"]
MASTER_COLUMNS = ["EVENT_ID", "ROADWAY_NAME", "DIRECTION_OF_TRAVEL", "DESCRIPTION", "REPORTED_LOCAL", "EVENT_TYPE", "SEVERITY", "IS_FULL_CLOSURE", "LANES_AFFECTED", "LATITUDE", "LONGITUDE", "IMPACT", "REPORTED_DATE", "REPORTED_HOUR", "REPORTED_DAY", "REPORTED_MONTH", "REPORTED_YEAR", "LAST_UPDATED"]


def fetch_events():
    print("Fetching latest Ontario 511 data...")
    response = requests.get(API_URL, params={"key": os.environ["ONTARIO_511_API_KEY"], "format": "json"}, timeout=60)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        raise ValueError("Unexpected API response format")
    if not data:
        raise ValueError("API returned no events; stopping safely")
    df = pd.json_normalize(data)
    required = ["ID", "Reported", "LastUpdated", "EventType"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing API columns: {missing}")
    print(f"Fetched {len(df)} events")
    return df


def clean_events(df):
    print("Cleaning traffic data...")
    df = df.copy()
    for col in RAW_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[RAW_COLUMNS].copy()
    df["ID"] = pd.to_numeric(df["ID"], errors="raise")
    if df["ID"].isna().any():
        raise ValueError("Missing event IDs")
    if df["ID"].duplicated().any():
        raise ValueError("Duplicate IDs found in API response")
    for col in ["Reported", "LastUpdated", "StartDate", "PlannedEndDate"]:
        df[col] = pd.to_datetime(pd.to_numeric(df[col], errors="coerce"), unit="s", utc=True, errors="coerce")
    local = df["Reported"].dt.tz_convert("America/Toronto")
    df["ReportedLocal"] = local
    df["ReportedDate"] = local.dt.date
    df["ReportedHour"] = local.dt.hour
    df["ReportedDay"] = local.dt.day_name()
    df["ReportedMonth"] = local.dt.month
    df["ReportedYear"] = local.dt.year
    for col in ["Latitude", "Longitude", "LatitudeSecondary", "LongitudeSecondary"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["LinkId"] = df["LinkId"].map(lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float)) else x)
    df.columns = STAGING_COLUMNS
    print(f"Cleaned {len(df)} events")
    return df


def build_merge_sql():
    comparison_cols = [c for c in MASTER_COLUMNS if c != "EVENT_ID"]
    changed_condition = " OR\n        ".join(f"t.{c} IS DISTINCT FROM s.{c}" for c in comparison_cols)
    update_assignments = ",\n        ".join(f"t.{c} = s.{c}" for c in comparison_cols)
    insert_cols = ", ".join(MASTER_COLUMNS)
    insert_values = ", ".join(f"s.{c}" for c in MASTER_COLUMNS)
    return f"""
        MERGE INTO PUBLIC.TRAFFIC_EVENTS t
        USING PUBLIC.TRAFFIC_EVENTS_STAGING s
        ON t.EVENT_ID = s.EVENT_ID
        WHEN MATCHED AND ({changed_condition}) THEN UPDATE SET
            {update_assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_cols})
        VALUES ({insert_values})
    """


def load_to_snowflake(df):
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"], password=os.environ["SNOWFLAKE_PASSWORD"], warehouse=os.environ["SNOWFLAKE_WAREHOUSE"], database=os.environ["SNOWFLAKE_DATABASE"], schema=os.environ["SNOWFLAKE_SCHEMA"])
    try:
        success, chunks, rows, output = write_pandas(conn, df, table_name="TRAFFIC_EVENTS_STAGING", schema="PUBLIC", overwrite=True, auto_create_table=False, quote_identifiers=False)
        if not success or rows != len(df):
            raise RuntimeError("Staging upload incomplete")
        print(f"Staging upload complete: {rows} rows")
        cursor = conn.cursor()
        try:
            cursor.execute("""SELECT COUNT(*) FROM PUBLIC.TRAFFIC_EVENTS_STAGING s WHERE NOT EXISTS (SELECT 1 FROM PUBLIC.TRAFFIC_EVENTS t WHERE t.EVENT_ID = s.EVENT_ID)""")
            print(f"New event IDs: {cursor.fetchone()[0]}")
            cursor.execute(build_merge_sql())
            print("MERGE result:", cursor.fetchone())
            cursor.execute("SELECT COUNT(*) FROM PUBLIC.TRAFFIC_EVENTS")
            print("Master table total:", cursor.fetchone()[0])
        finally:
            cursor.close()
    finally:
        conn.close()


def main():
    started = datetime.now(timezone.utc)
    print(f"Pipeline started at {started.isoformat()}")
    raw_df = fetch_events()
    clean_df = clean_events(raw_df)
    load_to_snowflake(clean_df)
    print("Daily pipeline completed successfully!")


if __name__ == "__main__":
    main()
