# Ontario Traffic Analysis

An end-to-end data engineering and analytics portfolio project that collects live Ontario road-event data from the Ontario 511 API, cleans and transforms it with Python and Pandas, loads it into Snowflake, tracks active and inactive traffic events, and prepares the data for analysis in Power BI.

## Project Goal

The goal is to build a repeatable traffic-data pipeline rather than analyze a single static CSV. Each pipeline run retrieves the latest Ontario 511 events and updates a Snowflake master table so new, changed, active, and closed events can be analyzed over time.

## Architecture

```text
Ontario 511 API
      |
      v
Python / Requests
      |
      v
Pandas cleaning + transformation
      |
      v
Snowflake TRAFFIC_EVENTS_STAGING
      |
      v
MERGE + active/inactive event logic
      |
      v
Snowflake TRAFFIC_EVENTS
      |
      v
Power BI dashboard
```

## Technologies

- Python
- Pandas
- Requests
- Snowflake + Snowflake Python Connector
- SQL
- Power BI
- python-dotenv
- Git / GitHub

## What the Pipeline Does

1. Calls the Ontario 511 events API.
2. Validates that the response contains expected event fields.
3. Normalizes JSON data into a Pandas DataFrame.
4. Cleans timestamps, numeric fields, event IDs, and optional API columns.
5. Converts reported timestamps from UTC to the `America/Toronto` timezone.
6. Creates reporting fields including date, hour, weekday, month, and year.
7. Loads the current API snapshot into `TRAFFIC_EVENTS_STAGING` in Snowflake.
8. Compares staging data with the master `TRAFFIC_EVENTS` table.
9. Inserts new events and updates changed events using a Snowflake `MERGE`.
10. Uses event presence between snapshots to support active/inactive event tracking.
11. Makes the Snowflake data available to Power BI for dashboarding.

## Repository Structure

```text
ontario-traffic-analysis/
├── README.md
├── requirements.txt
├── .gitignore
├── pipeline.py
├── src/
│   ├── download_traffic.py
│   ├── clean_traffic.py
│   ├── load_traffic_to_snowflake.py
│   ├── comparefile.py
│   └── test_snowflake_connection.py
└── sql/
    └── validation_queries.sql
```

Generated traffic snapshots, cleaned CSV files, `.env` credentials, private keys, and local working files are excluded from source control where appropriate.

## Snowflake Design

### `TRAFFIC_EVENTS_STAGING`

The staging table represents the latest Ontario 511 API snapshot. It is overwritten during each load so it can be compared against the persistent master table.

### `TRAFFIC_EVENTS`

The master table stores traffic events across pipeline runs. `EVENT_ID` is used to match records between staging and master data. The pipeline updates existing records when tracked fields change and inserts events that have not been seen before.

### Active / Inactive Events

An event that exists in the master table but no longer appears in the newest staging snapshot can be marked inactive. This preserves historical events instead of deleting them when they disappear from the live API.

```sql
UPDATE PUBLIC.TRAFFIC_EVENTS t
SET IS_ACTIVE = FALSE
WHERE NOT EXISTS (
    SELECT 1
    FROM PUBLIC.TRAFFIC_EVENTS_STAGING s
    WHERE s.EVENT_ID = t.EVENT_ID
);
```

Events appearing in the latest snapshot can be maintained as active, allowing Power BI to distinguish current road conditions from historical/closed events.

## Data Fields

The pipeline works with fields such as event ID, roadway name, direction of travel, description, reported and last-updated timestamps, start/planned end dates, event type, severity, full-closure indicator, lanes affected, latitude/longitude, impact, and derived local date/time fields.

## Data Quality and Validation

Validation is the next major development step. Planned checks include:

- staging row count
- null `EVENT_ID` checks
- duplicate `EVENT_ID` checks
- new-event counts
- inactive/closed-event counts
- staging-to-master load validation
- pipeline failure checks
- timestamp and required-column validation

Starter validation queries are included in `sql/validation_queries.sql`.

## Power BI

Snowflake is connected to Power BI for the reporting layer. The dashboard workstream is designed to include KPIs and visuals such as active traffic events, new events, closed/inactive events, event type distribution, frequently affected roads, hourly/weekday patterns, full road closures, and geographic analysis.

Dashboard screenshots will be added as the reporting layer is finalized.

## Local Setup

```bash
git clone https://github.com/kataira-karan/ontario-traffic-analysis.git
cd ontario-traffic-analysis
pip install -r requirements.txt
```

Create a local `.env` file containing your own credentials:

```text
ONTARIO_511_API_KEY=your_api_key
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=PUBLIC
```

Never commit the `.env` file or private credentials to GitHub.

Run the integrated pipeline with:

```bash
python pipeline.py
```

## Project Status

Completed / working:

- Ontario 511 API data retrieval
- Python/Pandas data cleaning
- Snowflake connectivity
- staging-table loading
- staging-to-master comparison
- insert/update `MERGE` logic
- active/inactive event tracking
- Snowflake connection to Power BI

In progress / next:

- data-quality validation
- Power BI dashboard refinement
- automated/scheduled pipeline execution
- dashboard screenshots and final portfolio documentation

## Security

Credentials are loaded from environment variables using `python-dotenv`. Secrets, `.env` files, PEM/private-key files, generated data, and local cache files are excluded through `.gitignore` and should never be committed.

## Author

**Karan Kataria**

Portfolio project focused on Python, SQL, Snowflake, data engineering, and Power BI analytics.