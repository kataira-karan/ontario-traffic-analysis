import pandas as pd
from pathlib import Path

files = sorted(Path("data/processed").glob("traffic_*.csv"))
if not files:
    raise FileNotFoundError("No traffic CSV found")

df = pd.read_csv(files[-1])
print("Shape:", df.shape)
print("Duplicate IDs:", df.duplicated(subset=["ID"]).sum())

empty_columns = df.columns[df.isnull().all()]
df = df.drop(columns=empty_columns)

for col in ["Reported", "StartDate", "LastUpdated", "PlannedEndDate"]:
    df[col] = pd.to_datetime(pd.to_numeric(df[col], errors="coerce"), unit="s", utc=True, errors="coerce")

for col in ["RoadwayName", "DirectionOfTravel", "EventType", "Severity", "Organization", "Description"]:
    df[col] = df[col].astype("string").str.strip()

reported_local = df["Reported"].dt.tz_convert("America/Toronto")
df["ReportedLocal"] = reported_local
df["ReportedDate"] = reported_local.dt.date
df["ReportedHour"] = reported_local.dt.hour
df["ReportedDay"] = reported_local.dt.day_name()
df["ReportedMonth"] = reported_local.dt.month
df["ReportedYear"] = reported_local.dt.year

output = Path("data/cleaned")
output.mkdir(parents=True, exist_ok=True)
df.to_csv(output / "ontario_traffic_cleaned.csv", index=False)

print("Cleaned dataset saved successfully!")
print("Final shape:", df.shape)
