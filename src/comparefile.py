import pandas as pd
from pathlib import Path

files = sorted(Path("data/processed").glob("traffic_*.csv"))
if len(files) < 2:
    raise ValueError("At least two CSV snapshots are required")

day1_file = files[-2]
day2_file = files[-1]
day1 = pd.read_csv(day1_file)
day2 = pd.read_csv(day2_file)

print("Day 1 file:", day1_file.name)
print("Day 2 file:", day2_file.name)
print("Day 1 events:", len(day1))
print("Day 2 events:", len(day2))
