import os
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("ONTARIO_511_API_KEY")
if not api_key:
    raise ValueError("API key missing from .env file")

url = "https://511on.ca/api/v2/get/event"
response = requests.get(url, params={"key": api_key, "format": "json"}, timeout=60)
response.raise_for_status()
data = response.json()

print("API connection successful!")
print("Total traffic events:", len(data))

os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

df = pd.json_normalize(data)
csv_file = f"data/processed/traffic_{timestamp}.csv"
df.to_csv(csv_file, index=False)
print("CSV saved:", csv_file)
print("Dataset shape:", df.shape)
