# scripts/refresh_seeds.py
import os
import csv
import requests
from pathlib import Path
from datetime import datetime

# ---- Config (env vars override the defaults) -------------------------------
API_URL = os.getenv("API_URL", "https://jsonplaceholder.typicode.com/users")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "seeds"))
OUTPUT_FILE = OUTPUT_DIR / os.getenv("OUTPUT_FILE", "customers.csv")

# Columns we’ll write to the CSV (keep these stable for dbt)
CSV_COLUMNS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "company",
    "city",
    "last_refreshed_utc",
]

def split_name(full_name: str):
    if not full_name:
        return ("", "")
    parts = full_name.split()
    if len(parts) == 1:
        return (parts[0], "")
    return (parts[0], " ".join(parts[1:]))

def fetch_records():
    r = requests.get(API_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    # JSONPlaceholder returns a list already
    if not isinstance(data, list):
        raise ValueError("Expected a list of records from the API.")
    return data

def transform(records):
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    rows = []
    for rec in records:
        first, last = split_name(rec.get("name"))
        rows.append({
            "customer_id": rec.get("id"),
            "first_name": first,
            "last_name": last,
            "email": rec.get("email"),
            "company": (rec.get("company") or {}).get("name"),
            "city": (rec.get("address") or {}).get("city"),
            "last_refreshed_utc": now,
        })
    return rows

def write_csv(rows):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

def main():
    print(f"[refresh-seeds] Fetching from {API_URL} …")
    data = fetch_records()
    print(f"[refresh-seeds] Received {len(data)} records")
    rows = transform(data)
    write_csv(rows)
    print(f"[refresh-seeds] Wrote {len(rows)} rows to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
