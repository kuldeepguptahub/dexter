import os
import json
import pandas as pd
from datetime import datetime
import logging

DATA_DIR = "data"
CACHE_FILE = os.path.join(DATA_DIR, "cache.json")
BRONZE_DIR = "lakehouse/bronze"
LOG_FILE = "logs/bronze.log"

os.makedirs(BRONZE_DIR, exist_ok=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(filename=LOG_FILE, level=logging.INFO)

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {}

def cleanup_old_csvs(keep=3):
    csv_files = sorted(
        [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")],
        key=lambda x: os.path.getmtime(os.path.join(DATA_DIR, x)),
        reverse=True,
    )
    for old_file in csv_files[keep:]:
        os.remove(os.path.join(DATA_DIR, old_file))
        logging.info(f"{datetime.now()} - Deleted old file {old_file}")

def ingest_to_bronze():
    cache = load_cache()
    file_path = cache.get("last_file")
    if not file_path or not os.path.exists(file_path):
        print("❌ No cached file found. Please upload first.")
        return

    # Read CSV
    df = pd.read_csv(file_path)

    # Save to bronze (Parquet = efficient storage)
    ts = datetime.now().strftime("%d%m%Y_%H%M%S")
    bronze_file = os.path.join(BRONZE_DIR, f"bronze_{ts}.parquet")
    df.to_parquet(bronze_file, index=False)

    logging.info(f"{datetime.now()} - Ingested {file_path} → {bronze_file}")
    print(f"✅ Ingested {file_path} → {bronze_file}")

    # Cleanup old CSVs
    cleanup_old_csvs()

if __name__ == "__main__":
    ingest_to_bronze()
