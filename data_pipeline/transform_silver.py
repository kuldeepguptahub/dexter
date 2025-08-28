import os
from datetime import datetime
import logging
import pandas as pd

BRONZE_DIR = "lakehouse/bronze"
SILVER_DIR = "lakehouse/silver"
LOG_FILE = "logs/silver.log"

os.makedirs(SILVER_DIR, exist_ok=True)

logging.basicConfig(filename=LOG_FILE, level=logging.INFO)

def get_latest_bronze():
    """Fetch the latest parquet file from bronze"""
    files = [f for f in os.listdir(BRONZE_DIR) if f.endswith(".parquet")]
    if not files:
        raise FileNotFoundError("❌ No bronze data found.")
    latest = max(files, key=lambda x: os.path.getmtime(os.path.join(BRONZE_DIR, x)))
    return os.path.join(BRONZE_DIR, latest)

def transform_silver():
    bronze_file = get_latest_bronze()
    df = pd.read_parquet(bronze_file)

    # --- Transformations ---
    # 1. Convert datetime → date (string format YYYY-MM-DD)
    if "datetime" in df.columns:
        df["date"] = pd.to_datetime(df["Timestamp"]).dt.date
        df.drop(columns=["Timestamp"], inplace=True, errors="ignore")

    # 2. Remove duplicate chat_id
    if "Chat ID" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["Chat ID"], keep="first")
        after = len(df)
        logging.info(f"Removed {before - after} duplicate chat_id rows")

    # Save to Silver
    ts = datetime.now().strftime("%d%m%Y_%H%M%S")
    silver_file = os.path.join(SILVER_DIR, f"silver_{ts}.parquet")
    df.to_parquet(silver_file, index=False)

    logging.info(f"{datetime.now()} - Transformed {bronze_file} → {silver_file}")
    print(f"✅ Transformed {bronze_file} → {silver_file}")

if __name__ == "__main__":
    transform_silver()

