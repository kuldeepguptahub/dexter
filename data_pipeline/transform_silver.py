from datetime import datetime
import logging
import pandas as pd
from pathlib import Path
import logging
import glob, os
import streamlit as st
from data_pipeline.utils import get_latest_file, timestamp

BRONZE_PATH = Path("lakehouse/bronze")
SILVER_PATH = Path("lakehouse/silver")
LOG_FILE = Path("logs/pipeline.log")

logging.basicConfig(filename=LOG_FILE, level=logging.INFO)

# ------------------------
# TRANSFORM SILVER
# ------------------------

def transform_silver():
    """Transforms latest bronze file and saves cleaned output to silver zone."""
    st.sidebar.success("Transforming data to Silver layer...")
    try:
        # Step 1: Load latest bronze file
        latest_bronze_file = get_latest_file(BRONZE_PATH)
        df = pd.read_parquet(latest_bronze_file)
        logging.info(f"{timestamp()} SILVER - Loaded bronze file: {latest_bronze_file.name}")

        # Step 2: Rename columns
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        # Step 3: Convert timestamp to date
        if 'timestamp' in df.columns:
            df['date'] = pd.to_datetime(df['timestamp'], errors='coerce').dt.date
            df = df.drop(columns=['timestamp'])
            logging.info(f"{timestamp()} SILVER - Converted 'timestamp' to 'date' and dropped original column")
        else:
            logging.warning(f"{timestamp()} SILVER - 'timestamp' column not found")

        # Step 4: Drop duplicates by chat_id
        if 'chat_id' in df.columns:
            df = df.drop_duplicates(subset=['chat_id'])
            logging.info(f"{timestamp()} SILVER - Dropped duplicates based on 'chat_id'")
        else:
            logging.warning(f"{timestamp()} SILVER - 'chat_id' column not found")

        # Step 5: Save to silver zone
        SILVER_PATH.mkdir(parents=True, exist_ok=True)
        silver_file = SILVER_PATH / f"silver_{timestamp()}.parquet"
        df.to_parquet(silver_file, index=False)
        logging.info(f"{timestamp()} SILVER - Saved transformed data to {silver_file.name}")
        st.sidebar.success("Data transformed and saved to Silver layer")    

    except Exception as e:
        logging.error(f"{timestamp()} SILVER - Transformation failed: {e}")
        st.sidebar.error("Data transformation failed")
        raise
