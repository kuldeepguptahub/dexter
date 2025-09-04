import pandas as pd
import numpy as np
from pathlib import Path
import logging
import streamlit as st
from data_pipeline.utils import get_latest_file, timestamp
from transformers import pipeline

# Define paths
SILVER_PATH = Path("lakehouse/silver")
GOLD_PATH = Path("lakehouse/gold")

def enrich_gold_summary():
    """Update data with one-line summary using DistilBART CNN"""

    # Load latest silver and gold files
    silver_file = get_latest_file(SILVER_PATH)
    silver = pd.read_parquet(silver_file)
    silver['one_line_summary'] = pd.NA
    silver['tags'] = np.nan

    gold_file = get_latest_file(GOLD_PATH)
    gold = pd.read_parquet(gold_file)

    # Merge silver and gold, keeping latest chat_id entries
    df = pd.concat([silver, gold], ignore_index=True)
    df = df.drop_duplicates(subset=['chat_id'], keep='last')
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Filter rows needing summarization
    df_no_one_liner = df[df['one_line_summary'].isna()].copy()

    # Load summarizer
    summarizer = pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")

    # Generate summaries
    one_line_summaries = []
    summary_status = []

    for text in df_no_one_liner["chat_summary"].fillna("").tolist():
        if not text.strip():
            one_line_summaries.append("NA")
            summary_status.append("EMPTY")
            continue

        try:
            result = summarizer(text, max_length=25, min_length=10, do_sample=False)
            one_line_summaries.append(result[0]['summary_text'])
            summary_status.append("OK")
        except Exception as e:
            logging.error(f"{timestamp()} Error summarizing text: {e}")
            one_line_summaries.append("ERROR")
            summary_status.append("ERROR")

    # Assign summaries and status
    df_no_one_liner["one_line_summary"] = one_line_summaries
    df_no_one_liner["summary_status"] = summary_status

    # Merge updated rows back into full DataFrame
    df.update(df_no_one_liner)

    # Save updated gold file
    output_file = GOLD_PATH / f"gold_summary_{timestamp()}.parquet"
    df.to_parquet(output_file, index=False)

    # Log and notify
    updated_count = len(df_no_one_liner)
    logging.info(f"{timestamp()} GOLD - Updated {silver_file} with one_line_summary - {output_file} for {updated_count} rows")
    st.sidebar.success(f"✅ Updated {silver_file} with one_line_summary - {output_file} for {updated_count} rows")

if __name__ == "__main__":
    enrich_gold_summary()
