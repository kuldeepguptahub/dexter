import pandas as pd
from pathlib import Path
import logging
import os
import streamlit as st
from utils import get_latest_file, timestamp
from transformers import pipeline

SILVER_PATH = Path("lakehouse/silver")
GOLD_PATH = Path("lakehouse/gold")
LOG_FILE = Path("logs/pipeline.log")

logging.basicConfig(filename=LOG_FILE, level=logging.INFO)

# ------------------------
# TRANSFORM GOLD - Add One-Line Summary
# ------------------------

def enrich_gold_summary():
    """Update data with one-line summary"""
    silver_file = get_latest_file(SILVER_PATH)
    df = pd.read_parquet(silver_file)

    # Load summarizer (DistilBART CNN)
    summarizer = pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")

    one_line_summaries = []
    for text in df["chat_summary"].fillna("").tolist():
        if not text.strip():
            one_line_summaries.append("NA")
            continue

        try:
            result = summarizer(text, max_length=25, min_length=10, do_sample=False)
            one_line_summaries.append(result[0]['summary_text'])
        except Exception as e:
            logging.error(f"Error summarizing text: {e}")
            one_line_summaries.append("ERROR")

    df["one_line_summary"] = one_line_summaries

    # Save Gold Layer
    gold_file = GOLD_PATH / f"gold_{timestamp()}.parquet"
    df.to_parquet(gold_file, index=False)

    logging.info(f"Updated {silver_file} with one_line_summary - {gold_file}")
    print(f"✅ Updated {silver_file} with one_line_summary - {gold_file}")

if __name__ == "__main__":
    enrich_gold_summary()