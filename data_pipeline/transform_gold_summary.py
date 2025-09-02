import pandas as pd
from pathlib import Path
import logging
import streamlit as st
from data_pipeline.utils import get_latest_file, timestamp
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

    # Merge with latest file in gold on chat_id
    gold_file = get_latest_file(GOLD_PATH)
    if gold_file:
        gold_df = pd.read_parquet(gold_file)
        df = pd.merge(df, gold_df, on="chat_id", how="outer", suffixes=("_silver", "_gold"))

        df["chat_summary"] = df["chat_summary_gold"].combine_first(df["chat_summary_silver"])
        df.drop(columns=["chat_summary_silver", "chat_summary_gold"], inplace=True)

    # Check if one-line summary exists
    if "one_line_summary" not in df.columns:
        df["one_line_summary"] = df["one_line_summary"].astype(str)

    # Filter the df where df['one_line_summary'] is blank
    df_no_one_liner = df[df['one_line_summary'].str.strip() == ""]

    # Load summarizer (DistilBART CNN)
    summarizer = pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")

    one_line_summaries = []
    for text in df_no_one_liner["chat_summary"].fillna("").tolist():
        if not text.strip():
            one_line_summaries.append("NA")
            continue

        try:
            result = summarizer(text, max_length=25, min_length=10, do_sample=False)
            one_line_summaries.append(result[0]['summary_text'])
        except Exception as e:
            logging.error(f"{timestamp()} Error summarizing text: {e}")
            one_line_summaries.append("ERROR")

    df_no_one_liner["one_line_summary"] = df_no_one_liner["one_line_summary"].astype(str)

    # Combine back with original df
    df = df.combine_first(df_no_one_liner)

    # Save Gold Layer
    gold_file = GOLD_PATH / f"gold_summary_{timestamp()}.parquet"
    df.to_parquet(gold_file, index=False)

    logging.info(f"{timestamp()} GOLD - Updated {silver_file} with one_line_summary - {gold_file}")
    st.sidebar.success(f"✅ Updated {silver_file} with one_line_summary - {gold_file}")

if __name__ == "__main__":
    enrich_gold_summary()