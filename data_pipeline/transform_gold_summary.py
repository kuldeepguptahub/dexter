import os
from datetime import datetime
import pandas as pd
import logging
from transformers import pipeline

SILVER_DIR = "lakehouse/silver"
GOLD_DIR = "lakehouse/gold"
LOG_FILE = "logs/gold.log"

os.makedirs(GOLD_DIR, exist_ok=True)

logging.basicConfig(filename=LOG_FILE, level=logging.INFO)

def get_latest_silver():
    """Fetch latest silver file"""
    files = [f for f in os.listdir(SILVER_DIR) if f.endswith(".parquet")]
    if not files:
        raise FileNotFoundError("❌ No silver data found.")
    latest = max(files, key=lambda x: os.path.getmtime(os.path.join(SILVER_DIR, x)))
    return os.path.join(SILVER_DIR, latest)

def enrich_gold_summary():
    """Update data with one-line summary"""
    silver_file = get_latest_silver()
    df = pd.read_parquet(silver_file)

    # Load summarizer (DistilBART CNN)
    summarizer = pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")

    one_line_summaries = []
    for text in df["Chat Summary"].fillna("").tolist():
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
    ts = datetime.now().strftime("%d%m%Y_%H%M%S")
    gold_file = os.path.join(GOLD_DIR, f"gold_summary_{ts}.parquet")
    df.to_parquet(gold_file, index=False)

    logging.info(f"{datetime.now()} - Enriched {silver_file} → {gold_file} with one_line_summary")
    print(f"✅ Enriched {silver_file} → {gold_file}")

if __name__ == "__main__":
    enrich_gold_summary()