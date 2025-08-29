import pandas as pd
import json
import logging
from transformers import pipeline
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import glob
import os

GOLD_DIR = "lakehouse/gold"
# -------------------
# Logging setup
# -------------------
logging.basicConfig(
    filename="logs/gold.log",
    level=logging.INFO,
    format="%(asctime)s - TAGS - %(levelname)s - %(message)s"
)

# -------------------
# Load vocab
# -------------------
with open("config/tags.json", "r") as f:
    tag_vocab = json.load(f)

# -------------------
# Load classifier
# -------------------
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

def classify_text(text, labels):
    """Classify text against candidate labels with multi-label support"""
    result = classifier(text, candidate_labels=labels, multi_label=True)
    return [label for label, score in zip(result["labels"], result["scores"]) if score > 0.5]

def tag_row(row):
    """Generate tags for a single row"""
    text = f"Summary: {row['Chat Summary']} One-liner: {row['one_line_summary']} Comment: {row['Customer Comment']}"
    department = classify_text(text, tag_vocab["department"])
    issue_type = classify_text(text, tag_vocab["issue_type"])
    resolution = classify_text(text, tag_vocab["resolution_type"])
    return department, issue_type, resolution

def get_latest_gold():
    """Fetch latest silver file"""
    files = [f for f in os.listdir(GOLD_DIR) if f.endswith(".parquet")]
    if not files:
        raise FileNotFoundError("❌ No gold data found.")
    latest = max(files, key=lambda x: os.path.getmtime(os.path.join(GOLD_DIR, x)))
    return os.path.join(GOLD_DIR, latest)

def process_tags():
    start_time = datetime.now()
    logging.info("Tagging process started...")

    # Load latest gold_summary file
    input_path = get_latest_gold()
    df = pd.read_parquet(input_path)
    logging.info(f"Loaded {len(df)} rows from {input_path}")

    # Parallel execution
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(tag_row, [row for _, row in df.iterrows()]))

    # Attach results
    df[['chat_department', 'issue_type', 'resolution_type']] = pd.DataFrame(results, index=df.index)

    # Save enriched dataset with timestamp
    timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")
    output_path = f"lakehouse/gold/gold_enriched_{timestamp}.parquet"
    df.to_parquet(output_path, index=False)

    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()

    logging.info(f"Processed {len(df)} rows. Saved enriched file to {output_path}")
    logging.info(f"Time taken: {elapsed:.2f} seconds")
    logging.info("Tagging process completed.\n")

    print(f"✅ Tags added and saved to {output_path}")

if __name__ == "__main__":
    process_tags()
