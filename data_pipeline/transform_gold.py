import pandas as pd
import re
import json
from pathlib import Path
import logging
from data_pipeline.utils import get_latest_file, timestamp

SILVER_PATH = Path("lakehouse/silver")
GOLD_PATH = Path("lakehouse/gold")
LOG_FILE = Path("logs/pipeline.log")

logging.basicConfig(filename=LOG_FILE, level=logging.INFO)

# ------------------------
# TRANSFORM GOLD - Add tags based on chat summary
# ------------------------

# Get latest file from Gold and create dataframe
logging.info(f"{timestamp()} GOLD - Fetching latest file from Gold layer")
silver_file = get_latest_file(SILVER_PATH)
df = pd.read_parquet(silver_file)

# add tags column if not present
if "tags" not in df.columns:
    df["tags"] = None
    
# Fallbacks
fallback_department = "technical-support"
fallback_issue = "general-issue"


def assign_tags(summary, existing):
    """Generate tags based on vocab, but keep existing if already present."""
    if existing and str(existing).strip().lower() not in ["none", "nan", ""]:
        return existing  # keep provided sample tags
    
    text = f"{summary}".lower()
    tags = []
    
    # Department
    if "payment" in text or "invoice" in text or "paypal" in text or "billing" in text:
        dept = "billing"
    elif "price" in text or "plan" in text or "sales" in text or "renew" in text:
        dept = "sales"
    elif "affiliate" in text:
        dept = "affiliates"
    else:
        dept = fallback_department
    tags.append(dept)
    
    # Issue type based on keywords
    issues = []
    tags_map = json.loads(Path('config/tags.json').read_text())

    for k, v in tags_map.items():
        for keyword in v:
            pattern = r'\b' + re.escape(keyword.lower()) + r'\b'
            if re.search(pattern, text):
                issues.append(k)            
    
    if not issues:
        issues = [fallback_issue]
    
    tags.extend(issues)
    
    # Resolution type (optional)
    if "escalated" in text or "forwarded" in text:
        tags.append("escalated")
    elif "resolved" in text or "fixed" in text or "solved" in text:
        tags.append("resolved")
    
    # Format as quoted CSV-style
    return ", ".join([f"\"{t}\"" for t in tags])