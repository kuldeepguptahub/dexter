import pandas as pd
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
    
    # Issue type (max 2)
    issues = []
    mapping = {
        "ip": "IP-block",
        "blocked": "IP-block",
        "email": "email-sending-issue",
        "receive": "email-receiving-issue",
        "ssl": "ssl-problem",
        "domain": "primary-domain-change",
        "dns": "dns-related",
        "wordpress": "wordpress",
        "design": "web-design",
        "grace": "grace-period",
        "cancel": "cancellation",
        "refund": "refund",
        "order": "new-order",
        "payment": "payment-issue",
        "ai builder": "ai-builder",
        "password": "wp-password-reset",
        "policy": "policy-related",
        "503": "503-error",
        "resource": "high-resource-usage",
        "price": "pricing-concern",
        "500": "500-error",
        "ftp": "ftp-issue",
        "400": "400-error",
        "install": "wp-installation",
        "delete": "delete-website",
        "inode": "inode-issue",
        "disk": "disk-space-issue",
        "outage": "server-outage",
        "vps": "vps-server",
        "node": "node.js",
        "python": "python",
        "php": "php-update",
    }
    
    for k, v in mapping.items():
        if k in text and v not in issues:
            issues.append(v)
    
    if not issues:
        issues = [fallback_issue]
    
    tags.extend(issues[:2])
    
    # Resolution type (optional)
    if "escalated" in text or "forwarded" in text:
        tags.append("escalated")
    elif "resolved" in text or "fixed" in text or "solved" in text:
        tags.append("resolved")
    
    # Format as quoted CSV-style
    return ", ".join([f"\"{t}\"" for t in tags])