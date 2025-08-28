import os
import json
import streamlit as st
from datetime import datetime
import subprocess

DATA_DIR = 'data'
CACHE_FILE = os.path.join(DATA_DIR, 'cache.json')

def save_cache(file_path):
    cache = {"last_file": file_path}
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {}

st.title("Hi! I am Dexter..")

uploaded_file = st.file_uploader('Upload your data file here', type=['csv'])

if uploaded_file:
    # Generate unique name with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(DATA_DIR, f"data_{timestamp}.csv")

    # Save uploaded file
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    # Update cache
    save_cache(file_path)

    st.success(f"✅ File saved as {file_path}")

    # DVC add + commit
    try:
        subprocess.run(["dvc", "add", file_path], check=True)
        subprocess.run(["git", "add", file_path + ".dvc"], check=True)
        subprocess.run(["git", "commit", "-m", f"Added {file_path}"], check=True)
        st.info("📦 Data version tracked with DVC.")
    except Exception as e:
        st.error(f"DVC tracking failed: {e}")

# Show last file info
cache = load_cache()
if cache.get("last_file"):
    st.write(f"📂 Last uploaded file: {cache['last_file']}")