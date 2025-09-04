import streamlit as st
from data_pipeline.transform_silver import transform_silver
from data_pipeline.transform_gold_summary import enrich_gold_summary
from data_pipeline.transform_gold_tags import assign_tags
from data_pipeline.utils import get_latest_file, timestamp
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

LOG_PATH = Path("logs/pipeline.log")
BRONZE_PATH = Path("lakehouse/bronze")
SILVER_PATH = Path("lakehouse/silver")
GOLD_PATH = Path("lakehouse/gold")

logging.basicConfig(filename=LOG_PATH, level=logging.INFO)

st.sidebar.title("Refresh Data")

uploaded_file = st.sidebar.file_uploader("Upload a file", type=["csv"])

# Read CSV as Dataframe
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    st.sidebar.success("File uploaded successfully")

    timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    logging.info(f"File uploaded at {timestamp}")

    # Convert CSV to parquet
    parquet_file = BRONZE_PATH / f"bronze_{timestamp}.parquet"
    df.to_parquet(parquet_file, index=False)
    logging.info(f"File converted to parquet at {parquet_file}")

    st.sidebar.success("Data refreshed and saved to Bronze layer")

    #transform silver
    transform_silver()
    st.sidebar.success("Data transformed and saved to Silver layer")

    # enrich gold summary
    enrich_gold_summary()
    st.sidebar.success("Data enriched and saved to Gold layer, starting tagging process")

    # create df from latest gold file
    latest_gold = get_latest_file(GOLD_PATH)
    df = pd.read_parquet(latest_gold)

    # assign tags
    df = df.copy()  # avoid SettingWithCopyWarning
    df["tags"] = df.apply(lambda row: assign_tags(row["chat_summary"], row["one_line_summary"], row["tags"]), axis=1)
    logging.info(f"{timestamp} GOLD - Assigned tags to {len(df)} records")
    st.sidebar.success("Tags updated and saved to Gold layer, data ready for analysis")

    # Save updated gold file
    gold_file = GOLD_PATH / f"gold_summary_tags_{timestamp}.parquet"
    df.to_parquet(gold_file, index=False)

# Display the dataframe in the main area
latest_gold = get_latest_file(GOLD_PATH)
df = pd.read_parquet(latest_gold)

st.title("Data")
st.dataframe(df)
