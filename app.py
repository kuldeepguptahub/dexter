import streamlit as st
from data_pipeline.transform_silver import transform_silver
from data_pipeline.utils import get_latest_file, timestamp
import logging
from pathlib import Path
import pandas as pd

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

    logging.info(f"File uploaded at {timestamp()} with {len(df)} records")

    # Convert CSV to parquet
    parquet_file = BRONZE_PATH / f"bronze_{timestamp()}.parquet"
    df.to_parquet(parquet_file, index=False)
    logging.info(f"BRONZE - File converted to parquet")

    st.sidebar.success("Data refreshed and saved to Bronze layer")

    #transform silver
    transform_silver()
    st.sidebar.success("Data transformed and saved to Silver layer")

    # Create df from latest silver file
    df = pd.read_parquet(get_latest_file(SILVER_PATH))

    # Transform Gold
    from data_pipeline.transform_gold import assign_tags

    df = df.copy()  # avoid SettingWithCopyWarning
    df["tags"] = None  # initialize tags column
    st.sidebar.success(f'Adding tags to {len(df)} records')
    
    df["tags"] = df.apply(lambda row: assign_tags(row["chat_summary"], row["tags"]), axis=1)
    gold_file = GOLD_PATH / f"gold_{timestamp()}.parquet"
    df.to_parquet(gold_file, index=False)
    logging.info(f"GOLD - Data transformed and saved to Gold layer")
    st.sidebar.success("Data transformed and saved to Gold layer, ready for analysis")

# Display the dataframe in the main area
latest_gold = get_latest_file(GOLD_PATH)
df = pd.read_parquet(latest_gold)

st.title("Data")
st.dataframe(df)
