import os
from datetime import datetime
import streamlit as st
import logging
import pandas as pd
from pathlib import Path

LOG_PATH = Path("logs/pipeline.log")
BRONZE_PATH = Path("lakehouse/bronze")
logging.basicConfig(filename=LOG_PATH, level=logging.INFO)

def refresh_data():
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
