import streamlit as st
import matplotlib.pyplot as plt
from data_pipeline.transform_silver import transform_silver
from data_pipeline.utils import get_latest_file, timestamp
import logging
from pathlib import Path
import pandas as pd
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

st.title("Customer Support Chat Analysis")
col1, col2 = st.columns(2)

# Prepare dataframe from latest gold file
latest_gold = get_latest_file(GOLD_PATH)
df = pd.read_parquet(latest_gold)
df["date"] = pd.to_datetime(df["date"], errors="coerce")

# Prepare df from tags
tags_list = []
for tags in df['tags']:
    tags_list.extend([t.strip().strip('"') for t in tags.split(',')])

df_tags = pd.Series(tags_list)

with col1:
    # Display total chats
    st.metric(label='Total Chats', value=len(df))

    # Last 7 days data
    start_date = pd.to_datetime(datetime.now().date()) - pd.Timedelta(days=7)
    end_date = pd.to_datetime(datetime.now().date())
    last_week_data = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
    st.write(f"### Common Issues in the Last 7 Days")
    if not last_week_data.empty:
        last_week_tags = []
        for tags in last_week_data['tags']:
            last_week_tags.extend([t.strip().strip('"') for t in tags.split(',')])
        last_week_series = pd.Series(last_week_tags)
        last_week_top_tags = last_week_series[~last_week_series.isin(["sales", "billing", "technical-support", 'general-issue', 'resolved', 'escalated'])].value_counts().head(5)
        for tag, count in last_week_top_tags.items():
            st.write(f"- {tag} - {count} chats")
    else:
        st.write("No data available for the last 7 days.")

    # Last 30 days data
    start_date = pd.to_datetime(datetime.now().date()) - pd.Timedelta(days=30)
    end_date = pd.to_datetime(datetime.now().date())
    last_month_data = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
    st.write(f"### Common Issues in the Last 30 Days")
    if not last_month_data.empty:
        last_month_tags = []
        for tags in last_month_data['tags']:
            last_month_tags.extend([t.strip().strip('"') for t in tags.split(',')])
        last_month_series = pd.Series(last_month_tags)
        last_month_top_tags = last_month_series[~last_month_series.isin(["sales", "billing", "technical-support", 'general-issue', 'resolved', 'escalated'])].value_counts().head(5)
        for tag, count in last_month_top_tags.items():
            st.write(f"- {tag} - {count} chats")


with col2:
    # Display department wise pie chart
    fig, ax = plt.subplots(figsize=(8, 6))
    df_tags[df_tags.isin(["sales", "billing", "technical-support", "affiliates"])].value_counts().plot.pie(autopct='%1.1f%%')
    plt.title('Department wise Chat Distribution')
    plt.ylabel('')
    st.pyplot(fig)



# Common Issues by tags
st.write("### Review Common Issues")
# Filter out generic/system tags
excluded_tags = ["sales", "billing", "technical-support", "general-issue", "resolved", "escalated"]
tag_options = df_tags[~df_tags.isin(excluded_tags)].value_counts().index.tolist()

# Dropdown for tag selection
selected = st.selectbox("Select Type", options=tag_options)

# Filter relevant chats
filtered_df = df[df['tags'].str.contains(selected, na=False)]

# Sample 5 random chats
sample_size = min(5, len(filtered_df))
sampled_df = filtered_df[['chat_id', 'chat_summary']].sample(n=sample_size, random_state=42)

# Display result
st.dataframe(sampled_df.reset_index(drop=True))