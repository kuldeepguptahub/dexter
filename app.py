import streamlit as st
from components.sidebar import refresh_data
from data_pipeline.transform_silver import transform_silver
from data_pipeline.transform_gold_summary import enrich_gold_summary

refresh_data()
transform_silver()
enrich_gold_summary()
