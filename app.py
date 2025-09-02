import streamlit as st
from components.sidebar import refresh_data
from data_pipeline.transform_silver import transform_silver

refresh_data()
transform_silver()
