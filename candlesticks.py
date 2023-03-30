# streamlit_app.py
import pandas as pd
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import numpy as np
from statsmodels.nonparametric.smoothers_lowess import lowess


# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows

rows = run_query("""
SELECT
  *
FROM
  `kf-feast.latency.latency_stats`
ORDER BY
  timestamp DESC
LIMIT
  10000
""")


# Print results.
st.write("Latency graphs of the API providers:")
df = pd.DataFrame(rows)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.set_index('timestamp', inplace=True)
import altair as alt


# Add a dropdown for y-axis scale selection
y_axis_scale_options = ["linear", "log", "symlog", "sqrt"]
selected_y_axis_scale = st.selectbox("Select y-axis scale", options=y_axis_scale_options)
y_axis = alt.Y('latency:Q', scale=alt.Scale(type=selected_y_axis_scale), title=f'Latency ({selected_y_axis_scale} scale)')
# Apply LOESS smoothing for each provider_api_name
unique_providers = df['provider_api_name'].unique()
smoothed_data = []


# Remove rows with 0 latency
df = df[df['latency'] != 0]

# Set the timestamp column as the index and sort it
# df.set_index('timestamp', inplace=True)
df.sort_index(inplace=True)

# Resample the data to hourly intervals
hourly_data = df.groupby(['provider_api_name', pd.Grouper(freq='1D')]).agg(
    median_latency=pd.NamedAgg(column='latency', aggfunc='median'),
    std_dev_latency=pd.NamedAgg(column='latency', aggfunc=np.std),
    min_latency=pd.NamedAgg(column='latency', aggfunc='min'),
    max_latency=pd.NamedAgg(column='latency', aggfunc='max')
).reset_index()

# Create the candlestick chart
candlestick_chart = alt.Chart().mark_rule().encode(
    x='timestamp:T',
    y='min_latency:Q',
    y2='max_latency:Q',
    color='provider_api_name:N',
    tooltip=['timestamp', 'min_latency', 'max_latency', 'provider_api_name']
)

# Add median and standard deviation bars
candlestick_chart += alt.Chart().mark_bar(size=5).encode(
    x='timestamp:T',
    y=alt.Y('lower_bound:Q', title='Latency'),
    y2='upper_bound:Q',
    color='provider_api_name:N'
).transform_calculate(
    lower_bound="datum.median_latency - datum.std_dev_latency",
    upper_bound="datum.median_latency + datum.std_dev_latency"
)

# Combine the charts for each provider
combined_chart = alt.layer(candlestick_chart, data=hourly_data.loc[hourly_data['provider_api_name'] == unique_providers[0]])

for provider in unique_providers[1:]:
  if st.checkbox(f"Show {provider}", value=True):
    provider_data = hourly_data.loc[hourly_data['provider_api_name'] == provider]
    provider_chart = alt.layer(candlestick_chart, data=provider_data)
    combined_chart += provider_chart

# Make the combined chart interactive
combined_chart = combined_chart.interactive()
# Display the combined chart in Streamlit
st.altair_chart(combined_chart, use_container_width=True)