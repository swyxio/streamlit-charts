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
non_zero_df = df[df['latency'] != 0]
zero_df = df[df['latency'] == 0]

import altair as alt


# Add a dropdown for y-axis scale selection
y_axis_scale_options = ["linear", "log", "symlog", "sqrt"]
selected_y_axis_scale = st.selectbox("Select y-axis scale", options=y_axis_scale_options)
y_axis = alt.Y('latency:Q', scale=alt.Scale(type=selected_y_axis_scale), title=f'Latency ({selected_y_axis_scale} scale)')
# Apply LOESS smoothing for each provider_api_name
unique_providers = df['provider_api_name'].unique()
smoothed_data = []

for provider in unique_providers:
    provider_data = non_zero_df[non_zero_df['provider_api_name'] == provider]
    x = np.array(range(len(provider_data)))
    y = provider_data['latency'].values

    smoothed_latencies = lowess(y, x, frac=0.05)[:, 1]

    for i, timestamp in enumerate(provider_data.index):
        smoothed_data.append({
            'timestamp': timestamp,
            'latency': smoothed_latencies[i],
            'provider_api_name': provider
        })

# Convert the smoothed data into a DataFrame
smoothed_df = pd.DataFrame(smoothed_data)
smoothed_df.set_index('timestamp', inplace=True)



# # Create an Altair chart
# chart = alt.Chart(smoothed_df.reset_index()).mark_line().encode(
#     x='timestamp:T',
#     y='latency:Q',
#     color='provider_api_name:N',
#     tooltip=['timestamp', 'latency', 'provider_api_name']
# ).interactive()

# # Display the chart in Streamlit
# st.altair_chart(chart, use_container_width=True)

# Add checkboxes to toggle line chart and point chart
show_line_chart = st.checkbox("Show smoothed line chart", value=True)
show_point_chart = st.checkbox("Show data points")

# Create an empty base chart
combined_chart = alt.Chart().mark_point().encode()



# Loop through each unique provider API name
for provider in unique_providers:
    # Check if the corresponding checkbox is checked
    if st.checkbox(f"Show {provider}", value=True):
        # Add the smoothed line chart if the corresponding checkbox is checked
        if show_line_chart:
            smoothed_line_chart = alt.Chart(smoothed_df.loc[smoothed_df['provider_api_name'] == provider].reset_index()).mark_line().encode(
                x='timestamp:T',
                y=y_axis,
                color='provider_api_name:N',
            )
            combined_chart += smoothed_line_chart

        # Add the point chart if the corresponding checkbox is checked
        if show_point_chart:
            point_chart = alt.Chart(df.loc[df['provider_api_name'] == provider].reset_index()).mark_point().encode(
                x='timestamp:T',
                y=y_axis,
                color='provider_api_name:N',
                tooltip=['timestamp', 'latency', 'provider_api_name'],
            )
            combined_chart += point_chart
            
        zero_point_chart = alt.Chart(zero_df.loc[zero_df['provider_api_name'] == provider].reset_index()).mark_point(
            shape='cross', color='red', size=50
        ).encode(
            x='timestamp:T',
            y=y_axis,
            tooltip=['timestamp', 'latency', 'provider_api_name'],
        )

# Make the combined chart interactive
combined_chart = combined_chart.interactive()

# Display the combined chart in Streamlit
st.altair_chart(combined_chart, use_container_width=True)
