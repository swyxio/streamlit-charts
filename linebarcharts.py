# streamlit_app.py
import pandas as pd
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import numpy as np
from statsmodels.nonparametric.smoothers_lowess import lowess
from scipy.interpolate import interp1d

def loess_smooth(window):
    x = np.array(range(len(window)))
    y = window.values
    smoothed = lowess(y, x, frac=0.05)[:, 1]
    return smoothed[-1]

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

limit_options = [100, 500, 1000, 10000]
selected_limit = st.selectbox("Select the number of data points to display", options=limit_options)



# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query, limit):
    query = query.format(limit)
    query_job = client.query(query)
    rows_raw = query_job.result()
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
  {}
""", selected_limit)


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

# # simple smoothing
# for provider in unique_providers:
#     provider_data = non_zero_df[non_zero_df['provider_api_name'] == provider]
#     x = np.array(range(len(provider_data)))
#     y = provider_data['latency'].values

#     smoothed_latencies = lowess(y, x, frac=0.05)[:, 1]

#     for i, timestamp in enumerate(provider_data.index):
#         smoothed_data.append({
#             'timestamp': timestamp,
#             'latency': smoothed_latencies[i],
#             'provider_api_name': provider
#         })
# # Convert the smoothed data into a DataFrame
# smoothed_df = pd.DataFrame(smoothed_data)

# smooth by resampling based on timestamp
for provider in unique_providers:
    provider_data = df[df['provider_api_name'] == provider]
    # provider_data = provider_data.resample('1T').mean().interpolate() #
    
    provider_data['smoothed_latency'] = provider_data['latency'].rolling('1D', min_periods=1).apply(loess_smooth, raw=False)
    
    provider_smoothed_df = provider_data.reset_index()
    provider_smoothed_df['provider_api_name'] = provider
    
    smoothed_data.append(provider_smoothed_df)

smoothed_df = pd.concat(smoothed_data)
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

        combined_chart += zero_point_chart

# Make the combined chart interactive
combined_chart = combined_chart.interactive()

# Display the combined chart in Streamlit
st.altair_chart(combined_chart, use_container_width=True)
