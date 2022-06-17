import os
from numpy import float64
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from objects.errors import EmptyPollError, NegativeDapprovalError
from methods.conn_init import load_connection
from methods.poll_data import fetch_poll_data, fetch_poll_list
from methods.poll_iter import poll_iter

# State management for cursor object
if 'cur' not in st.session_state:
    st.session_state.cur = load_connection()
elif st.session_state.cur.is_closed():
    st.session_state.cur = load_connection()

# State management for ranked poll list
if 'ranked_polls' not in st.session_state:
    st.session_state.ranked_polls = fetch_poll_list()

# Set streamlit page layout/orientation to 'wide' 
st.set_page_config(layout="wide")

# Display titles
st.title("Ranked Poll Simulation")
st.write("This app simulates the Instant Runoff Voting algorithm for ranked polls. Showing how choices compete against each other. \n The intent is to help voters when ranking choices in Priotization Sentiment polls.")

# Render select box for poll options
option = st.selectbox(
     'Select a poll',
     st.session_state.ranked_polls.keys()
)

# Display selected poll
st.write("Selected poll:", st.session_state.ranked_polls[option])

# Fetch poll data and analyze df
poll_metadata, poll_results = fetch_poll_data(st.session_state.cur, st.session_state.ranked_polls[option])

# Error handle
try:
    poll_result_df, poll_options_df = poll_iter(poll_metadata, poll_results)
except EmptyPollError:
    st.write("Poll dataframe is empty!")
except NegativeDapprovalError:
    st.write("Negative value detected in dapproval!")

# Create visualization dimensions
dims = list()
for dim in poll_result_df[df.columns[2:]]:
    dims.append(go.parcats.Dimension(values=poll_result_df[dim], label=dim))

# Create parcats trace
color = poll_result_df.power

# Capitalize and add space in labels
for dim in dims:
    store = dim['label'].split('_')
    dim['label'] = ' '.join([store[0].capitalize(), str(int(store[1]) + 1)])

# Create figure
fig = go.Figure(
    data = [go.Parcats(
        dimensions=dims,
        line={'color': color, 'colorscale': px.colors.sequential.Burgyl, 'shape':'hspline'},
        counts=[float64(i) for i in poll_result_df.power],
        hoveron='dimension',)]
    )

# Display 'fig' plotly chart
st.plotly_chart(fig, use_container_width=True)

# Final vote prioritization table
st.write("Final vote prioritization (descending).")
st.table(poll_options_df)    
        
# Text feeds
st.caption("""
            Built by Data Insights with the support of GovAlpha & DUX.\n
            [Prioritization Framework forum post](https://forum.makerdao.com/t/prioritization-framework-sentiment-polling/15554)
             """
)

st.info("Upcoming improvements: \n 1. User input of votes \n 2. Exclude *Abstain* from calculations \n 3. Split app between sentiment & non-sentiment polls")