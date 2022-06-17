import os
from re import X
from typing import Any, Dict, List, Tuple
from dotenv import load_dotenv
import snowflake.connector
import json
import requests
import copy
from collections import OrderedDict
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from objects.errors import EmptyPollError, NegativeDapprovalError

# Set streamlit page layout/orientation to 'wide' 
st.set_page_config(layout="wide")

# Load environment variables and create sf connection
load_dotenv()
conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USERNAME"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_HOST"),
                role=os.getenv("SNOWFLAKE_ROLE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database='TOKEN_FLOW',
                schema='DICU',
                protocol='https',
                port=443
                )


# Create a cursor object.
cur = conn.cursor()
conn.cursor().execute("USE ROLE ETL")

# Display titles
st.title("Ranked Poll Simulation")
st.write("This app simulates the Instant Runoff Voting algorithm for ranked polls. Showing how choices compete against each other. \n The intent is to help voters when ranking choices in Priotization Sentiment polls.")

# Fetch poll list 
ranked_polls = fetch_poll_list()

# Render select box for poll options
option = st.selectbox(
     'Select a poll',
     ranked_polls.keys()
)

# Display selected poll
st.write("Selected poll:", ranked_polls[option])

def fetch_poll_list() -> Dict[str, str]:
    """
    Fetch list of polls
    """
    
    # Get ranked_polls from GovAlpha/DUX
    url = "https://governance-portal-v2.vercel.app/api/polling/all-polls"
    r = requests.get(url)

    # Fit into a json object
    res = r.json()
    polls = res['polls']

    # Create dictionary storage of poll title -> poll ID
    ranked_polls = {}
    
    # Iterate through polls and populate aforementioned dict
    for poll in polls:
        if poll['voteType'] == 'Ranked Choice IRV':
            ranked_polls[poll['title']] = poll['pollId']

    # Sort dictionary items by pollId
    ranked_polls = dict(sorted(ranked_polls.items(), key=lambda item: item[1], reverse=True))

    return ranked_polls


def fetch_poll_data(cur, option: int) -> Tuple[List[Tuple[Any]]]:
    """
    Fetch necessary poll data
    """
    
    # Fetch poll metadata
    poll_metadata = cur.execute(f"""
        select code, parse_json(options)::string as options
        from mcd.internal.yays
        where type = 'poll'
        and code in ('{option}');
    """).fetchall()

    # Get total voting power of voters that took part in the poll
    total_votes_weight = cur.execute(f"""
        select sum(dapproval) from (select distinct voter, last_value(dapproval) over (partition by voter order by timestamp) as dapproval
        from mcd.public.votes
        where yay = '{option}')
    """).fetchall()[0]

    # Fetch polling results
    poll_results = cur.execute(f"""
        select distinct voter, option, last_value(dapproval) over (partition by voter order by timestamp) as dapproval
        from mcd.public.votes 
        where yay = '{option}';
    """).fetchall()

    return (poll_metadata, total_votes_weight, poll_results)


def poll_iter(poll_metadata: list, poll_results: list) -> pd.DataFrame:
    """
    Generate result dataframe
    """

    # Create options set
    options_set = {int(k): v for k, v in json.loads(poll_metadata[0][1]).items()}

    # Create template of options
    options_layout = {k: v for v, k in enumerate(options_set)}

    # Create round schema & append options layout to every round
    rounds = {rnd: options_layout for rnd in range(len(options_layout))}

    # Populate rounds dictionary
    for _, options, dapproval in poll_results:
        user_ranked_choices = list(map(int, options.split(',')))
        for idx, selection in enumerate(user_ranked_choices):
            rounds[idx][user_ranked_choices[idx]] += dapproval

    # Create list of [voter, dapproval] from poll_results
    voters = [[voter, dapproval] for voter, _, dapproval in poll_results]

    # Create dataframe of voters
    df = pd.DataFrame(voters)
    df.rename(columns={0:'voter', 1:'power'}, inplace=True)

    # Raise error if poll data is empty
    if df.empty:
        raise EmptyPollError
    # Raise error if negative values are found in dapproval column
    elif (df['power']< 0).any():
        raise NegativeDapprovalError

    # Create list of available options
    available_options = list(
        {int(result) for results in [poll_result[1].split(',') for poll_result in poll_results] for result in results}
    )

    # Create list storage of eliminated options
    eliminated_options = []

    # Create list storage for poll rounds
    poll_algo_rounds = []
    
    # Iterate through poll_metadata and populate df
    for pointer in range(len(options_set)):

        # Create f-string of round title for future referencing
        round_title = f"Round {str(pointer + 1)}"

        # Add round column to df
        df[round_title] = ''
        
        # Dictionary storage of final results
        final_results = {pointer: {i: 0 for i in available_options if i not in eliminated_options}}
        
        # Calculate the support for options
        for voter, options, dapproval in poll_results:
            # Iterate through split options
            for option in list(map(int, options.split(','))):
                if option not in eliminated_options:
                    # Populate final_results
                    final_results.setdefault(pointer, {})
                    final_results[pointer].setdefault(option, 0)
                    final_results[pointer][option] += dapproval
                    # Populate result df
                    df.at[df.index[df['voter'] == voter][0], round_title] = options_set[option]
                    # Exit loop
                    break

        # override the 'abstain' option. Currently disabled.
        # for voter, user_choices, dapproval in poll_results:
        #     if len(user_choices.split(',')) == 1 and user_choices.split(',')[0] == '0':
        #         df.at[df.index[df['voter'] == voter][0], category] = options_set['0']

        # Create list of ordered results
        ordered_results = [option for option in sorted(final_results[pointer].values())]
        print(ordered_results)
        # Iterate through final results
        for option in final_results[pointer]:
            # What exactly is this code block doing?
            if final_results[pointer][option] == ordered_results[0]:
                if pointer < (len(options_set) - 1):
                    eliminated_options.append(option)
                    for available_option in available_options:
                        if available_option == option:
                            del(available_option)

        # If the only option selected was one zero, populate final_results with this
        for _, user_choices, dapproval in poll_results:
            if len(user_choices.split(',')) == 1 and user_choices.split(',')[0] == 0:
                final_results[pointer][0] = (final_results[pointer].get(0,0) + dapproval)

    # Final df formatting
    df = df.replace(
            '', np.nan
        ).dropna(
            how='all', axis=1
        ).replace(
            np.nan, 'Discarded votes'
        ).sort_values(
            by='power', ascending=False
        ).reset_index(
            drop=True
    )

    return df


# VIZ

# Create visualization dimensions
dims = list()
for dim in df[df.columns[2:]]:
    dims.append(go.parcats.Dimension(values=df1[dim], label=dim))

# Create parcats trace
color = df1.power

# Capitalize and add space in labels
for dim in dims:
    store = dim['label'].split('_')
    dim['label'] = ' '.join([store[0].capitalize(), str(int(store[1]) + 1)])

# Create figure
fig = go.Figure(
    data = [go.Parcats(
        dimensions=dims,
        line={'color': color, 'colorscale': px.colors.sequential.Burgyl, 'shape':'hspline'},
        counts=[np.float64(i) for i in df1.power],
        hoveron='dimension',)]
    )

# Modify sizing
# Deprecated in favour of st.plotly_chart(use_container_width=True))
# fig.update_layout(
#     autosize=True,
#     width=1800,
#     height=1000,
#     margin=dict(
#         l=250,
#         r=250,
#         b=250,
#         t=250,
#         pad=400
#     )
# )

# Display 'fig' plotly chart
st.plotly_chart(fig, use_container_width=True)

# Final vote prioritization table
st.write("Final vote prioritization (descending).")
    final_options = (eliminated_options + available_options)[::-1]
st.table([options_set[i] for i in final_options])    
        
# Text feeds
st.caption("Built by Data Insights with the support of GovAlpha & DUX.")
st.caption("[Prioritization Framework forum post](https://forum.makerdao.com/t/prioritization-framework-sentiment-polling/15554)")
st.info("Upcoming improvements: \n 1. User input of votes \n 2. Exclude *Abstain* from calculations \n 3. Split app between sentiment & non-sentiment polls")

st.error("There was an issue retrieving the poll results. It could be that the poll does not have enough votes to display, or something much much worse has happened.")