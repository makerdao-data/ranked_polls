import os
from re import X
from typing import Dict, List
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

class Error(Exception):
    """Base class for other exceptions"""
    pass


class EmptyPollError(Error):
    """Raised when a poll dataframe is empty"""
    pass

class NegativeDapprovalError(Exception):
    """Raised when negative values are found in dapproval data"""
    pass

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
    Fetched list 
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

def fetch_poll_data(cur, option: int) -> List[List[tuple]]:
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


def poll_iter(poll_metadata: list, total_votes_weight: list, poll_results: list) -> pd.DataFrame:
    """
    Generate result dataframe
    """

    # Create template of options
    options_layout = {k: v for v, k in enumerate(json.loads(poll_metadata[1]))}

    # Create round schema & append options layout to every round
    rounds = {str(round): options_layout for round in range(len(options_layout))}

    # Populate rounds dictionary
    for _, option, dapproval in poll_results:
        user_ranked_choices = option.split(',')
        for idx, selection in enumerate(user_ranked_choices):
            rounds[str(idx)][str(user_ranked_choices[idx])] += dapproval

    # Create list of [voter, dapproval] from poll_results
    voters = [[voter, dapproval] for voter, _, dapproval in poll_results]

    # Create dataframe of voters
    df = pd.DataFrame(voters)

    # Raise error if poll data is empty
    if df.empty:
        raise EmptyPollError
    # Raise error if negative values are found in dapproval column
    elif (df[1]< 0).any():
        raise NegativeDapprovalError

    # Rename dataframe columns
    df.rename(columns=['voter', 'power'], inplace=True)

    # Log for debugging
    print(f"VOTERS & POWER\n{df}")

    available_options = list()
    for voter, user_choices, dapproval in poll_results:
        for i in user_choices.split(','):
            if i not in available_options:
                available_options.append(i)

    eliminated_options = list()

    poll_algo_rounds = list()
    for pointer in range(0, len(options_set)):

        # add round (category) column to df
        df[f'round_{pointer}'] = ''
        category = f'round_{pointer}'
        poll_algo_rounds.append(category)

        final_results = dict()
        final_results.setdefault(str(pointer), {})
        for i in available_options:
            if i not in eliminated_options:
                final_results[str(pointer)].setdefault(str(i), 0)

        print(f"STARTING ROUND: {pointer}")

        # counting the support for options
        for voter, user_choices, dapproval in poll_results:
            for i in user_choices.split(','):
                if i not in eliminated_options:
                    final_results.setdefault(str(pointer), {})
                    final_results[str(pointer)].setdefault(str(i), 0)
                    final_results[str(pointer)][str(i)] += dapproval

                    print(options_set[str(i)])
                    df.at[df.index[df['voter'] == voter][0], category] = options_set[str(i)]

                    break

        # override the 'abstain' option. Currently disabled.
        # for voter, user_choices, dapproval in poll_results:
        #     if len(user_choices.split(',')) == 1 and user_choices.split(',')[0] == '0':
        #         df.at[df.index[df['voter'] == voter][0], category] = options_set['0']

        r = list()
        for option in final_results[str(pointer)]:
            r.append(final_results[str(pointer)][option])
        ordered_results = sorted(r)

        for option in final_results[str(pointer)]:
            if final_results[str(pointer)][option] == ordered_results[0]:
                if pointer < len(options_set) -1:
                    print(f"eliminating least supported option: {option}")
                    least_supported_option = option
                    eliminated_options.append(least_supported_option)
                    c = 0
                    while c <= len(available_options) -1:
                        if str(available_options[c]) == least_supported_option:
                            available_options.pop(c)
                        c += 1

        print(f"ROUND {pointer} SUMMARY")
        for voter, user_choices, dapproval in poll_results:
            if len(user_choices.split(',')) == 1 and user_choices.split(',')[0] == '0':
                final_results[str(pointer)]['0'] = 0
                final_results[str(pointer)]['0'] += dapproval

        print(f"{final_results}\neliminated options: {eliminated_options}\navailable options: {available_options}")

        pointer += 1

    df1 = df.replace('', np.nan).dropna(how='all', axis=1).replace(np.nan, 'Discarded votes')
    df1.sort_values(by='power', ascending=False, inplace=True)
    #df1 = df1.T.drop_duplicates().T
    print(df1)

    poll_algo_rounds = list()
    for i in df1.columns:
        if i[:6] == 'round_':
            poll_algo_rounds.append(i)

# VIZ

# Create visualization dimensions
dims = list()
for dim in poll_algo_rounds:
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

else:
st.error("There was an issue retrieving the poll results. It could be that the poll does not have enough votes to display, or something much much worse has happened.")