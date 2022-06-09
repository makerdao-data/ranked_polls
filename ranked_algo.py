import os
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

st.set_page_config(layout="wide")

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

# get ranked_polls from GovAlpha/DUX
url = "https://governance-portal-v2.vercel.app/api/polling/all-polls"
r = requests.get(url)
res = r.json()
polls = res['polls']

ranked_polls = {}
for poll in polls:
    if poll['voteType'] == 'Ranked Choice IRV':
        ranked_polls[poll['title']] = poll['pollId']
ranked_polls = {k: v for k, v in sorted(ranked_polls.items(), key=lambda item: item[1], reverse=True)}

st.title("Ranked Poll Sentiment Simulation")

option = st.selectbox(
     'Select a poll',
     ranked_polls.keys()
)

st.write("Selected poll:", ranked_polls[option])

polls_metadata = cur.execute(f"""
    select code, parse_json(options)::string as options
    from mcd.internal.yays
    where type = 'poll'
    and code in ('{ranked_polls[option]}');
""").fetchall()

for code, options in polls_metadata:

    # Get options forom yays table
    poll_metadata = cur.execute(f"""
        select code, parse_json(options)::string as options
        from mcd.internal.yays
        where type = 'poll'
        and code = '{code}';
    """).fetchall()

    # get total voting power of voters that took part in the poll
    total_votes_weight = cur.execute(f"""
        select sum(dapproval) from (select distinct voter, last_value(dapproval) over (partition by voter order by timestamp) as dapproval
        from mcd.public.votes
        where yay = '{code}')
    """).fetchall()[0]


    options_set = json.loads(options)
    options_layout = dict()
    for option in options_set:
        options_layout.setdefault(option, 0)

    poll_results = cur.execute(f"""
        select distinct voter, option, last_value(dapproval) over (partition by voter order by timestamp) as dapproval
        from mcd.public.votes 
        where yay = '{code}';
    """).fetchall()

    # create round schema & append options layout to every round
    # options layout: all possible options to pick for poll
    rounds = OrderedDict()
    for round in range(0, len(options_set)):
        # append a copy of options layout to round
        rounds.setdefault(str(round), copy.deepcopy(options_layout))

    for voter, option, dapproval in poll_results:
        user_ranked_choices = option.split(',')
        round = 0
        while round <= len(user_ranked_choices) -1:
            rounds[str(round)][str(user_ranked_choices[round])] += dapproval
            round += 1

    # ALGO STARTS HERE
    voters = list()
    for voter, user_choices, dapproval in poll_results:
        voters.append([voter, dapproval])

    df = pd.DataFrame(voters)
    if not df.empty:
        df.columns =['voter', 'power']
        print('VOTERS & POWER')
        print(df)
        print()

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

            print(f"""STARTING ROUND: {pointer}""")

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

            # override the 'abstain' option
            for voter, user_choices, dapproval in poll_results:
                if len(user_choices.split(',')) == 1 and user_choices.split(',')[0] == '0':
                    df.at[df.index[df['voter'] == voter][0], category] = '0'

            r = list()
            for option in final_results[str(pointer)]:
                r.append(final_results[str(pointer)][option])
            ordered_results = sorted(r)

            for option in final_results[str(pointer)]:
                if final_results[str(pointer)][option] == ordered_results[0]:
                    if pointer < len(options_set) -1:
                        print(f"""eliminating least supported option: {option}""")
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

            print(final_results)
            print(f"""eliminated options: {eliminated_options}""")
            print(f"""available options: {available_options}""")
            print()

            pointer += 1

        df_x = df.replace([''], np.nan)
        df_y = df_x.dropna(how='all', axis=1)
        df1 = df_y.replace([np.nan], 'Discarded votes')
        print(df1)
        print()

        poll_algo_rounds = list()
        for i in df1.columns:
            if i[:6] == 'round_':
                poll_algo_rounds.append(i)

        # VIZ
        df1.sort_values(by='power', ascending=False, inplace=True)

        # EXTENDED
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
                counts=df1.power,
                hoveron='dimension',)]
            )

        fig.update_layout(
            autosize=True,
            width=1800,
            height=1000,
            margin=dict(
                l=250,
                r=250,
                b=250,
                t=250,
                pad=400
            )
        )

        st.plotly_chart(fig)

    else:
        """
            POLL DIDN'T END YET OR DOESN'T EXIST
        """