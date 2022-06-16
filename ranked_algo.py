import os
from re import X
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

st.title("Ranked Poll Simulation")
st.write("This app simulates the Instant Runoff Voting algorithm for ranked polls. Showing how choices compete against each other. \n The intent is to help voters when ranking choices in Priotization Sentiment polls.")

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

    # Get options from yays table
    poll_metadata = cur.execute(f"""
        select code, parse_json(options)::string as options
        from mcd.internal.yays
        where type = 'poll'
        and code = '{code}';
    """).fetchall()

    # get total voting power of voters that took part in the poll
    total_votes_weight = cur.execute(f"""
        select sum(dapproval) from (select distinct voter, last_value(dapproval) over (partition by voter order by order_index) as dapproval
        from mcd.public.votes
        where yay = '{code}')
    """).fetchall()[0]

    options_set = json.loads(options)
    options_layout = dict()
    for option in options_set:
        options_layout.setdefault(option, 0)

    poll_results = cur.execute(f"""
        select distinct voter, option, last_value(dapproval) over (partition by voter order by order_index) as dapproval
        from mcd.public.votes 
        where yay = '{code}';
    """).fetchall()

    # clean duplicated voting
    poll_results_dict = {}
    for voter, option, dapproval in poll_results:
        poll_results_dict[voter] = {}
        poll_results_dict[voter] = dict(
            option=option,
            dapproval=dapproval
        )
    
    poll_results = []
    for voter in poll_results_dict:
        poll_results.append([voter, poll_results_dict[voter]['option'], poll_results_dict[voter]['dapproval']])


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
    if ((not df.empty) & (not (df[1]< 0).any())):
        df.columns =['voter', 'power']
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

        st.caption("**Count**: total MKR support, **Discarded votes**: votes with all their ranked options eliminated")

        # Final vote prioritization table
        st.write("**Final ranking**")

        final_options = (eliminated_options + available_options)[::-1]
        table_final_options = [options_set[i] for i in final_options]
        df_options = pd.DataFrame(table_final_options, columns=["Option"])
        df_options.index += 1 
        #df_options.rename(columns = {' ':'Ranking', '0':'Option'}, inplace = True)

        st.dataframe(df_options)

        #style = df_options.style #.hide_index()
        #style.hide_columns()
        #st.write(style.to_html(), unsafe_allow_html=True)



        # Text feeds
        st.write(" ")
        st.write(" ")
        st.write("Built by Data Insights with the support of GovAlpha & DUX. Read more about here [Prioritization Framework forum post](https://forum.makerdao.com/t/prioritization-framework-sentiment-polling/15554)")
        st.info("Upcoming improvements: \n 1. User input of votes \n 2. Exclude *Abstain* from calculations \n 3. Split app between sentiment & non-sentiment polls")

    else:
        st.error("There was an issue retrieving the poll results. It could be that the poll does not have enough votes to display, or something much much worse has happened.")