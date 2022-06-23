import os
import json
from numpy import float64
import streamlit as st
from objects.errors import EmptyPollError, NegativeDapprovalError
from methods.conn_init import load_connection
from methods.poll_data import fetch_poll_data, fetch_poll_list
from methods.poll_iter import poll_iter
from methods.viz_gen import viz_gen


def main():
    """
    Main streamlit app
    """

    # Configure page layout and display basic intro
    st.set_page_config(page_title="MakerDAO Ranked Poll Simulation", layout="wide")
    st.title("MakerDAO Ranked Poll Simulation")
    st.write("This app simulates the Instant Runoff Voting algorithm for ranked polls, showing how choices compete against each other.\nThe intent is to help voters when ranking choices in Priotization Sentiment polls.")


    # State management for cursor and ranked poll objects
    if 'cur' not in st.session_state:
        st.session_state.cur = load_connection()
    elif st.session_state.cur.is_closed():
        st.session_state.cur = load_connection()
    if 'ranked_polls' not in st.session_state:
        st.session_state.ranked_polls = fetch_poll_list()
    
    # Render select box for poll options and display selected poll
    option = st.selectbox('Select a poll',st.session_state.ranked_polls.keys())

    # Fetch poll data and analyze df
    poll_metadata, poll_results = fetch_poll_data(st.session_state.cur, st.session_state.ranked_polls[option])
    
    # Display input for simulation initialization
    simulate = st.checkbox("Simulate poll voting")
    # Initialization boolean for Streamlit control flow
    initialized = False
    if simulate:
        # Generate dictionary of available options with abstentions removed 
        sim_options = {k:v for k, v in json.loads(poll_metadata[0][1]).items() if v != 'Abstain'}
        # Create auto-expanded expander where simulation parameters are input
        with st.expander("Simulation parameters", expanded=True):
            # Display input widgets
            sim_selections = st.multiselect(
                "Select options in order of preference.", 
                sim_options.values()
            )
            selection_weight = st.number_input(
                "Input option MKR support.",
                max_value=100000
            )
            # Ensure inputs have been supplied
            if (len(sim_selections) > 0) and (selection_weight != 0):
                # Add new "vote" to poll data
                poll_results.append(
                    (
                        '0x0000000000000000000000000000000000',
                        ','.join([list(sim_options.keys())[list(sim_options.values()).index(i)] for i in sim_selections]),
                        selection_weight
                    )
                )
                # Toggle display initialization
                initialized = True
    else:
        # Toggle display initialization
        initialized = True

    # Attempt to generate result dataframe and catch errors
    if initialized:
        try:
            poll_result_df, poll_options_df = poll_iter(poll_metadata, poll_results)
        except EmptyPollError:
            st.error("Poll dataframe is empty!")
        except NegativeDapprovalError:
            st.error("Negative value detected in dapproval!")

    # Continue if the poll results were successfully obtained
    if 'poll_result_df' in locals():

        # Create figure and display 'fig' plotly chart
        fig = viz_gen(poll_result_df)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("**Count:** total MKR support, **Discarded votes:** votes with all their ranked options eliminated")

        # Final vote prioritization table
        st.table(poll_options_df.astype(str))    
                
        # Footer text feeds
        st.caption("""
                    \nBuilt by Data Insights with the support of GovAlpha & DUX.
                    \n[Prioritization Framework Thread](https://forum.makerdao.com/t/prioritization-framework-sentiment-polling/15554)
                    """
        )
        # st.info("Upcoming improvements: 1. Exclude *Abstain* from calculations \n 2. Split app between sentiment & non-sentiment polls")

if __name__ == '__main__':
    main()