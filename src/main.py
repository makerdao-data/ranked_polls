import os
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
    st.write("Selected poll:", st.session_state.ranked_polls[option])

    # Fetch poll data and analyze df
    poll_metadata, poll_results = fetch_poll_data(st.session_state.cur, st.session_state.ranked_polls[option])

    # Attempt to generate result dataframe and catch errors
    try:
        poll_result_df, poll_options_df = poll_iter(poll_metadata, poll_results)
    except EmptyPollError:
        st.error("Poll dataframe is empty!")
    except NegativeDapprovalError:
        st.error("Negative value detected in dapproval!")

    # Continue if the poll results were successfully obtained
    if 'poll_result_df' in locals():

        # Create figure
        fig = viz_gen(poll_result_df)

        # Display 'fig' plotly chart
        st.plotly_chart(fig, use_container_width=True)

        st.caption("**Count:** total MKR support, **Discarded votes:** votes with all their ranked options eliminated")

        # Final vote prioritization table
        st.write("Final vote prioritization (descending).")
        st.table(poll_options_df.astype(str))    
                
        # Footer text feeds
        st.caption("""
                    Built by Data Insights with the support of GovAlpha & DUX.\n
                    [Prioritization Framework forum post](https://forum.makerdao.com/t/prioritization-framework-sentiment-polling/15554)
                    """
        )
        st.info("Upcoming improvements: \n 1. User input of votes \n 2. Exclude *Abstain* from calculations \n 3. Split app between sentiment & non-sentiment polls")

if __name__ == '__main__':
    main()