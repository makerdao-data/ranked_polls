import json
import pandas as pd
from streamlit import cache
from numpy import nan
from typing import Tuple
from objects.errors import EmptyPollError, NegativeDapprovalError
import textwrap


def poll_iter(poll_metadata: list, poll_results: list) -> Tuple[pd.DataFrame]:
    """
    Generate voting result dataframe
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

        # Create list of ordered results
        ordered_results = [option for option in sorted(final_results[pointer].values())]

        # Iterate through final results and update available/eliminated poll lists
        for option in final_results[pointer]:
            if final_results[pointer][option] == ordered_results[0]:
                if pointer < (len(options_set) - 1):
                    eliminated_options.append(option)
                    for _ in range(len(available_options) - 1):
                        if available_options[_] == option:
                            available_options.pop(_)

        # If the only option selected was one zero, populate final_results with this
        for _, user_choices, dapproval in poll_results:
            if len(user_choices.split(',')) == 1 and user_choices.split(',')[0] == 0:
                final_results[pointer][0] = (final_results[pointer].get(0,0) + dapproval)

    # Final df formatting
    df = df.replace(
            '', nan
        ).dropna(
            how='all', axis=1
        ).replace(
            nan, 'Discarded votes'
        ).sort_values(
            by='power', ascending=False
        ).reset_index(
            drop=True
    )
    df['Round 1'] = df['Round 1'].map(trimstr)

    # Get final options and place into dataframe
    if available_options[0] not in eliminated_options:
        final_options = (eliminated_options + [available_options[0]])[::-1]
    else:
        final_options = eliminated_options[::-1]
        
    print(available_options)
    print(eliminated_options)
    print(final_options)
    df_options = pd.DataFrame([options_set[i] for i in final_options], columns=["Final option preference (descending)"])
    df_options.index += 1

    print(df_options)

    return (df, df_options)


def trimstr(s: str) -> str:
    """
    Function to shorten string with elipsis
    """

    if len(str(s)) > 49:
        return str(s)[:50] + '...'
    else:
        return s

# func currently not in use
# def customwrap(s,width=50):
#     return "<br>".join(textwrap.wrap(s,width=width))