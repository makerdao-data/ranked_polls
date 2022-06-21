import json
import requests
import streamlit as st
from typing import Any, Dict, List, Tuple

@st.experimental_singleton
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
            ranked_polls[str(poll['pollId']) + ' : ' + poll['title']] = poll['pollId']

    # Sort dictionary items by pollId
    ranked_polls = dict(sorted(ranked_polls.items(), key=lambda item: item[1], reverse=True))

    return ranked_polls

@st.experimental_memo(show_spinner=False)
def fetch_poll_data(_cur, option: int) -> Tuple[List[Tuple[Any]]]:
    """
    Fetch necessary poll data
    """
    
    # Fetch poll metadata
    poll_metadata = _cur.execute(f"""
        select code, parse_json(options)::string as options
        from mcd.internal.yays
        where type = 'poll'
        and code in ('{option}');
    """).fetchall()

    # Fetch polling results
    poll_results = _cur.execute(f"""
        select distinct x.voter, last_value(x.option) over (partition by x.voter order by x.timestamp) as option, y.dapproval
        from mcd.public.votes x, (select v.voter, round(sum(v.dstake), 3) dapproval from mcd.public.votes v group by v.voter) y
        where x.yay = '{option}' and
        x.operation = 'CHOOSE' and
        x.voter = y.voter;
    """).fetchall()

    return (poll_metadata, poll_results)