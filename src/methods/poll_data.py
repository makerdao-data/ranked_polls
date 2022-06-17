import json
import requests
from typing import Any, Dict, List, Tuple

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