import pandas as pd
from numpy import float64
import plotly.express as px
import plotly.graph_objects as go

def viz_gen(poll_result_df: pd.DataFrame) -> go.Figure:
    """
    Function to generate 
    """
    
    # Create visualization dimensions
    dims = [go.parcats.Dimension(values=poll_result_df[dim], label=dim) for dim in poll_result_df[poll_result_df.columns[2:]]]

    # Create figure
    fig = go.Figure(
        data = [go.Parcats(
            dimensions=dims,
            line={'color': poll_result_df.power, 'colorscale': px.colors.sequential.Burgyl, 'shape':'hspline'},
            counts=[float64(i) for i in poll_result_df.power],
            hoveron='dimension',)]
        )
    
    fig.update_layout(
        autosize=False,
        margin=dict(
            l=200,
            r=200,
            b=25,
            t=100,
            pad=4
        ),
    )

    return fig