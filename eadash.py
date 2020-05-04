"""
EA Dashboard Additions besides headline metrics
"""
from losttheplotly import plotly_ts_ma
import plotly.graph_objs as go
from plotly.offline import iplot


def plot_mau(dfs, online=False):
    dfv = dfs['views']
    dfv['month_year'] = dfv['createdAt'].apply(lambda x: x.strftime('%Y-%m'))
    dfvp = dfv[['month_year', 'userId']]
    dfv1 = dfvp.groupby([dfv.month_year]).nunique()

    # remove last partial month
    dfv1 = dfv1.iloc[:-1, :]

    # hackily remove may (spampocalypse)
    maus = dfv1['userId']
    maus['2019-05'] = 658  # Average of April and June

    title = 'Monthly Active Users (Logged-in Viewers)'

    data = [go.Scatter(
        x=dfv1.index, y=maus, line={'color': 'blue', 'width': 1},
        name='Monthly Active Users'
    )]

    layout = go.Layout(
        autosize=True, width=700, height=400,
        title=title,
        xaxis={'range': ['2019-03', dfv1.index.max()]},
        yaxis={'range': [0, dfv1['userId'].max() * 1.2]}
    )

    fig = go.Figure(data=data, layout=layout)

    iplot(fig, filename=title)


def plot_views_all(dfs, online=False):
    plotly_ts_ma(
        dfs['views'], title='Views [all] by Logged-in Users', ma=[7, 30], color='grey',
        start_date='2019-03-01', date_col='createdAt', exclude_last_period=False,
        multiple_mas_show_daily=False, online=online
    )


def plot_ea_dashboard_other(dfs, online=False):
    plot_views_all(dfs, online=online)
