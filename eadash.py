"""
EA Dashboard Additions besides headline metrics
"""
from losttheplotly import plotly_ts_ma


def plot_views_all(dfs, online=False):
    plotly_ts_ma(
        dfs['views'], title='Views [all] by Logged-in Users', ma=[7, 30], color='grey',
        start_date='2019-03-01', date_col='createdAt', exclude_last_period=False,
        multiple_mas_show_daily=False, online=online
    )


def plot_ea_dashboard_other(dfs, online=False):
    plot_views_all(dfs, online=online)
