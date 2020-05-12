"""
EA Dashboard Additions besides headline metrics
"""
from losttheplotly import plotly_ts_ma
import plotly.graph_objs as go
from plotly.offline import iplot


def calculate_mau(df_all):
    df_views = df_all['views']
    df_views['month_year'] = df_views['createdAt'].apply(lambda x: x.strftime('%Y-%m'))
    df_views_months = df_views[['month_year', 'userId']]
    df_monthly_users = df_views_months.groupby([df_views.month_year]).nunique()

    # remove last partial month
    df_monthly_users = df_monthly_users.iloc[:-1, :]

    # hackily remove may (spampocalypse)
    df_slice_monthly_users = df_monthly_users['userId']
    df_slice_monthly_users['2019-05'] = 658  # Average of April and June

    df_monthly_users['maus'] = df_slice_monthly_users
    return df_monthly_users


def plot_mau(df_monthly_users, online=False):
    title = 'Monthly Active Users (Logged-in Viewers)'
    data = [go.Scatter(
        x=df_monthly_users.index, y=df_monthly_users['maus'], line={'color': 'blue', 'width': 1},
        name='Monthly Active Users'
    )]
    layout = go.Layout(
        autosize=True, width=700, height=400,
        title=title,
        xaxis={'range': ['2019-03', df_monthly_users.index.max()]},
        yaxis={'range': [0, df_monthly_users['userId'].max() * 1.2]}
    )
    fig = go.Figure(data=data, layout=layout)
    iplot(fig, filename=title)


def plot_views_all(df_all, online=False):
    plotly_ts_ma(
        df_all['views'], title='Views [all] by Logged-in Users', ma=[7, 30], color='grey',
        start_date='2019-03-01', date_col='createdAt', exclude_last_period=False,
        multiple_mas_show_daily=False, online=online
    )


def plot_ea_dashboard_other(df_all, online=False):
    plot_views_all(df_all, online=online)
    df_monthly_users = calculate_mau(df_all)
    plot_mau(df_monthly_users, online=online)
