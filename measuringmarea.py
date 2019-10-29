from gspread_pandas import Spread, Client
import re
import numpy as np
import pandas as pd
import plotly
import plotly.plotly as py
import plotly.graph_objs as go
from plotly.offline import init_notebook_mode, iplot

from utils import get_config_field

post_id_regex = r"/posts/(\w+)/"


def get_id_from_link(link):
    # print(link)
    matches = re.finditer(post_id_regex, link, re.MULTILINE)
    # print('matches', matches)
    if not matches:
        return ''
    for match in matches:
        # print('match', match)
        if not match.groups():
            return ''
        for group in match.groups():
            # print('group', group)
            return group


def get_good_posts(ids=True):
    sheet_id = get_config_field('GSHEETS', 'good_posts_sheet')
    # user = get_config_field('GSHEETS', 'user')
    goodPostsSpreadsheet = Spread(sheet_id, sheet='Excellent Post URLs')
    goodPostsDf = goodPostsSpreadsheet.sheet_to_df(index=0, header_rows=0)
    goodPostsArr = goodPostsDf.values[:, 0]
    if not ids:
        return set(goodPostsArr)
    goodPostsIds = [get_id_from_link(link) for link in goodPostsArr]
    return set(goodPostsIds)


def get_post_dates(post_ids):
    pass


def get_good_views(dfs):
    good_posts = get_good_posts()
    # print('gps', good_posts, len(good_posts))
    views = dfs['views']
    # print(views.columns)
    # print('views', views)
    user_id_col = list(views.columns.values).index('userId')          # evaluates to 1
    document_id_col = list(views.columns.values).index('documentId')  # evaluates to 2
    created_at_col = list(views.columns.values).index('createdAt')    # evaluates to 3
    # print(user_id_col, document_id_col)
    good_views = []
    already_stored_views = set()
    for view in views.values:
        if (view[document_id_col] in good_posts and
                (view[user_id_col], view[document_id_col]) not in already_stored_views):
            good_views.append([view[user_id_col], view[document_id_col], view[created_at_col]])
            already_stored_views.add((view[user_id_col], view[document_id_col]))
    good_views = pd.DataFrame(data=good_views, columns=['user_id', 'document_id', 'created_at'])
    return good_views


def plot_good_views(good_views):
    good_views_by_day = good_views.set_index('created_at').resample('D')['document_id'].count()
    good_views_by_day = good_views_by_day.reset_index().iloc[:-1]
    # return good_views_by_day
    good_views_by_week_moving_average = good_views_by_day.set_index('created_at')['document_id'].rolling(7).mean().round(1).reset_index()

    date_col = 'created_at'
    title = 'Views of Good Posts by Logged-in Users'
    color = 'red'
    size = (600, 500)
    start_date = '2019-03-01'
    end_date = good_views['created_at'].max().strftime('%Y-%m-%d')

    data = [
        go.Scatter(
            x=good_views_by_day[date_col],
            y=good_views_by_day['document_id'].round(1),
            line={'color': color, 'width': 0.5},
            name='daily-value',
            hoverinfo='x+y+name'
        ),
        go.Scatter(
            x=good_views_by_week_moving_average[date_col],
            y=good_views_by_week_moving_average['document_id'].round(1),
            line={'color': color, 'width': 4}
        )
    ]

    layout = go.Layout(
        autosize=True, width=size[0], height=size[1],
        title=title,
        xaxis={'range': [start_date, end_date], 'title': None},
        yaxis={'range': [0, good_views_by_day.set_index(date_col)[start_date:]['document_id'].max() * 1.1], 'title': 'Views'}
    )

    fig = go.Figure(data=data, layout=layout)

    plotly.tools.set_credentials_file(
        username=get_config_field('PLOTLY', 'username'),
        api_key=get_config_field('PLOTLY', 'api_key')
    )
    init_notebook_mode(connected=True)

    if get_config_field('PLOTLY', 'go_live') == 'True':
        py.iplot(fig, filename='Good Post Views Metric')
    else:
        iplot(fig, filename='Good Post Views Metric')


def run_ea_metric_pipeline(dfs, plot=True):
    # good_posts = get_good_posts
    good_views = get_good_views(dfs) # , good_posts
    if plot:
        plot_good_views(good_views)
        # plot_good_posts_by_time()
    # print(good_views)
