from gspread_pandas import Spread
import re
import pandas as pd
from losttheplotly import plotly_ts_ma
import ast

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
    doc_id = get_config_field('GSHEETS', 'good_posts_doc_id')
    sheet_ndx = int(get_config_field('GSHEETS', 'good_posts_sheet_ndx'))
    goodPostsSpreadsheet = Spread(doc_id, sheet=sheet_ndx)
    goodPostsDf = goodPostsSpreadsheet.sheet_to_df(index=0, header_rows=0)
    goodPostsArr = goodPostsDf.values[:, 0]
    if not ids:
        return set(goodPostsArr)
    goodPostsIds = [get_id_from_link(link) for link in goodPostsArr]
    return set(goodPostsIds)


def get_good_views(dfs, good_posts):
    staff_user_ids = set(ast.literal_eval(get_config_field('VARS', 'staff_user_ids')))
    views = dfs['views']
    user_id_col = list(views.columns.values).index('userId')          # evaluates to 1
    document_id_col = list(views.columns.values).index('documentId')  # evaluates to 2
    created_at_col = list(views.columns.values).index('createdAt')    # evaluates to 3
    good_views = []
    already_stored_views = set()
    for view in views.values:
        if (view[document_id_col] in good_posts and
                (view[user_id_col], view[document_id_col]) not in already_stored_views and
                view[user_id_col] not in staff_user_ids):
            good_views.append([view[user_id_col], view[document_id_col], view[created_at_col]])
            already_stored_views.add((view[user_id_col], view[document_id_col]))
    good_views = pd.DataFrame(data=good_views, columns=['user_id', 'document_id', 'created_at'])
    return good_views


def plot_good_posts(dfs, good_posts, online=False):
    dfp = dfs['posts']
    df_good_posts_full = dfp[dfp['_id'].isin(good_posts)]
    plotly_ts_ma(
        df_good_posts_full, title='Good Posts', ma=[7, 30],
        color='green', start_date='2019-03-01', online=online
    )


def run_ea_metric_pipeline(dfs, plot=True, online=True):
    good_posts = get_good_posts()
    good_views = get_good_views(dfs, good_posts)
    if plot:
        # Good posts number tracker
        plot_good_posts(dfs, good_posts, online=online)
        # Key metric itself
        plotly_ts_ma(
            good_views, title='Views of Good Posts by Logged-in Users', ma=[7, 30], color='red',
            start_date='2019-03-01', date_col='created_at', online=online
        )
