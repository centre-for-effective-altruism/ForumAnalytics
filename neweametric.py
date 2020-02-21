import pathlib

from measuringmarea import get_good_posts, get_id_from_link
from elephantsandpandas import sql_file_query
from losttheplotly import plotly_ts_ma


def get_all_good_post_views(good_posts):
    """Includes logged out users"""
    project_root = pathlib.Path(__file__).parent.absolute()
    postviews_filename = project_root / 'postviews.sql'
    views_df = sql_file_query(postviews_filename)
    views_df['post_id'] = views_df['path'].map(get_id_from_link)
    return views_df[views_df['post_id'].isin(good_posts)]


def run_new_ea_metric_pipeline(plot=True, online=True):
    good_posts = get_good_posts()
    all_gpvs = get_all_good_post_views(good_posts)
    if plot:
        # Key metric itself
        plotly_ts_ma(
            all_gpvs, title='Views of Good Posts', ma=[7, 30], color='purple',
            start_date='2020-02-18', date_col='first_viewed', online=online,
            multiple_mas_show_daily=True,
        )
