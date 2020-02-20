from measuringmarea import get_good_posts, plot_good_posts
from elephantsandpandas import db_query
from losttheplotly import plotly_ts_ma


def get_all_post_views():
    db_query()


def run_ea_metric_pipeline(dfs, plot=True, online=True):
    good_posts = get_good_posts()
    if plot:
        # Good posts number tracker
        plot_good_posts(dfs, good_posts, online=online)
        # Key metric itself
        plotly_ts_ma(
            good_views, title='Views of Good Posts', ma=[7, 30], color='purple',
            start_date='2020-02-18', date_col='created_at', online=online,
            multiple_mas_show_daily=True,
        )
