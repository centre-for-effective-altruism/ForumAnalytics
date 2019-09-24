"""
ea version of etlw
"""
# from plzhal import google_auth
from etlw import get_collections_cleaned, enrich_collections, write_collections
from losttheplotly import run_plotline
from measuringmarea import run_ea_metric
from utils import timed


@timed
def run(plot=True, metric=True, limit=None):
    """
    TODO; doc
    TODO; take params
    """
    dfs_cleaned = get_collections_cleaned(coll_names=('views',), limit=limit)
    # treat max date in collections as "today"
    today = dfs_cleaned['views']['createdAt'].max().strftime('%Y%m%d')
    dfs_enriched = enrich_collections(dfs_cleaned, date_str=today)
    if metric:
        run_ea_metric(dfs_enriched, plot=plot)
    if plot:
        run_plotline(dfs_enriched, start_date='2019-04-01', size=(700, 350), online=True)

    # TODO;
    # write_collections(dfs_enriched, date_str=today)
    # create_and_update_all_sheets(dfs_enriched, spreadsheet_name=get_config_field('GSHEETS', 'spreadsheet_name'))
    # run_pg_pandas_transfer(dfs_enriched, date_str=date_str)


    # auth = google_auth()
    # good_posts = get_good_posts()

    # getfromgoog
    # getfromdbmaybe
    # plot()

if __name__ == "__main__":
    run(limit=10000, plot=False)
