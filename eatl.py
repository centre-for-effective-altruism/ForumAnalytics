"""
ea version of etlw
"""
from etlw import get_collections_cleaned, enrich_collections, write_collections
from losttheplotly import run_plotline
from measuringmarea import run_ea_metric_pipeline
from utils import timed


@timed
def run(plot=True, metric=True, limit=None):
    dfs_cleaned = get_collections_cleaned(limit=limit)
    # treat max date in collections as "today"
    today = dfs_cleaned['views']['createdAt'].max().strftime('%Y-%m-%d')
    dfs_enriched = enrich_collections(dfs_cleaned, date_str=today)
    if metric:
        run_ea_metric_pipeline(dfs_enriched, plot=plot)
    if plot:
        run_plotline(dfs_enriched, start_date='2019-03-01', size=(700, 350), online=True)

if __name__ == "__main__":
    run()
