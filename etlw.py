import pandas as pd
import datetime
import matplotlib
import numpy as np

from pymongo import MongoClient
import pathlib
import html2text

import os
import shutil
import configparser

from losttheplotly import run_plotline
from cellularautomaton import *
from karmametric import run_metric_pipeline
from flipthetable import run_pg_pandas_transfer

from utils import timed, print_and_log, get_config_field

MONGO_DB_NAME = get_config_field('MONGODB', 'db_name')
MONGO_DB_URL = get_config_field('MONGODB', 'prod_db_url')
BASE_PATH = get_config_field('PATHS','base')
ENV = get_config_field('ENV', 'env')


def get_mongo_db_object():
    client = MongoClient(MONGO_DB_URL)
    db = client[MONGO_DB_NAME]
    return db


def get_collection(coll_name, db, projection=None, query_filter=None, limit=None):
    """
    Downloads and returns single collection from MongoDB and returns dataframe.

    Optional query filter can be applied (useful for downloading logins post-views from events table.
    Returns a dataframe.
    """

    kwargs = {} # necessary because pymongo function doesn't accept limit=None
    if limit:
        kwargs.update({'limit':limit})

    print_and_log('{} download started at {}. . .'.format(coll_name, datetime.datetime.today()))
    coll_list = db[coll_name].find(query_filter, projection=projection, **kwargs)
    coll_df = pd.DataFrame(list(coll_list))
    print_and_log('{} download completed at {}!'.format(coll_name, datetime.datetime.today()))

    return coll_df


@timed
def get_collection_cleaned(coll_name, db,
                           limit=None):  # (name of collection, MongoDB object, read/write arg bundle) -> dataframe
    """
    Downloads, *processes* and returns single collection from MongoDB.

     Processing retains only some columns, fills in missing values, and casts datatypes.
     Processing is performed using a custom function for each collection.
     Collection must be one of ['post', 'comments', 'users', 'votes', 'views' (lwevents with post-view filter)

     Optionally writes to file based on io_config argument bundle.

     Returns a dataframe.

     """

    selected_columns = {
        'posts': [
            'af',
            '_id',
            'userId',
            'title',
            'postedAt',
            'excerpt',
            # 'contents', #not using at present, is large.
            'baseScore',
            'afBaseScore',
            'score',
            'viewCount',
            'clickCount',
            'commentCount',
            'wordCount',
            'commenters',
            'createdAt',
            'frontpageDate',
            'curatedDate',
            'draft',
            'url',
            'slug',
            'legacy',
            'question',
            'userAgent',
            'canonicalCollectionSlug',
            # 'moderationGuidelinesHtmlBody',
            # 'deleted', #there's only a single post with this flag, remove so as make sampling posts not fail
            'legacySpam',
            'isEvent',
            'plaintextExcerpt',
            'website',
            'authorIsUnreviewed',
            'status'
        ],
        'comments': [
            '_id',
            'af',
            'userId',
            'postId',
            'postedAt',
            'createdAt',
            'baseScore',
            'afBaseScore',
            'score',
            'deleted',
            'parentCommentId',
            'legacy',
            'draft',
            'answer',
            'parentAnswerId',
            'userAgent',
            'wordCount',
            # 'contents'
        ],
        'users': [
            '_id',
            'username',
            'displayName',
            'createdAt',
            'postCount',
            'commentCount',
            'frontpagePostCount',
            'karma',
            'legacyKarma',
            'bio',
            'deleted',
            'banned',
            'email',
            'legacy',
            'afKarma',
            'moderationGuidelinesHtmlBody',
            'subscribers',
            'shortformFeedId',
            'signUpReCaptchaRating',
            'reviewedByUserId'
        ],
        'votes': [
            'afPower',
            'collectionName',
            'documentId',
            'legacy',
            'power',
            'userId',
            'voteType',
            'votedAt',
            'cancelled',
            'isUnvote'
        ],
        'views': [
            'userId',
            'documentId',
            'createdAt',
            # 'name',
            # 'legacy',
            # 'important',
            # 'intercom',
        ],
        'logins': [
            '_id',
            'userId',
            'properties',
            'createdAt',
            'schema'
        ]
    }

    cleaning_functions = {
        'users': clean_raw_users,
        'posts': clean_raw_posts,
        'votes': clean_raw_votes,
        'views': clean_raw_views,
        'comments': clean_raw_comments,
        'logins': clean_raw_logins
    }

    query_filters = {'logins': {'name': 'login'}, 'views': {'name': 'post-view'}}

    def name_check(coll_name):
        # ugly, but how else to do it?
        if coll_name in ('views', 'logins'):
            return 'lwevents'
        else:
            return coll_name

    raw_collection_df = get_collection(
        db=db,
        coll_name=name_check(coll_name),
        projection=selected_columns[coll_name],
        query_filter=query_filters.get(coll_name),
        limit=limit
    )
    # when number of items pulled is small, some fields aren't present in any  of the items returned, which causes errors when you try to manipulate that column.
    for col in selected_columns[coll_name]:
        if col not in raw_collection_df.columns:
            raw_collection_df.loc[:, col] = np.nan

    cleaned_collection_df = cleaning_functions[coll_name](raw_collection_df)

    # if io_config is not None:
    #
    #     if functions_and_filters[coll_name][1] is not None:
    #         if functions_and_filters[coll_name][1]['name'] == 'post-view':
    #             coll_name = 'views'  # haven't figured out a more elegant way to do this
    #
    #     write_collection(
    #         coll_name=coll_name,
    #         coll_df=df,
    #         io_config=io_config
    #     )

    return cleaned_collection_df


@timed
def get_collections_cleaned(coll_names=('comments', 'views', 'votes', 'posts', 'users'), limit=None):
    """
    For all collections in argument, downloads and cleans them.
    Returns a dict of dataframes.
    """
    db = get_mongo_db_object()
    colls_dict = {name: get_collection_cleaned(name, db, limit) for name in coll_names}

    return colls_dict


@timed
def write_collection(coll_name, coll_df, date_str):  # (string, df, arg_bundle) -> None
    # hardcoded to write to db directory. wonderful hardcoding
    # this function really needs some cleanin'

    print_and_log('Writing {} to disk.'.format(coll_name))

    directory = BASE_PATH + '{folder}/{date}'.format(folder='processed', date=date_str)  # vestigial folder structure
    pathlib.Path(directory).mkdir(exist_ok=True)
    coll_df.to_csv(directory + '/{}.csv'.format(coll_name), index=False)

    print_and_log('Writing {} to disk completed.\n'.format(coll_name))

    return None


def write_collections(dfs, date_str):  # dict[{string: df}] -> None
    """Writes all dataframes in dataframe dictionary to file."""
    [write_collection(coll_name, coll_df, date_str) for coll_name, coll_df in dfs.items()]
    return None


def get_list_of_dates():
    """Searches folder path for list of folders by dates with data downloads

    Returns a list of folder/directory names.
    """
    directory = BASE_PATH + '{folder}'.format(folder='processed')

    date_folders = [x[0] for x in os.walk(directory)][1:]
    date_folders.sort(reverse=True)

    return date_folders


@timed
def clean_up_old_files(days_to_keep=1):
    """Function for deleting old file downloads. Accepts """

    date_folders = get_list_of_dates()


@timed
def load_from_file(date_str, coll_names=('votes', 'views', 'comments', 'posts', 'users')):
    """Loads database collections from csvs to dataframes, ensures datetimes load correctly."""

    @timed
    def read_csv(coll_name):

        print_and_log('Reading {}'.format(coll_name))
        df = pd.read_csv(complete_path_to_file(coll_name), dtype=read_dtypes_arg[coll_name])

        # read in all datetime types correctly
        for dt_col in ['postedAt', 'createdAt', 'votedAt', 'startTime', 'endTime',
                       'earliest_comment', 'most_recent_comment', 'earliest_vote', 'most_recent_vote',
                       'most_recent_post', 'earliest_post', 'most_recent_activity', 'earliest_activity',
                       'true_earliest', 'curatedAt', 'earliest_view'
                       ]:
            if dt_col in df.columns:
                df.loc[:, dt_col] = pd.to_datetime(df[dt_col])

        return df

    def complete_path_to_file(coll_name):
        return BASE_PATH + '{folder}/{date}/{coll_name}.csv'.format(folder='processed', date=date_str, coll_name=coll_name)

    if date_str == 'most_recent':
        date_str = get_list_of_dates()[0][-8:]

    read_dtypes_arg = {
        'users': None,
        'posts': None,
        'comments': None,
        'votes': {'collectionName': 'category', 'voteType': 'category', 'afPower': 'int8', 'power': 'int8'},
        'views': None
    }

    print_and_log("Files to be loaded:")
    [print(complete_path_to_file(coll_name)) for coll_name in coll_names]

    return {coll_name: read_csv(coll_name) for coll_name in coll_names}


def htmlBody2plaintext(html_series, ignore_links=False):
    h = html2text.HTML2Text()
    h.ignore_links = ignore_links

    return html_series.apply(lambda x: h.handle(x))


def remove_mjx(df, preserve_original=False):
    """Function use to remove stray mjx in content bodies, currently not used."""
    if preserve_original:
        df['body_original'] = df['body'].copy()

    df.loc[:, 'body'] = df['body'].fillna('')
    ix = df['body'].str.contains('.mjx')
    df.loc[ix, 'body'] = htmlBody2plaintext(df.loc[ix, 'htmlBody'])
    return df


def convertContents2Body(df):
    index = df['contents'].str['html'].notnull() & df['body'].isnull()
    df.loc[index, 'body'] = htmlBody2plaintext(df.loc[index, 'contents'].str['html'])
    return df


def clean_raw_posts(posts):
    """
    Takes raw dataframe of posts collections and fixes datatypes and similar.
    Casting important for memory optimization.
    """

    # posts = remove_mjx(posts)
    # posts = convertContents2Body(posts)

    # ensure proper datetime encoding
    posts.loc[:, 'postedAt'] = pd.to_datetime(posts['postedAt'])
    posts.loc[:, 'createdAt'] = pd.to_datetime(posts['createdAt'])

    # fill in missing values and cast to appropriate types
    for col in ['viewCount', 'clickCount', 'commentCount', 'wordCount']:
        posts.loc[:, col] = posts.loc[:, col].fillna(0).astype(int)
    for col in ['draft', 'legacy', 'af', 'question', 'legacySpam', 'isEvent']:
        posts.loc[:, col] = posts.loc[:, col].fillna(False).astype(bool)

    return posts


def clean_raw_comments(comments):
    comments.loc[:, 'postedAt'] = pd.to_datetime(comments['postedAt'])
    comments.loc[:, 'createdAt'] = pd.to_datetime(comments['createdAt'])
    for col in ['deleted', 'legacy', 'af', 'answer']:
        comments.loc[:, col] = comments.loc[:, col].fillna(False).astype(bool)

    return comments


def clean_raw_users(users):
    """
    Takes raw dataframe of users collections and returns subset of columns + processes columns.
    Casting is important for memory optimization.
    """
    users.loc[:, 'createdAt'] = pd.to_datetime(users['createdAt'])
    users.loc[:, 'afKarma'] = users['afKarma'].fillna(0)
    for col in ['postCount', 'commentCount', 'frontpagePostCount', 'karma', 'legacyKarma']:
        users.loc[:, col] = users.loc[:, col].fillna(0).astype(int)
    for col in ['deleted', 'legacy', 'banned']:
        users.loc[:, col] = users.loc[:, col].fillna(False).astype(bool)

    return users


def clean_raw_votes(votes):
    """
    Takes raw dataframe of votes collections and returns subset of columns + processes columns.
    Casting here *very* important for memory optimization. Use categories and small integer types.
    """

    votes.loc[:, 'cancelled'] = votes['cancelled'].fillna(False).astype(bool)
    votes.loc[:, 'isUnvote'] = votes['isUnvote'].fillna(False).astype(bool)
    votes = votes.loc[~votes['cancelled'], :]

    votes.loc[:, 'afPower'] = votes['afPower'].fillna(0).astype('int8')
    votes.loc[:, 'collectionName'] = votes['collectionName'].astype('category')
    votes.loc[:, 'legacy'] = votes['legacy'].fillna(False).astype(bool)
    votes.loc[:, 'power'] = votes['power'].astype('int8')
    votes.loc[:, 'voteType'] = votes['voteType'].astype('category')
    votes.loc[:, 'votedAt'] = pd.to_datetime(votes['votedAt'])
    votes.loc[:, 'userId'] = votes['userId'].astype(str)

    return votes


def clean_raw_views(views):
    """Takes raw dataframe of views collection and returns filtered/processed dataframe."""

    views.loc[:, 'createdAt'] = pd.to_datetime(views['createdAt'])
    # views.loc[:, 'name'] = views['name'].astype('category') #only ever contains "post-view"
    # views.loc[:, 'legacy'] = views['legacy'].fillna(False).astype(bool) #never use it, but want to remember it's there

    return views


def clean_raw_logins(logins_df):
    """Takes raw dataframe of logins collection and returns filtered/processed dataframe."""

    logins_parsed = logins_df
    logins_parsed.loc[:, 'createdAt'] = pd.to_datetime(logins_parsed['createdAt'])

    return logins_parsed


def calculate_vote_stats_for_content(votes_df):
    """Accepts dataframe on votes, aggregates to document level and returns stats.

    Returns stats about kinds of votes placed (small/big,up/down) and when last vote was made.
    """

    votes_df['voteType'] = votes_df['voteType'].astype(str)

    vote_type_stats = votes_df.groupby(['documentId', 'voteType']).size().unstack(level='voteType').fillna(0).astype(
        int)

    for col in ['smallUpvote', 'smallDownvote', 'bigUpvote', 'bigDownvote']:
        if col not in vote_type_stats.columns:
            vote_type_stats[col] = 0

    vote_type_stats['num_votes'] = vote_type_stats.sum(axis=1)
    vote_type_stats['percent_downvotes'] = (
            vote_type_stats[['smallDownvote', 'bigDownvote']].sum(axis=1) / vote_type_stats['num_votes']).round(2)
    vote_type_stats['percent_bigvotes'] = (
            vote_type_stats[['bigUpvote', 'bigDownvote']].sum(axis=1) / vote_type_stats['num_votes']).round(2)
    vote_date_stats = votes_df.groupby('documentId').apply(
        lambda x: pd.Series(data={'most_recent_vote': x['votedAt'].max()}))

    vote_stats = vote_type_stats.merge(vote_date_stats, left_index=True, right_index=True)

    return vote_stats


def calculate_vote_stats_for_users(votes_df):
    """Accepts dataframe on votes, aggregates to users and returns stats for users.

    Returns stats about kinds of votes placed (small/big,up/down) and when last and earliest votes were made.
    """

    votes_df['voteType'] = votes_df['voteType'].astype(str)

    vote_date_stats = votes_df.groupby('userId').apply(lambda x: pd.Series(data={ 'most_recent_vote': x['votedAt'].max(),
                                                                                 'earliest_vote': x['votedAt'].min()}))

    vote_type_stats = votes_df.groupby(['userId', 'voteType']).size().unstack(level='voteType').fillna(0).astype(int)
    for col in ['smallUpvote', 'smallDownvote', 'bigUpvote', 'bigDownvote']:
        if col not in vote_type_stats.columns:
            vote_type_stats[col] = 0

    vote_type_stats['num_votes'] = vote_type_stats.sum(axis=1)

    vote_type_stats['percent_downvotes'] = (
            vote_type_stats[['smallDownvote', 'bigDownvote']].sum(axis=1) / vote_type_stats['num_votes']).round(2)
    vote_type_stats['percent_bigvotes'] = (
            vote_type_stats[['bigUpvote', 'bigDownvote']].sum(axis=1) / vote_type_stats['num_votes']).round(2)

    vote_stats = vote_date_stats.merge(vote_type_stats, left_index=True, right_index=True)

    return vote_stats


def calc_user_view_stats(views_df):
    view_date_stats = views_df.groupby('userId')['createdAt'].agg(
        {'num_views': 'count', 'most_recent_view': 'max', 'earliest_view': 'min'})
    view_post_stats = views_df.groupby('userId')['documentId'].nunique().to_frame('num_distinct_posts_viewed')

    views_df['date'] = views_df['createdAt'].dt.date
    views_last_30 = views_df[views_df['createdAt'] >= views_df['createdAt'].max() - pd.Timedelta(30 - 1, unit='d')]
    view_presence_stats = views_last_30.groupby('userId')['date'].nunique().to_frame('num_days_present_last_30_days')
    view_presence_stats['num_days_present_last_30_days'] = view_presence_stats['num_days_present_last_30_days'].fillna(
        0)

    view_stats = (
        view_date_stats
            .merge(view_post_stats, left_index=True, right_index=True, how='outer')
            .merge(view_presence_stats, left_index=True, right_index=True, how='outer')
    )

    return view_stats


def calc_user_comment_stats(comments):  # df -> df
    """Calculates aggregates statistics over a user's comments."""

    comment_stats = comments.groupby('userId')['postedAt'].agg({'total_comments': 'size',
                                                                'earliest_comment': 'min',
                                                                'most_recent_comment': 'max'})

    return comment_stats


def calc_user_post_stats(posts):  # df -> df
    """Calculates aggregate statistics over a user's posts."""

    # currently comment out all word character related stuff due to mess.
    # dfp['characters'] = dfp['body'].str.len()
    # dfp['words'] = (dfp['characters']/6).round(2)
    # dfp.loc[dfp['body'].str.contains('image/png|image/jpeg') == True, 'characters'] = np.nan

    # dfp['frontpageDate'] = dfp['frontpageDate'].replace(0, np.nan) # this should *not* be necessary. Remember to track it upstream.
    posts['frontpaged'] = posts['frontpageDate'].notnull()
    postsByUser = posts[~posts['draft']].groupby('userId')

    post_date_stats = postsByUser['postedAt'].agg(
        {'total_posts': 'size', 'earliest_post': 'min', 'most_recent_post': 'max'})
    post_draft_stats = postsByUser['draft'].agg({'num_drafts': 'sum', 'percent_drafts': 'mean'})
    post_frontpage_stats = postsByUser['frontpaged'].sum().to_frame('num_frontpage_posts')

    post_stats = (
        post_date_stats
            .merge(post_draft_stats, left_index=True, right_index=True)
            .merge(post_frontpage_stats, left_index=True, right_index=True)
    )

    return post_stats


def calc_user_recent_activity(posts, comments, votes, views, present_date):
    def activity_last_n(n, date):
        # could be made to contain another function called repeatedly, but it's fine. It works.

        n_days_ago = date - pd.to_timedelta(n, 'days')

        comments_ln = comments[(comments['postedAt'] > n_days_ago)].groupby('userId').size().to_frame(
            'num_comments_last_{}_days'.format(n))
        posts_ln = posts[(posts['postedAt'] > n_days_ago) & (~posts['draft'])].groupby('userId').size().to_frame(
            'num_posts_last_{}_days'.format(n))
        votes_ln = votes[(votes['votedAt'] > n_days_ago)].groupby('userId').size().to_frame(
            'num_votes_last_{}_days'.format(n))
        views_ln = views[(views['createdAt'] > n_days_ago)].groupby('userId').size().to_frame(
            'num_views_last_{}_days'.format(n))
        distinct_posts_viewed_ln = views[(views['createdAt'] > n_days_ago)].groupby('userId')[
            'documentId'].nunique().to_frame('num_distinct_posts_viewed_last_{}_days'.format(n))

        ln_stats = (
            posts_ln
                .merge(comments_ln, left_index=True, right_index=True, how='outer')
                .merge(votes_ln, left_index=True, right_index=True, how='outer')
                .merge(views_ln, left_index=True, right_index=True, how='outer')
                .merge(distinct_posts_viewed_ln, left_index=True, right_index=True, how='outer')

        )

        return ln_stats

    recent_activity = (activity_last_n(30, present_date).merge(activity_last_n(180, present_date),
                                                               left_index=True, right_index=True, how='outer')
                       ).fillna(0).astype(int)

    return recent_activity


def enrich_posts(colls_dfs):
    users = colls_dfs['users']
    posts = colls_dfs['posts']
    comments = colls_dfs['comments']
    views = colls_dfs['views']
    votes = colls_dfs['votes']

    def num_commenters(commenters_list):
        if commenters_list == commenters_list:  # check for isnan, works since nan == nan is false
            if type(commenters_list) == str:
                return len(
                    [u.replace("'", '').replace('"', '').strip() for u in commenters_list.strip('[]').split(',')])
            else:
                return len(commenters_list)
        else:
            return 0

    # comment stats
    comment_stats = comments.groupby('postId').apply(lambda x: pd.Series(data={
        'num_comments_rederived': x['_id'].nunique(),
        'most_recent_comment': x['postedAt'].max()
    }))

    # vote stats for post
    vote_stats = calculate_vote_stats_for_content(votes)

    # view stats for post
    view_date_stats = views.groupby('documentId').apply(lambda x: pd.Series(data={
        'most_recent_view_logged': x['createdAt'].max(),
        'viewCountLogged': x.shape[0]
    }))
    view_distinct_viewers = views.groupby('documentId')['userId'].nunique().to_frame('num_distinct_viewers')
    view_stats = view_date_stats.merge(view_distinct_viewers, left_index=True, right_index=True, how='left')

    posts = (posts
             .merge(comment_stats, left_on='_id', right_index=True, how='left')
             .merge(vote_stats, left_on='_id', right_index=True, how='left')
             .merge(view_stats, left_on='_id', right_index=True, how='left')
             )

    # recent activity stats
    recent_activity_cols = ['most_recent_vote', 'most_recent_view_logged', 'most_recent_comment']
    for col in recent_activity_cols:
        posts[col] = pd.to_datetime(posts[col])
    posts['most_recent_activity'] = posts[recent_activity_cols].max(axis=1)

    # further column additions

    # dfp['frontpageDate'] = dfp['frontpageDate'].replace(0, np.nan) #shouldn't be necessary, track upstream
    posts['frontpaged'] = posts['frontpageDate'].notnull()
    posts['num_distinct_commenters'] = posts['commenters'].apply(num_commenters)
    posts['gw'] = posts['userAgent'].astype(str).str.contains('drakma', case=False).fillna(False)

    posts = users.set_index('_id')[['username', 'displayName']].merge(posts, left_index=True, right_on='userId',
                                                                      how='right')  # add username to posts cols

    return posts


def enrich_comments(colls_dfs):  # dict(df) -> df
    """Add extra data to comments dataframe."""

    users = colls_dfs['users']
    comments = colls_dfs['comments']
    votes = colls_dfs['votes']

    vote_stats = calculate_vote_stats_for_content(votes)
    comments = comments.merge(vote_stats, left_on='_id', right_index=True, how='left')

    comments['top_level'] = comments['parentCommentId'].isnull()
    comments['gw'] = comments['userAgent'].astype(str).str.contains('drakma', case=False)
    comments = users.set_index('_id')[['username', 'displayName']].merge(comments, left_index=True,
                                                                         right_on='userId')  # add username to comments collection

    return comments


def enrich_users(colls_dfs, date_str):
    """Takes in many dataframes and return one super-enriched users dataframe."""

    users = colls_dfs['users']
    posts = colls_dfs['posts']
    comments = colls_dfs['comments']
    views = colls_dfs['views']
    votes = colls_dfs['votes']

    date = datetime.datetime.strptime(date_str, '%Y-%m-%d')

    post_stats = calc_user_post_stats(posts)
    comment_stats = calc_user_comment_stats(comments)
    vote_stats = calculate_vote_stats_for_users(votes)
    view_stats = calc_user_view_stats(views)
    recent_activity = calc_user_recent_activity(posts, comments, votes, views, date)

    users = (users
             .merge(post_stats, left_on='_id', right_index=True, how='left')
             .merge(comment_stats, left_on='_id', right_index=True, how='left')
             .merge(vote_stats, left_on='_id', right_index=True, how='left')
             .merge(view_stats, left_on='_id', right_index=True, how='left')
             .merge(recent_activity, left_on='_id', right_index=True, how='left')
             )

    users['earliest_activity'] = users[['earliest_post', 'earliest_comment', 'earliest_vote', 'earliest_view']].min(
        axis=1)
    users['true_earliest'] = users[['earliest_activity', 'createdAt']].min(axis=1)
    users['most_recent_activity'] = users[
        ['most_recent_post', 'most_recent_comment', 'most_recent_vote', 'most_recent_view', 'createdAt']].max(axis=1)
    users['days_since_active'] = (date - users['most_recent_activity']).dt.days

    return users


@timed
def enrich_collections(colls_dfs, date_str):  # dict[str:df] -> dict[str:df]
    """Single function for collectively enriching all collection dataframes.

    Input: dictionary of basic-parsed collection dataframes.
    Output: dictionary of enriched (fully processed) collection dataframe.

    """

    enriched_dfs = {
        'users': enrich_users(colls_dfs, date_str=date_str),
        'posts': enrich_posts(colls_dfs),
        'comments': enrich_comments(colls_dfs),
        'votes': colls_dfs['votes'],
        'views': colls_dfs['views']
    }

    return enriched_dfs


@timed
def run_etlw_pipeline(date_str, clean_up=False, plotly=False, gsheets=False,
                      metrics=False, postgres=False, limit=None):
    # ##0&1. DOWNLOAD DATA and BASIC PARSE
    dfs_cleaned = get_collections_cleaned(limit=limit)

    # ##2. ENRICHING OF COLLECTIONS
    today = dfs_cleaned['views']['createdAt'].max().strftime('%Y-%m-%d')  # treat max date in collections as "today"
    dfs_enriched = enrich_collections(dfs_cleaned, date_str=today)

    # ##3. WRITE OUT ENRICHED COLLECTIONS
    write_collections(dfs_enriched, date_str=today)

    # ##4 METRIC STUFF - PLOTS AND SHEETS
    if metrics:
        run_metric_pipeline(dfs_enriched, online=True, sheets=gsheets, plots=True) # TODO; change a lot of these default options to respect input options

    # ##5. PLOT GRAPHS TO PLOTLY DASHBOARD
    if plotly:
        run_plotline(dfs_enriched, start_date='2019-04-01', size=(700, 350), online=True)

    # ##6. PLOT GRAPHS TO PLOTLY DASHBOARD
    if gsheets:
        create_and_update_all_sheets(dfs_enriched, spreadsheet_name=get_config_field('GSHEETS', 'spreadsheet_name'))

    # ##7. LOAD DATA FILES TO POSTGRES DB
    if postgres and ENV=='ec2':
        run_pg_pandas_transfer(dfs_enriched, date_str=date_str)

    # ##8. CLEAN UP OLD FILES TO SAVE SPACE
    if clean_up:
        clean_up_old_files(days_to_keep=2)

    return None


if __name__ == '__main__':
    run_etlw_pipeline(
                      date_str=pd.datetime.today().strftime('%Y-%m-%d'),
                      plotly=True,
                    #   gsheets=True,
                    #   metrics=True,
                    #   postgres=False,
                      clean_up=True
                      )
