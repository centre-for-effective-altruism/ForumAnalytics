import pandas as pd
import plotly
import plotly.plotly as py
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
from lwdash import *
from karmametric import *
from utils import timed

def plotly_ts(ss=None, title='missing', color='yellow', dd=None, start_date=None, end_date=pd.datetime.today(),
              ma=1, pr='D', date_col='postedAt', size=(700, 400), online=False, exclude_last_period=True,):
    if dd is None:
        dd = ss.set_index(date_col).resample(pr, closed='left', label='left').size().rolling(ma).mean().to_frame(title).reset_index()

    if exclude_last_period:
        dd = dd.iloc[:-1]

    data = [
                go.Scatter(x=dd[date_col], y=dd[title], line={'color': color, 'width': 2})]

    pr_dict = {'D': 'daily', 'W': 'weekly'}

    layout = go.Layout(
        autosize = True, width=size[0], height=size[1],
        title= '{} ({})'.format(title, pr_dict[pr]),
        xaxis={'range': [start_date, end_date]},
        yaxis={'range': [0, dd.set_index(date_col)[start_date:][title].max() * 1.2],
               'title': title}
    )


    fig = go.Figure(data=data, layout=layout)

    if online:
        py.iplot(fig, filename=title)
    else:
        iplot(fig, filename=title)

def plotly_ts_ma(ss=None, title='missing', color='yellow', dd=None, start_date=None, end_date=pd.datetime.today(),
                 date_col='postedAt', size=(700, 400), online=False, exclude_last_period=True,
                 pr='D', ma=7):

    pr_dict = {'D':'day', 'W':'week', 'M':'month', 'Y':'year'}
    pr_dictly = {'D':'daily', 'W':'weekly', 'M':'monthly', 'Y':'yearly'}

    if dd is None:
        dd = ss.set_index(date_col).resample(pr).size().to_frame(title).reset_index()

    dd_ma = dd.set_index(date_col)[title].rolling(ma).mean().round(1).reset_index()

    if exclude_last_period:
        dd = dd.iloc[:-1]
        dd_ma = dd_ma.iloc[:-1]

    data = [
        go.Scatter(x=dd[date_col], y=dd[title], line={'color': color, 'width': 0.75}, name='{}-value'.format(pr_dictly[pr])),
        go.Scatter(x=dd_ma[date_col], y=dd_ma[title], line={'color': color, 'width': 3}, name='{} {} avg'.format(ma, pr_dict[pr]))
    ]


    layout = go.Layout(
        autosize = True, width=size[0], height=size[1],
        title= title,
        xaxis={'range': [start_date, end_date]},
        yaxis={'range': [0, dd.set_index(date_col)[start_date:][title].max() * 1.2],
               'title': title}
    )


    fig = go.Figure(data=data, layout=layout)

    if online:
        py.iplot(fig, filename=title)
    else:
        iplot(fig, filename=title)


def plotly_ds_uniques(df, date_col, title, start_date, color, size, online, pr='D', ma=7):
    dd = df.set_index(date_col)['2009':].resample(pr)['userId'].nunique().to_frame(title).reset_index()
    plotly_ts_ma(dd=dd, date_col=date_col, title=title, start_date=start_date, color=color, size=size,
                 online=online, pr=pr, ma=ma)

def plot_table(df, title='missing', online=False):
    trace = go.Table(
        header=dict(values=df.columns.tolist(),
                    fill={'color': '#C2D4FF'},
                    align=['left'] * 2),
        cells=dict(values=[df[col] for col in df.columns],
                   fill={'color': '#F5F8FF'},
                   align=['left'] * 2)
    )

    data = [trace]

    if online:
        py.iplot(data, filename=title)
    else:
        iplot(data, filename=title)


def get_valid_non_self_votes(dfv, dfp, dfc, dfu):
    a = dfv[~dfv['cancelled']].merge(dfp[['_id', 'userId']], left_on='documentId', right_on='_id',
                                     suffixes=['', '_post'], how='left')
    b = a.merge(dfc[['_id', 'userId']], left_on='documentId', right_on='_id', suffixes=['', '_comment'], how='left')
    b['userId_document'] = b['userId_comment'].fillna(b['userId_post'])
    b['self_vote'] = b['userId'] == b['userId_document']
    b = b[b['userId'].isin(dfu[(dfu['signUpReCaptchaRating'].fillna(1)>0.3)&(~dfu['banned'])]['_id'])]

    return b[~b['self_vote']]

@timed
def run_plotline(dfs, online=False, start_date=None, size=(1000, 400), pr='D', ma=7):

    plotly.tools.set_credentials_file(username=get_config_field('PLOTLY', 'username'),
                                      api_key=get_config_field('PLOTLY', 'api_key'))
    init_notebook_mode(connected=True)

    dfu = dfs['users']
    dfp = dfs['posts']
    dfc = dfs['comments']
    dfv = dfs['votes']
    dpv = dfs['views']  # pv = post-views


    valid_users = dfu[(~dfu['banned'])&(~dfu['deleted'])&(dfu['num_distinct_posts_viewed']>=5)]
    valid_posts = dfp[(dfp[['smallUpvote', 'bigUpvote']].sum(axis=1) >= 2)&~dfp['draft']&~dfp['legacySpam']]
    valid_comments = dfc[dfc['userId']!='pgoCXxuzpkPXADTp2'] #remove GPT-2
    valid_votes = get_valid_non_self_votes(dfv,dfp, dfc, dfu)


    # logged-in users
    plotly_ds_uniques(dpv[dpv['userId'].isin(valid_users['_id'])], date_col='createdAt', title='Num Logged-In Users',
                            start_date=start_date, online=online, size=size, color='black', pr=pr, ma=ma)
    # posts
    plotly_ts_ma(valid_posts, 'Num Posts with 2+ upvotes', 'blue', start_date=start_date, online=online, size=size, pr=pr, ma=ma)
    # comments
    plotly_ts_ma(valid_comments, 'Num Comments', 'green', start_date=start_date, online=online, size=size, pr=pr, ma=ma) #exclude GPT2
    # logged-in post-views
    plotly_ts_ma(dpv, 'Num Logged-In Post Views', 'red', date_col='createdAt', start_date=start_date, online=online,
                 size=size, pr=pr, ma=ma)
    # num votes
    plotly_ts_ma(valid_votes, 'Num Votes (excluding self-votes)', 'orange', date_col='votedAt', start_date=start_date,
                 online=online, size=size, pr=pr, ma=ma)

    # num accounts created
    plotly_ts_ma(valid_users, title='Accounts Created, 5+ posts_viewed',
                 date_col='true_earliest', color='grey', online=online, size=size, start_date=start_date, pr=pr, ma=ma)

    # unique voters
    plotly_ds_uniques(valid_votes, 'votedAt', title='Num Unique Voters', start_date=start_date,
                            size=size, color='darkorange', online=online, pr=pr, ma=ma)
    # unique commenters
    plotly_ds_uniques(valid_comments, 'postedAt', title='Num Unique Commenters',
                            start_date=start_date, size=size, color='darkgreen', online=online, pr=pr, ma=ma)
    # unique poster
    plotly_ds_uniques(valid_posts, 'postedAt', title='Num Unique Posters',
                            start_date=start_date, size=size, color='darkblue', online=online, pr=pr, ma=ma)

    plot_table(downvote_monitoring(dfv, dfp, dfc, dfu, 2, ), title='Downvote Monitoring', online=online)

