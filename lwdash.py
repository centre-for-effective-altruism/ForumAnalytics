import pandas as pd


def downvote_monitoring(dfv, dfp, dfc, dfu, num_days, num_rows=5):
    n_days_ago = dfv['votedAt'].max() - pd.Timedelta(num_days, unit='d')

    data = dfv[~dfv['cancelled']].set_index('votedAt').sort_index()[n_days_ago:]

    document_votes = data.groupby([data['documentId'], data['power'] < 0]).size().unstack(1).fillna(0)
    document_votes['total'] = document_votes.sum(axis=1)
    document_votes.columns = ['upvote', 'downvote', 'total_votes']

    a = document_votes.reset_index().merge(dfp[['_id', 'userId', 'username', 'title']], left_on='documentId',
                                           right_on='_id', how='left')
    b = a.merge(dfc[['_id', 'userId']], left_on='documentId', right_on='_id', how='left')
    b['userId'] = b['userId_x'].fillna(b['userId_y'])
    c = b[['documentId', 'upvote', 'downvote', 'total_votes', 'userId']]

    d = c.groupby('userId')[['upvote', 'downvote', 'total_votes']].sum().sort_values('total_votes', ascending=False)
    e = d.merge(dfu[['_id', 'username']], left_index=True, right_on='_id', how='left')

    # e[['_id', 'username', 'upvote', 'downvote', 'total_votes']].sort_values('upvote', ascending=False).head(3)
    f = e[['_id', 'upvote', 'downvote', 'total_votes']].sort_values('downvote', ascending=False).head(num_rows)
    f.columns = ['userId', 'upvotes_received', 'downvotes_received', 'total_votes_received']

    return f


