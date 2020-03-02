WITH lessraw AS (
  SELECT
    environment,
    timestamp,
    event ->> 'clientId' AS client_id,
    event ->> 'tabId' AS tab_id,
    event ->> 'path' AS path,
    (event ->> 'seconds')::NUMERIC AS seconds,
    event ->> 'increment' AS increment
  FROM raw
  -- timerEvent means you've been on active on a page for N seconds, recorded
  -- with a backoff
  WHERE event_type = 'timerEvent'
), all_views AS (
  SELECT
    -- client_id is unique to each browser
    client_id,
    path,
    -- TODO: Maybe get post_id here?
    -- substring(path from 8 for N)
    min(timestamp) AS first_viewed
  FROM lessraw
  WHERE substring(path from 2 for 5) = 'posts' AND
    -- TODO: Seconds as input variable?
    seconds >= 60 AND
    environment = 'production'
  GROUP BY path, client_id
), first_visits AS (
  SELECT
    -- We only count the first view of the post
    min(timestamp) AS timestamp,
    client_id
  FROM lessraw
  GROUP BY client_id
) SELECT
  all_views.*,
  first_visits.timestamp AS first_client_visit
FROM all_views
JOIN first_visits
  ON all_views.client_id = first_visits.client_id
WHERE
  -- The view must happen 3 days after the first time you visit the site
  all_views.first_viewed > first_visits.timestamp + INTERVAL '3 days';
