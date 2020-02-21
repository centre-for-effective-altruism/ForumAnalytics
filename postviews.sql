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
    WHERE event_type = 'timerEvent'
) SELECT
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
GROUP BY path, client_id;
