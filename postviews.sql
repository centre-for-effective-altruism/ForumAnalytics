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
  -- substring(path from 8 for
  min(timestamp) AS first_viewed
FROM lessraw
WHERE substring(path from 2 for 5) = 'posts' AND
  seconds >= 60 AND -- TODO; seconds input var
  environment = 'production'
GROUP BY path, client_id;
