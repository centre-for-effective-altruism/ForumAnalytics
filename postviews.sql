create temp view lessraw as (
  select
    environment,
    timestamp,
    event ->> 'clientId' as client_id,
    event ->> 'tabId' as tab_id,
    event ->> 'path' as path,
    (event ->> 'seconds')::NUMERIC as seconds,
    event ->> 'increment' as increment
  from raw
    where event_type = 'timerEvent'
);

select client_id, path, min(timestamp) as first_viewed from lessraw where client_id='YzkxZniRf82ezAJ9Q' and substring(path from 2 for 5) = 'posts' and seconds >= 60 and environment = 'production' group by path, client_id limit 3;
