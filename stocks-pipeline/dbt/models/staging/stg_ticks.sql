with src as (
  select
    symbol,
    ts::timestamptz as ts,
    coalesce(price, close)::numeric(18,6) as price,
    volume::bigint as volume,
    open::numeric(18,6) as open,
    high::numeric(18,6) as high,
    low::numeric(18,6)  as low,
    close::numeric(18,6) as close,
    ingest_ts,
    payload
  from raw.stocks_ticks
)
select * from src;
