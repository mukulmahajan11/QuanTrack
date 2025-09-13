with base as (
  select symbol, date_trunc('minute', ts) as minute, 
         min(low) as low, max(high) as high,
         first_value(open) over (partition by symbol, date_trunc('minute', ts) order by ts) as open,
         last_value(close) over  (partition by symbol, date_trunc('minute', ts) order by ts
                                  rows between unbounded preceding and unbounded following) as close,
         sum(volume) as volume
  from {{ ref('stg_ticks') }}
  group by symbol, date_trunc('minute', ts)
)
select * from base;
