with d as (
  select symbol, date_trunc('day', ts) as day,
         min(low) as day_low, max(high) as day_high,
         first_value(open) over (partition by symbol, date_trunc('day', ts) order by ts) as day_open,
         last_value(close) over  (partition by symbol, date_trunc('day', ts) order by ts
                                  rows between unbounded preceding and unbounded following) as day_close,
         sum(volume) as day_volume
  from {{ ref('stg_ticks') }}
  group by symbol, date_trunc('day', ts)
)
select * from d;
