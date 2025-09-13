create schema if not exists raw;
create schema if not exists staging;
create schema if not exists analytics;
create schema if not exists ops;

create table if not exists raw.stocks_ticks (
  ingest_ts timestamptz default now(),
  symbol text,
  ts timestamptz,
  price numeric(18,6),
  volume bigint,
  open numeric(18,6),
  high numeric(18,6),
  low numeric(18,6),
  close numeric(18,6),
  payload jsonb
);

create index if not exists idx_ticks_symbol_ts on raw.stocks_ticks (symbol, ts);

create table if not exists ops.ingest_state (
  stream_name text primary key,
  last_cursor_value text,
  updated_at timestamptz default now()
);

create table if not exists ops.pipeline_metrics (
  run_id uuid primary key,
  dag_id text,
  task_id text,
  records_read bigint,
  records_loaded bigint,
  bytes_loaded bigint,
  started_at timestamptz,
  finished_at timestamptz,
  duration_secs numeric,
  throughput_rows_per_sec numeric,
  success boolean,
  error_message text
);
