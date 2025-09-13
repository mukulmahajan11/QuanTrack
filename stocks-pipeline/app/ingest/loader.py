import io, json, time, uuid
from typing import Iterable, Dict, Any
from app.utils.db import get_conn, put_conn
from app.utils.logging import get_logger
from app.metrics.metrics import MetricsRecorder

log = get_logger("loader")

def rows_to_csv_buffer(rows: Iterable[Dict[str, Any]]) -> tuple[io.StringIO, int, int]:
    buf = io.StringIO()
    count = 0
    bytes_est = 0
    for r in rows:
        payload = json.dumps(r, separators=(",", ":"), ensure_ascii=False)
        line = "|".join([
            r.get("symbol",""),
            r.get("ts",""),
            str(r.get("price") or r.get("close") or ""),
            str(r.get("volume") or ""),
            str(r.get("open") or ""),
            str(r.get("high") or ""),
            str(r.get("low") or ""),
            str(r.get("close") or ""),
            payload.replace("\n"," ")
        ]) + "\n"
        buf.write(line)
        count += 1
        bytes_est += len(line.encode("utf-8"))
    buf.seek(0)
    return buf, count, bytes_est

def load_into_raw(rows: Iterable[Dict[str, Any]], run_id: uuid.UUID, dag_id: str, task_id: str) -> dict:
    start = time.time()
    buf, count, bytes_est = rows_to_csv_buffer(rows)
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("set session statement_timeout = '600s'")
            cur.copy_expert("""
                COPY raw.stocks_ticks (symbol, ts, price, volume, open, high, low, close, payload)
                FROM STDIN WITH (FORMAT csv, DELIMITER '|', QUOTE E'\b', ESCAPE '\\')
            """, buf)
    finally:
        put_conn(conn)
    dur = time.time() - start
    thr = count / dur if dur > 0 else 0
    MetricsRecorder().record(
        run_id=run_id, dag_id=dag_id, task_id=task_id,
        records_read=count, records_loaded=count, bytes_loaded=bytes_est,
        duration_secs=dur, throughput_rows_per_sec=thr, success=True
    )
    log.info(f"Loaded {count} rows in {dur:.2f}s ({thr:,.0f} rows/s)")
    return {"count": count, "duration_secs": dur, "throughput_rps": thr, "bytes": bytes_est}
