import uuid, time
from app.utils.db import get_conn, put_conn

class MetricsRecorder:
    def record(self, run_id: uuid.UUID, dag_id: str, task_id: str,
               records_read: int, records_loaded: int, bytes_loaded: int,
               duration_secs: float, throughput_rows_per_sec: float,
               success: bool, error_message: str | None = None):
        conn = get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("""
                  insert into ops.pipeline_metrics
                  (run_id, dag_id, task_id, records_read, records_loaded, bytes_loaded,
                   started_at, finished_at, duration_secs, throughput_rows_per_sec, success, error_message)
                  values (%s,%s,%s,%s,%s,%s, to_timestamp(%s), to_timestamp(%s), %s, %s, %s, %s)
                """, (
                    str(run_id), dag_id, task_id, records_read, records_loaded, bytes_loaded,
                    time.time() - duration_secs, time.time(),
                    duration_secs, throughput_rows_per_sec, success, error_message
                ))
        finally:
            put_conn(conn)
