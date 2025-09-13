from app.utils.db import get_conn, put_conn

def get_last_cursor(stream_name: str) -> str | None:
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("select last_cursor_value from ops.ingest_state where stream_name=%s", (stream_name,))
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        put_conn(conn)

def upsert_cursor(stream_name: str, cursor: str) -> None:
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                insert into ops.ingest_state (stream_name, last_cursor_value)
                values (%s, %s)
                on conflict (stream_name)
                do update set last_cursor_value=excluded.last_cursor_value, updated_at=now()
            """, (stream_name, cursor))
    finally:
        put_conn(conn)
