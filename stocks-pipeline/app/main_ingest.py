import uuid
from app.config import settings
from app.ingest.state import get_last_cursor, upsert_cursor
from app.ingest.api_client import stream_ticks
from app.ingest.loader import load_into_raw
from app.utils.logging import get_logger

log = get_logger("main_ingest")

def run_ingest(dag_id: str = "stocks_pipeline", task_id: str = "extract_load"):
    run_id = uuid.uuid4()
    cursor = get_last_cursor(settings.stream_name)
    log.info(f"Starting ingest with cursor={cursor}")
    rows = list(stream_ticks(cursor))
    if not rows:
        log.info("No new data.")
        return {"run_id": str(run_id), "loaded": 0}
    stats = load_into_raw(rows, run_id, dag_id, task_id)
    new_cursor = max([r.get("ts") for r in rows if r.get("ts")])
    upsert_cursor(settings.stream_name, new_cursor)
    return {"run_id": str(run_id), "loaded": stats["count"], "throughput": stats["throughput_rps"]}
