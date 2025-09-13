import time, requests
from typing import Iterator, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from app.config import settings
from app.utils.logging import get_logger

log = get_logger("api")

class RateLimitError(Exception): ...

def _headers():
    return {"Authorization": f"Bearer {settings.api_key}", "Accept": "application/json"}

@retry(reraise=True, stop=stop_after_attempt(5),
       wait=wait_exponential(multiplier=1, min=1, max=30),
       retry=retry_if_exception_type((requests.RequestException, RateLimitError)))
def _get(url, params):
    r = requests.get(url, headers=_headers(), params=params, timeout=settings.api_timeout)
    if r.status_code == 429:
        raise RateLimitError("rate limited")
    r.raise_for_status()
    return r.json()

def stream_ticks(since_iso: str | None) -> Iterator[Dict[str, Any]]:
    endpoint = f"{settings.api_base_url}/timeseries"
    for sym in settings.api_symbols:
        next_cursor = since_iso
        while True:
            params = {
                "symbol": sym,
                "interval": settings.api_granularity,
                "limit": 1000
            }
            if next_cursor:
                params["start"] = next_cursor
            data = _get(endpoint, params=params)
            items = data.get("items", [])
            if not items:
                break
            for row in items:
                row["symbol"] = sym
                yield row
            next_cursor = data.get("next") or items[-1].get("ts")
            if not next_cursor:
                break
            time.sleep(0.05)
