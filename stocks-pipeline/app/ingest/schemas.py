from pydantic import BaseModel
from typing import Optional

class Tick(BaseModel):
    symbol: str
    ts: str
    price: float
    volume: Optional[int] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    payload: dict
