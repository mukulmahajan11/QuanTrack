from dataclasses import dataclass
import os

@dataclass
class Settings:
    pg_host: str = os.getenv("POSTGRES_HOST", "localhost")
    pg_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    pg_db: str = os.getenv("POSTGRES_DB", "marketdw")
    pg_user: str = os.getenv("POSTGRES_USER", "marketuser")
    pg_password: str = os.getenv("POSTGRES_PASSWORD", "marketpass")

    api_base_url: str = os.getenv("STOCKS_API_BASE_URL")
    api_key: str = os.getenv("STOCKS_API_KEY")
    api_timeout: int = int(os.getenv("STOCKS_API_TIMEOUT_SECS", "20"))
    api_symbols: list[str] = os.getenv("STOCKS_API_SYMBOLS","AAPL,MSFT").split(",")
    api_granularity: str = os.getenv("STOCKS_API_GRANULARITY","1min")

    stream_name: str = "stocks_ticks"

settings = Settings()
