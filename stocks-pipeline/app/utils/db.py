from psycopg2.pool import SimpleConnectionPool
from app.config import settings

_pool = SimpleConnectionPool(
    1, 10,
    host=settings.pg_host, port=settings.pg_port,
    dbname=settings.pg_db, user=settings.pg_user, password=settings.pg_password
)

def get_conn():
    return _pool.getconn()

def put_conn(conn):
    _pool.putconn(conn)
