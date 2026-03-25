import psycopg2
from psycopg2.extras import RealDictCursor

class PostgresClient:
    def __init__(self, connection_params):
        self.params = connection_params

    def execute_query(self, query, args=None, fetch_all=True):
        with psycopg2.connect(**self.params, cursor_factory=RealDictCursor) as conn:
            with conn.cursor() as cur:
                cur.execute(query, args)
                return cur.fetchall() if fetch_all else cur.fetchone()