class DataSampleService:
    def __init__(self, postgres_client):
        self.db = postgres_client

    def get_sample_rows(self, schema, table, limit=3):
        try:
            query = f'SELECT * FROM "{schema}"."{table}" LIMIT %s;'
            return self.db.execute_query(query, (limit,))
        except Exception as e:
            print(f"Warning: Could not fetch sample for {schema}.{table}: {e}")
            return []