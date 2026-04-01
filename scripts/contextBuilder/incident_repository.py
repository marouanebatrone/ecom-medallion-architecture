import json

class IncidentRepository:
    def __init__(self, postgres_client):
        self.db = postgres_client

    def fetch_todays_failures(self):
        query = """
            SELECT incident_id, observer_id, run_id, status, detected_at 
            FROM incidents 
            WHERE DATE(created_at) = CURRENT_DATE AND status = 'fail';
        """
        return self.db.execute_query(query)

    def fetch_observer_details(self, observer_id, run_id):
        query = """
            SELECT o.*, r.metric_value, r.threshold_value, r.execution_time
            FROM observers o 
            JOIN observer_runs r ON o.observer_id = r.observer_id
            WHERE o.observer_id = %s AND r.run_id = %s;
        """
        return self.db.execute_query(query, (observer_id, run_id), fetch_all=False)

    def fetch_historical_metrics(self, observer_id, limit=3):
        query = """
            SELECT execution_time, metric_value, status 
            FROM observer_runs 
            WHERE observer_id = %s 
            ORDER BY execution_time DESC LIMIT %s;
        """
        return self.db.execute_query(query, (observer_id, limit))
    
    def find_matching_observer_id(self, schema, table, source_config):
        query = """
            SELECT observer_id 
            FROM observers 
            WHERE schema_name = %s 
              AND table_name = %s 
              AND observer_type = %s 
              AND resource_type = %s 
              AND db_name = %s 
              AND (column_name = %s OR (column_name IS NULL AND %s IS NULL))
              AND condition_config::jsonb = %s::jsonb
            LIMIT 1;
        """
        config_json = json.dumps(source_config.get('condition_config'))
        
        params = (
            schema,
            table,
            source_config.get('observer_type'),
            source_config.get('resource_type'),
            source_config.get('db_name'),
            source_config.get('column_name'),
            source_config.get('column_name'),
            config_json
        )
        
        result = self.db.execute_query(query, params, fetch_all=False)
        return result['observer_id'] if result else None
    
    def fetch_table_observers(self, schema, table):
        query = "SELECT observer_name, condition_config FROM observers WHERE schema_name=%s AND table_name=%s"
        return self.db.execute_query(query, (schema, table))