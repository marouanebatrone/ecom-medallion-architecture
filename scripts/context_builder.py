import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
import requests
from urllib.parse import quote_plus

class CustomJSONEncoder(json.JSONEncoder):
    """Handles datetime serialization for the JSON output."""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

class IncidentContextBuilder:
    def __init__(self, obs_db_conn_params, target_db_conn_params):
        self.obs_db_conn_params = obs_db_conn_params
        self.target_db_conn_params = target_db_conn_params
        self.output_dir = os.path.join(os.getcwd(), "incidents")
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.lineage_base_url = "https://qupid-watchpipe.qupid.clusterdiali.me/api/v1/lineage"
        self.qupid_namespace = "postgres://host.docker.internal:5433"

    def _get_obs_db_connection(self):
        return psycopg2.connect(**self.obs_db_conn_params, cursor_factory=RealDictCursor)

    def _get_target_db_connection(self):
        return psycopg2.connect(**self.target_db_conn_params, cursor_factory=RealDictCursor)

    def get_todays_incidents(self):
        query = """
            SELECT incident_id, observer_id, run_id, status, detected_at
            FROM incidents
            WHERE DATE(created_at) = CURRENT_DATE AND status = 'fail';
        """
        with self._get_obs_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()

    def get_observer_details(self, observer_id, run_id):
        query = """
            SELECT 
                o.observer_name, o.observer_type, o.resource_type,
                o.db_name, o.schema_name, o.table_name, o.column_name, o.condition_config,
                r.metric_value, r.threshold_value, r.execution_time
            FROM observers o
            JOIN observer_runs r ON o.observer_id = r.observer_id
            WHERE o.observer_id = %s AND r.run_id = %s;
        """
        with self._get_obs_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (observer_id, run_id))
                return cur.fetchone()

    def get_historical_runs(self, observer_id, limit=3):
        query = """
            SELECT execution_time, metric_value, threshold_value, status 
            FROM observer_runs 
            WHERE observer_id = %s 
            ORDER BY execution_time DESC LIMIT %s;
        """
        with self._get_obs_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (observer_id, limit))
                return cur.fetchall()

    def get_data_sample(self, schema_name, table_name, limit=3):
        if not table_name:
            return []
        
        safe_schema = "".join(c for c in schema_name if c.isalnum() or c == '_')
        safe_table = "".join(c for c in table_name if c.isalnum() or c == '_')
        
        query = f"SELECT * FROM {safe_schema}.{safe_table} LIMIT {limit};"
        try:
            with self._get_target_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    return cur.fetchall()
        except Exception as e:
            return [{"error": f"Could not fetch sample: {str(e)}"}]

    def get_lineage_directions(self, schema_name, table_name):
        """
        Calls the Lineage API and traverses the graph to dynamically 
        separate upstream and downstream dataset dependencies.
        """
        full_table_name = f"{schema_name}.{table_name}"
        urn = f"dataset:{self.qupid_namespace}:{full_table_name}"
        encoded_urn = quote_plus(urn)
        url = f"{self.lineage_base_url}?nodeId={encoded_urn}&depth=10"
        
        upstreams = set()
        downstreams = set()

        try:
            response = requests.get(url)
            if response.status_code != 200:
                print(f"Failed to fetch lineage for {urn}. Status: {response.status_code}")
                return [], []

            graph = response.json().get('graph', [])
            
            # Create a lookup dictionary for fast node access
            node_lookup = {node['id']: node for node in graph}
            
            if urn not in node_lookup:
                return [], []

            # --- Traversal Helper Function ---
            def parse_dataset_name(node_data):
                name = node_data.get('data', {}).get('name')
                if not name: return None
                parts = name.split('.')
                return (parts[0], parts[1]) if len(parts) == 2 else ('public', name)

            # 1. Traverse Upstreams via 'inEdges'
            queue = [urn]
            visited = set()
            while queue:
                current_id = queue.pop(0)
                if current_id in visited:
                    continue
                visited.add(current_id)

                node = node_lookup.get(current_id)
                if not node: continue

                # If it's a dataset and not the starting node, record it
                if node.get('type') == 'DATASET' and current_id != urn:
                    parsed_name = parse_dataset_name(node)
                    if parsed_name: upstreams.add(parsed_name)

                # Queue the origins of the incoming edges (usually jobs, which then point back to datasets)
                for edge in node.get('inEdges', []):
                    queue.append(edge['origin'])

            # 2. Traverse Downstreams via 'outEdges'
            queue = [urn]
            visited = set()
            while queue:
                current_id = queue.pop(0)
                if current_id in visited:
                    continue
                visited.add(current_id)

                node = node_lookup.get(current_id)
                if not node: continue

                if node.get('type') == 'DATASET' and current_id != urn:
                    parsed_name = parse_dataset_name(node)
                    if parsed_name: downstreams.add(parsed_name)

                # Queue the destinations of outgoing edges
                for edge in node.get('outEdges', []):
                    if 'destination' in edge:
                        queue.append(edge['destination'])

        except Exception as e:
            print(f"Error processing lineage for {full_table_name}: {e}")
            
        return list(upstreams), list(downstreams)

    def get_table_observers(self, schema_name, table_name):
        query = """
            SELECT o.observer_name, o.condition_config, r.status, r.metric_value, r.execution_time
            FROM observers o
            LEFT JOIN (
                SELECT observer_id, status, metric_value, execution_time,
                       ROW_NUMBER() OVER(PARTITION BY observer_id ORDER BY execution_time DESC) as rn
                FROM observer_runs
            ) r ON o.observer_id = r.observer_id AND r.rn = 1
            WHERE o.schema_name = %s AND o.table_name = %s AND o.is_active = True;
        """
        with self._get_obs_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (schema_name, table_name))
                return cur.fetchall()

    def _fetch_context_for_dependencies(self, dependencies):
        """Helper to fetch observers and data samples for a list of table tuples."""
        context_list = []
        for schema, table in dependencies:
            observers = self.get_table_observers(schema, table)
            sample = self.get_data_sample(schema, table, limit=2)
            context_list.append({
                "schema_name": schema,
                "table_name": table,
                "active_observers_status": observers,
                "data_sample": sample
            })
        return context_list

    def build_context_for_incident(self, incident):
        incident_id = incident['incident_id']
        observer_id = incident['observer_id']
        run_id = incident['run_id']

        print(f"Building context for Incident: {incident_id}")
        
        details = self.get_observer_details(observer_id, run_id)
        if not details: return None

        schema = details.get('schema_name', 'public')
        table = details.get('table_name')

        history = self.get_historical_runs(observer_id)
        data_sample = self.get_data_sample(schema, table)

        # Utilize the new dynamic graph traversal
        upstream_tables, downstream_tables = self.get_lineage_directions(schema, table)
        
        upstream_context = self._fetch_context_for_dependencies(upstream_tables)
        downstream_context = self._fetch_context_for_dependencies(downstream_tables)

        payload = {
            "incident_metadata": {
                "incident_id": incident_id,
                "detected_at": incident['detected_at'],
                "status": incident['status']
            },
            "failing_resource": {
                "configuration": details,
                "recent_runs": history,
                "data_sample": data_sample
            },
            "lineage_context": {
                "upstream_dependencies": upstream_context,
                "downstream_dependencies": downstream_context
            }
        }
        return payload

    def run(self):
        incidents = self.get_todays_incidents()
        print(f"Found {len(incidents)} new incidents to process.")
        
        for incident in incidents:
            context = self.build_context_for_incident(incident)
            if context:
                filename = f"incident_{incident['incident_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                filepath = os.path.join(self.output_dir, filename)
                
                with open(filepath, 'w') as f:
                    json.dump(context, f, indent=4, cls=CustomJSONEncoder)
                print(f"Saved context payload to: {filepath}")

if __name__ == "__main__":
    OBS_DB = {
        "dbname": "observers_db", "user": "postgres",
        "password": "postgres", "host": "host.docker.internal", "port": 5433
    }
    TARGET_DB = {
        "dbname": "sales_data_platform", "user": "postgres", 
        "password": "postgres", "host": "host.docker.internal", "port": 5433
    }
    
    builder = IncidentContextBuilder(OBS_DB, TARGET_DB)
    builder.run()