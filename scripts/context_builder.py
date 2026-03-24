import psycopg2
import requests
from psycopg2.extras import RealDictCursor
from datetime import datetime
from urllib.parse import quote_plus
from pymongo import MongoClient

class IncidentContextBuilder:
    def __init__(self, obs_db_params, target_db_params, mongo_uri):
        self.obs_params = obs_db_params
        self.target_params = target_db_params
        
        self.client = MongoClient(mongo_uri)
        self.db = self.client["qupid_observability"]
        self.collection = self.db["incident_contexts"]

        self.lineage_url = "https://qupid-watchpipe.qupid.clusterdiali.me/api/v1/lineage"
        self.namespace = "postgres://host.docker.internal:5433"

    def _query_db(self, params, query, args=None, fetch_all=True):
        with psycopg2.connect(**params, cursor_factory=RealDictCursor) as conn:
            with conn.cursor() as cur:
                cur.execute(query, args)
                return cur.fetchall() if fetch_all else cur.fetchone()

    def get_todays_incidents(self):
        query = "SELECT incident_id, observer_id, run_id, status, detected_at FROM incidents WHERE DATE(created_at) = CURRENT_DATE AND status = 'fail';"
        return self._query_db(self.obs_params, query)

    def get_lineage_directions(self, schema, table):
        urn = quote_plus(f"dataset:{self.namespace}:{schema}.{table}")
        try:
            resp = requests.get(f"{self.lineage_url}?nodeId={urn}&depth=10")
            if resp.status_code != 200: return [], []
            
            graph = resp.json().get('graph', [])
            nodes = {n['id']: n for n in graph}
            
            def traverse(start_node, edge_type):
                visited, queue, results = set(), [start_node], set()
                while queue:
                    curr = queue.pop(0)
                    if curr in visited or curr not in nodes: continue
                    visited.add(curr)
                    
                    node = nodes[curr]
                    if node.get('type') == 'DATASET' and curr != f"dataset:{self.namespace}:{schema}.{table}":
                        name = node['data']['name'].split('.')
                        results.add((name[0], name[1]) if len(name) == 2 else ('public', name[0]))
                    
                    for edge in node.get(edge_type, []):
                        queue.append(edge.get('origin') or edge.get('destination'))
                return list(results)

            return traverse(f"dataset:{self.namespace}:{schema}.{table}", 'inEdges'), traverse(f"dataset:{self.namespace}:{schema}.{table}", 'outEdges')
        except Exception: return [], []

    def build_context(self, incident):
        obs_detail_query = """
            SELECT o.*, r.metric_value, r.threshold_value, r.execution_time
            FROM observers o JOIN observer_runs r ON o.observer_id = r.observer_id
            WHERE o.observer_id = %s AND r.run_id = %s;
        """
        details = self._query_db(self.obs_params, obs_detail_query, (incident['observer_id'], incident['run_id']), False)
        if not details: return None

        # 2. History & Lineage
        history_query = "SELECT execution_time, metric_value, status FROM observer_runs WHERE observer_id = %s ORDER BY execution_time DESC LIMIT 3;"
        up_tables, down_tables = self.get_lineage_directions(details['schema_name'], details['table_name'])

        def enrich(tables):
            return [{
                "table": f"{s}.{t}",
                "observers": self._query_db(self.obs_params, "SELECT observer_name, condition_config FROM observers WHERE schema_name=%s AND table_name=%s", (s, t))
            } for s, t in tables]

        return {
            "incident_id": incident['incident_id'],
            "processed_at": datetime.now(),
            "metadata": incident,
            "resource": {
                "config": details,
                "history": self._query_db(self.obs_params, history_query, (incident['observer_id'],))
            },
            "lineage": {
                "upstreams": enrich(up_tables),
                "downstreams": enrich(down_tables)
            }
        }

    def run(self):
        incidents = self.get_todays_incidents()
        print(f"Processing {len(incidents)} incidents...")
        
        for inc in incidents:
            payload = self.build_context(inc)
            if payload:
                self.collection.update_one({"incident_id": inc['incident_id']},{"$set": payload},upsert=True)
                print(f"Context stored for Incident {inc['incident_id']}")

if __name__ == "__main__":
    DB_CONFIGS = {
        "obs": {"dbname": "observers_db", "user": "postgres", "password": "postgres", "host": "localhost", "port": 5433},
        "target": {"dbname": "sales_data_platform", "user": "postgres", "password": "postgres", "host": "localhost", "port": 5433}
    }
    MONGO_URI = "mongodb://admin:admin@localhost:27017/"
    
    builder = IncidentContextBuilder(DB_CONFIGS["obs"], DB_CONFIGS["target"], MONGO_URI)
    builder.run()