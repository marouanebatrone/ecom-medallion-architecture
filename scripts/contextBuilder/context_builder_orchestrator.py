from datetime import datetime
from postgres_client import PostgresClient
from incident_repository import IncidentRepository
from data_sample_service import DataSampleService
from lineage_service import LineageService
from context_storage import ContextStorage

class ContextBuilderOrchestrator:
    def __init__(self, configs):
        obs_db = PostgresClient(configs['obs_db'])
        target_db = PostgresClient(configs['target_db'])
        
        self.incidents = IncidentRepository(obs_db)
        self.samples = DataSampleService(target_db)
        self.lineage = LineageService(configs['lineage_url'], configs['namespace'])
        self.storage = ContextStorage(configs['mongo_uri'])

    def run_daily_context_enrichment(self):
        failures = self.incidents.fetch_todays_failures()
        print(f"Enriching context for {len(failures)} failures...")

        for failure in failures:
            context_payload = self._assemble_context(failure)
            
            if context_payload:
                self.storage.store_incident_context(failure['incident_id'], context_payload)
                print(f"Successfully enriched Incident: {failure['incident_id']}")

    def _assemble_context(self, incident):
        details = self.incidents.fetch_observer_details(incident['observer_id'], incident['run_id'])
        if not details: return None

        history = self.incidents.fetch_historical_metrics(incident['observer_id'])
        sample = self.samples.get_sample_rows(details['schema_name'], details['table_name'], limit=3)
        rich_lineage = self.lineage.get_rich_lineage_context(details['schema_name'], details['table_name'])

        return {
            "incident_id": incident['incident_id'],
            "processed_at": datetime.now(),
            "metadata": incident,
            "resource": {
                "config": details,
                "history": history,
                "sample_data": sample
            },
            "lineage": {
                "upstreams": self._enrich_upstream_lineage(rich_lineage['upstreams'], details),
                "downstreams": self._enrich_lineage_list(rich_lineage['downstreams']),
                "column_lineage": rich_lineage['column_level'],
                "facets": rich_lineage['facets']
            }
        }

    def _enrich_upstream_lineage(self, table_tuples, failing_config):
        enriched = []
        for s, t in table_tuples:
            observers = self.incidents.fetch_table_observers(s, t)
            
            matching_obs_id = self.incidents.find_matching_observer_id(s, t, failing_config)
            
            obs_history = []
            if matching_obs_id:
                obs_history = self.incidents.fetch_historical_metrics(matching_obs_id, limit=3)

            enriched.append({
                "table": f"{s}.{t}",
                "observers": observers,
                "observer_history": obs_history
            })
        return enriched

    def _enrich_lineage_list(self, table_tuples):
        return [{
            "table": f"{s}.{t}",
            "observers": self.incidents.fetch_table_observers(s, t)
        } for s, t in table_tuples]

if __name__ == "__main__":
    CONFIGS = {
        "obs_db": {"dbname": "observers_db", "user": "postgres", "password": "admin", "host": "localhost", "port": 5433},
        "target_db": {"dbname": "sales_data_platform", "user": "postgres", "password": "admin", "host": "localhost", "port": 5433},
        "mongo_uri": "mongodb://admin:admin@localhost:27017/",
        "lineage_url": "https://qupid-watchpipe.qupid.clusterdiali.me/api/v1/lineage",
        "namespace": "postgres://host.docker.internal:5433"
    }

    orchestrator = ContextBuilderOrchestrator(CONFIGS)
    orchestrator.run_daily_context_enrichment()