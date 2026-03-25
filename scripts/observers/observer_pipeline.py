import uuid
from datetime import datetime, timezone
from config import SparkConfig
from test_fetcher import TestFetcher
from observer_executor import ObserverExecutor
from result_storage import ResultStorage
from incident_manager import IncidentManager

class ObservabilityOrchestrator:
    def __init__(self):
        self.spark = SparkConfig.get_session()
        self.fetcher = TestFetcher(self.spark)
        self.executor = ObserverExecutor(self.spark)
        self.storage = ResultStorage(self.spark)
        self.incidents = IncidentManager(self.spark)

    def run_daily_pipeline(self):
        active_tests = self.fetcher.get_active_observers()
        if not active_tests:
            print("No active observers found. Exiting.")
            return

        execution_results = []
        now = datetime.now(timezone.utc)

        for test in active_tests:
            metric, threshold, status = self.executor.run_observer(test)
            execution_results.append({
                "run_id": str(uuid.uuid4()),
                "observer_id": test.observer_id,
                "execution_time": now.isoformat(),
                "metric_value": metric,
                "threshold_value": threshold,
                "status": status,
                "created_at": now.isoformat()
            })

        self.storage.save_results(execution_results)

        self.incidents.log_new_incidents()
        
        print("Pipeline execution completed.")

if __name__ == "__main__":
    pipeline = ObservabilityOrchestrator()
    pipeline.run_daily_pipeline()