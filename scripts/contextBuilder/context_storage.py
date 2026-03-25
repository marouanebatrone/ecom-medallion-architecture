from pymongo import MongoClient

class ContextStorage:
    def __init__(self, mongo_uri):
        self.client = MongoClient(mongo_uri)
        self.collection = self.client["qupid_observability"]["incident_contexts"]

    def store_incident_context(self, incident_id, payload):
        self.collection.update_one({"incident_id": incident_id},{"$set": payload},upsert=True)