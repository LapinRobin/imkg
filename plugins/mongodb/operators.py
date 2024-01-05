import json

from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models import BaseOperator


class MongoDBUpdateOperator(BaseOperator):
    """
    Custom Airflow operator to update a documents in a MongoDB collection.
    """

    def __init__(
        self, conn_id: str, database: str, collection: str, filter: str, update, **kwargs
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.database = database
        self.collection = collection
        self.filter = filter
        self.update = update

    def execute(self, context):
        try:
            # Connect to MongoDB
            hook = MongoHook(mongo_conn_id=self.conn_id)
            client = hook.get_conn()
            db = client[self.database]
            collection = db[self.collection]

            # Update documents
            documents = collection.find(self.filter)
            for document in documents:
                collection.update_one(document, self.update(document))
        except Exception as e:
            print(f"Error connecting to MongoDB -- {e}")
            exit(1)


class MongoDBInsertJSONFileOperator(BaseOperator):
    """
    Custom Airflow operator to insert documents from a JSON file into a MongoDB collection.
    """

    def __init__(
        self, conn_id: str, database: str, collection: str, filepath: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.database = database
        self.collection = collection
        self.filepath = filepath

    def execute(self, context):
        try:
            # Connect to MongoDB
            hook = MongoHook(mongo_conn_id=self.conn_id)
            client = hook.get_conn()
            db = client[self.database]
            collection = db[self.collection]

            # Insert documents
            with open(self.filepath) as f:
                file_data = json.load(f)
            collection.insert_many(file_data)
        except Exception as e:
            print(f"Error connecting to MongoDB -- {e}")
            exit(1)
