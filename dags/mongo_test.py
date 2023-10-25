from airflow import DAG
from airflow.models import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.decorators import apply_defaults
from airflow.operators.dummy_operator import DummyOperator
import airflow.utils as utils
import json

default_args = {
    'start_date': utils.dates.days_ago(0),
    'concurrency': 1,
    'retries': 0
}

class MongoDBInsertOperator(BaseOperator):
    """
    Custom Airflow operator to insert a document into a MongoDB collection using the MongoDocumentOperator.
    """
    @apply_defaults
    def __init__(self, mongo_conn_id, database, collection, document, *args, **kwargs):
        super(MongoDBInsertOperator, self).__init__(*args, **kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.database = database
        self.collection = collection
        self.document = document

    def execute(self, context):
        try:
            hook = MongoHook(mongo_conn_id='mongo_default')
            client = hook.get_conn()
            db = client[self.database]
            collection = db[self.collection]
            collection.insert_one(self.document)
        except Exception as e:
            print(f"Error connecting to MongoDB -- {e}")
            exit(1)

class MongoDBInsertJSONFileOperator(BaseOperator):
    """
    Custom Airflow operator to insert a JSON file into a MongoDB collection using the MongoDocumentOperator.
    """

    @apply_defaults
    def __init__(self, mongo_conn_id, database, collection, filepath, *args, **kwargs):
        super(MongoDBInsertJSONFileOperator, self).__init__(*args, **kwargs)
        self.mongo_conn_id = mongo_conn_id
        self.database = database
        self.collection = collection
        self.filepath = filepath

    def execute(self, context):
        try:
            hook = MongoHook(mongo_conn_id='mongo_default')
            client = hook.get_conn()
            db = client[self.database]
            collection = db[self.collection]
            with open(self.filepath) as f:
                file_data = json.load(f)
            collection.insert_many(file_data)
        except Exception as e:
            print(f"Error connecting to MongoDB -- {e}")
            exit(1)

dag = DAG('mongo_test', default_args=default_args, schedule_interval=None)

start_task = DummyOperator(task_id='start_task', dag=dag)

insert_task = MongoDBInsertOperator(
    task_id='insert_into_mongodb',
    mongo_conn_id='mongodb_default',  # Connection ID for MongoDB
    database='airflow',
    collection='tests',
    document={'key': 'value', 'timestamp': "now"},
    dag=dag,
)

insert_json_file_task = MongoDBInsertJSONFileOperator(
    task_id='insert_json_file_into_mongodb',
    mongo_conn_id='mongodb_default',  # Connection ID for MongoDB
    database='airflow',
    collection='tests',
    filepath='/opt/airflow/data/test.json',
    dag=dag,
)

start_task >> (insert_task, insert_json_file_task)