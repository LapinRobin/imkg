from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.mongo.operators.mongo_document import MongoDocumentOperator

# DAG default arguments
default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
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
        mongo_operator = MongoDocumentOperator(
            task_id='insert_into_mongodb',
            mongo_conn_id=self.mongo_conn_id,
            database=self.database,
            collection=self.collection,
            document=self.document,
        )
        return mongo_operator.execute(context)

dag = DAG('mongo_insert_dag', default_args=default_args, schedule_interval=None)

start_task = DummyOperator(task_id='start_task', dag=dag)

insert_mongo_task = MongoDBInsertOperator(
    task_id='insert_into_mongodb',
    mongo_conn_id='mongodb_default',  # Connection ID for MongoDB
    database='your_database',
    collection='your_collection',
    document={'key': 'value', 'timestamp': datetime.now()},
    dag=dag,
)

start_task >> insert_mongo_task