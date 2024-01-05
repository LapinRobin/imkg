from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from mongodb.operators import MongoDBInsertJSONFileOperator, MongoDBUpdateOperator

default_args = {
    "descriptions": "A DAG demonstrating custom MongoDB operators",
    "retries": 0,
}

with DAG("mongodb_examples", start_date=datetime.now(), default_args=default_args, tags=["mongodb", "example"]):
    start_task = DummyOperator(task_id="start_task")

    insert_json_file_task = MongoDBInsertJSONFileOperator(
        task_id="insert_json_file",
        conn_id="mongodb_default",  # Connection ID for MongoDB
        database="airflow",
        collection="examples",
        filepath="/opt/airflow/data/examples/mongodb.json",
    )

    def update_function(document):
        document["last_updated"] = datetime.now()
        return document

    update_task = MongoDBUpdateOperator(
        task_id="update",
        conn_id="mongodb_default",  # Connection ID for MongoDB
        database="airflow",
        collection="examples",
        filter={},  # Select all documents
        update=update_function,
    )

    end_task = DummyOperator(task_id="end_task")

start_task >> insert_json_file_task >> update_task >> end_task
