import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime,timedelta

def on_failure_callback(**context):
    print(f"Task {context['task_instance_key_str']} failed.")

def uploadtomongo(ti, **context):
    try:
        print("Start of the UploadToMongo Function")
        hook = MongoHook(mongo_conn_id='mongo_default')
        client = hook.get_conn()
        db = client.airflow
        currency_collection=db.currency_collection
        print(f"Connected to MongoDB - {client.server_info()}")
        currency_collection.insert_one({"test": "test"})
    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")

with DAG(
    dag_id="load_currency_data",
    schedule_interval=None,
    start_date=datetime(2022,10,28),
    catchup=False,
    tags= ["mongo"],
    default_args={
        "owner": "Rob",
        "retries": 0,
        # "retry_delay": timedelta(minutes=5),
        'on_failure_callback': on_failure_callback
    }
) as dag:

    t2 = PythonOperator(
        task_id='upload-mongodb',
        python_callable=uploadtomongo,
        op_kwargs={"result": {"test": "test"}},
        dag=dag
        )

    t2