from mongo.operators import MongoDBInsertOperator, MongoDBInsertJSONFileOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.papermill_operator import PapermillOperator
import airflow.utils as utils
from airflow import DAG
import pandas as pd
import pymongo
import json, csv
from csv import writer, reader
import os

default_args = {"start_date": utils.dates.days_ago(0), "concurrency": 1, "retries": 0}

# Generate the DAG
new_full_kym_dag = DAG(
    dag_id="new_full_kym_dag",
    default_args=default_args,
    schedule_interval=None,
)

#file_sensor = FileSensor(
#    task_id="file_sensor",
#    filepath="/opt/airflow/data/raw.json",
#    fs_conn_id="fs_default",
#    poke_interval=30,
#    dag=new_full_kym_dag,
#)

#insert_json_file_task = MongoDBInsertJSONFileOperator(
#    task_id="insert_json_file_into_mongodb",
#    mongo_conn_id="mongodb_default",  # Connection ID for MongoDB
#    database="memes",
#    collection="raw_memes",
#    filepath="/opt/airflow/data/raw.json",
#    dag=new_full_kym_dag,
#)

def save2json(filename, dump):
    out_file = open(filename, "w")
    json.dump(dump, out_file, indent=6)
    out_file.close()

def extractFromMongo():
    # MongoDB connection details
    mongo_uri = "mongodb://mongo:27017"
    database_name = "memes"
    collection_name = "raw_memes"

    # Retreive dataset
    client = pymongo.MongoClient(mongo_uri)
    db = client[database_name]
    collection = db[collection_name]

    data = list(collection.find())

    for document in data:
        if "_id" in document:
            document.pop("_id")

    save2json("/opt/airflow/notebooks/data/raw_data.json", data)

    client.close()


load_data_from_Mongo = PythonOperator(
    task_id="load_data_from_Mongo",
    dag=new_full_kym_dag,
    python_callable=extractFromMongo,
    trigger_rule="all_success",
)

notebook_cleaning = PapermillOperator(
    task_id="notebook_cleaning",
    input_nb="/opt/airflow/notebooks/Cleaning.ipynb",
    output_nb="/opt/airflow/notebooks/Cleaning.ipynb",
    dag=new_full_kym_dag,
    trigger_rule="all_success",
)

notebook_extract_seed = PapermillOperator(
    task_id="notebook_extract_seed",
    input_nb="/opt/airflow/notebooks/Extract_Seed.ipynb",
    output_nb="/opt/airflow/notebooks/Extract_Seed.ipynb",
    dag=new_full_kym_dag,
    trigger_rule="all_success",
)

notebook_extract_sibling = PapermillOperator(
    task_id="notebook_extract_sibling",
    input_nb="/opt/airflow/notebooks/Extract_Sibling.ipynb",
    output_nb="/opt/airflow/notebooks/Extract_Sibling.ipynb",
    dag=new_full_kym_dag,
    trigger_rule="all_success",
)

load_data_from_Mongo >> notebook_cleaning >> notebook_extract_seed >> notebook_extract_sibling