from mongo.operators import MongoDBInsertOperator, MongoDBInsertJSONFileOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.papermill_operator import PapermillOperator
from airflow.operators.bash_operator import BashOperator
import airflow.utils as utils
from airflow import DAG
import pandas as pd
import pymongo
import json, csv
from csv import writer, reader
import os
from docker.types import Mount

default_args = {"start_date": utils.dates.days_ago(0), "concurrency": 1, "retries": 0}

# Generate the DAG
generate_rdf = DAG(
    dag_id="generate_rdf",
    default_args=default_args,
    schedule_interval=None,
)

create_nt_files = BashOperator(
    task_id='create_nt_files',
    dag=generate_rdf,
    bash_command="/opt/airflow/generate_media_frame.rdf.sh ",
)

create_nt_files