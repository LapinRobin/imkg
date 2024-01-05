import urllib.request
import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def get_models(host: str):
    """
    Make a request to the Ollama API to get a list of available models.
    """
    url = f"http://{host}/api/tags"

    try:
        with urllib.request.urlopen(url) as response:
            # Ensure the request was successful (status code 200)
            if response.getcode() == 200:
                # Load JSON directly from the response
                data = json.load(response)
                return data
            else:
                print(f"Request failed with status code: {response.getcode()}")
                return None

    except Exception as e:
        print(f"Error during request: {e}")
        exit(1)


default_args = {
    "retries": 0,
}

with DAG(
    "ollama_examples",
    start_date=datetime.now(),
    description="A DAG demonstrating the use of Ollama from host machine.",
    default_args=default_args,
    tags=["ollama", "example"],
):
    get_models_task = PythonOperator(
        task_id="get_models", # host.docker.internal resolve to host, only tested on Mac
        python_callable=lambda: print(get_models("host.docker.internal:11434")),
    )

    get_models_task
