from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from mongodb.operators import MongoDBUpdateOperator
import requests


def llm(prompt, model, base_url):
    """
    Prompt a given LLM model using Ollama API.
    """
    url = f"{base_url}/api/generate"
    data = {"model": model, "prompt": prompt, "stream": False}

    # If response is successful, return the generated text
    print(f"Prompting {model} with {prompt}")
    if response := requests.post(url, json=data):
        print(f"Generated in {response.elapsed.total_seconds()}")
        return response.json()["response"].strip()


def extract_message(document):
    """
    Extract message from a document using LLMs.
    """
    # Extract context
    text = document["content"]["about"]["text"][0]

    # Enrich document
    document["message"] = llm(text, "message-llm", "http://host.docker.internal:11434")
    return document



with DAG(
    "llm_enrichment",
    start_date=datetime.now(),
    description="A DAG that enrich imkg dataset using LLMs.",
    default_args={"retries": 0},
    tags=["llm", "enrichment"],
):
    start_task = DummyOperator(task_id="start")

    enrich_message_description_task = MongoDBUpdateOperator(
        task_id="enrich_message_description",
        conn_id="mongodb_default",
        database="airflow",
        collection="memes",
        filter={"content.about.text": {"$exists": True}},  # Used as context for LLM
        update=extract_message,
    )

    enrich_image_description_task = DummyOperator(task_id="enrich_image_description")

    enrich_situation_description_task = DummyOperator(
        task_id="enrich_situation_description"
    )

    end_task = DummyOperator(task_id="end")

    (
        start_task
        >> [
            enrich_message_description_task,
            enrich_image_description_task,
            enrich_situation_description_task,
        ]
        >> end_task
    )
