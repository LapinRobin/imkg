from datetime import datetime

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator

default_args = {
    "retries": 0,
}

# Docker containers can be configured with environment variables
docker_env = {
    "MONGO_URL": "mongodb://localhost:27017",
    "MONGO_DB": "airflow",
    "MONGO_COLLECTION": "memes",
    "REDIS_URL": "redis://localhost:6379/",
    "REDIS_PORT": 6379,
    "POSTGRES_USER": "airflow",
    "POSTGRES_PASSWORD": "airflow",
    "POSTGRES_DB": "airflow",
    "POSTGRES_HOST": "localhost",
}

docker_params = {
    "api_version": "1.37",  # Docker API version
    "docker_url": "TCP://docker-socket-proxy:2375",
    "network_mode": "host",
}

with DAG(
    "docker_examples",
    start_date=datetime.now(),
    description="A DAG demonstrating the use of Docker operators",
    default_args=default_args,
    tags=["docker", "example"],
):
    feed_redis = DockerOperator(
        task_id="feed_redis",
        command="scrapy feed 1",
        image="kym-scraper",
        environment=docker_env,
        **docker_params
    )

    bootstrap = DockerOperator(
        task_id="bootstrap",
        command="scrapy crawl bootstrap",
        image="kym-scraper",
        environment=docker_env,
        **docker_params
    )

    scraping = DockerOperator(
        task_id="scraping",
        command="scrapy crawl memes",
        image="kym-scraper",
        environment=docker_env,
        **docker_params
    )

    syncing = DockerOperator(
        task_id="syncing",
        command="scrapy sync_children",
        image="kym-scraper",
        environment=docker_env,
        **docker_params
    )

    feed_redis >> bootstrap >> scraping >> syncing
