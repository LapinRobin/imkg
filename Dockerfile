FROM apache/airflow

# Install a java runtime environment
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    default-jdk \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Install additional python packages
ADD requirements.txt . 
RUN pip install apache-airflow==2.8.1 -r requirements.txt
