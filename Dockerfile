FROM apache/airflow:2.7.2
ADD requirements.txt . 
RUN pip install apache-airflow==2.7.2 -r requirements.txt