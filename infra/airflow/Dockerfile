FROM apache/airflow:3.0.0-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==3.0.0" \
    "apache-airflow-providers-fab" \
    boto3 \
    pandas \
    pyarrow \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.11.txt"

RUN pip install --no-cache-dir "great_expectations==0.18.19"
RUN pip install --no-cache-dir "pyspark==3.5.0"
