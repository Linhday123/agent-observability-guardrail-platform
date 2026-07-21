FROM apache/airflow:3.0.0-python3.11

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==3.0.0" \
    "apache-airflow-providers-fab" \
    boto3 \
    pandas \
    pyarrow \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.11.txt"

RUN pip install --no-cache-dir "great_expectations==0.18.19"

RUN pip check