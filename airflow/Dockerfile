FROM apache/airflow:2.5.1-python3.8

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip --no-cache-dir install --upgrade awscli
