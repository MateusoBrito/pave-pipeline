import os
import yaml
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def read_entities():
    file_path = os.path.join(os.path.dirname(__file__), ".." ,"config", "entities_config.yaml")

    print(f"Loading entities from: {file_path}")
    with open(file_path, "r") as file:
        entities = yaml.safe_load(file)
    print(f"Entities loaded: {entities}")

    return entities

default_args = {
    "owner":        "airflow",
    "retries":      3,
    "retry_delay":  timedelta(minutes=5),
    "start_date":   datetime(2026, 8, 1),
}

with DAG(
    "collect_youtube_dag",
    schedule_interval = "@daily",
    default_args = default_args,
    catchup = False,
    tags = ["youtube", "collector"]
) as dag:

    task_read_entities = PythonOperator(
        task_id = "read_entities",
        python_callable = read_entities
    )

    file_script_path = os.path.join(os.path.dirname(__file__), "..", "pipelines", "collectors", "youtube_collector.py")
    task_collect_youtube = BashOperator(
        task_id = "collect_youtube",
        bash_command = f"echo 'Iniciando {file_script_path}...' && sleep 5 && echo 'Coleta finalizada com sucesso!'"
    )

    task_read_entities >> task_collect_youtube