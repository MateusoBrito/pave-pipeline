import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from common.datasets import PREPROCESSED, POSTGRES_LOAD

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="load_postgres_dag",
    default_args=default_args,
    description="Carrega documento/documento_entidade (Mongo -> Postgres) a partir dos dados brutos coletados",
    schedule =[PREPROCESSED],
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["postgres", "etl"],
    is_paused_upon_creation=True,
) as dag:

    seed_script = os.path.join(os.path.dirname(__file__), "..", "pipelines", "etl", "seed_entidades.py")
    load_script = os.path.join(os.path.dirname(__file__), "..", "pipelines", "etl", "load_documentos.py")

    seed_entidades = BashOperator(
        task_id="seed_entidades",
        bash_command=f"python3 {seed_script}",
    )

    load_youtube = BashOperator(
        task_id="load_youtube",
        bash_command=f"python3 {load_script} --fontes youtube",
    )

    load_reddit = BashOperator(
        task_id="load_reddit",
        bash_command=f"python3 {load_script} --fontes reddit",
    )

    load_meta = BashOperator(
        task_id="load_meta",
        bash_command=f"python3 {load_script} --fontes meta",
    )

    seed_entidades >> [load_youtube, load_reddit, load_meta]
    postgres_loaded = EmptyOperator(task_id="postgres_loaded", outlets=[POSTGRES_LOAD])
    [load_youtube, load_reddit, load_meta] >> postgres_loaded