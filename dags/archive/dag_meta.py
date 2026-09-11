import os
import yaml
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from common.datasets import RAW_META  # ajuste o import conforme o PYTHONPATH do seu projeto

file_path = os.path.join(os.path.dirname(__file__), "..", "config", "entities.yaml")
try:
    with open(file_path, "r") as file:
        entities_config = yaml.safe_load(file)
        perfis = entities_config.get("meta", {}).get("perfis", [])
except Exception as e:
    print(f"Erro ao carregar entidades: {e}")
    perfis = []

default_args = {
    "owner":        "airflow",
    "retries":      3,
    "retry_delay":  timedelta(minutes=5),
    "start_date":   datetime(2026, 1, 1),
}

with DAG(
    "collect_meta_ads_dag",
    schedule_interval = "@daily",
    default_args = default_args,
    catchup = True,
    max_active_runs = 1,
    tags = ["meta", "collector"],
    is_paused_upon_creation=True,
) as dag:

    file_script_path = os.path.join(os.path.dirname(__file__), "..", "pipelines", "collectors", "meta_collector.py")

    perfis_params = [
        {
            "page_id": perfil.get("page_id"),
            "nome": perfil.get("nome", "").replace(" ", "_").lower(),
        }
        for perfil in perfis
    ]

    collect_meta = BashOperator.partial(
        task_id="collect_meta",
        bash_command=(
            f"python3 {file_script_path} "
            "--entidade {{ params.page_id }} "
            "--data-inicio {{ data_interval_start | ds }} "
            "--data-fim {{ data_interval_start | ds }}"  
        ),
    ).expand(params=perfis_params)

    meta_collected = EmptyOperator(task_id="meta_collected", outlets=[RAW_META])

    collect_meta >> meta_collected