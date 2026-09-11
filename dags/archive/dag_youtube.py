import os
import yaml
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from common.datasets import RAW_YOUTUBE

file_path = os.path.join(os.path.dirname(__file__), "..", "config", "entities.yaml")
try:
    with open(file_path, "r") as file:
        entities_config = yaml.safe_load(file)
        canais = entities_config.get("youtube", {}).get("canais", [])
except Exception as e:
    print(f"Erro ao carregar entidades: {e}")
    canais = []

default_args = {
    "owner":        "airflow",
    "retries":      3,
    "retry_delay":  timedelta(minutes=5),
    "start_date":   datetime(2026, 1, 1),
}

with DAG(
    "collect_youtube_dag",
    schedule_interval = "@daily",
    default_args = default_args,
    catchup = True,
    tags = ["youtube", "collector"],
    is_paused_upon_creation=True,
) as dag:

    file_script_path = os.path.join(os.path.dirname(__file__), "..", "pipelines", "collectors", "youtube_collector.py")

    canais_params = [
        {"channel_id": c.get("channel_id"), "nome": c.get("nome", "").replace(" ", "_").lower()}
        for c in canais
    ]

    collect_youtube = BashOperator.partial(
        task_id="collect_youtube",
        bash_command=(
            f"python3 {file_script_path} "
            "--entidade {{ params.channel_id }} "
            "--data-inicio {{ data_interval_start | ds }} "
            "--data-fim {{ data_interval_end | ds }}"
        ),
    ).expand(params=canais_params)

    youtube_collected = EmptyOperator(task_id="youtube_collected", outlets=[RAW_YOUTUBE])
    collect_youtube >> youtube_collected
