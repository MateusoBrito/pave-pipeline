import os
import yaml
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from common.datasets import RAW_REDDIT

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 5, 1),
}

def carregar_combinacoes():
    file_path = os.path.join(os.path.dirname(__file__), "..", "config", "entities.yaml")
    with open(file_path, "r") as file:
        reddit_config = yaml.safe_load(file).get("reddit", {})

    subreddits = reddit_config.get("subreddits", [])
    termos = reddit_config.get("termo_busca", [])

    combinacoes = [
        {"subreddit": s, "termo_busca": t}
        for s in subreddits
        for t in termos
    ]

    if not combinacoes:
        raise ValueError(
            f"Nenhuma combinação subreddit/termo encontrada em {file_path}. "
            f"subreddits={subreddits}, termos={termos}"
        )

    return combinacoes

with DAG(
    "collect_reddit_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=True,
    max_active_tasks=1,
    tags=["reddit", "collector"],
    is_paused_upon_creation=True,
) as dag:

    file_script_path = os.path.join(
        os.path.dirname(__file__), "..", "pipelines", "collectors", "reddit_collector.py"
    )

    combinacoes = carregar_combinacoes()

    def montar_args(item, data_interval_start=None, data_interval_end=None, **context):
        return [
            "--subreddit", item["subreddit"],
            "--termo-busca", item["termo_busca"],
            "--data-inicio", context["ds"],
            "--data-fim", context["ds_nodash"],  # ajuste conforme sua necessidade
        ]

    collect_reddit = BashOperator.partial(
        task_id="collect_reddit",
        bash_command=(
            f"python3 {file_script_path} "
            "--subreddit {{ params.subreddit }} "
            "--termo-busca \"{{ params.termo_busca }}\" "
            "--data-inicio {{ data_interval_start | ds }} "
            "--data-fim {{ data_interval_end | ds }}"
        ),
    ).expand(params=combinacoes)

    reddit_collected = EmptyOperator(task_id="reddit_collected", outlets=[RAW_REDDIT])
    collect_reddit >> reddit_collected