import os
import yaml
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

file_path = os.path.join(os.path.dirname(__file__), "..", "config", "entities.yaml")
try:
    with open(file_path, "r") as file:
        entities_config = yaml.safe_load(file)
        subreddits = entities_config.get("reddit", {}).get("subreddits", [])
except Exception as e:
    print(f"Erro ao carregar entidades: {e}")
    subreddits = []

default_args = {
    "owner":        "airflow",
    "retries":      3,
    "retry_delay":  timedelta(minutes=5),
    "start_date":   datetime(2026, 8, 1),
}

with DAG(
    "collect_reddit_dag",
    schedule_interval = "@daily",
    default_args = default_args,
    catchup = True,
    tags = ["reddit", "collector"]
) as dag:

    file_script_path = os.path.join(os.path.dirname(__file__), "..", "pipelines", "collectors", "reddit_collector.py")

    for subreddit in subreddits:
        
        comando = (
            f"python3 {file_script_path} "
            f"--entidade {subreddit} "
            f"--data-inicio {{{{ data_interval_start | ds }}}} "
            f"--data-fim {{{{ data_interval_end | ds }}}}"
        )

        task_collect_reddit = BashOperator(
            task_id = f"collect_reddit_{subreddit}",
            bash_command = comando
        )