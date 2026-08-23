import os
import yaml
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

file_path = os.path.join(os.path.dirname(__file__), "..", "config", "entities.yaml")
try:
    with open(file_path, "r") as file:
        entities_config = yaml.safe_load(file)
        reddit_config = entities_config.get("reddit", {})
        subreddits = reddit_config.get("subreddits", [])
        termos_busca = reddit_config.get("termo_busca", [])
except Exception as e:
    print(f"Erro ao carregar entidades: {e}")
    subreddits = []
    termos_busca = []

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
    max_active_tasks = 1,   # só 1 requisição por vez, mesmo dentro do mesmo dia (evita sobrecarregar a API pública)
    max_active_runs = 1,    # só 1 execução (dia) por vez -- impede catchup + trigger manual rodando juntos
    tags = ["reddit", "collector"]
) as dag:

    file_script_path = os.path.join(os.path.dirname(__file__), "..", "pipelines", "collectors", "reddit_collector.py")

    # Uma task para cada combinação (subreddit x termo_busca).
    # Ex: 2 subreddits x 2 candidatos = 4 tasks por execução diária.
    for subreddit in subreddits:
        for termo in termos_busca:

            comando = (
                f"python3 {file_script_path} "
                f"--subreddit {subreddit} "
                f"--termo-busca \"{termo}\" "
                f"--data-inicio {{{{ data_interval_start | ds }}}} "
                f"--data-fim {{{{ data_interval_end | ds }}}}"
            )

            # task_id não pode ter espaços/acentos -> normalizamos o termo
            termo_slug = "".join(c if c.isalnum() else "_" for c in termo.lower())

            task_collect_reddit = BashOperator(
                task_id = f"collect_reddit_{subreddit}_{termo_slug}",
                bash_command = comando,
            )