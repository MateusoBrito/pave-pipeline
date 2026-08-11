import os
import yaml
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

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
    "start_date":   datetime(2026, 8, 1),
}

with DAG(
    "collect_meta_dag",
    schedule_interval = "@daily",
    default_args = default_args,
    catchup = True,
    tags = ["meta", "collector"]
) as dag:

    file_script_path = os.path.join(os.path.dirname(__file__), "..", "pipelines", "collectors", "meta_collector.py")

    for perfil in perfis:
        page_id = perfil.get("page_id")
        nome = perfil.get("nome", "").replace(" ", "_").lower()
        
        comando = (
            f"python3 {file_script_path} "
            f"--entidade {page_id} "
            f"--data-inicio {{{{ data_interval_start | ds }}}} "
            f"--data-fim {{{{ data_interval_end | ds }}}}"
        )

        task_collect_meta = BashOperator(
            task_id = f"collect_meta_{nome}",
            bash_command = comando
        )