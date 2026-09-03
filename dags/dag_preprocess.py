from datetime import datetime, timedelta
import yaml
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "entities.yaml")

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    entities_config = yaml.safe_load(f)


tarefas = []

reddit_cfg = entities_config.get("reddit", {})
if reddit_cfg.get("text_columns"):
    tarefas.append({
        "rede": "reddit",
        "collection": reddit_cfg.get("colecao", "reddit"),
        "columns": reddit_cfg["text_columns"],
    })

meta_cfg = entities_config.get("meta", {})
if meta_cfg.get("text_columns"):
    tarefas.append({
        "rede": "meta",
        "collection": meta_cfg.get("colecao", "meta"),
        "columns": meta_cfg["text_columns"],
    })

youtube_cfg = entities_config.get("youtube", {})
for col_item in youtube_cfg.get("colecoes", []):
    if col_item.get("text_columns"):
        tarefas.append({
            "rede": "youtube",
            "collection": col_item["nome"],
            "columns": col_item["text_columns"],
        })

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nlp_preprocessing_dag",
    default_args=default_args,
    description="Pré-processamento de texto das coleções do MongoDB",
    schedule_interval="0 3 * * *",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["nlp", "preprocessing"],
) as dag:

    script_path = os.path.join(
        os.path.dirname(__file__), "..", "pipelines", "nlp", "preprocess.py"
    )

    file_nlp_path = os.path.join(
        os.path.dirname(__file__), "..", "pipelines", "nlp", "classificar_sentimentos.py"
    )

    task_classificacao_nlp = BashOperator(
        task_id="classificar_sentimentos_bertimbau",
        bash_command=f"python3 {file_nlp_path} --data-inicio {{{{ ds }}}}"
    )

    for tarefa in tarefas:
        task_preprocess = BashOperator(
            task_id=f"preprocess_{tarefa['rede']}_{tarefa['collection']}",
            bash_command=(
                f"python3 {script_path} "
                f"--collection {tarefa['collection']} "
                f"--columns {' '.join(tarefa['columns'])} "
                f"--rede-social {tarefa['rede']}"
            ),
        )

        task_preprocess >> task_classificacao_nlp