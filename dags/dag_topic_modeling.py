import os
import yaml
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

file_path = os.path.join(os.path.dirname(__file__), "..", "config", "entities.yaml")
try:
    with open(file_path, "r") as file:
        entities_config = yaml.safe_load(file)
except Exception as e:
    print(f"Erro ao carregar entidades: {e}")
    entities_config = {}


def obter_candidatos(rede_social: str) -> list:
    config_rede = entities_config.get(rede_social, {})

    if rede_social == "reddit":
        return config_rede.get("termo_busca", [])

    if rede_social == "meta":
        return [p.get("nome") for p in config_rede.get("perfis", []) if p.get("nome")]

    if rede_social == "youtube":
        return [c.get("nome") for c in config_rede.get("canais", []) if c.get("nome")]

    return []


REDES_SOCIAIS = ["reddit", "meta", "youtube"]

default_args = {
    "owner":        "airflow",
    "retries":      1,
    "retry_delay":  timedelta(minutes=10),
    "start_date":   datetime(2026, 1, 1),
}

with DAG(
    "topic_modeling_dag",
    schedule_interval = "@monthly",
    default_args = default_args,
    catchup = False,
    max_active_runs = 1,
    tags = ["nlp", "modeling", "topicos"],
) as dag:

    file_script_path = os.path.join(
        os.path.dirname(__file__), "..", "pipelines", "nlp", "topic_modeling.py"
    )

    for rede_social in REDES_SOCIAIS:
        candidatos = obter_candidatos(rede_social)

        for candidato in candidatos:
            nome_task = candidato.replace(" ", "_").lower()

            comando = (
                f"python3 {file_script_path} "
                f"--rede-social {rede_social} "
                f'--candidato "{candidato}" '
                f"--database panorama"
            )

            task_modelagem = BashOperator(
                task_id = f"topic_modeling_{rede_social}_{nome_task}",
                bash_command = comando,
            )