import os
import yaml
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule

from airflow import DAG
from airflow.operators.bash import BashOperator

# Usando caminho absoluto baseado na raiz do Airflow no container (/opt/airflow)
AIRFLOW_HOME = "/opt/airflow"
file_path = os.path.join(AIRFLOW_HOME, "config", "entities.yaml")

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
    "retry_delay":  timedelta(minutes=5),
    "start_date":   datetime(2026, 1, 1),
}

with DAG(
    "dag_inferencia",
    default_args = default_args,
    schedule_interval="0 5 * * *",
    catchup = False,
    max_active_runs = 1,
    tags = ["nlp", "inferencia", "topicos", "diario"],
) as dag:

    # Caminho absoluto para o script de inferência
    file_script_path = os.path.join(AIRFLOW_HOME, "pipelines", "etl", "infer_topic.py")

    # O diretório mapeado no Docker que aponta para /home/labpi/pave-tm
    model_dir_base = "/opt/airflow/pave-tm/resultado_final"

    tasks_criadas = []
    num_k = "k15"
    model_clustering = "bertopic_kmeans"
    embedding_model = "qwen_qwen3_embedding_0_6b"

    python_venv_path = "/opt/airflow/pave-tm/.venv/bin/python"

    # LISTA HARDCODED PARA TESTE
    CANDIDATOS_TESTE = ["lula", "flavio_bolsonaro"]

    for rede_social in REDES_SOCIAIS:
        candidatos = obter_candidatos(rede_social)
        
        for candidato in candidatos:
            nome_task = candidato.replace(" ", "_").lower()
            
            if nome_task not in CANDIDATOS_TESTE:
                continue
            
            model_dir = os.path.join(model_dir_base, rede_social, nome_task, num_k, model_clustering, embedding_model)
            
            comando = (
                f"source /opt/airflow/pave-tm/.venv/bin/activate && "
                f"python {file_script_path} "
                f"--fonte {rede_social} "
                f'--candidato "{nome_task}" '
                f'--model_dir "{model_dir}"'
            )
            
            task = BashOperator(
                task_id=f"inferencia_{rede_social}_{nome_task}",
                bash_command=comando,
                trigger_rule=TriggerRule.ALL_DONE,
            )
            tasks_criadas.append(task)

    if tasks_criadas:
        for i in range(len(tasks_criadas) - 1):
            tasks_criadas[i] >> tasks_criadas[i + 1]