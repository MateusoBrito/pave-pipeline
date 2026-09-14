"""
dag_inferencia.py
"Transform" a cada 2 horas: aplica o modelo do PERÍODO (dia/semana/N dias - ver
TOPIC_MODEL_PERIOD_DAYS) já ajustado (ver dag_topic_modeling.py) aos documentos que
chegaram desde então e ainda não têm tópico - roda pipelines/etl/infer_topic.py, que
faz o MESMO pré-processamento usado para ajustar o modelo (pipeline.preprocessing do
pave-tm - nominalização de verbos via Stanza) antes do `.transform()`. Sem isso, o
`.transform()` vê um tipo de texto diferente do que o modelo aprendeu (bruto vs.
pré-processado) e as atribuições de tópico ficam ruins - ver o docstring de
infer_topic.py.

Não passamos --day aqui: infer_topic.py já calcula sozinho o início do último período
completo (ultimo_periodo_completo, usando as env vars TOPIC_MODEL_PERIOD_DAYS/_EPOCH -
ver docker-compose.yml) - repetir essa conta aqui em bash só duplicaria a lógica com o
risco de as duas divergirem se a cadência mudar.

Roda depois da coleta (dag_pipeline.py, também a cada 2h) e do período já ter sido
modelado (dag_topic_modeling.py) - antes disso, ou num período sem volume suficiente
pra gerar modelo, infer_topic.py simplesmente não encontra um `modelo` vigente pra
aquele período e não faz nada (sem erro).
"""
import os
from datetime import datetime, timedelta

import yaml

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from common.datasets import TOPICOS_INFERIDOS

AIRFLOW_HOME = "/opt/airflow"
ENTITIES_PATH = os.path.join(AIRFLOW_HOME, "config", "entities.yaml")

try:
    with open(ENTITIES_PATH, "r") as file:
        entities_config = yaml.safe_load(file)
except Exception as e:
    print(f"[dag_inferencia] Erro ao carregar entidades: {e}")
    entities_config = {}


def obter_candidatos(rede_social: str) -> list:
    """Nomes de candidato como aparecem no Mongo/entidade.nome - mesma fonte usada por
    dag_topic_modeling.py (ver docstring de obter_candidatos lá)."""
    config_rede = entities_config.get(rede_social, {})
    if rede_social == "reddit":
        return config_rede.get("termo_busca", [])
    if rede_social == "meta":
        return [p.get("nome") for p in config_rede.get("perfis", []) if p.get("nome")]
    if rede_social == "youtube":
        # Modelagem/inferência são por candidato, não por canal de notícia - ver
        # mesma nota em dag_topic_modeling.py.
        return config_rede.get("termo_busca", [])
    return []


def slugify(nome: str) -> str:
    """Mesmo algoritmo de seed_entidades.py: é o que gera entidade.codigo no Postgres -
    precisa bater exatamente, senão --candidato não acha o `alvo_coleta`/`modelo` certo."""
    import re
    import unicodedata

    sem_acento = unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-zA-Z0-9]+", "_", sem_acento).strip("_").lower()


REDES_SOCIAIS = ["reddit", "meta", "youtube"]

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 8, 31),
}

INFER_SCRIPT = os.path.join(AIRFLOW_HOME, "pipelines", "etl", "infer_topic.py")

with DAG(
    "dag_inferencia",
    default_args=default_args,
    schedule_interval="0 */2 * * *",
    catchup=False,
    max_active_runs=1,
    # Sequencial, não paralelo: cada task carrega seu próprio encoder de embeddings +
    # modelo BERTopic na GPU (mesma cautela do dag_inferencia original, que encadeava
    # as tasks uma após a outra em vez de deixá-las concorrentes).
    max_active_tasks=1,
    tags=["nlp", "inferencia", "topicos"],
    is_paused_upon_creation=False,
) as dag:

    tasks_inferencia = []
    for rede_social in REDES_SOCIAIS:
        for candidato in obter_candidatos(rede_social):
            slug = slugify(candidato)
            comando = (
                f"python3 {INFER_SCRIPT} "
                f"--fonte {rede_social} "
                f'--candidato "{slug}" '
                f'--candidato-mongo "{candidato}"'
                # Sem --day: infer_topic.py calcula sozinho o último período completo
                # (ultimo_periodo_completo, TOPIC_MODEL_PERIOD_DAYS/_EPOCH em horário de
                # Brasília) - ver docstring do topo deste arquivo.
            )
            tasks_inferencia.append(
                BashOperator(
                    task_id=f"inferencia_{rede_social}_{slug}",
                    bash_command=comando,
                    trigger_rule=TriggerRule.ALL_DONE,
                )
            )

    # Marca o dataset - dag_classificacao_sentimentos roda em cima disso, não num cron
    # próprio: sentimento tem que vir depois da atribuição de tópico, não em paralelo.
    inferencia_done = EmptyOperator(
        task_id="inferencia_done",
        trigger_rule=TriggerRule.ALL_DONE,
        outlets=[TOPICOS_INFERIDOS],
    )
    tasks_inferencia >> inferencia_done
