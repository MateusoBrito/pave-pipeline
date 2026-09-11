"""
dag_pipeline_backfill.py
DAG de backfill diário que orquestra a coleta + carga:

  Coleta (Meta + Reddit + YouTube)
      -> Carga no Postgres

Sem etapa de pré-processamento (removida - ver comentário antes de "CARGA -
Postgres"): a modelagem/inferência de tópicos fazem seu próprio pré-processamento
no momento do uso (pipeline.preprocessing.preprocess_dataframe, em pave-tm) -
ver dag_topic_modeling.py e dag_inferencia.py.

Executa uma coleta simultânea por rede. Os itens de uma mesma rede são
processados sequencialmente para evitar sobrecarregar as APIs.
"""

import os
import shlex
import yaml
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Caminhos base
# ---------------------------------------------------------------------------
DAGS_DIR          = os.path.dirname(__file__)
ROOT_DIR          = os.path.join(DAGS_DIR, "..")
ENTITIES_PATH     = os.path.join(ROOT_DIR, "config", "entities.yaml")

COLLECTORS_DIR    = os.path.join(ROOT_DIR, "pipelines", "collectors")
ETL_DIR           = os.path.join(ROOT_DIR, "pipelines", "etl")

META_SCRIPT       = os.path.join(COLLECTORS_DIR, "meta_collector.py")
REDDIT_SCRIPT     = os.path.join(COLLECTORS_DIR, "reddit_collector.py")
YOUTUBE_SCRIPT    = os.path.join(COLLECTORS_DIR, "youtube_collector.py")
SEED_SCRIPT       = os.path.join(ETL_DIR,        "seed_entidades.py")
LOAD_SCRIPT       = os.path.join(ETL_DIR,        "load_documentos.py")

START_DATE = pendulum.datetime(2026, 6, 1, tz="UTC")
COLETA_POOL = "coleta_pool"


def _comando_coleta(script: str, argumentos: list[str]) -> str:
    """Executa os itens de uma rede em série dentro de uma única task."""
    comandos = []
    for argumento in argumentos:
        comandos.append(
            f"python3 {script} {argumento} "
            "--data-inicio {{ data_interval_start | ds }} "
            "--data-fim {{ macros.ds_add(data_interval_start | ds, 1) }}"
        )
    return "set -e\n" + "\n".join(comandos)

# ---------------------------------------------------------------------------
# Carrega entities.yaml  (quais candidatos/canais/subreddits coletar)
# ---------------------------------------------------------------------------
try:
    with open(ENTITIES_PATH, "r", encoding="utf-8") as f:
        entities_config = yaml.safe_load(f)
except Exception as e:
    print(f"[dag_pipeline] Erro ao carregar entities.yaml: {e}")
    entities_config = {}

# -- Meta
meta_cfg = entities_config.get("meta", {})
perfis   = meta_cfg.get("perfis", [])

# -- Reddit
reddit_cfg         = entities_config.get("reddit", {})
subreddits         = reddit_cfg.get("subreddits", [])
termos             = reddit_cfg.get("termo_busca", [])
combinacoes_reddit = [{"subreddit": s, "termo_busca": t} for s in subreddits for t in termos]

# -- YouTube
youtube_cfg = entities_config.get("youtube", {})
canais      = youtube_cfg.get("canais", [])

# ---------------------------------------------------------------------------
# default_args  (usado como base pela DAG)
# ---------------------------------------------------------------------------
default_args = {
    "owner":          "airflow",
    "retries":        3,
    "retry_delay":    timedelta(minutes=5),
    "start_date":     START_DATE,
    "email_on_failure": False,
    "email_on_retry":   False,
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="pipeline_backfill_dag",
    default_args=default_args,
    description="Backfill diário de 2026-06-01 até hoje: coleta e carga Postgres",
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["backfill", "meta", "reddit", "youtube", "postgres"],
) as dag:

    # -----------------------------------------------------------------------
    # COLETA - Meta Ads
    # -----------------------------------------------------------------------
    meta_argumentos = [
        f"--entidade {shlex.quote(perfil.get('page_id', ''))}"
        for perfil in perfis
    ]
    tg_meta = BashOperator(
        task_id="collect_meta",
        bash_command=_comando_coleta(META_SCRIPT, meta_argumentos),
        pool=COLETA_POOL,
        pool_slots=1,
    )

    # -----------------------------------------------------------------------
    # COLETA - Reddit
    # -----------------------------------------------------------------------
    reddit_argumentos = [
        "--subreddit " + shlex.quote(combo["subreddit"]) +
        " --termo-busca " + shlex.quote(combo["termo_busca"])
        for combo in combinacoes_reddit
    ]
    tg_reddit = BashOperator(
        task_id="collect_reddit",
        bash_command=_comando_coleta(REDDIT_SCRIPT, reddit_argumentos),
        pool=COLETA_POOL,
        pool_slots=1,
    )

    # -----------------------------------------------------------------------
    # COLETA - YouTube
    # -----------------------------------------------------------------------
    youtube_argumentos = [
        f"--entidade {shlex.quote(canal.get('channel_id', ''))}"
        for canal in canais
    ]
    tg_youtube = BashOperator(
        task_id="collect_youtube",
        bash_command=_comando_coleta(YOUTUBE_SCRIPT, youtube_argumentos),
        pool=COLETA_POOL,
        pool_slots=1,
    )

    # -----------------------------------------------------------------------
    # Barreira: aguarda toda a coleta terminar (mesmo com falhas parciais)
    # -----------------------------------------------------------------------
    all_collected = EmptyOperator(
        task_id="all_collected",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    [tg_meta, tg_reddit, tg_youtube] >> all_collected

    # -----------------------------------------------------------------------
    # PRE-PROCESSAMENTO (removido)
    # O pré-processamento antigo (pipelines/nlp/preprocess.py, PreProcessing por
    # lematização NLTK) foi deprecado - ninguém lê o `processed_text` que ele
    # escrevia de volta no Mongo (a carga no Postgres só copia `texto` bruto, e a
    # modelagem/inferência de tópicos fazem seu próprio pré-processamento no
    # momento do uso, via pipeline.preprocessing.preprocess_dataframe do pave-tm -
    # ver dag_topic_modeling.py e pipelines/etl/infer_topic.py). Rodar essa etapa
    # aqui era só custo de Stanza/NLTK sem consumidor.
    # -----------------------------------------------------------------------

    # -----------------------------------------------------------------------
    # CARGA - Postgres
    # -----------------------------------------------------------------------
    seed_entidades = BashOperator(
        task_id="seed_entidades",
        bash_command=f"python3 {SEED_SCRIPT}",
    )

    load_youtube = BashOperator(
        task_id="load_youtube",
        bash_command=f"python3 {LOAD_SCRIPT} --fontes youtube",
    )

    load_reddit = BashOperator(
        task_id="load_reddit",
        bash_command=f"python3 {LOAD_SCRIPT} --fontes reddit",
    )

    load_meta = BashOperator(
        task_id="load_meta",
        bash_command=f"python3 {LOAD_SCRIPT} --fontes meta",
    )

    postgres_loaded = EmptyOperator(task_id="postgres_loaded", trigger_rule=TriggerRule.ALL_DONE)

    all_collected >> seed_entidades >> [load_youtube, load_reddit, load_meta] >> postgres_loaded
