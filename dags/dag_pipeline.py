"""
dag_pipeline.py
DAG unificada que orquestra a coleta + carga diaria:

  Coleta (Meta + Reddit + YouTube)
      -> Carga no Postgres

Sem etapa de pré-processamento (removida - ver comentário antes de "CARGA -
Postgres"): a modelagem/inferência de tópicos fazem seu próprio pré-processamento
no momento do uso (pipeline.preprocessing.preprocess_dataframe, em pave-tm) -
ver dag_topic_modeling.py e dag_inferencia.py.

Comportamento configuravel em: config/pipeline.yaml
- start_date, retries, retry_delay por etapa
- falha_bloqueia_pipeline por etapa: se true, uma falha para as etapas seguintes
- pool e pool_slots para controlar paralelismo na coleta
"""

import os
import shlex
import yaml
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Caminhos base
# ---------------------------------------------------------------------------
DAGS_DIR          = os.path.dirname(__file__)
ROOT_DIR          = os.path.join(DAGS_DIR, "..")
ENTITIES_PATH     = os.path.join(ROOT_DIR, "config", "entities.yaml")
PIPELINE_CFG_PATH = os.path.join(ROOT_DIR, "config", "pipeline.yaml")

COLLECTORS_DIR    = os.path.join(ROOT_DIR, "pipelines", "collectors")
ETL_DIR           = os.path.join(ROOT_DIR, "pipelines", "etl")

META_SCRIPT       = os.path.join(COLLECTORS_DIR, "meta_collector.py")
REDDIT_SCRIPT     = os.path.join(COLLECTORS_DIR, "reddit_collector.py")
YOUTUBE_SCRIPT    = os.path.join(COLLECTORS_DIR, "youtube_collector.py")
SEED_SCRIPT       = os.path.join(ETL_DIR,        "seed_entidades.py")
LOAD_SCRIPT       = os.path.join(ETL_DIR,        "load_documentos.py")

# ---------------------------------------------------------------------------
# Carrega pipeline.yaml  (configuracoes operacionais da DAG)
# ---------------------------------------------------------------------------
try:
    with open(PIPELINE_CFG_PATH, "r", encoding="utf-8") as f:
        pcfg = yaml.safe_load(f)
except Exception as e:
    print(f"[dag_pipeline] AVISO: nao foi possivel carregar pipeline.yaml: {e}. Usando defaults.")
    pcfg = {}

_dag_cfg      = pcfg.get("dag",           {})
_default_cfg  = pcfg.get("default_task",  {})
_coleta_cfg   = pcfg.get("coleta",        {})
_load_cfg     = pcfg.get("load_postgres", {})

# -- Helpers
def _td(minutes: int) -> timedelta:
    return timedelta(minutes=minutes)

def _trigger(bloqueia: bool) -> TriggerRule:
    """Converte falha_bloqueia_pipeline em TriggerRule."""
    return TriggerRule.ALL_SUCCESS if bloqueia else TriggerRule.ALL_DONE


# -- start_date
_start_date_str = _dag_cfg.get("start_date", "2026-01-01")
START_DATE = datetime.strptime(_start_date_str, "%Y-%m-%d")

# -- Configuracoes por etapa
COLETA_RETRIES         = _coleta_cfg.get("retries",              _default_cfg.get("retries", 3))
COLETA_RETRY_DELAY     = _td(_coleta_cfg.get("retry_delay_minutes", _default_cfg.get("retry_delay_minutes", 5)))
COLETA_POOL            = _coleta_cfg.get("pool",                 "coleta_pool")
COLETA_BLOQUEIA        = _coleta_cfg.get("falha_bloqueia_pipeline", False)

LOAD_RETRIES           = _load_cfg.get("retries",                _default_cfg.get("retries", 2))
LOAD_RETRY_DELAY       = _td(_load_cfg.get("retry_delay_minutes",   _default_cfg.get("retry_delay_minutes", 5)))

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

# -- YouTube (canal de notícia x candidato buscado dentro dele, não mais canal
# próprio do candidato - mesmo padrão do Reddit acima)
youtube_cfg          = entities_config.get("youtube", {})
canais_noticia       = youtube_cfg.get("canais_noticia", [])
termos_youtube       = youtube_cfg.get("termo_busca", [])
combinacoes_youtube  = [{"canal": c["channel_id"], "termo_busca": t} for c in canais_noticia for t in termos_youtube]

# ---------------------------------------------------------------------------
# default_args  (usado como base pela DAG)
# ---------------------------------------------------------------------------
default_args = {
    "owner":          _default_cfg.get("owner", "airflow"),
    "retries":        _default_cfg.get("retries", 3),
    "retry_delay":    _td(_default_cfg.get("retry_delay_minutes", 5)),
    "start_date":     START_DATE,
    "email_on_failure": _default_cfg.get("email_on_failure", False),
    "email_on_retry":   _default_cfg.get("email_on_retry", False),
    "email":            _default_cfg.get("email", []),
}

# ---------------------------------------------------------------------------
# Coleta: pular fatias de 2h redundantes de um dia já encerrado
# ---------------------------------------------------------------------------
# Cada fatia de 2h de um dia HISTÓRICO (não hoje) pede a MESMA janela [dia, dia+1) -
# só faz sentido repetir isso 12x pra hoje (pega conteúdo novo publicado ao longo do
# dia); pra um dia que já passou, as 12 fatias trariam exatamente o mesmo resultado.
# Sem isso, um backfill de N dias custa 12x mais chamadas às APIs do que precisa - só a
# fatia das 00h UTC de um dia já encerrado realmente coleta; as outras 11 saem cedo.
# Checado em bash (não em Jinja) porque precisa do relógio real no momento da
# execução, não da data lógica agendada.
_SKIP_STALE_HISTORICAL_SLICE = (
    'DIA="{{ data_interval_start | ds }}"; HORA="{{ data_interval_start.strftime("%H") }}"; '
    'HOJE=$(date -u +%F); '
    'if [ "$DIA" != "$HOJE" ] && [ "$HORA" != "00" ]; then '
    'echo "Dia $DIA já encerrado e coberto pela fatia das 00h - pulando esta fatia."; exit 0; fi; '
)

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id=_dag_cfg.get("dag_id", "pipeline_diario_dag"),
    default_args=default_args,
    description="Pipeline completo: Coleta -> Carga Postgres",
    schedule_interval=_dag_cfg.get("schedule_interval", "@daily"),
    catchup=_dag_cfg.get("catchup", True),
    max_active_runs=_dag_cfg.get("max_active_runs", 1),
    max_active_tasks=_dag_cfg.get("max_active_tasks", 3),
    tags=["pipeline", "meta", "reddit", "youtube", "nlp", "postgres"],
) as dag:

    # -----------------------------------------------------------------------
    # COLETA - Meta Ads
    # -----------------------------------------------------------------------
    meta_params = [{"page_id": perfil.get("page_id", "")} for perfil in perfis]
    tg_meta = BashOperator.partial(
        task_id="collect_meta",
        bash_command=(
            _SKIP_STALE_HISTORICAL_SLICE +
            f"python3 {META_SCRIPT} "
            "--entidade {{ params.page_id }} "
            "--data-inicio {{ data_interval_start | ds }} "
            "--data-fim {{ macros.ds_add(data_interval_start | ds, 1) }}"
        ),
        pool=COLETA_POOL,
    ).expand(params=meta_params)

    # -----------------------------------------------------------------------
    # COLETA - Reddit
    # -----------------------------------------------------------------------
    tg_reddit = BashOperator.partial(
        task_id="collect_reddit",
        bash_command=(
            _SKIP_STALE_HISTORICAL_SLICE +
            f"python3 {REDDIT_SCRIPT} "
            "--subreddit {{ params.subreddit }} "
            "--termo-busca \"{{ params.termo_busca }}\" "
            "--data-inicio {{ data_interval_start | ds }} "
            "--data-fim {{ macros.ds_add(data_interval_start | ds, 1) }}"
        ),
        pool=COLETA_POOL,
    ).expand(params=combinacoes_reddit)

    # -----------------------------------------------------------------------
    # COLETA - YouTube (canal de notícia x candidato)
    # -----------------------------------------------------------------------
    tg_youtube = BashOperator.partial(
        task_id="collect_youtube",
        bash_command=(
            _SKIP_STALE_HISTORICAL_SLICE +
            f"python3 {YOUTUBE_SCRIPT} "
            "--canal {{ params.canal }} "
            "--termo-busca \"{{ params.termo_busca }}\" "
            "--data-inicio {{ data_interval_start | ds }} "
            "--data-fim {{ macros.ds_add(data_interval_start | ds, 1) }}"
        ),
        pool=COLETA_POOL,
    ).expand(params=combinacoes_youtube)

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
