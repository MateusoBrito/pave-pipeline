"""
dag_topic_modeling.py
Modelagem de tópicos POR PERÍODO (pave-tm/pipeline/weekly_topics.py) + carga no
Postgres. "Período" = TOPIC_MODEL_PERIOD_DAYS dias (default 7 = semanal) - único knob
de cadência, lido da mesma env var que pave-tm/pipeline/config.py e infer_topic.py
usam (ver docker-compose.yml, serviço airflow). Trocar diário <-> semanal <-> N dias é
só mudar TOPIC_MODEL_PERIOD_DAYS/_EPOCH lá e reiniciar o container - nada neste arquivo
precisa mudar.

Por que agrupar em períodos maiores que um dia (decisão de 2026-09-07): dia isolado
ficou com volume pequeno demais pra muitos candidatos/redes - choose_k caindo quase
sempre em k=3, e boa parte dos dias nem batia o mínimo de documentos pra gerar modelo
nenhum. Um período maior agrupado dá um corpus mais robusto por ajuste.

A DAG roda TODO DIA às 03:30 UTC (pouco depois da meia-noite de Brasília - 00:30 BRT;
não 01:00 UTC, que ainda seria 22h do dia anterior em Brasília), mas só faz algo de
fato nos dias em que um período termina (gate_periodo abaixo, via
pipeline.config.period_start_for) - nos outros dias, todas as tasks de modelagem são
puladas (ShortCircuitOperator). Com TOPIC_MODEL_PERIOD_DAYS=7 isso equivale a "só
rodar às segundas"; com =1, todo dia é fronteira de período (roda todo dia, como no
esquema diário original). Quando roda de fato, para cada combinação candidato/rede
ajusta UM modelo BERTopic+KMeans a partir dos documentos do período que acabou de
fechar (ver weekly_topics.build_weekly_topics - o período é sempre em horário de
Brasília, mesma convenção de TIMEZONE em pave-webapp/api/app/queries/base.py - não
UTC), gera os rótulos via LLM, e por fim carrega tudo no Postgres
(load_topicos_diarios.py), como o `modelo` vigente daquele período.

Cada período é modelado do zero, direto da coleção do Mongo (sem depender de um
"processed_text" pré-calculado em outro lugar) - o pré-processamento correto
(pipeline.preprocessing.preprocess_dataframe, nominalização de verbos via Stanza) roda
dentro do próprio weekly_topics.py. Isso é o que substitui o antigo
`pipelines/nlp/topic_modeling.py` (varredura mensal de k5/k10/k15/k20 sobre o corpus
inteiro, com limpeza de texto própria e não relacionada) - esse script antigo fica
deprecado; topic_modeling_dag (o antigo dono desta task) não é mais usado.
"""
import os
from datetime import date, datetime, timedelta

import yaml

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

DAGS_DIR = os.path.dirname(__file__)
ENTITIES_PATH = os.path.join(DAGS_DIR, "..", "config", "entities.yaml")

# Mesmas env vars lidas por pave-tm/pipeline/config.py e infer_topic.py - ver comentário
# no docstring acima e no docker-compose.yml (serviço airflow).
PERIOD_DAYS = int(os.getenv("TOPIC_MODEL_PERIOD_DAYS", "7"))
PERIOD_EPOCH = date.fromisoformat(os.getenv("TOPIC_MODEL_PERIOD_EPOCH", "2026-07-27"))

# Mesmo mapeamento rede -> coleção do Mongo usado por weekly_topics.py/load_topicos_diarios.py
# em pave-tm (RESULT_TO_DATA_COLLECTION / COLLECTION_TO_FONTE): a modelagem de YouTube sempre
# usa os comentários, nunca os vídeos.
REDE_PARA_COLECAO_MONGO = {"reddit": "reddit", "meta": "meta", "youtube": "youtube_comments"}

try:
    with open(ENTITIES_PATH, "r", encoding="utf-8") as f:
        entities_config = yaml.safe_load(f)
except Exception as e:
    print(f"[dag_topic_modeling] Erro ao carregar entities.yaml: {e}")
    entities_config = {}


def obter_candidatos(rede_social: str) -> list:
    """Nomes de candidato como aparecem no Mongo (campo `candidate`) - mesmo valor
    usado por --candidato-mongo em dag_inferencia.py/infer_topic.py, e o mesmo que
    seed_entidades.py grava em `entidade.nome` (ver slugify lá: o código Postgres é
    derivado deste nome, nunca o contrário)."""
    config_rede = entities_config.get(rede_social, {})
    if rede_social == "reddit":
        return config_rede.get("termo_busca", [])
    if rede_social == "meta":
        return [p.get("nome") for p in config_rede.get("perfis", []) if p.get("nome")]
    if rede_social == "youtube":
        # Não é mais canal próprio do candidato - a modelagem é por candidato, não
        # por canal de notícia (um candidato tem documentos vindos de vários canais
        # de notícia diferentes, todos com o mesmo `candidate` no Mongo via
        # _entidade_busca - ver youtube_collector.py).
        return config_rede.get("termo_busca", [])
    return []


REDES_SOCIAIS = ["reddit", "meta", "youtube"]

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "start_date": datetime(2026, 8, 31),
}

# pave-tm é montado em /opt/airflow/pave-tm, mas roda com o PYTHON DESTE CONTAINER, não
# com o .venv próprio de pave-tm: `.venv/bin/python` de lá é um symlink para o path
# absoluto do host (/usr/bin/python3.12), que não existe dentro do container - as
# dependências de pave-tm (torch, bertopic, stanza...) precisam estar instaladas na
# imagem do Airflow (ver Dockerfile). "cd" pro diretório de pave-tm deixa `pipeline.*`
# importável (import relativo ao cwd).
PAVE_TM_DIR = "/opt/airflow/pave-tm"
LOAD_SCRIPT = os.path.join(os.path.dirname(__file__), "..", "pipelines", "etl", "load_topicos_diarios.py")

def _e_fronteira_de_periodo(ds: str, **_context) -> bool:
    """ShortCircuitOperator: True só nos dias em que um período (de PERIOD_DAYS dias,
    grade ancorada em PERIOD_EPOCH) acabou de fechar - mesma conta de
    pipeline.config.period_start_for em pave-tm. Com PERIOD_DAYS=7 e epoch numa
    segunda, isso é sempre segunda-feira (equivalente ao cron antigo "* * 1"); com
    PERIOD_DAYS=1, todo dia é fronteira."""
    hoje = date.fromisoformat(ds)
    offset = (hoje - PERIOD_EPOCH).days
    fronteira = offset % PERIOD_DAYS == 0
    if not fronteira:
        print(f"[dag_topic_modeling] {ds} não é início de período "
              f"(period_days={PERIOD_DAYS}, epoch={PERIOD_EPOCH}); pulando modelagem hoje.")
    return fronteira


with DAG(
    "topic_modeling_dag",
    # Todo dia, 03:30 UTC = 00:30 em Brasília - meia-noite local (00:00 BRT) é 03:00
    # UTC; rodar antes disso pegaria o dia ainda incompleto. A cadência de verdade
    # (rodar só 1x por período, não todo dia) é decidida pelo gate_periodo abaixo, não
    # pelo cron - isso é o que permite trocar PERIOD_DAYS sem editar este cron.
    schedule_interval="30 3 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    # Sequencial: cada task ajusta um BERTopic do zero (Stanza + encoder de embeddings)
    # na GPU - rodar várias ao mesmo tempo estoura memória.
    max_active_tasks=1,
    tags=["nlp", "modeling", "topicos", "diario"],
    is_paused_upon_creation=False,
) as dag:

    gate_periodo = ShortCircuitOperator(
        task_id="gate_periodo",
        python_callable=_e_fronteira_de_periodo,
    )

    # `ds` de uma run "03:30 UTC de um dia" (=00:30 BRT desse dia) já é esse dia - o
    # período LOCAL que se quer modelar (o que acabou de fechar à meia-noite local)
    # começou PERIOD_DAYS dias antes: ds-PERIOD_DAYS é o início desse período. Baked em
    # Python (não Jinja) porque PERIOD_DAYS já é conhecido no parse da DAG.
    # build_weekly_topics (pave-tm) interpreta week_start em America/Sao_Paulo, não
    # UTC - ver LOCAL_TZ em weekly_topics.py.
    SEMANA_A_MODELAR = "{{ macros.ds_add(ds, -%d) }}" % PERIOD_DAYS

    tasks_modelagem = []
    for rede_social in REDES_SOCIAIS:
        colecao_mongo = REDE_PARA_COLECAO_MONGO[rede_social]
        for candidato in obter_candidatos(rede_social):
            nome_task = candidato.replace(" ", "_").lower()
            comando = (
                f"cd {PAVE_TM_DIR} && python3 -m pipeline.weekly_topics "
                f"--collection {colecao_mongo} "
                f'--candidate "{candidato}" '
                f"--week-start {SEMANA_A_MODELAR} "
                f"--label-topics"
            )
            tasks_modelagem.append(
                BashOperator(
                    task_id=f"topic_modeling_{rede_social}_{nome_task}",
                    bash_command=comando,
                    trigger_rule=TriggerRule.ALL_DONE,
                )
            )

    modelagem_done = EmptyOperator(task_id="modelagem_done", trigger_rule=TriggerRule.ALL_DONE)

    carga_topicos = BashOperator(
        task_id="carga_topicos_postgres",
        bash_command=f"python3 {LOAD_SCRIPT}",
    )

    gate_periodo >> tasks_modelagem >> modelagem_done >> carga_topicos
