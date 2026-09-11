"""
================================================================================
CARGA DE MODELAGEM DIARIA DE TOPICOS: pave-tm/resultado_semana -> PostgreSQL
================================================================================
Variante de load_topicos.py para a modelagem diaria (pipeline/weekly_topics.py em
pave-tm): um `modelo` por dia (`resultado_semana/{fonte}/{candidato}/{data}/k{n}/
bertopic_kmeans/{embedding}/`), em vez de um por combinacao k/embedding.

Diferencas para load_topicos.py:
  - `modelo.janela_inicio`/`janela_fim` gravam a data do dia (ambas iguais - o modelo
    so descreve aquele dia). load_topicos.py nunca grava essas colunas.
  - Todo modelo diario entra com status=vigente - nao ha "modelo campeao" por
    candidato para escolher manualmente, cada dia e o seu proprio vigente (ver
    vigente_model_ids em pave-webapp/api/app/queries/base.py: com `day` informado,
    filtra pela janela; sem `day`, um modelo com janela sempre teria que ser
    escolhido por outro criterio - por isso ela so deveria ser chamada sem `day`
    para modelos SEM janela, como o de sentimento).
  - So existe uma combinacao k/embedding por dia (a escolhida por
    pipeline.config.choose_k em pave-tm), nao uma varredura - "k" ainda entra em
    `versao` para manter o formato, mas so ha um valor por dia.

Idempotente, mesma lógica de upsert de load_topicos.py (reexecutar so insere o que
faltar). NAO apaga nada - dias ja carregados continuam vigentes.

Uso:
    python3 pipelines/etl/load_topicos_diarios.py
    python3 pipelines/etl/load_topicos_diarios.py --resultado-dir /home/labpi/pave-tm/resultado_semana
    python3 pipelines/etl/load_topicos_diarios.py --dry-run
"""
import argparse
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert as pg_insert

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.database.postgres import get_engine, get_session
from src.database.models import Modelo, StatusModeloEnum, TipoModeloEnum
from sqlalchemy import MetaData, Table

from load_topicos import (
    COLLECTION_TO_FONTE,
    EMBEDDING_SLUG_TO_KEY,
    carregar_documento_topico,
    mapear_documentos,
    parse_resumo_csv,
    parse_result_topic_txt,
    parse_rotulos_json,
    upsert_topicos,
)

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("load_topicos_diarios")

import os

# pave-tm é montado em /opt/airflow/pave-tm dentro do container do Airflow (onde esta
# task roda de verdade, via dag_topic_modeling.py) - o path antigo, hardcoded pro host
# (/home/labpi/pave-tm/...), não existe lá dentro e fazia este script carregar
# silenciosamente 0 modelos sempre que rodado via `docker exec` (nenhum erro, só
# "Concluido. modelos=0" - descoberto em 2026-09-09 quando infer_topic.py acabou
# encontrando modelos diários antigos por coincidência de janela_inicio em vez dos
# semanais recém-gerados). Mesma env var/fallback que infer_topic.py já usa.
PAVE_TM_DIR = os.environ.get("PAVE_TM_DIR", "/opt/airflow/pave-tm")
RESULTADO_DIR_PADRAO = f"{PAVE_TM_DIR}/resultado_semana"


SEMANA_PREFIXO = "semana_"


def find_dias(resultado_dir: Path, collections: list) -> list:
    """[(collection, candidate_slug, janela_inicio, janela_fim, candidate_day_dir), ...]
    existentes em disco. Dois formatos de diretório (ver build_daily_topics/
    build_weekly_topics em pave-tm/pipeline/weekly_topics.py):
      - "YYYY-MM-DD"          -> um dia só (janela_inicio == janela_fim)
      - "semana_YYYY-MM-DD"   -> uma semana inteira, começando nessa segunda-feira
                                  (janela_fim = janela_inicio + 6 dias)
    """
    dias = []
    for collection in collections:
        collection_dir = resultado_dir / COLLECTION_TO_FONTE[collection]
        if not collection_dir.is_dir():
            continue
        for candidate_dir in sorted(collection_dir.iterdir()):
            if not candidate_dir.is_dir():
                continue
            for day_dir in sorted(candidate_dir.iterdir()):
                if not day_dir.is_dir():
                    continue
                nome = day_dir.name
                try:
                    if nome.startswith(SEMANA_PREFIXO):
                        janela_inicio = date.fromisoformat(nome[len(SEMANA_PREFIXO):])
                        janela_fim = janela_inicio + timedelta(days=6)
                    else:
                        janela_inicio = date.fromisoformat(nome)
                        janela_fim = janela_inicio
                except ValueError:
                    logger.warning("Nome de dia/semana inesperado (ignorado): %s", day_dir)
                    continue
                dias.append((collection, candidate_dir.name, janela_inicio, janela_fim, day_dir))
    return dias


def find_combinacao_do_dia(day_dir: Path):
    """Um dia so tem UMA combinacao k/embedding (a escolhida por choose_k), diferente
    de find_combinacoes em load_topicos.py que varre varias. Retorna None se
    incompleta."""
    k_dirs = sorted(day_dir.glob("k*"))
    if not k_dirs:
        return None
    k_dir = k_dirs[0]
    bt_dir = k_dir / "bertopic_kmeans"
    if not bt_dir.is_dir():
        return None
    emb_dirs = sorted(bt_dir.iterdir())
    if not emb_dirs:
        return None
    emb_dir = emb_dirs[0]
    result_txt = next(emb_dir.glob("result_topic_*.txt"), None)
    resumo_csv = emb_dir / "Resumo_Topicos_Dominantes.csv"
    rotulos_json = emb_dir / "rotulos_topicos.json"
    if result_txt and result_txt.exists() and resumo_csv.exists():
        return int(k_dir.name.lstrip("k")), emb_dir.name, emb_dir, result_txt, resumo_csv, rotulos_json
    logger.warning("Combinacao incompleta (ignorada): %s", emb_dir)
    return None


def upsert_modelo_diario(session, fonte_codigo, entidade_codigo, embedding_key, embedding_nome_hf,
                          k, janela_inicio: date, janela_fim: date, treinado_em, parametros, metricas) -> int:
    # nome inclui a data de início - sem isso, duas janelas do mesmo candidato/embedding
    # colidiriam no unique index (tipo, nome, versao) e uma sobrescreveria a outra.
    nome = f"{fonte_codigo}_{entidade_codigo}__{embedding_key}__{janela_inicio.isoformat()}"
    versao = f"k{k}"

    stmt = (
        pg_insert(Modelo)
        .values(
            tipo=TipoModeloEnum.topico,
            fonte_codigo=fonte_codigo,
            nome=nome,
            versao=versao,
            janela_inicio=janela_inicio,
            janela_fim=janela_fim,
            treinado_em=treinado_em,
            parametros=parametros,
            metricas=metricas,
            status=StatusModeloEnum.vigente,
        )
        .on_conflict_do_update(
            index_elements=[Modelo.tipo, Modelo.nome, Modelo.versao],
            set_=dict(
                fonte_codigo=fonte_codigo,
                janela_inicio=janela_inicio,
                janela_fim=janela_fim,
                treinado_em=treinado_em,
                parametros=parametros,
                metricas=metricas,
                status=StatusModeloEnum.vigente,
            ),
        )
        .returning(Modelo.id)
    )
    return session.execute(stmt).scalar_one()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--resultado-dir", type=str, default=RESULTADO_DIR_PADRAO)
    parser.add_argument("--collections", nargs="+", default=list(COLLECTION_TO_FONTE), choices=list(COLLECTION_TO_FONTE))
    parser.add_argument("--dry-run", action="store_true", help="So mostra o que seria carregado, sem escrever no banco.")
    args = parser.parse_args()

    resultado_dir = Path(args.resultado_dir)
    engine = get_engine()
    metadata = MetaData()
    documento_topico_table = Table("documento_topico", metadata, autoload_with=engine)

    session = get_session()
    total_modelos = total_topicos = total_doc_topico = 0
    doc_map_cache = {}
    try:
        for collection, candidate_slug, janela_inicio, janela_fim, day_dir in find_dias(resultado_dir, args.collections):
            fonte_codigo = COLLECTION_TO_FONTE[collection]
            combo = find_combinacao_do_dia(day_dir)
            if combo is None:
                continue
            k, embedding_slug, emb_dir, result_txt, resumo_csv, rotulos_json = combo
            if embedding_slug not in EMBEDDING_SLUG_TO_KEY:
                logger.warning("Embedding desconhecido (ignorado): %s", embedding_slug)
                continue
            embedding_key, embedding_nome_hf = EMBEDDING_SLUG_TO_KEY[embedding_slug]

            cache_key = (fonte_codigo, candidate_slug)
            if cache_key not in doc_map_cache:
                doc_map_cache[cache_key] = mapear_documentos(session, fonte_codigo, candidate_slug)
            id_nativo_para_doc_id = doc_map_cache[cache_key]
            if not id_nativo_para_doc_id:
                logger.warning("Nenhum documento em %s/%s no Postgres -- pulando.", fonte_codigo, candidate_slug)
                continue

            topics_palavras = parse_result_topic_txt(result_txt)
            doc_assignments = parse_resumo_csv(resumo_csv)
            rotulos = parse_rotulos_json(rotulos_json)
            tamanhos = {}
            for _, numero in doc_assignments:
                tamanhos[numero] = tamanhos.get(numero, 0) + 1

            treinado_em = datetime.fromtimestamp(result_txt.stat().st_mtime, tz=timezone.utc)
            parametros = {
                "entidade_codigo": candidate_slug,
                "collection_origem": collection,
                "embedding_model": embedding_nome_hf,
                "k": k,
                "algoritmo_clustering": "kmeans",
                "origem": "pave-tm/resultado_semana",
                "janela_inicio": janela_inicio.isoformat(),
                "janela_fim": janela_fim.isoformat(),
            }
            metricas = {
                "n_documentos_atribuidos": len(doc_assignments),
                "n_topicos": len(topics_palavras),
            }

            logger.info("%s/%s %s a %s: k=%d embedding=%s (%d docs, %d topicos, %d rotulos)",
                        fonte_codigo, candidate_slug, janela_inicio, janela_fim, k, embedding_key,
                        len(doc_assignments), len(topics_palavras), len(rotulos))

            if args.dry_run:
                continue

            modelo_id = upsert_modelo_diario(session, fonte_codigo, candidate_slug, embedding_key,
                                              embedding_nome_hf, k, janela_inicio, janela_fim,
                                              treinado_em, parametros, metricas)
            numero_para_topico_id = upsert_topicos(session, modelo_id, topics_palavras, tamanhos, rotulos)
            n_dt = carregar_documento_topico(session, documento_topico_table, doc_assignments,
                                              id_nativo_para_doc_id, numero_para_topico_id)
            session.commit()

            total_modelos += 1
            total_topicos += len(numero_para_topico_id)
            total_doc_topico += n_dt

        if args.dry_run:
            logger.info("Dry-run concluido -- nada foi escrito no banco.")
        else:
            logger.info("Concluido. modelos=%d topicos=%d documento_topico=%d",
                        total_modelos, total_topicos, total_doc_topico)
    finally:
        session.close()


if __name__ == "__main__":
    main()
