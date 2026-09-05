"""
==================================================================================
CARGA DE DOCUMENTOS BRUTOS: MongoDB -> PostgreSQL - pipelines/etl/load_documentos.py
==================================================================================
Lê os documentos brutos já coletados no MongoDB e preenche `documento` no
Postgres. Cada documento aponta direto para um `alvo_coleta_id` (que já
carrega junto: fonte + candidato + canal + termo de busca) -- não existe mais
uma tabela de "links" separada.

DE PROPÓSITO, esta primeira versão NÃO mexe em:
  - engajamento / autor_hash / suspeito -- não existem mais no schema atual
    (foram comentados no models.py; fica pra uma fase seguinte)
  - metadados (JSONB) -- fica NULL por enquanto
  - qualquer coisa de tópico/sentimento (fases posteriores, dependem do NLP)

Idempotente: usa ON CONFLICT DO NOTHING no índice único de `documento`
(alvo_coleta_id, id_nativo) -- rodar de novo só insere o que for novo.

Pré-requisito: rodar `python3 pipelines/etl/seed_entidades.py` antes (esse
script assume que fonte/entidade/alvo_coleta já existem).

Execução manual:
    python3 pipelines/etl/load_documentos.py
    python3 pipelines/etl/load_documentos.py --fontes youtube --limit 20   # teste rápido
==================================================================================
"""
import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

from dotenv import load_dotenv
from pymongo import MongoClient
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.database.postgres import get_session
from src.database.models import Fonte, AlvoColeta, Documento, TipoDocumentoEnum

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("load_documentos")


# ----------------------------------------------------------------------------
# MONGODB
# ----------------------------------------------------------------------------
def get_mongo_db():
    mongo_user = os.getenv("MONGO_INITDB_ROOT_USERNAME")
    mongo_password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
    mongo_host = os.getenv("MONGO_HOST", "localhost")
    mongo_port = os.getenv("MONGO_PORT", "27017")
    mongo_database = os.getenv("MONGO_DATABASE", "panorama")

    if mongo_user and mongo_password:
        uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/"
    else:
        uri = os.getenv("MONGO_URI", f"mongodb://{mongo_host}:{mongo_port}/")

    cliente = MongoClient(uri, serverSelectionTimeoutMS=5000)
    return cliente[mongo_database]


# ----------------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------------
def _coletado_em(mongo_id) -> datetime:
    """Usa o timestamp embutido no próprio ObjectId do Mongo como coletado_em."""
    return mongo_id.generation_time


def _parse_iso(valor) -> Optional[datetime]:
    if not valor:
        return None
    if isinstance(valor, datetime):
        return valor if valor.tzinfo else valor.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(valor).replace("Z", "+00:00"))
    except ValueError:
        logger.warning("Não consegui converter data '%s'.", valor)
        return None
    # Alguns campos (ex: ad_delivery_start_time do Meta) vêm só com a data
    # ("2026-08-15"), sem timezone -- assumimos UTC pra não gravar um
    # datetime "naive" numa coluna timestamptz.
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _parse_epoch(valor) -> Optional[datetime]:
    if valor is None:
        return None
    try:
        return datetime.fromtimestamp(int(valor), tz=timezone.utc)
    except (ValueError, TypeError):
        logger.warning("Não consegui converter epoch '%s'.", valor)
        return None


def _texto(*partes) -> Optional[str]:
    texto = " ".join(str(p).strip() for p in partes if p and str(p).strip())
    return texto or None


def _remover_nul(valor):
    """Postgres/psycopg2 não aceita o caractere NUL (0x00) em texto -- às
    vezes aparece em dados raspados (encoding malformado, emoji quebrado
    etc). Removemos em vez de deixar o insert inteiro falhar. Recursivo
    porque `metadados` pode ter texto aninhado (ex: tags, page_name)."""
    if isinstance(valor, str):
        return valor.replace("\x00", "") if "\x00" in valor else valor
    if isinstance(valor, dict):
        return {chave: _remover_nul(v) for chave, v in valor.items()}
    if isinstance(valor, list):
        return [_remover_nul(v) for v in valor]
    return valor


def _sanitizar_linha(linha: dict) -> dict:
    return {chave: _remover_nul(valor) for chave, valor in linha.items()}


def carregar_mapa_alvo_coleta(session, fonte_codigo: str) -> dict:
    """(canal, termo_busca) -> alvo_coleta_id, para uma fonte."""
    linhas = session.execute(
        select(AlvoColeta.canal, AlvoColeta.termo_busca, AlvoColeta.id)
        .where(AlvoColeta.fonte_codigo == fonte_codigo)
    ).all()
    return {(canal, termo_busca): alvo_id for canal, termo_busca, alvo_id in linhas}


def upsert_documentos(session, linhas: list) -> int:
    """Insere (ou atualiza só o `metadados`, se já existir). Retorna quantas
    linhas únicas foram enviadas.

    Usamos ON CONFLICT DO UPDATE (em vez de DO NOTHING) especificamente para
    a coluna `metadados`: assim, rodar este script de novo não só insere
    documentos novos, como também retroalimenta o `metadados` nos ~460 mil
    documentos que já existiam antes desse campo começar a ser preenchido --
    sem tocar em nenhuma outra coluna (texto, url, publicado_em etc. ficam
    exatamente como estavam)."""
    if not linhas:
        return 0

    linhas_limpas = [_sanitizar_linha(l) for l in linhas]

    dedup = {}
    for linha in linhas_limpas:
        chave = (linha["alvo_coleta_id"], linha["id_nativo"])
        dedup.setdefault(chave, linha)

    stmt = pg_insert(Documento).values(list(dedup.values()))
    stmt = stmt.on_conflict_do_update(
        index_elements=["alvo_coleta_id", "id_nativo"],
        set_={"metadados": stmt.excluded.metadados},
    )
    session.execute(stmt)
    session.commit()
    return len(dedup)


# ----------------------------------------------------------------------------
# YOUTUBE
# ----------------------------------------------------------------------------
def carregar_youtube(session, mongo_db, mapa_alvo: dict, limit: Optional[int]):
    # Comentário não guarda o channel_id direto -- só dá pra achar via o
    # vídeo dele (videoId -> channelId). Carregamos esse mapa uma vez.
    mapa_video_channel = {
        doc["videoId"]: doc.get("channelId")
        for doc in mongo_db["youtube_videos"].find({}, {"videoId": 1, "channelId": 1})
    }

    # --- vídeos ---
    linhas = []
    cursor = mongo_db["youtube_videos"].find({})
    if limit:
        cursor = cursor.limit(limit)

    for doc in cursor:
        video_id = doc.get("videoId")
        publicado_em = _parse_iso(doc.get("publishedAt"))
        if not video_id or not publicado_em:
            continue

        channel_id = doc.get("channelId")
        alvo_id = mapa_alvo.get((channel_id, ""))
        if not alvo_id:
            logger.warning("Vídeo %s: channel_id '%s' sem alvo_coleta cadastrado.", video_id, channel_id)
            continue

        linhas.append({
            "alvo_coleta_id": alvo_id,
            "id_nativo": video_id,
            "id_mongo": str(doc["_id"]),
            "tipo": TipoDocumentoEnum.video,
            "texto": _texto(doc.get("title"), doc.get("description")),
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "publicado_em": publicado_em,
            "coletado_em": _coletado_em(doc["_id"]),
            "metadados": {
                "viewCount": doc.get("viewCount"),
                "likeCount": doc.get("likeCount"),
                "commentCount": doc.get("commentCount"),
                "channelTitle": doc.get("channelTitle"),
                "tags": doc.get("tags"),
                "categoryId": doc.get("categoryId"),
            },
        })

    n = upsert_documentos(session, linhas)
    logger.info("[youtube_videos] %d linhas processadas, %d únicas inseridas/confirmadas.", len(linhas), n)

    # --- comentários ---
    linhas = []
    cursor = mongo_db["youtube_comments"].find({})
    if limit:
        cursor = cursor.limit(limit)

    for doc in cursor:
        comment_id = doc.get("commentId")
        publicado_em = _parse_iso(doc.get("publishedAt"))
        if not comment_id or not publicado_em:
            continue

        channel_id = mapa_video_channel.get(doc.get("videoId"))
        alvo_id = mapa_alvo.get((channel_id, ""))
        if not alvo_id:
            logger.warning(
                "Comentário %s: não achei channel_id do vídeo %s (ou vídeo sem alvo_coleta).",
                comment_id, doc.get("videoId"),
            )
            continue

        linhas.append({
            "alvo_coleta_id": alvo_id,
            "id_nativo": comment_id,
            "id_mongo": str(doc["_id"]),
            "tipo": TipoDocumentoEnum.comentario,
            "texto": _texto(doc.get("text")),
            "url": None,
            "publicado_em": publicado_em,
            "coletado_em": _coletado_em(doc["_id"]),
            "metadados": {
                "likeCount": doc.get("likeCount"),
                "replyCount": doc.get("replyCount"),
            },
        })

    n = upsert_documentos(session, linhas)
    logger.info("[youtube_comments] %d linhas processadas, %d únicas inseridas/confirmadas.", len(linhas), n)


# ----------------------------------------------------------------------------
# REDDIT
# ----------------------------------------------------------------------------
def carregar_reddit(session, mongo_db, mapa_alvo: dict, limit: Optional[int]):
    linhas = []
    cursor = mongo_db["reddit"].find({})
    if limit:
        cursor = cursor.limit(limit)

    for doc in cursor:
        id_nativo = doc.get("id")
        publicado_em = _parse_epoch(doc.get("created_utc"))
        if not id_nativo or not publicado_em:
            continue

        subreddit = doc.get("_subreddit_busca")
        termo_busca = doc.get("_termo_busca")
        alvo_id = mapa_alvo.get((subreddit, termo_busca))
        if not alvo_id:
            logger.warning(
                "Documento reddit %s: (subreddit=%s, termo=%s) sem alvo_coleta cadastrado.",
                id_nativo, subreddit, termo_busca,
            )
            continue

        eh_post = doc.get("_tipo_documento") == "post"
        texto = _texto(doc.get("title"), doc.get("selftext"), doc.get("body"))
        permalink = doc.get("permalink")

        linhas.append({
            "alvo_coleta_id": alvo_id,
            "id_nativo": id_nativo,
            "id_mongo": str(doc["_id"]),
            "tipo": TipoDocumentoEnum.post if eh_post else TipoDocumentoEnum.comentario,
            "texto": texto,
            "url": f"https://reddit.com{permalink}" if permalink else None,
            "publicado_em": publicado_em,
            "coletado_em": _coletado_em(doc["_id"]),
            "metadados": {
                "score": doc.get("score"),
                "num_comments": doc.get("num_comments"),          # só post
                "upvote_ratio": doc.get("upvote_ratio"),           # só post
                "total_awards_received": doc.get("total_awards_received"),
                "controversiality": doc.get("controversiality"),   # só comentário
                "stickied": doc.get("stickied"),
                "distinguished": doc.get("distinguished"),
                "over_18": doc.get("over_18"),                     # só post
                "link_flair_text": doc.get("link_flair_text"),     # só post
            },
        })

    n = upsert_documentos(session, linhas)
    logger.info("[reddit] %d linhas processadas, %d únicas inseridas/confirmadas.", len(linhas), n)


# ----------------------------------------------------------------------------
# META
# ----------------------------------------------------------------------------
def carregar_meta(session, mongo_db, mapa_alvo: dict, limit: Optional[int]):
    linhas = []
    cursor = mongo_db["meta"].find({})
    if limit:
        cursor = cursor.limit(limit)

    for doc in cursor:
        id_nativo = doc.get("id")
        publicado_em = _parse_iso(doc.get("ad_delivery_start_time"))
        if not id_nativo or not publicado_em:
            continue

        page_id = doc.get("page_id") or doc.get("_entidade_busca")
        alvo_id = mapa_alvo.get((page_id, ""))
        if not alvo_id:
            logger.warning("Anúncio %s: page_id '%s' sem alvo_coleta cadastrado.", id_nativo, page_id)
            continue

        corpos = doc.get("ad_creative_bodies") or []
        texto = _texto(*corpos) if isinstance(corpos, list) else _texto(corpos)

        linhas.append({
            "alvo_coleta_id": alvo_id,
            "id_nativo": id_nativo,
            "id_mongo": str(doc["_id"]),
            "tipo": TipoDocumentoEnum.anuncio,
            "texto": texto,
            "url": None,  # ad_snapshot_url não está nos FIELDS coletados hoje
            "publicado_em": publicado_em,
            "coletado_em": _coletado_em(doc["_id"]),
            "metadados": {
                "spend": doc.get("spend"),
                "impressions": doc.get("impressions"),
                "page_name": doc.get("page_name"),
                "ad_delivery_stop_time": doc.get("ad_delivery_stop_time"),
                "ad_creative_link_titles": doc.get("ad_creative_link_titles"),
            },
        })

    n = upsert_documentos(session, linhas)
    logger.info("[meta] %d linhas processadas, %d únicas inseridas/confirmadas.", len(linhas), n)


# ----------------------------------------------------------------------------
# CLI / ORQUESTRAÇÃO
# ----------------------------------------------------------------------------
CARREGADORES = {
    "youtube": carregar_youtube,
    "reddit": carregar_reddit,
    "meta": carregar_meta,
}


def run(fontes=None, limit=None):
    fontes = fontes or list(CARREGADORES.keys())
    mongo_db = get_mongo_db()
    session = get_session()

    try:
        fontes_no_banco = {f.codigo for f in session.query(Fonte).all()}
        if not fontes_no_banco:
            raise SystemExit(
                "Tabela 'fonte' está vazia. Rode primeiro: "
                "python3 pipelines/etl/seed_entidades.py"
            )

        for fonte_codigo in fontes:
            if fonte_codigo not in fontes_no_banco:
                logger.warning("Fonte '%s' não está cadastrada no Postgres. Pulando.", fonte_codigo)
                continue
            logger.info("=== Carregando fonte: %s ===", fonte_codigo)
            mapa_alvo = carregar_mapa_alvo_coleta(session, fonte_codigo)
            if not mapa_alvo:
                logger.warning(
                    "Nenhum alvo_coleta cadastrado para '%s'. Rode seed_entidades.py. Pulando.",
                    fonte_codigo,
                )
                continue
            CARREGADORES[fonte_codigo](session, mongo_db, mapa_alvo, limit)

    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description="Carrega documentos brutos do MongoDB para o Postgres")
    parser.add_argument(
        "--fontes", nargs="+", choices=list(CARREGADORES.keys()), default=None,
        help="Quais fontes carregar (default: todas)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Limita quantos documentos ler por coleção (útil para testar antes de rodar tudo)",
    )
    args = parser.parse_args()
    run(fontes=args.fontes, limit=args.limit)


if __name__ == "__main__":
    main()