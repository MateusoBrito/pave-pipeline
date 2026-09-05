"""
==================================================================================
SEED DE FONTE / ENTIDADE / ALVO_COLETA - pipelines/etl/seed_entidades.py
==================================================================================
Lê `config/entities.yaml` e popula as tabelas de configuração do Postgres:

  - fonte         (youtube, reddit, meta)
  - entidade      (candidatos, deduplicados pelo nome entre YouTube e Meta)
  - alvo_coleta   (onde + o que buscar: canal [channel_id/page_id/subreddit]
                    + termo_busca [ex: "Lula", ou "" quando não se aplica])

Idempotente: usa ON CONFLICT DO NOTHING nas colunas/índices únicos, então
rodar de novo depois de editar o `entities.yaml` só insere o que for novo.

Pré-requisito: `alembic upgrade head` já rodado (tabelas precisam existir).

Execução manual:
    python3 pipelines/etl/seed_entidades.py
==================================================================================
"""
import re
import sys
import logging
import unicodedata
from pathlib import Path

import yaml
from sqlalchemy.dialects.postgresql import insert as pg_insert

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.database.postgres import get_session
from src.database.models import Fonte, Entidade, AlvoColeta, TipoTermoEnum

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_entidades")

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "entities.yaml"

# Decisão do time (28/08/2026): no Reddit só se busca por "Lula" e
# "Bolsonaro" (não existe uma busca separada por "Flavio Bolsonaro").
# Atualizado em 27/08/2026: o termo passou a ser "Flavio Bolsonaro" (mais
# específico). Mantemos "Bolsonaro" aqui também porque documentos antigos já
# renomeados no Mongo continuam usando esse valor -- é inofensivo manter.
REDDIT_TERMO_PARA_ENTIDADE = {
    "Lula": "lula",
    "Bolsonaro": "flavio_bolsonaro",
    "Flavio Bolsonaro": "flavio_bolsonaro",
}

FONTES = [
    ("youtube", "YouTube"),
    ("reddit", "Reddit"),
    ("meta", "Meta Ads"),
]


def slugify(nome: str) -> str:
    """'Flavio Bolsonaro' -> 'flavio_bolsonaro' (sem acento, minúsculo)."""
    sem_acento = unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-zA-Z0-9]+", "_", sem_acento).strip("_").lower()


def carregar_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def montar_entidades(cfg: dict) -> dict:
    """
    Une os candidatos cadastrados no YouTube e no Meta (a mesma pessoa pode
    estar nos dois) num único dicionário, casando pelo slug do nome.
    """
    entidades = {}

    for canal in cfg.get("youtube", {}).get("canais", []):
        slug = slugify(canal["nome"])
        entidades.setdefault(slug, {"nome": canal["nome"]})["youtube_channel_id"] = canal["channel_id"]

    for perfil in cfg.get("meta", {}).get("perfis", []):
        slug = slugify(perfil["nome"])
        entidades.setdefault(slug, {"nome": perfil["nome"]})["meta_page_id"] = perfil["page_id"]

    return entidades


def seed_fontes(session) -> set:
    stmt = pg_insert(Fonte).values(
        [{"codigo": codigo, "nome": nome, "ativa": True} for codigo, nome in FONTES]
    ).on_conflict_do_nothing(index_elements=["codigo"])
    session.execute(stmt)
    session.commit()

    codigos = {f.codigo for f in session.query(Fonte).all()}
    logger.info("Fontes no banco: %s", codigos)
    return codigos


def seed_entidades(session, entidades: dict) -> set:
    stmt = pg_insert(Entidade).values(
        [{"codigo": slug, "nome_exibicao": info["nome"], "ativa": True} for slug, info in entidades.items()]
    ).on_conflict_do_nothing(index_elements=["codigo"])
    session.execute(stmt)
    session.commit()

    codigos = {e.codigo for e in session.query(Entidade).all()}
    logger.info("%d entidades no banco.", len(codigos))
    return codigos


def seed_alvo_coleta(session, entidades: dict, codigos_entidade: set, reddit_subreddits: list) -> int:
    linhas = []

    # Processa YouTube e Meta
    for slug, info in entidades.items():
        if slug not in codigos_entidade:
            continue

        if "youtube_channel_id" in info:
            linhas.append({
                "entidade_codigo": slug,
                "fonte_codigo": "youtube",
                "canal": info["youtube_channel_id"],
                "termo_busca": "",
                "tipo": TipoTermoEnum.canal,
                "usar_na_busca": True,
                "ativo": True,
            })

        if "meta_page_id" in info:
            linhas.append({
                "entidade_codigo": slug,
                "fonte_codigo": "meta",
                "canal": info["meta_page_id"],
                "termo_busca": "",
                "tipo": TipoTermoEnum.handle,
                "usar_na_busca": True,
                "ativo": True,
            })

    # Processa Reddit
    vistos = set() # Evita duplicação se o yaml tiver "r/brasil" e "brasil"
    
    for subreddit in reddit_subreddits:
        # Remove o "r/" se existir, padronizando para "brasil", "brasilivre", etc.
        canal_limpo = subreddit.replace("r/", "").strip()
        
        for termo_busca, slug in REDDIT_TERMO_PARA_ENTIDADE.items():
            
            # --- AQUI ESTÁ O FILTRO ---
            # Pula a inserção se o termo for exatamente "Bolsonaro"
            if termo_busca == "Bolsonaro":
                continue

            if slug not in codigos_entidade:
                logger.warning(
                    "Termo Reddit '%s' aponta para a entidade '%s', que não está "
                    "cadastrada (via youtube/meta). Pulando.", termo_busca, slug,
                )
                continue
            
            chave_unica = (slug, canal_limpo, termo_busca)
            if chave_unica in vistos:
                continue
            vistos.add(chave_unica)

            linhas.append({
                "entidade_codigo": slug,
                "fonte_codigo": "reddit",
                "canal": canal_limpo,
                "termo_busca": termo_busca,  # <-- Inserindo o termo no banco!
                "tipo": TipoTermoEnum.consulta,
                "usar_na_busca": True,
                "ativo": True,
            })

    if not linhas:
        return 0

    stmt = pg_insert(AlvoColeta).values(linhas).on_conflict_do_nothing(
        index_elements=["entidade_codigo", "fonte_codigo", "canal", "termo_busca"]
    )
    session.execute(stmt)
    session.commit()
    return len(linhas)


def run():
    cfg = carregar_config()
    entidades = montar_entidades(cfg)
    reddit_subreddits = cfg.get("reddit", {}).get("subreddits", [])

    session = get_session()
    try:
        seed_fontes(session)
        codigos_entidade = seed_entidades(session, entidades)
        n = seed_alvo_coleta(session, entidades, codigos_entidade, reddit_subreddits)
        logger.info(
            "Seed concluído: %d entidades, %d alvos de coleta processados.",
            len(codigos_entidade), n,
        )
    finally:
        session.close()


if __name__ == "__main__":
    run()