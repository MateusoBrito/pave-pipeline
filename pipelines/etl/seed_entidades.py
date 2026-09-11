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
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.database.postgres import get_session
from src.database.models import Fonte, Entidade, AlvoColeta, TipoTermoEnum

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_entidades")

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "entities.yaml"

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

    # YouTube não tem mais canal próprio por candidato (ver canais_noticia + termo_busca
    # em entities.yaml) - a lista de candidatos vem só do Meta agora (tem todos os 13).
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
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["codigo"],
        set_={"nome_exibicao": stmt.excluded.nome_exibicao, "ativa": True},
    )
    session.execute(stmt)
    session.commit()

    codigos = {e.codigo for e in session.query(Entidade).all()}
    logger.info("%d entidades no banco.", len(codigos))
    return codigos


def reconciliar_entidades_existentes(session, entidades: dict) -> None:
    """Move alvos legados para o codigo atual, usando o identificador da fonte.

    O nome/slug e mutavel; channel_id e page_id sao as identidades estaveis.
    Quando a troca de nome ja criou dois alvos para o mesmo identificador,
    preservamos apenas o alvo canonico e removemos documentos duplicados.
    """
    for slug, info in entidades.items():
        for fonte_codigo, canal in (
            ("meta", info.get("meta_page_id")),
        ):
            if not canal:
                continue

            alvos = session.execute(
                text(
                    """
                    SELECT id, entidade_codigo
                    FROM alvo_coleta
                    WHERE fonte_codigo = :fonte_codigo AND canal = :canal
                    ORDER BY id
                    """
                ),
                {"fonte_codigo": fonte_codigo, "canal": canal},
            ).mappings().all()

            canonico = next((alvo for alvo in alvos if alvo["entidade_codigo"] == slug), None)
            if canonico is None:
                legado = next((alvo for alvo in alvos if alvo["entidade_codigo"] != slug), None)
                if legado:
                    session.execute(
                        text("UPDATE alvo_coleta SET entidade_codigo = :slug WHERE id = :alvo_id"),
                        {"slug": slug, "alvo_id": legado["id"]},
                    )
                    canonico = {"id": legado["id"], "entidade_codigo": slug}

            if canonico is None:
                continue

            for legado in alvos:
                if legado["id"] == canonico["id"]:
                    continue
                session.execute(
                    text(
                        """
                        DELETE FROM documento legado
                        WHERE legado.alvo_coleta_id = :legado_id
                          AND EXISTS (
                              SELECT 1 FROM documento canonico
                              WHERE canonico.alvo_coleta_id = :canonico_id
                                AND canonico.id_nativo = legado.id_nativo
                          )
                        """
                    ),
                    {"legado_id": legado["id"], "canonico_id": canonico["id"]},
                )
                session.execute(
                    text(
                        "UPDATE documento SET alvo_coleta_id = :canonico_id "
                        "WHERE alvo_coleta_id = :legado_id"
                    ),
                    {"canonico_id": canonico["id"], "legado_id": legado["id"]},
                )
                session.execute(
                    text("DELETE FROM alvo_coleta WHERE id = :alvo_id"),
                    {"alvo_id": legado["id"]},
                )

    session.execute(
        text(
            "UPDATE entidade SET ativa = FALSE "
            "WHERE codigo NOT IN :codigos AND NOT EXISTS "
            "(SELECT 1 FROM alvo_coleta WHERE alvo_coleta.entidade_codigo = entidade.codigo)"
        ).bindparams(codigos=tuple(entidades)),
    )
    session.commit()


def seed_alvo_coleta(
    session,
    entidades: dict,
    codigos_entidade: set,
    reddit_subreddits: list,
    reddit_termos: list,
    youtube_canais: list,
    youtube_termos: list,
) -> int:
    linhas = []
    slug_por_termo = {slugify(info["nome"]): slug for slug, info in entidades.items()}

    # Processa Meta
    for slug, info in entidades.items():
        if slug not in codigos_entidade:
            continue

        if "meta_page_id" in info:
            linhas.append({
                "entidade_codigo": slug,
                "fonte_codigo": "meta",
                "canal": info["meta_page_id"],
                "rotulo": None,
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

        for termo_busca in reddit_termos:
            slug = slug_por_termo.get(slugify(termo_busca))

            if not slug or slug not in codigos_entidade:
                logger.warning(
                    "Termo Reddit '%s' não corresponde a uma entidade cadastrada "
                    "via YouTube/Meta. Pulando.", termo_busca,
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
                "rotulo": None,
                "termo_busca": termo_busca,  # <-- Inserindo o termo no banco!
                "tipo": TipoTermoEnum.consulta,
                "usar_na_busca": True,
                "ativo": True,
            })

    # Processa YouTube - canal de notícia x candidato, mesma lógica do Reddit (não é
    # mais canal próprio do candidato, ver montar_entidades/canais_noticia).
    for canal in youtube_canais:
        canal_id = canal["channel_id"]

        for termo_busca in youtube_termos:
            slug = slug_por_termo.get(slugify(termo_busca))

            if not slug or slug not in codigos_entidade:
                logger.warning(
                    "Termo YouTube '%s' não corresponde a uma entidade cadastrada "
                    "via Meta. Pulando.", termo_busca,
                )
                continue

            chave_unica = (slug, "youtube", canal_id, termo_busca)
            if chave_unica in vistos:
                continue
            vistos.add(chave_unica)

            linhas.append({
                "entidade_codigo": slug,
                "fonte_codigo": "youtube",
                "canal": canal_id,
                # Nome legível do canal (ex: "Jovem Pan News") - `canal` em si é o
                # channel_id, ilegível na UI (ver canal_label() no webapp).
                "rotulo": canal.get("nome"),
                "termo_busca": termo_busca,
                "tipo": TipoTermoEnum.consulta,
                "usar_na_busca": True,
                "ativo": True,
            })

    if not linhas:
        return 0

    stmt = pg_insert(AlvoColeta).values(linhas)
    stmt = stmt.on_conflict_do_update(
        index_elements=["entidade_codigo", "fonte_codigo", "canal", "termo_busca"],
        # rotulo entra no update pra alvo_coleta já existente também ganhar o nome
        # legível numa reexecução (ele não é passado pra reddit/meta - stmt.excluded
        # nesses casos vale None, que sobrescreveria um rotulo já setado à toa; como
        # só o YouTube popula essa coluna, não há risco real aqui).
        set_={"ativo": True, "usar_na_busca": True, "rotulo": stmt.excluded.rotulo},
    )
    session.execute(stmt)
    session.commit()
    return len(linhas)


def run():
    cfg = carregar_config()
    entidades = montar_entidades(cfg)
    reddit_subreddits = cfg.get("reddit", {}).get("subreddits", [])
    reddit_termos = cfg.get("reddit", {}).get("termo_busca", [])
    youtube_canais = cfg.get("youtube", {}).get("canais_noticia", [])
    youtube_termos = cfg.get("youtube", {}).get("termo_busca", [])

    session = get_session()
    try:
        seed_fontes(session)
        codigos_entidade = seed_entidades(session, entidades)
        reconciliar_entidades_existentes(session, entidades)
        n = seed_alvo_coleta(
            session, entidades, codigos_entidade, reddit_subreddits, reddit_termos,
            youtube_canais, youtube_termos,
        )
        logger.info(
            "Seed concluído: %d entidades, %d alvos de coleta processados.",
            len(codigos_entidade), n,
        )
    finally:
        session.close()


if __name__ == "__main__":
    run()