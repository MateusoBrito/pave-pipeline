"""
================================================================================
CARGA DE MODELAGEM DE TOPICOS: pave-tm/resultado_final -> PostgreSQL
================================================================================
Le as saidas do BERTopic+KMeans geradas no projeto pave-tm (todas as
combinacoes embedding_model x k, por candidato/fonte) e povoa, no Postgres:

  - modelo            (uma linha por combinacao fonte/candidato/k/embedding_model)
  - topico            (um por numero de topico dentro de cada modelo)
  - documento_topico  (topico dominante de cada documento, nessa combinacao)

O schema atual (src/database/models.py) nao tem colunas dedicadas para
candidato/embedding_model/k em `modelo`; ficam em `modelo.parametros` (JSONB)
e embutidas em `nome`/`versao` para manter unicidade (unique index em
tipo+nome+versao). Todo modelo entra com status=arquivado -- promova
manualmente a 'vigente' a configuracao escolhida de cada candidato (nao ha
metrica automatica de qualidade calculada ainda, ex: coerencia/silhouette).

`documento_topico` e escrito via Core (Table refletida), nao via o modelo
ORM `DocumentoTopico` -- esse modelo esta com um bug de indentacao em
src/database/models.py:231 (topico_id cai fora do corpo da classe) e por
isso nao tem essa coluna mapeada.

Idempotente: reexecutar so insere o que faltar (upsert por chave natural em
modelo/topico, ON CONFLICT DO NOTHING em documento_topico).

Uso:
    python3 pipelines/etl/load_topicos.py
    python3 pipelines/etl/load_topicos.py --resultado-dir /home/labpi/pave-tm/resultado_final --collections reddit meta
    python3 pipelines/etl/load_topicos.py --dry-run
"""
import argparse
import csv
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, case, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.database.postgres import get_engine, get_session
from src.database.models import AlvoColeta, Documento, Modelo, StatusModeloEnum, Topico, TipoModeloEnum

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("load_topicos")

# ==============================================================================
# CONFIGURAÇÃO DE MODELO VIGENTE (CAMPEÃO)
# Altere esses valores para definir qual modelo entrará automaticamente 
# como 'vigente'. O resto continuará entrando como 'arquivado'.
# ==============================================================================
K_VIGENTE = 15
EMBEDDING_VIGENTE = "qwen3_0_6b"  # Opções: "qwen3_0_6b", "minilm_l12", "mpnet_multi", "bertugues"
# ==============================================================================

RESULTADO_DIR_PADRAO = "/home/labpi/pave-tm/resultado_final"

COLLECTION_TO_FONTE = {
    "reddit": "reddit",
    "meta": "meta",
    "youtube_comments": "youtube",
}

# slug(embedding_model) em disco -> (chave curta p/ modelo.nome, nome HF completo)
EMBEDDING_SLUG_TO_KEY = {
    "sentence_transformers_all_minilm_l12_v2": ("minilm_l12", "sentence-transformers/all-MiniLM-L12-v2"),
    "sentence_transformers_paraphrase_multilingual_mpnet_base_v2": ("mpnet_multi", "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"),
    "qwen_qwen3_embedding_0_6b": ("qwen3_0_6b", "Qwen/Qwen3-Embedding-0.6B"),
    "ricardoz_bertugues_base_portuguese_cased": ("bertugues", "ricardoz/BERTugues-base-portuguese-cased"),
}

TOPIC_LINE_RE = re.compile(r"^(\d+)\s*-\s*(.+)$")


# ----------------------------------------------------------------------------
# PARSING DOS ARQUIVOS DO pave-tm
# ----------------------------------------------------------------------------
def parse_result_topic_txt(path: Path) -> dict:
    """'result_topic_10.txt' -> {numero_topico: [palavra, ...]}."""
    topics = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        m = TOPIC_LINE_RE.match(line.strip())
        if m:
            topics[int(m.group(1))] = m.group(2).split()
    return topics


def parse_resumo_csv(path: Path) -> list:
    """'Resumo_Topicos_Dominantes.csv' -> [(id_nativo, numero_topico), ...]."""
    rows = []
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append((row["papers"], int(row["dominant_topic"])))
    return rows


def parse_rotulos_json(path: Path) -> dict:
    """'rotulos_topicos.json' (see pave-tm/pipeline/llm_labeling.py) -> {numero_topico: rotulo}."""
    if not path.exists():
        return {}
    return {int(numero): rotulo for numero, rotulo in json.loads(path.read_text(encoding="utf-8")).items()}


def find_candidatos(resultado_dir: Path, collections: list) -> list:
    """[(collection, candidate_slug, candidate_dir), ...] existentes em disco.

    O diretorio em `resultado_dir` e nomeado pelo fonte_codigo (COLLECTION_TO_FONTE),
    nao pelo nome nativo do Mongo em `collection` - para reddit/meta os dois coincidem
    (mascarando isso ate agora), mas youtube_comments (Mongo) -> youtube (fonte_codigo e
    o nome real do diretorio em resultado_final/).
    """
    candidatos = []
    for collection in collections:
        collection_dir = resultado_dir / COLLECTION_TO_FONTE[collection]
        if not collection_dir.is_dir():
            continue
        for candidate_dir in sorted(collection_dir.iterdir()):
            if candidate_dir.is_dir() and candidate_dir.name != "old":
                candidatos.append((collection, candidate_dir.name, candidate_dir))
    return candidatos


def find_combinacoes(candidate_dir: Path) -> list:
    """[(k, embedding_slug, emb_dir, result_txt, resumo_csv, rotulos_json), ...] completas para 1 candidato.

    `rotulos_json` (rotulos_topicos.json, gerado por pave-tm/pipeline/llm_labeling.py) e opcional -
    o Path e retornado mesmo quando o arquivo nao existe; parse_rotulos_json trata a ausencia.
    """
    combos = []
    for k_dir in sorted(candidate_dir.glob("k*")):
        bt_dir = k_dir / "bertopic_kmeans"
        if not bt_dir.is_dir():
            continue
        for emb_dir in sorted(bt_dir.iterdir()):
            result_txt = next(emb_dir.glob("result_topic_*.txt"), None)
            resumo_csv = emb_dir / "Resumo_Topicos_Dominantes.csv"
            rotulos_json = emb_dir / "rotulos_topicos.json"
            if result_txt and result_txt.exists() and resumo_csv.exists():
                combos.append((int(k_dir.name.lstrip("k")), emb_dir.name, emb_dir, result_txt, resumo_csv, rotulos_json))
            else:
                logger.warning("Combinacao incompleta (ignorada): %s", emb_dir)
    return combos


# ----------------------------------------------------------------------------
# CARGA
# ----------------------------------------------------------------------------
def upsert_modelo(session, fonte_codigo, entidade_codigo, embedding_key, embedding_nome_hf,
                   k, treinado_em, parametros, metricas) -> int:
    # A unicidade natural de `modelo` e (tipo, nome, versao) -- SEM fonte_codigo. Uma mesma
    # entidade pode existir em mais de uma fonte (ex: "lula" em reddit e em meta), entao
    # fonte_codigo precisa entrar em `nome` para nao colidir (ver bug corrigido: um upsert de
    # meta/lula sobrescrevia o modelo de reddit/lula quando nome era so entidade+embedding).
    nome = f"{fonte_codigo}_{entidade_codigo}__{embedding_key}"
    versao = f"k{k}"

    if k == K_VIGENTE and embedding_key == EMBEDDING_VIGENTE:
        status_modelo = StatusModeloEnum.vigente
    else:
        status_modelo = StatusModeloEnum.arquivado

    stmt = (
        pg_insert(Modelo)
        .values(
            tipo=TipoModeloEnum.topico,
            fonte_codigo=fonte_codigo,
            nome=nome,
            versao=versao,
            treinado_em=treinado_em,
            parametros=parametros,
            metricas=metricas,
            status=status_modelo,
        )
        .on_conflict_do_update(
            index_elements=[Modelo.tipo, Modelo.nome, Modelo.versao],
            set_=dict(
                fonte_codigo=fonte_codigo, 
                treinado_em=treinado_em, 
                parametros=parametros, 
                metricas=metricas,
                status=status_modelo  # Atualiza o status caso o registro já exista
            ),
        )
        .returning(Modelo.id)
    )
    return session.execute(stmt).scalar_one()


def upsert_topicos(session, modelo_id: int, topics_palavras: dict, tamanhos: dict, rotulos: dict) -> dict:
    """Retorna {numero_topico: topico_id}.

    `rotulos` (numero_topico -> rotulo, de rotulos_topicos.json) so sobrescreve topico.rotulo
    quando o topico ainda nao foi `revisado` -- um rotulo confirmado/editado manualmente nao e
    reprocessado por uma rodada posterior do gerador automatico.
    """
    numero_para_id = {}
    for numero, palavras in topics_palavras.items():
        rotulo_data = rotulos.get(numero) or {}
        
        # Extrai titulo e descricao do novo JSON, mantendo compatibilidade com textos antigos
        if isinstance(rotulo_data, dict):
            rotulo_texto = rotulo_data.get("titulo")
            descricao_texto = rotulo_data.get("descricao")
        else:
            rotulo_texto = rotulo_data
            descricao_texto = None

        stmt = (
            pg_insert(Topico)
            .values(
                modelo_id=modelo_id,
                numero=numero,
                palavras_chave=palavras,
                tamanho=tamanhos.get(numero, 0),
                rotulo=rotulo_texto,     
                descricao=descricao_texto, 
            )
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['modelo_id', 'numero'],
            set_=dict(
                palavras_chave=stmt.excluded.palavras_chave,
                tamanho=stmt.excluded.tamanho,
                rotulo=case(
                    (Topico.revisado == True, Topico.rotulo),
                    else_=stmt.excluded.rotulo
                ),
                descricao=case(
                    (Topico.revisado == True, Topico.descricao),
                    else_=stmt.excluded.descricao
                )
            )
        ).returning(Topico.id)
        numero_para_id[numero] = session.execute(stmt).scalar_one()
    return numero_para_id


def carregar_documento_topico(session, documento_topico_table, doc_assignments, id_nativo_para_doc_id, numero_para_topico_id):
    linhas = []
    faltando = 0
    for id_nativo, numero_topico in doc_assignments:
        documento_id = id_nativo_para_doc_id.get(id_nativo)
        topico_id = numero_para_topico_id.get(numero_topico)
        if documento_id is None or topico_id is None:
            faltando += 1
            continue
        linhas.append({"documento_id": documento_id, "topico_id": topico_id})
    if faltando:
        logger.warning("  %d atribuicoes sem documento/topico correspondente (ignoradas).", faltando)
    if not linhas:
        return 0
    stmt = pg_insert(documento_topico_table).values(linhas)
    stmt = stmt.on_conflict_do_nothing(index_elements=["documento_id", "topico_id"])
    session.execute(stmt)
    return len(linhas)


def mapear_documentos(session, fonte_codigo: str, entidade_codigo: str) -> dict:
    """{id_nativo: documento.id} para todos os documentos de (fonte, entidade)."""
    rows = (
        session.query(Documento.id_nativo, Documento.id)
        .join(AlvoColeta, Documento.alvo_coleta_id == AlvoColeta.id)
        .filter(AlvoColeta.fonte_codigo == fonte_codigo, AlvoColeta.entidade_codigo == entidade_codigo)
        .all()
    )
    return dict(rows)


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
    try:
        for collection, candidate_slug, candidate_dir in find_candidatos(resultado_dir, args.collections):
            fonte_codigo = COLLECTION_TO_FONTE[collection]
            combos = find_combinacoes(candidate_dir)
            if not combos:
                continue
            logger.info("=== %s/%s (%d combinacoes) ===", collection, candidate_slug, len(combos))

            id_nativo_para_doc_id = mapear_documentos(session, fonte_codigo, candidate_slug)
            if not id_nativo_para_doc_id:
                logger.warning("Nenhum documento em %s/%s no Postgres -- pulando candidato.", fonte_codigo, candidate_slug)
                continue

            for k, embedding_slug, emb_dir, result_txt, resumo_csv, rotulos_json in combos:
                if embedding_slug not in EMBEDDING_SLUG_TO_KEY:
                    logger.warning("  Embedding desconhecido (ignorado): %s", embedding_slug)
                    continue
                embedding_key, embedding_nome_hf = EMBEDDING_SLUG_TO_KEY[embedding_slug]

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
                    "origem": "pave-tm/resultado_final",
                }
                metricas = {
                    "n_documentos_atribuidos": len(doc_assignments),
                    "n_topicos": len(topics_palavras),
                }

                logger.info("  k=%d embedding=%s (%d docs, %d topicos, %d rotulos)",
                            k, embedding_key, len(doc_assignments), len(topics_palavras), len(rotulos))

                if args.dry_run:
                    continue

                modelo_id = upsert_modelo(session, fonte_codigo, candidate_slug, embedding_key,
                                           embedding_nome_hf, k, treinado_em, parametros, metricas)
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
