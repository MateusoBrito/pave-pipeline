"""
Este script conecta ao MongoDB e extrai uma amostra aleatória de comentários 
válidos (Reddit e/ou YouTube), distribuindo-os em formulários balanceados 
para alimentar o Google Apps Script e criar os Google Forms.

Uso Básico:
    # 1. Extrair ambas as redes (gera 2 arquivos CSV separados):
    python3 pipelines/nlp/extrair_comentarios_forms.py

    # 2. Extrair apenas comentários do Reddit:
    python3 pipelines/nlp/extrair_comentarios_forms.py --rede reddit

    # 3. Extrair apenas comentários do YouTube:
    python3 pipelines/nlp/extrair_comentarios_forms.py --rede youtube

    # 4. Customizando parâmetros:
    python3 pipelines/nlp/extrair_comentarios_forms.py \
        --rede youtube \
        --num-forms 5 \
        --por-rede 200 \
        --pasta-saida data/forms

Parâmetros:
    --rede          Qual rede extrair: 'todas' (padrão), 'reddit' ou 'youtube'.
    --num-forms     Número de formulários a serem criados (padrão: 10).
    --por-rede      Total de comentários aleatórios por rede (padrão: 500).
    --pasta-saida   Pasta onde os CSVs serão salvos (padrão: 'data/forms').

Arquivos Gerados:
    - data/forms/reddit_comentarios_para_forms.csv
    - data/forms/youtube_comentarios_para_forms.csv
==================================================================================
"""
import os
import csv
import logging
import argparse
from pathlib import Path
from urllib.parse import quote_plus
from pymongo import MongoClient
from pymongo.errors import PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("extracao_forms")


def carregar_env():
    """Carrega .env subindo os diretórios se não estiverem no ambiente."""
    diretorio = Path(__file__).resolve().parent
    for _ in range(5):
        candidato = diretorio / ".env"
        if candidato.is_file():
            for linha in candidato.read_text().splitlines():
                linha = linha.strip()
                if not linha or linha.startswith("#") or "=" not in linha:
                    continue
                chave, _, valor = linha.partition("=")
                os.environ.setdefault(chave.strip(), valor.strip().strip('"').strip("'"))
            logger.info(f"Variáveis carregadas de: {candidato}")
            return
        if diretorio.parent == diretorio:
            break
        diretorio = diretorio.parent


carregar_env()

MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "panorama")


def get_mongo_client() -> MongoClient:
    if not MONGO_USER or not MONGO_PASSWORD:
        mongo_uri = os.getenv("MONGO_URI", f"mongodb://{MONGO_HOST}:{MONGO_PORT}/")
    else:
        mongo_uri = f"mongodb://{quote_plus(MONGO_USER)}:{quote_plus(MONGO_PASSWORD)}@{MONGO_HOST}:{MONGO_PORT}/"
    
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client


def extrair_comentarios_reddit(db, total_amostra: int = 500):
    """
    Filtra comentários válidos do Reddit (removendo [deleted], [removed] e textos curtos)
    e extrai uma amostra aleatória.
    """
    colecao = db["reddit"]
    pipeline = [
        {
            "$match": {
                "_tipo_documento": "comentario",
                "body": {
                    "$exists": True,
                    "$ne": None,
                    "$nin": ["[deleted]", "[removed]", ""]
                },
                "$expr": {"$gte": [{"$strLenCP": "$body"}, 15]}
            }
        },
        {"$sample": {"size": total_amostra}},
        {
            "$project": {
                "_id": 0,
                "id_origem": "$id",
                "texto": "$body",
                "termo_busca": "$_termo_busca",
                "contexto": "$_subreddit_busca"
            }
        }
    ]
    
    docs = list(colecao.aggregate(pipeline))
    logger.info(f"Reddit: {len(docs)} comentários extraídos.")
    for d in docs:
        d["rede_social"] = "reddit"
    return docs


def extrair_comentarios_youtube(db, total_amostra: int = 500):
    """
    Filtra comentários válidos do YouTube e extrai uma amostra aleatória.
    """
    colecao = db["youtube_comments"]
    pipeline = [
        {
            "$match": {
                "text": {"$exists": True, "$ne": None, "$ne": ""},
                "$expr": {"$gte": [{"$strLenCP": "$text"}, 15]}
            }
        },
        {"$sample": {"size": total_amostra}},
        {
            "$project": {
                "_id": 0,
                "id_origem": "$commentId",
                "texto": "$text",
                "termo_busca": "$_entidade_busca",
                "contexto": "$channelTitle"
            }
        }
    ]
    
    docs = list(colecao.aggregate(pipeline))
    logger.info(f"YouTube: {len(docs)} comentários extraídos.")
    for d in docs:
        d["rede_social"] = "youtube"
    return docs


def distribuir_em_formularios(docs: list, rede_social: str, num_forms: int = 10):
    """
    Distribui os comentários de UMA rede social em 'num_forms' lotes.
    Exemplo para Reddit: Form_Reddit_01 até Form_Reddit_10.
    Exemplo para YouTube: Form_YouTube_01 até Form_YouTube_10.
    """
    total_docs = len(docs)
    if total_docs == 0:
        return []

    tamanho_lote = total_docs // num_forms
    resto = total_docs % num_forms
    
    todos_itens = []
    item_global_id = 1
    cursor = 0

    nome_rede_formatado = rede_social.capitalize()

    for i in range(num_forms):
        form_id = f"Form_{nome_rede_formatado}_{i+1:02d}"
        
        # Distribui o resto entre os primeiros lotes se houver sobra
        tamanho_atual = tamanho_lote + (1 if i < resto else 0)
        lote = docs[cursor : cursor + tamanho_atual]
        cursor += tamanho_atual
        
        for idx_no_form, item in enumerate(lote, start=1):
            texto_limpo = " ".join(item["texto"].split())
            
            todos_itens.append({
                "item_id": f"{rede_social.upper()}_{item_global_id:04d}",
                "form_id": form_id,
                "ordem_no_form": idx_no_form,
                "rede_social": item["rede_social"],
                "id_origem": item["id_origem"],
                "contexto": item.get("contexto", ""),
                "texto": texto_limpo
            })
            item_global_id += 1

    return todos_itens


def exportar_csv(itens, caminho_saida: str):
    if not itens:
        logger.warning(f"Nenhum item para exportar em: {caminho_saida}")
        return
    Path(caminho_saida).parent.mkdir(parents=True, exist_ok=True)
    colunas = ["item_id", "form_id", "ordem_no_form", "rede_social", "id_origem", "contexto", "texto"]
    
    with open(caminho_saida, mode="w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=colunas)
        writer.writeheader()
        writer.writerows(itens)
        
    logger.info(f"Arquivo salvo com sucesso: {caminho_saida} ({len(itens)} itens).")


def main():
    parser = argparse.ArgumentParser(description="Extração aleatória de comentários para Forms (Separado por Rede)")
    parser.add_argument(
        "--rede",
        type=str,
        choices=["todas", "reddit", "youtube"],
        default="todas",
        help="Qual rede extrair: 'todas' (gera 2 arquivos), 'reddit' ou 'youtube'"
    )
    parser.add_argument("--pasta-saida", type=str, default="data/forms", help="Pasta de saída para os CSVs")
    parser.add_argument("--num-forms", type=int, default=10, help="Número de formulários por rede (padrão: 10)")
    parser.add_argument("--por-rede", type=int, default=500, help="Total de comentários a extrair por rede (padrão: 500)")
    args = parser.parse_args()

    client = get_mongo_client()
    db = client[MONGO_DATABASE]

    try:
        if args.rede in ["todas", "reddit"]:
            reddit_docs = extrair_comentarios_reddit(db, total_amostra=args.por_rede)
            if reddit_docs:
                itens_reddit = distribuir_em_formularios(reddit_docs, "reddit", num_forms=args.num_forms)
                saida_reddit = os.path.join(args.pasta_saida, "reddit_comentarios_para_forms.csv")
                exportar_csv(itens_reddit, saida_reddit)
            else:
                logger.warning("Nenhum comentário do Reddit encontrado.")

        if args.rede in ["todas", "youtube"]:
            youtube_docs = extrair_comentarios_youtube(db, total_amostra=args.por_rede)
            if youtube_docs:
                itens_youtube = distribuir_em_formularios(youtube_docs, "youtube", num_forms=args.num_forms)
                saida_youtube = os.path.join(args.pasta_saida, "youtube_comentarios_para_forms.csv")
                exportar_csv(itens_youtube, saida_youtube)
            else:
                logger.warning("Nenhum comentário do YouTube encontrado.")

    finally:
        client.close()


if __name__ == "__main__":
    main()

