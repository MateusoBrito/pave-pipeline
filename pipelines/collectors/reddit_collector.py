import argparse
import os
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError, PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("coleta_reddit")

MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "panorama")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "reddit")

if not MONGO_USER or not MONGO_PASSWORD:
    raise SystemExit(
        "Defina as variaveis de ambiente MONGO_INITDB_ROOT_USERNAME e "
        "MONGO_INITDB_ROOT_PASSWORD antes de rodar o script."
    )

MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"


BASE_URL = "https://arctic-shift.photon-reddit.com"
ENDPOINT_POSTS = f"{BASE_URL}/api/posts/search"
ENDPOINT_COMMENTS = f"{BASE_URL}/api/comments/search"

API_LIMIT_POR_PAGINA = 100
SLEEP_ENTRE_REQUESTS_SEGUNDOS = 10.0

MAX_TENTATIVAS_POR_REQUISICAO = 5
BACKOFF_INICIAL_SEGUNDOS = 15
MENSAGENS_ERRO_TRANSITORIO = {
    "Timeout. Maybe slow down a bit",
    "Too many requests",
}

def _gerar_janelas_mensais(data_inicio: str, data_fim: str) -> List[Dict[str, int]]:
    inicio = datetime.strptime(data_inicio, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    fim = datetime.strptime(data_fim, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    janelas = []
    cursor = inicio
    while cursor < fim:
        if cursor.month == 12:
            proximo_mes = cursor.replace(year=cursor.year + 1, month=1, day=1)
        else:
            proximo_mes = cursor.replace(month=cursor.month + 1, day=1)

        fim_janela = min(proximo_mes, fim)
        janelas.append({
            "after": int(cursor.timestamp()),
            "before": int(fim_janela.timestamp()),
        })
        cursor = fim_janela

    return janelas

def _buscar_pagina(endpoint: str, params: Dict) -> Optional[List[Dict]]:
    for tentativa in range(1, MAX_TENTATIVAS_POR_REQUISICAO + 1):
        try:
            resposta = requests.get(endpoint, params=params, timeout=30)
            resposta.raise_for_status()
            corpo = resposta.json()
            return corpo.get("data", [])

        except requests.exceptions.RequestException as erro:
            resposta_erro = getattr(erro, "response", None)
            texto_erro = resposta_erro.text[:500] if resposta_erro is not None else ""

            mensagem_api = ""
            if resposta_erro is not None:
                try:
                    mensagem_api = resposta_erro.json().get("error", "")
                except ValueError:
                    pass

            eh_transitorio = (
                mensagem_api in MENSAGENS_ERRO_TRANSITORIO
                or (resposta_erro is not None and resposta_erro.status_code == 429)
            )

            if eh_transitorio and tentativa < MAX_TENTATIVAS_POR_REQUISICAO:
                espera = BACKOFF_INICIAL_SEGUNDOS * tentativa
                logger.warning(
                    "Erro transitorio da API ('%s') na tentativa %d/%d. Aguardando %ds...",
                    mensagem_api or erro, tentativa, MAX_TENTATIVAS_POR_REQUISICAO, espera,
                )
                time.sleep(espera)
                continue

            logger.error(
                "Erro na requisicao para %s | params=%s | erro=%s | resposta_api=%s",
                endpoint, params, erro, texto_erro,
            )
            return None

    return None


def _coletar_com_paginacao(endpoint: str, params_base: Dict, tipo: str) -> List[Dict]:
    todos_resultados: List[Dict] = []
    params = dict(params_base)
    after_atual = params["after"]
    before_fixo = params["before"]

    while True:
        params["after"] = after_atual
        params["before"] = before_fixo

        pagina = _buscar_pagina(endpoint, params)

        if pagina is None:
            logger.warning(
                "[%s] Falha definitiva ao buscar pagina (after=%s). "
                "ESTA JANELA PODE ESTAR INCOMPLETA.",
                tipo, after_atual,
            )
            break

        if not pagina:
            break

        todos_resultados.extend(pagina)
        logger.info("[%s] +%d itens (total: %d)", tipo, len(pagina), len(todos_resultados))

        if len(pagina) < params["limit"]:
            break

        ultimo_item = pagina[-1]
        after_atual = int(ultimo_item["created_utc"]) + 1
        time.sleep(SLEEP_ENTRE_REQUESTS_SEGUNDOS)

    return todos_resultados


def coletar_dados(entidade: str, data_inicio: str, data_fim: str) -> Dict[str, List[Dict]]:

    janelas = _gerar_janelas_mensais(data_inicio, data_fim)
    logger.info("[%s] Periodo dividido em %d janela(s) mensal(is).", entidade, len(janelas))

    todos_posts: List[Dict] = []
    todos_comentarios: List[Dict] = []

    # A entidade recebida eh o proprio nome do subreddit
    subreddit = entidade

    for janela in janelas:
        mes = datetime.fromtimestamp(janela["after"], tz=timezone.utc).strftime("%Y-%m")

        params_posts = {
            "subreddit": subreddit,
            "after": janela["after"],
            "before": janela["before"],
            "limit": API_LIMIT_POR_PAGINA,
            "sort": "asc",
        }
        posts = _coletar_com_paginacao(ENDPOINT_POSTS, params_posts, tipo=f"posts/r_{subreddit}/{mes}")
        for p in posts:
            p["_subreddit_busca"] = subreddit
            p["_entidade_busca"] = entidade
            p["_tipo_documento"] = "post"
        todos_posts.extend(posts)
        time.sleep(SLEEP_ENTRE_REQUESTS_SEGUNDOS)

        params_comentarios = {
            "subreddit": subreddit,
            "after": janela["after"],
            "before": janela["before"],
            "limit": API_LIMIT_POR_PAGINA,
            "sort": "asc",
        }
        comentarios = _coletar_com_paginacao(
            ENDPOINT_COMMENTS, params_comentarios, tipo=f"comments/r_{subreddit}/{mes}"
        )
        for c in comentarios:
            c["_subreddit_busca"] = subreddit
            c["_entidade_busca"] = entidade
            c["_tipo_documento"] = "comentario"
        todos_comentarios.extend(comentarios)
        time.sleep(SLEEP_ENTRE_REQUESTS_SEGUNDOS)

    logger.info(
        "[%s] Coleta finalizada: %d posts e %d comentarios.",
        entidade, len(todos_posts), len(todos_comentarios),
    )
    return {"posts": todos_posts, "comentarios": todos_comentarios}

def salvar_mongodb(dados: Dict[str, List[Dict]], entidade: str) -> None:
    documentos = dados.get("posts", []) + dados.get("comentarios", [])

    if not documentos:
        logger.warning("[%s] Nenhum dado para salvar no MongoDB.", entidade)
        return

    cliente = None
    try:
        cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        cliente.admin.command("ping")

        colecao = cliente[MONGO_DATABASE][MONGO_COLLECTION]
        colecao.create_index([("id", 1), ("_tipo_documento", 1)], unique=True, background=True)

        operacoes = [InsertOne(doc) for doc in documentos]

        try:
            resultado = colecao.bulk_write(operacoes, ordered=False)
            logger.info("[%s] Inseridos: %d novos documentos.", entidade, resultado.inserted_count)
        except BulkWriteError as bwe:
            inseridos = bwe.details.get("nInserted", 0)
            duplicados = sum(
                1 for e in bwe.details.get("writeErrors", []) if e.get("code") == 11000
            )
            logger.warning(
                "[%s] Insercao parcial: %d inseridos, %d ja existiam.",
                entidade, inseridos, duplicados,
            )

    except PyMongoError as erro:
        logger.error("[%s] Erro ao conectar/inserir no MongoDB: %s", entidade, erro)
        raise
    finally:
        if cliente is not None:
            cliente.close()

def main():
    parser = argparse.ArgumentParser(description="Coletor do Reddit")
    parser.add_argument("--entidade", type=str, required=True, help="Nome do subreddit para coleta")
    parser.add_argument("--data-inicio", type=str, required=True, help="Data de início (YYYY-MM-DD)")
    parser.add_argument("--data-fim", type=str, required=True, help="Data de fim (YYYY-MM-DD)")
    args = parser.parse_args()

    logger.info("=== Coletando Reddit para: %s ===", args.entidade)
    dados = coletar_dados(entidade=args.entidade, data_inicio=args.data_inicio, data_fim=args.data_fim)
    salvar_mongodb(dados, entidade=args.entidade)


if __name__ == "__main__":
    main()