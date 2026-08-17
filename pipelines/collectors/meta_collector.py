import requests
import time
import os
import argparse
from datetime import datetime
import logging
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError, PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("coleta_meta")

MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "panorama")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION_META", "meta")

if not MONGO_USER or not MONGO_PASSWORD:
    raise SystemExit(
        "Defina as variaveis de ambiente MONGO_INITDB_ROOT_USERNAME e "
        "MONGO_INITDB_ROOT_PASSWORD antes de rodar o script."
    )

MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"

ACCESS_TOKENS = [
    "EAAObkoZCck74BSDBkyA5AeIheVuyTkT5yt1VnNTYu0EEkI1ybGhJeKdTNzZCV9uWBSR5YAagkOPaPZA0aCMzUiuI8GUJ9t4XrA8zLReJ70ZBMh4ZAvMLmtCIenyEPyfR3k2UWagODaZCbquttxQ075rZAv7Ts1jJf8o8EaULz1gmZAfcZAkZASJX06KR84cLfZBLZB9yUeyrKfrYOuioFBem"
    "EAAOir8WJXxABSBcGqKWy2pCXTaSQY2IEWQshxwHK9G7eu9F5kZCfzt5zZBJ8Bqa6MLUEB8inH4p2uJkVZA93oYmFiDqwqpxCelKwWE2Xejr53EzxryMFjgZBbkVeXoHUZAYJsqrGjd7O9dzElSDzqZCiHphIUFL5hzMNP2IzDUhv6efdaVSVXcGKzwbH3mCOJ8duDR8Yb1xk56DymH11z03NRZAnftru4087su3uAZDZD"
]

ACESS_TOKENS = [token.strip() for token in ACCESS_TOKENS if token.strip()]

if not ACCESS_TOKEN:
    raise SystemExit("Adicione um tokens de acesso ao ACESS_TOKENS.")

GRAPH_VERSION = "v21.0"
BASE_URL = f"https://graph.facebook.com/{GRAPH_VERSION}"
ADS_URL = f"{BASE_URL}/ads_archive"


FIELDS = ",".join([
    "page_name",
    "page_id",
    "ad_delivery_start_time",
    "ad_delivery_stop_time",
    "ad_creative_bodies",
    "ad_creative_link_titles",
    "spend",
    "impressions",
])


def coletar_dados(page_id, data_inicio, data_fim):
    params = {
        "access_token": ACCESS_TOKEN,
        "search_page_ids": f'["{page_id}"]',
        "ad_type": "POLITICAL_AND_ISSUE_ADS",
        "ad_reached_countries": '["BR"]',
        "fields": FIELDS,
        "limit": 100,
    }

    dados_totais = []
    next_url = ADS_URL
    first = True

    while next_url:
        if first:
            response = requests.get(next_url, params=params, timeout=30)
            first = False
        else:
            response = requests.get(next_url, timeout=30)

        if response.status_code != 200:
            logger.error(f"Erro {response.status_code} para page_id={page_id}: {response.text}")
            break

        data = response.json()

        if "error" in data:
            logger.error(f"Erro da API para page_id={page_id}: {data['error']}")
            break

        pagina = data.get("data", [])
        
        for ad in pagina:
            ad_start = ad.get("ad_delivery_start_time")
            if not ad_start:
                dados_totais.append(ad)
                continue
            
            # Formato do ad_delivery_start_time costuma ser YYYY-MM-DD
            if data_inicio <= ad_start[:10] <= data_fim:
                ad["_entidade_busca"] = page_id
                dados_totais.append(ad)

        logger.info(f"[{page_id}] +{len(pagina)} anúncios processados (total filtrado: {len(dados_totais)})")

        next_url = data.get("paging", {}).get("next")
        time.sleep(1)  # respeita rate limit

    return dados_totais


def salvar_mongodb(dados, entidade):
    if not dados:
        logger.warning(f"[{entidade}] Nenhum dado para salvar no MongoDB.")
        return

    cliente = None
    try:
        cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        cliente.admin.command("ping")

        colecao = cliente[MONGO_DATABASE][MONGO_COLLECTION]
        # Meta usa id para os anúncios
        colecao.create_index("id", unique=True, background=True)

        operacoes = [InsertOne(doc) for doc in dados]

        try:
            resultado = colecao.bulk_write(operacoes, ordered=False)
            logger.info(f"[{entidade}] Inseridos: {resultado.inserted_count} novos documentos.")
        except BulkWriteError as bwe:
            inseridos = bwe.details.get("nInserted", 0)
            duplicados = sum(
                1 for e in bwe.details.get("writeErrors", []) if e.get("code") == 11000
            )
            logger.warning(f"[{entidade}] Insercao parcial: {inseridos} inseridos, {duplicados} ja existiam.")

    except PyMongoError as erro:
        logger.error(f"[{entidade}] Erro ao conectar/inserir no MongoDB: {erro}")
        raise
    finally:
        if cliente is not None:
            cliente.close()


def main():
    parser = argparse.ArgumentParser(description="Coletor do Meta")
    parser.add_argument("--entidade", type=str, required=True, help="Page ID (ex: 267949976607343)")
    parser.add_argument("--data-inicio", type=str, required=True, help="Data de início (YYYY-MM-DD)")
    parser.add_argument("--data-fim", type=str, required=True, help="Data de fim (YYYY-MM-DD)")
    args = parser.parse_args()

    logger.info(f"=== Coletando anúncios para page_id={args.entidade} ===")
    dados = coletar_dados(page_id=args.entidade, data_inicio=args.data_inicio, data_fim=args.data_fim)
    salvar_mongodb(dados, entidade=args.entidade)


if __name__ == "__main__":
    main()