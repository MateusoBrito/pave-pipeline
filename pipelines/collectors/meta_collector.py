import requests
import time
import os
import argparse
from datetime import datetime, timedelta
import logging
from pymongo import MongoClient, UpdateOne
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

_tokens_raw = os.getenv("META_ACCESS_TOKEN", "")
ACCESS_TOKENS = [token.strip() for token in _tokens_raw.split(",") if token.strip()]

if not ACCESS_TOKENS:
    raise SystemExit(
        "Defina a variavel de ambiente META_ACCESS_TOKENS (um ou mais tokens "
        "separados por virgula) antes de rodar o script."
    )

RATE_LIMIT_ERROR_CODES = {4, 17, 32, 613}

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
    # Faltava - sem isso, o filtro de plataforma (Facebook/Instagram) no webapp nunca
    # batia com nenhum anúncio: metadados.publisher_platforms simplesmente não existia
    # em nenhum documento (ver has_platform() em pave-webapp/api/app/queries/metadata.py).
    "publisher_platforms",
])

class TokenManager:
    def __init__(self, tokens):
        self.tokens = tokens
        self.indice_atual = 0

    def token_atual(self):
        return self.tokens[self.indice_atual]

    def rotacionar(self):
        self.indice_atual = (self.indice_atual + 1) % len(self.tokens)

    def total_tokens(self):
        return len(self.tokens)

def erro_rate_limit(response):
    erro = response.get("error", {})
    codigo = erro.get("code")
    return codigo in RATE_LIMIT_ERROR_CODES

def requisicao_com_rotacao(url, params, token_manager, contexto):
    tentativas = token_manager.total_tokens()
 
    for tentativa in range(tentativas):
        token_atual = token_manager.token_atual()
        params_com_token = dict(params)
        params_com_token["access_token"] = token_atual
 
        try:
            response = requests.get(url, params=params_com_token, timeout=30)
        except requests.RequestException as erro_rede:
            logger.error(f"[{contexto}] Falha de rede: {erro_rede}. Tentando novamente em 5s...")
            time.sleep(5)
            continue
 
        try:
            data = response.json()
        except ValueError:
            logger.error(
                f"[{contexto}] Resposta sem corpo JSON válido (status {response.status_code}): "
                f"{response.text[:300]}"
            )
            time.sleep(2)
            continue
 
        if response.status_code == 200 and "error" not in data:
            return data
 
        if erro_rate_limit(data):
            logger.warning(
                f"[{contexto}] Rate limit atingido no token #{token_manager.indice_atual + 1}"
                f"/{tentativas}. Rotacionando token e repetindo a MESMA página."
            )
            token_manager.rotacionar()
            time.sleep(2)
            continue
 
        # Erro que não é de rate limit: não adianta trocar de token.
        logger.error(f"[{contexto}] Erro da API (não é rate limit): {data.get('error')}")
        return None
 
    logger.error(
        f"[{contexto}] Todos os {tentativas} tokens atingiram o rate limit para esta página. "
        f"Desistindo desta requisição."
    )
    return None


def coletar_dados(page_id, data_inicio, data_fim, token_manager):
    params = {
        "search_page_ids": f'["{page_id}"]',
        "ad_type": "POLITICAL_AND_ISSUE_ADS",
        "ad_reached_countries": '["BR"]',
        "ad_active_status": "ALL",
        "ad_delivery_date_min": data_inicio,
        "ad_delivery_date_max": data_fim,
        "fields": FIELDS,
        "limit": 100,
    }

    dados_totais = []
    after_cursor = None
    pagina_num = 1

    while True:
        params = dict(params)
        if after_cursor:
            params["after"] = after_cursor

        contexto = f"page_id={page_id}, pagina={pagina_num}"
        data = requisicao_com_rotacao(ADS_URL, params, token_manager, contexto)

        if data is None:
            # não conseguiu com nenhum token
            logger.error(f"[{contexto}] Falha na requisição. Encerrando coleta para a página {page_id}.")
            break

        pagina = data.get("data", [])

        for ad in pagina:
            ad["_entidade_busca"] = page_id
            dados_totais.append(ad)

        logger.info(
            f"[{contexto}] +{len(pagina)} anúncios processados "
            f"(total filtrado: {len(dados_totais)})"
        )

        paging = data.get("paging", {})
        if "next" not in paging:
            break

        after_cursor = paging.get("cursors", {}).get("after")
        if not after_cursor:
            logger.warning(f"[{contexto}] 'next' presente, mas sem cursor 'after'. Encerrando coleta.")
            break

        pagina_num += 1
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

        operacoes = [UpdateOne({"id": doc["id"]}, {"$set": doc}, upsert=True) for doc in dados]

        try:
            resultado = colecao.bulk_write(operacoes, ordered=False)
            inseridos = resultado.upserted_count
            atualizados = resultado.modified_count
            logger.info(f"[{entidade}] Inseridos: {inseridos} novos | Atualizados: {atualizados} existentes.")
        except BulkWriteError as bwe:
            logger.warning(f"[{entidade}] Erro parcial no BulkWrite: {bwe.details}")

    except PyMongoError as erro:
        logger.error(f"[{entidade}] Erro ao conectar/inserir no MongoDB: {erro}")
        raise
    finally:
        if cliente is not None:
            cliente.close()


def _formatar_data(d):
    return d.strftime("%Y-%m-%d")


def main():
    ontem = _formatar_data(datetime.utcnow().date() - timedelta(days=1))

    parser = argparse.ArgumentParser(description="Coletor do Meta")
    parser.add_argument("--entidade", type=str, required=True, help="Page ID (ex: 267949976607343)")
    parser.add_argument(
        "--data-inicio", type=str, default=ontem,
        help=f"Data de início da veiculação (YYYY-MM-DD). Default: D-1 ({ontem}), para uso em coleta diária via Airflow.",
    )
    parser.add_argument(
        "--data-fim", type=str, default=ontem,
        help=f"Data de fim da veiculação (YYYY-MM-DD). Default: D-1 ({ontem}).",
    )
    args = parser.parse_args()

    for data in (args.data_inicio, args.data_fim):
        try:
            datetime.strptime(data, "%Y-%m-%d")
        except ValueError:
            raise SystemExit(f"Data invalida: '{data}'. Use o formato YYYY-MM-DD.")

    token_manager = TokenManager(ACCESS_TOKENS)

    logger.info(
        f"=== Coletando anúncios para page_id={args.entidade} "
        f"(janela: {args.data_inicio} a {args.data_fim}) ==="
    )
    dados = coletar_dados(page_id=args.entidade, data_inicio=args.data_inicio, data_fim=args.data_fim, token_manager=token_manager)
    salvar_mongodb(dados, entidade=args.entidade)


if __name__ == "__main__":
    main()