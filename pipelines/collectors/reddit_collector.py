"""
==================================================================================
COLETOR DE DADOS DO REDDIT (API ARCTIC SHIFT) - pipelines/collectors/reddit_collector.py
==================================================================================
Recebe um SUBREDDIT e um TERMO DE BUSCA (candidato/entidade) e coleta os
posts e comentários desse subreddit, dentro do período informado, que
mencionem o termo. Pensado para ser chamado uma vez por combinação
(subreddit x termo_busca) pela DAG do Airflow (dags/dag_reddit.py).

Execução manual (fora do Airflow), exemplo:
    python3 reddit_collector.py --subreddit brasil --termo-busca "Lula" \
        --data-inicio 2026-08-10 --data-fim 2026-08-11

Como a coleta DIÁRIA funciona:
    Este script NÃO decide sozinho "o dia de ontem" -- ele só recebe
    --data-inicio / --data-fim como parâmetros e busca exatamente esse
    intervalo. Quem decide que o intervalo é de 1 dia é o Airflow: a DAG
    roda com `schedule_interval="@daily"` e passa
    `data_interval_start` / `data_interval_end` (via template Jinja) como
    esses parâmetros -- cada execução diária cobre automaticamente as
    últimas 24h daquele dia.
==================================================================================
"""

import argparse
import logging
import os
import sys
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests
from pymongo import InsertOne, MongoClient
from pymongo.errors import BulkWriteError, PyMongoError
import atexit

# Logger global que será configurado dinamicamente dentro do main()
logger = logging.getLogger("reddit_collector")


def _carregar_env_do_projeto():
    """
    Carrega variáveis do arquivo .env do projeto automaticamente (subindo
    diretórios a partir deste script até achar um `.env`), para funcionar
    tanto rodando manualmente no terminal (backfill) quanto via Airflow
    (BashOperator), sem depender de `export` manual em cada sessão.
    Não sobrescreve variáveis que já estejam definidas no ambiente (ex: as
    que o docker-compose já injeta dentro do container do Airflow).
    """
    diretorio = Path(__file__).resolve().parent
    for _ in range(5):
        candidato = diretorio / ".env"
        if candidato.is_file():
            for linha in candidato.read_text().splitlines():
                linha = linha.strip()
                if not linha or linha.startswith("#") or "=" not in linha:
                    continue
                chave, _, valor = linha.partition("=")
                chave = chave.strip()
                valor = valor.strip().strip('"').strip("'")
                os.environ.setdefault(chave, valor)
            return
        if diretorio.parent == diretorio:
            break
        diretorio = diretorio.parent


_carregar_env_do_projeto()

# ----------------------------------------------------------------------------
# MONGODB
# ----------------------------------------------------------------------------
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

# ----------------------------------------------------------------------------
# API ARCTIC SHIFT
# ----------------------------------------------------------------------------
BASE_URL = "https://arctic-shift.photon-reddit.com"
ENDPOINT_POSTS = f"{BASE_URL}/api/posts/search"
ENDPOINT_COMMENTS = f"{BASE_URL}/api/comments/search"

API_LIMIT_POR_PAGINA = 100
SLEEP_ENTRE_REQUESTS_SEGUNDOS = 20.0

MAX_TENTATIVAS_POR_REQUISICAO = 5
BACKOFF_INICIAL_SEGUNDOS = 15
MENSAGENS_ERRO_TRANSITORIO = {
    "Timeout. Maybe slow down a bit",
    "Too many requests",
}


# ----------------------------------------------------------------------------
# JANELAS DE TEMPO
# ----------------------------------------------------------------------------
def _gerar_janelas_mensais(data_inicio: str, data_fim: str) -> List[Dict[str, int]]:
    """
    Quebra o período em janelas de no máximo 1 mês (epoch), para evitar
    paginação profunda demais em subreddits grandes/ativos.

    Para uma execução DIÁRIA (data_fim = data_inicio + 1 dia, como a DAG
    faz), isso naturalmente gera uma única janela cobrindo aquele dia --
    não há necessidade de tratamento especial para o caso "diário".
    """
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


# ----------------------------------------------------------------------------
# REQUISIÇÕES + RETRY
# ----------------------------------------------------------------------------
def _buscar_pagina(endpoint: str, params: Dict) -> Optional[List[Dict]]:
    """
    Retorna a lista de resultados (mesmo que vazia) em caso de sucesso, ou
    None se todas as tentativas falharam definitivamente (para distinguir
    de "não há mais dados").
    """
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
                or resposta_erro is None
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


# ----------------------------------------------------------------------------
# COLETA
# ----------------------------------------------------------------------------
def coletar_dados(subreddit: str, termo_busca: str, data_inicio: str, data_fim: str) -> Dict[str, List[Dict]]:
    """
    Coleta posts (via `query`) e comentários (via `body`) do `subreddit`
    informado que mencionem `termo_busca`, dentro do período dado.
    """
    janelas = _gerar_janelas_mensais(data_inicio, data_fim)
    logger.info(
        "[r/%s | '%s'] Periodo dividido em %d janela(s).",
        subreddit, termo_busca, len(janelas),
    )

    todos_posts: List[Dict] = []
    todos_comentarios: List[Dict] = []

    for janela in janelas:
        mes = datetime.fromtimestamp(janela["after"], tz=timezone.utc).strftime("%Y-%m-%d")

        # --- POSTS ---
        params_posts = {
            "subreddit": subreddit,
            "query": termo_busca,        # <-- filtro de palavra-chave (título + corpo do post)
            "after": janela["after"],
            "before": janela["before"],
            "limit": API_LIMIT_POR_PAGINA,
            "sort": "asc",
        }
        posts = _coletar_com_paginacao(
            ENDPOINT_POSTS, params_posts, tipo=f"posts/r_{subreddit}/{termo_busca}/{mes}"
        )
        for p in posts:
            p["_subreddit_busca"] = subreddit
            p["_termo_busca"] = termo_busca
            p["_tipo_documento"] = "post"
        todos_posts.extend(posts)
        time.sleep(SLEEP_ENTRE_REQUESTS_SEGUNDOS)

        # --- COMENTÁRIOS ---
        params_comentarios = {
            "subreddit": subreddit,
            "body": termo_busca,         # <-- filtro de palavra-chave (corpo do comentário)
            "after": janela["after"],
            "before": janela["before"],
            "limit": API_LIMIT_POR_PAGINA,
            "sort": "asc",
        }
        comentarios = _coletar_com_paginacao(
            ENDPOINT_COMMENTS, params_comentarios, tipo=f"comments/r_{subreddit}/{termo_busca}/{mes}"
        )
        for c in comentarios:
            c["_subreddit_busca"] = subreddit
            c["_termo_busca"] = termo_busca
            c["_tipo_documento"] = "comentario"
        todos_comentarios.extend(comentarios)
        time.sleep(SLEEP_ENTRE_REQUESTS_SEGUNDOS)

    logger.info(
        "[r/%s | '%s'] Coleta finalizada: %d posts e %d comentarios.",
        subreddit, termo_busca, len(todos_posts), len(todos_comentarios),
    )
    return {"posts": todos_posts, "comentarios": todos_comentarios}


# ----------------------------------------------------------------------------
# PERSISTÊNCIA
# ----------------------------------------------------------------------------
def salvar_mongodb(dados: Dict[str, List[Dict]], subreddit: str, termo_busca: str) -> None:
    """
    Salva os documentos coletados no MongoDB, diferenciando:
      - post vs comentário -> campo `_tipo_documento` ("post" | "comentario")
      - candidato buscado  -> campo `_termo_busca` (ex: "Lula", "Bolsonaro")
      - subreddit de origem -> campo `_subreddit_busca`

    Índice único em (id, _tipo_documento, _termo_busca): um mesmo post pode
    aparecer 1x por termo de busca que ele menciona (ex: um post que fala de
    Lula E Bolsonaro gera 2 documentos, um para cada termo) -- isso é
    intencional, para facilitar contagem/análise por candidato depois.
    Reexecuções do mesmo dia/termo/subreddit não duplicam (idempotente).
    """
    documentos = dados.get("posts", []) + dados.get("comentarios", [])

    if not documentos:
        logger.warning("[r/%s | '%s'] Nenhum dado para salvar no MongoDB.", subreddit, termo_busca)
        return

    cliente = None
    try:
        cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        cliente.admin.command("ping")

        colecao = cliente[MONGO_DATABASE][MONGO_COLLECTION]
        colecao.create_index(
            [("id", 1), ("_tipo_documento", 1), ("_termo_busca", 1)],
            unique=True,
            background=True,
        )

        operacoes = [InsertOne(doc) for doc in documentos]

        try:
            resultado = colecao.bulk_write(operacoes, ordered=False)
            logger.info(
                "[r/%s | '%s'] Inseridos: %d novos documentos.",
                subreddit, termo_busca, resultado.inserted_count,
            )
        except BulkWriteError as bwe:
            inseridos = bwe.details.get("nInserted", 0)
            duplicados = sum(
                1 for e in bwe.details.get("writeErrors", []) if e.get("code") == 11000
            )
            logger.warning(
                "[r/%s | '%s'] Insercao parcial: %d inseridos, %d ja existiam.",
                subreddit, termo_busca, inseridos, duplicados,
            )

    except PyMongoError as erro:
        logger.error("[r/%s | '%s'] Erro ao conectar/inserir no MongoDB: %s", subreddit, termo_busca, erro)
        raise
    finally:
        if cliente is not None:
            cliente.close()

# tratamento dos termos de busca -----------------------------------------------------------------------------

def normalizar_expandir_termo(termo: str) -> list[str]:

    termo = termo.strip()
    
    # Remove acentos para criar a variação sem acento
    termo_sem_acento = ''.join(
        c for c in unicodedata.normalize('NFD', termo)
        if unicodedata.category(c) != 'Mn'
    )
    
    # Coleta as variações com acento e sem acento
    variacoes = {termo, termo_sem_acento}
    
    termos_finais = []
    for var in variacoes:
        # Se for nome composto com espaço adiciona aspas
        if " " in var and not (var.startswith('"') and var.endswith('"')):
            termos_finais.append(f'"{var}"')
        else:
            termos_finais.append(var)
            
    return termos_finais


# ----------------------------------------------------------------------------
# CLI / ORQUESTRAÇÃO
# ----------------------------------------------------------------------------
def _configurar_logger(subreddit: str, termo_busca: str) -> None:
    """
    Configura o logger para gerar um arquivo específico por combinação em logs/reddit_collector/
    e enviar as mesmas mensagens para o terminal (stdout), funcionando dinamicamente
    tanto localmente quanto dentro do container Docker/Airflow.
    """
    termo_slug = "".join(c if c.isalnum() else "_" for c in termo_busca.lower())
    sub_slug = "".join(c if c.isalnum() else "_" for c in subreddit.lower())

    # Resolve o caminho a partir da localização do próprio arquivo (pipelines/collectors/) -> sobe para a raiz do projeto
    raiz_projeto = Path(__file__).resolve().parent.parent.parent
    log_dir = raiz_projeto / "logs" / "reddit_collector"

    # Cria a estrutura de pastas sem caminhos absolutos como /home/labpi
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"reddit_{sub_slug}_{termo_slug}.log"

    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # File Handler
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Stream Handler (Terminal/Airflow)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logger.propagate = False

    # Garante o flush e encerramento limpo de todos os handlers de log ao finalizar o processo
    def fechar_logs():
        for handler in logger.handlers:
            handler.flush()
            handler.close()

    atexit.register(fechar_logs)


def main():
    parser = argparse.ArgumentParser(description="Coletor do Reddit (por subreddit + termo de busca)")
    parser.add_argument("--subreddit", type=str, required=True, help="Nome do subreddit (sem 'r/')")
    parser.add_argument("--termo-busca", type=str, required=True, help="Candidato/entidade a buscar (ex: 'Lula')")
    parser.add_argument("--data-inicio", type=str, required=True, help="Data de início (YYYY-MM-DD)")
    parser.add_argument("--data-fim", type=str, required=True, help="Data de fim (YYYY-MM-DD)")
    args = parser.parse_args()

    # Configura o log dinâmico criando o arquivo em logs/reddit/reddit_<subreddit>_<termo>.log
    _configurar_logger(args.subreddit, args.termo_busca)

    logger.info("=== Coletando r/%s para '%s' (%s a %s) ===",
                args.subreddit, args.termo_busca, args.data_inicio, args.data_fim)

    termo_ne = normalizar_expandir_termo(args.termo_busca)

    dados = {
        "posts": [],
        "comentarios": []
    }

    for termo in termo_ne:
        logger.info("Buscando variação: %s", termo)

        result = coletar_dados(
            subreddit=args.subreddit,
            termo_busca=termo,
            data_inicio=args.data_inicio,
            data_fim=args.data_fim,
        )

        dados["posts"].extend(result.get("posts", []))
        dados["comentarios"].extend(result.get("comentarios", []))

    for doc in dados["posts"] + dados["comentarios"]:
        doc["_termo_busca"] = args.termo_busca
    
    salvar_mongodb(dados, subreddit=args.subreddit, termo_busca=args.termo_busca)

    logger.info("=== Coletando r/%s para '%s' (%s a %s) ===\n\n",
                    args.subreddit, args.termo_busca, args.data_inicio, args.data_fim)


if __name__ == "__main__":
    main()