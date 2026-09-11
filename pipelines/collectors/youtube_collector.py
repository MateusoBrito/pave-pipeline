import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo import InsertOne, MongoClient
from pymongo.errors import BulkWriteError, PyMongoError
import atexit

load_dotenv()

# Logger global que será configurado dinamicamente dentro do main()
logger = logging.getLogger("youtube_collector")

# config banco =========================================================================================
MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "panorama")
MONGO_COLLECTION_VIDEOS = "youtube_videos"
MONGO_COLLECTION_COMMENTS = "youtube_comments"

if not MONGO_USER or not MONGO_PASSWORD:
    MONGO_URI = os.getenv("MONGO_URI", f"mongodb://{MONGO_HOST}:{MONGO_PORT}/")
else:
    MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"


# config yt =============================================================================================

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
if not YOUTUBE_API_KEY:
    raise SystemExit("Defina a variável de ambiente YOUTUBE_API_KEY no .env")


# Funcoes YAML ==========================================================================================

def buscar_canal_no_yaml(nome_ou_id: str) -> tuple[str, str]:
    """
    Carrega o entities.yaml e busca o CANAL DE NOTÍCIA (não mais o canal do próprio
    candidato - ver canais_noticia) por nome ou por channel_id.
    Retorna a tupla (nome_canal, channel_id).
    """
    diretorio = Path(__file__).resolve().parent
    yaml_path = None

    # Procura o arquivo entities.yaml subindo diretórios
    for _ in range(5):
        candidato = diretorio / "config" / "entities.yaml"
        if candidato.is_file():
            yaml_path = candidato
            break
        if diretorio.parent == diretorio:
            break
        diretorio = diretorio.parent

    if not yaml_path or not yaml_path.is_file():
        raise FileNotFoundError("Arquivo config/entities.yaml não foi encontrado.")

    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    canais = config.get("youtube", {}).get("canais_noticia", [])
    termo_busca = nome_ou_id.strip().lower()

    for canal in canais:
        nome = canal.get("nome", "")
        c_id = canal.get("channel_id", "")

        if nome.lower() == termo_busca or c_id.lower() == termo_busca:
            return nome, c_id

    # Se não encontrar no YAML, assume que a própria string informada é o Channel ID
    return nome_ou_id, nome_ou_id


# Funcoes YT ============================================================================================

def build_youtube_client():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)


def _janela_str(data_inicio: str, data_fim: str) -> tuple[str, str]:
    """Converte YYYY-MM-DD em strings ISO8601 UTC ("...T00:00:00Z"/"...T23:59:59Z")
    comparáveis lexicograficamente com o publishedAt que a API devolve (mesmo formato,
    mesmo fuso) - é assim que buscar_comentarios_por_canal corta a paginação por data,
    já que commentThreads().list não tem publishedAfter/publishedBefore."""
    dt_inicio = datetime.strptime(data_inicio, "%Y-%m-%d")
    dt_fim = datetime.strptime(data_fim, "%Y-%m-%d")
    return dt_inicio.strftime("%Y-%m-%dT00:00:00Z"), dt_fim.strftime("%Y-%m-%dT23:59:59Z")


def buscar_comentarios_por_canal(youtube, channel_id, termo_busca, data_inicio, data_fim,
                                  max_paginas=5000):
    """Busca comentários do CANAL INTEIRO (todos os vídeos, sem precisar enumerá-los
    antes) que mencionem termo_busca, via commentThreads().list com
    allThreadsRelatedToChannelId+searchTerms - 1 unidade de cota por página (100
    comentários), não as 100 unidades/página do antigo search.list usado para achar
    vídeo por vídeo (esse método antigo esgotava a cota diária em 1-2 combinações
    canal/candidato; este resolve o mesmo problema por ~1/100 do custo).

    Sem filtro de data nativo nesse endpoint - pagina do mais recente pro mais antigo
    (order="time") e corta manualmente pelo publishedAt de cada comentário: ignora o
    que é mais novo que data_fim (ainda paginando) e para de vez ao cruzar data_inicio
    (dali pra trás só tem comentário fora da janela). Isso também é o que faz vídeos
    ANTIGOS (de fora da janela do candidato/rede na coleta original) continuarem
    ganhando comentários novos "de graça": não há mais uma lista fixa de vídeos
    revisitados - toda rodada revarre o canal inteiro de novo.
    """
    inicio_str, fim_str = _janela_str(data_inicio, data_fim)
    comentarios = []
    token = None
    paginas = 0

    while paginas < max_paginas:
        try:
            res = youtube.commentThreads().list(
                part="snippet",
                allThreadsRelatedToChannelId=channel_id,
                searchTerms=termo_busca,
                order="time",
                maxResults=100,
                pageToken=token,
                textFormat="plainText",
            ).execute()
        except HttpError as e:
            logger.error(f"Falha ao buscar comentários de '{termo_busca}' no canal {channel_id}: {e}")
            if e.resp.status in (403, 429):
                logger.critical("Cota da API do YouTube excedida. Interrompendo execução para retry no Airflow.\n\n")
                sys.exit(1)
            break

        paginas += 1
        cruzou_inicio = False
        for item in res.get("items", []):
            top = item["snippet"]["topLevelComment"]["snippet"]
            publicado_em = top["publishedAt"]

            if publicado_em > fim_str:
                continue  # mais novo que a janela pedida - ignora mas segue paginando
            if publicado_em < inicio_str:
                cruzou_inicio = True
                break  # dali pra trás só tem comentário mais antigo que o início - para

            comentarios.append({
                "commentId": item["id"],
                "videoId": item["snippet"].get("videoId"),
                "text": top["textOriginal"],
                "likeCount": int(top.get("likeCount", 0)),
                "replyCount": int(item["snippet"].get("totalReplyCount", 0)),
                "publishedAt": publicado_em,
                # Id estável (não o nome) de quem comentou - platform-only, sem PII
                # real, igual ao que o resto do painel já promete (ver CommentsPanel
                # no front: "autores aparecem sem identificação, só o identificador
                # da plataforma"). Usado só pra não contar a mesma pessoa 2x, não pra
                # expor quem é.
                "authorChannelId": top.get("authorChannelId", {}).get("value"),
                "_entidade_busca": termo_busca,
            })

        if cruzou_inicio:
            break
        token = res.get("nextPageToken")
        if not token:
            break

    return comentarios


def buscar_detalhes_videos(youtube, video_ids, termo_busca):
    """videos().list em lotes de até 50 ids - 1 unidade/chamada. Só é chamado para os
    videoIds que apareceram nos comentários encontrados (não mais para todo vídeo do
    canal), mas roda de novo a cada execução mesmo pra vídeo já visto antes: é barato,
    e salvar_mongodb (InsertOne + índice único) já ignora o que já existe."""
    ids_unicos = list(dict.fromkeys(v for v in video_ids if v))
    videos_coletados = []

    for i in range(0, len(ids_unicos), 50):
        lote = ids_unicos[i:i + 50]
        try:
            video_res = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(lote)
            ).execute()
        except HttpError as e:
            logger.error(f"Falha ao buscar detalhes de vídeos: {e}")
            continue

        for v in video_res.get("items", []):
            snippet = v["snippet"]
            stats = v.get("statistics", {})
            details = v["contentDetails"]

            duracao_iso = details.get("duration", "PT0S")
            is_short = ("M" not in duracao_iso and "H" not in duracao_iso)

            videos_coletados.append({
                "videoId": v["id"],
                "channelId": snippet["channelId"],
                "channelTitle": snippet["channelTitle"],
                "publishedAt": snippet["publishedAt"],
                "title": snippet["title"],
                "description": snippet["description"],
                "tags": ",".join(snippet.get("tags", [])),
                "categoryId": snippet["categoryId"],
                "duration": details["duration"],
                "is_short": is_short,
                "viewCount": int(stats.get("viewCount", 0)),
                "likeCount": int(stats.get("likeCount", 0)),
                "commentCount": int(stats.get("commentCount", 0)),
                "_entidade_busca": termo_busca,
            })

    return videos_coletados


# Funcoes banco ==============================================================================================

def salvar_mongodb(dados, colecao_nome, id_field):
    if not dados:
        return

    try:
        cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        cliente.admin.command("ping")
        colecao = cliente[MONGO_DATABASE][colecao_nome]
        colecao.create_index(id_field, unique=True, background=True)

        operacoes = [InsertOne(doc) for doc in dados]

        try:
            resultado = colecao.bulk_write(operacoes, ordered=False)
            logger.info(f"[{colecao_nome}] Inseridos: {resultado.inserted_count} novos documentos.")
        except BulkWriteError as bwe:
            inseridos = bwe.details.get("nInserted", 0)
            duplicados = sum(1 for e in bwe.details.get("writeErrors", []) if e.get("code") == 11000)
            logger.info(f"[{colecao_nome}] Inserção parcial: {inseridos} inseridos, {duplicados} já existiam.")

    except PyMongoError as erro:
        logger.error(f"Erro MongoDB na coleção {colecao_nome}: {erro}")
        raise
    finally:
        if 'cliente' in locals():
            cliente.close()


# Logger Config ==============================================================================================

def _configurar_logger(nome_canal: str) -> None:
    canal_slug = "".join(c if c.isalnum() else "_" for c in nome_canal.lower())

    raiz_projeto = Path(__file__).resolve().parent.parent.parent
    log_dir = raiz_projeto / "logs" / "youtube_collector"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"youtube_{canal_slug}.log"

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

    # CORREÇÃO CRÍTICA: Garante que os logs sejam descarregados no disco ao encerrar o script
    def fechar_logs():
        for handler in logger.handlers:
            handler.flush()
            handler.close()

    atexit.register(fechar_logs)


# ==================================================================================================================

def main():

    parser = argparse.ArgumentParser(description="Coletor do YouTube (canal de notícia x candidato, via entities.yaml)")
    parser.add_argument("--canal", type=str, required=True, help="Nome do canal de notícia ou Channel ID (ex: 'CNN Brasil')")
    parser.add_argument("--termo-busca", type=str, required=True, help="Candidato a buscar dentro do canal (ex: 'Lula')")
    parser.add_argument("--data-inicio", type=str, required=True, help="Data inicio")
    parser.add_argument("--data-fim", type=str, required=True, help="Data fim")
    args = parser.parse_args()

    termo_busca = args.termo_busca
    data_inicio = args.data_inicio[:10]
    data_fim = args.data_fim[:10]

    # Mapeia o argumento recebido para o nome oficial e channel_id cadastrados no entities.yaml
    nome_canal, channel_id = buscar_canal_no_yaml(args.canal)

    # Configura o arquivo de log baseado no nome do canal + candidato (uma combinação
    # por task, igual ao Reddit)
    _configurar_logger(f"{nome_canal}_{termo_busca}")

    logger.info(
        f"=== Iniciando Coleta YouTube | Canal: '{nome_canal}' (ID: {channel_id}) | "
        f"Candidato: '{termo_busca}' | Período: {data_inicio} a {data_fim} ==="
    )

    youtube = build_youtube_client()

    logger.info(f"Buscando comentários sobre '{termo_busca}' em todo o canal '{nome_canal}' "
                f"({data_inicio} a {data_fim})...")
    comentarios = buscar_comentarios_por_canal(youtube, channel_id, termo_busca, data_inicio, data_fim)
    logger.info(f"{len(comentarios)} comentários encontrados na janela.")

    video_ids = [c["videoId"] for c in comentarios if c.get("videoId")]
    videos = buscar_detalhes_videos(youtube, video_ids, termo_busca) if video_ids else []
    if videos:
        salvar_mongodb(videos, MONGO_COLLECTION_VIDEOS, "videoId")
    else:
        logger.info("Nenhum vídeo associado aos comentários encontrados.")

    if comentarios:
        salvar_mongodb(comentarios, MONGO_COLLECTION_COMMENTS, "commentId")
    else:
        logger.info("Nenhum comentário encontrado no período.")

    logger.info(
        f"=== Fim da coleta '{nome_canal}' / '{termo_busca}'. "
        f"{len(comentarios)} comentários, {len(videos)} vídeos. ===\n\n"
    )


if __name__ == "__main__":
    main()