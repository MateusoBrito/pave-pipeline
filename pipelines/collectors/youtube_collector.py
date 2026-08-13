import argparse
import os
import logging
from datetime import datetime
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError, PyMongoError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("coleta_youtube")

MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DATABASE = os.getenv("POSTGRES_DB", "panorama")
MONGO_COLLECTION_VIDEOS = "youtube_videos"
MONGO_COLLECTION_COMMENTS = "youtube_comments"

if not MONGO_USER or not MONGO_PASSWORD:
    MONGO_URI = os.getenv("MONGO_URI", f"mongodb://{MONGO_HOST}:{MONGO_PORT}/")
else:
    MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
if not YOUTUBE_API_KEY:
    raise SystemExit("Defina a variável de ambiente YOUTUBE_API_KEY no .env")

def build_youtube_client():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

def coletar_videos(youtube, channel_id, data_inicio, data_fim):
    try:
        res = youtube.channels().list(part="contentDetails", id=channel_id).execute()
        items = res.get("items", [])
        if not items:
            logger.error(f"Canal {channel_id} não encontrado.")
            return []
        uploads_playlist = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
    except HttpError as e:
        logger.error(f"Erro ao buscar canal {channel_id}: {e}")
        return []

    videos_coletados = []
    token = None
    while True:
        try:
            res = youtube.playlistItems().list(
                part="snippet",
                playlistId=uploads_playlist,
                maxResults=50,
                pageToken=token
            ).execute()

            for item in res.get("items", []):
                published_at = item["snippet"]["publishedAt"]
                data_publicacao = published_at[:10]
                
                if data_publicacao > data_fim:
                    continue
                
                if data_publicacao < data_inicio:
                    return videos_coletados
                
                video_id = item["snippet"]["resourceId"]["videoId"]
                video_res = youtube.videos().list(
                    part="snippet,statistics,contentDetails",
                    id=video_id
                ).execute()
                
                for v in video_res.get("items", []):
                    snippet = v["snippet"]
                    stats = v.get("statistics", {})
                    details = v["contentDetails"]
                    
                    duracao_iso = details.get("duration", "PT0S")
                    is_short = True if ("M" not in duracao_iso and "H" not in duracao_iso) else False
                    
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
                        "_entidade_busca": channel_id
                    })

            token = res.get("nextPageToken")
            if not token:
                break
        except HttpError as e:
            logger.error(f"Erro ao paginar vídeos: {e}")
            break

    return videos_coletados

def coletar_comentarios(youtube, video_id, channel_title, limite_paginas=30):
    comentarios = []
    token = None
    paginas = 0

    while paginas < limite_paginas:
        try:
            req = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                pageToken=token,
                textFormat="plainText"
            )
            res = req.execute()

            for item in res.get("items", []):
                top = item["snippet"]["topLevelComment"]["snippet"]
                comentarios.append({
                    "commentId": item["id"],
                    "videoId": video_id,
                    "channelTitle": channel_title,
                    "text": top["textOriginal"],
                    "likeCount": int(top.get("likeCount", 0)),
                    "replyCount": int(item["snippet"].get("totalReplyCount", 0)),
                    "publishedAt": top["publishedAt"]
                })

            token = res.get("nextPageToken")
            paginas += 1
            if not token:
                break
        except HttpError as e:
            if e.resp.status == 403:
                logger.warning(f"Comentários desativados para o vídeo {video_id}")
            else:
                logger.error(f"Erro ao buscar comentários do vídeo {video_id}: {e}")
            break
        except Exception as e:
            logger.error(f"Erro inesperado ao buscar comentários: {e}")
            break

    return comentarios

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

def main():
    parser = argparse.ArgumentParser(description="Coletor do YouTube")
    parser.add_argument("--entidade", type=str, required=True, help="Channel ID")
    parser.add_argument("--data-inicio", type=str, required=True, help="Data inicio")
    parser.add_argument("--data-fim", type=str, required=True, help="Data fim")
    args = parser.parse_args()

    logger.info(f"Coleta YouTube canal {args.entidade} de {args.data_inicio} a {args.data_fim}")
    youtube = build_youtube_client()
    
    videos = coletar_videos(youtube, args.entidade, args.data_inicio, args.data_fim)
    if videos:
        salvar_mongodb(videos, MONGO_COLLECTION_VIDEOS, "videoId")
    else:
        logger.info(f"Nenhum vídeo no período.")

    total_comentarios = 0
    for v in videos:
        logger.info(f"Buscando comentários do vídeo {v['videoId']}...")
        comentarios = coletar_comentarios(youtube, v["videoId"], v["channelTitle"])
        if comentarios:
            salvar_mongodb(comentarios, MONGO_COLLECTION_COMMENTS, "commentId")
            total_comentarios += len(comentarios)
            
    logger.info(f"Fim da coleta. {total_comentarios} comentários inseridos.")

if __name__ == "__main__":
    main()
