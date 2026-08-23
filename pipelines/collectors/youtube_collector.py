import argparse
import os
from datetime import datetime
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError, PyMongoError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from collections import defaultdict

import pandas as pd

MONGO_USER = os.getenv("MONGO_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DATABASE = os.getenv("POSTGRES_DB", "panorama")
MONGO_COLLECTION_VIDEOS = "youtube_videos_teste"
MONGO_COLLECTION_COMMENTS = "youtube_comments_teste"

if not MONGO_USER or not MONGO_PASSWORD:
    MONGO_URI = os.getenv("MONGO_URI", f"mongodb://{MONGO_HOST}:{MONGO_PORT}/")
else:
    MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
if not YOUTUBE_API_KEY:
    raise SystemExit("[ERRO CRÍTICO] Defina a variável de ambiente YOUTUBE_API_KEY no .env")


# Funcoes YT ============================================================================================

def build_youtube_client():
    print("[API] Conectando à API do YouTube...")
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)


def coletar_videos(youtube, channel_id, data_inicio, data_fim):
    print(f"\n--- [ETAPA] Coletando vídeos do canal: {channel_id} ---")
    try:
        res = youtube.channels().list(part="contentDetails", id=channel_id).execute()
        items = res.get("items", [])
        if not items:
            print(f"[ERRO] Canal {channel_id} não encontrado na API do YouTube.")
            return []
        uploads_playlist = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
        print(f"[API] Playlist de Uploads identificada: {uploads_playlist}")
    except HttpError as e:
        print(f"[ERRO] Falha ao buscar informações do canal {channel_id}: {e}")
        return []

    dt_inicio = datetime.strptime(data_inicio, "%Y-%m-%d").date()
    dt_fim = datetime.strptime(data_fim, "%Y-%m-%d").date()

    videos_coletados = []
    token = None
    pagina = 1

    while True:
        try:
            print(f"[API] Buscando página {pagina} de uploads...")
            res = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=uploads_playlist,
                maxResults=50,
                pageToken=token
            ).execute()

            video_ids_da_pagina = []
            atingiu_limite_antigo = False

            for item in res.get("items", []):
                raw_date = item["contentDetails"]["videoPublishedAt"]
                data_publicacao = datetime.strptime(raw_date[:10], "%Y-%m-%d").date()

                if data_publicacao > dt_fim:
                    continue

                if data_publicacao < dt_inicio:
                    atingiu_limite_antigo = True
                    break

                video_ids_da_pagina.append(item["contentDetails"]["videoId"])

            if atingiu_limite_antigo:
                print(f"[FILTRO] Alcançou vídeos anteriores à data de início ({dt_inicio}). Parando paginação.")

            if video_ids_da_pagina:
                print(f"[API] Coletando detalhes de {len(video_ids_da_pagina)} vídeos desta página...")
                video_res = youtube.videos().list(
                    part="snippet,statistics,contentDetails",
                    id=",".join(video_ids_da_pagina)
                ).execute()

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
                        "_entidade_busca": channel_id
                    })

            token = res.get("nextPageToken")
            pagina += 1
            if atingiu_limite_antigo or not token:
                break

        except HttpError as e:
            print(f"[ERRO] Falha ao paginar vídeos: {e}")
            break

    print(f"[RESULTADO] Total de vídeos encontrados no intervalo ({data_inicio} a {data_fim}): {len(videos_coletados)}")
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
                print(f"[AVISO] Comentários desativados para o vídeo {video_id}")
            elif e.resp.status == 404:
                print(f"[AVISO] Vídeo {video_id} não encontrado ou removido.")
            else:
                print(f"[ERRO] Falha ao buscar comentários do vídeo {video_id}: {e}")
            break
        except Exception as e:
            print(f"[ERRO] Falha inesperada ao buscar comentários do vídeo {video_id}: {e}")
            break

    return comentarios


def atualizar_comentarios(youtube, video_id, comentsUsed, channel_title, limite_paginas=30):
    comentarios = []
    token = None
    paginas = 0

    comentsUsed_set = set(comentsUsed)

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
                comment_id = item["id"]

                if comment_id in comentsUsed_set:
                    continue

                top = item["snippet"]["topLevelComment"]["snippet"]
                comentarios.append({
                    "commentId": comment_id,
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
                print(f"[AVISO] Comentários desativados para o vídeo {video_id}")
            elif e.resp.status == 404:
                print(f"[AVISO] Vídeo {video_id} não encontrado ou removido.")
            else:
                print(f"[ERRO] Falha ao atualizar comentários do vídeo {video_id}: {e}")
            break
        except Exception as e:
            print(f"[ERRO] Falha inesperada ao atualizar comentários do vídeo {video_id}: {e}")
            break

    return comentarios


# Funcoes banco ==============================================================================================

def salvar_mongodb(dados, colecao_nome, id_field):
    if not dados:
        print(f"[{colecao_nome}] Nenhum dado fornecido para inserção.")
        return

    print(f"\n[BANCO] Conectando ao MongoDB para salvar na coleção '{colecao_nome}'...")
    try:
        cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        cliente.admin.command("ping")
        colecao = cliente[MONGO_DATABASE][colecao_nome]
        colecao.create_index(id_field, unique=True, background=True)

        operacoes = [InsertOne(doc) for doc in dados]
        
        try:
            resultado = colecao.bulk_write(operacoes, ordered=False)
            print(f"[{colecao_nome}] SUCESSO: {resultado.inserted_count} novos documentos inseridos.")
        except BulkWriteError as bwe:
            inseridos = bwe.details.get("nInserted", 0)
            duplicados = sum(1 for e in bwe.details.get("writeErrors", []) if e.get("code") == 11000)
            print(f"[{colecao_nome}] INSERÇÃO PARCIAL: {inseridos} novos inseridos, {duplicados} ignorados (já existiam no banco).")

    except PyMongoError as erro:
        print(f"[ERRO CRÍTICO] Erro de conexão/escrita no MongoDB ({colecao_nome}): {erro}")
        raise
    finally:
        if 'cliente' in locals():
            cliente.close()


def buscar_ids_comentarios():
    print("\n--- [ETAPA] Consultando comentários já existentes no MongoDB ---")
    try:
        cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        cliente.admin.command("ping")
        colecao = cliente[MONGO_DATABASE][MONGO_COLLECTION_COMMENTS]

        projecao = {
            "_id": 0,
            "commentId": 1,
            "videoId": 1,
            "channelTitle": 1
        }

        cursor = colecao.find({}, projecao)
        resultados = list(cursor)

        print(f"[{MONGO_COLLECTION_COMMENTS}] Total de registros já no banco: {len(resultados)}")
        return resultados

    except PyMongoError as erro:
        print(f"[ERRO] Falha ao consultar o MongoDB: {erro}")
        return []
    finally:
        if 'cliente' in locals():
            cliente.close()


# ==================================================================================================================

def main():
    print("=" * 80)
    print("               INICIANDO SCRIPT DE COLETA YOUTUBE PAVE")
    print("=" * 80)

    parser = argparse.ArgumentParser(description="Coletor do YouTube")
    parser.add_argument("--entidade", type=str, required=True, help="Channel ID")
    parser.add_argument("--data-inicio", type=str, required=True, help="Data inicio")
    parser.add_argument("--data-fim", type=str, required=True, help="Data fim")
    args = parser.parse_args()

    data_inicio = args.data_inicio[:10]
    data_fim = args.data_fim[:10]

    print(f"[PARÂMETROS] Canal ID: {args.entidade} | Período: {data_inicio} até {data_fim}")

    youtube = build_youtube_client()

    # --- PARTE 1: ATUALIZAÇÃO DE COMENTÁRIOS DE VÍDEOS JÁ EXISTENTES ---
    resultados = buscar_ids_comentarios()

    comentarios_por_video = defaultdict(list)
    for r in resultados:
        comentarios_por_video[r["videoId"]].append(r["commentId"])

    videos_para_atualizar = list(comentarios_por_video.keys())
    total_videos_existentes = len(videos_para_atualizar)

    print(f"\n--- [ETAPA] Atualizando comentários recentes para {total_videos_existentes} vídeos conhecidos ---")

    visited = set()
    novos_comentarios = []
    total_novos = 0

    for idx, v in enumerate(resultados, 1):
        video_id = v["videoId"]

        if video_id not in visited:
            visited.add(video_id)
            comentarios_antigos = comentarios_por_video.get(video_id, [])
            channel_title = v["channelTitle"]

            print(f"[{len(visited)}/{total_videos_existentes}] Checando novos comentários no vídeo {video_id} ({channel_title})...", end="\r")

            novos = atualizar_comentarios(youtube, video_id, comentarios_antigos, channel_title)

            if novos:
                print(f"\n -> {len(novos)} novos comentários encontrados no vídeo {video_id}!")
                novos_comentarios.extend(novos)
                total_novos += len(novos)

    print(f"\n[RESUMO ATUALIZAÇÃO] Total de novos comentários encontrados em vídeos existentes: {total_novos}")

    if novos_comentarios:
        salvar_mongodb(novos_comentarios, MONGO_COLLECTION_COMMENTS, "commentId")


    # --- PARTE 2: COLETA DE NOVOS VÍDEOS DO CANAL NO PERÍODO ---
    videos = coletar_videos(youtube, args.entidade, data_inicio, data_fim)
    
    if videos:
        salvar_mongodb(videos, MONGO_COLLECTION_VIDEOS, "videoId")

        # --- PARTE 3: COLETA DE COMENTÁRIOS DOS NOVOS VÍDEOS ---
        print(f"\n--- [ETAPA] Coletando comentários dos {len(videos)} vídeos recém-encontrados ---")
        total_comentarios = 0
        todos_comentarios = []

        for idx, v in enumerate(videos, 1):
            print(f"[{idx}/{len(videos)}] Coletando comentários do vídeo: {v['title'][:40]}... (ID: {v['videoId']})")
            comentarios = coletar_comentarios(youtube, v["videoId"], v["channelTitle"])
            
            if comentarios:
                print(f"   └─ {len(comentarios)} comentários obtidos.")
                todos_comentarios.extend(comentarios)
                total_comentarios += len(comentarios)
            else:
                print("   └─ Nenhum comentário retornado.")

        if todos_comentarios:
            salvar_mongodb(todos_comentarios, MONGO_COLLECTION_COMMENTS, "commentId")
        else:
            print("[AVISO] Nenhum comentário coletado para os novos vídeos.")

    else:
        print("\n[AVISO] Nenhum vídeo novo encontrado para o canal no período informado.")

    print("\n" + "=" * 80)
    print("                    PROCESSAMENTO FINALIZADO COM SUCESSO!")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    main()