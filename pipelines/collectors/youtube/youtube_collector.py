from googleapiclient.discovery import build
import pandas as pd
import os

class YouTubeCollector:
    def __init__(self, api_key):
        self.youtube = build("youtube", "v3", developerKey=api_key)

    # Playlist de uploads do canal
    def obter_playlist_uploads(self, channel_id):
        resposta = self.youtube.channels().list(
            part="contentDetails",
            id=channel_id
        ).execute()

        items = resposta.get("items", [])

        if not items:
            raise ValueError(f"Canal com ID '{channel_id}' não foi encontrado.")

        return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # IDs dos vídeos do ano escolhido
    def obter_ids_videos(self, playlist_id, ano, videos_conhecidos):
        ids = []
        token = None
        pagina = 1

        while True:
            print(f"Vasculhando página {pagina}... ({len(ids)} vídeos na fila)", end="\r")
            resposta = self.youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=token
            ).execute()

            for item in resposta.get("items", []):
                video_id = item["snippet"]["resourceId"]["videoId"]
                
                if video_id in videos_conhecidos:
                    return ids

                published_at = item["snippet"]["publishedAt"]
                ano_video = int(published_at[:4])

                if ano_video < ano:
                    return ids
                
                if ano_video == ano:
                    ids.append(video_id)

            token = resposta.get("nextPageToken")

            if token is None:
                break

            pagina += 1

        print()
        return ids

    # Busca metadados em lotes de 50 vídeos
    def obter_metadados(self, video_ids):
        videos = []

        if not video_ids:
            return videos

        for i in range(0, len(video_ids), 50):
            lote = ",".join(video_ids[i:i+50])
            resposta = self.youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=lote
            ).execute()

            for video in resposta.get("items", []):
                snippet = video["snippet"]
                stats = video.get("statistics", {})
                details = video["contentDetails"]

                videos.append({
                    "videoId": video["id"],
                    "channelId": snippet["channelId"],
                    "channelTitle": snippet["channelTitle"],
                    "publishedAt": snippet["publishedAt"],
                    "title": snippet["title"],
                    "description": snippet["description"],
                    "tags": ",".join(snippet.get("tags", [])),
                    "categoryId": snippet["categoryId"],
                    "duration": details["duration"],
                    "viewCount": stats.get("viewCount"),
                    "likeCount": stats.get("likeCount"),
                    "commentCount": stats.get("commentCount")
                })

        return videos
    
    # Pipeline
    def coletar_e_salvar_videos(self, channel_ids, ano, arquivo_saida, videos_conhecidos=None):
        if videos_conhecidos is None:
            videos_conhecidos = set()

        os.makedirs(os.path.dirname(arquivo_saida), exist_ok=True)
        arquivo_existe = os.path.exists(arquivo_saida)

        for canal in channel_ids:
            print(f"Coletando canal: {canal}")
            
            playlist = self.obter_playlist_uploads(canal)
            ids_novos = self.obter_ids_videos(playlist, ano, videos_conhecidos)
            
            print(f"{len(ids_novos)} vídeos novos encontrados.")

            if not ids_novos:
                continue

            for i in range(0, len(ids_novos), 50):
                lote_ids = ids_novos[i:i+50]
                videos_lote = self.obter_metadados(lote_ids)
                
                df_lote = pd.DataFrame(videos_lote)
                
                df_lote.to_csv(
                    arquivo_saida,
                    mode='a', 
                    index=False,
                    header=not arquivo_existe, 
                    encoding="utf-8-sig"
                )
                
                arquivo_existe = True 
                videos_conhecidos.update(df_lote["videoId"].tolist())
                progresso = min(i + 50, len(ids_novos))    
                print(f"Lote salvo! Progresso: {progresso}/{len(ids_novos)} vídeos processados.")