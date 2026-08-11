import os
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from config import (
    YOUTUBE_API_KEY,
    ARQUIVO_SAIDA_COMENTARIOS,
    ARQUIVO_SAIDA_VIDEOS,
    LIMITE_COMENTARIOS_VIDEO
)

def iniciar_cliente_youtube():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

def coletar_comentarios_video(youtube, video_id, channel_title):
    comentarios = []
    next_page_token = None
    paginas_coletadas = 0
    max_paginas = LIMITE_COMENTARIOS_VIDEO // 100
    
    while paginas_coletadas < max_paginas:
        try:
            # Faz a requisição para a API
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100,
                pageToken=next_page_token,
                textFormat="plainText" # Garante que o texto venha sem tags HTML
            )
            response = request.execute()
            
            for item in response.get("items", []):
                top_level = item["snippet"]["topLevelComment"]["snippet"]
                
                comentarios.append({
                    "commentId": item["id"],
                    "videoId": video_id,
                    "channelTitle": channel_title,
                    "text": top_level["textOriginal"],
                    "likeCount": top_level.get("likeCount", 0),
                    "replyCount": item["snippet"].get("totalReplyCount", 0),
                    "publishedAt": top_level["publishedAt"]
                })
            
            # Verifica se há mais páginas
            next_page_token = response.get("nextPageToken")
            paginas_coletadas += 1
            
            if not next_page_token:
                break 
                
        except HttpError as e:
            if e.resp.status == 403:
                print(f"      -> Comentários desativados para o vídeo {video_id}")
            else:
                print(f"      -> Erro na API ao acessar o vídeo {video_id}: {e}")
            break
        except Exception as e:
            print(f"      -> Erro inesperado no vídeo {video_id}: {e}")
            break
            
    return comentarios

def executar_coleta_comentarios():
    print("Iniciando coleta de comentários.")
    
    youtube = iniciar_cliente_youtube()
    df_videos = pd.read_csv(ARQUIVO_SAIDA_VIDEOS)
    todos_comentarios_coletados = []
    total_videos = len(df_videos)
    
    for index, row in df_videos.iterrows():
        video_id = row['videoId']
        channel_title = row['channelTitle']
        
        print(f"[{index + 1}/{total_videos}] Coletando: {channel_title} - {video_id}")
        
        comentarios_video = coletar_comentarios_video(youtube, video_id, channel_title)
        todos_comentarios_coletados.extend(comentarios_video)
        
    if not todos_comentarios_coletados:
        print("\nNenhum comentário pôde ser coletado.")
        return
        
    df_novos = pd.DataFrame(todos_comentarios_coletados)
    
    # Carrega o CSV antigo se existir e junta com o novo
    if os.path.exists(ARQUIVO_SAIDA_COMENTARIOS):
        df_antigo = pd.read_csv(ARQUIVO_SAIDA_COMENTARIOS)
        df_final = pd.concat([df_antigo, df_novos], ignore_index=True)
    else:
        df_final = df_novos
        
    # Remove comentários duplicados (mantendo a versão mais recente)
    df_final = df_final.drop_duplicates(subset=['commentId'], keep='last')
        
    # Troca as quebras de linha por espaços vazios para não quebrar a estrutura do CSV
    df_final['text'] = df_final['text'].astype(str).str.replace('\n', ' ').str.replace('\r', '')
    
    # Salva usando o escapechar='\\' para proteger aspas e vírgulas no meio das frases
    df_final.to_csv(ARQUIVO_SAIDA_COMENTARIOS, index=False, encoding="utf-8-sig", escapechar='\\')
    
    print(f"\nColeta finalizada! {len(df_final)} comentários únicos salvos com segurança em '{ARQUIVO_SAIDA_COMENTARIOS}'.")

if __name__ == "__main__":
    executar_coleta_comentarios()