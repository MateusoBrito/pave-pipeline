import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
from config import (ARQUIVO_SAIDA_VIDEOS, ARQUIVO_SAIDA_COMENTARIOS)

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("POSTGRES_DB")

def migrar_para_mongo():
    print("Conectando ao MongoDB")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    colecao_videos = db["raw_youtube_videos"]
    colecao_comentarios = db["raw_youtube_comments"]
    
    # Vídeos
    if os.path.exists(ARQUIVO_SAIDA_VIDEOS):
        print("\nLendo CSV de vídeos")
        df_videos = pd.read_csv(ARQUIVO_SAIDA_VIDEOS)
        docs_videos = df_videos.to_dict(orient="records")
        
        if docs_videos:
            print(f"Enviando {len(docs_videos)} vídeos para a coleção 'raw_youtube_videos'")
            colecao_videos.insert_many(docs_videos)
            print("Vídeos salvos!")
    else:
        print(f"\nArquivo {ARQUIVO_SAIDA_VIDEOS} não encontrado")

    # Comentários
    if os.path.exists(ARQUIVO_SAIDA_COMENTARIOS):
        print("\nLendo CSV de comentários")
        df_comentarios = pd.read_csv(ARQUIVO_SAIDA_COMENTARIOS, escapechar='\\')
        docs_comentarios = df_comentarios.to_dict(orient="records")
        
        if docs_comentarios:
            print(f"Enviando {len(docs_comentarios)} comentários para a coleção 'raw_youtube_comments'")
            colecao_comentarios.insert_many(docs_comentarios)
            print("Comentários salvos!")
    else:
        print(f"\nArquivo {ARQUIVO_SAIDA_COMENTARIOS} não encontrado")

if __name__ == "__main__":
    migrar_para_mongo()