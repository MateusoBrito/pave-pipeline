import os
import pandas as pd
from youtube_collector import YouTubeCollector
from shorts_checker import atualizar_classificacao_shorts
from config import (
    YOUTUBE_API_KEY,
    CHANNEL_IDS,
    ANO,
    ARQUIVO_SAIDA_VIDEOS
)

def executar_coleta_videos():
    # Verifica os vídeos que já foram coletados
    videos_conhecidos = set()

    if os.path.exists(ARQUIVO_SAIDA_VIDEOS):
        df_antigo = pd.read_csv(ARQUIVO_SAIDA_VIDEOS)

        if "videoId" in df_antigo.columns:
            videos_conhecidos = set(df_antigo["videoId"].tolist())

        print(f"{len(videos_conhecidos)} vídeos já conhecidos.")
    else:
        print("Nenhuma base anterior encontrada. Iniciando coleta do zero.")

    # Inicia o coletor e a coleta
    collector = YouTubeCollector(YOUTUBE_API_KEY)

    collector.coletar_e_salvar_videos(
        channel_ids=CHANNEL_IDS, 
        ano=ANO, 
        arquivo_saida=ARQUIVO_SAIDA_VIDEOS, 
        videos_conhecidos=videos_conhecidos
    )

    # Classifica como Shorts
    atualizar_classificacao_shorts(ARQUIVO_SAIDA_VIDEOS)

    print("\nProcesso finalizado com sucesso!")

if __name__ == "__main__":
    executar_coleta_videos()