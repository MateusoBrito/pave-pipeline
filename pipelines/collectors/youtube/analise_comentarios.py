import pandas as pd
from config import ARQUIVO_SAIDA_COMENTARIOS

print("Lendo o arquivo...\n")

try:
    df = pd.read_csv(ARQUIVO_SAIDA_COMENTARIOS, on_bad_lines='skip')
        
    # Quantidade de linhas (Total de comentários únicos)
    total_linhas = len(df)
    print(f"Total de comentários coletados: {total_linhas}")
    
    qtd_videos = df['videoId'].nunique()
    qtd_canais = df['channelTitle'].nunique()

    print(f"Vídeos únicos analisados: {qtd_videos}")
    print(f"Canais diferentes: {qtd_canais}")
    
    # Métricas de engajamento
    df['likeCount'] = pd.to_numeric(df['likeCount'], errors='coerce').fillna(0)
    df['replyCount'] = pd.to_numeric(df['replyCount'], errors='coerce').fillna(0)
    
    total_likes = int(df['likeCount'].sum())
    total_respostas = int(df['replyCount'].sum())
    
    print(f"\nENGAJAMENTO DOS ELEITORES\n")
    print(f"Total de curtidas dadas nos comentários: {total_likes:,}".replace(',', '.'))
    print(f"Total de respostas geradas (debates): {total_respostas:,}".replace(',', '.'))
    
    # Ranking rápido dos canais com mais comentários no arquivo
    print("\nTOP 5 CANAIS COM MAIS COMENTÁRIOS\n")
    print(f"{df['channelTitle'].value_counts().head(5).to_string()}\n")

except FileNotFoundError:
    print(f"Erro: O arquivo '{ARQUIVO_SAIDA_COMENTARIOS}' não foi encontrado")
except Exception as e:
    print(f"Ocorreu um erro ao ler o arquivo: {e}")