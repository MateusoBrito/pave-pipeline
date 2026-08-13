import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor

def verificar_se_e_short(video_id):
    url = f"https://www.youtube.com/shorts/{video_id}"

    try:
        resposta = requests.head(url, allow_redirects=False, timeout=5)
        return resposta.status_code == 200
    except requests.RequestException:
        return None

def atualizar_classificacao_shorts(arquivo_csv):
    try:
        df = pd.read_csv(arquivo_csv)
    except FileNotFoundError:
        print("Arquivo CSV não encontrado para classificar Shorts.")
        return

    if 'isShort' not in df.columns:
        df['isShort'] = pd.NA

    mascara_vazios = df['isShort'].isna()
    videos_para_checar = df.loc[mascara_vazios, 'videoId'].tolist()

    if not videos_para_checar:
        print("Nenhum vídeo novo para classificar como Short.")
        return

    print(f"Classificando {len(videos_para_checar)} vídeos...")

    with ThreadPoolExecutor(max_workers=10) as executor:
        resultados = list(executor.map(verificar_se_e_short, videos_para_checar))

    df.loc[mascara_vazios, 'isShort'] = resultados  
    df.to_csv(arquivo_csv, index=False, encoding="utf-8-sig")

    qtd_shorts = sum([r for r in resultados if r is True])    
    qtd_erros = resultados.count(None)

    print(f"Classificação concluída! {qtd_shorts} Shorts identificados.")

    if qtd_erros > 0:
        print(f"Falha de conexão em {qtd_erros} vídeos. Eles tentarão novamente na próxima execução.")