import pandas as pd
from config import ARQUIVO_SAIDA_VIDEOS

df = pd.read_csv(ARQUIVO_SAIDA_VIDEOS)

# Se likes ou comentários estiverem desativados, viram NaN  e não quebram o cálculo
colunas_metricas = ['viewCount', 'likeCount', 'commentCount']

for col in colunas_metricas:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Contagem de Shorts e normais

print("\nDISTRIBUIÇÃO DE FORMATOS\n")

if 'isShort' in df.columns:
    df['isShort'] = df['isShort'].fillna("Não Classificado")    
    distribuicao = pd.crosstab(df['channelTitle'], df['isShort'])
    distribuicao = distribuicao.rename(columns={True: "Shorts", False: "Vídeos Normais"})
    print(f"{distribuicao.to_string()}\n")
else:
    print("A coluna 'isShort' não foi encontrada. Execute o script de classificação primeiro.\n")

# Métricas de engajamento

titulos = {
    'viewCount': 'VISUALIZAÇÕES',
    'likeCount': 'CURTIDAS',
    'commentCount': 'COMENTÁRIOS'
}

nomes_agregacoes = {
    'count': 'Qtd_Videos_Abertos', # Quantos vídeos não estão com a métrica ocultada/desativada
    'sum': 'Total',
    'mean': 'Média',
    'median': 'Mediana',
    'min': 'Mínimo',
    'max': 'Máximo'
}

# Imprime tabela por tabela
for coluna, titulo in titulos.items():
    print(f"MÉTRICAS DE {titulo}\n")
    
    resumo = df.groupby('channelTitle')[coluna].agg(
        ['count', 'sum', 'mean', 'median', 'min', 'max']
    ).round(0)
    
    resumo = resumo.rename(columns=nomes_agregacoes)
    
    print(f"{resumo.to_string()}\n")