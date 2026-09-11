from airflow.datasets import Dataset

RAW_YOUTUBE   = Dataset("mongo://raw/youtube")
RAW_REDDIT    = Dataset("mongo://raw/reddit")
RAW_META      = Dataset("mongo://raw/meta")
PREPROCESSED  = Dataset("mongo://preprocessed")
POSTGRES_LOAD = Dataset("postgres://documentos")
# dag_inferencia.py marca isso depois de rodar o "transform" de tópico em todos os
# candidatos/redes de um ciclo - dag_sentimentos.py roda em cima disso (não num cron
# próprio), porque a classificação de sentimento deve vir DEPOIS da atribuição de
# tópico, não em paralelo/antes.
TOPICOS_INFERIDOS = Dataset("postgres://documento_topico/inferido")