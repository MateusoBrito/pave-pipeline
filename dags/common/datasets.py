from airflow.datasets import Dataset

RAW_YOUTUBE = Dataset("mongo://raw/youtube")
RAW_REDDIT = Dataset("mongo://raw/reddit")
RAW_META = Dataset("mongo://raw/meta")
PREPROCESSED = Dataset("mongo://preprocessed")
POSTGRES_LOAD = Dataset("postgres://documentos")
TOPICOS_INFERIDOS = Dataset("postgres://documento_topico/inferido")