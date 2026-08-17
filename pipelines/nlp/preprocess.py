import argparse
import sys
import logging
import os
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
from pymongo import MongoClient

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.preprocessing import TextPreprocessor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Pré-processa colunas de texto de uma coleção do MongoDB e salva o resultado na mesma coleção")

    # 1. Argumentos de Banco e Coleção
    parser.add_argument(
        "--mongo-uri", "-u",
        type=str,
        default=None,
        help="URI de conexão com o MongoDB (opcional)"
    )
    parser.add_argument(
        "--db_name", "-db",
        type=str,
        default="panorama",
        help="Nome do banco de dados no MongoDB"
    )
    parser.add_argument(
        "--collection", "-c",
        type=str,
        required=True,
        help="Coleção de origem (leitura e escrita)"
    )
    parser.add_argument(
        "--rede-social", "-rs",
        type=str,
        default=None,
        help="Rede social de origem (ex: youtube, reddit). Se omitido, tenta inferir a partir do nome da coleção."
    )
    parser.add_argument(
        "--columns", "-cols",
        type=str,
        nargs="+",
        default=["title", "description"],
        help="Lista de colunas/campos de texto para concatenar e pré-processar"
    )

    args = parser.parse_args()

    rede_social = args.rede_social
    if not rede_social:
        if "youtube" in args.collection.lower():
            rede_social = "youtube"
        elif "reddit" in args.collection.lower():
            rede_social = "reddit"
        elif "meta" in args.collection.lower():
            rede_social = "meta"
        else:
            rede_social = args.collection

    # Construir URI do MongoDB
    mongo_uri = args.mongo_uri
    if not mongo_uri:
        mongo_user = os.getenv("MONGO_INITDB_ROOT_USERNAME")
        mongo_password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
        mongo_host = os.getenv("MONGO_HOST", "localhost")
        mongo_port = os.getenv("MONGO_PORT", "27017")
        if mongo_user and mongo_password:
            mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/"
        else:
            mongo_uri = os.getenv("MONGO_URI", f"mongodb://{mongo_host}:{mongo_port}/")

    logger.info(f"Conectando ao MongoDB [{args.db_name}]...")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    db = client[args.db_name]
    collection = db[args.collection]

    # Buscar apenas documentos que ainda NÃO foram pré-processados
    filtro = {"processed_text": {"$exists": False}}
    projection = {col: 1 for col in args.columns}

    logger.info(f"Buscando documentos não processados da coleção '{args.collection}'...")
    documents = list(collection.find(filtro, projection))

    if not documents:
        logger.info(f"Nenhum documento novo para processar na coleção '{args.collection}'. Tudo já está pré-processado.")
        return

    df = pd.DataFrame(documents)
    logger.info(f"-> {len(df)} registros carregados para pré-processamento.")

    # Concatenar colunas de texto
    logger.info(f"Concatenando os textos das colunas: {args.columns}...")
    for col in args.columns:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)

    df["merged_text"] = df[args.columns].agg(" ".join, axis=1)

    # Pré-processar
    logger.info("Aplicando limpeza de texto (TextPreprocessor)...")
    preprocessor = TextPreprocessor()
    df["processed_text"] = df["merged_text"].apply(preprocessor.preprocess)

    # Salvar de volta na MESMA coleção (update in-place)
    logger.info(f"Salvando campo 'processed_text' de volta na coleção '{args.collection}'...")
    updated_count = 0
    failed_count = 0

    for _, row in df.iterrows():
        try:
            collection.update_one(
                {"_id": row["_id"]},
                {"$set": {
                    "processed_text": row["processed_text"],
                    "preprocessed_at": datetime.now(timezone.utc),
                }}
            )
            updated_count += 1
        except Exception as e:
            logger.error(f"Falha ao atualizar documento {row['_id']}: {e}")
            failed_count += 1

    if failed_count == len(df) and len(df) > 0:
        logger.error("Todos os registros falharam na atualização. Encerrando com erro.")
        sys.exit(1)

    logger.info(f"Pré-processamento concluído! {updated_count} documentos atualizados, {failed_count} falhas.")
    client.close()

if __name__ == "__main__":
    main()