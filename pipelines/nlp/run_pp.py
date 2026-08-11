import argparse
import sys
from pathlib import Path
import pandas as pd
from pymongo import MongoClient

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.preprocessing import TextPreprocessor

def main():
    parser = argparse.ArgumentParser(description="Pré-processa colunas de texto de uma coleção do MongoDB")
    
    # 1. Argumentos de Banco e Coleção
    parser.add_argument(
        "--mongo-uri", "-u",
        type=str,
        default="mongodb://root:rootpassword@localhost:27017/",
        help="URI de conexão com o MongoDB"
    )
    parser.add_argument(
        "--db_name", "-db",
        type=str,
        default="panorama_db",
        help="Nome do banco de dados no MongoDB"
    )
    parser.add_argument(
        "--collection", "-c",
        type=str,
        required=True, # Ex: raw_youtube ou raw_reddit
        help="Coleção de origem (Input collection)"
    )
    parser.add_argument(
        "--output_collection", "-oc",
        type=str,
        default=None,
        help="Coleção de destino (se omitido, salva na mesma coleção ou sufixa _processed)"
    )
    
    # 2. Argumentos de Colunas
    parser.add_argument(
        "--columns", "-cols",
        type=str,
        nargs="+",
        default=["title", "description"], 
        help="Lista de colunas/campos de texto para concatenar e pré-processar"
    )

    args = parser.parse_args()

    # Define nome da coleção de saída caso não seja informada
    output_coll_name = args.output_collection or f"{args.collection}_processed"

    print(f"1. Conectando ao MongoDB [{args.db_name}]...")
    client = MongoClient(args.mongo_uri)
    db = client[args.db_name]
    
    source_coll = db[args.collection]
    target_coll = db[output_coll_name]

    print(f"2. Buscando documentos da coleção '{args.collection}'...")
    # Traz o ID e apenas as colunas solicitadas para poupar RAM
    projection = {col: 1 for col in args.columns}
    documents = list(source_coll.find({}, projection))

    if not documents:
        print(f"Nenhum documento encontrado na coleção '{args.collection}'. Encerrando.")
        return

    df = pd.DataFrame(documents)
    print(f"   -> {len(df)} registros carregados.")

    # 3. Concatenando os textos das colunas selecionadas
    print(f"3. Concatenando os textos das colunas: {args.columns}...")
    # Garante que todas as colunas existem no DF (preenche com string vazia caso falte em algum doc)
    for col in args.columns:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)

    df["merged_text"] = df[args.columns].agg(" ".join, axis=1)

    # 4. Pré-processamento
    print("4. Aplicando limpeza de texto (TextPreprocessor)...")
    preprocessor = TextPreprocessor()
    df["processed_text"] = df["merged_text"].apply(preprocessor.preprocess)

    # 5. Persistência no MongoDB
    print(f"5. Salvando resultados na coleção '{output_coll_name}'...")
    
    # Prepara os documentos para inserção (transforma o DF de volta em dicionários)
    output_docs = []
    for _, row in df.iterrows():
        doc = {
            "_id": row["_id"], # Mantém o mesmo ID para permitir JOIN ou referência
            "merged_text": row["merged_text"],
            "processed_text": row["processed_text"]
        }
        output_docs.append(doc)

    # Insere ou atualiza no Mongo (upsert para evitar duplicados se re-executar)
    for doc in output_docs:
        target_coll.replace_one({"_id": doc["_id"]}, doc, upsert=True)

    print(f"Pipeline concluído com sucesso! {len(output_docs)} documentos salvos em '{output_coll_name}'.")

if __name__ == "__main__":
    main()