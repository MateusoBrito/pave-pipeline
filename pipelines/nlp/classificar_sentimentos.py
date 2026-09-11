import os
import argparse
import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_

# Importa as tabelas do projeto
from src.database.models import Documento, Sentimento

def run_diario():
    # 1. Recebe a data enviada pelo Airflow
    parser = argparse.ArgumentParser(description="Classificador BERTimbau Diário")
    parser.add_argument("--data-inicio", type=str, required=True, help="Data de execucao (YYYY-MM-DD)")
    args = parser.parse_args()
    
    data_alvo = args.data_inicio[:10]
    print(f"Iniciando classificação para os documentos coletados no dia: {data_alvo}")

    # 2. Configuração do Banco de Dados embutida
    PG_USER = os.getenv("POSTGRES_USER", "postgres")
    PG_PASS = os.getenv("POSTGRES_PASSWORD", "")
    PG_HOST = os.getenv("POSTGRES_HOST", "localhost") 
    PG_DB = os.getenv("POSTGRES_DB", "panorama_db")
    PG_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:5432/{PG_DB}"

    engine = create_engine(PG_URI)
    Session = sessionmaker(bind=engine)
    session = Session()

    # 3. Busca apenas os comentários/respostas do dia alvo que não têm análise
    docs_do_dia = session.query(Documento).outerjoin(
        Sentimento, Documento.id == Sentimento.documento_id
    ).filter(
        Sentimento.documento_id == None,
        Documento.texto.isnot(None),
        Documento.tipo.in_(['comentario', 'resposta']),
        # Filtra do primeiro ao último segundo do dia alvo
        Documento.coletado_em >= f"{data_alvo} 00:00:00",
        Documento.coletado_em <= f"{data_alvo} 23:59:59"
    ).all()

    if not docs_do_dia:
        print(f"Nenhum comentário novo pendente para o dia {data_alvo}.")
        session.close()
        return

    print(f"Encontrados {len(docs_do_dia)} novos textos de hoje para classificar.")

    # 4. Carrega o Modelo
    pasta_modelo = os.path.join(os.path.dirname(os.path.abspath(__file__)), "meu_bertimbau")
    device = torch.device("cpu")
    tokenizer = AutoTokenizer.from_pretrained(pasta_modelo)
    model = AutoModelForSequenceClassification.from_pretrained(pasta_modelo).to(device)
    model.eval()

    # 5. Classificação em Lotes (Batch)
    batch_size = 32
    mapa_polaridade = {0: "negativo", 1: "neutro", 2: "positivo"}
    registros_analise = []

    for i in range(0, len(docs_do_dia), batch_size):
        lote = docs_do_dia[i : i + batch_size]
        textos = [doc.texto for doc in lote]

        inputs = tokenizer(textos, padding=True, truncation=True, max_length=128, return_tensors="pt").to(device)

        with torch.no_grad():
            outputs = model(**inputs)
            probabilidades = F.softmax(outputs.logits, dim=-1)
            confianca_max, previsao_idx = torch.max(probabilidades, dim=-1)

        for j, doc in enumerate(lote):
            nova_analise = Sentimento(
                documento_id=doc.id,
                polaridade=mapa_polaridade[previsao_idx[j].item()],
                modelo_id=130
            )
            registros_analise.append(nova_analise)

    # 6. Salva
    try:
        session.bulk_save_objects(registros_analise)
        session.commit()
        print(f"Sucesso! {len(registros_analise)} novos comentários classificados.")
    except Exception as e:
        session.rollback()
        print(f"[ERRO] Falha ao salvar no banco: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    run_diario()