import os
import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Importa as tabelas do projeto
from src.database.models import Documento, Sentimento

def run_historico():
    print("Iniciando a varredura nos comentários armazenados...")

    # 1. Configuração do Banco de Dados embutida no script
    PG_USER = os.getenv("POSTGRES_USER", "postgres")
    PG_PASS = os.getenv("POSTGRES_PASSWORD", "")
    PG_HOST = os.getenv("POSTGRES_HOST", "localhost") 
    PG_DB = os.getenv("POSTGRES_DB", "panorama_db")
    PG_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:5432/{PG_DB}"

    # Cria a conexão
    engine = create_engine(PG_URI)
    Session = sessionmaker(bind=engine)
    session = Session()

    # 2. Busca documentos que não estão na tabela de análise
    docs_pendentes = session.query(Documento).outerjoin(
        Sentimento, Documento.id == Sentimento.documento_id
    ).filter(
        Sentimento.documento_id == None,
        Documento.texto.isnot(None),
        Documento.tipo.in_(['comentario', 'resposta'])
    ).all()

    if not docs_pendentes:
        print("Histórico zerado! Todos os comentários do passado já estão classificados.")
        session.close()
        return

    print(f"Foram encontrados {len(docs_pendentes)} textos antigos sem classificação.")

    # 3. Carrega o Modelo BERTimbau
    pasta_modelo = os.path.join(os.path.dirname(__file__), "meu_bertimbau")
    
    print("Carregando o modelo (BERTimbau)...")
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    tokenizer = AutoTokenizer.from_pretrained(pasta_modelo)
    model = AutoModelForSequenceClassification.from_pretrained(pasta_modelo).to(device)
    model.eval()

    # 4. Classifica em Lotes (Batch) para não estourar a memória
    batch_size = 32
    mapa_polaridade = {0: "negativo", 1: "neutro", 2: "positivo"}
    registros_analise = []

    print("Iniciando inferência em lotes...")

    for i in range(0, len(docs_pendentes), batch_size):
        lote = docs_pendentes[i : i + batch_size]
        textos = [doc.texto for doc in lote]

        # Tokenização
        inputs = tokenizer(textos, padding=True, truncation=True, max_length=128, return_tensors="pt").to(device)

        with torch.no_grad():
            outputs = model(**inputs)
            probabilidades = F.softmax(outputs.logits, dim=-1)
            confianca_max, previsao_idx = torch.max(probabilidades, dim=-1)

        # Prepara a inserção no banco
        for j, doc in enumerate(lote):
            classe_numero = previsao_idx[j].item()
            confianca_valor = confianca_max[j].item()

            nova_analise = Sentimento(
                documento_id=doc.id,
                polaridade=mapa_polaridade[classe_numero],
                modelo_id=1 
            )
            registros_analise.append(nova_analise)

        # Mostra o progresso no log
        if (i // batch_size) % 10 == 0:
            print(f"Processados: {min(i + batch_size, len(docs_pendentes))} / {len(docs_pendentes)}")

    # 5. Salva tudo de uma vez (Bulk Save)
    print("Salvando o histórico classificado no PostgreSQL...")
    try:
        session.bulk_save_objects(registros_analise)
        session.commit()
        print(f"Sucesso! {len(registros_analise)} registros antigos foram classificados.")
    except Exception as e:
        session.rollback()
        print(f"[ERRO] Falha ao salvar no banco: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    run_historico()