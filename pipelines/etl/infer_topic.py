import os
import sys
import joblib
import logging 
import argparse
import functools
import numpy as np
from pathlib import Path
from sqlalchemy import text
from dotenv import load_dotenv

import torch

sys.path.append(str(Path(__file__).resolve().parents[2]))

load_dotenv()   

from src.database.postgres import get_session
from bertopic.backend._utils import select_backend
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("infer_topic")

FONTE_PARA_TIPO_DOC = {
    "meta": "anuncio",
    "reddit": "post",
    "youtube": "comentario",
}
BATCH_SIZE_PADRAO = 500

def main():
    parser = argparse.ArgumentParser(description="Realiza a inferência de tópicos em documentos pendentes utilizando o BERTopic.")
    parser.add_argument("--model_dir", "-m",type=str,required=True,help="Caminho para o modelo BERTopic salvo.")
    parser.add_argument("--fonte", "-f",type=str,required=True,choices=list(FONTE_PARA_TIPO_DOC.keys()),help="Rede social (meta, reddit, youtube)")
    parser.add_argument("--candidato", "-c",type=str,help="Código do candidato no postgres. (ex: flavio_bolsonaro)")
    parser.add_argument("--columns", "-cols",type=str,nargs="+",default=["title", "description"],help="Lista de colunas/campos de texto para concatenar e pré-processar")
    parser.add_argument("--batch-size", "-b",type=int,default=BATCH_SIZE_PADRAO,help="Tamanho do lote de documentos para a inferência em memória (padrão: 500)")
    args = parser.parse_args()

    model_dir = Path(args.model_dir)
    model_pkl_path = model_dir / "bertopic_model.pkl"
    if not model_pkl_path.exists():
        logger.error("Arquivo do modelo não encontrado em: %s", model_pkl_path)
        sys.exit(1)

    logger.info("1. Carregando arquivo do modelo BERTopic de %s...",model_pkl_path)
    topic_model = joblib.load(model_pkl_path)
    logger.info("Modelo carregado com sucesso! Nome do modelo: %s",topic_model.__class__.__name__)

    BERTOPIC_BATCH_SIZE_OVERRIDES = {"Qwen/Qwen3-Embedding-0.6B": 2,}
    nome_embedding = "Qwen/Qwen3-Embedding-0.6B"
    logger.info("Carregando modelo de embeddings: %s",nome_embedding)

    device = "cuda" if torch.cuda.is_available() else "cpu"
    #device = "cpu"

    encoder = SentenceTransformer(nome_embedding,trust_remote_code=True,device=device)
    encoder.max_seq_length = 256
    logger.info("Modelo de embeddings carregado. Device: %s",device)

    teste_emb = encoder.encode(["teste"])
    logger.info("dtype do embedding do Qwen: %s | shape: %s", np.asarray(teste_emb).dtype, np.asarray(teste_emb).shape)

    batch_size = BERTOPIC_BATCH_SIZE_OVERRIDES.get(nome_embedding)

    if batch_size:
        encoder.encode = functools.partial(
            encoder.encode,
            batch_size=batch_size
        )

    topic_model.embedding_model = select_backend(encoder,language=topic_model.language)
    topic_model.umap_model._input_distance_func = topic_model.umap_model.metric
    logger.info("Métrica do UMAP: %s", topic_model.umap_model.metric)
    logger.info("modelo carregado com sucesso! Nome do modelo: %s", topic_model.__class__.__name__)
    logger.info("Backend de embeddings configurado com sucesso.")

    tipo_doc_esperado = FONTE_PARA_TIPO_DOC[args.fonte]
    session = get_session()

    try:
        # 2. Busca o ID do modelo no banco correspondente a essa combinação (fonte/candidato)
        # O modelo em `modelo` armazena fonte e entidade em seus parâmetros/nome
        query_modelo = text("""
            SELECT id FROM modelo 
            WHERE fonte_codigo = :fonte 
              AND parametros->>'entidade_codigo' = :candidato
              AND status = 'vigente'
            LIMIT 1;
        """)
        modelo_id = session.execute(query_modelo, {
            "fonte": args.fonte,
            "candidato": args.candidato
        }).scalar()

        if not modelo_id:
            logger.error("Não existe modelo vigente para para %s em %s", args.fonte, args.candidato)
            sys.exit(1)
        
        # 3. Consulta de Filtro dos Documentos Pendentes
        # Garante: 1) fonte_codigo correta, 2) entidade_codigo correta, 3) tipo_documento específico da fonte,
        #          4) NÃO possui registro prévio na tabela documento_topico para o modelo
        logger.info("2. Buscando documentos pendentes no PostgreSQL (Tipo: '%s')...", tipo_doc_esperado)

        query_docs = text("""
            SELECT d.id, d.texto
            FROM documento d
            JOIN alvo_coleta ac ON d.alvo_coleta_id = ac.id
            LEFT JOIN documento_topico dt ON d.id = dt.documento_id
            LEFT JOIN topico t ON dt.topico_id = t.id AND t.modelo_id = :modelo_id
            WHERE ac.fonte_codigo = :fonte
            AND ac.entidade_codigo = :candidato
            AND d.tipo = :tipo_doc 
            AND dt.documento_id IS NULL;
        """)

        results = session.execute(query_docs, {
            "fonte": args.fonte,
            "candidato": args.candidato,
            "tipo_doc": tipo_doc_esperado,
            "modelo_id": modelo_id
        }).fetchall()

        logger.info("Total de documentos pendentes encontrados: %d", len(results))
        if results:
            logger.info("Exemplo do primeiro documento pendente [ID %s]: '%s...'", results[0][0], str(results[0][1])[:80])

            # 4. Processar em Lotes (Inferência)
            logger.info("Realizando inferência dos tópicos em lotes de %d...", args.batch_size)
            doc_ids = [r[0] for r in results]
            textos = [str(r[1]) if r[1] else "" for r in results]
            topicos_preditos = []
            for i in range(0, len(textos), args.batch_size):
                fim_lote = min(i + args.batch_size, len(textos))
                logger.info("Processando lote %d de %d documentos...", i + 1, len(textos))
                
                lote_textos = textos[i:fim_lote]
                lote_preditos, _ = topic_model.transform(lote_textos)
                topicos_preditos.extend(lote_preditos)
            
            # 5. Mapear tópicos preditos (-1, 0, 1...) para o ID da tabela `topico` do banco
            query_topicos = text("SELECT id, numero FROM topico WHERE modelo_id = :modelo_id")
            topicos_banco = session.execute(query_topicos, {"modelo_id": modelo_id}).fetchall()
            
            # Mapeia a coluna 'numero' (que guarda o número do tópico gerado pelo modelo: -1, 0, 1...)
            # para a chave primária 'id' da tabela 'topico'
            mapa_topicos = {}
            for t in topicos_banco:
                try:
                    num_topico = int(t.numero)
                    mapa_topicos[num_topico] = t.id
                except (ValueError, TypeError):
                    continue

            # 6. Salvar os resultados na tabela documento_topico
            insert_dt = text("""
                INSERT INTO documento_topico (documento_id, topico_id)
                VALUES (:doc_id, :topico_id)
                ON CONFLICT DO NOTHING;
            """)
            
            registros_para_inserir = []
            for doc_id, topic_num in zip(doc_ids, topicos_preditos):
                topico_id_banco = mapa_topicos.get(topic_num)
                if topico_id_banco is not None:
                    registros_para_inserir.append({
                        "doc_id": doc_id,
                        "topico_id": topico_id_banco
                    })
                
            if registros_para_inserir:
                logger.info("Inserindo %d registros na tabela documento_topico...", len(registros_para_inserir))
                session.execute(insert_dt, registros_para_inserir)
                session.commit()
                logger.info("Registros salvos com sucesso!")
            else:
                logger.warning("Nenhum registro mapeado para salvar. Verifique se os tópicos do banco correspondem aos do modelo.")

    except Exception as e:
        logger.error("Erro durante a execução: %s", e)
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()