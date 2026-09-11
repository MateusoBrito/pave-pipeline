"""
Aplica um modelo BERTopic diário já ajustado (pave-tm/resultado_semana, ver
pipeline/weekly_topics.py em pave-tm) aos documentos ainda sem tópico de um
candidato/rede - "encaminha" cada documento novo pro tópico existente mais próximo
(`.transform()`), em vez de reajustar um modelo do zero.

Pré-processamento: o MESMO usado para ajustar o modelo (pipeline.preprocessing do
pave-tm - nominalização de verbos via Stanza, depois limpeza leve, depois remoção do
nome do próprio candidato). `.transform()` só é comparável a `.fit()` quando os dois
veem o mesmo tipo de texto; alimentar `topic_model.transform()` com `documento.texto`
bruto (sem esse pré-processamento) é o que este script fazia antes - o texto bruto
carrega pontuação/stopwords/urls que o modelo nunca viu durante o ajuste, então os
embeddings ficam fora da distribuição que os tópicos aprenderam.
O pré-processamento antigo (pipelines/nlp/preprocess.py, lematização via NLTK) está
deprecado e não é usado aqui nem em nenhum lugar novo - ver dag_pipeline.py.
"""
import argparse
import functools
import glob
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import joblib
import pandas as pd
import torch
from dotenv import load_dotenv
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[2]))
load_dotenv()

from src.database.postgres import get_session  # noqa: E402

from bertopic.backend._utils import select_backend  # noqa: E402
from sentence_transformers import SentenceTransformer  # noqa: E402

# pave-tm não é um pacote instalado no venv deste repo - é montado ao lado
# (/opt/airflow/pave-tm em produção, ver dag_inferencia.py) e roda sob o PRÓPRIO venv
# (torch/bertopic/stanza vivem lá). PAVE_TM_DIR permite testar localmente com um
# checkout em outro caminho.
PAVE_TM_DIR = os.environ.get("PAVE_TM_DIR", "/opt/airflow/pave-tm")
sys.path.append(PAVE_TM_DIR)

from pipeline.preprocessing import build_pre_processor, preprocess_dataframe, strip_candidate_tokens  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("infer_topic")

FONTE_PARA_TIPO_DOC = {
    "meta": "anuncio",
    "reddit": "post",
    "youtube": "comentario",
}
# Mesmo mapeamento de weekly_topics.py/load_topicos_diarios.py: a modelagem de YouTube
# usa sempre os comentários, nunca os vídeos.
FONTE_PARA_COLECAO_MONGO = {"reddit": "reddit", "meta": "meta", "youtube": "youtube_comments"}
BATCH_SIZE_PADRAO = 500
RESULTADO_DIR_PADRAO = f"{PAVE_TM_DIR}/resultado_semana"

# O modelo "vigente" agora (o que este script deve usar por padrão) é sempre o do
# ÚLTIMO PERÍODO que já fechou por completo, em horário de Brasília, nunca o período
# corrente: a modelagem (dag_topic_modeling.py) roda uma vez por período, logo depois
# da meia-noite local do dia em que o período fecha, e ajusta o modelo do período que
# ACABOU DE FECHAR - o período corrente só ganha o SEU PRÓPRIO modelo quando ele
# também fechar. Até lá, todo documento novo (coletado a cada 2h) deve ser encaminhado
# pro modelo do último período completo, que é o mais recente que existe - por isso os
# tópicos, na prática, quase sempre têm documentos de mais de um período. Ver mesma
# conta em weekly_topics.py/LOCAL_TZ.
LOCAL_TZ = ZoneInfo("America/Sao_Paulo")

# Único knob de cadência (dia/semana/N dias) - mesma env var lida por
# pave-tm/pipeline/config.py (TOPIC_MODEL_PERIOD_DAYS/_EPOCH, ver comentário lá) e
# setada no docker-compose.yml do serviço airflow. Mudar só isso ali (e reiniciar o
# container) troca a granularidade aqui também, sem editar este arquivo.
PERIOD_DAYS = int(os.getenv("TOPIC_MODEL_PERIOD_DAYS", "7"))
PERIOD_EPOCH = date.fromisoformat(os.getenv("TOPIC_MODEL_PERIOD_EPOCH", "2026-07-27"))


def periodo_atual_start(hoje: date, period_days: int = PERIOD_DAYS, epoch: date = PERIOD_EPOCH) -> date:
    """Início do período (de duração period_days) que contém `hoje`, numa grade
    ancorada em `epoch` - mesma conta de pipeline.config.period_start_for em pave-tm."""
    offset = (hoje - epoch).days
    period_index = offset // period_days
    return epoch + timedelta(days=period_index * period_days)


def ultimo_periodo_completo(hoje: date, period_days: int = PERIOD_DAYS, epoch: date = PERIOD_EPOCH) -> date:
    """Início do último período que já fechou por completo, relativo a `hoje` -
    generaliza a antiga semana_passada() (period_days=7 fixo, segunda-feira ISO) para
    qualquer period_days/epoch configurados."""
    return periodo_atual_start(hoje, period_days, epoch) - timedelta(days=period_days)


def encontrar_model_pkl(resultado_dir: str, fonte: str, candidato_slug: str, semana_inicio: date) -> Path | None:
    """resultado_semana/{fonte}/{candidato}/semana_{segunda}/k*/bertopic_kmeans/*/
    bertopic_model.pkl - só existe uma combinação k/embedding por semana (a escolhida
    por choose_k), então o glob pega a única correspondência; ver find_combinacao_do_dia
    em load_topicos_diarios.py, mesma lógica."""
    padrao = os.path.join(resultado_dir, fonte, candidato_slug, f"semana_{semana_inicio.isoformat()}",
                           "k*", "bertopic_kmeans", "*", "bertopic_model.pkl")
    encontrados = sorted(glob.glob(padrao))
    return Path(encontrados[0]) if encontrados else None


def main():
    parser = argparse.ArgumentParser(
        description="Aplica o modelo BERTopic diário aos documentos pendentes (sem tópico) de um candidato/rede.")
    parser.add_argument("--fonte", "-f", required=True, choices=list(FONTE_PARA_TIPO_DOC.keys()),
                         help="Rede social (meta, reddit, youtube)")
    parser.add_argument("--candidato", "-c", required=True,
                         help="Código do candidato no Postgres (entidade.codigo, ex: flavio_bolsonaro)")
    parser.add_argument("--candidato-mongo", required=True,
                         help="Nome do candidato como aparece no Mongo/entidade.nome (ex: 'Flavio Bolsonaro') "
                              "- usado só para remover o nome do próprio candidato do texto antes do transform, "
                              "igual ao que build_daily_topics faz ao ajustar o modelo.")
    parser.add_argument("--day", type=str, default=None,
                         help="Início (YYYY-MM-DD) do período do modelo vigente a usar (valor de "
                              "modelo.janela_inicio - com TOPIC_MODEL_PERIOD_DAYS=7 é uma segunda-feira, "
                              "ISO week start). Default: o último período completo em horário de "
                              "Brasília - é sempre o mais recente que existe até a modelagem do próximo "
                              "período rodar (ver LOCAL_TZ/ultimo_periodo_completo acima).")
    parser.add_argument("--resultado-dir", default=RESULTADO_DIR_PADRAO)
    parser.add_argument("--batch-size", "-b", type=int, default=BATCH_SIZE_PADRAO,
                         help="Tamanho do lote de documentos para a inferência em memória (padrão: 500)")
    args = parser.parse_args()

    semana_inicio = date.fromisoformat(args.day) if args.day else ultimo_periodo_completo(datetime.now(LOCAL_TZ).date())

    session = get_session()
    try:
        # 1. Acha o `modelo` vigente PARA ESTA SEMANA (cada semana tem seu próprio
        # vigente, não há mais "o" modelo vigente de um candidato - ver
        # load_topicos_diarios.py).
        query_modelo = text("""
            SELECT id FROM modelo
            WHERE fonte_codigo = :fonte
              AND parametros->>'entidade_codigo' = :candidato
              AND status = 'vigente'
              AND janela_inicio = :semana_inicio
            LIMIT 1;
        """)
        modelo_id = session.execute(query_modelo, {
            "fonte": args.fonte, "candidato": args.candidato, "semana_inicio": semana_inicio,
        }).scalar()

        if not modelo_id:
            logger.warning("Nenhum modelo vigente para %s/%s na semana de %s (semana sem volume suficiente "
                            "para modelagem, ou modelagem de segunda ainda não rodou) - nada a fazer.",
                            args.fonte, args.candidato, semana_inicio)
            return

        model_pkl_path = encontrar_model_pkl(args.resultado_dir, args.fonte, args.candidato, semana_inicio)
        if model_pkl_path is None:
            logger.error("Modelo %s vigente no Postgres para %s/%s na semana de %s, mas bertopic_model.pkl "
                         "não encontrado sob %s - inconsistência entre banco e disco.",
                         modelo_id, args.fonte, args.candidato, semana_inicio, args.resultado_dir)
            sys.exit(1)

        logger.info("1. Carregando modelo BERTopic de %s...", model_pkl_path)
        topic_model = joblib.load(model_pkl_path)

        # 2. Busca documentos pendentes (sem linha em documento_topico para ESTE modelo).
        tipo_doc_esperado = FONTE_PARA_TIPO_DOC[args.fonte]
        query_docs = text("""
            SELECT d.id, d.texto
            FROM documento d
            JOIN alvo_coleta ac ON d.alvo_coleta_id = ac.id
            LEFT JOIN documento_topico dt ON d.id = dt.documento_id
            LEFT JOIN topico t ON dt.topico_id = t.id AND t.modelo_id = :modelo_id
            WHERE ac.fonte_codigo = :fonte
              AND ac.entidade_codigo = :candidato
              AND d.tipo = :tipo_doc
              AND d.texto IS NOT NULL
              AND dt.documento_id IS NULL;
        """)
        results = session.execute(query_docs, {
            "fonte": args.fonte, "candidato": args.candidato, "tipo_doc": tipo_doc_esperado,
            "modelo_id": modelo_id,
        }).fetchall()

        logger.info("Total de documentos pendentes: %d", len(results))
        if not results:
            return

        # 3. Pré-processa com o MESMO pipeline usado para ajustar o modelo (Stanza,
        # nominalização de verbos, depois limpeza + remoção do nome do candidato).
        df = pd.DataFrame({"doc_id": [r[0] for r in results], "text_raw": [str(r[1]) for r in results]})
        colecao_mongo = FONTE_PARA_COLECAO_MONGO[args.fonte]
        cache_key = f"infer__{args.fonte}__{args.candidato}__{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
        pp = build_pre_processor()
        df = preprocess_dataframe(colecao_mongo, df, pp, cache_key=cache_key)
        df["processed_text"] = strip_candidate_tokens(df["processed_text"].tolist(), args.candidato_mongo)
        df = df[df["processed_text"].str.strip().astype(bool)].reset_index(drop=True)
        if df.empty:
            logger.warning("Todos os %d documentos pendentes ficaram vazios após a limpeza; nada a atribuir.",
                            len(results))
            return
        logger.info("%d/%d documentos pendentes sobraram após o pré-processamento.", len(df), len(results))

        # 4. Carrega o encoder de embeddings (mesmo modelo usado no ajuste) e aplica o transform.
        nome_embedding = "Qwen/Qwen3-Embedding-0.6B"
        device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info("Carregando modelo de embeddings %s (device=%s)...", nome_embedding, device)
        encoder = SentenceTransformer(nome_embedding, trust_remote_code=True, device=device)
        encoder.max_seq_length = 256
        BERTOPIC_BATCH_SIZE_OVERRIDES = {"Qwen/Qwen3-Embedding-0.6B": 2}
        batch_size = BERTOPIC_BATCH_SIZE_OVERRIDES.get(nome_embedding)
        if batch_size:
            encoder.encode = functools.partial(encoder.encode, batch_size=batch_size)

        topic_model.embedding_model = select_backend(encoder, language=topic_model.language)
        topic_model.umap_model._input_distance_func = topic_model.umap_model.metric

        logger.info("Realizando inferência dos tópicos em lotes de %d...", args.batch_size)
        textos = df["processed_text"].tolist()
        topicos_preditos = []
        for i in range(0, len(textos), args.batch_size):
            fim_lote = min(i + args.batch_size, len(textos))
            logger.info("Lote %d-%d de %d...", i, fim_lote, len(textos))
            lote_preditos, _ = topic_model.transform(textos[i:fim_lote])
            topicos_preditos.extend(lote_preditos)

        # 5. Mapeia número do tópico (-1, 0, 1...) -> id da tabela `topico` deste modelo.
        query_topicos = text("SELECT id, numero FROM topico WHERE modelo_id = :modelo_id")
        topicos_banco = session.execute(query_topicos, {"modelo_id": modelo_id}).fetchall()
        mapa_topicos = {int(t.numero): t.id for t in topicos_banco}

        registros = []
        for doc_id, topic_num in zip(df["doc_id"].tolist(), topicos_preditos):
            topico_id_banco = mapa_topicos.get(int(topic_num))
            if topico_id_banco is not None:
                registros.append({"doc_id": doc_id, "topico_id": topico_id_banco})

        if not registros:
            logger.warning("Nenhum documento mapeado para um tópico válido (tudo caiu em -1/outlier?).")
            return

        insert_dt = text("""
            INSERT INTO documento_topico (documento_id, topico_id)
            VALUES (:doc_id, :topico_id)
            ON CONFLICT DO NOTHING;
        """)
        logger.info("Inserindo %d registros em documento_topico...", len(registros))
        session.execute(insert_dt, registros)
        session.commit()
        logger.info("Concluído: %d/%d documentos pendentes atribuídos a um tópico.", len(registros), len(results))

    except Exception:
        logger.exception("Erro durante a inferência de %s/%s na semana de %s", args.fonte, args.candidato, semana_inicio)
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    main()
