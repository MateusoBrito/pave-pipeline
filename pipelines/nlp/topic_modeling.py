import argparse
import json
import logging
import os
import re
import unicodedata
from collections import Counter, defaultdict
from functools import partial

import numpy as np
import pandas as pd
from bertopic import BERTopic

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("topic_modeling")

MARCADORES_CONTEUDO_REMOVIDO = {"[removed]", "[deleted]"}
OUTPUT_DIR_PADRAO = "results_modelagem"
VALORES_K_PADRAO = [5, 10, 15, 20]

STOPWORDS_RUIDO_GENERICO = [
    "parabéns", "parabens", "lindo", "linda", "excelente", "maravilhoso",
    "maravilha", "emocionante", "emoção", "amei", "ótimo", "otimo", "incrível",
    "incrivel", "sensacional", "espetacular", "show", "top", "gostei",
    "adorei", "perfeito", "perfeita", "demais", "brilhante",
    "deus", "jesus", "cristo", "abençoe", "abencoe", "amém", "amem",
    "oração", "oracao", "reze", "rezar", "orando", "glória", "gloria",
    "misericórdia", "misericordia", "bênção", "bencao", "proteja", "protege",
    "vamos", "juntos", "força", "forca", "cima", "tudo", "todos", "sempre",
    "tmj", "bora", "avante", "querido", "querida",
    "vai", "vou", "vao", "vamo", "vem", "pra", "pro",
    "fica", "ficar", "ficou", "ficam", "fazer", "faz",
]


def obter_mongo_uri() -> str:
    usuario = os.getenv("MONGO_INITDB_ROOT_USERNAME")
    senha = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
    host = os.getenv("MONGO_HOST", "localhost")
    porta = os.getenv("MONGO_PORT", "27017")
    if not usuario or not senha:
        raise SystemExit("Defina MONGO_INITDB_ROOT_USERNAME e MONGO_INITDB_ROOT_PASSWORD.")
    return f"mongodb://{usuario}:{senha}@{host}:{porta}/"


def _buscar_documentos(database: str, collection: str, filtro: dict = None) -> list:
    from pymongo import MongoClient

    cliente = MongoClient(obter_mongo_uri(), serverSelectionTimeoutMS=5000)
    try:
        cliente.admin.command("ping")
        colecao = cliente[database][collection]
        documentos = list(colecao.find(filtro or {}))
        logger.info("Encontrados %d documentos em '%s.%s'.", len(documentos), database, collection)
        return documentos
    finally:
        cliente.close()


def _remover_acentos(texto: str) -> str:
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _candidato_bate(valor, candidato) -> bool:
    return _remover_acentos(str(valor).strip().lower()) == _remover_acentos(str(candidato).strip().lower())


def _limpar_marcador_removido(valor) -> str:
    v = str(valor).strip()
    return "" if v.lower() in MARCADORES_CONTEUDO_REMOVIDO else v


def _limpar_artefato_removido_do_stemizado(texto) -> str:
    tokens = [t for t in str(texto).split() if t.lower() not in {"removed", "deleted"}]
    return " ".join(tokens)


def _juntar_lista_de_texto(valor) -> str:
    if isinstance(valor, list):
        return " ".join(str(v) for v in valor if v)
    if valor is None:
        return ""
    return str(valor)


def extrair_texto(doc: dict, campos_textuais: list) -> str:
    partes = [_juntar_lista_de_texto(doc.get(campo)) for campo in campos_textuais]
    return _limpar_marcador_removido(" ".join(partes).strip())


def carregar_reddit(database: str, candidato: str, campos_textuais: list) -> pd.DataFrame:
    documentos = _buscar_documentos(database, "reddit", {"_tipo_documento": "post"})
    linhas = []
    for doc in documentos:
        if not _candidato_bate(doc.get("_termo_busca", ""), candidato):
            continue
        texto_original = extrair_texto(doc, campos_textuais)
        stemizado = _limpar_artefato_removido_do_stemizado(doc.get("processed_text", ""))
        if not texto_original or not stemizado.strip():
            continue
        linhas.append({
            "doc_id": doc.get("id", str(doc.get("_id", ""))),
            "texto_original": texto_original,
            "texto_stemizado": stemizado,
        })
    return pd.DataFrame(linhas)


def carregar_meta(database: str, candidato: str, campos_textuais: list) -> pd.DataFrame:
    documentos = _buscar_documentos(database, "meta")
    linhas = []
    for doc in documentos:
        nome = doc.get("page_name") or doc.get("_entidade_busca") or doc.get("page_id") or ""
        if not _candidato_bate(nome, candidato):
            continue
        texto_original = extrair_texto(doc, campos_textuais)
        stemizado = _limpar_artefato_removido_do_stemizado(doc.get("processed_text", ""))
        if not texto_original or not stemizado.strip():
            continue
        linhas.append({
            "doc_id": doc.get("id", str(doc.get("_id", ""))),
            "texto_original": texto_original,
            "texto_stemizado": stemizado,
        })
    return pd.DataFrame(linhas)


def carregar_youtube(database: str, candidato: str, campos_textuais: list) -> pd.DataFrame:
    comentarios = _buscar_documentos(database, "youtube_comments")
    videos = _buscar_documentos(database, "youtube_videos")

    mapa_video_para_termo = {}
    for v in videos:
        video_id = v.get("videoId")
        if not video_id:
            continue
        mapa_video_para_termo[video_id] = v.get("channelTitle") or v.get("_entidade_busca") or ""

    linhas = []
    for c in comentarios:
        termo = mapa_video_para_termo.get(c.get("videoId")) or c.get("channelTitle") or ""
        if not _candidato_bate(termo, candidato):
            continue
        texto_original = extrair_texto(c, campos_textuais)
        stemizado = _limpar_artefato_removido_do_stemizado(c.get("processed_text", ""))
        if not texto_original or not stemizado.strip():
            continue
        linhas.append({
            "doc_id": c.get("commentId", ""),
            "texto_original": texto_original,
            "texto_stemizado": stemizado,
        })
    return pd.DataFrame(linhas)


CARREGADORES_POR_REDE = {
    "reddit": carregar_reddit,
    "meta": carregar_meta,
    "youtube": carregar_youtube,
}

CAMPOS_TEXTUAIS_PADRAO_POR_REDE = {
    "reddit": ["title", "selftext"],
    "meta": ["ad_creative_link_titles", "ad_creative_bodies"],
    "youtube": ["text"],
}


def carregar_candidato(rede_social: str, database: str, candidato: str, campos_textuais: list) -> pd.DataFrame:
    df = CARREGADORES_POR_REDE[rede_social](database, candidato, campos_textuais)
    if df.empty:
        raise SystemExit(f"Nenhum documento encontrado para candidato={candidato!r} em rede_social={rede_social!r}.")
    df = df.drop_duplicates(subset=["texto_original"]).reset_index(drop=True)
    df["termo_busca"] = candidato
    logger.info("%d documentos validos e unicos carregados para '%s' / '%s'.", len(df), rede_social, candidato)
    return df


def calcular_ou_carregar_embeddings(textos, modelo_nome, cache_path, device=None) -> np.ndarray:
    if os.path.exists(cache_path):
        embeddings = np.load(cache_path)
        if embeddings.shape[0] == len(textos):
            logger.info("Cache de embeddings encontrado (%s), reutilizando.", cache_path)
            return embeddings
        logger.warning("Cache de embeddings com tamanho diferente. Recalculando.")

    logger.info("Calculando embeddings com '%s' (texto original, %d documentos)...", modelo_nome, len(textos))
    from sentence_transformers import SentenceTransformer

    modelo = SentenceTransformer(modelo_nome, device=device)
    try:
        embeddings = modelo.encode(textos, show_progress_bar=True, batch_size=64)
    except Exception as erro:
        if device == "cpu":
            raise
        logger.warning("Falha ao usar GPU (%s). Tentando novamente forcando CPU...", erro)
        modelo = SentenceTransformer(modelo_nome, device="cpu")
        embeddings = modelo.encode(textos, show_progress_bar=True, batch_size=64)

    np.save(cache_path, embeddings)
    return embeddings


def obter_stopwords_ruido_generico() -> set:
    from nltk.stem import RSLPStemmer
    try:
        import nltk
        nltk.data.find("stemmers/rslp")
    except LookupError:
        import nltk
        nltk.download("rslp", quiet=True)

    stemmer = RSLPStemmer()
    stopwords = set()
    for palavra in STOPWORDS_RUIDO_GENERICO:
        stopwords.add(palavra)
        try:
            stopwords.add(stemmer.stem(palavra))
        except Exception:
            pass
    return stopwords


def calcular_stopwords_por_frequencia(textos_stemizados: list, min_df: int = 1, max_df: float = 1.0) -> set:
    n_documentos = len(textos_stemizados)
    if n_documentos == 0:
        return set()

    contador_documentos = Counter()
    for texto in textos_stemizados:
        contador_documentos.update(set(str(texto).split()))

    excluidas = set()
    for palavra, freq in contador_documentos.items():
        if freq < min_df:
            excluidas.add(palavra)
        elif max_df < 1.0 and (freq / n_documentos) > max_df:
            excluidas.add(palavra)
    return excluidas


def expandir_stopwords_por_acento(stopwords_base: set, textos_stemizados: list) -> set:
    expandido = set(stopwords_base)

    for s in stopwords_base:
        expandido.add(_remover_acentos(s))

    sem_acento_base = {_remover_acentos(s) for s in stopwords_base}
    vocabulario = set()
    for texto in textos_stemizados:
        vocabulario.update(str(texto).split())
    for palavra in vocabulario:
        if _remover_acentos(palavra) in sem_acento_base:
            expandido.add(palavra)

    return expandido


def obter_stopwords_candidato(termo_busca: str) -> set:
    from nltk.stem import RSLPStemmer
    try:
        import nltk
        nltk.data.find("stemmers/rslp")
    except LookupError:
        import nltk
        nltk.download("rslp", quiet=True)

    stemmer = RSLPStemmer()
    partes = re.findall(r"[a-zA-ZÀ-ÿ]+", str(termo_busca).lower())

    stopwords = set()
    for parte in partes:
        if len(parte) < 2:
            continue
        stopwords.add(parte)
        try:
            stopwords.add(stemmer.stem(parte))
        except Exception:
            pass
    return stopwords


def _preprocessador_vetorizador(texto: str, normalizar_vogal_repetida: bool = False) -> str:
    texto = str(texto).lower()
    texto = _remover_acentos(texto)
    if normalizar_vogal_repetida:
        texto = re.sub(r"([aeiou])\1+", r"\1", texto)
    return texto


def treinar_bertopic(textos_stemizados, embeddings, ngram_max=1, min_df=1, max_df=1.0, min_topic_size=10, nr_topics=None, stopwords_candidato=None, seed=42, algoritmo_clustering="hdbscan", n_topicos_kmeans=10, normalizar_vogal_repetida=False):
    from sklearn.feature_extraction.text import CountVectorizer
    from umap import UMAP

    preprocessador = partial(_preprocessador_vetorizador, normalizar_vogal_repetida=normalizar_vogal_repetida)

    stop_words = [preprocessador(s) for s in stopwords_candidato] if stopwords_candidato else None
    vectorizer_model = CountVectorizer(ngram_range=(1, ngram_max), stop_words=stop_words, preprocessor=preprocessador)

    umap_model = UMAP(n_neighbors=15, n_components=5, min_dist=0.0, metric="cosine", random_state=seed)

    if algoritmo_clustering == "kmeans":
        from sklearn.cluster import KMeans
        cluster_model = KMeans(n_clusters=n_topicos_kmeans, random_state=seed, n_init=10)
    else:
        cluster_model = None

    topic_model = BERTopic(
        language="multilingual",
        umap_model=umap_model,
        hdbscan_model=cluster_model,
        vectorizer_model=vectorizer_model,
        min_topic_size=min_topic_size,
        nr_topics=nr_topics,
        calculate_probabilities=False,
        verbose=True,
    )
    topics, _ = topic_model.fit_transform(textos_stemizados, embeddings=embeddings)
    return topic_model, topics


def construir_mapa_destemizacao(textos_originais: list, normalizar_vogal_repetida: bool = False) -> dict:
    from nltk.stem import RSLPStemmer

    try:
        stemmer = RSLPStemmer()
    except LookupError:
        import nltk
        nltk.download("rslp", quiet=True)
        stemmer = RSLPStemmer()

    contagem_por_raiz = defaultdict(Counter)
    for texto in textos_originais:
        for palavra in re.findall(r"[a-zA-ZÀ-ÿ]+", str(texto).lower()):
            if len(palavra) < 3:
                continue
            try:
                raiz = stemmer.stem(palavra)
            except Exception:
                continue
            contagem_por_raiz[raiz][palavra] += 1

    resultado = {raiz: contagem.most_common(1)[0][0] for raiz, contagem in contagem_por_raiz.items()}

    for raiz, palavra_legivel in list(resultado.items()):
        resultado.setdefault(_remover_acentos(raiz), palavra_legivel)
        resultado.setdefault(_preprocessador_vetorizador(raiz, normalizar_vogal_repetida=normalizar_vogal_repetida), palavra_legivel)

    return resultado


def _palavras_legiveis_unicas(topic_model: BERTopic, topic_id, mapa_destemizacao: dict, top_n: int) -> list:
    candidatos = topic_model.get_topic(topic_id)
    vistas = set()
    resultado = []
    for raiz, _peso in candidatos:
        palavra = mapa_destemizacao.get(raiz, raiz) if mapa_destemizacao else raiz
        chave = palavra.lower().strip()
        if not chave or chave in vistas:
            continue
        vistas.add(chave)
        resultado.append(palavra)
        if len(resultado) >= top_n:
            break
    return resultado


def gerar_topicos_json(topic_model: BERTopic, caminho_saida, mapa_destemizacao=None, top_n=10):
    resultado = {}
    for topic_id in topic_model.get_topics():
        if topic_id == -1:
            continue
        resultado[str(topic_id)] = _palavras_legiveis_unicas(topic_model, topic_id, mapa_destemizacao, top_n)

    with open(caminho_saida, "w", encoding="utf-8") as f:
        json.dump(resultado, f, indent=2, ensure_ascii=False)
    logger.info("Palavras dos topicos salvas em: %s", caminho_saida)


def gerar_documentos_topicos_csv(df: pd.DataFrame, topics, caminho_saida):
    saida = df[["doc_id", "termo_busca"]].copy()
    saida["topico_dominante"] = topics
    saida.to_csv(caminho_saida, index=False)
    logger.info("Topico dominante por documento salvo em: %s", caminho_saida)


def gerar_relatorio_distribuicao(df: pd.DataFrame, topics, caminho_saida, mapa_destemizacao=None, topic_model=None, top_n_palavras=20, tamanho_assinatura_duplicata=60):
    topics_arr = np.array(topics)
    total = len(topics_arr)
    contagem = pd.Series(topics_arr).value_counts().sort_index()

    n_outliers = int(contagem.get(-1, 0))
    topicos_validos = contagem.drop(index=-1, errors="ignore").sort_values(ascending=False)

    linhas = [
        f"Total de documentos: {total}",
        f"Total de topicos (excluindo outliers): {len(topicos_validos)}",
        f"Outliers (-1): {n_outliers} ({n_outliers/total*100:.1f}%)",
        "",
    ]
    if len(topicos_validos) > 0:
        maior_id, maior_qtd = topicos_validos.index[0], topicos_validos.iloc[0]
        linhas += [f"Maior topico: #{maior_id} com {maior_qtd} documentos ({maior_qtd/total*100:.1f}%)", ""]

    linhas += ["=" * 90, "DISTRIBUICAO POR TOPICO (do maior para o menor)", "=" * 90,
               f"{'topico':>8} | {'qtd':>6} | {'%':>6} | palavras-chave", "-" * 90]
    for topic_id, qtd in topicos_validos.items():
        palavras_str = ""
        if topic_model is not None:
            palavras_str = ", ".join(_palavras_legiveis_unicas(topic_model, topic_id, mapa_destemizacao, 5))
        linhas.append(f"{topic_id:>8} | {qtd:>6} | {qtd/total*100:>5.1f}% | {palavras_str}")

    contador_docs = Counter()
    for texto_stemizado in df["texto_stemizado"]:
        contador_docs.update(set(str(texto_stemizado).split()))

    linhas += ["", "=" * 90, f"TOP {top_n_palavras} PALAVRAS MAIS REPETIDAS (em quantos documentos cada uma aparece)", "=" * 90,
               f"{'palavra':>20} | {'docs':>6} | {'%':>6}", "-" * 50]
    for raiz, n_docs in contador_docs.most_common(top_n_palavras):
        palavra_legivel = mapa_destemizacao.get(raiz, raiz) if mapa_destemizacao else raiz
        linhas.append(f"{palavra_legivel:>20} | {n_docs:>6} | {n_docs/total*100:>5.1f}%")

    assinaturas = {}
    for doc_id, texto in zip(df["doc_id"], df["texto_original"]):
        normalizado = re.sub(r"[^a-z0-9À-ÿ\s]", "", str(texto).lower())
        normalizado = re.sub(r"\s+", " ", normalizado).strip()
        assinaturas.setdefault(normalizado[:tamanho_assinatura_duplicata], []).append((doc_id, texto))

    grupos_duplicados = {k: v for k, v in assinaturas.items() if len(v) > 1 and k.strip()}
    total_docs_dup = sum(len(v) for v in grupos_duplicados.values())

    linhas += ["", "=" * 90, "POSSIVEIS NOTICIAS QUASE-DUPLICADAS (mesmo inicio de texto)", "=" * 90,
               f"Grupos de quase-duplicatas encontrados: {len(grupos_duplicados)}",
               f"Documentos envolvidos: {total_docs_dup} ({total_docs_dup/total*100:.1f}% da base)", ""]

    if grupos_duplicados:
        linhas.append("Maiores grupos (ate 15 exemplos):")
        for i, (assinatura, docs) in enumerate(sorted(grupos_duplicados.items(), key=lambda x: -len(x[1]))[:15], 1):
            linhas.append(f"\n[{i}] {len(docs)} documentos quase-identicos, comecando com:")
            linhas.append(f"    \"{docs[0][1][:120]}...\"")
            linhas.append(f"    doc_ids: {', '.join(str(d[0]) for d in docs[:10])}" + (" ..." if len(docs) > 10 else ""))

    with open(caminho_saida, "w", encoding="utf-8") as f:
        f.write("\n".join(linhas))
    logger.info("Relatorio de distribuicao salvo em: %s", caminho_saida)


def _indices_representativos_por_centroide(embeddings, topics, topic_id, n):
    topics_arr = np.array(topics)
    indices_topico = np.where(topics_arr == topic_id)[0]
    if len(indices_topico) == 0:
        return np.array([], dtype=int)

    sub_embeddings = embeddings[indices_topico]
    centroide = sub_embeddings.mean(axis=0)
    distancias = np.linalg.norm(sub_embeddings - centroide, axis=1)
    return indices_topico[np.argsort(distancias)[:n]]


def gerar_exemplos_csv(df: pd.DataFrame, embeddings, topics, caminho_saida, n_total=20, n_representativos=10, seed=42):
    rng = np.random.RandomState(seed)
    topics_arr = np.array(topics)
    linhas = []

    for topic_id in sorted(set(topics_arr)):
        if topic_id == -1:
            continue

        indices_topico = np.where(topics_arr == topic_id)[0]
        n_repr = min(n_representativos, len(indices_topico))
        idx_representativos = _indices_representativos_por_centroide(embeddings, topics, topic_id, n_repr)

        restantes = np.setdiff1d(indices_topico, idx_representativos)
        n_aleatorios = min(n_total - len(idx_representativos), len(restantes))
        idx_aleatorios = rng.choice(restantes, size=n_aleatorios, replace=False) if n_aleatorios > 0 else np.array([], dtype=int)

        for idx in idx_representativos:
            linhas.append({"topico": topic_id, "tipo": "representativo", "doc_id": df.iloc[idx]["doc_id"], "texto": df.iloc[idx]["texto_original"]})
        for idx in idx_aleatorios:
            linhas.append({"topico": topic_id, "tipo": "aleatorio", "doc_id": df.iloc[idx]["doc_id"], "texto": df.iloc[idx]["texto_original"]})

    pd.DataFrame(linhas).to_csv(caminho_saida, index=False)
    logger.info("Exemplos por topico salvos em: %s", caminho_saida)


def slug(texto: str) -> str:
    texto = texto.strip().lower()
    texto = re.sub(r"[^a-z0-9áàâãéêíóôõúüç\s_-]", "", texto)
    texto = re.sub(r"\s+", "_", texto)
    return texto or "sem_nome"


def _rodar_um_modelo(df, pasta_candidato, subpasta_algoritmo, embeddings, ngram_max, min_df, max_df,
                      stopwords_candidato, seed, algoritmo_clustering, n_topicos_kmeans,
                      min_topic_size, nr_topics, n_exemplos, n_exemplos_representativos, candidato,
                      normalizar_vogal_repetida=False):
    pasta_termo = os.path.join(pasta_candidato, subpasta_algoritmo)
    os.makedirs(pasta_termo, exist_ok=True)

    topic_model, topics = treinar_bertopic(
        df["texto_stemizado"].tolist(), embeddings,
        ngram_max=ngram_max, min_df=min_df, max_df=max_df,
        min_topic_size=min_topic_size, nr_topics=nr_topics,
        stopwords_candidato=stopwords_candidato, seed=seed,
        algoritmo_clustering=algoritmo_clustering, n_topicos_kmeans=n_topicos_kmeans,
        normalizar_vogal_repetida=normalizar_vogal_repetida,
    )

    n_topicos = len(set(topics)) - (1 if -1 in topics else 0)
    logger.info("'%s' [%s]: %d topicos encontrados, %d documentos outlier.", candidato, subpasta_algoritmo, n_topicos, list(topics).count(-1))

    mapa_destemizacao = construir_mapa_destemizacao(df["texto_original"].tolist(), normalizar_vogal_repetida=normalizar_vogal_repetida)

    gerar_topicos_json(topic_model, os.path.join(pasta_termo, "topicos_palavras.json"), mapa_destemizacao=mapa_destemizacao)
    gerar_documentos_topicos_csv(df, topics, os.path.join(pasta_termo, "documentos_topicos.csv"))
    gerar_relatorio_distribuicao(df, topics, os.path.join(pasta_termo, "distribuicao_topicos.txt"), mapa_destemizacao=mapa_destemizacao, topic_model=topic_model)
    gerar_exemplos_csv(df, embeddings, topics, os.path.join(pasta_termo, "exemplos_topicos.csv"), n_total=n_exemplos, n_representativos=n_exemplos_representativos)

    with open(os.path.join(pasta_termo, "parametros_usados.json"), "w", encoding="utf-8") as f:
        json.dump({
            "algoritmo_clustering": algoritmo_clustering, "n_topicos_kmeans": n_topicos_kmeans,
            "min_topic_size": min_topic_size, "nr_topics": nr_topics, "seed": seed,
            "min_df": min_df, "max_df": max_df, "normalizar_vogal_repetida": normalizar_vogal_repetida,
        }, f, indent=2, ensure_ascii=False)

    topic_model.save(os.path.join(pasta_termo, "bertopic_model"), serialization="pickle")


def rodar_modelagem(df, rede_social, candidato, output_dir, embedding_model, ngram_max, min_df, n_exemplos, n_exemplos_representativos, device=None, min_topic_size=10, nr_topics=None, seed=42, remover_ruido_generico=True, algoritmo_clustering="kmeans", valores_k=None, max_df=1.0, normalizar_vogal_repetida=False):
    valores_k = valores_k or VALORES_K_PADRAO

    pasta_candidato = os.path.join(output_dir, slug(rede_social), slug(candidato))
    os.makedirs(pasta_candidato, exist_ok=True)
    cache_embeddings = os.path.join(pasta_candidato, "embeddings_cache.npy")
    embeddings = calcular_ou_carregar_embeddings(df["texto_original"].tolist(), embedding_model, cache_embeddings, device=device)

    stopwords_candidato = obter_stopwords_candidato(candidato)
    if remover_ruido_generico:
        stopwords_candidato = stopwords_candidato | obter_stopwords_ruido_generico()
    if min_df > 1 or max_df < 1.0:
        stopwords_candidato = stopwords_candidato | calcular_stopwords_por_frequencia(
            df["texto_stemizado"].tolist(), min_df=min_df, max_df=max_df,
        )
    stopwords_candidato = expandir_stopwords_por_acento(stopwords_candidato, df["texto_stemizado"].tolist())
    logger.info("Stopwords usadas para '%s': %s", candidato, stopwords_candidato)

    if algoritmo_clustering == "kmeans":
        logger.info("KMeans: rodando para K = %s", valores_k)
        for k in valores_k:
            _rodar_um_modelo(
                df, pasta_candidato, f"k{k}", embeddings, ngram_max, min_df, max_df,
                stopwords_candidato, seed, "kmeans", k,
                min_topic_size, nr_topics, n_exemplos, n_exemplos_representativos, candidato,
                normalizar_vogal_repetida=normalizar_vogal_repetida,
            )
    else:
        _rodar_um_modelo(
            df, pasta_candidato, "hdbscan", embeddings, ngram_max, min_df, max_df,
            stopwords_candidato, seed, "hdbscan", None,
            min_topic_size, nr_topics, n_exemplos, n_exemplos_representativos, candidato,
            normalizar_vogal_repetida=normalizar_vogal_repetida,
        )


PARAMETROS_PADRAO = {
    "algoritmo_clustering": "kmeans",
    "valores_k": VALORES_K_PADRAO,
    "min_df": 3,
    "max_df": 1.0,
    "min_topic_size": 10,
    "nr_topics": None,
    "seed": 42,
    "stopwords_genericas": True,
    "normalizar_vogal_repetida": False,
    "ngram_max": 1,
    "n_exemplos": 20,
    "n_exemplos_representativos": 10,
}


def main():
    parser = argparse.ArgumentParser(description="Modelagem de topicos com BERTopic, lendo direto do MongoDB")
    parser.add_argument("--rede-social", type=str, required=True, choices=["reddit", "meta", "youtube"])
    parser.add_argument("--candidato", type=str, required=True)
    parser.add_argument("--campos-textuais", type=str, nargs="+", default=None)
    parser.add_argument("--parametros", type=str, default="{}")
    parser.add_argument("--database", type=str, default="panorama")
    parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR_PADRAO)
    parser.add_argument("--embedding-model", type=str, default="paraphrase-multilingual-MiniLM-L12-v2")
    parser.add_argument("--device", type=str, default=None, choices=[None, "cpu", "cuda", "mps"])
    args = parser.parse_args()

    parametros = {**PARAMETROS_PADRAO, **json.loads(args.parametros)}
    campos_textuais = args.campos_textuais or CAMPOS_TEXTUAIS_PADRAO_POR_REDE[args.rede_social]

    logger.info("Rede social: '%s'. Candidato: '%s'. Campos textuais: %s. Parametros: %s", args.rede_social, args.candidato, campos_textuais, parametros)
    df = carregar_candidato(args.rede_social, args.database, args.candidato, campos_textuais)

    os.makedirs(args.output_dir, exist_ok=True)

    rodar_modelagem(
        df, args.rede_social, args.candidato, args.output_dir, args.embedding_model,
        parametros["ngram_max"], parametros["min_df"],
        parametros["n_exemplos"], parametros["n_exemplos_representativos"],
        device=args.device, min_topic_size=parametros["min_topic_size"], nr_topics=parametros["nr_topics"], seed=parametros["seed"],
        remover_ruido_generico=parametros["stopwords_genericas"],
        algoritmo_clustering=parametros["algoritmo_clustering"], valores_k=parametros["valores_k"],
        max_df=parametros["max_df"], normalizar_vogal_repetida=parametros["normalizar_vogal_repetida"],
    )

    logger.info("Concluido. Resultados em: %s", args.output_dir)


if __name__ == "__main__":
    main()
