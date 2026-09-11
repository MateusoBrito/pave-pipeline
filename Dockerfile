FROM apache/airflow:2.8.1-python3.11
USER airflow

# torch pinned a 2.6.0+cu124 (não "torch" solto): a GPU deste host é uma GTX 1070
# (compute capability 6.1) - versões recentes de torch (testado: o que "pip install
# torch" solto resolvia, 2.13.x) trazem um cuDNN que exige SM >= 7.5 e falham com
# "cuDNN version ... is not compatible with devices with SM < 7.5" na primeira
# operação real na GPU (Stanza). 2.6.0+cu124 é a mesma versão que já funciona no
# .venv do próprio pave-tm neste host - mantém consistência em vez de descobrir de
# novo por tentativa e erro.
RUN pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cu124 \
    pymongo \
    requests \
    pyyaml \
    google-api-python-client \
    pandas \
    nltk \
    spacy \
    unidecode \
    tqdm \
    sentence-transformers \
    torch==2.6.0 \
    sqlalchemy \
    alembic \
    psycopg2-binary \
    bertopic \
    umap-learn \
    scikit-learn

# stanza/pyspellchecker EM LINHA SEPARADA da lista acima, de propósito: colocar tudo
# numa linha só invalida o cache do Docker pra ela inteira (reinstala torch/CUDA/tudo,
# não só os 2 pacotes novos) - já ficou sem espaço em disco uma vez por causa disso.
# Nova linha = só essa camada precisa ser refeita quando algo aqui mudar.
RUN pip install --no-cache-dir \
    stanza \
    pyspellchecker

# Download NLTK data during build to speed up execution
RUN python -c "import nltk; nltk.download('stopwords'); nltk.download('rslp');"

# Download spaCy Portuguese model used by PreProcessing
RUN python -m spacy download pt_core_news_sm

# Download the Stanza Portuguese model (tokenize/mwt/pos/lemma) used by pave-tm's real
# pre-processing (pipeline.preprocessing.preprocess_dataframe, verb nominalization) -
# dag_topic_modeling.py and dag_inferencia.py run pave-tm code directly against this
# image's Python (pave-tm's own .venv doesn't work from inside this container: its
# `.venv/bin/python` symlink is an absolute host path, /usr/bin/python3.12, that
# doesn't resolve to anything meaningful once mounted here - see pipeline/config.py's
# sys.path fix for the same class of problem with nlp_modules).
RUN python -c "import stanza; stanza.download('pt')"

# (Optional) Pre-download the embedding model to avoid downloading it on every run
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')"
