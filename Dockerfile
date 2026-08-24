FROM apache/airflow:2.8.1-python3.11
USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
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
    torch \
    sqlalchemy \
    alembic \
    psycopg2-binary \
    bertopic \
    umap-learn \
    scikit-learn

# Download NLTK data during build to speed up execution
RUN python -c "import nltk; nltk.download('stopwords'); nltk.download('rslp');"

# Download spaCy Portuguese model used by PreProcessing
RUN python -m spacy download pt_core_news_sm

# (Optional) Pre-download the embedding model to avoid downloading it on every run
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')"
